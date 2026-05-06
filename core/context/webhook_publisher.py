"""Webhook publisher — fan-out of context_updated events to subscribers.

Each ContextAssembler.assemble() call publishes a ``context_updated``
event. WebhookPublisher loads every active webhook subscribed to the
event type, POSTs the JSON payload (with optional HMAC signature), and
records the delivery + outcome in ``webhook_deliveries``. Failures
increment the webhook's ``failure_count`` but never propagate — the
core assembly path is never blocked by an external endpoint.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


class WebhookPublisher:
    def __init__(self, postgres_store, http_client_factory=None):
        self.pg = postgres_store
        # Indirection so tests can swap in a mock httpx-like client.
        self._http_client_factory = http_client_factory

    async def publish(
        self,
        event_type: str,
        application_id: str,
        payload: dict,
    ) -> None:
        """Fan out an event to every active webhook subscribed to it.

        Never raises — delivery failures are logged + persisted but the
        caller's request continues regardless.
        """
        try:
            webhooks = await self.pg.get_active_webhooks(event_type)
        except Exception as exc:
            logger.warning(
                "webhook_lookup_failed", extra={"error": str(exc)}
            )
            return

        if not webhooks:
            return

        for webhook in webhooks:
            try:
                await self._deliver(webhook, event_type, application_id, payload)
            except Exception as exc:
                logger.error(
                    "webhook_delivery_unhandled",
                    extra={
                        "webhook_id": webhook.get("webhook_id"),
                        "error": str(exc)[:200],
                    },
                )

    async def _deliver(
        self,
        webhook: dict,
        event_type: str,
        application_id: str,
        payload: dict,
    ) -> None:
        body = {
            "event_type":     event_type,
            "application_id": application_id,
            "timestamp":      datetime.utcnow().isoformat(),
            "payload":        payload,
        }
        body_bytes = json.dumps(body, default=str).encode()

        headers = {
            "Content-Type":     "application/json",
            "X-EDMS-Event":     event_type,
            "X-Application-ID": application_id or "",
        }
        secret = webhook.get("secret")
        if secret:
            sig = hmac.new(
                secret.encode(), body_bytes, hashlib.sha256
            ).hexdigest()
            headers["X-EDMS-Signature"] = f"sha256={sig}"

        success = False
        status: Optional[int] = None
        resp_body: Optional[str] = None

        try:
            client_ctx = self._open_client()
            async with client_ctx as client:
                resp = await client.post(
                    webhook["url"],
                    content=body_bytes,
                    headers=headers,
                )
                status = resp.status_code
                resp_body = (resp.text or "")[:500]
                success = resp.status_code < 300
                logger.info(
                    "webhook_delivered",
                    extra={
                        "webhook_id": webhook.get("webhook_id"),
                        "event":      event_type,
                        "status":     status,
                    },
                )
        except Exception as exc:
            resp_body = str(exc)[:500]
            logger.error(
                "webhook_failed",
                extra={
                    "webhook_id": webhook.get("webhook_id"),
                    "error":      str(exc)[:200],
                },
            )

        try:
            await self.pg.save_webhook_delivery({
                "webhook_id":      webhook.get("webhook_id"),
                "event_type":      event_type,
                "application_id":  application_id,
                "payload":         body,
                "response_status": status,
                "response_body":   resp_body,
                "success":         success,
            })
        except Exception as exc:
            logger.warning(
                "webhook_delivery_persist_failed", extra={"error": str(exc)}
            )

        if not success:
            try:
                await self.pg.increment_webhook_failures(webhook.get("webhook_id"))
            except Exception as exc:
                logger.warning(
                    "webhook_failure_increment_failed", extra={"error": str(exc)}
                )

    # ------------------------------------------------------------------
    # HTTP client indirection
    # ------------------------------------------------------------------

    def _open_client(self):
        if self._http_client_factory is not None:
            return self._http_client_factory()
        import httpx
        return httpx.AsyncClient(timeout=10)
