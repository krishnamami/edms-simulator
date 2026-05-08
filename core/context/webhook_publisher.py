"""Webhook publisher — async fan-out via the webhook_outbox.

Every ContextAssembler.assemble() call publishes a ``context_updated``
event. WebhookPublisher used to POST to subscribers inline — a slow
or dead subscriber would block the upload response for up to N×timeout
seconds. The current path writes one ``webhook_outbox`` row per
subscriber instead (a single INSERT each, milliseconds of latency)
and returns immediately. The actual HTTP delivery happens in
``core.webhooks.delivery_worker.run_delivery_loop`` running as a
background asyncio task inside the API process.

This module no longer makes outbound HTTP calls; the
``http_client_factory`` constructor parameter is kept as a no-op so
existing tests that pass it don't break, but it's unused.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from core.tenancy import current_tenant_id

logger = logging.getLogger(__name__)


class WebhookPublisher:
    def __init__(self, postgres_store, http_client_factory=None):
        self.pg = postgres_store
        # Kept for backward-compat with tests that swap in a mock
        # httpx-like factory; the outbox path makes no HTTP calls so
        # the factory is never invoked.
        self._http_client_factory = http_client_factory

    async def publish(
        self,
        event_type: str,
        application_id: str,
        payload: dict,
    ) -> None:
        """Fan out an event by writing one outbox row per subscriber.

        Never raises — outbox-write failures are logged but the caller's
        request continues regardless. The delivery worker takes over
        from there: it polls pending rows, POSTs, and applies
        exponential backoff on failure.
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

        # The body shape stored in the outbox matches what the worker
        # POSTs verbatim — keeping the publisher responsible for the
        # envelope (event_type / application_id / timestamp / payload)
        # so the worker stays a thin sender.
        body = {
            "event_type":     event_type,
            "application_id": application_id,
            "timestamp":      datetime.utcnow().isoformat(),
            "payload":        payload,
        }

        tenant_id = current_tenant_id()
        for webhook in webhooks:
            try:
                await self.pg.insert_outbox(
                    webhook_id=webhook.get("webhook_id"),
                    event_type=event_type,
                    payload=body,
                    application_id=application_id,
                    tenant_id=tenant_id,
                )
            except Exception as exc:
                logger.error(
                    "webhook_outbox_write_failed",
                    extra={
                        "webhook_id": str(webhook.get("webhook_id")),
                        "error": str(exc)[:200],
                    },
                )
