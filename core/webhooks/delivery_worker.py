"""Async webhook delivery worker.

Runs as a background asyncio task inside the API process. Drains
``webhook_outbox`` rows whose ``next_retry_at`` has elapsed, POSTs
each to its subscriber, and either flips ``status='delivered'`` or
applies exponential backoff for the next attempt.

Concurrency model:
- A polling loop wakes every ``interval_seconds`` (5s default) and
  calls :func:`tick_once`.
- :func:`tick_once` claims up to ``batch_size`` rows via
  ``SELECT ... FOR UPDATE SKIP LOCKED``, so multiple worker replicas
  never deliver the same row twice.
- Within a batch, deliveries run concurrently under a per-tick
  ``Semaphore(max_concurrent)`` (default 10) — bounds outbound
  socket pressure while keeping the head-of-line free for fast
  subscribers.
- Backoff: 2 ** attempts × 30 seconds. Caps at ``max_attempts`` (3
  by default per row), then ``status='failed'``.

The worker also writes a ``webhook_deliveries`` audit row on every
attempt and bumps the per-webhook ``failure_count`` on hard failures
— preserves the existing observability surface.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
from typing import Optional

import httpx

logger = logging.getLogger(__name__)


_DEFAULT_INTERVAL_SECONDS = 5
_DEFAULT_BATCH_SIZE       = 50
_DEFAULT_MAX_CONCURRENT   = 10
_DEFAULT_HTTP_TIMEOUT_S   = 10
_BACKOFF_BASE_SECONDS     = 30


def _backoff_seconds(attempts: int) -> int:
    """Backoff after the (attempts)-th failure. ``attempts`` here is
    the count BEFORE incrementing, so the first failure (attempts=0)
    waits 30s, second 60s, third 120s. Capped at 1h to keep retries
    visible in dashboards rather than disappearing into the future."""
    return min(_BACKOFF_BASE_SECONDS * (2 ** attempts), 3600)


def _build_headers(webhook: dict, body_bytes: bytes, event_type: str,
                   application_id: Optional[str]) -> dict:
    """Match the legacy publisher's header shape exactly so existing
    subscribers don't need to be re-onboarded after the outbox switch."""
    headers = {
        "Content-Type":     "application/json",
        "X-EDMS-Event":     event_type,
        "X-Application-ID": application_id or "",
    }
    secret = webhook.get("secret")
    if secret:
        sig = hmac.new(secret.encode(), body_bytes, hashlib.sha256).hexdigest()
        headers["X-EDMS-Signature"] = f"sha256={sig}"
    return headers


async def _deliver_one(
    item: dict,
    pg,
    sem: asyncio.Semaphore,
    http_timeout: int = _DEFAULT_HTTP_TIMEOUT_S,
) -> None:
    """Deliver one outbox row. Never raises — every failure mode flips
    the row to retry / failed and the worker loop continues."""
    outbox_id = str(item["id"])
    payload   = item["payload"]
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = {}

    event_type     = item.get("event_type") or payload.get("event_type") or "context_updated"
    application_id = item.get("application_id") or payload.get("application_id")

    # Look the webhook up at delivery time so a deactivation or URL
    # change between enqueue and delivery is honoured.
    try:
        webhook = await pg.get_webhook(str(item["webhook_id"]))
    except Exception as exc:
        await pg.mark_outbox_retry(
            outbox_id, f"webhook_lookup_failed: {exc}",
            _backoff_seconds(item.get("attempts", 0)),
        )
        return

    if not webhook or not webhook.get("is_active", True):
        await pg.mark_outbox_failed(outbox_id, "webhook_inactive_or_missing")
        return

    body_bytes = json.dumps(payload, default=str).encode()
    headers    = _build_headers(webhook, body_bytes, event_type, application_id)

    status:    Optional[int] = None
    resp_body: Optional[str] = None
    success                 = False
    last_error: Optional[str] = None

    async with sem:
        try:
            async with httpx.AsyncClient(timeout=http_timeout) as client:
                resp = await client.post(
                    webhook["url"], content=body_bytes, headers=headers,
                )
                status    = resp.status_code
                resp_body = (resp.text or "")[:500]
                success   = resp.status_code < 300
                if not success:
                    last_error = f"HTTP {status}: {resp_body[:200]}"
        except Exception as exc:
            last_error = str(exc)[:500]
            resp_body  = last_error

    # Persist a delivery audit row regardless of outcome — preserves
    # the same observability the legacy synchronous publisher offered.
    try:
        await pg.save_webhook_delivery({
            "webhook_id":      str(webhook.get("webhook_id")),
            "event_type":      event_type,
            "application_id":  application_id,
            "payload":         payload,
            "response_status": status,
            "response_body":   resp_body,
            "success":         success,
        })
    except Exception as exc:
        logger.warning(
            "outbox_delivery_audit_failed",
            extra={"outbox_id": outbox_id, "error": str(exc)[:200]},
        )

    if success:
        await pg.mark_outbox_delivered(outbox_id)
        logger.info(
            "outbox_delivered",
            extra={
                "outbox_id":  outbox_id,
                "webhook_id": str(webhook.get("webhook_id")),
                "status":     status,
            },
        )
        return

    # Failure path — bump retry counter + backoff, or flip to failed
    # if we're out of attempts.
    new_state = await pg.mark_outbox_retry(
        outbox_id, last_error or "unknown",
        _backoff_seconds(item.get("attempts", 0)),
    )
    try:
        await pg.increment_webhook_failures(str(webhook.get("webhook_id")))
    except Exception as exc:
        logger.warning(
            "outbox_failure_increment_skipped",
            extra={"error": str(exc)[:200]},
        )
    logger.warning(
        "outbox_delivery_failed",
        extra={
            "outbox_id":  outbox_id,
            "webhook_id": str(webhook.get("webhook_id")),
            "status":     new_state.get("status"),
            "attempts":   new_state.get("attempts"),
            "next_retry": (
                new_state.get("next_retry_at").isoformat()
                if hasattr(new_state.get("next_retry_at"), "isoformat")
                else str(new_state.get("next_retry_at"))
            ),
            "error":      last_error,
        },
    )


async def tick_once(
    pg,
    batch_size: int = _DEFAULT_BATCH_SIZE,
    max_concurrent: int = _DEFAULT_MAX_CONCURRENT,
    http_timeout: int = _DEFAULT_HTTP_TIMEOUT_S,
) -> int:
    """Drain one batch of pending outbox rows. Returns the number of
    rows attempted on this tick (zero when the queue is empty)."""
    try:
        batch = await pg.get_pending_outbox(limit=batch_size)
    except Exception as exc:
        logger.warning("outbox_poll_failed", extra={"error": str(exc)[:200]})
        return 0

    if not batch:
        return 0

    sem = asyncio.Semaphore(max_concurrent)
    tasks = [_deliver_one(item, pg, sem, http_timeout) for item in batch]
    await asyncio.gather(*tasks, return_exceptions=True)
    return len(batch)


async def run_delivery_loop(
    pg,
    interval_seconds: int = _DEFAULT_INTERVAL_SECONDS,
    batch_size: int = _DEFAULT_BATCH_SIZE,
    max_concurrent: int = _DEFAULT_MAX_CONCURRENT,
    http_timeout: int = _DEFAULT_HTTP_TIMEOUT_S,
    stop_event: Optional[asyncio.Event] = None,
) -> None:
    """Run the polling loop until the API process shuts down or
    ``stop_event`` is set. Designed to be launched via
    ``asyncio.create_task`` from the FastAPI lifespan."""
    logger.info(
        "outbox_worker_started",
        extra={
            "interval_seconds": interval_seconds,
            "batch_size":       batch_size,
            "max_concurrent":   max_concurrent,
        },
    )
    try:
        while True:
            if stop_event is not None and stop_event.is_set():
                break
            try:
                attempted = await tick_once(
                    pg, batch_size=batch_size,
                    max_concurrent=max_concurrent,
                    http_timeout=http_timeout,
                )
                if attempted:
                    logger.debug(
                        "outbox_tick_complete",
                        extra={"attempted": attempted},
                    )
            except Exception as exc:
                # Defensive: a single tick should never kill the worker.
                logger.error(
                    "outbox_tick_unhandled",
                    extra={"error": str(exc)[:200]},
                )
            try:
                await asyncio.sleep(interval_seconds)
            except asyncio.CancelledError:
                logger.info("outbox_worker_cancelled")
                raise
    finally:
        logger.info("outbox_worker_stopped")
