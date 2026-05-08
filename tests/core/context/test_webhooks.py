"""Webhook outbox + delivery worker tests.

After the outbox refactor, ContextAssembler.assemble() no longer POSTs
to subscribers inline — it writes one ``webhook_outbox`` row per
subscriber. The HTTP delivery happens later in
``core.webhooks.delivery_worker.tick_once`` (called periodically by the
background loop, exercised in-process here for determinism).

Tests are split into two surfaces:
- WebhookPublisher → outbox row written, never any HTTP
- delivery_worker  → drains outbox rows, signs payloads with the
                     webhook's secret, marks delivered on 2xx, and
                     applies exponential backoff on failure
"""
import asyncio
import hashlib
import hmac
import json

import pytest

from core.context.assembler import ContextAssembler
from core.context.webhook_publisher import WebhookPublisher
from core.webhooks import delivery_worker


# ---------------------------------------------------------------------------
# Fake httpx for the delivery_worker tests. The worker uses
# ``httpx.AsyncClient`` directly (not via a factory injection point),
# so we monkeypatch the symbol at module scope.
# ---------------------------------------------------------------------------


class _FakeResponse:
    def __init__(self, status_code=200, text="OK"):
        self.status_code = status_code
        self.text = text


class _FakeAsyncClient:
    def __init__(self, *, response=None, raise_exc=None):
        self._response = response or _FakeResponse(200, "OK")
        self._raise = raise_exc
        self.calls: list = []

    def __call__(self, *args, **kwargs):
        return self  # AsyncClient(timeout=...) returns the instance

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def post(self, url, content=None, headers=None):
        self.calls.append({
            "url": url, "content": content, "headers": dict(headers or {}),
        })
        if self._raise:
            raise self._raise
        return self._response


# ---------------------------------------------------------------------------
# Helpers — minimal application seeding so assemble() works
# ---------------------------------------------------------------------------


async def _seed_application(pg, *, application_id="APP-WH", applicant_id="APL-WH"):
    await pg.save_golden_record({
        "applicant_id": applicant_id,
        "full_name":    "Webhook Tester",
        "first_name":   "Webhook",
        "last_name":    "Tester",
        "dob":          "1990-01-01",
        "ssn_hash":     "wh-h",
        "ssn_last4":    "0000",
        "status":       "active",
        "identity_xrefs": [],
        "application_ids": [application_id],
    })
    await pg.save_application({
        "application_id":  application_id,
        "applicant_id":    applicant_id,
        "co_applicant_id": None,
        "los_id":          "LOS-WH",
        "status":          "active",
    })
    await pg.save_income_profile({
        "applicant_id":   applicant_id,
        "application_id": application_id,
        "assembled_at":   "2026-05-06T00:00:00",
        "primary_borrower": {
            "borrower_id": applicant_id, "role": "primary",
            "qualifying_monthly": 8_000, "overall_confidence": 0.95,
            "sources": [],
        },
        "co_borrower": None,
        "combined_qualifying_monthly": 8_000,
        "qualifying_score_used": 720,
        "monthly_debt_obligations": [],
        "total_monthly_obligations": 0.0,
        "dti_inputs_ready": True,
        "requires_human_review": False,
        "lineage_hash": "h",
    })
    await pg.save_credit_profile({
        "applicant_id": applicant_id, "mid_score": 720, "credit_band": "prime",
        "total_monthly_obligations": 0.0,
    })


# ---------------------------------------------------------------------------
# Publisher — writes outbox rows, never makes HTTP calls
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_publisher_writes_outbox_row_per_subscriber(
    postgres_store, redis_store
):
    await _seed_application(postgres_store)
    await postgres_store.save_webhook({
        "name":   "decision-os",
        "url":    "https://example.test/webhook",
        "secret": None,
        "events": ["context_updated"],
    })

    publisher = WebhookPublisher(postgres_store)
    assembler = ContextAssembler(postgres_store, redis_store, publisher)
    await assembler.assemble("APP-WH")

    outbox = getattr(postgres_store, "outbox", [])
    assert len(outbox) == 1
    row = outbox[0]
    assert row["status"] == "pending"
    assert row["event_type"] == "context_updated"
    assert row["application_id"] == "APP-WH"
    assert row["payload"]["event_type"] == "context_updated"
    assert row["payload"]["application_id"] == "APP-WH"
    assert "readiness" in row["payload"]["payload"]


@pytest.mark.asyncio
async def test_publisher_writes_one_row_per_subscriber(
    postgres_store, redis_store
):
    await _seed_application(postgres_store)
    for name, url in [("a", "https://a.test"), ("b", "https://b.test"), ("c", "https://c.test")]:
        await postgres_store.save_webhook({
            "name": name, "url": url, "secret": None,
            "events": ["context_updated"],
        })

    publisher = WebhookPublisher(postgres_store)
    assembler = ContextAssembler(postgres_store, redis_store, publisher)
    await assembler.assemble("APP-WH")

    assert len(getattr(postgres_store, "outbox", [])) == 3


@pytest.mark.asyncio
async def test_no_active_webhook_no_outbox_row(postgres_store, redis_store):
    """When no webhook is registered, assemble() still works and no
    outbox rows are written."""
    await _seed_application(postgres_store)
    publisher = WebhookPublisher(postgres_store)
    assembler = ContextAssembler(postgres_store, redis_store, publisher)
    await assembler.assemble("APP-WH")
    assert getattr(postgres_store, "outbox", []) == []


# ---------------------------------------------------------------------------
# Delivery worker — drains outbox rows, makes HTTP, marks status
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_worker_delivers_and_marks_row(
    postgres_store, redis_store, monkeypatch
):
    await _seed_application(postgres_store)
    webhook_id = await postgres_store.save_webhook({
        "name":   "decision-os",
        "url":    "https://example.test/webhook",
        "secret": None,
        "events": ["context_updated"],
    })

    publisher = WebhookPublisher(postgres_store)
    assembler = ContextAssembler(postgres_store, redis_store, publisher)
    await assembler.assemble("APP-WH")

    fake = _FakeAsyncClient()
    monkeypatch.setattr(delivery_worker, "httpx",
                        type("M", (), {"AsyncClient": fake}))

    attempted = await delivery_worker.tick_once(postgres_store)
    assert attempted == 1

    # The HTTP POST landed at the registered URL with the payload from
    # the outbox row.
    assert len(fake.calls) == 1
    body = json.loads(fake.calls[0]["content"].decode())
    assert body["event_type"] == "context_updated"
    assert body["application_id"] == "APP-WH"

    # The outbox row was flipped to delivered.
    outbox = postgres_store.outbox
    assert outbox[0]["status"] == "delivered"
    assert outbox[0]["delivered_at"] is not None


@pytest.mark.asyncio
async def test_worker_signs_payload_with_webhook_secret(
    postgres_store, redis_store, monkeypatch
):
    await _seed_application(postgres_store)
    await postgres_store.save_webhook({
        "name":   "signed",
        "url":    "https://example.test/secure",
        "secret": "test_secret",
        "events": ["context_updated"],
    })

    publisher = WebhookPublisher(postgres_store)
    assembler = ContextAssembler(postgres_store, redis_store, publisher)
    await assembler.assemble("APP-WH")

    fake = _FakeAsyncClient()
    monkeypatch.setattr(delivery_worker, "httpx",
                        type("M", (), {"AsyncClient": fake}))

    await delivery_worker.tick_once(postgres_store)

    headers = fake.calls[0]["headers"]
    assert "X-EDMS-Signature" in headers
    sig_header = headers["X-EDMS-Signature"]
    assert sig_header.startswith("sha256=")
    expected = "sha256=" + hmac.new(
        b"test_secret", fake.calls[0]["content"], hashlib.sha256,
    ).hexdigest()
    assert sig_header == expected


@pytest.mark.asyncio
async def test_worker_retries_with_backoff_on_failure(
    postgres_store, redis_store, monkeypatch
):
    await _seed_application(postgres_store)
    webhook_id = await postgres_store.save_webhook({
        "name":   "broken",
        "url":    "https://example.test/down",
        "secret": None,
        "events": ["context_updated"],
    })

    publisher = WebhookPublisher(postgres_store)
    assembler = ContextAssembler(postgres_store, redis_store, publisher)
    await assembler.assemble("APP-WH")

    fake = _FakeAsyncClient(raise_exc=RuntimeError("connection refused"))
    monkeypatch.setattr(delivery_worker, "httpx",
                        type("M", (), {"AsyncClient": fake}))

    await delivery_worker.tick_once(postgres_store)

    # First failure: row is still pending (one attempt left), error
    # captured, next_retry_at pushed out, failure_count bumped.
    row = postgres_store.outbox[0]
    assert row["status"] == "pending"
    assert row["attempts"] == 1
    assert "connection refused" in (row["last_error"] or "")
    assert postgres_store.webhooks[webhook_id]["failure_count"] == 1


@pytest.mark.asyncio
async def test_worker_marks_failed_after_max_attempts(
    postgres_store, redis_store, monkeypatch
):
    """After max_attempts (3 by default), a persistently-failing
    delivery flips to status='failed' and the worker stops retrying."""
    await _seed_application(postgres_store)
    await postgres_store.save_webhook({
        "name":   "always-down",
        "url":    "https://example.test/down",
        "secret": None,
        "events": ["context_updated"],
    })

    publisher = WebhookPublisher(postgres_store)
    assembler = ContextAssembler(postgres_store, redis_store, publisher)
    await assembler.assemble("APP-WH")

    fake = _FakeAsyncClient(raise_exc=RuntimeError("503"))
    monkeypatch.setattr(delivery_worker, "httpx",
                        type("M", (), {"AsyncClient": fake}))

    # Force the next_retry_at back in time after each failure so the
    # FakePG re-includes the row on the next tick. Real Postgres uses
    # NOW() + interval so successive ticks won't re-run until the
    # interval elapses; here we simulate the elapsed-time path.
    from datetime import datetime, timezone
    for _ in range(3):
        await delivery_worker.tick_once(postgres_store)
        for r in postgres_store.outbox:
            if r["status"] == "pending":
                r["next_retry_at"] = datetime.now(timezone.utc)

    row = postgres_store.outbox[0]
    assert row["status"] == "failed"
    assert row["attempts"] >= 3


# ---------------------------------------------------------------------------
# Context-versioning regression coverage (unaffected by the outbox refactor)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_context_version_stored_on_assembly(postgres_store, redis_store):
    await _seed_application(postgres_store)
    assembler = ContextAssembler(postgres_store, redis_store)
    await assembler.assemble("APP-WH", trigger_event="manual_test")

    versions = await postgres_store.get_context_versions("APP-WH")
    assert len(versions) == 1
    v = versions[0]
    assert v["application_id"] == "APP-WH"
    assert v["trigger_event"] == "manual_test"
    assert v["context_data"]["application_id"] == "APP-WH"


@pytest.mark.asyncio
async def test_context_at_timestamp(postgres_store, redis_store):
    await _seed_application(postgres_store)
    assembler = ContextAssembler(postgres_store, redis_store)

    # Manually pin the timestamps so the test is deterministic.
    ctx1 = await assembler.assemble("APP-WH", trigger_event="t1")
    postgres_store.context_versions[-1]["assembled_at"] = "2026-05-01T09:00:00"

    # Bump the borrower's qualifying income, then re-assemble.
    inc = postgres_store.income_profiles["APL-WH"]
    inc["primary_borrower"]["qualifying_monthly"] = 12_000
    inc["combined_qualifying_monthly"] = 12_000
    await redis_store.invalidate_context("APP-WH")
    ctx2 = await assembler.assemble("APP-WH", trigger_event="t2")
    postgres_store.context_versions[-1]["assembled_at"] = "2026-05-01T10:00:00"

    at_t1 = await postgres_store.get_context_at("APP-WH", "2026-05-01T09:30:00")
    assert at_t1 is not None
    assert at_t1["context_data"]["combined_qualifying_monthly"] == 8000.0

    at_t2 = await postgres_store.get_context_at("APP-WH", "2026-05-01T11:00:00")
    assert at_t2 is not None
    assert at_t2["context_data"]["combined_qualifying_monthly"] == 12000.0
