"""Phase E — webhook publisher + context versioning tests."""
import asyncio
import hashlib
import hmac
import json

import pytest

from core.context.assembler import ContextAssembler
from core.context.webhook_publisher import WebhookPublisher


# ---------------------------------------------------------------------------
# Fake httpx client — async context manager that records POSTs.
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


def _factory_for(client: _FakeAsyncClient):
    """A factory the WebhookPublisher will call to obtain a client per
    delivery — we always return the same singleton so the test can
    inspect ``client.calls``."""
    def _factory():
        return client
    return _factory


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
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_webhook_fires_on_context_update(postgres_store, redis_store):
    await _seed_application(postgres_store)
    await postgres_store.save_webhook({
        "name":   "decision-os",
        "url":    "https://example.test/webhook",
        "secret": None,
        "events": ["context_updated"],
    })

    fake = _FakeAsyncClient()
    publisher = WebhookPublisher(
        postgres_store, http_client_factory=_factory_for(fake)
    )
    assembler = ContextAssembler(postgres_store, redis_store, publisher)

    await assembler.assemble("APP-WH")

    assert len(fake.calls) == 1
    call = fake.calls[0]
    assert call["url"] == "https://example.test/webhook"
    body = json.loads(call["content"].decode())
    assert body["event_type"] == "context_updated"
    assert body["application_id"] == "APP-WH"
    assert "readiness" in body["payload"]

    # Delivery row written, success=True (200)
    assert len(postgres_store.webhook_deliveries) == 1
    delivery = postgres_store.webhook_deliveries[0]
    assert delivery["success"] is True
    assert delivery["application_id"] == "APP-WH"


@pytest.mark.asyncio
async def test_webhook_signature_added_when_secret_set(
    postgres_store, redis_store
):
    await _seed_application(postgres_store)
    await postgres_store.save_webhook({
        "name":   "signed",
        "url":    "https://example.test/secure",
        "secret": "test_secret",
        "events": ["context_updated"],
    })

    fake = _FakeAsyncClient()
    publisher = WebhookPublisher(
        postgres_store, http_client_factory=_factory_for(fake)
    )
    assembler = ContextAssembler(postgres_store, redis_store, publisher)
    await assembler.assemble("APP-WH")

    headers = fake.calls[0]["headers"]
    assert "X-EDMS-Signature" in headers
    sig_header = headers["X-EDMS-Signature"]
    assert sig_header.startswith("sha256=")

    expected = "sha256=" + hmac.new(
        b"test_secret", fake.calls[0]["content"], hashlib.sha256
    ).hexdigest()
    assert sig_header == expected


@pytest.mark.asyncio
async def test_webhook_failure_increments_failure_count(
    postgres_store, redis_store
):
    await _seed_application(postgres_store)
    webhook_id = await postgres_store.save_webhook({
        "name":   "broken",
        "url":    "https://example.test/down",
        "secret": None,
        "events": ["context_updated"],
    })

    fake = _FakeAsyncClient(raise_exc=RuntimeError("connection refused"))
    publisher = WebhookPublisher(
        postgres_store, http_client_factory=_factory_for(fake)
    )
    assembler = ContextAssembler(postgres_store, redis_store, publisher)
    await assembler.assemble("APP-WH")

    delivery = postgres_store.webhook_deliveries[0]
    assert delivery["success"] is False
    assert "connection refused" in (delivery["response_body"] or "")
    assert postgres_store.webhooks[webhook_id]["failure_count"] == 1


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


@pytest.mark.asyncio
async def test_no_active_webhook_no_delivery(postgres_store, redis_store):
    """When no webhook is registered, assemble() still works and no
    delivery rows are written."""
    await _seed_application(postgres_store)
    fake = _FakeAsyncClient()
    publisher = WebhookPublisher(
        postgres_store, http_client_factory=_factory_for(fake)
    )
    assembler = ContextAssembler(postgres_store, redis_store, publisher)
    await assembler.assemble("APP-WH")
    assert fake.calls == []
    assert postgres_store.webhook_deliveries == []
