"""Phase F — observability endpoint tests."""
import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def app_seeded(aggregation_service, postgres_store, redis_store, xref_store):
    from api.main import app as fastapi_app
    fastapi_app.state.postgres_store     = postgres_store
    fastapi_app.state.redis_store        = redis_store
    fastapi_app.state.xref_store         = xref_store
    fastapi_app.state.aggregation_service = aggregation_service
    return fastapi_app


async def _seed_basic(pg, application_id="APP-OBS", applicant_id="APL-OBS"):
    await pg.save_golden_record({
        "applicant_id": applicant_id, "full_name": "Obs Watcher",
        "first_name": "Obs", "last_name": "Watcher",
        "dob": "1990-01-01", "ssn_hash": "obs-h", "ssn_last4": "9000",
        "status": "active", "identity_xrefs": [],
        "application_ids": [application_id],
    })
    await pg.save_application({
        "application_id":  application_id,
        "applicant_id":    applicant_id,
        "co_applicant_id": None,
        "los_id":          "LOS-OBS",
        "status":          "active",
        "created_at":      "2026-05-06T00:00:00",
    })
    await pg.save_income_profile({
        "applicant_id":   applicant_id,
        "application_id": application_id,
        "assembled_at":   "2026-05-06T00:00:00",
        "primary_borrower": {
            "borrower_id": applicant_id, "role": "primary",
            "qualifying_monthly": 7_700, "overall_confidence": 0.95,
            "sources": [],
        },
        "co_borrower": None,
        "combined_qualifying_monthly": 7_700,
        "qualifying_score_used": 720,
        "monthly_debt_obligations": [], "total_monthly_obligations": 0.0,
        "dti_inputs_ready": True, "requires_human_review": False,
        "lineage_hash": "h",
    })
    await pg.save_credit_profile({
        "applicant_id": applicant_id, "mid_score": 720,
        "credit_band": "prime", "total_monthly_obligations": 0.0,
    })
    # One persisted document so the timeline + pipeline-state have content
    await pg.save_document({
        "document_id":      "DOC-OBS-1",
        "applicant_id":     applicant_id,
        "application_id":   application_id,
        "document_type":    "W2_CURRENT",
        "document_category": "income",
        "borrower_role":    "primary",
        "extracted_fields": {"box1_wages": 92400, "employer_name": "Acme"},
        "confidence_score": 0.94,
    })


# ---------------------------------------------------------------------------


def test_dashboard_returns_html(app_seeded):
    client = TestClient(app_seeded)
    resp = client.get("/dashboard")
    assert resp.status_code == 200, resp.text
    assert resp.headers["content-type"].startswith("text/html")
    body = resp.text
    assert "EDMS Pipeline Dashboard" in body
    assert "Application ID" in body
    assert "AUS" in body


def test_dashboard_no_auth_required(app_seeded):
    """Read-only dashboard — no API key required so it can be opened
    in a browser tab."""
    client = TestClient(app_seeded)
    resp = client.get("/dashboard")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_pipeline_state_correct_structure(app_seeded, postgres_store):
    await _seed_basic(postgres_store)
    client = TestClient(app_seeded)
    resp = client.get(
        "/application/APP-OBS/pipeline-state",
        headers={"X-API-Key": "test_key"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["application_id"] == "APP-OBS"
    for key in (
        "application", "borrowers", "property", "graph", "vendor_checks",
        "context", "readiness", "pipeline_complete",
    ):
        assert key in data
    assert len(data["borrowers"]) == 1
    primary = data["borrowers"][0]
    assert primary["role"] == "primary"
    assert len(primary["documents"]) == 1
    assert primary["documents"][0]["document_type"] == "W2_CURRENT"
    assert "redis_keys" in primary
    assert "income" in primary["redis_keys"]


@pytest.mark.asyncio
async def test_pipeline_state_404_unknown_application(app_seeded):
    client = TestClient(app_seeded)
    resp = client.get(
        "/application/APP-DOES-NOT-EXIST/pipeline-state",
        headers={"X-API-Key": "test_key"},
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_timeline_ordered(app_seeded, postgres_store):
    await _seed_basic(postgres_store)

    # Force a context assembly so a context_versions row exists.
    from core.context.assembler import ContextAssembler
    assembler = ContextAssembler(
        postgres_store, app_seeded.state.redis_store
    )
    await assembler.assemble("APP-OBS", trigger_event="seed")
    # Pin the timestamp so the ordering is deterministic vs. the
    # application's created_at.
    postgres_store.context_versions[-1]["assembled_at"] = "2026-05-06T01:00:00"

    client = TestClient(app_seeded)
    resp = client.get(
        "/application/APP-OBS/timeline",
        headers={"X-API-Key": "test_key"},
    )
    assert resp.status_code == 200, resp.text
    events = resp.json()["events"]
    assert len(events) >= 2
    # First event is the application_submitted seed
    assert events[0]["event_type"] == "application_submitted"
    # Events sorted ascending by timestamp
    timestamps = [e["timestamp"] for e in events]
    assert timestamps == sorted(timestamps)
    # context_assembled event present
    assert any(e["event_type"] == "context_assembled" for e in events)


@pytest.mark.asyncio
async def test_pipeline_state_includes_redis_keys_for_active_keys(
    app_seeded, postgres_store, redis_store
):
    await _seed_basic(postgres_store)
    # Warm a redis key
    redis_store.set_income_profile("APL-OBS", {"qualifying_monthly": 7700})

    client = TestClient(app_seeded)
    resp = client.get(
        "/application/APP-OBS/pipeline-state",
        headers={"X-API-Key": "test_key"},
    )
    assert resp.status_code == 200
    primary = resp.json()["borrowers"][0]
    income_state = primary["redis_keys"]["income"]
    assert income_state["present"] is True
    # ttl_seconds should be a positive number near TTL_INCOME_PROFILE
    assert isinstance(income_state["ttl_seconds"], int)
    assert income_state["ttl_seconds"] > 0
