"""Phase E — persona slice + missing-documents endpoint tests."""
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


async def _seed(pg, *, application_id="APP-SL", applicant_id="APL-SL",
                co_applicant_id=None):
    await pg.save_golden_record({
        "applicant_id": applicant_id, "full_name": "Slice Tester",
        "first_name": "Slice", "last_name": "Tester",
        "dob": "1990-01-01", "ssn_hash": "sl-h", "ssn_last4": "1111",
        "status": "active", "identity_xrefs": [], "application_ids": [application_id],
    })
    if co_applicant_id:
        await pg.save_golden_record({
            "applicant_id": co_applicant_id, "full_name": "Co Tester",
            "first_name": "Co", "last_name": "Tester",
            "dob": "1992-02-02", "ssn_hash": "co-h", "ssn_last4": "2222",
            "status": "active", "identity_xrefs": [],
            "application_ids": [application_id],
        })
    await pg.save_application({
        "application_id": application_id,
        "applicant_id":   applicant_id,
        "co_applicant_id": co_applicant_id,
        "los_id":         "LOS-SL",
        "status":         "active",
    })
    await pg.save_income_profile({
        "applicant_id": applicant_id, "application_id": application_id,
        "assembled_at": "2026-05-06T00:00:00",
        "primary_borrower": {
            "borrower_id": applicant_id, "role": "primary",
            "qualifying_monthly": 8_000, "overall_confidence": 0.95,
            "sources": [{"source_type": "W2_SALARIED", "qualifying_monthly": 8_000}],
        },
        "co_borrower": (
            {"borrower_id": co_applicant_id, "role": "co_borrower",
             "qualifying_monthly": 4_000, "overall_confidence": 0.92, "sources": []}
            if co_applicant_id else None
        ),
        "combined_qualifying_monthly": 12_000 if co_applicant_id else 8_000,
        "qualifying_score_used": 720,
        "monthly_debt_obligations": [], "total_monthly_obligations": 0.0,
        "dti_inputs_ready": True, "requires_human_review": False,
        "lineage_hash": "h",
    })
    await pg.save_credit_profile({
        "applicant_id": applicant_id, "mid_score": 720,
        "credit_band": "prime", "total_monthly_obligations": 400,
    })


@pytest.mark.asyncio
async def test_income_slice_correct(app_seeded, postgres_store):
    await _seed(postgres_store, co_applicant_id="APL-CO-SL")
    client = TestClient(app_seeded)
    headers = {"X-API-Key": "test_key"}
    resp = client.get(
        "/application/APP-SL/context/income", headers=headers
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["combined_qualifying_monthly"] == 12_000.0
    assert data["primary_qualifying_monthly"] == 8_000.0
    assert data["co_borrower_qualifying"] == 4_000.0
    # Income slice does not leak property fields
    assert "appraised_value" not in data
    assert "piti_total" not in data


@pytest.mark.asyncio
async def test_credit_slice_correct(app_seeded, postgres_store):
    await _seed(postgres_store)
    client = TestClient(app_seeded)
    resp = client.get(
        "/application/APP-SL/context/credit",
        headers={"X-API-Key": "test_key"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["primary_mid_score"] == 720
    assert data["primary_credit_band"] == "prime"


@pytest.mark.asyncio
async def test_compliance_slice_includes_missing(app_seeded, postgres_store):
    await _seed(postgres_store)
    client = TestClient(app_seeded)
    resp = client.get(
        "/application/APP-SL/context/compliance",
        headers={"X-API-Key": "test_key"},
    )
    assert resp.status_code == 200
    data = resp.json()
    # No property → "appraisal" must be in missing_items
    assert "appraisal" in data["missing_items"]
    assert "readiness" in data


@pytest.mark.asyncio
async def test_fraud_slice_empty_when_no_vendor_returns(
    app_seeded, postgres_store
):
    await _seed(postgres_store)
    client = TestClient(app_seeded)
    resp = client.get(
        "/application/APP-SL/context/fraud",
        headers={"X-API-Key": "test_key"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["fraud_score"] is None
    assert data["ssn_valid"] is None


@pytest.mark.asyncio
async def test_missing_documents_list(app_seeded, postgres_store):
    await _seed(postgres_store)
    client = TestClient(app_seeded)
    resp = client.get(
        "/application/APP-SL/missing-documents",
        headers={"X-API-Key": "test_key"},
    )
    assert resp.status_code == 200
    data = resp.json()
    # No documents at all yet — required catalog items should all be missing
    assert "W2_CURRENT" in data["borrower_missing"]
    assert "BANK_STATEMENT_M1" in data["borrower_missing"]
    assert "APPRAISAL_URAR" in data["property_missing"]
    assert "AUS_DU_FINDINGS" in data["vendor_missing"]
    assert any(
        item["doc_type"] == "HOI_BINDER" for item in data["checklist"]
    )


@pytest.mark.asyncio
async def test_register_and_list_webhook(app_seeded, postgres_store):
    client = TestClient(app_seeded)
    headers = {"X-API-Key": "test_key"}
    resp = client.post(
        "/webhooks", headers=headers,
        json={
            "name":   "decision-os",
            "url":    "https://example.test/hook",
            "events": ["context_updated"],
        },
    )
    assert resp.status_code == 200, resp.text
    webhook_id = resp.json()["webhook_id"]
    assert resp.json()["is_active"] is True

    list_resp = client.get("/webhooks", headers=headers)
    assert list_resp.status_code == 200
    assert list_resp.json()["count"] == 1

    del_resp = client.delete(f"/webhooks/{webhook_id}", headers=headers)
    assert del_resp.status_code == 200
    assert del_resp.json()["is_active"] is False
