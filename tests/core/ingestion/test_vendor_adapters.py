"""Phase D — vendor return adapter tests."""
import pytest
from fastapi.testclient import TestClient

from core.ingestion.adapters import vendor_synthetic
from core.ingestion.adapters.vendor_aus_adapter import VendorAUSAdapter
from core.ingestion.adapters.vendor_fraud_adapter import VendorFraudAdapter
from core.ingestion.adapters.vendor_ssn_adapter import (
    VendorOFACAdapter,
    VendorSSNAdapter,
)
from core.ingestion.adapters.vendor_voe_adapter import VendorVOEAdapter


# ---------------------------------------------------------------------------
# AUS — DU + LP
# ---------------------------------------------------------------------------


def test_du_approve_extracted():
    xml = vendor_synthetic.generate_du_response(
        credit_score=760, dti=35, ltv=78
    )
    event = VendorAUSAdapter().process({
        "aus_type":    "DU",
        "xml_content": xml,
        "applicant_id":   "APL-1",
        "application_id": "APP-1",
    })
    assert event.document_type == "AUS_DU_FINDINGS"
    assert event.extracted_fields["recommendation"] == "Approve/Eligible"
    assert VendorAUSAdapter.is_approved(event.extracted_fields) is True


def test_du_refer_extracted():
    xml = vendor_synthetic.generate_du_response(
        credit_score=620, dti=55, ltv=95
    )
    event = VendorAUSAdapter().process({
        "aus_type":    "DU",
        "xml_content": xml,
        "applicant_id":   "APL-1",
        "application_id": "APP-1",
    })
    assert "Refer" in (event.extracted_fields["recommendation"] or "")
    assert VendorAUSAdapter.is_approved(event.extracted_fields) is False


def test_lp_accept_extracted():
    xml = vendor_synthetic.generate_lp_response(
        credit_score=740, dti=40, ltv=80
    )
    event = VendorAUSAdapter().process({
        "aus_type":    "LP",
        "xml_content": xml,
        "applicant_id":   "APL-1",
        "application_id": "APP-1",
    })
    assert event.document_type == "AUS_LP_FINDINGS"
    assert event.extracted_fields["recommendation"] == "Accept"
    assert VendorAUSAdapter.is_approved(event.extracted_fields) is True


def test_du_handles_namespaced_xml():
    xml = (
        '<?xml version="1.0"?>'
        '<DU_RESPONSE xmlns:du="http://www.fanniemae.com/du">'
        '  <du:RECOMMENDATION>'
        '    <du:RecommendationDescription>Approve/Eligible</du:RecommendationDescription>'
        '  </du:RECOMMENDATION>'
        '  <du:CasefileIdentifier>CF-NAMESPACED-001</du:CasefileIdentifier>'
        '</DU_RESPONSE>'
    )
    event = VendorAUSAdapter().process({
        "aus_type":    "DU",
        "xml_content": xml,
        "applicant_id":   "APL-1",
        "application_id": "APP-1",
    })
    assert event.extracted_fields["recommendation"] == "Approve/Eligible"
    assert event.extracted_fields["casefile_id"] == "CF-NAMESPACED-001"


# ---------------------------------------------------------------------------
# Fraud — Socure / LexisNexis
# ---------------------------------------------------------------------------


def test_socure_high_risk_requires_review():
    response = vendor_synthetic.generate_fraud_response("APL-1", "high")
    event = VendorFraudAdapter().process({
        "vendor": "socure", "response": response,
        "applicant_id": "APL-1", "application_id": "APP-1",
    })
    assert event.extracted_fields["risk_band"] == "high_risk"
    assert VendorFraudAdapter.requires_review(event.extracted_fields) is True


def test_socure_low_risk_no_review():
    response = vendor_synthetic.generate_fraud_response("APL-1", "low")
    event = VendorFraudAdapter().process({
        "vendor": "socure", "response": response,
        "applicant_id": "APL-1", "application_id": "APP-1",
    })
    assert event.extracted_fields["risk_band"] == "low_risk"
    assert VendorFraudAdapter.requires_review(event.extracted_fields) is False


def test_lexisnexis_response_parsed():
    event = VendorFraudAdapter().process({
        "vendor": "lexisnexis",
        "response": {
            "riskScore": 0.40,
            "kycPass":   True,
            "alerts":    [],
            "ofacHit":   False,
        },
        "applicant_id": "APL-1", "application_id": "APP-1",
    })
    assert event.extracted_fields["vendor"] == "lexisnexis"
    assert event.extracted_fields["risk_score"] == 0.40
    assert event.extracted_fields["risk_band"] == "low_risk"


# ---------------------------------------------------------------------------
# VOE — TWN / Equifax
# ---------------------------------------------------------------------------


def test_twn_active_employment_verified():
    response = vendor_synthetic.generate_voe_response(
        "Acme Corp", 120_000, status="A"
    )
    event = VendorVOEAdapter().process({
        "vendor": "twn", "response": response,
        "applicant_id": "APL-1", "application_id": "APP-1",
    })
    fields = event.extracted_fields
    assert event.document_type == "EMPLOYMENT_VERIFICATION"
    assert fields["employer_name"] == "Acme Corp"
    assert fields["employment_verified"] is True
    assert fields["base_pay_annual"] == 120_000


def test_twn_terminated_not_verified():
    response = vendor_synthetic.generate_voe_response(
        "Acme Corp", 120_000, status="T"
    )
    event = VendorVOEAdapter().process({
        "vendor": "twn", "response": response,
        "applicant_id": "APL-1", "application_id": "APP-1",
    })
    assert event.extracted_fields["employment_verified"] is False


def test_equifax_voe_yes_verified():
    event = VendorVOEAdapter().process({
        "vendor": "equifax_voe",
        "response": {
            "employerName":     "Globex Inc",
            "currentlyEmployed": "Yes",
            "hireDate":         "2019-03-01",
            "annualSalary":     85_000,
            "verificationDate": "2026-05-06",
        },
        "applicant_id": "APL-1", "application_id": "APP-1",
    })
    fields = event.extracted_fields
    assert fields["employment_verified"] is True
    assert fields["annual_salary"] == 85_000


# ---------------------------------------------------------------------------
# SSN + OFAC
# ---------------------------------------------------------------------------


def test_ssn_valid_extracted():
    event = VendorSSNAdapter().process({
        "vendor": "ssa",
        "response": vendor_synthetic.generate_ssn_response(verified=True),
        "applicant_id": "APL-1", "application_id": "APP-1",
    })
    assert event.document_type == "SSN_VALIDATION"
    assert event.extracted_fields["ssn_valid"] is True


def test_ssn_invalid_extracted():
    event = VendorSSNAdapter().process({
        "vendor": "ssa",
        "response": vendor_synthetic.generate_ssn_response(verified=False),
        "applicant_id": "APL-1", "application_id": "APP-1",
    })
    assert event.extracted_fields["ssn_valid"] is False


def test_ofac_clear_extracted():
    event = VendorOFACAdapter().process({
        "vendor": "ofac",
        "response": vendor_synthetic.generate_ofac_response(hit=False),
        "applicant_id": "APL-1", "application_id": "APP-1",
    })
    assert event.document_type == "OFAC_REPORT"
    assert event.extracted_fields["ofac_clear"] is True


def test_ofac_hit_extracted():
    event = VendorOFACAdapter().process({
        "vendor": "ofac",
        "response": vendor_synthetic.generate_ofac_response(hit=True),
        "applicant_id": "APL-1", "application_id": "APP-1",
    })
    assert event.extracted_fields["ofac_clear"] is False
    assert event.extracted_fields["hit_count"] == 1


# ---------------------------------------------------------------------------
# End-to-end: POST /ingest/vendor-return → context.vendor_checks
# ---------------------------------------------------------------------------


@pytest.fixture
def app_with_seeded_state(
    aggregation_service, postgres_store, redis_store, xref_store
):
    """A FastAPI app whose state is wired to the in-memory test fixtures."""
    from api.main import app as fastapi_app
    fastapi_app.state.postgres_store     = postgres_store
    fastapi_app.state.redis_store        = redis_store
    fastapi_app.state.xref_store         = xref_store
    fastapi_app.state.aggregation_service = aggregation_service
    return fastapi_app


@pytest.mark.asyncio
async def test_vendor_return_endpoint_updates_context(
    app_with_seeded_state, postgres_store, redis_store
):
    """POSTing a DU finding lands a doc, invalidates context, and the
    next /context call surfaces aus_findings.approved=True."""
    # Seed an application with a property + income/credit so aus_ready can
    # actually flip true once the AUS finding lands.
    await postgres_store.save_golden_record({
        "applicant_id": "APL-VR", "full_name": "Vendor Return",
        "first_name": "Vendor", "last_name": "Return",
        "dob": "1990-01-01", "ssn_hash": "vr-h", "ssn_last4": "9999",
        "status": "active", "identity_xrefs": [], "application_ids": ["APP-VR"],
    })
    await postgres_store.save_application({
        "application_id": "APP-VR",
        "applicant_id":   "APL-VR",
        "co_applicant_id": None,
        "los_id":         "LOS-VR",
        "status":         "active",
    })
    await postgres_store.save_property({
        "property_id":    "PROP-VR",
        "application_id": "APP-VR",
        "address_line1":  "1 Main St", "city": "SF", "state": "CA",
        "zip_code": "94105", "property_type": "single_family", "units": 1,
    })
    await postgres_store.update_application_property("APP-VR", "PROP-VR")
    await postgres_store.update_application_loan_data(
        "APP-VR", {"loan_amount": 320_000}
    )
    await postgres_store.save_income_profile({
        "applicant_id":  "APL-VR",
        "application_id": "APP-VR",
        "assembled_at":  "2026-05-06T00:00:00",
        "primary_borrower": {
            "borrower_id": "APL-VR", "role": "primary",
            "qualifying_monthly": 10_000, "overall_confidence": 0.95,
            "sources": [],
        },
        "co_borrower": None,
        "combined_qualifying_monthly": 10_000,
        "qualifying_score_used": 720,
        "monthly_debt_obligations": [], "total_monthly_obligations": 0.0,
        "dti_inputs_ready": True, "requires_human_review": False,
        "lineage_hash": "h",
    })
    await postgres_store.save_credit_profile({
        "applicant_id": "APL-VR", "mid_score": 740, "credit_band": "prime",
        "total_monthly_obligations": 0.0,
    })
    await postgres_store.save_property_profile({
        "property_id":    "PROP-VR",
        "application_id": "APP-VR",
        "appraised_value": 400_000,
        "appraisal_confidence": 0.97,
        "annual_taxes":  6_000,
        "monthly_taxes": 500,
        "hoi_monthly":   150,
        "flood_zone":    "X",
        "flood_insurance_required": False,
        "hoa_monthly":   0,
        "condition_rating": "C3",
        "piti_components": {
            "principal_interest": 1850, "taxes_monthly": 500,
            "insurance_monthly": 150, "hoa_monthly": 0, "flood_monthly": 0,
            "total_piti": 2500,
        },
        "lineage_hash": "h", "assembled_at": "2026-05-06T00:00:00",
    })

    client = TestClient(app_with_seeded_state)
    headers = {"X-API-Key": "test_key"}

    # Submit a DU "Approve/Eligible" finding via the universal endpoint
    du_xml = vendor_synthetic.generate_du_response(
        credit_score=740, dti=38, ltv=80
    )
    resp = client.post(
        "/ingest/vendor-return",
        headers=headers,
        json={
            "vendor_type":    "aus",
            "vendor":         "du",
            "response":       {"xml_content": du_xml},
            "application_id": "APP-VR",
            "applicant_id":   "APL-VR",
        },
    )
    assert resp.status_code == 200, resp.text
    assert resp.json()["document_type"] == "AUS_DU_FINDINGS"

    # Now fetch context and verify vendor_checks + readiness
    ctx_resp = client.get(
        "/application/APP-VR/context", headers=headers
    )
    assert ctx_resp.status_code == 200, ctx_resp.text
    data = ctx_resp.json()["data"]
    aus = (data["vendor_checks"] or {}).get("aus_findings")
    assert aus is not None
    assert aus["approved"] is True
    assert data["readiness"]["aus_ready"] is True
