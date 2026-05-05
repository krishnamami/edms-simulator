"""AggregationService end-to-end tests (with in-memory stores)."""
import pytest

from core.aggregation.events import (
    ApplicationSubmittedEvent,
    DocumentUploadedEvent,
    EventType,
)
from core.identity.golden_record import GoldenRecord


def _new_loan_payload(los_id: str = "LOS-001", ssn: str = "123-45-6789") -> dict:
    return {
        "los_id": los_id,
        "borrower": {
            "first_name": "John",
            "last_name": "Doe",
            "dob": "1980-01-15",
            "ssn_hash": GoldenRecord.hash_ssn(ssn),
            "ssn_last4": ssn[-4:],
            "email": "john@example.com",
        },
        "loan": {"credit_band": "near-prime"},
        "documents": [
            {
                "document_id": "DOC-001",
                "document_type": "W2",
                "document_category": "income",
                "borrower_role": "primary",
                "box1_wages": 95000,
                "employer_name": "Acme",
            },
            {
                "document_id": "DOC-002",
                "document_type": "PAYSTUB",
                "document_category": "income",
                "borrower_role": "primary",
            },
        ],
    }


@pytest.mark.asyncio
async def test_application_submitted_creates_new_golden_record(aggregation_service):
    payload = _new_loan_payload()
    event = ApplicationSubmittedEvent(
        event_type=EventType.APPLICATION_SUBMITTED, payload=payload
    )
    result = await aggregation_service.handle(event)

    assert result["status"] == "active"
    assert result["match_method"] == "new_record"
    assert result["is_new_record"] is True
    assert result["applicant_id"].startswith("APL-")
    assert result["application_id"] == "APP-LOS-001"


@pytest.mark.asyncio
async def test_same_ssn_resolves_to_same_applicant(aggregation_service):
    p1 = _new_loan_payload("LOS-100", ssn="111-22-3333")
    p2 = _new_loan_payload("LOS-101", ssn="111-22-3333")

    r1 = await aggregation_service.handle(
        ApplicationSubmittedEvent(
            event_type=EventType.APPLICATION_SUBMITTED, payload=p1
        )
    )
    r2 = await aggregation_service.handle(
        ApplicationSubmittedEvent(
            event_type=EventType.APPLICATION_SUBMITTED, payload=p2
        )
    )

    assert r2["applicant_id"] == r1["applicant_id"]
    assert r2["match_method"] == "deterministic"
    assert r2["is_new_record"] is False


@pytest.mark.asyncio
async def test_income_profile_persisted_after_submit(
    aggregation_service, postgres_store, redis_store
):
    payload = _new_loan_payload("LOS-200", ssn="222-33-4444")
    result = await aggregation_service.handle(
        ApplicationSubmittedEvent(
            event_type=EventType.APPLICATION_SUBMITTED, payload=payload
        )
    )
    aid = result["applicant_id"]
    pg_profile = await postgres_store.get_income_profile(aid)
    assert pg_profile is not None
    assert pg_profile["combined_qualifying_monthly"] > 0
    cached = redis_store.get_income_profile(aid)
    assert cached is not None
    assert cached["applicant_id"] == aid


@pytest.mark.asyncio
async def test_document_upload_re_assembles_and_versions(
    aggregation_service, postgres_store
):
    submit = _new_loan_payload("LOS-300", ssn="333-44-5555")
    submit_result = await aggregation_service.handle(
        ApplicationSubmittedEvent(
            event_type=EventType.APPLICATION_SUBMITTED, payload=submit
        )
    )
    aid = submit_result["applicant_id"]
    v1 = (await postgres_store.get_income_profile(aid))["_version"]

    extra_docs = list(submit["documents"]) + [
        {
            "document_id": "DOC-NEW",
            "document_type": "W2",
            "document_category": "income",
            "borrower_role": "primary",
            "box1_wages": 110000,
            "employer_name": "NewCo",
        }
    ]
    upload_event = DocumentUploadedEvent(
        event_type=EventType.DOCUMENT_UPLOADED,
        payload={
            "applicant_id": aid,
            "application_id": submit_result["application_id"],
            "all_documents": extra_docs,
        },
    )
    await aggregation_service.handle(upload_event)
    v2 = (await postgres_store.get_income_profile(aid))["_version"]
    assert v2 == v1 + 1


@pytest.mark.asyncio
async def test_published_events_include_golden_record_created(
    aggregation_service,
):
    payload = _new_loan_payload("LOS-400", ssn="444-55-6666")
    await aggregation_service.handle(
        ApplicationSubmittedEvent(
            event_type=EventType.APPLICATION_SUBMITTED, payload=payload
        )
    )
    types = {e["event_type"] for e in aggregation_service.get_published_events()}
    assert "golden_record_created" in types
