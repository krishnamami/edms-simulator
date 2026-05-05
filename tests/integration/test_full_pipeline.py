"""End-to-end integration test through the AggregationService.

Uses in-memory FakePostgresStore + fakeredis. Asserts the full path:
submit -> resolve -> assemble -> persist -> cache.
"""
import pytest

from core.aggregation.events import (
    ApplicationSubmittedEvent,
    DocumentUploadedEvent,
    EventType,
)
from core.identity.golden_record import GoldenRecord


def _payload(los_id: str, ssn: str, with_co: bool = False) -> dict:
    docs = [
        {
            "document_id": f"DOC-{los_id}-W2",
            "document_type": "W2",
            "borrower_role": "primary",
            "box1_wages": 84000,
            "employer_name": "Acme",
        },
        {
            "document_id": f"DOC-{los_id}-PAY",
            "document_type": "PAYSTUB",
            "borrower_role": "primary",
        },
    ]
    payload = {
        "los_id": los_id,
        "borrower": {
            "first_name": "Sam",
            "last_name": "River",
            "dob": "1982-07-04",
            "ssn_hash": GoldenRecord.hash_ssn(ssn),
            "ssn_last4": ssn[-4:],
            "email": f"sam-{los_id}@example.com",
        },
        "loan": {"credit_band": "near-prime"},
        "documents": docs,
    }
    if with_co:
        payload["co_borrower"] = {
            "first_name": "Pat",
            "last_name": "River",
            "dob": "1984-02-14",
            "ssn_hash": GoldenRecord.hash_ssn("888-77-6666"),
            "ssn_last4": "6666",
        }
        payload["documents"].append(
            {
                "document_id": f"DOC-{los_id}-CO-W2",
                "document_type": "W2",
                "borrower_role": "co_borrower",
                "box1_wages": 60000,
                "employer_name": "OtherCo",
            }
        )
    return payload


@pytest.mark.asyncio
async def test_full_pipeline_single_borrower(
    aggregation_service, postgres_store, redis_store
):
    payload = _payload("INT-001", "555-66-7777")
    result = await aggregation_service.handle(
        ApplicationSubmittedEvent(
            event_type=EventType.APPLICATION_SUBMITTED, payload=payload
        )
    )
    assert result["status"] == "active"

    aid = result["applicant_id"]
    income = await postgres_store.get_income_profile(aid)
    credit = await postgres_store.get_credit_profile(aid)
    assert income["combined_qualifying_monthly"] == 7000.0
    assert credit["mid_score"] >= 600

    cached_status = redis_store.get_status(aid)
    assert cached_status == "active"
    cached_lookup = redis_store.get_app_lookup("INT-001")
    assert cached_lookup["application_id"] == result["application_id"]


@pytest.mark.asyncio
async def test_full_pipeline_with_coborrower(
    aggregation_service, postgres_store
):
    payload = _payload("INT-002", "777-88-9999", with_co=True)
    result = await aggregation_service.handle(
        ApplicationSubmittedEvent(
            event_type=EventType.APPLICATION_SUBMITTED, payload=payload
        )
    )
    assert result["co_applicant_id"] is not None

    profile = await postgres_store.get_income_profile(result["applicant_id"])
    assert profile["co_borrower"] is not None
    assert profile["combined_qualifying_monthly"] == 12000.0  # 84k+60k /12


@pytest.mark.asyncio
async def test_document_upload_increments_version(
    aggregation_service, postgres_store
):
    payload = _payload("INT-003", "121-21-2121")
    submit_result = await aggregation_service.handle(
        ApplicationSubmittedEvent(
            event_type=EventType.APPLICATION_SUBMITTED, payload=payload
        )
    )
    aid = submit_result["applicant_id"]
    v1 = (await postgres_store.get_income_profile(aid))["_version"]
    h1 = (await postgres_store.get_income_profile(aid))["lineage_hash"]

    upload_event = DocumentUploadedEvent(
        event_type=EventType.DOCUMENT_UPLOADED,
        payload={
            "applicant_id": aid,
            "application_id": submit_result["application_id"],
            "all_documents": payload["documents"]
            + [
                {
                    "document_id": "DOC-EXTRA",
                    "document_type": "W2",
                    "borrower_role": "primary",
                    "box1_wages": 100000,
                    "employer_name": "BigCo",
                }
            ],
        },
    )
    await aggregation_service.handle(upload_event)
    after = await postgres_store.get_income_profile(aid)
    assert after["_version"] == v1 + 1
    assert after["lineage_hash"] != h1
