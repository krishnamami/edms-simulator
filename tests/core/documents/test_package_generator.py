"""Package generator: produces full doc set, optionally indexed via FakePG."""
import pytest

from core.documents.generators.package_generator import (
    SCENARIO_DOC_SETS,
    generate_package,
)


_PRIMARY = {
    "applicant_id": "APL-00001-P",
    "first_name": "James",
    "last_name": "Okafor",
    "dob": "1982-07-14",
    "ssn_last4": "4729",
    "annual_income": 92400,
    "employer": "Accenture LLC",
    "address": "100 Main St\nSan Francisco, CA 94105",
}

_CO = {
    "applicant_id": "APL-00002-C",
    "first_name": "Sarah",
    "last_name": "Okafor",
    "dob": "1985-03-22",
    "ssn_last4": "8821",
    "annual_income": 56_200,
    "employer": "Dell Technologies",
}

_CREDIT = {
    "applicant_id": "APL-00001-P",
    "experian_score": 752, "equifax_score": 748, "transunion_score": 750,
    "mid_score": 750, "credit_band": "prime",
    "open_tradelines": 8, "revolving_utilization": 0.22,
    "monthly_obligations": [
        {"type": "car", "creditor": "Auto", "monthly_payment": 425},
    ],
    "total_monthly_obligations": 425.00,
    "derogatory_marks": 0, "active_bankruptcy": False,
    "foreclosure_last_36mo": False,
    "late_30day": 0, "late_60day": 0, "late_90day": 0,
    "hard_inquiries_12mo": 2, "report_date": "2024-04-01",
}


@pytest.mark.asyncio
async def test_standard_w2_package_produces_all_documents():
    manifest = await generate_package(
        application_id="APP-LOS-TEST-001",
        primary=_PRIMARY,
        loan_data={"credit_band": "prime"},
        credit_profile=_CREDIT,
        scenario_type="standard_w2",
    )
    types = sorted({d["document_type"] for d in manifest})
    assert types == sorted(SCENARIO_DOC_SETS["standard_w2"])
    for doc in manifest:
        assert doc["size_bytes"] > 0
        assert 0 < doc["confidence"] <= 1


@pytest.mark.asyncio
async def test_package_with_co_borrower_doubles_doc_set():
    manifest = await generate_package(
        application_id="APP-LOS-TEST-002",
        primary=_PRIMARY,
        co_borrower=_CO,
        loan_data={"credit_band": "prime"},
        credit_profile=_CREDIT,
        co_credit_profile=_CREDIT,
        scenario_type="standard_w2",
    )
    roles = {d["borrower_role"] for d in manifest}
    assert roles == {"primary", "co_borrower"}


@pytest.mark.asyncio
async def test_package_indexes_via_postgres_store():
    saved: list[dict] = []

    class FakePG:
        async def save_document(self, doc):
            saved.append(doc)

    manifest = await generate_package(
        application_id="APP-LOS-TEST-003",
        primary=_PRIMARY,
        loan_data={"credit_band": "prime"},
        credit_profile=_CREDIT,
        scenario_type="minimal",
        postgres_store=FakePG(),
    )
    assert len(saved) == len(manifest)
    saved_types = {s["document_type"] for s in saved}
    assert saved_types == set(SCENARIO_DOC_SETS["minimal"])
    for s in saved:
        assert s["status"] == "received"
        assert s["is_current"] is True
        assert s["confidence_score"] > 0
