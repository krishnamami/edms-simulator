"""IncomeAssembler integration tests."""
from core.income.assembler import IncomeAssembler


def test_w2_only_combined_from_primary():
    docs = [
        {
            "document_id": "D1",
            "document_type": "W2",
            "borrower_role": "primary",
            "box1_wages": 96000,
            "employer_name": "Acme",
        },
        {
            "document_id": "D2",
            "document_type": "PAYSTUB",
            "borrower_role": "primary",
        },
    ]
    credit = {
        "mid_score": 720,
        "monthly_obligations": [
            {"type": "car", "monthly_payment": 400, "creditor": "Auto"},
        ],
    }
    profile = IncomeAssembler().assemble(
        primary_docs=docs,
        co_borrower_docs=None,
        primary_credit=credit,
        co_borrower_credit=None,
        application_id="APP-1",
        applicant_id="APL-1",
    )
    assert profile.combined_qualifying_monthly == 8000.0
    assert profile.qualifying_score_used == 720
    assert profile.total_monthly_obligations == 400.0
    assert profile.dti_inputs_ready


def test_co_borrower_income_combined():
    p_docs = [
        {
            "document_id": "P1",
            "document_type": "W2",
            "borrower_role": "primary",
            "box1_wages": 60000,
        },
    ]
    c_docs = [
        {
            "document_id": "C1",
            "document_type": "W2",
            "borrower_role": "co_borrower",
            "box1_wages": 48000,
        },
    ]
    profile = IncomeAssembler().assemble(
        primary_docs=p_docs,
        co_borrower_docs=c_docs,
        primary_credit={"mid_score": 720, "monthly_obligations": []},
        co_borrower_credit={"mid_score": 700, "monthly_obligations": []},
        application_id="APP-2",
        applicant_id="APL-PRI",
        co_applicant_id="APL-CO",
    )
    assert profile.combined_qualifying_monthly == 9000.0  # (60k+48k)/12
    assert profile.qualifying_score_used == 700  # min of two


def test_lineage_hash_changes_with_doc_set():
    base_docs = [
        {"document_id": "D1", "document_type": "W2", "borrower_role": "primary",
         "box1_wages": 60000},
    ]
    asm = IncomeAssembler()
    p1 = asm.assemble(
        primary_docs=base_docs,
        co_borrower_docs=None,
        primary_credit={"mid_score": 720, "monthly_obligations": []},
        co_borrower_credit=None,
        application_id="APP",
        applicant_id="APL-1",
    )
    p2 = asm.assemble(
        primary_docs=base_docs + [
            {"document_id": "D2", "document_type": "PAYSTUB",
             "borrower_role": "primary"}
        ],
        co_borrower_docs=None,
        primary_credit={"mid_score": 720, "monthly_obligations": []},
        co_borrower_credit=None,
        application_id="APP",
        applicant_id="APL-1",
    )
    assert p1.lineage_hash != p2.lineage_hash
