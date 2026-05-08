"""PDF adapter tests — detection + extraction round-trip."""
from datetime import date

from core.documents.generators.bank_stmt_generator import generate_bank_statement
from core.documents.generators.credit_report_generator import generate_credit_report
from core.documents.generators.paystub_generator import generate_paystub
from core.documents.generators.w2_generator import generate_w2
from core.ingestion.adapters import pdf_adapter
from core.ingestion.events import ChannelType


def test_detects_w2_and_extracts_box1():
    pdf, _ = generate_w2(
        employee_name="James Okafor",
        employee_ssn_last4="4729",
        employee_address="X",
        employer_name="Accenture LLC",
        employer_ein="123456789",
        employer_address="X",
        tax_year=2024,
        box1_wages=92400,
    )
    event = pdf_adapter.adapt(pdf)
    assert event.source_channel == ChannelType.PDF_UPLOAD
    assert event.document_type == "W2"
    assert event.confidence >= 0.85
    assert event.extracted_fields["box1_wages"] == 92400.00
    assert event.requires_verification is False


def test_detects_paystub():
    pdf, _ = generate_paystub(
        employer_name="Accenture LLC", employee_name="James Okafor",
        employee_ssn_last4="4729",
        pay_period_start=date(2024, 2, 1), pay_period_end=date(2024, 2, 14),
        pay_date=date(2024, 2, 17),
        gross_pay=3553.85, ytd_gross=14215.40,
    )
    event = pdf_adapter.adapt(pdf)
    assert event.document_type == "PAYSTUB"
    assert event.confidence >= 0.85


def test_detects_bank_statement():
    pdf, _ = generate_bank_statement(
        bank_name="Pacific First Bank",
        account_holder="James Okafor",
        account_number="1234567890",
        statement_end_date=date(2024, 3, 31),
        seed=11,
    )
    event = pdf_adapter.adapt(pdf)
    assert event.document_type == "BANK_STATEMENT"


def test_detects_credit_report():
    profile = {
        "applicant_id": "APL-X",
        "experian_score": 752, "equifax_score": 748, "transunion_score": 750,
        "mid_score": 750, "credit_band": "prime",
        "open_tradelines": 5, "revolving_utilization": 0.15,
        "monthly_obligations": [{"type": "car", "creditor": "Auto", "monthly_payment": 425}],
        "total_monthly_obligations": 425.0,
        "derogatory_marks": 0, "active_bankruptcy": False,
        "foreclosure_last_36mo": False,
        "late_30day": 0, "late_60day": 0, "late_90day": 0,
        "hard_inquiries_12mo": 1, "report_date": "2024-04-01",
    }
    pdf, _ = generate_credit_report(applicant_name="James Okafor", profile=profile)
    event = pdf_adapter.adapt(pdf)
    assert event.document_type == "CREDIT_REPORT"


def test_low_confidence_on_unrecognized_pdf(monkeypatch):
    # Tiny PDF unrelated to mortgage docs — neither pymupdf nor claude
    # can extract; expect requires_verification=True and notes captured.
    # Disable AI extraction so this test runs deterministically without
    # requiring a real ANTHROPIC_API_KEY.
    monkeypatch.setenv("ENABLE_AI_EXTRACTION", "false")
    minimal_pdf = b"%PDF-1.4\n%\xc7\xec\x8f\xa2\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n2 0 obj<</Type/Pages/Count 0/Kids[]>>endobj\nxref\n0 3\n0000000000 65535 f\n0000000017 00000 n\n0000000060 00000 n\ntrailer<</Size 3/Root 1 0 R>>\nstartxref\n104\n%%EOF\n"
    event = pdf_adapter.adapt(minimal_pdf)
    assert event.requires_verification is True
    # Tier-3 made claude_extractor.extract() always-graceful (returns
    # ({}, 0.5) instead of raising), so the note is now
    # ``claude_fallback_empty`` rather than ``claude_fallback_unavailable``.
    notes = event.extracted_fields.get("_notes", [])
    assert any("claude_fallback_empty" in n for n in notes), notes
