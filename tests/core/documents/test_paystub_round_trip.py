"""Round-trip: generate paystub PDF -> extract via pymupdf -> fields match."""
from datetime import date

from core.documents.extractors.pymupdf_extractor import extract_paystub
from core.documents.generators.paystub_generator import generate_paystub


def test_paystub_round_trip():
    pdf_bytes, meta = generate_paystub(
        employer_name="Accenture LLC",
        employee_name="James Okafor",
        employee_ssn_last4="4729",
        pay_period_start=date(2024, 2, 1),
        pay_period_end=date(2024, 2, 14),
        pay_date=date(2024, 2, 17),
        gross_pay=3553.85,
        ytd_gross=14215.40,
    )
    assert pdf_bytes.startswith(b"%PDF")

    fields, confidence = extract_paystub(pdf_bytes)

    assert confidence >= 0.85, f"low confidence: {confidence}, fields={fields}"
    assert fields["employer_name"] == "Accenture LLC"
    assert fields["employee_name"] == "James Okafor"
    assert fields["pay_period_start"] == "2024-02-01"
    assert fields["pay_period_end"] == "2024-02-14"
    assert fields["pay_date"] == "2024-02-17"
    assert fields["gross_pay"] == 3553.85
    assert fields["ytd_gross"] == 14215.40
    assert abs(fields["net_pay"] - meta["net_pay"]) < 0.01
