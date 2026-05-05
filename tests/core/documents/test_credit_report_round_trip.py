"""Round-trip: credit report PDF + pymupdf extractor."""
from core.documents.extractors.pymupdf_extractor import extract_credit_report
from core.documents.generators.credit_report_generator import generate_credit_report


_SAMPLE_PROFILE = {
    "applicant_id": "APL-00001-P",
    "experian_score": 752,
    "equifax_score": 748,
    "transunion_score": 750,
    "mid_score": 750,
    "credit_band": "prime",
    "open_tradelines": 8,
    "revolving_utilization": 0.22,
    "monthly_obligations": [
        {"type": "car",         "creditor": "Auto Finance",  "monthly_payment": 425},
        {"type": "credit_card", "creditor": "Chase",         "monthly_payment": 120},
        {"type": "student",     "creditor": "Student Loans", "monthly_payment": 280},
    ],
    "total_monthly_obligations": 825.00,
    "derogatory_marks": 0,
    "active_bankruptcy": False,
    "foreclosure_last_36mo": False,
    "late_30day": 0,
    "late_60day": 0,
    "late_90day": 0,
    "hard_inquiries_12mo": 2,
    "report_date": "2024-04-01",
}


def test_credit_report_round_trip():
    pdf_bytes, meta = generate_credit_report(
        applicant_name="James Okafor",
        profile=_SAMPLE_PROFILE,
    )
    assert pdf_bytes.startswith(b"%PDF")

    fields, confidence = extract_credit_report(pdf_bytes)

    assert confidence >= 0.85, f"low confidence: {confidence}, fields={fields}"
    assert fields["experian_score"] == 752
    assert fields["equifax_score"] == 748
    assert fields["transunion_score"] == 750
    assert fields["mid_score"] == 750
    assert fields["credit_band"] == "prime"
    assert fields["total_monthly_obligations"] == 825.00
    assert fields["hard_inquiries_12mo"] == 2
