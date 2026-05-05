"""Round-trip: generate W2 PDF -> extract via pymupdf -> fields match."""
from datetime import date

from core.documents.extractors.pymupdf_extractor import extract_w2
from core.documents.generators.w2_generator import generate_w2


def test_w2_round_trip_recovers_all_boxes():
    pdf_bytes, meta = generate_w2(
        employee_name="James Okafor",
        employee_ssn_last4="4729",
        employee_address="100 Main St\nSan Francisco, CA 94105",
        employer_name="Accenture LLC",
        employer_ein="123456789",
        employer_address="1 Corporate Way\nSan Francisco, CA 94105",
        tax_year=2024,
        box1_wages=92400.00,
    )
    assert pdf_bytes.startswith(b"%PDF")

    fields, confidence = extract_w2(pdf_bytes)

    assert confidence >= 0.85, f"low confidence: {confidence}, fields={fields}"
    assert fields["tax_year"] == 2024
    assert fields["employer_ein"] == "12-3456789"
    assert fields["employer_name"] == "Accenture LLC"
    assert fields["employee_name"] == "James Okafor"
    assert fields["box1_wages"] == 92400.00
    # Computed defaults — verify within tolerance
    assert abs(fields["box2_fed_tax"] - meta["box2_fed_tax"]) < 0.01
    assert abs(fields["box4_ss_tax"] - meta["box4_ss_tax"]) < 0.01
    assert abs(fields["box6_medicare_tax"] - meta["box6_medicare_tax"]) < 0.01


def test_w2_metadata_masks_ssn():
    _, meta = generate_w2(
        employee_name="A B",
        employee_ssn_last4="1234",
        employee_address="X",
        employer_name="X",
        employer_ein="999999999",
        employer_address="X",
        tax_year=2023,
        box1_wages=50_000,
    )
    assert meta["employee_ssn_masked"] == "***-**-1234"
    assert meta["employer_ein"] == "99-9999999"
