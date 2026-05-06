"""Round-trip tests for property document generators + extractors."""
from core.property.extractors import (
    extract_appraisal_pdf,
    extract_flood_pdf,
    extract_hoi_pdf,
    extract_tax_pdf,
)
from core.property.generators.appraisal_generator import generate_appraisal
from core.property.generators.flood_cert_generator import generate_flood_cert
from core.property.generators.hoi_generator import generate_hoi_binder
from core.property.generators.tax_bill_generator import generate_tax_bill
from core.property.generators.title_generator import generate_title_commitment


def test_appraisal_generates_pdf():
    pdf, meta = generate_appraisal(
        property_address="123 Main St\nSF, CA 94105",
        appraised_value=625_000,
        condition_rating="C2",
    )
    assert pdf.startswith(b"%PDF")
    assert meta["appraised_value"] == 625_000
    assert meta["condition_rating"] == "C2"
    assert meta["document_type"] == "APPRAISAL_URAR"


def test_round_trip_appraisal():
    pdf, meta = generate_appraisal(
        property_address="500 Pine Ave\nOakland, CA 94612",
        appraised_value=720_000,
        condition_rating="C3",
        effective_date="2025-02-10",
        year_built=2010,
        sqft=2200,
    )
    fields, confidence = extract_appraisal_pdf(pdf)
    assert confidence >= 0.5
    assert fields["appraised_value"] == 720_000
    assert fields["condition_rating"] == "C3"
    assert fields["effective_date"] == "2025-02-10"


def test_hoi_generates_pdf():
    pdf, meta = generate_hoi_binder(
        insured_name="James Q. Public",
        property_address="500 Pine Ave\nOakland, CA 94612",
        annual_premium=2_400,
    )
    assert pdf.startswith(b"%PDF")
    assert meta["annual_premium"] == 2_400
    assert meta["monthly_premium"] == 200


def test_round_trip_hoi():
    pdf, meta = generate_hoi_binder(
        insured_name="James Q. Public",
        property_address="500 Pine Ave\nOakland, CA 94612",
        annual_premium=2_400,
        policy_number="HOI-2025-99999",
    )
    fields, _ = extract_hoi_pdf(pdf)
    assert fields["annual_premium"] == 2_400
    assert fields["policy_number"] == "HOI-2025-99999"


def test_flood_cert_generates_pdf():
    pdf, meta = generate_flood_cert(
        property_address="900 River Rd\nMobile, AL 36602",
        flood_zone="AE",
    )
    assert pdf.startswith(b"%PDF")
    assert meta["flood_zone"] == "AE"
    assert meta["sfha"] is True


def test_round_trip_flood_cert():
    pdf, _ = generate_flood_cert(
        property_address="900 River Rd\nMobile, AL 36602",
        flood_zone="AE",
        determination_date="2025-03-01",
        firm_panel="01097C0420H",
    )
    fields, _ = extract_flood_pdf(pdf)
    assert fields["flood_zone"] == "AE"
    assert fields["sfha"] is True
    assert fields["determination_date"] == "2025-03-01"


def test_tax_bill_generates_pdf():
    pdf, meta = generate_tax_bill(
        property_address="123 Main St\nSF, CA 94105",
        owner_name="James Q. Public",
        annual_tax=8_400,
        tax_year=2024,
    )
    assert pdf.startswith(b"%PDF")
    assert meta["annual_tax"] == 8_400
    assert meta["tax_year"] == 2024


def test_round_trip_tax_bill():
    pdf, _ = generate_tax_bill(
        property_address="123 Main St\nSF, CA 94105",
        owner_name="James Q. Public",
        annual_tax=8_400,
        tax_year=2024,
        parcel_number="555-444-333",
    )
    fields, _ = extract_tax_pdf(pdf)
    assert fields["annual_tax"] == 8_400
    assert fields["tax_year"] == 2024
    assert fields["parcel_number"] == "555-444-333"


def test_title_commitment_generates_pdf():
    pdf, meta = generate_title_commitment(
        property_address="123 Main St\nSF, CA 94105",
        estate_amount=575_000,
    )
    assert pdf.startswith(b"%PDF")
    assert meta["estate_amount"] == 575_000
    assert meta["requirements_count"] == 3
    assert meta["exceptions_count"] == 3
