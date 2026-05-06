"""Tests for core.property.rules — PITI math + flood zone classification."""
from core.property.rules import (
    calculate_piti,
    extract_flood,
    extract_hoi,
    extract_property_tax,
)


def test_calculate_piti_standard():
    piti = calculate_piti(
        loan_amount=400_000,
        interest_rate=7.0,
        loan_term_months=360,
        annual_taxes=6_000,
        hoi_monthly=150,
    )
    # Principal & interest on $400k @ 7% / 360 months ≈ $2,661.21
    assert abs(piti.principal_interest - 2_661.21) < 5.0
    assert piti.taxes_monthly == 500.0
    assert piti.insurance_monthly == 150.0
    assert piti.hoa_monthly == 0
    assert piti.flood_monthly == 0
    assert abs(piti.total_piti - (piti.principal_interest + 500 + 150)) < 0.01


def test_calculate_piti_with_hoa_and_flood():
    piti = calculate_piti(
        loan_amount=300_000,
        interest_rate=6.5,
        loan_term_months=360,
        annual_taxes=4_800,
        hoi_monthly=120,
        hoa_monthly=250,
        flood_monthly=80,
    )
    assert piti.hoa_monthly == 250
    assert piti.flood_monthly == 80
    assert piti.total_piti == round(
        piti.principal_interest + 400 + 120 + 250 + 80, 2
    )


def test_calculate_piti_zero_rate():
    piti = calculate_piti(
        loan_amount=120_000,
        interest_rate=0,
        loan_term_months=360,
        annual_taxes=0,
        hoi_monthly=0,
    )
    assert piti.principal_interest == round(120_000 / 360, 2)


def test_flood_zone_x_not_required():
    out = extract_flood({"extracted_fields": {"flood_zone": "X"}})
    assert out["flood_zone"] == "X"
    assert out["flood_insurance_required"] is False


def test_flood_zone_ae_required():
    out = extract_flood({"extracted_fields": {"flood_zone": "AE"}})
    assert out["flood_zone"] == "AE"
    assert out["flood_insurance_required"] is True


def test_flood_zone_ve_required():
    out = extract_flood({"extracted_fields": {"flood_zone": "VE"}})
    assert out["flood_insurance_required"] is True


def test_flood_zone_b_not_required():
    out = extract_flood({"extracted_fields": {"flood_zone": "B"}})
    assert out["flood_insurance_required"] is False


def test_extract_hoi_monthly_derived():
    out = extract_hoi({"extracted_fields": {"annual_premium": 1_800}})
    assert out["hoi_annual"] == 1_800
    assert out["hoi_monthly"] == 150


def test_extract_property_tax_monthly_derived():
    out = extract_property_tax({"extracted_fields": {
        "annual_tax": 6_000, "assessed_value": 500_000, "tax_year": 2024,
    }})
    assert out["annual_taxes"] == 6_000
    assert out["monthly_taxes"] == 500
    assert out["tax_assessed_value"] == 500_000
