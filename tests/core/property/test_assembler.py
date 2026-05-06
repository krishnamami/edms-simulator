"""PropertyAssembler integration tests."""
from core.property.assembler import PropertyAssembler


def _appraisal(value=575_000, condition="C3"):
    return {
        "document_id":   "AP1",
        "document_type": "APPRAISAL_URAR",
        "extracted_fields": {
            "appraised_value":  value,
            "condition_rating": condition,
            "effective_date":   "2025-01-15",
        },
    }


def _hoi(annual=1_800):
    return {
        "document_id":   "HOI1",
        "document_type": "HOI_BINDER",
        "extracted_fields": {
            "annual_premium": annual,
            "carrier_name":   "State Farm",
            "policy_number":  "P-1",
        },
    }


def _tax(annual=7_200, assessed=575_000):
    return {
        "document_id":   "TAX1",
        "document_type": "PROPERTY_TAX_BILL",
        "extracted_fields": {
            "annual_tax":     annual,
            "assessed_value": assessed,
            "tax_year":       2024,
        },
    }


def _flood(zone="X"):
    return {
        "document_id":   "FL1",
        "document_type": "FLOOD_CERT",
        "extracted_fields": {"flood_zone": zone},
    }


def test_appraisal_only_no_piti():
    profile = PropertyAssembler().assemble(
        property_docs=[_appraisal()],
        loan_data={"loan_amount": 460_000, "interest_rate": 7.0,
                   "loan_term_months": 360},
        property_id="PROP-1",
        application_id="APP-1",
    )
    assert profile.appraised_value == 575_000
    assert profile.piti_components is None
    joined = " ".join(profile.assembly_warnings)
    assert "property_tax" in joined
    assert "HOI_binder" in joined


def test_full_assembly_calculates_piti():
    profile = PropertyAssembler().assemble(
        property_docs=[_appraisal(), _hoi(annual=1_800), _tax(annual=6_000)],
        loan_data={"loan_amount": 400_000, "interest_rate": 7.0,
                   "loan_term_months": 360},
        property_id="PROP-2",
        application_id="APP-2",
    )
    assert profile.piti_components is not None
    piti = profile.piti_components
    assert abs(piti.taxes_monthly - 500.0) < 0.01
    assert abs(piti.insurance_monthly - 150.0) < 0.01
    expected_total = round(
        piti.principal_interest + piti.taxes_monthly
        + piti.insurance_monthly + piti.hoa_monthly + piti.flood_monthly,
        2,
    )
    assert abs(piti.total_piti - expected_total) < 0.01


def test_condition_c5_requires_review():
    profile = PropertyAssembler().assemble(
        property_docs=[_appraisal(condition="C5")],
        loan_data={},
        property_id="PROP-3",
        application_id="APP-3",
    )
    assert profile.condition_rating == "C5"
    assert profile.requires_review is True


def test_flood_zone_ae_requires_insurance():
    profile = PropertyAssembler().assemble(
        property_docs=[_appraisal(), _flood(zone="AE")],
        loan_data={},
        property_id="PROP-4",
        application_id="APP-4",
    )
    assert profile.flood_zone == "AE"
    assert profile.flood_insurance_required is True
    assert any("flood" in w.lower() for w in profile.assembly_warnings)


def test_lineage_hash_changes_with_new_doc():
    asm = PropertyAssembler()
    p1 = asm.assemble(
        property_docs=[_appraisal()],
        loan_data={},
        property_id="PROP-5",
        application_id="APP-5",
    )
    p2 = asm.assemble(
        property_docs=[_appraisal(), _hoi()],
        loan_data={},
        property_id="PROP-5",
        application_id="APP-5",
    )
    assert p1.lineage_hash != p2.lineage_hash
