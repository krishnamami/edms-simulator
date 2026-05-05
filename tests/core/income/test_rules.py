"""Income calculation rule tests."""
from core.income.rules import (
    calculate_asset_depletion,
    calculate_military,
    calculate_rental,
    calculate_retirement_ssa,
    calculate_self_employed,
    calculate_w2_salaried,
)


def test_w2_no_docs_excluded():
    s = calculate_w2_salaried([], [], "APL-1")
    assert s.excluded
    assert s.qualifying_monthly == 0


def test_w2_basic_monthly_division():
    w2 = {"document_id": "D1", "box1_wages": 96000, "employer_name": "X"}
    paystub = {"document_id": "D2"}
    s = calculate_w2_salaried([w2], [paystub], "APL-1")
    assert not s.excluded
    assert s.qualifying_monthly == 8000.0
    assert s.confidence >= 0.95


def test_w2_without_paystub_lower_confidence():
    w2 = {"document_id": "D1", "box1_wages": 60000, "employer_name": "X"}
    s = calculate_w2_salaried([w2], [], "APL-1")
    assert not s.excluded
    assert s.confidence == 0.90
    assert "No pay stub" in s.warnings[0]


def test_self_employed_one_year_excluded():
    docs = [{"document_id": "D1", "net_income_after_addbacks": 60000}]
    s = calculate_self_employed(docs, "APL-1")
    assert s.excluded


def test_self_employed_two_years_average():
    docs = [
        {"document_id": "D1", "net_income_after_addbacks": 60000},
        {"document_id": "D2", "net_income_after_addbacks": 80000},
    ]
    s = calculate_self_employed(docs, "APL-1")
    assert not s.excluded
    # (60000 + 80000)/2 = 70000 / 12 = 5833.33
    assert s.qualifying_monthly == 5833.33


def test_self_employed_decline_warning():
    docs = [
        {"document_id": "D1", "net_income_after_addbacks": 100000},
        {"document_id": "D2", "net_income_after_addbacks": 50000},
    ]
    s = calculate_self_employed(docs, "APL-1")
    assert any("declining" in w for w in s.warnings)


def test_rental_75_percent_minus_expenses():
    schedule_e = {
        "document_id": "D-SE",
        "gross_rent_annual": 36000,
        "expenses_annual": 12000,
    }
    s = calculate_rental(schedule_e, [], "APL-1")
    # gross/mo=3000, 75%=2250, exp/mo=1000 -> 1250
    assert s.qualifying_monthly == 1250.0


def test_rental_negative_excluded():
    schedule_e = {
        "document_id": "D-SE",
        "gross_rent_annual": 12000,
        "expenses_annual": 24000,
    }
    s = calculate_rental(schedule_e, [], "APL-1")
    assert s.excluded


def test_ssa_taxable_no_grossup():
    letter = {"document_id": "D1", "monthly_benefit": 2000, "is_non_taxable": False}
    s = calculate_retirement_ssa(letter, "APL-1")
    assert s.qualifying_monthly == 2000.0


def test_ssa_non_taxable_grossed_up():
    letter = {"document_id": "D1", "monthly_benefit": 2000, "is_non_taxable": True}
    s = calculate_retirement_ssa(letter, "APL-1")
    assert s.qualifying_monthly == 2500.0  # 2000 * 1.25


def test_asset_depletion_70_pct_over_360():
    statements = [
        {"document_id": "S1", "account_type": "checking", "balance": 100000},
        {"document_id": "S2", "account_type": "brokerage", "balance": 200000},
    ]
    s = calculate_asset_depletion(statements, age=65, borrower_id="APL-1")
    # (300000 * 0.70) / 360 = 583.33
    assert s.qualifying_monthly == 583.33


def test_asset_depletion_under_age_discounts_retirement():
    statements = [
        {"document_id": "S1", "account_type": "401k", "balance": 100000},
    ]
    s = calculate_asset_depletion(statements, age=40, borrower_id="APL-1")
    # 100000 * 0.6 = 60000 -> *0.7/360 = 116.67
    assert s.qualifying_monthly == 116.67


def test_military_grossed_up_allowances():
    les = {
        "document_id": "D1",
        "base_pay_monthly": 4000,
        "bah_monthly": 2000,
        "bas_monthly": 400,
        "special_pay_monthly": 100,
    }
    s = calculate_military(les, "APL-1")
    # 4000 + 2000*1.25 + 400*1.25 + 100 = 4000+2500+500+100 = 7100
    assert s.qualifying_monthly == 7100.0
