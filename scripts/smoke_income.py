"""Income rules + assembler smoke test."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.income.assembler import IncomeAssembler
from core.income.rules import (
    calculate_asset_depletion,
    calculate_military,
    calculate_rental,
    calculate_retirement_ssa,
    calculate_w2_salaried,
)


def main():
    print("--- rules ---")

    w2 = calculate_w2_salaried(
        [{"document_id": "D1", "box1_wages": 96000, "employer_name": "X"}],
        [{"document_id": "D2"}],
        "APL-1",
    )
    assert w2.qualifying_monthly == 8000.0, w2
    print(f"[PASS] w2: ${w2.qualifying_monthly}/mo at conf {w2.confidence}")

    rental = calculate_rental(
        {"document_id": "S", "gross_rent_annual": 36000, "expenses_annual": 12000},
        [],
        "APL-1",
    )
    assert rental.qualifying_monthly == 1250.0, rental
    print(f"[PASS] rental: ${rental.qualifying_monthly}/mo")

    ssa = calculate_retirement_ssa(
        {"document_id": "L", "monthly_benefit": 2000, "is_non_taxable": True},
        "APL-1",
    )
    assert ssa.qualifying_monthly == 2500.0, ssa
    print(f"[PASS] ssa grossed up: ${ssa.qualifying_monthly}/mo")

    asset = calculate_asset_depletion(
        [{"document_id": "S1", "account_type": "brokerage", "balance": 360000}],
        age=65,
        borrower_id="APL-1",
    )
    assert asset.qualifying_monthly == 700.00, asset
    print(f"[PASS] asset depletion: ${asset.qualifying_monthly}/mo")

    mil = calculate_military(
        {
            "document_id": "L",
            "base_pay_monthly": 4000,
            "bah_monthly": 2000,
            "bas_monthly": 400,
            "special_pay_monthly": 100,
        },
        "APL-1",
    )
    assert mil.qualifying_monthly == 7100.0, mil
    print(f"[PASS] military: ${mil.qualifying_monthly}/mo")

    print("--- assembler ---")
    profile = IncomeAssembler().assemble(
        primary_docs=[
            {
                "document_id": "D1",
                "document_type": "W2",
                "borrower_role": "primary",
                "box1_wages": 96000,
                "employer_name": "Acme",
            },
        ],
        co_borrower_docs=None,
        primary_credit={
            "mid_score": 720,
            "monthly_obligations": [
                {"type": "car", "monthly_payment": 400, "creditor": "Auto"}
            ],
        },
        co_borrower_credit=None,
        application_id="APP-1",
        applicant_id="APL-1",
    )
    assert profile.combined_qualifying_monthly == 8000.0
    assert profile.total_monthly_obligations == 400.0
    assert profile.qualifying_score_used == 720
    print(
        f"[PASS] assembled: combined=${profile.combined_qualifying_monthly}/mo "
        f"score={profile.qualifying_score_used} obligations=${profile.total_monthly_obligations}"
    )
    print("\nAll income smoke checks passed.")


if __name__ == "__main__":
    main()
