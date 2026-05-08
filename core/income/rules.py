"""GSE-style income calculations.

One function per income type. Functions never raise — instead they return an
IncomeSource with excluded=True and exclusion_reason populated. Each function
emits a human-readable calculation_method string for audit.
"""
from typing import Optional

from core.income.sources import IncomeSource, IncomeSourceType


def _f(value) -> float:
    """Best-effort numeric coercion that NEVER raises.

    Tolerates None, bool, currency strings (``"$92,400.00"``,
    ``"92,400"``), and unparseable values (``"one hundred ten thousand"``)
    — returns 0 for anything that isn't a parseable number. The API
    boundary now accepts any JSON value for extracted_fields (per the
    chaos-test fix), so the assemblers are the place to defend against
    unparseable strings instead of the API dropping them with a 422.
    """
    if value is None or isinstance(value, bool):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    try:
        cleaned = (
            str(value)
            .replace("$", "")
            .replace(",", "")
            .replace("(", "-")
            .replace(")", "")
            .strip()
        )
        if not cleaned:
            return 0.0
        return float(cleaned)
    except (ValueError, TypeError):
        return 0.0


def calculate_w2_salaried(
    w2_docs: list, paystubs: list, borrower_id: str
) -> IncomeSource:
    warnings: list[str] = []
    if not w2_docs:
        return IncomeSource(
            source_type=IncomeSourceType.W2_SALARIED,
            borrower_id=borrower_id,
            borrower_role="primary",
            gross_monthly=0,
            qualifying_monthly=0,
            confidence=0,
            continuance_months=0,
            calculation_method="No W2 found",
            excluded=True,
            exclusion_reason="Missing W2 documents",
        )
    if not paystubs:
        warnings.append("No pay stub - using W2 annualized only")
    w2 = w2_docs[0]
    annual = _f(w2.get("box1_wages"))
    monthly = round(annual / 12, 2)
    conf = 0.97 if paystubs else 0.90
    doc_ids = [d.get("document_id", "") for d in w2_docs + paystubs]
    return IncomeSource(
        source_type=IncomeSourceType.W2_SALARIED,
        borrower_id=borrower_id,
        borrower_role=w2.get("borrower_role", "primary"),
        employer_or_source_name=w2.get("employer_name"),
        gross_monthly=monthly,
        qualifying_monthly=monthly,
        confidence=conf,
        continuance_months=120,
        calculation_method=f"W2 box1 ${annual:,.0f} / 12 = ${monthly:,.2f}/mo",
        documents_used=doc_ids,
        warnings=warnings,
    )


def calculate_self_employed(
    tax_returns: list, borrower_id: str
) -> IncomeSource:
    if len(tax_returns) < 2:
        return IncomeSource(
            source_type=IncomeSourceType.SELF_EMPLOYED,
            borrower_id=borrower_id,
            borrower_role="primary",
            gross_monthly=0,
            qualifying_monthly=0,
            confidence=0,
            continuance_months=0,
            calculation_method="2 years required",
            excluded=True,
            exclusion_reason="Only 1 year available",
        )
    warnings: list[str] = []
    yr1 = _f(tax_returns[0].get("net_income_after_addbacks"))
    yr2 = _f(tax_returns[1].get("net_income_after_addbacks"))
    if yr2 < yr1 * 0.80:
        warnings.append(
            f"Business declining: yr2 ${yr2:,.0f} < 80% of yr1 ${yr1:,.0f}"
        )
    avg = (yr1 + yr2) / 2
    monthly = round(avg / 12, 2)
    has_cpa = any(
        d.get("document_type") == "cpa_letter" for d in tax_returns
    )
    conf = 0.80 if has_cpa else 0.70
    doc_ids = [d.get("document_id", "") for d in tax_returns]
    return IncomeSource(
        source_type=IncomeSourceType.SELF_EMPLOYED,
        borrower_id=borrower_id,
        borrower_role="primary",
        gross_monthly=monthly,
        qualifying_monthly=monthly,
        confidence=conf,
        continuance_months=60,
        calculation_method=(
            f"(yr1 ${yr1:,.0f} + yr2 ${yr2:,.0f}) / 2 / 12 = "
            f"${monthly:,.2f}/mo"
        ),
        documents_used=doc_ids,
        warnings=warnings,
    )


def calculate_rental(
    schedule_e: dict, leases: list, borrower_id: str
) -> IncomeSource:
    if not schedule_e:
        return IncomeSource(
            source_type=IncomeSourceType.RENTAL,
            borrower_id=borrower_id,
            borrower_role="primary",
            gross_monthly=0,
            qualifying_monthly=0,
            confidence=0,
            continuance_months=0,
            calculation_method="No Schedule E",
            excluded=True,
            exclusion_reason="Missing Schedule E",
        )
    # ``or 0`` rather than ``.get(..., 0)`` because the key is sometimes
    # present with value ``None`` (e.g. caller supplied an explicit None,
    # or a doc was hydrated from PG with a NULL column). The default
    # second arg only fires when the key is missing.
    gross = _f(schedule_e.get("gross_rent_annual")) / 12
    expenses = _f(schedule_e.get("expenses_annual")) / 12
    net = round((gross * 0.75) - expenses, 2)
    if net <= 0:
        return IncomeSource(
            source_type=IncomeSourceType.RENTAL,
            borrower_id=borrower_id,
            borrower_role="primary",
            gross_monthly=gross,
            qualifying_monthly=0,
            confidence=0.80,
            continuance_months=60,
            calculation_method=(
                f"75% x ${gross:,.2f} - ${expenses:,.2f} = "
                f"${net:,.2f} (negative - excluded)"
            ),
            excluded=True,
            exclusion_reason="Net rental is negative",
        )
    doc_ids = [schedule_e.get("document_id", "")] + [
        l.get("document_id", "") for l in leases
    ]
    return IncomeSource(
        source_type=IncomeSourceType.RENTAL,
        borrower_id=borrower_id,
        borrower_role="primary",
        gross_monthly=gross,
        qualifying_monthly=net,
        confidence=0.88 if leases else 0.80,
        continuance_months=60,
        calculation_method=(
            f"75% x ${gross:,.2f} - ${expenses:,.2f} expenses = "
            f"${net:,.2f}/mo"
        ),
        documents_used=doc_ids,
    )


def calculate_retirement_ssa(
    ssa_letter: dict, borrower_id: str
) -> IncomeSource:
    if not ssa_letter:
        return IncomeSource(
            source_type=IncomeSourceType.RETIREMENT_SSA,
            borrower_id=borrower_id,
            borrower_role="primary",
            gross_monthly=0,
            qualifying_monthly=0,
            confidence=0,
            continuance_months=0,
            calculation_method="No SSA award letter",
            excluded=True,
            exclusion_reason="Missing SSA award letter",
        )
    benefit = _f(ssa_letter.get("monthly_benefit"))
    non_taxable = ssa_letter.get("is_non_taxable", False)
    qualifying = round(benefit * 1.25 if non_taxable else benefit, 2)
    method = f"SSA ${benefit:,.2f}/mo"
    if non_taxable:
        method += f" x 125% gross-up = ${qualifying:,.2f}/mo"
    return IncomeSource(
        source_type=IncomeSourceType.RETIREMENT_SSA,
        borrower_id=borrower_id,
        borrower_role=ssa_letter.get("borrower_role", "primary"),
        gross_monthly=benefit,
        qualifying_monthly=qualifying,
        confidence=0.99,
        continuance_months=360,
        calculation_method=method,
        documents_used=[ssa_letter.get("document_id", "")],
    )


def calculate_asset_depletion(
    asset_statements: list, age: int, borrower_id: str
) -> IncomeSource:
    if not asset_statements:
        return IncomeSource(
            source_type=IncomeSourceType.ASSET_DEPLETION,
            borrower_id=borrower_id,
            borrower_role="primary",
            gross_monthly=0,
            qualifying_monthly=0,
            confidence=0,
            continuance_months=0,
            calculation_method="No asset statements",
            excluded=True,
            exclusion_reason="Missing asset statements",
        )
    warnings: list[str] = []
    total = 0.0
    for s in asset_statements:
        atype = s.get("account_type", "checking")
        balance = _f(s.get("balance"))
        if atype in ["checking", "savings", "investment", "brokerage"]:
            total += balance
        elif atype in ["retirement", "401k", "ira"]:
            factor = 1.0 if age >= 59.5 else 0.60
            total += balance * factor
            if age < 59.5:
                warnings.append(
                    f"Retirement discounted 60% (age {age} < 59.5)"
                )
        else:
            warnings.append(f"Account type '{atype}' excluded")
    qualifying = round((total * 0.70) / 360, 2)
    doc_ids = [s.get("document_id", "") for s in asset_statements]
    return IncomeSource(
        source_type=IncomeSourceType.ASSET_DEPLETION,
        borrower_id=borrower_id,
        borrower_role="primary",
        gross_monthly=qualifying,
        qualifying_monthly=qualifying,
        confidence=0.92,
        continuance_months=360,
        calculation_method=(
            f"(${total:,.0f} x 70%) / 360 = ${qualifying:,.2f}/mo"
        ),
        documents_used=doc_ids,
        warnings=warnings,
    )


def calculate_military(les_data: dict, borrower_id: str) -> IncomeSource:
    if not les_data:
        return IncomeSource(
            source_type=IncomeSourceType.MILITARY,
            borrower_id=borrower_id,
            borrower_role="primary",
            gross_monthly=0,
            qualifying_monthly=0,
            confidence=0,
            continuance_months=0,
            calculation_method="No LES",
            excluded=True,
            exclusion_reason="Missing Leave and Earnings Statement",
        )
    base = _f(les_data.get("base_pay_monthly"))
    bah = _f(les_data.get("bah_monthly"))
    bas = _f(les_data.get("bas_monthly"))
    special = _f(les_data.get("special_pay_monthly"))
    q = round(base + (bah * 1.25) + (bas * 1.25) + special, 2)
    return IncomeSource(
        source_type=IncomeSourceType.MILITARY,
        borrower_id=borrower_id,
        borrower_role=les_data.get("borrower_role", "primary"),
        employer_or_source_name="US Military",
        gross_monthly=base + bah + bas + special,
        qualifying_monthly=q,
        confidence=0.99,
        continuance_months=60,
        calculation_method=(
            f"Base ${base:,.0f} + BAH ${bah:,.0f}x125% + "
            f"BAS ${bas:,.0f}x125% + Special ${special:,.0f} = "
            f"${q:,.2f}/mo"
        ),
        documents_used=[les_data.get("document_id", "")],
    )
