"""Per-document property extraction helpers + PITI calculation.

One function per property doc type. Functions never raise — they emit
warnings via the returned dict instead. Same shape contract as
``core.income.rules``.
"""
import logging
from typing import Optional

from core.property.sources import PITIComponents

logger = logging.getLogger(__name__)


def calculate_piti(
    loan_amount: float,
    interest_rate: float,
    loan_term_months: int,
    annual_taxes: float,
    hoi_monthly: float,
    hoa_monthly: float = 0,
    flood_monthly: float = 0,
) -> PITIComponents:
    """Standard mortgage payment math (P&I + escrows).

    ``interest_rate`` is the annual rate as a percent (e.g. 7.0 for 7%).
    """
    monthly_rate = interest_rate / 100 / 12
    if monthly_rate > 0:
        pi = (
            loan_amount
            * (monthly_rate * (1 + monthly_rate) ** loan_term_months)
            / ((1 + monthly_rate) ** loan_term_months - 1)
        )
    else:
        pi = loan_amount / loan_term_months
    taxes_monthly = round(annual_taxes / 12, 2)
    total = round(
        pi + taxes_monthly + hoi_monthly + hoa_monthly + flood_monthly, 2
    )
    return PITIComponents(
        principal_interest=round(pi, 2),
        taxes_monthly=taxes_monthly,
        insurance_monthly=hoi_monthly,
        hoa_monthly=hoa_monthly,
        flood_monthly=flood_monthly,
        total_piti=total,
    )


def _to_float(value) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def extract_appraisal(appraisal_doc: dict) -> dict:
    """Pull URAR fields. Returns dict with appraised_value, condition,
    appraisal_date, appraisal_type, appraisal_confidence and warnings."""
    warnings: list[str] = []
    fields = appraisal_doc.get("extracted_fields") or {}
    appraised_value = _to_float(
        fields.get("appraised_value") or fields.get("opinion_of_value")
    )
    if appraised_value is None:
        warnings.append("No appraised value found in appraisal document")
    condition = fields.get("condition_rating") or fields.get("condition")
    if condition and condition in ("C5", "C6"):
        warnings.append(f"Property condition {condition} — requires review")
    return {
        "appraised_value":      appraised_value,
        "appraisal_date":       fields.get("effective_date") or fields.get("appraisal_date"),
        "appraisal_type":       "APPRAISAL_URAR",
        "appraisal_confidence": 0.97,
        "condition_rating":     condition,
        "warnings":             warnings,
    }


def extract_hoi(hoi_doc: dict) -> dict:
    fields = hoi_doc.get("extracted_fields") or {}
    annual = _to_float(
        fields.get("annual_premium") or fields.get("policy_premium")
    )
    monthly = round(annual / 12, 2) if annual else None
    return {
        "hoi_annual":    annual,
        "hoi_monthly":   monthly,
        "hoi_carrier":   fields.get("carrier_name"),
        "policy_number": fields.get("policy_number"),
    }


def extract_flood(flood_doc: dict) -> dict:
    fields = flood_doc.get("extracted_fields") or {}
    zone = fields.get("flood_zone") or fields.get("zone_designation")
    required = bool(zone) and zone.upper() not in ("X", "X500", "C", "B")
    return {
        "flood_zone":               zone,
        "flood_insurance_required": required,
        "flood_determination_date": fields.get("determination_date"),
    }


def extract_property_tax(tax_doc: dict) -> dict:
    fields = tax_doc.get("extracted_fields") or {}
    annual = _to_float(fields.get("annual_tax") or fields.get("total_tax"))
    return {
        "tax_assessed_value": _to_float(fields.get("assessed_value")),
        "annual_taxes":       annual,
        "monthly_taxes":      round(annual / 12, 2) if annual else None,
        "tax_year":           fields.get("tax_year"),
    }


def extract_hoa(hoa_doc: dict) -> dict:
    fields = hoa_doc.get("extracted_fields") or {}
    monthly = _to_float(fields.get("monthly_dues") or fields.get("hoa_fee"))
    return {
        "hoa_monthly":    monthly if monthly is not None else 0,
        "hoa_name":       fields.get("association_name"),
        "hoa_delinquent": fields.get("is_delinquent", False),
    }
