"""Structured-text extractors for income-side mortgage documents.

Six extractors covering the doc types that drive an underwriter's
income calculation:

  - IRS wage-and-income transcript (4506-C output)
  - Form 1040 (current-year personal return)
  - Schedule C (sole proprietor profit/loss)
  - Schedule E (supplemental income — rentals + pass-through)
  - 1099 (NEC / MISC / INT / DIV)
  - K-1 (partnership / S-corp pass-through)

All follow the same contract:
  ``extract_<type>(pdf_bytes: bytes) -> (fields: dict, confidence: float)``

The extractors target *digitally generated* PDFs — synthetic test
fixtures and structured LOS exports. Real-world OCR is a future phase
(Claude Vision). On any failure (non-PDF bytes, corrupted file, etc.)
the contract returns ``({}, 0.5)`` rather than raising — the indexer's
anti-clobber path treats this as "no fields recovered" and keeps the
caller-supplied extracted_fields if any.

Confidence is the documented base ceiling × fraction of expected
fields populated, so a fully-populated IRS transcript scores 0.99 and
a half-populated one scores ~0.50 — readable by
``ConfidenceResolver``'s SOURCE_CONFIDENCE_RANKING.
"""
from __future__ import annotations

import re

from core.documents.extractors._utils import (
    find_int, find_labeled, find_money, fraction_populated,
    money_to_float, safe_text,
)


# ---------------------------------------------------------------------------
# 1. IRS Transcript
# ---------------------------------------------------------------------------

_IRS_BASE_CONF = 0.99
_IRS_EXPECTED = [
    "agi", "wages_salaries", "tax_year", "filing_status",
    "self_employment_income", "interest_income", "dividend_income",
    "schedule_c_net", "schedule_e_net",
]


def extract_irs_transcript(pdf_bytes: bytes) -> tuple[dict, float]:
    text = safe_text(pdf_bytes)
    if text is None:
        return {}, 0.5

    out: dict = {"document_type": "IRS_TRANSCRIPT"}

    # Tax year — IRS transcripts label as "Tax Period:" or "Tax Year:".
    if (year := find_int(text, "Tax Year")) is not None:
        out["tax_year"] = year
    elif (year := find_int(text, "Tax Period")) is not None:
        out["tax_year"] = year

    # Filing status comes back as one of single / mfj / mfs / hoh / qw.
    if (status := find_labeled(text, "Filing Status")):
        out["filing_status"] = status

    # Money fields. The IRS transcript labels things like
    # "ADJUSTED GROSS INCOME PER COMPUTER" — we accept either the
    # short or the verbose label.
    money_fields = [
        ("agi",                    ["AGI", "Adjusted Gross Income"]),
        ("wages_salaries",         ["Wages, Salaries", "Wages and Salaries", "Wages"]),
        ("self_employment_income", ["Self-Employment Income", "SE Income"]),
        ("interest_income",        ["Interest Income", "Taxable Interest"]),
        ("dividend_income",        ["Dividend Income", "Ordinary Dividends"]),
        ("schedule_c_net",         ["Schedule C Net Profit", "Schedule C Net"]),
        ("schedule_e_net",         ["Schedule E Net Income", "Schedule E Net"]),
    ]
    for field, labels in money_fields:
        for label in labels:
            if (val := find_money(text, label)) is not None:
                out[field] = val
                break

    return out, round(_IRS_BASE_CONF * fraction_populated(out, _IRS_EXPECTED), 3)


# ---------------------------------------------------------------------------
# 2. Form 1040
# ---------------------------------------------------------------------------

_1040_BASE_CONF = 0.90
_1040_EXPECTED = [
    "agi", "total_income", "taxable_income", "tax_year", "filing_status",
    "wages_line1", "schedule_c_income", "schedule_e_income",
    "schedule_f_income", "other_income",
]


def extract_1040(pdf_bytes: bytes) -> tuple[dict, float]:
    text = safe_text(pdf_bytes)
    if text is None:
        return {}, 0.5

    out: dict = {"document_type": "FORM_1040"}

    if (year := find_int(text, "Tax Year")) is not None:
        out["tax_year"] = year
    elif m := re.search(r"Form\s*1040[^\d]*(\d{4})", text):
        out["tax_year"] = int(m.group(1))

    if (status := find_labeled(text, "Filing Status")):
        out["filing_status"] = status

    money_fields = [
        ("wages_line1",        ["Line 1", "Wages, Salaries", "Wages"]),
        ("schedule_c_income",  ["Schedule C", "Business Income"]),
        ("schedule_e_income",  ["Schedule E", "Rental Real Estate"]),
        ("schedule_f_income",  ["Schedule F", "Farm Income"]),
        ("other_income",       ["Other Income", "Line 8"]),
        ("total_income",       ["Total Income", "Line 9"]),
        ("agi",                ["AGI", "Adjusted Gross Income", "Line 11"]),
        ("taxable_income",     ["Taxable Income", "Line 15"]),
    ]
    for field, labels in money_fields:
        for label in labels:
            if (val := find_money(text, label)) is not None:
                out[field] = val
                break

    return out, round(_1040_BASE_CONF * fraction_populated(out, _1040_EXPECTED), 3)


# ---------------------------------------------------------------------------
# 3. Schedule C — Sole proprietor profit/loss
# ---------------------------------------------------------------------------

_SCHED_C_BASE_CONF = 0.90
_SCHED_C_EXPECTED = [
    "gross_receipts", "total_expenses", "net_profit", "business_name",
    "principal_business", "ein", "tax_year",
]


def extract_schedule_c(pdf_bytes: bytes) -> tuple[dict, float]:
    text = safe_text(pdf_bytes)
    if text is None:
        return {}, 0.5

    out: dict = {"document_type": "SCHEDULE_C"}

    if (year := find_int(text, "Tax Year")) is not None:
        out["tax_year"] = year

    if (name := find_labeled(text, "Business Name")):
        out["business_name"] = name
    elif (name := find_labeled(text, "Name of proprietor")):
        out["business_name"] = name

    if (biz := find_labeled(text, "Principal Business")):
        out["principal_business"] = biz
    elif (biz := find_labeled(text, "Principal Profession")):
        out["principal_business"] = biz

    if m := re.search(r"\b(\d{2}-\d{7})\b", text):
        out["ein"] = m.group(1)

    money_fields = [
        ("gross_receipts",  ["Gross Receipts", "Gross Sales", "Line 1"]),
        ("total_expenses",  ["Total Expenses", "Line 28"]),
        ("net_profit",      ["Net Profit", "Net Profit or Loss", "Line 31"]),
    ]
    for field, labels in money_fields:
        for label in labels:
            if (val := find_money(text, label)) is not None:
                out[field] = val
                break

    return out, round(_SCHED_C_BASE_CONF * fraction_populated(out, _SCHED_C_EXPECTED), 3)


# ---------------------------------------------------------------------------
# 4. Schedule E — Supplemental income (rentals)
# ---------------------------------------------------------------------------

_SCHED_E_BASE_CONF = 0.90
_SCHED_E_EXPECTED = [
    "rental_income_gross", "rental_expenses", "net_rental_income",
    "property_address", "property_count", "depreciation", "tax_year",
]


def extract_schedule_e(pdf_bytes: bytes) -> tuple[dict, float]:
    text = safe_text(pdf_bytes)
    if text is None:
        return {}, 0.5

    out: dict = {"document_type": "SCHEDULE_E"}

    if (year := find_int(text, "Tax Year")) is not None:
        out["tax_year"] = year

    if (addr := find_labeled(text, "Property Address")):
        out["property_address"] = addr
    elif (addr := find_labeled(text, "Address of property")):
        out["property_address"] = addr

    if (count := find_int(text, "Property Count")) is not None:
        out["property_count"] = count
    elif (count := find_int(text, "Number of Properties")) is not None:
        out["property_count"] = count

    money_fields = [
        ("rental_income_gross", ["Rental Income", "Gross Rents", "Rents Received"]),
        ("rental_expenses",     ["Rental Expenses", "Total Expenses"]),
        ("net_rental_income",   ["Net Rental", "Net Income", "Net Rental Income"]),
        ("depreciation",        ["Depreciation", "Depreciation Expense"]),
    ]
    for field, labels in money_fields:
        for label in labels:
            if (val := find_money(text, label)) is not None:
                out[field] = val
                break

    return out, round(_SCHED_E_BASE_CONF * fraction_populated(out, _SCHED_E_EXPECTED), 3)


# ---------------------------------------------------------------------------
# 5. Form 1099 (NEC / MISC / INT / DIV)
# ---------------------------------------------------------------------------

_1099_BASE_CONF = 0.93
_1099_EXPECTED = [
    "nonemployee_compensation", "payer_name", "payer_tin",
    "recipient_name", "tax_year", "form_type",
]


def extract_1099(pdf_bytes: bytes) -> tuple[dict, float]:
    text = safe_text(pdf_bytes)
    if text is None:
        return {}, 0.5

    out: dict = {"document_type": "1099"}

    # Detect 1099 variant by the form name in the title.
    upper = text.upper()
    for variant in ("NEC", "MISC", "INT", "DIV"):
        if f"1099-{variant}" in upper or f"FORM 1099 {variant}" in upper:
            out["form_type"] = variant
            break

    if (year := find_int(text, "Tax Year")) is not None:
        out["tax_year"] = year

    if (payer := find_labeled(text, "Payer")):
        out["payer_name"] = payer
    elif (payer := find_labeled(text, "Payer's Name")):
        out["payer_name"] = payer

    if (recip := find_labeled(text, "Recipient")):
        out["recipient_name"] = recip
    elif (recip := find_labeled(text, "Recipient's Name")):
        out["recipient_name"] = recip

    if m := re.search(r"\b(\d{2}-\d{7})\b", text):
        out["payer_tin"] = m.group(1)

    if (val := find_money(text, "Nonemployee Compensation")) is not None:
        out["nonemployee_compensation"] = val
    elif (val := find_money(text, "Box 1")) is not None:
        out["nonemployee_compensation"] = val

    return out, round(_1099_BASE_CONF * fraction_populated(out, _1099_EXPECTED), 3)


# ---------------------------------------------------------------------------
# 6. K-1 — Partnership / S-corp / trust pass-through
# ---------------------------------------------------------------------------

_K1_BASE_CONF = 0.90
_K1_EXPECTED = [
    "ordinary_income", "guaranteed_payments", "rental_income",
    "interest_income", "dividend_income", "partnership_name",
    "partnership_ein", "tax_year",
]


def extract_k1(pdf_bytes: bytes) -> tuple[dict, float]:
    text = safe_text(pdf_bytes)
    if text is None:
        return {}, 0.5

    out: dict = {"document_type": "K1"}

    if (year := find_int(text, "Tax Year")) is not None:
        out["tax_year"] = year

    if (name := find_labeled(text, "Partnership")):
        out["partnership_name"] = name
    elif (name := find_labeled(text, "Partnership's Name")):
        out["partnership_name"] = name

    if m := re.search(r"\b(\d{2}-\d{7})\b", text):
        out["partnership_ein"] = m.group(1)

    money_fields = [
        ("ordinary_income",     ["Ordinary Business Income", "Line 1", "Ordinary Income"]),
        ("guaranteed_payments", ["Guaranteed Payments", "Line 4"]),
        ("rental_income",       ["Net Rental Real Estate", "Line 2"]),
        ("interest_income",     ["Interest Income", "Line 5"]),
        ("dividend_income",     ["Dividend Income", "Ordinary Dividends", "Line 6"]),
    ]
    for field, labels in money_fields:
        for label in labels:
            if (val := find_money(text, label)) is not None:
                out[field] = val
                break

    return out, round(_K1_BASE_CONF * fraction_populated(out, _K1_EXPECTED), 3)
