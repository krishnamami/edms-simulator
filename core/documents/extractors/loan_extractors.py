"""Structured-text extractors for loan-terms and employment documents.

Three extractors covering the doc types that drive the loan-terms slice
and the offer-letter income-source path:

  - URLA / 1003 (the canonical mortgage application form)
  - Rate lock agreement
  - Offer letter (new-job income source per Fannie Mae B3-3.1-09)

Same contract as the income / asset extractors:
  ``extract_<type>(pdf_bytes: bytes) -> (fields: dict, confidence: float)``
On failure return ``({}, 0.5)``; on success return the populated dict
plus ``base_conf × fraction_populated``.
"""
from __future__ import annotations

import re

from core.documents.extractors._utils import (
    find_int, find_labeled, find_money, fraction_populated,
    money_to_float, safe_text,
)


# ---------------------------------------------------------------------------
# 13. URLA 1003
# ---------------------------------------------------------------------------
#
# The 1003 is highly standardized across LOSes — Fannie/Freddie's redesign
# (effective 2021-03-01) defines the field labels and section numbering.
# We pull the headline fields the rest of the simulator's slices need
# (loan_amount, property, occupancy, borrower identity, stated income).

_URLA_BASE_CONF = 0.95
_URLA_EXPECTED = [
    "loan_purpose", "loan_amount", "interest_rate", "loan_term_months",
    "property_address", "property_type", "occupancy", "num_units",
    "borrower_name", "borrower_ssn_last4", "borrower_dob",
    "co_borrower_name", "monthly_income_stated", "monthly_expenses_stated",
]


def extract_urla_1003(pdf_bytes: bytes) -> tuple[dict, float]:
    text = safe_text(pdf_bytes)
    if text is None:
        return {}, 0.5

    out: dict = {"document_type": "URLA_1003"}

    if (purpose := find_labeled(text, "Loan Purpose")):
        out["loan_purpose"] = purpose.lower()
    elif (purpose := find_labeled(text, "Purpose of Loan")):
        out["loan_purpose"] = purpose.lower()

    if (addr := find_labeled(text, "Property Address")):
        out["property_address"] = addr
    elif (addr := find_labeled(text, "Subject Property")):
        out["property_address"] = addr

    if (ptype := find_labeled(text, "Property Type")):
        out["property_type"] = ptype
    if (occ := find_labeled(text, "Occupancy")):
        out["occupancy"] = occ.lower()

    if (units := find_int(text, "Number of Units")) is not None:
        out["num_units"] = units
    elif (units := find_int(text, "Units")) is not None:
        out["num_units"] = units

    if (borr := find_labeled(text, "Borrower Name")):
        out["borrower_name"] = borr
    elif (borr := find_labeled(text, "Borrower")):
        out["borrower_name"] = borr

    if (co := find_labeled(text, "Co-Borrower Name")):
        out["co_borrower_name"] = co
    elif (co := find_labeled(text, "Co-Borrower")):
        out["co_borrower_name"] = co

    # SSN last 4 — accept "***-**-1234" or "Last 4 SSN: 1234".
    if m := re.search(r"\*+-?\*+-?(\d{4})\b", text):
        out["borrower_ssn_last4"] = m.group(1)
    elif (raw := find_labeled(text, "Last 4 SSN")):
        if m := re.search(r"\d{4}", raw):
            out["borrower_ssn_last4"] = m.group(0)

    if (dob := find_labeled(text, "Date of Birth")):
        out["borrower_dob"] = dob
    elif (dob := find_labeled(text, "DOB")):
        out["borrower_dob"] = dob

    money_fields = [
        ("loan_amount",              ["Loan Amount", "Mortgage Amount"]),
        ("monthly_income_stated",    ["Monthly Income", "Total Monthly Income"]),
        ("monthly_expenses_stated",  ["Monthly Expenses", "Total Monthly Expenses"]),
    ]
    for field, labels in money_fields:
        for label in labels:
            if (val := find_money(text, label)) is not None:
                out[field] = val
                break

    if (raw := find_labeled(text, "Interest Rate")):
        # "Interest Rate: 6.5%" → 6.5
        m = re.search(r"-?\d+(?:\.\d+)?", raw)
        if m:
            try:
                out["interest_rate"] = float(m.group(0))
            except ValueError:
                pass

    if (term := find_int(text, "Loan Term")) is not None:
        out["loan_term_months"] = term
    elif (term := find_int(text, "Term in Months")) is not None:
        out["loan_term_months"] = term
    elif (raw := find_labeled(text, "Term")):
        # "30 years" → 360
        if m := re.search(r"(\d+)\s*years?", raw, re.IGNORECASE):
            out["loan_term_months"] = int(m.group(1)) * 12

    return out, round(_URLA_BASE_CONF * fraction_populated(out, _URLA_EXPECTED), 3)


# ---------------------------------------------------------------------------
# 14. Rate lock agreement
# ---------------------------------------------------------------------------

_RATE_LOCK_BASE_CONF = 0.93
_RATE_LOCK_EXPECTED = [
    "locked_rate", "lock_expiry", "lock_days", "points",
    "loan_amount", "loan_program", "arm_index", "arm_margin",
]


def extract_rate_lock(pdf_bytes: bytes) -> tuple[dict, float]:
    text = safe_text(pdf_bytes)
    if text is None:
        return {}, 0.5

    out: dict = {"document_type": "RATE_LOCK"}

    if (program := find_labeled(text, "Loan Program")):
        out["loan_program"] = program

    if (expiry := find_labeled(text, "Lock Expiry")):
        out["lock_expiry"] = expiry
    elif (expiry := find_labeled(text, "Expiration Date")):
        out["lock_expiry"] = expiry
    elif (expiry := find_labeled(text, "Lock Expiration")):
        out["lock_expiry"] = expiry

    if (days := find_int(text, "Lock Period")) is not None:
        out["lock_days"] = days
    elif (days := find_int(text, "Lock Days")) is not None:
        out["lock_days"] = days
    elif (days := find_int(text, "Lock Term")) is not None:
        out["lock_days"] = days

    if (raw := find_labeled(text, "Locked Rate")):
        m = re.search(r"-?\d+(?:\.\d+)?", raw)
        if m:
            try:
                out["locked_rate"] = float(m.group(0))
            except ValueError:
                pass
    elif (raw := find_labeled(text, "Note Rate")):
        m = re.search(r"-?\d+(?:\.\d+)?", raw)
        if m:
            try:
                out["locked_rate"] = float(m.group(0))
            except ValueError:
                pass

    if (raw := find_labeled(text, "Points")):
        m = re.search(r"-?\d+(?:\.\d+)?", raw)
        if m:
            try:
                out["points"] = float(m.group(0))
            except ValueError:
                pass

    if (val := find_money(text, "Loan Amount")) is not None:
        out["loan_amount"] = val

    if (idx := find_labeled(text, "ARM Index")):
        out["arm_index"] = idx
    elif (idx := find_labeled(text, "Index")):
        out["arm_index"] = idx
    if (raw := find_labeled(text, "ARM Margin")):
        if (val := money_to_float(raw)) is not None:
            out["arm_margin"] = val
    elif (raw := find_labeled(text, "Margin")):
        if (val := money_to_float(raw)) is not None:
            out["arm_margin"] = val

    return out, round(
        _RATE_LOCK_BASE_CONF * fraction_populated(out, _RATE_LOCK_EXPECTED), 3
    )


# ---------------------------------------------------------------------------
# 15. Offer letter
# ---------------------------------------------------------------------------
#
# Offer letters are highly variable — every employer's HR template is
# different. We pull the labeled fields when they're present and accept
# a base ceiling of 0.82 to reflect that.

_OFFER_BASE_CONF = 0.82
_OFFER_EXPECTED = [
    "employer_name", "position_title", "start_date", "base_salary",
    "bonus_target", "signing_bonus", "employment_type", "pay_frequency",
]


def extract_offer_letter(pdf_bytes: bytes) -> tuple[dict, float]:
    text = safe_text(pdf_bytes)
    if text is None:
        return {}, 0.5

    out: dict = {"document_type": "OFFER_LETTER"}

    # Employer name = either an explicit label or the first non-empty
    # line of the letter (most company letterheads start that way).
    if (emp := find_labeled(text, "Employer")):
        out["employer_name"] = emp
    elif (emp := find_labeled(text, "Company")):
        out["employer_name"] = emp
    else:
        for line in text.splitlines():
            stripped = line.strip()
            if stripped:
                out["employer_name"] = stripped
                break

    if (title := find_labeled(text, "Position Title")):
        out["position_title"] = title
    elif (title := find_labeled(text, "Title")):
        out["position_title"] = title
    elif (title := find_labeled(text, "Position")):
        out["position_title"] = title

    if (date := find_labeled(text, "Start Date")):
        out["start_date"] = date
    elif (date := find_labeled(text, "Effective Date")):
        out["start_date"] = date

    # Employment type — look for full_time / part_time / contract markers
    # in the body. Default None when ambiguous.
    lower = text.lower()
    if "full-time" in lower or "full time" in lower or "full_time" in lower:
        out["employment_type"] = "full_time"
    elif "part-time" in lower or "part time" in lower or "part_time" in lower:
        out["employment_type"] = "part_time"
    elif "contract" in lower or "contractor" in lower or "1099" in lower:
        out["employment_type"] = "contract"

    if (freq := find_labeled(text, "Pay Frequency")):
        out["pay_frequency"] = freq.lower()
    elif (freq := find_labeled(text, "Pay Period")):
        out["pay_frequency"] = freq.lower()
    elif "biweekly" in lower or "bi-weekly" in lower:
        out["pay_frequency"] = "biweekly"
    elif "monthly" in lower:
        out["pay_frequency"] = "monthly"
    elif "weekly" in lower:
        out["pay_frequency"] = "weekly"

    money_fields = [
        ("base_salary",     ["Base Salary", "Annual Salary", "Base Compensation"]),
        ("bonus_target",    ["Bonus Target", "Target Bonus", "Annual Bonus"]),
        ("signing_bonus",   ["Signing Bonus", "Sign-on Bonus", "Sign On Bonus"]),
    ]
    for field, labels in money_fields:
        for label in labels:
            if (val := find_money(text, label)) is not None:
                out[field] = val
                break

    return out, round(
        _OFFER_BASE_CONF * fraction_populated(out, _OFFER_EXPECTED), 3
    )
