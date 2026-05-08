"""Structured-text extractors for asset-side mortgage documents.

Three extractors covering the doc types that drive the asset / reserves
calculation:

  - Retirement account statement (401k / IRA / 403b / pension)
  - Brokerage account statement (individual / joint / trust)
  - Gift letter (down-payment gift attestation)

Same contract as the income extractors:
  ``extract_<type>(pdf_bytes: bytes) -> (fields: dict, confidence: float)``
On failure return ``({}, 0.5)``; on success return the populated dict
plus ``base_conf × fraction_populated``.
"""
from __future__ import annotations

import re

from core.documents.extractors._utils import (
    find_int, find_labeled, find_money, fraction_populated, safe_text,
)


# ---------------------------------------------------------------------------
# 7. Retirement account statement
# ---------------------------------------------------------------------------

_RETIREMENT_BASE_CONF = 0.92
_RETIREMENT_EXPECTED = [
    "account_type", "balance", "vested_balance", "institution",
    "account_number_last4", "statement_date", "employer_match",
    "loan_balance",
]

# Lower-case detection — accept "401(k)", "401k", "IRA", "Roth IRA",
# "403(b)", "Pension Plan", etc.
_RETIREMENT_TYPE_PATTERNS = [
    (re.compile(r"\b401\s*\(?k\)?", re.IGNORECASE),    "401k"),
    (re.compile(r"\b403\s*\(?b\)?", re.IGNORECASE),    "403b"),
    (re.compile(r"\bRoth\s*IRA",   re.IGNORECASE),     "Roth IRA"),
    (re.compile(r"\bIRA\b",         re.IGNORECASE),    "IRA"),
    (re.compile(r"\bPension",       re.IGNORECASE),    "Pension"),
]


def extract_retirement_account(pdf_bytes: bytes) -> tuple[dict, float]:
    text = safe_text(pdf_bytes)
    if text is None:
        return {}, 0.5

    out: dict = {"document_type": "RETIREMENT_ACCOUNT"}

    # Account type — first matching pattern wins (more-specific Roth IRA
    # / 403b checked before the bare "IRA" / "401k" patterns).
    for pat, label in _RETIREMENT_TYPE_PATTERNS:
        if pat.search(text):
            out["account_type"] = label
            break

    if (inst := find_labeled(text, "Institution")):
        out["institution"] = inst
    elif (inst := find_labeled(text, "Plan Provider")):
        out["institution"] = inst

    # Account number last 4 — accept "Account: ****1234" or
    # "Account Number: XXXX-1234" or "ending in 1234".
    if m := re.search(
        r"(?:Account[^\n]*?)(?:\*+|X+|x+|ending in\s*)(\d{4})\b", text
    ):
        out["account_number_last4"] = m.group(1)

    if (date := find_labeled(text, "Statement Date")):
        out["statement_date"] = date
    elif (date := find_labeled(text, "As of")):
        out["statement_date"] = date

    money_fields = [
        ("balance",          ["Total Balance", "Account Balance", "Balance"]),
        ("vested_balance",   ["Vested Balance", "Vested Amount"]),
        ("employer_match",   ["Employer Match", "Employer Contribution"]),
        ("loan_balance",     ["Loan Balance", "Outstanding Loan"]),
    ]
    for field, labels in money_fields:
        for label in labels:
            if (val := find_money(text, label)) is not None:
                out[field] = val
                break

    return out, round(
        _RETIREMENT_BASE_CONF * fraction_populated(out, _RETIREMENT_EXPECTED), 3
    )


# ---------------------------------------------------------------------------
# 8. Brokerage account statement
# ---------------------------------------------------------------------------

_BROKERAGE_BASE_CONF = 0.92
_BROKERAGE_EXPECTED = [
    "total_value", "liquid_value", "margin_balance", "institution",
    "account_type", "statement_date", "unrealized_gains",
]


def extract_brokerage_account(pdf_bytes: bytes) -> tuple[dict, float]:
    text = safe_text(pdf_bytes)
    if text is None:
        return {}, 0.5

    out: dict = {"document_type": "BROKERAGE_ACCOUNT"}

    if (inst := find_labeled(text, "Institution")):
        out["institution"] = inst
    elif (inst := find_labeled(text, "Brokerage")):
        out["institution"] = inst

    # Account type — labels usually include "Individual", "Joint",
    # "Trust", "Corporate".
    upper = text.upper()
    for label in ("INDIVIDUAL", "JOINT", "TRUST", "CORPORATE"):
        if label in upper:
            out["account_type"] = label.lower()
            break

    if (date := find_labeled(text, "Statement Date")):
        out["statement_date"] = date
    elif (date := find_labeled(text, "As of")):
        out["statement_date"] = date

    money_fields = [
        ("total_value",       ["Total Account Value", "Total Value", "Net Account Value"]),
        ("liquid_value",      ["Liquid Value", "Cash and Equivalents", "Available Cash"]),
        ("margin_balance",    ["Margin Balance", "Margin Loan"]),
        ("unrealized_gains",  ["Unrealized Gains", "Unrealized Gain", "Unrealized G/L"]),
    ]
    for field, labels in money_fields:
        for label in labels:
            if (val := find_money(text, label)) is not None:
                out[field] = val
                break

    return out, round(
        _BROKERAGE_BASE_CONF * fraction_populated(out, _BROKERAGE_EXPECTED), 3
    )


# ---------------------------------------------------------------------------
# 9. Gift letter
# ---------------------------------------------------------------------------

_GIFT_BASE_CONF = 0.88  # often hand-formatted — keep ceiling honest
_GIFT_EXPECTED = [
    "gift_amount", "donor_name", "donor_relationship", "donor_address",
    "repayment_required", "source_of_funds", "borrower_name",
]


def extract_gift_letter(pdf_bytes: bytes) -> tuple[dict, float]:
    text = safe_text(pdf_bytes)
    if text is None:
        return {}, 0.5

    out: dict = {"document_type": "GIFT_LETTER"}

    if (donor := find_labeled(text, "Donor Name")):
        out["donor_name"] = donor
    elif (donor := find_labeled(text, "Donor")):
        out["donor_name"] = donor

    if (rel := find_labeled(text, "Relationship")):
        out["donor_relationship"] = rel

    if (addr := find_labeled(text, "Donor Address")):
        out["donor_address"] = addr
    elif (addr := find_labeled(text, "Address")):
        out["donor_address"] = addr

    if (borrower := find_labeled(text, "Borrower Name")):
        out["borrower_name"] = borrower
    elif (borrower := find_labeled(text, "Borrower")):
        out["borrower_name"] = borrower

    if (source := find_labeled(text, "Source of Funds")):
        out["source_of_funds"] = source

    # Repayment — look for explicit "no repayment" / "is not required" /
    # "repayment is required" wording. Default None when unclear.
    lower = text.lower()
    if any(k in lower for k in (
        "no repayment", "not required", "is not expected", "is a gift",
    )):
        out["repayment_required"] = False
    elif "repayment is required" in lower:
        out["repayment_required"] = True

    if (val := find_money(text, "Gift Amount")) is not None:
        out["gift_amount"] = val
    elif (val := find_money(text, "Amount")) is not None:
        out["gift_amount"] = val

    return out, round(
        _GIFT_BASE_CONF * fraction_populated(out, _GIFT_EXPECTED), 3
    )
