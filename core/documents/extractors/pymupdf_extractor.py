"""PyMuPDF text extractor for clean (digitally-generated) PDFs.

For each document type we know the expected fields. The extractor pulls
text via fitz and applies type-specific regex patterns. Confidence is the
fraction of expected fields successfully extracted.
"""
from __future__ import annotations

import re
from typing import Optional

import fitz


def _open(pdf_bytes: bytes):
    return fitz.open(stream=pdf_bytes, filetype="pdf")


def _all_text(pdf_bytes: bytes) -> str:
    doc = _open(pdf_bytes)
    try:
        return "\n".join(page.get_text("text") for page in doc)
    finally:
        doc.close()


def _money_to_float(s: str) -> Optional[float]:
    if s is None:
        return None
    cleaned = s.replace("$", "").replace(",", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def _confidence(found: dict, expected: list[str]) -> float:
    if not expected:
        return 0.0
    hit = sum(1 for k in expected if found.get(k) not in (None, "", []))
    return round(hit / len(expected), 3)


# ---------------------------------------------------------------------------
# W-2
# ---------------------------------------------------------------------------

_W2_EXPECTED = [
    "tax_year", "employer_name", "employer_ein", "employee_name",
    "box1_wages", "box2_fed_tax", "box3_ss_wages", "box4_ss_tax",
    "box5_medicare_wages", "box6_medicare_tax",
]


def extract_w2(pdf_bytes: bytes) -> tuple[dict, float]:
    text = _all_text(pdf_bytes)
    out: dict = {"document_type": "W2"}

    if m := re.search(r"Tax Year\s+(\d{4})", text):
        out["tax_year"] = int(m.group(1))

    if m := re.search(r"\b(\d{2}-\d{7})\b", text):
        out["employer_ein"] = m.group(1)

    # Employer name = line right after "Employer's name, address, and ZIP code"
    if m := re.search(
        r"Employer's name[^\n]*\n([^\n]+)", text, re.IGNORECASE
    ):
        out["employer_name"] = m.group(1).strip()

    # Employee name
    if m := re.search(
        r"Employee's first name[^\n]*\n([^\n]+)", text, re.IGNORECASE
    ):
        out["employee_name"] = m.group(1).strip()

    # Boxes — labels are unique, values follow on the next line
    box_patterns = {
        "box1_wages": r"1\s+Wages[^\n]*\n([\d,]+\.\d{2})",
        "box2_fed_tax": r"2\s+Federal income tax withheld\s*\n([\d,]+\.\d{2})",
        "box3_ss_wages": r"3\s+Social security wages\s*\n([\d,]+\.\d{2})",
        "box4_ss_tax": r"4\s+Social security tax withheld\s*\n([\d,]+\.\d{2})",
        "box5_medicare_wages": r"5\s+Medicare wages and tips\s*\n([\d,]+\.\d{2})",
        "box6_medicare_tax": r"6\s+Medicare tax withheld\s*\n([\d,]+\.\d{2})",
    }
    for field, pat in box_patterns.items():
        if m := re.search(pat, text):
            val = _money_to_float(m.group(1))
            if val is not None:
                out[field] = val

    if m := re.search(r"12a[^\n]*\n\s*D\s+([\d,]+\.\d{2})", text):
        if (val := _money_to_float(m.group(1))) is not None:
            out["box12a_code_d_401k"] = val

    return out, _confidence(out, _W2_EXPECTED)


# ---------------------------------------------------------------------------
# Pay stub
# ---------------------------------------------------------------------------

_PAYSTUB_EXPECTED = [
    "employer_name", "employee_name", "pay_period_start", "pay_period_end",
    "pay_date", "gross_pay", "ytd_gross", "net_pay",
]


def extract_paystub(pdf_bytes: bytes) -> tuple[dict, float]:
    text = _all_text(pdf_bytes)
    out: dict = {"document_type": "PAYSTUB"}

    # Employer name = first non-empty line
    for line in text.splitlines():
        if line.strip():
            out["employer_name"] = line.strip()
            break

    if m := re.search(r"Employee:\s*(.+)", text):
        out["employee_name"] = m.group(1).strip()

    if m := re.search(
        r"Pay Period:\s*(\d{4}-\d{2}-\d{2})\s*[–-]\s*(\d{4}-\d{2}-\d{2})", text
    ):
        out["pay_period_start"] = m.group(1)
        out["pay_period_end"] = m.group(2)

    if m := re.search(r"Pay Date:\s*(\d{4}-\d{2}-\d{2})", text):
        out["pay_date"] = m.group(1)

    if m := re.search(r"Gross Pay\s*\$([\d,]+\.\d{2})", text):
        out["gross_pay"] = _money_to_float(m.group(1))

    # YTD column on the same row as Regular Pay / Gross Pay
    if m := re.search(
        r"Gross Pay\s*\$([\d,]+\.\d{2})\s*\$([\d,]+\.\d{2})", text
    ):
        out["ytd_gross"] = _money_to_float(m.group(2))
    elif m := re.search(r"Regular Pay\s*\$([\d,]+\.\d{2})\s*\$([\d,]+\.\d{2})", text):
        out["ytd_gross"] = _money_to_float(m.group(2))

    if m := re.search(r"Net Pay\s*\$([\d,]+\.\d{2})", text):
        out["net_pay"] = _money_to_float(m.group(1))

    return out, _confidence(out, _PAYSTUB_EXPECTED)


# ---------------------------------------------------------------------------
# Bank statement
# ---------------------------------------------------------------------------

_BANK_EXPECTED = [
    "bank_name", "account_holder", "account_number_masked",
    "months_count", "ending_balance",
]


def extract_bank_statement(pdf_bytes: bytes) -> tuple[dict, float]:
    text = _all_text(pdf_bytes)
    out: dict = {"document_type": "BANK_STATEMENT"}

    # Bank name = first non-empty line
    for line in text.splitlines():
        if line.strip():
            out["bank_name"] = line.strip()
            break

    if m := re.search(r"Account:\s*(\*+\d+)", text):
        out["account_number_masked"] = m.group(1)

    if m := re.search(r"Holder:\s*(.+)", text):
        out["account_holder"] = m.group(1).strip()

    statements = re.findall(r"Statement:\s*(\d{4}-\d{2})", text)
    if statements:
        out["months_count"] = len(set(statements))
        out["months"] = sorted(set(statements))

    closing_balances = re.findall(
        r"Closing Balance:\s*-?\$([\d,]+\.\d{2})", text
    )
    if closing_balances:
        out["ending_balance"] = _money_to_float(closing_balances[-1])

    return out, _confidence(out, _BANK_EXPECTED)


# ---------------------------------------------------------------------------
# Credit report
# ---------------------------------------------------------------------------

_CREDIT_EXPECTED = [
    "experian_score", "equifax_score", "transunion_score",
    "mid_score", "credit_band", "total_monthly_obligations",
    "hard_inquiries_12mo",
]


def extract_credit_report(pdf_bytes: bytes) -> tuple[dict, float]:
    text = _all_text(pdf_bytes)
    out: dict = {"document_type": "CREDIT_REPORT"}

    # Three bureau scores — each label is a section, value follows on its own line
    for bureau, key in [
        ("EXPERIAN", "experian_score"),
        ("EQUIFAX", "equifax_score"),
        ("TRANSUNION", "transunion_score"),
    ]:
        if m := re.search(rf"{bureau}\s*\n\s*(\d{{3}})", text):
            out[key] = int(m.group(1))

    if m := re.search(r"Mid Score[^:]*:\s*(\d{3})", text):
        out["mid_score"] = int(m.group(1))

    if m := re.search(r"Band:\s*([\w\-]+)", text):
        out["credit_band"] = m.group(1).strip()

    if m := re.search(
        r"Total Monthly Obligations\s*\$([\d,]+\.\d{2})", text
    ):
        out["total_monthly_obligations"] = _money_to_float(m.group(1))

    if m := re.search(r"Hard inquiries[^:]*:\s*(\d+)", text):
        out["hard_inquiries_12mo"] = int(m.group(1))

    return out, _confidence(out, _CREDIT_EXPECTED)
