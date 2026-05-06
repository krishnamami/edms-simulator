"""PyMuPDF text extractors for property documents.

Same shape as ``core.documents.extractors.pymupdf_extractor`` — each
``extract_*`` function returns ``(fields_dict, confidence)`` where
confidence is the fraction of expected fields recovered.
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
# Appraisal (URAR)
# ---------------------------------------------------------------------------

_APPRAISAL_EXPECTED = [
    "appraised_value",
    "condition_rating",
    "effective_date",
    "property_address",
]


def extract_appraisal_pdf(pdf_bytes: bytes) -> tuple[dict, float]:
    text = _all_text(pdf_bytes)
    out: dict = {"document_type": "APPRAISAL_URAR"}

    if m := re.search(
        r"(?:Opinion of Value|Appraised Value)[^\n$]*\n?\s*\$([\d,]+(?:\.\d{2})?)",
        text,
        re.IGNORECASE,
    ):
        if (val := _money_to_float(m.group(1))) is not None:
            out["appraised_value"] = val

    if m := re.search(r"Condition\s*\n?\s*(C[1-6])", text, re.IGNORECASE):
        out["condition_rating"] = m.group(1).upper()

    if m := re.search(
        r"Effective Date\s*\n?\s*(\d{4}-\d{2}-\d{2})",
        text,
        re.IGNORECASE,
    ):
        out["effective_date"] = m.group(1)

    if m := re.search(
        r"Property Address\s*\n([^\n]+)", text, re.IGNORECASE
    ):
        out["property_address"] = m.group(1).strip()

    if m := re.search(
        r"Year Built\s*\n?\s*(\d{4})", text, re.IGNORECASE
    ):
        out["year_built"] = int(m.group(1))

    if m := re.search(
        r"(?:Gross Living Area|SqFt)[^\n]*\n?\s*([\d,]+)",
        text,
        re.IGNORECASE,
    ):
        try:
            out["sqft"] = int(m.group(1).replace(",", ""))
        except ValueError:
            pass

    return out, _confidence(out, _APPRAISAL_EXPECTED)


# ---------------------------------------------------------------------------
# Homeowners insurance binder
# ---------------------------------------------------------------------------

_HOI_EXPECTED = [
    "annual_premium",
    "policy_number",
    "carrier_name",
    "effective_date",
]


def extract_hoi_pdf(pdf_bytes: bytes) -> tuple[dict, float]:
    text = _all_text(pdf_bytes)
    out: dict = {"document_type": "HOI_BINDER"}

    if m := re.search(
        r"Annual Premium\s*\n?\s*\$([\d,]+(?:\.\d{2})?)",
        text,
        re.IGNORECASE,
    ):
        if (val := _money_to_float(m.group(1))) is not None:
            out["annual_premium"] = val
    elif m := re.search(
        r"Policy Premium\s*\n?\s*\$([\d,]+(?:\.\d{2})?)",
        text,
        re.IGNORECASE,
    ):
        if (val := _money_to_float(m.group(1))) is not None:
            out["annual_premium"] = val

    if m := re.search(
        r"Policy Number[:\s]+([A-Z0-9-]+)", text, re.IGNORECASE
    ):
        out["policy_number"] = m.group(1).strip()

    if m := re.search(
        r"Carrier Name\s*\n([^\n]+)", text, re.IGNORECASE
    ):
        out["carrier_name"] = m.group(1).strip()

    if m := re.search(
        r"Effective Date[:\s]+(\d{4}-\d{2}-\d{2})", text, re.IGNORECASE
    ):
        out["effective_date"] = m.group(1)

    if m := re.search(
        r"Dwelling[^\n$]*\$([\d,]+(?:\.\d{2})?)", text, re.IGNORECASE
    ):
        if (val := _money_to_float(m.group(1))) is not None:
            out["dwelling_coverage"] = val

    return out, _confidence(out, _HOI_EXPECTED)


# ---------------------------------------------------------------------------
# Flood certificate
# ---------------------------------------------------------------------------

_FLOOD_EXPECTED = [
    "flood_zone",
    "sfha",
    "determination_date",
    "firm_panel",
]


def extract_flood_pdf(pdf_bytes: bytes) -> tuple[dict, float]:
    text = _all_text(pdf_bytes)
    out: dict = {"document_type": "FLOOD_CERT"}

    if m := re.search(
        r"Flood Zone\s*\n?\s*(X500|AE|AO|AH|AR|VE|A99|[ABCDVX])\b",
        text,
    ):
        out["flood_zone"] = m.group(1).upper()

    if m := re.search(
        r"Special Flood Hazard Area\s*\n?\s*(Yes|No)", text, re.IGNORECASE
    ):
        out["sfha"] = m.group(1).strip().lower() == "yes"

    if m := re.search(
        r"Determination Date\s*\n?\s*(\d{4}-\d{2}-\d{2})",
        text,
        re.IGNORECASE,
    ):
        out["determination_date"] = m.group(1)

    if m := re.search(
        r"FIRM Panel Number\s*\n?\s*([A-Z0-9]+)", text, re.IGNORECASE
    ):
        out["firm_panel"] = m.group(1).strip()
    elif m := re.search(
        r"NFIP Map[^\n]*\n([A-Z0-9]+)", text, re.IGNORECASE
    ):
        out["firm_panel"] = m.group(1).strip()

    return out, _confidence(out, _FLOOD_EXPECTED)


# ---------------------------------------------------------------------------
# Property tax bill
# ---------------------------------------------------------------------------

_TAX_EXPECTED = [
    "annual_tax",
    "assessed_value",
    "tax_year",
    "parcel_number",
]


def extract_tax_pdf(pdf_bytes: bytes) -> tuple[dict, float]:
    text = _all_text(pdf_bytes)
    out: dict = {"document_type": "PROPERTY_TAX_BILL"}

    if m := re.search(
        r"Annual Tax\s*\n?\s*\$([\d,]+(?:\.\d{2})?)", text, re.IGNORECASE
    ):
        if (val := _money_to_float(m.group(1))) is not None:
            out["annual_tax"] = val
    elif m := re.search(
        r"Total Tax\s*\n?\s*\$([\d,]+(?:\.\d{2})?)", text, re.IGNORECASE
    ):
        if (val := _money_to_float(m.group(1))) is not None:
            out["annual_tax"] = val

    if m := re.search(
        r"Assessed Value\s*\n?\s*\$([\d,]+(?:\.\d{2})?)",
        text,
        re.IGNORECASE,
    ):
        if (val := _money_to_float(m.group(1))) is not None:
            out["assessed_value"] = val

    if m := re.search(r"Tax Year\s*(\d{4})", text, re.IGNORECASE):
        out["tax_year"] = int(m.group(1))

    if m := re.search(
        r"Parcel Number[:\s]+([\w\-]+)", text, re.IGNORECASE
    ):
        out["parcel_number"] = m.group(1).strip()

    return out, _confidence(out, _TAX_EXPECTED)
