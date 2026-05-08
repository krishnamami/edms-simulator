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


# ---------------------------------------------------------------------------
# AVM report — automated valuation model output
# ---------------------------------------------------------------------------
#
# Each AVM (CoreLogic, HouseCanary, Veros, Black Knight) ships a
# slightly different shape but they all surface a single point estimate
# plus a confidence score / FSD / value range. We pull the common
# labels — vendors that use a different label can extend ``_AVM_LABELS``
# rather than fork the function.

_AVM_BASE_CONF  = 0.87
_AVM_EXPECTED = [
    "avm_value", "confidence_score", "model_name", "effective_date",
    "forecast_standard_deviation", "value_range_low", "value_range_high",
    "property_address",
]


def extract_avm_report(pdf_bytes: bytes) -> tuple[dict, float]:
    from core.documents.extractors._utils import (
        find_int, find_labeled, find_money, fraction_populated, safe_text,
    )

    text = safe_text(pdf_bytes)
    if text is None:
        return {}, 0.5

    out: dict = {"document_type": "AVM_REPORT"}

    if (model := find_labeled(text, "Model")):
        out["model_name"] = model
    elif (model := find_labeled(text, "AVM Provider")):
        out["model_name"] = model

    if (date := find_labeled(text, "Effective Date")):
        out["effective_date"] = date
    elif (date := find_labeled(text, "Valuation Date")):
        out["effective_date"] = date

    if (addr := find_labeled(text, "Property Address")):
        out["property_address"] = addr
    elif (addr := find_labeled(text, "Subject Property")):
        out["property_address"] = addr

    if (conf := find_int(text, "Confidence Score")) is not None:
        out["confidence_score"] = conf
    elif (conf := find_int(text, "AVM Confidence")) is not None:
        out["confidence_score"] = conf

    money_fields = [
        ("avm_value",                    ["AVM Value", "Estimated Value", "Point Value"]),
        ("forecast_standard_deviation",  ["FSD", "Forecast Standard Deviation"]),
        ("value_range_low",              ["Value Range Low", "Low Value"]),
        ("value_range_high",             ["Value Range High", "High Value"]),
    ]
    for field, labels in money_fields:
        for label in labels:
            if (val := find_money(text, label)) is not None:
                out[field] = val
                break

    return out, round(_AVM_BASE_CONF * fraction_populated(out, _AVM_EXPECTED), 3)


# ---------------------------------------------------------------------------
# Form 1004MC — Market Conditions Addendum
# ---------------------------------------------------------------------------
#
# Mandatory addendum to URAR (and most appraisal forms) summarizing the
# subject's market: trend, median price changes, days on market,
# foreclosure share. The form is mostly tabular checkboxes; the most
# valuable text fields are the trend label and the median values.

_1004MC_BASE_CONF = 0.85
_1004MC_EXPECTED = [
    "market_trend", "median_sale_price", "median_sale_price_prior",
    "months_supply", "dom_average", "seller_concession_trend",
    "foreclosure_pct",
]

_TREND_LABELS = ("increasing", "stable", "declining")


def extract_1004mc(pdf_bytes: bytes) -> tuple[dict, float]:
    from core.documents.extractors._utils import (
        find_labeled, find_money, fraction_populated, money_to_float, safe_text,
    )

    text = safe_text(pdf_bytes)
    if text is None:
        return {}, 0.5

    out: dict = {"document_type": "FORM_1004MC"}

    lower = text.lower()
    for label in _TREND_LABELS:
        # Look for the trend label near "Property Values" or "Trend" so
        # we don't pick up "increasing" elsewhere in the report.
        if re.search(rf"(?:Trend|Property Values)[^\n]*\b{label}\b", lower):
            out["market_trend"] = label
            break
    if "market_trend" not in out:
        # Loose fallback — any of the trend keywords in the page text.
        for label in _TREND_LABELS:
            if label in lower:
                out["market_trend"] = label
                break

    if (concession := find_labeled(text, "Seller Concession Trend")):
        out["seller_concession_trend"] = concession.lower()

    money_fields = [
        ("median_sale_price",        ["Median Sale Price", "Current Median Sale Price"]),
        ("median_sale_price_prior",  ["Prior Median Sale Price", "Prior Period Median"]),
    ]
    for field, labels in money_fields:
        for label in labels:
            if (val := find_money(text, label)) is not None:
                out[field] = val
                break

    # Months supply + DOM are usually small integers / decimals.
    if (raw := find_labeled(text, "Months Supply")):
        if (val := money_to_float(raw)) is not None:
            out["months_supply"] = val
    if (raw := find_labeled(text, "Average DOM")):
        if (val := money_to_float(raw)) is not None:
            out["dom_average"] = val
    elif (raw := find_labeled(text, "Days on Market")):
        if (val := money_to_float(raw)) is not None:
            out["dom_average"] = val

    if (raw := find_labeled(text, "Foreclosure")):
        # Strip the trailing "%" if present, parse number.
        if m := re.search(r"-?\d+(?:\.\d+)?", raw):
            try:
                out["foreclosure_pct"] = float(m.group(0))
            except ValueError:
                pass

    return out, round(
        _1004MC_BASE_CONF * fraction_populated(out, _1004MC_EXPECTED), 3
    )


# ---------------------------------------------------------------------------
# Purchase agreement / sales contract
# ---------------------------------------------------------------------------
#
# Format varies wildly by jurisdiction (TREC for Texas, CAR for
# California, ALTA elsewhere) but the must-have fields are consistent.
# Anywhere a price / date / party shows up under a labeled section we
# pick it up; anything we miss falls through to "no fields found".

_PURCHASE_BASE_CONF = 0.85
_PURCHASE_EXPECTED = [
    "purchase_price", "earnest_money", "closing_date", "seller_name",
    "buyer_name", "property_address", "seller_concessions",
    "financing_contingency_date", "inspection_contingency_date",
]


def extract_purchase_agreement(pdf_bytes: bytes) -> tuple[dict, float]:
    from core.documents.extractors._utils import (
        find_labeled, find_money, fraction_populated, safe_text,
    )

    text = safe_text(pdf_bytes)
    if text is None:
        return {}, 0.5

    out: dict = {"document_type": "PURCHASE_AGREEMENT"}

    if (seller := find_labeled(text, "Seller Name")):
        out["seller_name"] = seller
    elif (seller := find_labeled(text, "Seller")):
        out["seller_name"] = seller

    if (buyer := find_labeled(text, "Buyer Name")):
        out["buyer_name"] = buyer
    elif (buyer := find_labeled(text, "Buyer")):
        out["buyer_name"] = buyer

    if (addr := find_labeled(text, "Property Address")):
        out["property_address"] = addr
    elif (addr := find_labeled(text, "Subject Property")):
        out["property_address"] = addr

    if (date := find_labeled(text, "Closing Date")):
        out["closing_date"] = date
    elif (date := find_labeled(text, "Settlement Date")):
        out["closing_date"] = date

    if (date := find_labeled(text, "Financing Contingency")):
        out["financing_contingency_date"] = date
    if (date := find_labeled(text, "Inspection Contingency")):
        out["inspection_contingency_date"] = date

    money_fields = [
        ("purchase_price",      ["Purchase Price", "Sale Price", "Total Sales Price"]),
        ("earnest_money",       ["Earnest Money", "Earnest Money Deposit"]),
        ("seller_concessions",  ["Seller Concessions", "Seller Contribution"]),
    ]
    for field, labels in money_fields:
        for label in labels:
            if (val := find_money(text, label)) is not None:
                out[field] = val
                break

    return out, round(
        _PURCHASE_BASE_CONF * fraction_populated(out, _PURCHASE_EXPECTED), 3
    )
