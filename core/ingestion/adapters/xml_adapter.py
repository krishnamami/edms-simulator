"""XML adapter — IRS 4506-C transcript + MISMO 3.4 detection.

Real MISMO docs have deep namespace nesting; we navigate by local name to
stay schema-tolerant. IRS transcript data carries confidence=0.99 (highest
truth source per SOURCE_CONFIDENCE_RANKING).
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Optional

from core.ingestion.events import ChannelType, NormalizedIngestEvent


def _local_name(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _findall_local(root: ET.Element, name: str) -> list[ET.Element]:
    return [el for el in root.iter() if _local_name(el.tag).lower() == name.lower()]


def _find_first_text(root: ET.Element, name: str) -> Optional[str]:
    for el in root.iter():
        if _local_name(el.tag).lower() == name.lower() and el.text:
            return el.text.strip()
    return None


def _detect_format(root: ET.Element) -> str:
    tag = _local_name(root.tag).lower()
    # MISMO: root commonly <MESSAGE xmlns="http://www.mismo.org/...">
    if "mismo" in tag or tag == "message":
        ns = (root.tag.split("}", 1)[0] if "}" in root.tag else "").lower()
        if "mismo" in ns or tag == "message":
            return "MISMO"
    if "transcript" in tag or tag in ("taxtranscript", "irstranscript"):
        return "IRS_4506C"
    # Heuristic: presence of <AGI> or <FilingStatus>
    if _find_first_text(root, "AGI") or _find_first_text(root, "FilingStatus"):
        return "IRS_4506C"
    if _find_first_text(root, "BaseLoanAmount") or _find_first_text(root, "LoanIdentifier"):
        return "MISMO"
    return "UNKNOWN"


def _to_float(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None
    try:
        return float(str(s).replace(",", "").replace("$", ""))
    except (TypeError, ValueError):
        return None


def _adapt_irs(root: ET.Element) -> NormalizedIngestEvent:
    name = _find_first_text(root, "Name")
    first_name, last_name = None, None
    if name:
        parts = name.strip().split()
        if len(parts) >= 2:
            first_name, last_name = parts[0], parts[-1]
        else:
            first_name = name

    extracted = {
        "tax_year": _find_first_text(root, "TaxYear"),
        "filing_status": _find_first_text(root, "FilingStatus"),
        "agi": _to_float(_find_first_text(root, "AGI")),
        "wages": _to_float(_find_first_text(root, "Wages")),
        "taxpayer_name": name,
        "ssn_last4": _find_first_text(root, "SSNLast4"),
    }
    extracted = {k: v for k, v in extracted.items() if v is not None}

    return NormalizedIngestEvent(
        source_channel=ChannelType.XML,
        document_type="IRS_4506C",
        applicant_signals={
            "first_name": first_name,
            "last_name": last_name,
            "ssn_last4": extracted.get("ssn_last4"),
            "role": "primary",
        },
        extracted_fields=extracted,
        confidence=0.99,
        requires_verification=False,
    )


def _adapt_mismo(root: ET.Element) -> NormalizedIngestEvent:
    first_name = _find_first_text(root, "FirstName")
    last_name = _find_first_text(root, "LastName")
    extracted = {
        "first_name": first_name,
        "last_name": last_name,
        "loan_amount": _to_float(_find_first_text(root, "BaseLoanAmount")),
        "loan_identifier": _find_first_text(root, "LoanIdentifier"),
        "purpose": _find_first_text(root, "LoanPurposeType"),
        "property_address": _find_first_text(root, "AddressLineText"),
    }
    extracted = {k: v for k, v in extracted.items() if v is not None}

    return NormalizedIngestEvent(
        source_channel=ChannelType.XML,
        document_type="MISMO",
        applicant_signals={
            "first_name": first_name,
            "last_name": last_name,
            "role": "primary",
        },
        extracted_fields=extracted,
        confidence=0.95,
        requires_verification=False,
    )


def adapt(xml_bytes: bytes) -> NormalizedIngestEvent:
    text = xml_bytes.decode("utf-8", errors="replace")
    try:
        root = ET.fromstring(text)
    except ET.ParseError as exc:
        raise ValueError(f"Invalid XML: {exc}") from exc

    fmt = _detect_format(root)
    if fmt == "IRS_4506C":
        return _adapt_irs(root)
    if fmt == "MISMO":
        return _adapt_mismo(root)

    return NormalizedIngestEvent(
        source_channel=ChannelType.XML,
        document_type="UNKNOWN_XML",
        extracted_fields={"root_tag": _local_name(root.tag)},
        confidence=0.20,
        requires_verification=True,
    )
