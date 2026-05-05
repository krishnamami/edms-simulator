"""PDF adapter — detects document type, runs pymupdf, falls back to claude.

Detection is text-based against signature phrases. Once classified, the
matching pymupdf_extractor function runs. Below confidence 0.70, we attempt
the claude_extractor fallback (raises if not available, in which case the
low-confidence pymupdf result is still returned with requires_verification=True).
"""
from __future__ import annotations

from typing import Optional

import fitz

from core.documents.extractors import claude_extractor, pymupdf_extractor
from core.ingestion.events import ChannelType, NormalizedIngestEvent


CONFIDENCE_FLOOR = 0.70


_DETECTION_PHRASES: list[tuple[str, list[str]]] = [
    ("W2",            ["Wage and Tax Statement"]),
    ("PAYSTUB",       ["YTD", "Pay Period"]),
    ("BANK_STATEMENT", ["Account Summary", "Balance"]),
    ("CREDIT_REPORT", ["EXPERIAN", "EQUIFAX", "TRANSUNION"]),
    ("TAX_RETURN",    ["Form 1040"]),
    ("APPRAISAL",     ["Uniform Residential Appraisal"]),
    ("TITLE",         ["Title Commitment", "ALTA"]),
]


def _all_text(pdf_bytes: bytes) -> str:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        return "\n".join(page.get_text("text") for page in doc)
    finally:
        doc.close()


def detect_document_type(pdf_bytes: bytes) -> Optional[str]:
    text = _all_text(pdf_bytes).upper()
    for doc_type, phrases in _DETECTION_PHRASES:
        if any(p.upper() in text for p in phrases):
            return doc_type
    return None


_EXTRACTORS = {
    "W2":             pymupdf_extractor.extract_w2,
    "PAYSTUB":        pymupdf_extractor.extract_paystub,
    "BANK_STATEMENT": pymupdf_extractor.extract_bank_statement,
    "CREDIT_REPORT":  pymupdf_extractor.extract_credit_report,
}


def _signals_from_fields(fields: dict, role: str) -> dict:
    name = fields.get("employee_name") or fields.get("account_holder") or fields.get("applicant_name")
    first_name, last_name = None, None
    if name:
        parts = name.strip().split()
        if len(parts) >= 2:
            first_name, last_name = parts[0], parts[-1]
        else:
            first_name = name
    return {
        "first_name": first_name,
        "last_name": last_name,
        "role": role,
    }


def adapt(
    pdf_bytes: bytes,
    *,
    applicant_id: Optional[str] = None,
    borrower_role: str = "primary",
) -> NormalizedIngestEvent:
    doc_type = detect_document_type(pdf_bytes)

    fields: dict = {}
    confidence = 0.0
    notes: list[str] = []

    extractor = _EXTRACTORS.get(doc_type)
    if extractor is not None:
        fields, confidence = extractor(pdf_bytes)

    if confidence < CONFIDENCE_FLOOR:
        try:
            claude_fields, claude_conf = claude_extractor.extract(pdf_bytes, hint=doc_type)
            if claude_conf > confidence:
                fields = {**fields, **claude_fields}
                confidence = claude_conf
                notes.append("claude_fallback")
        except (
            claude_extractor.ClaudeExtractorUnavailable,
            NotImplementedError,
        ) as exc:
            notes.append(f"claude_fallback_unavailable:{type(exc).__name__}")

    requires_verification = (confidence < CONFIDENCE_FLOOR)

    signals = _signals_from_fields(fields, borrower_role)
    if applicant_id:
        signals["applicant_id"] = applicant_id

    return NormalizedIngestEvent(
        source_channel=ChannelType.PDF_UPLOAD,
        document_type=doc_type,
        applicant_signals=signals,
        extracted_fields={**fields, "_notes": notes} if notes else fields,
        confidence=confidence,
        requires_verification=requires_verification,
    )
