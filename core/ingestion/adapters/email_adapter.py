"""Email adapter.

Returns a list of NormalizedIngestEvents: one for the email body (Claude
extraction) plus one per attachment (routed through pdf_adapter or
image_adapter). Subject keywords add a document-type hint to the body event.
"""
from __future__ import annotations

import base64
import json
import re
from typing import Any, Optional

from core.ingestion._claude_client import (
    CLAUDE_MODEL_ID,
    get_client,
)
from core.ingestion.adapters import image_adapter, pdf_adapter
from core.ingestion.events import ChannelType, NormalizedIngestEvent


SYSTEM_PROMPT = (
    "Extract any income, employment, or financial information mentioned in "
    "this email. Return JSON only with these keys when present: "
    "annual_income, monthly_income, employer, employment_type, "
    "income_sources, assets_mentioned. Use null when a value is not stated."
)

_SUBJECT_HINTS: list[tuple[str, str]] = [
    ("w2",        "W2"),
    ("w-2",       "W2"),
    ("pay stub",  "PAYSTUB"),
    ("paystub",   "PAYSTUB"),
    ("paycheck",  "PAYSTUB"),
    ("bank statement", "BANK_STATEMENT"),
    ("credit report",  "CREDIT_REPORT"),
    ("tax return",     "TAX_RETURN"),
    ("1040",           "TAX_RETURN"),
    ("driver",         "DRIVERS_LICENSE"),
    ("appraisal",      "APPRAISAL"),
]

_FENCE_RE = re.compile(r"^```(?:json)?\s*|\s*```$", re.MULTILINE)


def _hint_from_subject(subject: str) -> Optional[str]:
    s = (subject or "").lower()
    for needle, doc_type in _SUBJECT_HINTS:
        if needle in s:
            return doc_type
    return None


def _parse_json(text: str) -> dict:
    cleaned = _FENCE_RE.sub("", text).strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        return {}


def _detect_attachment_kind(raw: bytes) -> Optional[str]:
    head = bytes(raw[:8])
    if head.startswith(b"%PDF"):
        return "pdf"
    if head.startswith(b"\xff\xd8\xff"):
        return "image"
    if head.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image"
    if head.startswith(b"II*\x00") or head.startswith(b"MM\x00*"):
        return "image"
    return None


def _body_event(
    subject: str,
    body: str,
    *,
    client: Optional[Any],
    applicant_id: Optional[str],
) -> NormalizedIngestEvent:
    hint = _hint_from_subject(subject)
    api = client if client is not None else get_client()

    if api is None or not body.strip():
        # No Claude available or empty body — emit a low-confidence event
        # carrying just the raw text + subject hint.
        return NormalizedIngestEvent(
            source_channel=ChannelType.EMAIL,
            document_type=hint,
            applicant_signals={"applicant_id": applicant_id} if applicant_id else {},
            extracted_fields={
                "subject": subject,
                "body": body,
                "subject_hint": hint,
            },
            confidence=0.40,
            requires_verification=True,
        )

    response = api.messages.create(
        model=CLAUDE_MODEL_ID,
        max_tokens=1000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": f"Subject: {subject}\n\n{body}"}],
    )
    text_blocks = [b.text for b in response.content if getattr(b, "type", None) == "text"]
    extracted = _parse_json(text_blocks[0]) if text_blocks else {}

    return NormalizedIngestEvent(
        source_channel=ChannelType.EMAIL,
        document_type=hint,
        applicant_signals={"applicant_id": applicant_id} if applicant_id else {},
        extracted_fields={**extracted, "subject": subject, "subject_hint": hint},
        confidence=0.75,
        requires_verification=True,
    )


def adapt(
    payload: dict,
    *,
    applicant_id: Optional[str] = None,
    borrower_role: str = "primary",
    client: Optional[Any] = None,
) -> list[NormalizedIngestEvent]:
    subject = payload.get("subject", "") or ""
    body = payload.get("body", "") or ""
    attachments = payload.get("attachments", []) or []

    events: list[NormalizedIngestEvent] = [
        _body_event(subject, body, client=client, applicant_id=applicant_id)
    ]

    for att in attachments:
        b64 = att.get("content_base64", "")
        if not b64:
            continue
        raw = base64.b64decode(b64)
        kind = _detect_attachment_kind(raw)
        if kind == "pdf":
            events.append(pdf_adapter.adapt(
                raw, applicant_id=applicant_id, borrower_role=borrower_role,
            ))
        elif kind == "image":
            events.append(image_adapter.adapt(
                raw,
                applicant_id=applicant_id,
                borrower_role=borrower_role,
                client=client,
            ))
        # other types ignored (Phase C scope is PDF + image attachments)

    return events
