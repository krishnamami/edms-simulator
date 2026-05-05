"""Image adapter — Claude vision extraction for JPG/PNG/TIFF/WEBP uploads."""
from __future__ import annotations

import base64
import json
import re
from typing import Any, Optional

from core.ingestion._claude_client import (
    CLAUDE_MODEL_ID,
    ClaudeUnavailable,
    get_client,
)
from core.ingestion.events import ChannelType, NormalizedIngestEvent


SYSTEM_PROMPT = (
    "You are a mortgage document extraction specialist. "
    "Detect the document type and extract all fields. "
    "Return JSON only. Never hallucinate values. "
    "If a field is unclear return null."
)

_FENCE_RE = re.compile(r"^```(?:json)?\s*|\s*```$", re.MULTILINE)


def _media_type(image_bytes: bytes) -> str:
    head = bytes(image_bytes[:8])
    if head.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if head.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if head.startswith(b"II*\x00") or head.startswith(b"MM\x00*"):
        return "image/tiff"
    if head[:4] == b"RIFF":
        return "image/webp"
    raise ValueError("Unrecognized image format (expected JPG/PNG/TIFF/WEBP)")


def _parse_json(text: str) -> dict:
    cleaned = _FENCE_RE.sub("", text).strip()
    return json.loads(cleaned)


def adapt(
    image_bytes: bytes,
    *,
    applicant_id: Optional[str] = None,
    borrower_role: str = "primary",
    client: Optional[Any] = None,
) -> NormalizedIngestEvent:
    api = client if client is not None else get_client()
    if api is None:
        raise ClaudeUnavailable(
            "image_adapter requires ANTHROPIC_API_KEY or an injected client"
        )

    media_type = _media_type(image_bytes)
    b64 = base64.b64encode(image_bytes).decode("ascii")

    response = api.messages.create(
        model=CLAUDE_MODEL_ID,
        max_tokens=1500,
        system=SYSTEM_PROMPT,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": media_type,
                            "data": b64,
                        },
                    },
                    {
                        "type": "text",
                        "text": (
                            "Extract all fields from this mortgage document. "
                            "Include a 'document_type' field naming the document "
                            "(e.g. W2, PAYSTUB, DRIVERS_LICENSE) and a "
                            "'confidence' field 0-1."
                        ),
                    },
                ],
            }
        ],
    )

    text_blocks = [b.text for b in response.content if getattr(b, "type", None) == "text"]
    if not text_blocks:
        raise ValueError("Claude returned no text content for image extraction")
    extracted = _parse_json(text_blocks[0])

    confidence = extracted.get("confidence")
    if not isinstance(confidence, (int, float)):
        confidence = 0.85

    document_type = extracted.get("document_type")

    signals = {
        "first_name": extracted.get("first_name") or extracted.get("given_name"),
        "last_name": extracted.get("last_name") or extracted.get("surname"),
        "dob": extracted.get("dob") or extracted.get("date_of_birth"),
        "ssn_last4": extracted.get("ssn_last4"),
        "role": borrower_role,
    }
    if applicant_id:
        signals["applicant_id"] = applicant_id

    return NormalizedIngestEvent(
        source_channel=ChannelType.IMAGE_UPLOAD,
        document_type=document_type,
        applicant_signals=signals,
        extracted_fields=extracted,
        confidence=float(confidence),
        requires_verification=False,
    )
