"""Chat adapter — extracts mortgage data from a conversation transcript.

Sends the conversation to Claude with a structured-JSON system prompt.
All extracted values are flagged requires_verification=True (self-reported).
The client is injectable so tests can supply a mock without an API key.
"""
from __future__ import annotations

import json
import re
from typing import Any, Optional

from core.ingestion._claude_client import (
    CLAUDE_MODEL_ID,
    ClaudeUnavailable,
    get_client,
)
from core.ingestion.events import ChannelType, NormalizedIngestEvent


SYSTEM_PROMPT = """You are a mortgage data extraction specialist.
Extract all financial and personal information from this mortgage
application conversation.

Rules:
- Mark all values requires_verification=true (self-reported)
- Confidence 0.80-0.88 for clearly stated specific values
- Confidence 0.65-0.79 for approximate or vague values
- Extract co-borrower info if mentioned
- List what information is still missing
- List what documents should be requested

Return valid JSON only matching this schema:
{
  "primary_borrower": {
    "first_name": null,
    "last_name": null,
    "email": null,
    "phone": null,
    "dob": null,
    "employer": null,
    "employment_type": null,
    "annual_income_stated": null,
    "income_sources": [
      {"type": null, "amount": null, "frequency": null, "confidence": null}
    ]
  },
  "co_borrower": null,
  "assets_mentioned": [
    {"type": null, "approximate_value": null, "confidence": null}
  ],
  "liabilities_mentioned": [
    {"type": null, "monthly_payment": null, "confidence": null}
  ],
  "property_info": {"address": null, "purchase_price": null, "loan_amount": null},
  "missing_fields": [],
  "documents_needed": [],
  "overall_confidence": null
}
"""


_FENCE_RE = re.compile(r"^```(?:json)?\s*|\s*```$", re.MULTILINE)


def _parse_json(text: str) -> dict:
    cleaned = _FENCE_RE.sub("", text).strip()
    return json.loads(cleaned)


def _signals_from_extracted(extracted: dict) -> dict:
    pb = (extracted.get("primary_borrower") or {}) if isinstance(extracted, dict) else {}
    return {
        "first_name": pb.get("first_name"),
        "last_name": pb.get("last_name"),
        "email": pb.get("email"),
        "phone": pb.get("phone"),
        "dob": pb.get("dob"),
        "employer": pb.get("employer"),
        "annual_income_stated": pb.get("annual_income_stated"),
        "role": "primary",
    }


def adapt(
    messages: list[dict],
    *,
    applicant_id: Optional[str] = None,
    client: Optional[Any] = None,
) -> NormalizedIngestEvent:
    """Extract structured data from a chat transcript via Claude.

    Args:
        messages: list of {"role": "user"|"assistant", "content": str}
        applicant_id: existing applicant context (passed back unchanged)
        client: optional Anthropic client (for tests)

    Returns:
        NormalizedIngestEvent with extracted_fields populated.

    Raises:
        ClaudeUnavailable when no client and no API key.
    """
    api = client if client is not None else get_client()
    if api is None:
        raise ClaudeUnavailable(
            "chat_adapter requires ANTHROPIC_API_KEY or an injected client"
        )

    user_text = "\n".join(
        f"{m.get('role', 'user').upper()}: {m.get('content', '')}"
        for m in messages
    )

    response = api.messages.create(
        model=CLAUDE_MODEL_ID,
        max_tokens=2000,
        system=[
            {
                "type": "text",
                "text": SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[{"role": "user", "content": user_text}],
    )

    text_blocks = [b.text for b in response.content if getattr(b, "type", None) == "text"]
    if not text_blocks:
        raise ValueError("Claude returned no text content for chat extraction")

    extracted = _parse_json(text_blocks[0])
    overall_conf = extracted.get("overall_confidence")
    confidence = float(overall_conf) if isinstance(overall_conf, (int, float)) else 0.80

    signals = _signals_from_extracted(extracted)
    if applicant_id:
        signals["applicant_id"] = applicant_id

    return NormalizedIngestEvent(
        source_channel=ChannelType.CHAT,
        applicant_signals=signals,
        extracted_fields=extracted,
        confidence=confidence,
        requires_verification=True,
        missing_fields=list(extracted.get("missing_fields") or []),
        documents_needed=list(extracted.get("documents_needed") or []),
    )
