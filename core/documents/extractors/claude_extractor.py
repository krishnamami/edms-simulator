"""Claude vision-based extractor (Phase C implementation).

Phase B places the file with the documented signature so the rest of the
codebase can import it. The actual Anthropic API call lands in Phase C —
calling extract() now raises ClaudeExtractorUnavailable when no key is set,
or NotImplementedError when a key is present (so Phase C wiring is obvious).
"""
from __future__ import annotations

import os
from typing import Optional


CLAUDE_MODEL_ID = "claude-sonnet-4-6"


class ClaudeExtractorUnavailable(RuntimeError):
    """Raised when ANTHROPIC_API_KEY is not configured."""


def is_available() -> bool:
    return bool(os.getenv("ANTHROPIC_API_KEY"))


def extract(pdf_bytes: bytes, hint: Optional[str] = None) -> tuple[dict, float]:
    """Extract structured fields from a PDF using Claude vision.

    Args:
        pdf_bytes: raw PDF content
        hint: optional document type hint ("W2", "PAYSTUB", etc.)

    Returns:
        (fields_dict, confidence)
    """
    if not is_available():
        raise ClaudeExtractorUnavailable(
            "ANTHROPIC_API_KEY is not set; claude_extractor cannot run. "
            "Either configure the key or rely on pymupdf_extractor."
        )
    raise NotImplementedError(
        "claude_extractor.extract() body lands in Phase C — file is in place "
        "so callers (e.g. pymupdf fallback path) can import it now."
    )
