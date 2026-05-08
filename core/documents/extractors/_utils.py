"""Shared helpers for the structured-text extractors.

The extractors all follow the same shape: open a synthetic / digitally
generated PDF with PyMuPDF, run a small regex pattern table, return
``(fields_dict, confidence)``. Helpers here are kept tiny on purpose —
each extractor stays readable on its own page.
"""
from __future__ import annotations

import re
from typing import Optional

import fitz


def safe_text(pdf_bytes: bytes) -> Optional[str]:
    """Return concatenated page text, or ``None`` if the bytes can't be
    opened as a PDF (empty, garbage, password-protected, ...).
    Callers use a ``None`` return as the signal to bail with the
    graceful fallback ``({}, 0.5)``.
    """
    if not pdf_bytes:
        return None
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception:
        return None
    try:
        return "\n".join(page.get_text("text") for page in doc)
    except Exception:
        return None
    finally:
        try:
            doc.close()
        except Exception:
            pass


def money_to_float(s: Optional[str]) -> Optional[float]:
    """Coerce ``"$92,400.00"`` / ``"92,400"`` / ``"92400"`` to float.
    Returns ``None`` on coerce failure (rather than raising)."""
    if s is None:
        return None
    cleaned = (
        str(s)
        .replace("$", "")
        .replace(",", "")
        .replace("(", "-")  # accounting parens for negatives
        .replace(")", "")
        .strip()
    )
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def fraction_populated(found: dict, expected: list[str]) -> float:
    """Fraction of ``expected`` keys that have a non-empty value in
    ``found``. Sentinel keys like ``"document_type"`` should NOT appear
    in ``expected`` — only the real fields the extractor is trying to
    pull. Used by extractors to scale a base confidence ceiling.
    """
    if not expected:
        return 0.0
    hit = sum(
        1 for k in expected
        if found.get(k) not in (None, "", [], {})
    )
    return hit / len(expected)


_LABEL_VALUE_PATTERNS = [
    # "Label: value" — colon delimiter
    r"{label}\s*:\s*([^\n]+?)(?=\s*\n|$)",
    # "Label    value" — whitespace delimiter (>=2 spaces or tab)
    r"{label}\s{{2,}}([^\n]+?)(?=\s*\n|$)",
    # "Label\nvalue" — value on the next line
    r"{label}\s*\n\s*([^\n]+)",
]


def find_labeled(text: str, label: str, flags: int = re.IGNORECASE) -> Optional[str]:
    """Try a short cascade of "Label: value" / "Label    value" /
    "Label\\nvalue" patterns. Returns the first non-empty match
    (stripped) or ``None``. The label is regex-escaped so callers can
    pass plain text (e.g. ``"Adjusted Gross Income"``).
    """
    if not text or not label:
        return None
    escaped = re.escape(label)
    for tmpl in _LABEL_VALUE_PATTERNS:
        m = re.search(tmpl.format(label=escaped), text, flags)
        if m:
            value = m.group(1).strip()
            if value:
                return value
    return None


def find_money(text: str, label: str) -> Optional[float]:
    """``find_labeled`` + ``money_to_float`` in one shot. Convenience
    for the common ``"Label: $1,234.56"`` case."""
    return money_to_float(find_labeled(text, label))


def find_int(text: str, label: str) -> Optional[int]:
    """``find_labeled`` returning the leading integer the value parses
    to. Useful for tax_year, property_count, lock_days."""
    raw = find_labeled(text, label)
    if raw is None:
        return None
    m = re.search(r"-?\d+", raw)
    if not m:
        return None
    try:
        return int(m.group(0))
    except ValueError:
        return None
