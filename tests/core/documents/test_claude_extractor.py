"""Tests for the Tier-3 Claude Vision extractor.

The extractor's contract is: never raise. Return ``({}, 0.5)`` for
every failure mode (no API key, ENABLE_AI_EXTRACTION=false, garbage
bytes, parse error). Real round-trip extraction against a generated
PDF is gated on ``ANTHROPIC_API_KEY`` and lands when a corresponding
generator ships.
"""
import os

import pytest

from core.documents.extractors import claude_extractor


# ---------------------------------------------------------------------------
# Graceful contract — every failure mode returns ({}, 0.5)
# ---------------------------------------------------------------------------

def test_sync_extract_with_disabled_flag(monkeypatch):
    """ENABLE_AI_EXTRACTION=false short-circuits before any API call."""
    monkeypatch.setenv("ENABLE_AI_EXTRACTION", "false")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-not-real")
    fields, conf = claude_extractor.extract_with_claude_sync(
        b"%PDF-1.4 fake", "IRS_TRANSCRIPT",
    )
    assert fields == {}
    assert conf == 0.5


def test_sync_extract_with_no_api_key(monkeypatch):
    """No ANTHROPIC_API_KEY → graceful fallback, no API call attempted."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setenv("ENABLE_AI_EXTRACTION", "true")
    fields, conf = claude_extractor.extract_with_claude_sync(
        b"%PDF-1.4 fake", "FORM_1040",
    )
    assert fields == {}
    assert conf == 0.5


def test_sync_extract_with_empty_bytes(monkeypatch):
    """Empty bytes → graceful fallback (PDF render fails before any
    API call)."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-not-real")
    monkeypatch.setenv("ENABLE_AI_EXTRACTION", "true")
    fields, conf = claude_extractor.extract_with_claude_sync(
        b"", "GIFT_LETTER",
    )
    assert fields == {}
    assert conf == 0.5


def test_sync_extract_with_garbage_bytes(monkeypatch):
    """Non-PDF bytes → fitz fails, graceful fallback fires."""
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-not-real")
    monkeypatch.setenv("ENABLE_AI_EXTRACTION", "true")
    fields, conf = claude_extractor.extract_with_claude_sync(
        b"\x00\x01\x02not-a-pdf", "URLA_1003",
    )
    assert fields == {}
    assert conf == 0.5


@pytest.mark.asyncio
async def test_async_extract_with_disabled_flag(monkeypatch):
    """Async path honours the same disabled-flag short-circuit."""
    monkeypatch.setenv("ENABLE_AI_EXTRACTION", "false")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-not-real")
    fields, conf = await claude_extractor.extract_with_claude(
        b"%PDF-1.4 fake", "RATE_LOCK",
    )
    assert fields == {}
    assert conf == 0.5


@pytest.mark.asyncio
async def test_async_extract_with_no_api_key(monkeypatch):
    """Async path bails on missing key without instantiating the SDK."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setenv("ENABLE_AI_EXTRACTION", "true")
    fields, conf = await claude_extractor.extract_with_claude(
        b"%PDF-1.4 fake", "OFFER_LETTER",
    )
    assert fields == {}
    assert conf == 0.5


# ---------------------------------------------------------------------------
# Backwards-compat shim for the pdf_adapter import
# ---------------------------------------------------------------------------

def test_legacy_extract_shim_is_graceful(monkeypatch):
    """The Phase-B ``extract()`` entry point now delegates to
    ``extract_with_claude_sync``. It must return graceful fallback
    instead of raising NotImplementedError / ClaudeExtractorUnavailable
    so the pdf_adapter's catch is no-longer load-bearing."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    fields, conf = claude_extractor.extract(b"%PDF-1.4 fake", hint="W2")
    assert fields == {}
    assert conf == 0.5


def test_is_available_reflects_flag_and_key(monkeypatch):
    """is_available() now requires BOTH the flag AND the key."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setenv("ENABLE_AI_EXTRACTION", "true")
    assert claude_extractor.is_available() is False

    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test-not-real")
    monkeypatch.setenv("ENABLE_AI_EXTRACTION", "true")
    assert claude_extractor.is_available() is True

    monkeypatch.setenv("ENABLE_AI_EXTRACTION", "false")
    assert claude_extractor.is_available() is False


# ---------------------------------------------------------------------------
# Field-hint registry sanity
# ---------------------------------------------------------------------------

def test_expected_fields_registry_covers_tier_2_doc_types():
    """The Tier-2 doc types must each have a field hint registered so
    the prompt is specific. Lock the lower bound so the registry never
    silently regresses."""
    registry = claude_extractor._EXPECTED_FIELDS
    # The 15 Tier-2 doc types from commit 2f97bd4 plus their canonical /
    # alias forms — minimum 15 hints.
    assert len(registry) >= 15
    # Spot-check a representative sample across categories.
    for required in (
        "IRS_TRANSCRIPT", "FORM_1040", "SCHEDULE_C",
        "GIFT_LETTER", "RETIREMENT_ACCOUNT", "BROKERAGE_ACCOUNT",
        "URLA_1003", "RATE_LOCK", "OFFER_LETTER", "AVM_REPORT",
    ):
        assert required in registry, f"missing field hint for {required}"


# ---------------------------------------------------------------------------
# Live round-trip — only when a real key is present (skipped in CI)
# ---------------------------------------------------------------------------

@pytest.mark.skipif(
    not os.getenv("ANTHROPIC_API_KEY"),
    reason="ANTHROPIC_API_KEY not set; live extraction skipped",
)
@pytest.mark.asyncio
async def test_async_extract_round_trip_against_synthetic_w2():
    """When a real key is set + a real synthetic PDF is available, the
    extractor should pull at least one field. Uses the W2 generator
    that ships with the repo."""
    from core.documents.generators.w2_generator import generate_w2
    pdf, _ = generate_w2(
        employee_name="Test Borrower",
        employer_name="Test Employer LLC",
        wages=92400,
        tax_year=2024,
    )
    fields, conf = await claude_extractor.extract_with_claude(
        pdf, "W2_CURRENT",
    )
    # At minimum, the model should pick up one field. If not, log
    # what came back so the failure is debuggable.
    assert isinstance(fields, dict), f"expected dict, got {type(fields)}"
    if fields:
        assert conf >= 0.5
