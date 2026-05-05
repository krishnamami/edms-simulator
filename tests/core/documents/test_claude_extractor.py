"""Phase B placeholder behavior — Phase C lands the real implementation."""
import os

import pytest

from core.documents.extractors import claude_extractor


@pytest.mark.skipif(
    bool(os.getenv("ANTHROPIC_API_KEY")),
    reason="API key is set; skipping the unavailable-path check",
)
def test_extract_raises_when_api_key_missing():
    with pytest.raises(claude_extractor.ClaudeExtractorUnavailable):
        claude_extractor.extract(b"%PDF-1.7\n...")


@pytest.mark.skipif(
    not os.getenv("ANTHROPIC_API_KEY"),
    reason="ANTHROPIC_API_KEY not set; Phase C live test skipped",
)
def test_extract_phase_c_implementation_pending():
    # When Phase C implementation lands, this should be replaced with a
    # real round-trip assertion. Today, calling with a key still raises
    # NotImplementedError as a TODO marker.
    with pytest.raises(NotImplementedError):
        claude_extractor.extract(b"%PDF-1.7\n...", hint="W2")


def test_is_available_reflects_env():
    assert claude_extractor.is_available() == bool(os.getenv("ANTHROPIC_API_KEY"))
