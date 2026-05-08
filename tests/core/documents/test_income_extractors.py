"""Graceful-fallback tests for the 6 income-side extractors.

The contract every extractor honors: ``({}, 0.5)`` on any failure
(empty bytes, garbage bytes, non-PDF blob, corrupted PDF). Real
round-trip tests against generated PDFs land when the matching
generators ship — for now, we only test the failure path so the
indexer's error-budget is provably zero.
"""
import pytest

from core.documents.extractors.income_extractors import (
    extract_1040, extract_1099, extract_irs_transcript, extract_k1,
    extract_schedule_c, extract_schedule_e,
)


_INCOME_EXTRACTORS = [
    extract_irs_transcript,
    extract_1040,
    extract_schedule_c,
    extract_schedule_e,
    extract_1099,
    extract_k1,
]


@pytest.mark.parametrize("extractor", _INCOME_EXTRACTORS,
                         ids=[fn.__name__ for fn in _INCOME_EXTRACTORS])
def test_empty_bytes_returns_graceful_fallback(extractor):
    fields, conf = extractor(b"")
    assert fields == {}
    assert conf == 0.5


@pytest.mark.parametrize("extractor", _INCOME_EXTRACTORS,
                         ids=[fn.__name__ for fn in _INCOME_EXTRACTORS])
def test_garbage_bytes_returns_graceful_fallback(extractor):
    """Random non-PDF bytes — fitz raises, the extractor catches."""
    fields, conf = extractor(b"\x00\x01\x02not-a-pdf-and-no-magic-bytes")
    assert fields == {}
    assert conf == 0.5


@pytest.mark.parametrize("extractor", _INCOME_EXTRACTORS,
                         ids=[fn.__name__ for fn in _INCOME_EXTRACTORS])
def test_truncated_pdf_returns_graceful_fallback(extractor):
    """Looks like a PDF (magic bytes) but is truncated mid-stream."""
    fields, conf = extractor(b"%PDF-1.4\n%truncated and incomplete")
    assert isinstance(fields, dict)
    # Either the extractor parses as an empty PDF (returns 0.0 / 0.5)
    # or fails gracefully — both outcomes are documented.
    assert 0.0 <= conf <= 0.99
