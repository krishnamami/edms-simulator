"""Graceful-fallback tests for the 3 loan-terms / employment extractors."""
import pytest

from core.documents.extractors.loan_extractors import (
    extract_offer_letter, extract_rate_lock, extract_urla_1003,
)


_LOAN_EXTRACTORS = [
    extract_urla_1003,
    extract_rate_lock,
    extract_offer_letter,
]


@pytest.mark.parametrize("extractor", _LOAN_EXTRACTORS,
                         ids=[fn.__name__ for fn in _LOAN_EXTRACTORS])
def test_empty_bytes_returns_graceful_fallback(extractor):
    fields, conf = extractor(b"")
    assert fields == {}
    assert conf == 0.5


@pytest.mark.parametrize("extractor", _LOAN_EXTRACTORS,
                         ids=[fn.__name__ for fn in _LOAN_EXTRACTORS])
def test_garbage_bytes_returns_graceful_fallback(extractor):
    # Use binary garbage with no document-format magic bytes so fitz
    # definitively rejects it — HTML / JPEG headers can sometimes be
    # parsed leniently and trigger the "parsed but no fields" path
    # instead of the failure path.
    fields, conf = extractor(b"\x00\x01\x02not-a-pdf-and-no-magic-bytes")
    assert fields == {}
    assert conf == 0.5


@pytest.mark.parametrize("extractor", _LOAN_EXTRACTORS,
                         ids=[fn.__name__ for fn in _LOAN_EXTRACTORS])
def test_truncated_pdf_returns_graceful_fallback(extractor):
    fields, conf = extractor(b"%PDF-1.4\n%truncated and incomplete")
    assert isinstance(fields, dict)
    assert 0.0 <= conf <= 0.99
