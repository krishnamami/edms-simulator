"""Graceful-fallback tests for the 3 asset-side extractors."""
import pytest

from core.documents.extractors.asset_extractors import (
    extract_brokerage_account, extract_gift_letter, extract_retirement_account,
)


_ASSET_EXTRACTORS = [
    extract_retirement_account,
    extract_brokerage_account,
    extract_gift_letter,
]


@pytest.mark.parametrize("extractor", _ASSET_EXTRACTORS,
                         ids=[fn.__name__ for fn in _ASSET_EXTRACTORS])
def test_empty_bytes_returns_graceful_fallback(extractor):
    fields, conf = extractor(b"")
    assert fields == {}
    assert conf == 0.5


@pytest.mark.parametrize("extractor", _ASSET_EXTRACTORS,
                         ids=[fn.__name__ for fn in _ASSET_EXTRACTORS])
def test_garbage_bytes_returns_graceful_fallback(extractor):
    fields, conf = extractor(b"\xff\xd8\xff\xe0not-a-pdf-jfif-instead")
    assert fields == {}
    assert conf == 0.5


@pytest.mark.parametrize("extractor", _ASSET_EXTRACTORS,
                         ids=[fn.__name__ for fn in _ASSET_EXTRACTORS])
def test_truncated_pdf_returns_graceful_fallback(extractor):
    fields, conf = extractor(b"%PDF-1.4\n%truncated and incomplete")
    assert isinstance(fields, dict)
    assert 0.0 <= conf <= 0.99
