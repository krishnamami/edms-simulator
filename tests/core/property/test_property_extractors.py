"""Graceful-fallback tests for the 3 new property/valuation extractors
(AVM report, 1004MC market-conditions addendum, purchase agreement).
The original appraisal / HOI / flood / tax extractors are exercised in
test_generators.py via real round-trip tests; this file just locks in
the failure-path contract for the additions."""
import pytest

from core.property.extractors import (
    extract_1004mc, extract_avm_report, extract_purchase_agreement,
)


_PROPERTY_EXTRACTORS = [
    extract_avm_report,
    extract_1004mc,
    extract_purchase_agreement,
]


@pytest.mark.parametrize("extractor", _PROPERTY_EXTRACTORS,
                         ids=[fn.__name__ for fn in _PROPERTY_EXTRACTORS])
def test_empty_bytes_returns_graceful_fallback(extractor):
    fields, conf = extractor(b"")
    assert fields == {}
    assert conf == 0.5


@pytest.mark.parametrize("extractor", _PROPERTY_EXTRACTORS,
                         ids=[fn.__name__ for fn in _PROPERTY_EXTRACTORS])
def test_garbage_bytes_returns_graceful_fallback(extractor):
    fields, conf = extractor(b"\x00\x01\x02not-a-pdf")
    assert fields == {}
    assert conf == 0.5
