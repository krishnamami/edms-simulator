"""Tests for ``IncrementalGraphBuilder._classify_unknown_docs`` — the
AI-Vision classification step that the schedule-driven build uses to
turn synthesised ``UNCLASSIFIED`` shared-drive scans into typed,
applicant-resolvable docs.

The Vision call (``extract_with_claude``) is the only thing that needs
mocking — fetching the PDF bytes and running the extractor are wired
to local filesystem stubs to keep the test hermetic."""
from __future__ import annotations

import asyncio
from typing import Optional, Any

import pytest

from core.graph.incremental_builder import IncrementalGraphBuilder


class _StubConnector:
    """Bare connector — has just the surface ``_classify_unknown_docs``
    needs (``get_evidence_bytes``)."""

    def __init__(self, fake_bytes: bytes = b"%PDF-1.4 stub"):
        self._fake = fake_bytes

    def get_evidence_bytes(self, path: Any) -> bytes:
        return self._fake


class _StubPG:
    pass


def _build_builder(connector=None) -> IncrementalGraphBuilder:
    return IncrementalGraphBuilder(
        connector=connector or _StubConnector(),
        postgres_store=_StubPG(),
        redis_store=None,
    )


def _make_unclassified_doc() -> dict:
    return {
        "document_id":           "SCAN-test-2026-01-04",
        "document_type":         "UNKNOWN",
        "category":              "unknown",
        "los_id":                "UNCLASSIFIED",
        "source_channel":        "shared_drive",
        "received_at":           "2026-01-04T12:00:00Z",
        "extracted_fields":      {},
        "requires_classification": True,
        "evidence_file":         "/tmp/fake.pdf",
        "status":                "pending_classification",
    }


def test_vision_classify_updates_doc_type_and_los_id(monkeypatch):
    """Happy path: Vision returns ``document_type`` + ``los_id`` + extra
    fields → the doc's type/los_id flip, fields merge, provenance flips
    to ``ai_vision``, and stats track one classification."""
    builder = _build_builder()
    doc = _make_unclassified_doc()
    stats = {"documents_classified": 0}

    async def _fake_extract(pdf_bytes, doc_type, doc_category=""):
        return (
            {
                "document_type":   "W2_CURRENT",
                "los_id":          "LOAN-101",
                "employer_name":   "TechCorp Inc",
                "box1_wages":      125000,
            },
            0.91,
        )

    import core.documents.extractors.claude_extractor as ce
    monkeypatch.setattr(ce, "extract_with_claude", _fake_extract)

    asyncio.run(builder._classify_unknown_docs([doc], stats))

    assert doc["document_type"]               == "W2_CURRENT"
    assert doc["los_id"]                      == "LOAN-101"
    assert doc["extracted_fields"]["employer_name"] == "TechCorp Inc"
    assert doc["extracted_fields"]["box1_wages"]    == 125000
    # document_type + los_id should NOT bleed into extracted_fields —
    # they're meta-fields on the doc envelope, not loan data.
    assert "document_type" not in doc["extracted_fields"]
    assert "los_id"        not in doc["extracted_fields"]
    assert doc["extraction_method"]           == "ai_vision"
    assert doc["confidence_score"]            == 0.91
    assert doc["requires_classification"]     is False
    assert stats["documents_classified"]      == 1


def test_vision_classify_empty_response_leaves_doc_alone(monkeypatch):
    """Vision returned ``({}, 0.5)`` (graceful failure or empty page) →
    doc must stay UNCLASSIFIED so the persist gate skips it."""
    builder = _build_builder()
    doc = _make_unclassified_doc()
    stats = {"documents_classified": 0}

    async def _fake_extract(pdf_bytes, doc_type, doc_category=""):
        return ({}, 0.5)

    import core.documents.extractors.claude_extractor as ce
    monkeypatch.setattr(ce, "extract_with_claude", _fake_extract)

    asyncio.run(builder._classify_unknown_docs([doc], stats))

    assert doc["document_type"]                  == "UNKNOWN"
    assert doc["los_id"]                         == "UNCLASSIFIED"
    assert doc["requires_classification"]        is True
    assert stats["documents_classified"]         == 0


def test_vision_classify_skips_when_evidence_fetch_fails(monkeypatch):
    """S3 / disk error during evidence fetch must not propagate — doc
    falls through unmodified."""
    class _BrokenConnector(_StubConnector):
        def get_evidence_bytes(self, path):
            raise IOError("S3 timeout")

    builder = _build_builder(_BrokenConnector())
    doc = _make_unclassified_doc()
    stats = {"documents_classified": 0}

    # Fake extract still patched but should never be called.
    called = {"n": 0}

    async def _fake_extract(pdf_bytes, doc_type, doc_category=""):
        called["n"] += 1
        return ({"document_type": "W2_CURRENT"}, 0.9)

    import core.documents.extractors.claude_extractor as ce
    monkeypatch.setattr(ce, "extract_with_claude", _fake_extract)

    asyncio.run(builder._classify_unknown_docs([doc], stats))

    assert called["n"]                       == 0
    assert doc["document_type"]              == "UNKNOWN"
    assert stats["documents_classified"]     == 0


def test_vision_classify_no_candidates_short_circuits(monkeypatch):
    """A build with zero ``requires_classification=True`` docs must not
    invoke Vision at all — saves the API roundtrip on every clean
    tick."""
    builder = _build_builder()
    docs = [
        {"document_id": "EDMS-W2-001", "document_type": "W2_CURRENT",
         "los_id": "LOAN-101"},
        {"document_id": "ENC-URLA-001", "document_type": "URLA_1003",
         "los_id": "LOAN-101"},
    ]
    stats = {"documents_classified": 0}

    called = {"n": 0}

    async def _fake_extract(pdf_bytes, doc_type, doc_category=""):
        called["n"] += 1
        return ({}, 0.5)

    import core.documents.extractors.claude_extractor as ce
    monkeypatch.setattr(ce, "extract_with_claude", _fake_extract)

    asyncio.run(builder._classify_unknown_docs(docs, stats))

    assert called["n"]                   == 0
    assert stats["documents_classified"] == 0


def test_vision_classify_skipped_without_get_evidence_bytes(monkeypatch, caplog):
    """A connector without ``get_evidence_bytes`` (e.g. a future custom
    connector) logs a warning and short-circuits — it must NOT raise."""
    class _BareConnector:
        pass

    builder = _build_builder(_BareConnector())
    doc = _make_unclassified_doc()
    stats = {"documents_classified": 0}

    called = {"n": 0}

    async def _fake_extract(pdf_bytes, doc_type, doc_category=""):
        called["n"] += 1
        return ({}, 0.5)

    import core.documents.extractors.claude_extractor as ce
    monkeypatch.setattr(ce, "extract_with_claude", _fake_extract)

    asyncio.run(builder._classify_unknown_docs([doc], stats))

    assert called["n"]                       == 0
    assert stats["documents_classified"]     == 0
    assert doc["requires_classification"]    is True
