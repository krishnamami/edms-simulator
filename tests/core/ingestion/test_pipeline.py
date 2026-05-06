"""IngestionPipeline tests — verify raw-first ordering + state transitions.

Both the S3 client and the raw store are replaced with in-process fakes so
no network or DB is touched. The router is the real one (it dispatches to
the deterministic pdf_adapter for the test bytes).
"""
from __future__ import annotations

import io
import json
from datetime import date, datetime
from typing import Any

import pytest

from core.documents.generators.w2_generator import generate_w2
from core.ingestion.events import ChannelType, NormalizedIngestEvent
from core.ingestion.pipeline import IngestionPipeline


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeS3:
    """Records calls; can replay raw bytes by s3_key."""

    def __init__(self):
        self.stored: dict[str, bytes] = {}
        self.store_raw_calls: list[dict] = []
        self.get_raw_calls: list[str] = []

    def store_raw(self, source_channel, content, filename=None, applicant_id=None):
        key = f"raw/{source_channel}/{len(self.stored):04d}.bin"
        self.stored[key] = bytes(content)
        self.store_raw_calls.append({
            "source_channel": source_channel,
            "filename": filename,
            "applicant_id": applicant_id,
            "size": len(content),
            "key": key,
        })
        return key, len(content)

    def get_raw(self, s3_key: str) -> bytes:
        self.get_raw_calls.append(s3_key)
        return self.stored[s3_key]


class FakeRawStore:
    """Mirrors RawIngestionStore's async surface in-memory."""

    def __init__(self):
        self.rows: dict[str, dict] = {}
        self.calls: list[tuple[str, dict]] = []  # ordered call log

    async def create(self, payload: dict) -> str:
        import uuid as _u
        ingest_id = str(_u.uuid4())
        row = {
            "ingest_id":  ingest_id,
            "status":     "received",
            "received_at": datetime.utcnow(),
            **payload,
        }
        self.rows[ingest_id] = row
        self.calls.append(("create", dict(payload)))
        return ingest_id

    async def mark_extracting(self, ingest_id):
        self.rows[ingest_id]["status"] = "extracting"
        self.calls.append(("mark_extracting", {"ingest_id": ingest_id}))

    async def mark_indexed(self, ingest_id, document_id=None):
        self.rows[ingest_id]["status"] = "indexed"
        self.rows[ingest_id]["document_id"] = document_id
        self.rows[ingest_id]["extracted_at"] = datetime.utcnow()
        self.calls.append(("mark_indexed", {
            "ingest_id": ingest_id, "document_id": document_id,
        }))

    async def mark_failed(self, ingest_id, error):
        self.rows[ingest_id]["status"] = "failed"
        self.rows[ingest_id]["extraction_error"] = error
        self.calls.append(("mark_failed", {"ingest_id": ingest_id, "error": error}))

    async def mark_reprocessing(self, ingest_id):
        self.rows[ingest_id]["status"] = "reprocessing"
        self.rows[ingest_id]["extraction_error"] = None
        self.calls.append(("mark_reprocessing", {"ingest_id": ingest_id}))

    async def get(self, ingest_id):
        return self.rows.get(ingest_id)

    async def get_for_applicant(self, applicant_id, status=None):
        out = [r for r in self.rows.values() if r.get("applicant_id") == applicant_id]
        if status:
            out = [r for r in out if r.get("status") == status]
        return out

    async def get_failed(self, limit=50):
        return [r for r in self.rows.values() if r.get("status") == "failed"][:limit]

    async def get_pipeline_state(self, applicant_id):
        rows = await self.get_for_applicant(applicant_id)
        from collections import Counter
        c = Counter(r["status"] for r in rows)
        return {
            "received":     c.get("received", 0),
            "extracting":   c.get("extracting", 0),
            "indexed":      c.get("indexed", 0),
            "failed":       c.get("failed", 0),
            "reprocessing": c.get("reprocessing", 0),
            "total":        sum(c.values()),
        }


@pytest.fixture
def w2_pdf_bytes() -> bytes:
    pdf, _ = generate_w2(
        employee_name="James Okafor",
        employee_ssn_last4="4729",
        employee_address="100 Main",
        employer_name="Accenture LLC",
        employer_ein="123456789",
        employer_address="1 Corp",
        tax_year=2024,
        box1_wages=92400.00,
    )
    return pdf


@pytest.fixture
def pipeline_pair():
    s3 = FakeS3()
    rs = FakeRawStore()
    return IngestionPipeline(s3_client=s3, raw_store=rs), s3, rs


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_raw_stored_before_extraction(pipeline_pair, w2_pdf_bytes):
    pipeline, s3, rs = pipeline_pair
    result = await pipeline.ingest(
        channel=ChannelType.PDF_UPLOAD,
        payload=w2_pdf_bytes,
        applicant_id="APL-00001-P",
        filename="w2.pdf",
    )

    # S3 store_raw must run before raw_store.create — and both before mark_indexed.
    method_order = [c[0] for c in rs.calls]
    assert method_order == ["create", "mark_extracting", "mark_indexed"], method_order
    assert len(s3.store_raw_calls) == 1
    assert s3.store_raw_calls[0]["source_channel"] == "pdf_upload"
    assert s3.store_raw_calls[0]["applicant_id"] == "APL-00001-P"

    # And the create call carries the s3 key from the prior store_raw call.
    create_call = next(c for c in rs.calls if c[0] == "create")
    assert create_call[1]["raw_s3_key"] == s3.store_raw_calls[0]["key"]
    assert create_call[1]["source_channel"] == "pdf_upload"

    # Result preserves the underlying NormalizedIngestEvent.
    assert isinstance(result["event"], NormalizedIngestEvent)
    assert result["status"] == "indexed"


@pytest.mark.asyncio
async def test_raw_ingestion_marked_indexed_on_success(pipeline_pair, w2_pdf_bytes):
    pipeline, s3, rs = pipeline_pair
    result = await pipeline.ingest(
        channel=ChannelType.PDF_UPLOAD,
        payload=w2_pdf_bytes,
        applicant_id="APL-00001-P",
    )
    row = rs.rows[result["ingest_id"]]
    assert row["status"] == "indexed"
    assert row.get("extracted_at") is not None
    # document_id stays NULL (FK to document_index, no row exists yet)
    assert row.get("document_id") is None


@pytest.mark.asyncio
async def test_raw_ingestion_marked_failed_on_error(pipeline_pair):
    pipeline, s3, rs = pipeline_pair

    # Force the router to raise by patching its `route`.
    boom_message = "synthetic extractor failure"

    def _boom(payload, channel):
        raise RuntimeError(boom_message)

    pipeline.router.route = _boom  # type: ignore[assignment]

    with pytest.raises(RuntimeError, match=boom_message):
        await pipeline.ingest(
            channel=ChannelType.PDF_UPLOAD,
            payload=b"%PDF-1.4 not really",
            applicant_id="APL-X",
        )

    method_order = [c[0] for c in rs.calls]
    assert method_order == ["create", "mark_extracting", "mark_failed"], method_order
    failed_row = next(iter(rs.rows.values()))
    assert failed_row["status"] == "failed"
    assert boom_message in failed_row["extraction_error"]


@pytest.mark.asyncio
async def test_reprocess_reads_from_s3(pipeline_pair, w2_pdf_bytes):
    pipeline, s3, rs = pipeline_pair
    result = await pipeline.ingest(
        channel=ChannelType.PDF_UPLOAD,
        payload=w2_pdf_bytes,
        applicant_id="APL-00001-P",
        filename="w2.pdf",
    )
    raw_s3_key = result["raw_s3_key"]

    # Re-run extraction. Should read raw bytes back from S3.
    reproc = await pipeline.reprocess(result["ingest_id"])
    assert s3.get_raw_calls == [raw_s3_key]

    # After reprocess, status must be back to indexed (a NEW ingest row was
    # created by the chained ingest() call). The original row was flipped
    # to reprocessing along the way.
    assert any(c[0] == "mark_reprocessing" for c in rs.calls)
    final_status = rs.rows[reproc["ingest_id"]]["status"]
    assert final_status == "indexed"


@pytest.mark.asyncio
async def test_get_pipeline_state_counts(pipeline_pair, w2_pdf_bytes):
    pipeline, s3, rs = pipeline_pair

    # 3 successful ingests
    for _ in range(3):
        await pipeline.ingest(
            channel=ChannelType.PDF_UPLOAD,
            payload=w2_pdf_bytes,
            applicant_id="APL-00001-P",
        )

    # 1 failure
    pipeline.router.route = lambda p, c: (_ for _ in ()).throw(RuntimeError("nope"))
    with pytest.raises(RuntimeError):
        await pipeline.ingest(
            channel=ChannelType.PDF_UPLOAD,
            payload=b"junk",
            applicant_id="APL-00001-P",
        )

    state = await rs.get_pipeline_state("APL-00001-P")
    assert state["indexed"] == 3
    assert state["failed"] == 1
    assert state["total"] == 4
