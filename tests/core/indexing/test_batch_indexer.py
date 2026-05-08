"""BatchIndexer end-to-end tests with a fake S3 client + scanner."""
from datetime import datetime, timedelta, timezone
from typing import Optional

import pytest

from core.indexing.batch_indexer import BatchIndexer
from core.indexing.s3_scanner import S3Document


# ---------------------------------------------------------------------------
# Fakes — scanner returns canned S3Document rows; S3 client returns fake bytes.
# ---------------------------------------------------------------------------


class _FakeScanner:
    """Returns a fixed list of new docs strictly after ``since``."""

    def __init__(self, docs: list[S3Document]):
        self._docs = docs

    def scan_new(self, since, prefix="loans/"):
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        return [d for d in self._docs if d.last_modified > since]

    @staticmethod
    def group_by_los(docs):
        groups: dict = {}
        for d in docs:
            groups.setdefault(d.los_id, []).append(d)
        return groups


class _FakeS3Client:
    def __init__(self):
        self._store: dict[str, bytes] = {}

    def put(self, key: str, body: bytes) -> None:
        self._store[key] = body

    def get_raw(self, key: str) -> bytes:
        return self._store.get(key, b"%PDF-1.4 fake")


def _doc(los_id: str, category: str, filename: str,
         doc_type: str, modified: datetime, *, size: int = 1024) -> S3Document:
    return S3Document(
        bucket="test-bucket",
        key=f"loans/{los_id}/{category}/{filename}",
        los_id=los_id,
        category=category,
        filename=filename,
        last_modified=modified,
        size_bytes=size,
        doc_type=doc_type,
    )


async def _seed_application(pg, *, los_id, application_id, applicant_id,
                             property_id: Optional[str] = None):
    await pg.save_golden_record({
        "applicant_id": applicant_id, "full_name": f"User {applicant_id}",
        "first_name": "User", "last_name": applicant_id,
        "dob": "1990-01-01", "ssn_hash": f"h-{applicant_id}",
        "ssn_last4": "0000", "status": "active", "identity_xrefs": [],
        "application_ids": [application_id],
    })
    await pg.save_application({
        "application_id":  application_id,
        "applicant_id":    applicant_id,
        "co_applicant_id": None,
        "los_id":          los_id,
        "status":          "active",
        "property_id":     property_id,
    })


# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_batch_indexer_processes_new_docs(
    aggregation_service, postgres_store, redis_store
):
    await _seed_application(
        postgres_store, los_id="LOS-A",
        application_id="APP-A", applicant_id="APL-A",
    )
    later = datetime(2026, 6, 1, tzinfo=timezone.utc)
    docs = [_doc("LOS-A", "income", "w2_current.pdf", "W2_CURRENT", later)]
    scanner = _FakeScanner(docs)
    s3 = _FakeS3Client()
    s3.put(docs[0].key, b"%PDF-1.4 mock")

    indexer = BatchIndexer(
        postgres_store=postgres_store,
        redis_store=redis_store,
        aggregation_service=aggregation_service,
        s3_client=s3,
        scanner=scanner,
    )
    stats = await indexer.run(source="s3")

    assert stats["found"] == 1
    assert stats["processed"] == 1
    assert stats["applicants_affected"] == 1
    assert stats["errors"] == 0
    indexed = [d for d in postgres_store.documents if d["applicant_id"] == "APL-A"]
    assert any(d["document_type"] == "W2_CURRENT" for d in indexed)


@pytest.mark.asyncio
async def test_batch_indexer_skips_unchanged_applicant(
    aggregation_service, postgres_store, redis_store
):
    """Borrower A has files that pre-date the watermark; only borrower B
    should be re-assembled."""
    await _seed_application(
        postgres_store, los_id="LOS-A",
        application_id="APP-A", applicant_id="APL-A",
    )
    await _seed_application(
        postgres_store, los_id="LOS-B",
        application_id="APP-B", applicant_id="APL-B",
    )
    # Set the watermark so A's old file is *before* the cutoff.
    cutoff = datetime(2026, 5, 15, tzinfo=timezone.utc)
    await postgres_store.set_watermark_timestamp("s3", cutoff)

    a_old = _doc("LOS-A", "income", "w2_old.pdf", "W2_CURRENT",
                 datetime(2026, 5, 1, tzinfo=timezone.utc))  # pre-cutoff
    b_new = _doc("LOS-B", "income", "w2_new.pdf", "W2_CURRENT",
                 datetime(2026, 6, 1, tzinfo=timezone.utc))  # post-cutoff
    scanner = _FakeScanner([a_old, b_new])
    s3 = _FakeS3Client()
    s3.put(b_new.key, b"%PDF-1.4 mock")

    indexer = BatchIndexer(
        postgres_store=postgres_store,
        redis_store=redis_store,
        aggregation_service=aggregation_service,
        s3_client=s3,
        scanner=scanner,
    )
    stats = await indexer.run(source="s3")

    # Only B should land
    assert stats["found"] == 1
    assert stats["applicants_affected"] == 1
    a_docs = [d for d in postgres_store.documents if d["applicant_id"] == "APL-A"]
    b_docs = [d for d in postgres_store.documents if d["applicant_id"] == "APL-B"]
    assert a_docs == []
    assert len(b_docs) == 1


@pytest.mark.asyncio
async def test_batch_indexer_unknown_los_counted_as_skipped(
    aggregation_service, postgres_store, redis_store
):
    later = datetime(2026, 6, 1, tzinfo=timezone.utc)
    docs = [_doc("LOS-UNKNOWN", "income", "w2.pdf", "W2_CURRENT", later)]
    scanner = _FakeScanner(docs)
    s3 = _FakeS3Client()
    s3.put(docs[0].key, b"%PDF-1.4 mock")

    indexer = BatchIndexer(
        postgres_store=postgres_store,
        redis_store=redis_store,
        aggregation_service=aggregation_service,
        s3_client=s3,
        scanner=scanner,
    )
    stats = await indexer.run(source="s3")
    assert stats["found"] == 1
    assert stats["applicants_affected"] == 0
    assert stats["skipped"] == 1


@pytest.mark.asyncio
async def test_batch_indexer_dry_run_no_writes(
    aggregation_service, postgres_store, redis_store
):
    await _seed_application(
        postgres_store, los_id="LOS-DRY",
        application_id="APP-DRY", applicant_id="APL-DRY",
    )
    later = datetime(2026, 6, 1, tzinfo=timezone.utc)
    docs = [_doc("LOS-DRY", "income", "w2_current.pdf", "W2_CURRENT", later)]
    scanner = _FakeScanner(docs)
    s3 = _FakeS3Client()
    s3.put(docs[0].key, b"%PDF-1.4 mock")

    indexer = BatchIndexer(
        postgres_store=postgres_store,
        redis_store=redis_store,
        aggregation_service=aggregation_service,
        s3_client=s3,
        scanner=scanner,
    )
    stats = await indexer.run(source="s3", dry_run=True)

    assert stats["found"] == 1
    assert stats["applicants_affected"] == 1
    # No document_index write
    assert postgres_store.documents == []
    # Watermark untouched (still missing from store, since update wasn't called)
    wm = postgres_store.watermarks.get("s3")
    assert wm is None or not wm.get("last_indexed_at")


@pytest.mark.asyncio
async def test_batch_indexer_property_invalidates_redis(
    aggregation_service, postgres_store, redis_store
):
    """Property doc lands → property:{id} cache flushed."""
    await _seed_application(
        postgres_store, los_id="LOS-P",
        application_id="APP-P", applicant_id="APL-P",
        property_id="PROP-P",
    )
    # Pre-warm the property profile cache so we can verify invalidation.
    await redis_store.set_property_profile("PROP-P", {"appraised_value": 400_000})
    assert await redis_store.get_property_profile("PROP-P") is not None

    later = datetime(2026, 6, 1, tzinfo=timezone.utc)
    docs = [_doc("LOS-P", "property", "appraisal_urar.pdf",
                 "APPRAISAL_URAR", later)]
    scanner = _FakeScanner(docs)
    s3 = _FakeS3Client()
    s3.put(docs[0].key, b"%PDF-1.4 mock")

    indexer = BatchIndexer(
        postgres_store=postgres_store,
        redis_store=redis_store,
        aggregation_service=aggregation_service,
        s3_client=s3,
        scanner=scanner,
    )
    stats = await indexer.run(source="s3")
    assert stats["applicants_affected"] == 1
    # Cache flushed
    assert await redis_store.get_property_profile("PROP-P") is None


@pytest.mark.asyncio
async def test_batch_indexer_advances_watermark(
    aggregation_service, postgres_store, redis_store
):
    await _seed_application(
        postgres_store, los_id="LOS-W",
        application_id="APP-W", applicant_id="APL-W",
    )
    later = datetime(2026, 6, 1, tzinfo=timezone.utc)
    docs = [_doc("LOS-W", "income", "w2_current.pdf", "W2_CURRENT", later)]
    scanner = _FakeScanner(docs)
    s3 = _FakeS3Client()
    s3.put(docs[0].key, b"%PDF-1.4 mock")

    indexer = BatchIndexer(
        postgres_store=postgres_store,
        redis_store=redis_store,
        aggregation_service=aggregation_service,
        s3_client=s3,
        scanner=scanner,
    )
    await indexer.run(source="s3")
    wm = postgres_store.watermarks.get("s3")
    assert wm is not None
    assert wm["status"] == "complete"
    assert wm["files_processed"] == 1
