"""Watermark store unit tests."""
from datetime import datetime, timezone

import pytest

from core.indexing.watermark import WatermarkStore


@pytest.mark.asyncio
async def test_watermark_get_default_is_epoch(postgres_store):
    store = WatermarkStore(postgres_store)
    ts = await store.get("s3")
    assert ts == datetime(1970, 1, 1, tzinfo=timezone.utc)


@pytest.mark.asyncio
async def test_watermark_update_advances(postgres_store):
    store = WatermarkStore(postgres_store)
    new_ts = datetime(2026, 5, 6, 12, 0, tzinfo=timezone.utc)
    await store.update("s3", new_ts, {"processed": 4, "skipped": 1, "errors": 0})

    seen = await store.get("s3")
    assert seen == new_ts
    full = await store.get_full("s3")
    assert full["status"] == "complete"
    assert full["files_processed"] == 4


@pytest.mark.asyncio
async def test_watermark_run_lifecycle(postgres_store):
    store = WatermarkStore(postgres_store)
    started = datetime(2026, 5, 6, 9, 0, tzinfo=timezone.utc)
    finished = datetime(2026, 5, 6, 9, 5, tzinfo=timezone.utc)
    run_id = await store.create_run("s3", started, finished)

    runs = await postgres_store.get_indexing_runs(source="s3")
    assert any(r["run_id"] == run_id for r in runs)

    await store.complete_run(run_id, {
        "found": 3, "processed": 3, "skipped": 0,
        "applicants_affected": 1, "errors": 0,
    })
    detail = await postgres_store.get_indexing_run(run_id)
    assert detail["status"] == "complete"
    assert detail["applicants_affected"] == 1
