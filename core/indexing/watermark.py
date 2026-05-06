"""Watermark store for the incremental indexer.

A thin wrapper over PostgresStore that owns the per-source watermark
lifecycle: get → mark_running → create_run → ... → update + complete_run.

Why wrap rather than call ``db.execute`` directly: tests use the
in-memory FakePostgresStore. Routing through ``self.pg`` lets the same
WatermarkStore work in both production and unit tests with no plumbing
gymnastics.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _coerce_dt(value) -> datetime:
    if value is None:
        return _EPOCH
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


class WatermarkStore:
    def __init__(self, postgres_store):
        self.pg = postgres_store

    async def get(self, source: str) -> datetime:
        """Return the last_indexed_at timestamp for ``source``.

        Defaults to epoch when no row exists — that's the "scan
        everything" sentinel for the first run.
        """
        row = await self.pg.get_watermark(source)
        if not row:
            return _EPOCH
        return _coerce_dt(row.get("last_indexed_at"))

    async def get_full(self, source: str) -> Optional[dict]:
        """Return the full watermark row (including stats) or None."""
        return await self.pg.get_watermark(source)

    async def mark_running(self, source: str) -> None:
        await self.pg.upsert_watermark_status(source, "running")

    async def update(
        self, source: str, timestamp: datetime, stats: dict
    ) -> None:
        """Advance the watermark + record per-source stats. Status flips
        to ``complete`` (or ``failed`` when nothing was processed)."""
        await self.pg.upsert_watermark_complete(
            source=source,
            last_indexed_at=timestamp,
            files_processed=int(stats.get("processed") or 0),
            files_skipped=int(stats.get("skipped") or 0),
            errors=int(stats.get("errors") or 0),
            run_duration_ms=stats.get("duration_ms"),
        )

    async def set_timestamp(
        self, source: str, timestamp: datetime
    ) -> None:
        """Manual override — used by PUT /indexing/watermark."""
        await self.pg.set_watermark_timestamp(source, timestamp)

    async def create_run(
        self, source: str, watermark_from: datetime, watermark_to: datetime
    ) -> str:
        return await self.pg.create_indexing_run(
            source, watermark_from, watermark_to
        )

    async def complete_run(self, run_id: str, stats: dict) -> None:
        await self.pg.complete_indexing_run(run_id, stats)
