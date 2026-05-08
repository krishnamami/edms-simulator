"""End-of-day snapshot scheduler.

Copies the live ``entity_states`` rows for a tenant into
``entity_snapshots`` keyed on a ``snapshot_date``. Re-running on the
same date is idempotent — the UNIQUE(snapshot_date, entity_id) lets
ON CONFLICT update the snapshot in place, so a 1-build-per-day or
N-builds-per-day cadence both yield exactly one row per (date, entity).

This module is deliberately thin — the SQL lives in
``PostgresStore.take_snapshot``. The wrapper exists so the backtest
runner + a cron job both have one obvious entry point.
"""
from __future__ import annotations

import logging
from datetime import date

logger = logging.getLogger(__name__)


class SnapshotScheduler:
    def __init__(self, postgres_store):
        self.pg = postgres_store

    async def take_daily_snapshot(
        self,
        snapshot_date: date,
        tenant_id: str = "default",
    ) -> int:
        count = await self.pg.take_snapshot(snapshot_date, tenant_id)
        logger.info(
            "snapshot_taken",
            extra={"date": str(snapshot_date), "entities": count, "tenant": tenant_id},
        )
        return count
