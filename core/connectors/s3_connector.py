"""S3 (or local-filesystem) EDMS connector.

Walks date-folder layout::

    s3://bucket/2026-01-01/LOS-001/W2_CURRENT.json
    s3://bucket/2026-01-02/LOS-001/PAYSTUB_CURRENT.json
    ...

Skip folders dated before the watermark; for in-window folders, read
each ``.json`` and filter on ``received_at`` so an intraday build tick
sees only the docs that arrived between two clock points.

Local mode: ``source`` is a filesystem path. S3 mode: ``source`` starts
with ``s3://`` — boto3 wiring is stubbed for the next pass; the
backtest harness uses local mode.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable, Optional

from core.connectors.base_connector import BaseEDMSConnector

logger = logging.getLogger(__name__)


SOURCE_NAME = "s3_edms_connector"
_WATERMARK_EPOCH = "2025-12-31T00:00:00+00:00"


def _parse_iso(value: str) -> datetime:
    """ISO-8601 → datetime (UTC). Accepts ``Z`` suffix as a stand-in
    for ``+00:00`` (Python <3.11 doesn't support Z natively)."""
    cleaned = value[:-1] + "+00:00" if value.endswith("Z") else value
    ts = datetime.fromisoformat(cleaned)
    return ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)


class S3EDMSConnector(BaseEDMSConnector):
    """Date-folder S3 / local-filesystem connector.

    The PG store is consulted for watermark CRUD via the existing
    ``indexing_watermarks`` table — same shape we use for the regular
    incremental indexer, just keyed on ``SOURCE_NAME='s3_edms_connector'``
    so the two cursors stay independent.
    """

    SOURCE_NAME = SOURCE_NAME

    def __init__(self, source: str, postgres_store):
        self.source   = str(source)
        self.pg       = postgres_store
        self.is_local = not self.source.startswith("s3://")
        if not self.is_local:
            # boto3 path — TODO. The backtest harness uses local mode.
            try:
                import boto3  # noqa: F401
            except ImportError as exc:  # pragma: no cover
                raise RuntimeError(
                    "S3 mode requires boto3; install or use a local path."
                ) from exc

    # ------------------------------------------------------------------
    # Watermark CRUD — reuses indexing_watermarks via the existing PG
    # store methods. New deployments start at the epoch.
    # ------------------------------------------------------------------

    async def get_watermark(self) -> str:
        try:
            row = await self.pg.get_watermark(self.SOURCE_NAME)
        except Exception as exc:
            logger.warning(
                "watermark_lookup_failed", extra={"error": str(exc)[:200]}
            )
            return _WATERMARK_EPOCH
        if not row:
            return _WATERMARK_EPOCH
        last = row.get("last_indexed_at")
        if last is None:
            return _WATERMARK_EPOCH
        if isinstance(last, datetime):
            return last.isoformat()
        return str(last)

    async def set_watermark(self, timestamp) -> None:
        try:
            await self.pg.set_watermark_timestamp(self.SOURCE_NAME, timestamp)
        except Exception as exc:
            logger.warning(
                "watermark_save_failed", extra={"error": str(exc)[:200]}
            )

    # ------------------------------------------------------------------
    # Pull
    # ------------------------------------------------------------------

    async def pull_documents_since(
        self,
        watermark: str,
        until: Optional[str] = None,
    ) -> list[dict]:
        wm = _parse_iso(watermark)
        upper = _parse_iso(until) if until else None

        docs: list[dict] = []
        for folder_date, folder_path in self._iter_date_folders():
            # Skip folders strictly before the watermark date — none of
            # those rows can satisfy received_at > watermark.
            if folder_date < wm.date():
                continue
            # Skip folders strictly after the upper bound — by symmetry.
            if upper is not None and folder_date > upper.date():
                continue
            for file_path in self._iter_files(folder_path):
                try:
                    doc = self._read_json(file_path)
                except Exception as exc:
                    logger.warning(
                        "connector_doc_read_failed",
                        extra={"path": str(file_path), "error": str(exc)[:200]},
                    )
                    continue
                received_str = doc.get("received_at")
                if not received_str:
                    continue
                try:
                    received_dt = _parse_iso(received_str)
                except Exception:
                    continue
                if received_dt <= wm:
                    continue
                if upper is not None and received_dt > upper:
                    continue
                docs.append(doc)

        # Stable order so retries on the same window are deterministic.
        docs.sort(key=lambda d: (d.get("received_at"), d.get("document_id")))
        return docs

    # ------------------------------------------------------------------
    # Iterators
    # ------------------------------------------------------------------

    def _iter_date_folders(self) -> Iterable[tuple[date, Path]]:
        """Yield (date, path) pairs for every YYYY-MM-DD folder in
        ``self.source``, sorted ascending. S3 mode is stubbed — the
        boto3 list-objects-v2 + Delimiter='/' pattern goes here."""
        if not self.is_local:
            raise NotImplementedError(
                "S3 mode pending — use a local path for the backtest harness."
            )
        root = Path(self.source)
        if not root.exists():
            return
        for child in sorted(root.iterdir()):
            if not child.is_dir():
                continue
            try:
                d = datetime.strptime(child.name, "%Y-%m-%d").date()
            except ValueError:
                continue
            yield d, child

    def _iter_files(self, folder_path: Path) -> Iterable[Path]:
        if not self.is_local:
            raise NotImplementedError("S3 list_objects_v2")
        for root, _dirs, files in os.walk(folder_path):
            for fn in files:
                if fn.endswith(".json"):
                    yield Path(root) / fn

    def _read_json(self, path: Path) -> dict:
        if not self.is_local:
            raise NotImplementedError("S3 GetObject")
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
