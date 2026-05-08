"""S3 (or local-filesystem) EDMS connector.

Walks date-folder layout::

    s3://bucket/prefix/2026-01-01/LOS-001/W2_CURRENT.json
    s3://bucket/prefix/2026-01-02/LOS-001/PAYSTUB_CURRENT.json
    ...

Skip folders dated before the watermark; for in-window folders, read
each ``.json`` and filter on ``received_at`` so an intraday build tick
sees only the docs that arrived between two clock points.

Two source modes:
- **Local filesystem** — ``source`` is a path on disk. Used by the
  backtest harness + local development.
- **S3** — ``source`` is ``s3://bucket/prefix``. Production path:
  the ECS task definition injects
  ``S3_SIMULATION_SOURCE=s3://edms-simulator-loans/s3_simulation``
  via the YAML's ``${VAR:-default}`` indirection.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

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
        # Lazy boto3 client — only instantiated on first call so unit
        # tests + local-fs runs don't hit ``import boto3`` at all.
        self._s3_client = None
        if not self.is_local:
            self._bucket, self._prefix = self._parse_s3_url(self.source)

    @staticmethod
    def _parse_s3_url(url: str) -> tuple[str, str]:
        """``s3://bucket/prefix/sub`` → ``("bucket", "prefix/sub")``.

        The prefix has any trailing slash trimmed so we can build
        sub-paths uniformly with ``f"{prefix}/{folder}/"``."""
        without_scheme = url[len("s3://"):]
        parts = without_scheme.split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        return bucket, prefix.rstrip("/")

    def _s3(self):
        """Return a memoised boto3 S3 client. Picks up credentials from
        the standard chain (instance role on ECS, ~/.aws/credentials
        locally)."""
        if self._s3_client is None:
            try:
                import boto3  # noqa: F401
            except ImportError as exc:  # pragma: no cover
                raise RuntimeError(
                    "S3 mode requires boto3 — pip install boto3 or use "
                    "a local-filesystem path for ``source``."
                ) from exc
            import boto3
            self._s3_client = boto3.client(
                "s3",
                region_name=os.getenv("AWS_REGION", "us-east-1"),
            )
        return self._s3_client

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

    def _iter_date_folders(self) -> Iterable[tuple[date, Any]]:
        """Yield ``(date, path)`` pairs for every ``YYYY-MM-DD`` folder
        under ``self.source``, sorted ascending. ``path`` is a ``Path``
        in local mode and the absolute S3 prefix string in S3 mode —
        downstream readers branch on ``self.is_local``."""
        if self.is_local:
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
            return

        # S3 mode — paginated list_objects_v2 with Delimiter='/' so we
        # only get the immediate sub-prefixes ("date folders") rather
        # than walking the entire tree on a per-tick basis.
        s3 = self._s3()
        base = f"{self._prefix}/" if self._prefix else ""
        paginator = s3.get_paginator("list_objects_v2")
        seen: set[str] = set()
        try:
            for page in paginator.paginate(
                Bucket=self._bucket, Prefix=base, Delimiter="/",
            ):
                for cp in page.get("CommonPrefixes") or []:
                    p = cp.get("Prefix", "")
                    name = p.rstrip("/").rsplit("/", 1)[-1]
                    if name in seen:
                        continue
                    seen.add(name)
                    try:
                        d = datetime.strptime(name, "%Y-%m-%d").date()
                    except ValueError:
                        continue
                    yield d, p  # full S3 prefix incl. trailing slash
        except Exception as exc:
            logger.warning(
                "s3_list_date_folders_failed",
                extra={"bucket": self._bucket, "prefix": base,
                       "error": str(exc)[:200]},
            )

    def _iter_files(self, folder_path: Any) -> Iterable[Any]:
        """In local mode yields ``Path`` instances; in S3 mode yields
        S3 keys (strings). ``_read_json`` knows how to read both."""
        if self.is_local:
            for root, _dirs, files in os.walk(folder_path):
                for fn in files:
                    if fn.endswith(".json"):
                        yield Path(root) / fn
            return

        # S3 mode — list every object under the date-folder prefix.
        s3 = self._s3()
        paginator = s3.get_paginator("list_objects_v2")
        try:
            for page in paginator.paginate(
                Bucket=self._bucket, Prefix=str(folder_path),
            ):
                for obj in page.get("Contents") or []:
                    key = obj.get("Key", "")
                    if key.endswith(".json"):
                        yield key
        except Exception as exc:
            logger.warning(
                "s3_list_files_failed",
                extra={"bucket": self._bucket, "prefix": str(folder_path),
                       "error": str(exc)[:200]},
            )

    def _read_json(self, path: Any) -> dict:
        """``path`` is a ``Path`` in local mode and an S3 key string in
        S3 mode. Returns the parsed doc dict."""
        if self.is_local:
            with Path(path).open("r", encoding="utf-8") as f:
                return json.load(f)
        s3 = self._s3()
        obj = s3.get_object(Bucket=self._bucket, Key=str(path))
        body = obj["Body"].read()
        return json.loads(body.decode("utf-8"))
