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

import asyncio
import json
import logging
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

from core.connectors.base_connector import BaseEDMSConnector

logger = logging.getLogger(__name__)


SOURCE_NAME = "s3_edms_connector"
_WATERMARK_EPOCH = "1970-01-01T00:00:00+00:00"


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

    On S3 errors (auth, throttle, bucket-not-found) the listing helpers
    deliberately RAISE rather than swallowing — the calling
    ``IncrementalGraphBuilder.run_build`` already wraps
    ``pull_documents_since`` in a try/except that records ``status=failed``
    on the ``graph_build_runs`` row. Better a loud failure with a clear
    error_details than a silent zero-doc tick that looks like "no new
    data" when really the credentials are wrong.
    """

    SOURCE_NAME = SOURCE_NAME

    def __init__(self, source: str, postgres_store):
        self.source   = str(source).strip()
        self.pg       = postgres_store
        self.is_local = not self.source.startswith("s3://")
        self._s3_client = None
        if not self.is_local:
            self._bucket, self._prefix = self._parse_s3_url(self.source)
            logger.info(
                "s3_connector_initialized",
                extra={"bucket": self._bucket,
                       "prefix": self._prefix,
                       "source": self.source},
            )

    # ------------------------------------------------------------------
    # URL parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_s3_url(url: str) -> tuple[str, str]:
        """``s3://bucket/prefix/sub[/]`` → ``(bucket, "prefix/sub")``.

        Both ``s3://bucket`` and ``s3://bucket/`` give back ``("bucket", "")``;
        both ``s3://bucket/prefix`` and ``s3://bucket/prefix/`` give back
        ``("bucket", "prefix")``. The trailing slash is stripped here so
        every consumer can build sub-paths uniformly with
        ``f"{prefix}/{folder}/"`` (no ``"//"`` accident)."""
        without_scheme = url[len("s3://"):].strip().lstrip("/")
        parts = without_scheme.split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        return bucket, prefix.strip("/")

    def _s3(self):
        """Memoised boto3 S3 client. Picks up credentials from the
        standard chain (instance role on ECS, ``~/.aws/credentials``
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
    # store methods. Null / missing watermarks fall back to the epoch
    # so a first-run pull catches everything in the bucket.
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
        """Walk every date folder >= watermark, read every .json file,
        filter on ``received_at`` ∈ (watermark, until]. The actual
        listing + reads happen on a thread executor in S3 mode so the
        event loop doesn't stall during multi-second boto3 calls."""
        if self.is_local:
            return self._pull_sync(watermark, until)
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, self._pull_sync, watermark, until,
        )

    def _pull_sync(
        self,
        watermark: str,
        until: Optional[str],
    ) -> list[dict]:
        wm = _parse_iso(watermark)
        upper = _parse_iso(until) if until else None

        logger.info(
            "connector_pull_start",
            extra={
                "is_local":  self.is_local,
                "source":    self.source,
                "bucket":    getattr(self, "_bucket", None),
                "prefix":    getattr(self, "_prefix", None),
                "watermark": wm.isoformat(),
                "until":     upper.isoformat() if upper else None,
            },
        )

        folder_count   = 0
        in_window      = 0
        total_files    = 0
        read_failed    = 0
        filtered_pre   = 0
        filtered_post  = 0
        no_received_at = 0

        docs: list[dict] = []
        for folder_date, folder_path in self._iter_date_folders():
            folder_count += 1
            if folder_date < wm.date():
                continue
            if upper is not None and folder_date > upper.date():
                continue
            in_window += 1

            folder_files    = 0
            folder_accepted = 0
            for file_path in self._iter_files(folder_path):
                folder_files  += 1
                total_files   += 1
                try:
                    doc = self._read_json(file_path)
                except Exception as exc:
                    read_failed += 1
                    logger.warning(
                        "connector_doc_read_failed",
                        extra={"path":  str(file_path),
                               "error": str(exc)[:200]},
                    )
                    continue

                received_str = doc.get("received_at")
                if not received_str:
                    no_received_at += 1
                    continue
                try:
                    received_dt = _parse_iso(received_str)
                except Exception:
                    no_received_at += 1
                    continue
                if received_dt <= wm:
                    filtered_pre += 1
                    continue
                if upper is not None and received_dt > upper:
                    filtered_post += 1
                    continue
                docs.append(doc)
                folder_accepted += 1

            logger.info(
                f"connector_folder_scanned date={folder_date.isoformat()} "
                f"files={folder_files} accepted={folder_accepted} "
                f"folder={folder_path}"
            )

        docs.sort(key=lambda d: (d.get("received_at"), d.get("document_id")))
        # Embed the funnel stats directly in the message string so the
        # default stdlib formatter (used in the production container)
        # surfaces them in CloudWatch — ``extra={}`` keys are dropped
        # by the default formatter and won't show up in log output.
        logger.info(
            "connector_pull_complete "
            f"folders_total={folder_count} folders_in_win={in_window} "
            f"files_total={total_files} read_failed={read_failed} "
            f"no_received_at={no_received_at} "
            f"filtered_pre_wm={filtered_pre} filtered_post={filtered_post} "
            f"accepted={len(docs)} "
            f"bucket={getattr(self, '_bucket', None)} "
            f"prefix={getattr(self, '_prefix', None)} "
            f"watermark={wm.isoformat()}"
        )
        return docs

    # ------------------------------------------------------------------
    # Iterators — S3 helpers RAISE on errors (vs swallowing) so the
    # outer build records a failed run rather than silently zero docs.
    # ------------------------------------------------------------------

    def _iter_date_folders(self) -> Iterable[tuple[date, Any]]:
        """Yield ``(date, folder)`` pairs for every ``YYYY-MM-DD``
        immediately under ``self.source``. ``folder`` is a ``Path`` in
        local mode and an S3 prefix string (with trailing slash) in
        S3 mode."""
        if self.is_local:
            root = Path(self.source)
            if not root.exists():
                logger.warning(
                    "connector_local_root_missing",
                    extra={"path": str(root)},
                )
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

        # S3 mode. Prefix MUST end in '/' so list_objects_v2 with
        # Delimiter='/' returns each immediate sub-prefix as a
        # CommonPrefixes entry.
        base = (f"{self._prefix}/" if self._prefix else "")
        logger.info(
            "s3_list_date_folders_start",
            extra={"bucket": self._bucket, "prefix": base},
        )
        s3 = self._s3()
        paginator = s3.get_paginator("list_objects_v2")
        seen: set[str] = set()
        page_count = 0
        common_prefix_count = 0
        for page in paginator.paginate(
            Bucket=self._bucket, Prefix=base, Delimiter="/",
        ):
            page_count += 1
            for cp in page.get("CommonPrefixes") or []:
                common_prefix_count += 1
                p = cp.get("Prefix", "")
                # "s3_simulation/2026-01-01/" → "2026-01-01"
                name = p.rstrip("/").rsplit("/", 1)[-1]
                if name in seen:
                    continue
                seen.add(name)
                try:
                    d = datetime.strptime(name, "%Y-%m-%d").date()
                except ValueError:
                    logger.debug(
                        "s3_skip_non_date_folder",
                        extra={"prefix": p, "name": name},
                    )
                    continue
                yield d, p  # full S3 prefix incl. trailing slash
        logger.info(
            f"s3_list_date_folders_complete bucket={self._bucket} "
            f"prefix={base!r} pages={page_count} "
            f"common_prefixes={common_prefix_count} date_folders={len(seen)}"
        )

    def _iter_files(self, folder_path: Any) -> Iterable[Any]:
        """In local mode yields ``Path``; in S3 mode yields S3 keys.
        ``_read_json`` knows how to read both."""
        if self.is_local:
            for root, _dirs, files in os.walk(folder_path):
                for fn in files:
                    if fn.endswith(".json"):
                        yield Path(root) / fn
            return

        s3 = self._s3()
        paginator = s3.get_paginator("list_objects_v2")
        prefix = str(folder_path)
        keys_yielded = 0
        for page in paginator.paginate(
            Bucket=self._bucket, Prefix=prefix,
        ):
            for obj in page.get("Contents") or []:
                key = obj.get("Key", "")
                if key.endswith(".json"):
                    keys_yielded += 1
                    yield key
        logger.debug(
            "s3_list_files_complete",
            extra={"bucket": self._bucket, "prefix": prefix,
                   "json_keys": keys_yielded},
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
