"""S3 (or local-filesystem) EDMS connector.

Walks a date-folder layout and yields one ``dict`` per document, ready
to feed the incremental graph builder. Two layouts are supported:

**v2 (channel-segmented)** — the production layout::

    s3://bucket/prefix/2026-01-01/
        edms_pull/             ← individual .json (one doc each;
                                 plus sibling .pdf for format-renderable
                                 doc types)
        los_encompass/         ← batch .json (a JSON array of docs;
                                 plus sibling .pdf per format-renderable
                                 batch entry)
        email_inbox/           ← {name}.pdf + {name}_meta.json pairs
        borrower_portal/       ← {name}.pdf + {name}_meta.json pairs
        vendor_equifax/        ← individual .json
        vendor_corelogic/      ← individual .json
        vendor_title/          ← {name}.pdf + {name}_meta.json pairs
        shared_drive/          ← raw .pdf only (NO metadata)
        ai_chat/               ← individual .json

**v1 (legacy flat)** — kept for backwards compat with older buckets::

    s3://bucket/prefix/2026-01-01/LOS-001/W2_CURRENT.json
                                  /LOS-001/PAYSTUB_CURRENT.json
                                  /LOS-002/...

Detection is automatic per date-folder: if any immediate sub-folder name
is in the known-channel set, the v2 dispatcher runs. Otherwise the
legacy path does a recursive ``.json`` scan and treats each file as a
single document — same behaviour the connector had before this change.

Skip folders dated before the watermark; for in-window folders, filter
each doc on ``received_at`` so an intraday build tick sees only the docs
that arrived between two clock points.

Two source modes:
- **Local filesystem** — ``source`` is a path on disk. Used by the
  backtest harness + local development.
- **S3** — ``source`` is ``s3://bucket/prefix``. Production path:
  the ECS task definition injects
  ``S3_SIMULATION_SOURCE=s3://edms-simulator-loans/s3_simulation_v2``
  via the YAML's ``${VAR:-default}`` indirection.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Optional

from core.connectors.base_connector import BaseEDMSConnector

logger = logging.getLogger(__name__)


SOURCE_NAME = "s3_edms_connector"
_WATERMARK_EPOCH = "1970-01-01T00:00:00+00:00"

# Channel dispatch — keep these as constants so a v2 backwards-compat
# extension only requires one edit.
_INDIVIDUAL_JSON_CHANNELS = {
    "edms_pull", "vendor_equifax", "vendor_corelogic", "ai_chat",
}
_BATCH_JSON_CHANNELS = {"los_encompass"}
_META_PAIR_CHANNELS  = {"email_inbox", "borrower_portal", "vendor_title"}
_RAW_SCAN_CHANNELS   = {"shared_drive"}
_KNOWN_CHANNELS = (
    _INDIVIDUAL_JSON_CHANNELS
    | _BATCH_JSON_CHANNELS
    | _META_PAIR_CHANNELS
    | _RAW_SCAN_CHANNELS
)
# Suffixes the meta-pair + raw-scan handlers treat as "evidence files"
# (the actual document binary, separate from the JSON metadata).
_EVIDENCE_SUFFIXES = (".pdf", ".jpg", ".jpeg", ".png")


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
        """Walk every date folder >= watermark, dispatch by channel,
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
        by_channel:      dict[str, int] = {}     # post-filter accept
        by_channel_seen: dict[str, int] = {}     # pre-filter raw yields

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
            folder_by_channel:      dict[str, int] = {}    # raw yields
            folder_by_channel_acc:  dict[str, int] = {}    # post-filter
            for channel_name, channel_path in self._iter_channels(folder_path):
                for doc, source_path in self._iter_channel_docs(
                    channel_name, channel_path, folder_date,
                ):
                    folder_files += 1
                    total_files  += 1
                    folder_by_channel[channel_name] = (
                        folder_by_channel.get(channel_name, 0) + 1
                    )
                    by_channel_seen[channel_name] = (
                        by_channel_seen.get(channel_name, 0) + 1
                    )
                    if doc is None:
                        # Reader already logged; bump counter and move on.
                        read_failed += 1
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
                    by_channel[channel_name] = (
                        by_channel.get(channel_name, 0) + 1
                    )
                    folder_by_channel_acc[channel_name] = (
                        folder_by_channel_acc.get(channel_name, 0) + 1
                    )

            seen_brk = ",".join(f"{k}={v}" for k, v in sorted(folder_by_channel.items())) or "-"
            acc_brk  = ",".join(f"{k}={v}" for k, v in sorted(folder_by_channel_acc.items())) or "-"
            logger.info(
                f"connector_folder_scanned date={folder_date.isoformat()} "
                f"files={folder_files} accepted={folder_accepted} "
                f"folder={folder_path} "
                f"by_channel_seen={seen_brk} by_channel_accepted={acc_brk}"
            )

        docs.sort(key=lambda d: (d.get("received_at"), d.get("document_id")))
        # Inline funnel + by_channel stats so the production stdlib
        # formatter surfaces them in CloudWatch — ``extra={}`` is dropped
        # by the default formatter and won't show up there.
        chan_acc  = ",".join(f"{k}={v}" for k, v in sorted(by_channel.items())) or "-"
        chan_seen = ",".join(f"{k}={v}" for k, v in sorted(by_channel_seen.items())) or "-"
        logger.info(
            "connector_pull_complete "
            f"folders_total={folder_count} folders_in_win={in_window} "
            f"files_total={total_files} read_failed={read_failed} "
            f"no_received_at={no_received_at} "
            f"filtered_pre_wm={filtered_pre} filtered_post={filtered_post} "
            f"accepted={len(docs)} "
            f"by_channel_seen={chan_seen} "
            f"by_channel_accepted={chan_acc} "
            f"bucket={getattr(self, '_bucket', None)} "
            f"prefix={getattr(self, '_prefix', None)} "
            f"watermark={wm.isoformat()} "
            f"until={(upper.isoformat() if upper else 'None')}"
        )
        return docs

    # ------------------------------------------------------------------
    # Date-folder + channel iteration
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
            f"s3_list_date_folders_call bucket={self._bucket} "
            f"prefix={base!r} delimiter=/"
        )
        s3 = self._s3()
        paginator = s3.get_paginator("list_objects_v2")
        seen: set[str] = set()
        non_date_skipped: list[str] = []
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
                    # Surface the rejected sub-prefix at INFO so a
                    # mis-shaped bucket (e.g. extra ``raw/`` directory
                    # alongside the dated ones) is visible in CloudWatch.
                    non_date_skipped.append(name)
                    logger.info(
                        f"s3_skip_non_date_folder prefix={p!r} name={name!r}"
                    )
                    continue
                yield d, p  # full S3 prefix incl. trailing slash
        skip_str = ",".join(non_date_skipped) or "-"
        logger.info(
            f"s3_list_date_folders_complete bucket={self._bucket} "
            f"prefix={base!r} pages={page_count} "
            f"common_prefixes={common_prefix_count} date_folders={len(seen)} "
            f"non_date_skipped=[{skip_str}]"
        )

    def _iter_channels(self, folder_path: Any) -> Iterable[tuple[str, Any]]:
        """Yield ``(channel_name, channel_path)`` pairs for each known
        channel sub-folder within a date folder.

        If no known-channel sub-folder is found the date folder is
        treated as v1 legacy: a single ``("legacy", folder_path)`` pair
        is yielded so the dispatcher can recursively scan ``.json``
        files (the original behaviour pre-channel-dispatch)."""
        if self.is_local:
            sub_dirs = [
                c for c in sorted(Path(folder_path).iterdir()) if c.is_dir()
            ]
            known = [c for c in sub_dirs if c.name in _KNOWN_CHANNELS]
            if known:
                for c in known:
                    yield c.name, c
            else:
                yield "legacy", folder_path
            return

        # S3 mode: list common prefixes under the date folder.
        s3 = self._s3()
        prefix = str(folder_path)
        if not prefix.endswith("/"):
            prefix += "/"
        try:
            result = s3.list_objects_v2(
                Bucket=self._bucket, Prefix=prefix, Delimiter="/",
            )
        except Exception:
            raise

        sub_prefixes = [
            cp.get("Prefix", "") for cp in (result.get("CommonPrefixes") or [])
        ]
        sub_names = [sp.rstrip("/").rsplit("/", 1)[-1] for sp in sub_prefixes]
        known: list[tuple[str, str]] = []
        unknown: list[str] = []
        for sp, name in zip(sub_prefixes, sub_names):
            if name in _KNOWN_CHANNELS:
                known.append((name, sp))
            else:
                unknown.append(name)

        # Emit the channel-discovery diagnostic at INFO so CloudWatch
        # surfaces it even with the default stdlib formatter. If
        # ``common_prefixes`` is 0 here that's the smoking gun — the
        # bucket contents under this date folder don't sit in immediate
        # sub-folders, so the v1 legacy recursive scan kicks in.
        is_truncated = bool(result.get("IsTruncated"))
        contents_count = len(result.get("Contents") or [])
        known_str   = ",".join(n for n, _ in known) or "-"
        unknown_str = ",".join(unknown) or "-"
        logger.info(
            f"s3_iter_channels prefix={prefix!r} delimiter=/ "
            f"common_prefixes={len(sub_prefixes)} "
            f"contents_at_root={contents_count} "
            f"is_truncated={is_truncated} "
            f"known_channels=[{known_str}] "
            f"unknown_subfolders=[{unknown_str}]"
        )

        if known:
            for chan, p in known:
                yield chan, p
        else:
            # No channel sub-folders — fall back to legacy recursive
            # scan rooted at the date folder itself. Log the fallback
            # explicitly so an operator scanning CloudWatch can tell at
            # a glance that legacy mode kicked in for this folder.
            logger.info(
                f"s3_iter_channels_legacy_fallback prefix={prefix!r} "
                f"reason=no_known_channel_subfolders "
                f"unknown_subfolders=[{unknown_str}]"
            )
            yield "legacy", folder_path

    def _iter_channel_docs(
        self,
        channel_name: str,
        channel_path: Any,
        folder_date: date,
    ) -> Iterator[tuple[Optional[dict], Any]]:
        """Channel-format-aware reader. Yields ``(doc | None, src_path)``;
        ``None`` signals a read/parse failure (caller bumps the counter).

        Every yielded doc is tagged with ``source_channel`` so downstream
        consumers can distinguish, e.g., an Encompass batch from an EDMS
        pull even when both produced the same ``document_type``."""
        if (channel_name in _INDIVIDUAL_JSON_CHANNELS
                or channel_name == "legacy"):
            for f in self._iter_files_with_suffix(channel_path, ".json"):
                # Legacy may be the meta side of a v2 pair if a layout is
                # mixed by accident — skip those so we don't double-read.
                fname = self._basename(f)
                if channel_name == "legacy" and fname.endswith("_meta.json"):
                    continue
                payload = self._safe_read_json(f)
                if payload is None:
                    yield None, f
                    continue
                if isinstance(payload, dict):
                    payload["source_channel"] = (
                        payload.get("source_channel") or channel_name
                    )
                    yield payload, f
                elif isinstance(payload, list):
                    # Defensive: someone dropped a batch into an
                    # individual-JSON channel. Explode it rather than
                    # silently dropping — log a debug note.
                    logger.debug(
                        "connector_unexpected_array",
                        extra={"path": str(f), "channel": channel_name,
                               "count": len(payload)},
                    )
                    for item in payload:
                        if isinstance(item, dict):
                            item["source_channel"] = (
                                item.get("source_channel") or channel_name
                            )
                            yield item, f
                else:
                    yield None, f
            return

        if channel_name in _BATCH_JSON_CHANNELS:
            for f in self._iter_files_with_suffix(channel_path, ".json"):
                payload = self._safe_read_json(f)
                if payload is None:
                    yield None, f
                    continue
                if isinstance(payload, list):
                    for item in payload:
                        if isinstance(item, dict):
                            item["source_channel"] = (
                                item.get("source_channel") or channel_name
                            )
                            yield item, f
                elif isinstance(payload, dict):
                    # Single-doc batch file — still valid.
                    payload["source_channel"] = (
                        payload.get("source_channel") or channel_name
                    )
                    yield payload, f
                else:
                    yield None, f
            return

        if channel_name in _META_PAIR_CHANNELS:
            for f in self._iter_files_with_suffix(channel_path, "_meta.json"):
                payload = self._safe_read_json(f)
                if payload is None:
                    yield None, f
                    continue
                if not isinstance(payload, dict):
                    yield None, f
                    continue
                payload["source_channel"] = (
                    payload.get("source_channel") or channel_name
                )
                # Attach a hint to the sibling evidence binary so a future
                # AI-vision step can fetch it. We don't probe S3 — just
                # build the conventional path (the generator strips the
                # _meta.json suffix and appends the binary extension).
                if not payload.get("evidence_file"):
                    base = str(f)[:-len("_meta.json")]
                    # Convention from generate_realworld_simulation.py:
                    # every meta-pair channel writes a sibling raw
                    # ``.pdf`` so it can be downloaded + opened directly.
                    payload["evidence_file"] = base + ".pdf"
                yield payload, f
            return

        if channel_name in _RAW_SCAN_CHANNELS:
            for f in self._iter_evidence_files(channel_path):
                yield self._synthesize_unclassified_doc(
                    f, folder_date, channel_name,
                ), f
            return

        # Unknown channel — log + skip rather than fail loudly.
        logger.debug(
            "connector_unknown_channel",
            extra={"channel": channel_name, "path": str(channel_path)},
        )
        return

    # ------------------------------------------------------------------
    # File listing helpers
    # ------------------------------------------------------------------

    def _iter_files_with_suffix(
        self, channel_path: Any, suffix: str,
    ) -> Iterable[Any]:
        """Yield Paths/keys whose names end with ``suffix``. Emits an
        INFO log per S3 call so the per-channel funnel (objects-listed
        vs suffix-matched) is visible in CloudWatch."""
        if self.is_local:
            for root, _dirs, files in os.walk(channel_path):
                for fn in sorted(files):
                    if fn.endswith(suffix):
                        yield Path(root) / fn
            return

        s3 = self._s3()
        paginator = s3.get_paginator("list_objects_v2")
        prefix = str(channel_path)
        if not prefix.endswith("/"):
            prefix += "/"
        total_seen = 0
        matched   = 0
        pages     = 0
        for page in paginator.paginate(
            Bucket=self._bucket, Prefix=prefix,
        ):
            pages += 1
            for obj in page.get("Contents") or []:
                total_seen += 1
                key = obj.get("Key", "")
                if key.endswith(suffix):
                    matched += 1
                    yield key
        logger.info(
            f"s3_iter_files prefix={prefix!r} suffix={suffix!r} "
            f"pages={pages} keys_listed={total_seen} keys_matched={matched}"
        )

    def _iter_evidence_files(self, channel_path: Any) -> Iterable[Any]:
        """Yield raw-scan file paths/keys for the shared_drive channel.
        Tries every known evidence suffix so a future generator that
        switches PDFs to PNGs / JPGs Just Works."""
        seen: set[str] = set()
        for ext in _EVIDENCE_SUFFIXES:
            for f in self._iter_files_with_suffix(channel_path, ext):
                key = str(f)
                if key in seen:
                    continue
                seen.add(key)
                yield f

    @staticmethod
    def _basename(path: Any) -> str:
        s = str(path)
        return s.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]

    def _safe_read_json(self, path: Any) -> Any:
        """Wrap ``_read_json`` with the standard error-surfacing path so
        every channel handler routes failures the same way."""
        try:
            return self._read_json(path)
        except Exception as exc:
            logger.warning(
                f"connector_doc_read_failed "
                f"path={path} "
                f"error_type={type(exc).__name__} "
                f"error={str(exc)[:300]}"
            )
            return None

    def _read_json(self, path: Any) -> Any:
        """``path`` is a ``Path`` in local mode and an S3 key string in
        S3 mode. Returns the parsed payload (dict or list)."""
        if self.is_local:
            with Path(path).open("r", encoding="utf-8") as f:
                return json.load(f)
        s3 = self._s3()
        obj = s3.get_object(Bucket=self._bucket, Key=str(path))
        body = obj["Body"].read()
        return json.loads(body.decode("utf-8"))

    # ------------------------------------------------------------------
    # Synthesised records for raw scans
    # ------------------------------------------------------------------

    def _synthesize_unclassified_doc(
        self, evidence_path: Any, folder_date: date, channel_name: str,
    ) -> dict:
        """Build a minimal doc dict for a shared-drive scan that arrived
        without metadata. The downstream builder will see
        ``los_id="UNCLASSIFIED"``, fail to resolve to an applicant, and
        skip persistence — but the file is still surfaced in the funnel
        stats so an operator can chase it down."""
        fname = self._basename(evidence_path)
        # Use a deterministic doc_id so re-pulls don't manufacture new
        # rows on every tick.
        return {
            "document_id":           f"SCAN-{fname}-{folder_date.isoformat()}",
            "document_type":         "UNKNOWN",
            "category":              "unknown",
            "los_id":                "UNCLASSIFIED",
            "source_system":         "SHARED_DRIVE",
            "source_channel":        channel_name,
            "received_at":           f"{folder_date.isoformat()}T12:00:00Z",
            "extracted_fields":      {},
            "requires_classification": True,
            "evidence_file":         str(evidence_path),
            "status":                "pending_classification",
        }
