"""S3Scanner — list files in S3 (or local_storage fallback) modified
after a watermark timestamp and group them by LOS id.

Path convention: ``loans/{los_id}/{category}/{filename}``. The scanner
respects the watermark strictly — anything modified ``<= since`` is
skipped, so a re-run never re-indexes the same file.
"""
from __future__ import annotations

import logging
import os
import pathlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class S3Document:
    bucket:        str
    key:           str
    los_id:        str
    category:      str
    filename:      str
    last_modified: datetime
    size_bytes:    int
    doc_type:      str  # detected from filename + category


def _ensure_utc(dt: Optional[datetime]) -> datetime:
    if dt is None:
        return datetime(1970, 1, 1, tzinfo=timezone.utc)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


class S3Scanner:
    def __init__(self, bucket: Optional[str] = None,
                 use_local: Optional[bool] = None,
                 local_path: Optional[str] = None):
        self.bucket = bucket or os.getenv("AWS_S3_BUCKET", "edms-simulator-loans")
        if use_local is None:
            use_local = os.getenv("USE_LOCAL_STORAGE", "true").lower() == "true"
        self.use_local = use_local
        self.local_path = pathlib.Path(
            local_path or os.getenv("LOCAL_STORAGE_PATH", "./local_storage")
        )
        self._s3 = None
        if not self.use_local:
            import boto3
            self._s3 = boto3.client(
                "s3", region_name=os.getenv("AWS_REGION", "us-east-1")
            )

    # ------------------------------------------------------------------

    def scan_new(
        self, since: datetime, prefix: str = "loans/"
    ) -> list[S3Document]:
        """Return S3Document rows modified strictly after ``since``."""
        if self.use_local:
            return self._scan_local(_ensure_utc(since), prefix)
        return self._scan_s3(_ensure_utc(since), prefix)

    def _scan_s3(
        self, since: datetime, prefix: str
    ) -> list[S3Document]:
        docs: list[S3Document] = []
        paginator = self._s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []) or []:
                modified = _ensure_utc(obj.get("LastModified"))
                if modified <= since:
                    continue
                doc = self._parse_key(
                    obj["Key"], modified, int(obj.get("Size") or 0)
                )
                if doc:
                    docs.append(doc)
        logger.info(
            "s3_scan_complete",
            extra={
                "bucket":    self.bucket,
                "since":     since.isoformat(),
                "new_files": len(docs),
            },
        )
        return docs

    def _scan_local(
        self, since: datetime, prefix: str
    ) -> list[S3Document]:
        loans_dir = self.local_path / prefix.rstrip("/")
        if not loans_dir.exists():
            return []
        docs: list[S3Document] = []
        for f in loans_dir.rglob("*"):
            if not f.is_file():
                continue
            modified = datetime.fromtimestamp(f.stat().st_mtime, tz=timezone.utc)
            if modified <= since:
                continue
            rel = str(f.relative_to(self.local_path)).replace("\\", "/")
            doc = self._parse_key(rel, modified, f.stat().st_size)
            if doc:
                docs.append(doc)
        return docs

    # ------------------------------------------------------------------

    def _parse_key(
        self, key: str, modified: datetime, size: int
    ) -> Optional[S3Document]:
        from core.ingestion.mismo import MISMOMapper

        norm = key.replace("\\", "/")
        parts = norm.split("/")
        if len(parts) < 4 or parts[0] != "loans":
            return None
        los_id   = parts[1]
        category = parts[2]
        filename = parts[3]
        if not filename or filename.startswith("."):
            return None
        doc_type = (
            MISMOMapper.detect_type_from_filename(filename, category)
            or "UNKNOWN"
        )
        return S3Document(
            bucket=self.bucket,
            key=norm,
            los_id=los_id,
            category=category,
            filename=filename,
            last_modified=modified,
            size_bytes=size,
            doc_type=doc_type,
        )

    @staticmethod
    def group_by_los(docs: list[S3Document]) -> dict[str, list[S3Document]]:
        groups: dict[str, list[S3Document]] = {}
        for doc in docs:
            groups.setdefault(doc.los_id, []).append(doc)
        return groups
