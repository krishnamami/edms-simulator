"""Postgres operations for the ``raw_ingestion`` audit table.

Every inbound payload gets a row here BEFORE extraction runs. The status
field tracks the row through ``received → extracting → indexed`` (or
``failed``). Re-extraction flips the row to ``reprocessing`` then back to
``indexed``. The raw bytes themselves live in S3 at ``raw_s3_key``.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Optional

from core.storage import db

logger = logging.getLogger(__name__)


class RawIngestionStore:
    async def create(self, payload: dict) -> str:
        """Insert a ``received`` row immediately when data arrives.
        Returns the new ingest_id."""
        ingest_id = str(uuid.uuid4())
        await db.execute(
            """
            INSERT INTO raw_ingestion (
                ingest_id, applicant_id, application_id,
                source_channel, raw_s3_key, raw_payload_type,
                raw_size_bytes, filename, mime_type, status
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'received')
            """,
            ingest_id,
            payload.get("applicant_id"),
            payload.get("application_id"),
            payload["source_channel"],
            payload.get("raw_s3_key"),
            payload["raw_payload_type"],
            payload.get("raw_size_bytes"),
            payload.get("filename"),
            payload.get("mime_type"),
        )
        logger.info(
            "raw_ingestion_created",
            extra={"ingest_id": ingest_id, "channel": payload["source_channel"]},
        )
        return ingest_id

    async def mark_extracting(self, ingest_id: str) -> None:
        await db.execute(
            """UPDATE raw_ingestion
               SET status='extracting', updated_at=NOW()
               WHERE ingest_id=$1""",
            ingest_id,
        )

    async def mark_indexed(
        self, ingest_id: str, document_id: Optional[str] = None
    ) -> None:
        """Flip to ``indexed``. ``document_id`` is optional — set only when
        an actual ``document_index`` row exists (the FK fires otherwise).
        """
        if document_id is not None:
            await db.execute(
                """UPDATE raw_ingestion
                   SET status='indexed',
                       document_id=$1,
                       extracted_at=NOW(),
                       updated_at=NOW()
                   WHERE ingest_id=$2""",
                document_id,
                ingest_id,
            )
        else:
            await db.execute(
                """UPDATE raw_ingestion
                   SET status='indexed',
                       extracted_at=NOW(),
                       updated_at=NOW()
                   WHERE ingest_id=$1""",
                ingest_id,
            )
        logger.info(
            "raw_ingestion_indexed",
            extra={"ingest_id": ingest_id, "document_id": document_id},
        )

    async def mark_failed(self, ingest_id: str, error: str) -> None:
        await db.execute(
            """UPDATE raw_ingestion
               SET status='failed',
                   extraction_error=$1,
                   updated_at=NOW()
               WHERE ingest_id=$2""",
            (error or "")[:2000],
            ingest_id,
        )
        logger.error(
            "raw_ingestion_failed",
            extra={"ingest_id": ingest_id, "error": (error or "")[:200]},
        )

    async def mark_reprocessing(self, ingest_id: str) -> None:
        await db.execute(
            """UPDATE raw_ingestion
               SET status='reprocessing',
                   extraction_error=NULL,
                   updated_at=NOW()
               WHERE ingest_id=$1""",
            ingest_id,
        )

    async def get(self, ingest_id: str) -> Optional[dict]:
        row = await db.fetchrow(
            "SELECT * FROM raw_ingestion WHERE ingest_id=$1", ingest_id
        )
        return dict(row) if row else None

    async def get_for_applicant(
        self, applicant_id: str, status: Optional[str] = None
    ) -> list:
        if status:
            rows = await db.fetch(
                """SELECT * FROM raw_ingestion
                   WHERE applicant_id=$1 AND status=$2
                   ORDER BY received_at DESC""",
                applicant_id,
                status,
            )
        else:
            rows = await db.fetch(
                """SELECT * FROM raw_ingestion
                   WHERE applicant_id=$1
                   ORDER BY received_at DESC""",
                applicant_id,
            )
        return [dict(r) for r in rows]

    async def get_failed(self, limit: int = 50) -> list:
        rows = await db.fetch(
            """SELECT * FROM raw_ingestion
               WHERE status='failed'
               ORDER BY received_at DESC
               LIMIT $1""",
            limit,
        )
        return [dict(r) for r in rows]

    async def get_pipeline_state(self, applicant_id: str) -> dict:
        """Returns counts by status for an applicant — used by the
        ``GET /applicant/{id}/raw-ingestion`` observability endpoint."""
        rows = await db.fetch(
            """SELECT status, COUNT(*) AS count
               FROM raw_ingestion
               WHERE applicant_id=$1
               GROUP BY status""",
            applicant_id,
        )
        counts = {r["status"]: r["count"] for r in rows}
        return {
            "received":   counts.get("received", 0),
            "extracting": counts.get("extracting", 0),
            "indexed":    counts.get("indexed", 0),
            "failed":     counts.get("failed", 0),
            "reprocessing": counts.get("reprocessing", 0),
            "total":      sum(counts.values()),
        }
