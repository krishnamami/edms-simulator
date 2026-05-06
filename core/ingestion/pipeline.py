"""IngestionPipeline — single entry point that wraps every adapter with
raw-first storage.

Order of operations on every inbound payload:

  1. Serialise the payload to bytes
  2. Store the bytes in S3 at ``raw/{channel}/.../{uuid}.{ext}``
  3. Insert a ``raw_ingestion`` row with ``status='received'``
  4. Run the channel's adapter (via ``IngestRouter.route``)
  5. Mark the row ``indexed`` (or ``failed``)

The original bytes are preserved forever, so re-extraction is always
possible via :meth:`reprocess` even after an extractor regression.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from core.ingestion.events import ChannelType, NormalizedIngestEvent
from core.ingestion.router import IngestRouter
from core.storage.raw_ingestion_store import RawIngestionStore
from core.storage.s3_client import S3Client

logger = logging.getLogger(__name__)


class IngestionPipeline:
    """Wraps :class:`IngestRouter` with raw-first persistence."""

    def __init__(
        self,
        postgres_store=None,
        redis_store=None,
        s3_client: Optional[S3Client] = None,
        raw_store: Optional[RawIngestionStore] = None,
        router: Optional[IngestRouter] = None,
    ):
        self.postgres_store = postgres_store
        self.redis_store = redis_store
        self.s3_client = s3_client or S3Client()
        self.raw_store = raw_store or RawIngestionStore()
        self.router = router or IngestRouter()

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    async def ingest(
        self,
        channel: ChannelType,
        payload: Any,
        applicant_id: Optional[str] = None,
        application_id: Optional[str] = None,
        filename: Optional[str] = None,
    ) -> dict:
        """Persist raw + run extraction. Returns a result dict.

        ``result["event"]`` mirrors the underlying ``router.route`` return
        type — ``NormalizedIngestEvent`` for most channels, ``list`` for
        EMAIL, ``tuple[list, dict]`` for CSV_BATCH. Endpoints unpack
        accordingly.
        """
        raw_bytes, mime = self._to_bytes(payload, channel)

        raw_s3_key, raw_size = self.s3_client.store_raw(
            source_channel=channel.value,
            content=raw_bytes,
            filename=filename,
            applicant_id=applicant_id,
        )

        ingest_id = await self.raw_store.create({
            "applicant_id":     applicant_id,
            "application_id":   application_id,
            "source_channel":   channel.value,
            "raw_s3_key":       raw_s3_key,
            "raw_payload_type": mime,
            "raw_size_bytes":   raw_size,
            "filename":         filename,
            "mime_type":        mime,
        })

        await self.raw_store.mark_extracting(ingest_id)
        try:
            event = self.router.route(payload, channel)
        except Exception as exc:
            await self.raw_store.mark_failed(ingest_id, str(exc))
            logger.error(
                "ingestion_failed",
                extra={"ingest_id": ingest_id, "error": str(exc)},
            )
            raise

        # raw_ingestion.document_id has a FK to document_index.document_id, so
        # we leave it NULL until a real row exists. Phase A only persists the
        # raw payload — downstream code (aggregation service, /ingest/los)
        # creates the document_index row and can update document_id later.
        await self.raw_store.mark_indexed(ingest_id, document_id=None)

        confidence = self._summary_confidence(event)
        doc_type = self._summary_doc_type(event)
        logger.info(
            "ingestion_complete",
            extra={
                "ingest_id":     ingest_id,
                "channel":       channel.value,
                "doc_type":      doc_type,
                "confidence":    confidence,
                "raw_s3_key":    raw_s3_key,
            },
        )

        return {
            "ingest_id":  ingest_id,
            "event":      event,
            "status":     "indexed",
            "raw_s3_key": raw_s3_key,
            "raw_size_bytes": raw_size,
        }

    # ------------------------------------------------------------------
    # Reprocess
    # ------------------------------------------------------------------

    async def reprocess(self, ingest_id: str) -> dict:
        """Re-run extraction on a previously stored raw payload."""
        raw = await self.raw_store.get(ingest_id)
        if not raw:
            raise ValueError(f"No raw_ingestion row for {ingest_id}")
        if raw["status"] not in ("failed", "indexed"):
            raise ValueError(f"Cannot reprocess status={raw['status']!r}")

        raw_bytes = self.s3_client.get_raw(raw["raw_s3_key"])
        await self.raw_store.mark_reprocessing(ingest_id)

        channel = ChannelType(raw["source_channel"])
        # Decode JSON-channel payloads back into dicts so the adapter's
        # type expectations are preserved.
        if raw.get("mime_type") == "application/json":
            import json as _json
            try:
                payload = _json.loads(raw_bytes.decode("utf-8"))
            except Exception:
                payload = raw_bytes
        else:
            payload = raw_bytes

        return await self.ingest(
            channel=channel,
            payload=payload,
            applicant_id=raw.get("applicant_id"),
            application_id=raw.get("application_id"),
            filename=raw.get("filename"),
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _to_bytes(payload: Any, channel: ChannelType) -> tuple[bytes, str]:
        import json as _json

        if isinstance(payload, (bytes, bytearray)):
            return bytes(payload), "application/octet-stream"
        if isinstance(payload, str):
            return payload.encode("utf-8"), "text/plain"
        # dict, list, etc. — JSON encode
        return _json.dumps(payload, default=str).encode("utf-8"), "application/json"

    @staticmethod
    def _summary_confidence(event: Any) -> Optional[float]:
        if isinstance(event, NormalizedIngestEvent):
            return event.confidence
        if isinstance(event, list) and event and isinstance(event[0], NormalizedIngestEvent):
            return event[0].confidence
        if isinstance(event, tuple) and event and isinstance(event[0], list):
            inner = event[0]
            if inner and isinstance(inner[0], NormalizedIngestEvent):
                return inner[0].confidence
        return None

    @staticmethod
    def _summary_doc_type(event: Any) -> Optional[str]:
        if isinstance(event, NormalizedIngestEvent):
            return event.document_type
        if isinstance(event, list) and event and isinstance(event[0], NormalizedIngestEvent):
            return event[0].document_type
        return None
