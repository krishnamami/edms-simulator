"""Document-ingestion pipeline (consumed by document-ingestion-queue.fifo)."""
import structlog

logger = structlog.get_logger()


class IngestPipeline:
    def __init__(self, s3_client, postgres_store, aggregation_service):
        self.s3_client = s3_client
        self.postgres_store = postgres_store
        self.aggregation_service = aggregation_service

    async def process(self, raw_event: dict) -> dict:
        p = raw_event.get("payload", raw_event)
        s3_key = self.s3_client.upload_document(
            application_id=p["application_id"],
            category=p.get("document_category", "income"),
            document_id=p["document_id"],
            content=p.get("content", b""),
        )
        doc = {
            "document_id": p["document_id"],
            "applicant_id": p["applicant_id"],
            "application_id": p["application_id"],
            "document_type": p["document_type"],
            "document_category": p.get("document_category", "income"),
            "borrower_role": p.get("borrower_role", "primary"),
            "s3_key": s3_key,
            "status": "received",
            "expiry_date": p.get("expiry_date"),
            "is_current": True,
            "extracted_fields": p.get("extracted_fields"),
            "confidence_score": p.get("confidence_score"),
        }
        await self.postgres_store.save_document(doc)
        logger.info(
            "doc_ingested", document_id=p["document_id"], s3_key=s3_key
        )
        return {"document_id": p["document_id"], "s3_key": s3_key}
