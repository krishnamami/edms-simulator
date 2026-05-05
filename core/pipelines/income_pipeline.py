"""Income-assembly pipeline (consumed by income-assembly-queue.fifo)."""
import structlog

from core.aggregation.events import DocumentUploadedEvent, EventType

logger = structlog.get_logger()


class IncomePipeline:
    def __init__(self, aggregation_service):
        self.aggregation_service = aggregation_service

    async def process(self, raw_event: dict) -> dict:
        event = DocumentUploadedEvent(
            event_type=EventType.DOCUMENT_UPLOADED,
            payload=raw_event.get("payload", raw_event),
        )
        result = await self.aggregation_service.handle(event)
        logger.info("income_pipeline_processed", **result)
        return result
