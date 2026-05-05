"""Identity-resolution pipeline (consumed by identity-resolution-queue.fifo)."""
import structlog

from core.aggregation.events import (
    ApplicationSubmittedEvent,
    EventType,
    IdentityResolvedEvent,
)

logger = structlog.get_logger()


class IdentityPipeline:
    def __init__(self, aggregation_service):
        self.aggregation_service = aggregation_service

    async def process(self, raw_event: dict) -> dict:
        event = ApplicationSubmittedEvent(
            event_type=EventType.APPLICATION_SUBMITTED,
            payload=raw_event.get("payload", raw_event),
        )
        result = await self.aggregation_service.handle(event)
        # Emit a follow-up identity-resolved event for downstream consumers.
        resolved = IdentityResolvedEvent(
            event_type=EventType.IDENTITY_RESOLVED,
            payload={"applicant_id": result["applicant_id"]},
        )
        await self.aggregation_service.handle(resolved)
        logger.info("identity_pipeline_processed", **result)
        return result
