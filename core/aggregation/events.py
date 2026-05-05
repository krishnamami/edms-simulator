"""Pydantic event models for the aggregation pipeline."""
import uuid
from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class EventType(str, Enum):
    APPLICATION_SUBMITTED = "application_submitted"
    DOCUMENT_UPLOADED = "document_uploaded"
    IDENTITY_RESOLVED = "identity_resolved"
    GOLDEN_RECORD_CREATED = "golden_record_created"
    PROFILE_UPDATED = "profile_updated"
    AGGREGATION_FAILED = "aggregation_failed"
    CONFLICT_FLAGGED = "conflict_flagged"


class BaseEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    source: str = "edms-simulator"
    payload: dict


class ApplicationSubmittedEvent(BaseEvent):
    event_type: EventType = EventType.APPLICATION_SUBMITTED


class DocumentUploadedEvent(BaseEvent):
    event_type: EventType = EventType.DOCUMENT_UPLOADED


class IdentityResolvedEvent(BaseEvent):
    event_type: EventType = EventType.IDENTITY_RESOLVED
