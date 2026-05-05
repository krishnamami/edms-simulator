"""Normalized ingestion event — produced by every adapter.

Every channel adapter emits one of these. The aggregation service consumes
them uniformly so downstream logic doesn't care whether the data arrived
via JSON API, PDF upload, chat transcript, etc.
"""
from datetime import datetime
from enum import Enum
from typing import Optional
from uuid import uuid4

from pydantic import BaseModel, Field


class ChannelType(str, Enum):
    API = "api"
    PDF_UPLOAD = "pdf_upload"
    IMAGE_UPLOAD = "image_upload"
    EMAIL = "email"
    CHAT = "chat"
    FORM = "form"
    CSV_BATCH = "csv_batch"
    XML = "xml"


class NormalizedIngestEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    source_channel: ChannelType
    received_at: datetime = Field(default_factory=datetime.utcnow)
    applicant_signals: dict = Field(default_factory=dict)
    document_type: Optional[str] = None
    extracted_fields: dict = Field(default_factory=dict)
    raw_content_key: Optional[str] = None
    confidence: float = 0.0
    requires_verification: bool = False
    missing_fields: list[str] = Field(default_factory=list)
    documents_needed: list[str] = Field(default_factory=list)
