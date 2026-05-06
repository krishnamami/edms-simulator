"""Document knowledge graph models.

The graph layer reasons across documents over time. Field-level conflict
detection within a single ingestion event lives in core/ingestion/confidence.py
(ConfidenceResolver). These two are complementary.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, Field


class RelationshipType(str, Enum):
    CONFIRMS     = "confirms"
    CONTRADICTS  = "contradicts"
    SUPERSEDES   = "supersedes"
    REFERENCES   = "references"
    CORROBORATES = "corroborates"


class DocumentRelationship(BaseModel):
    relationship_id:   str = Field(default_factory=lambda: str(uuid.uuid4()))
    applicant_id:      str
    source_doc_id:     str
    target_doc_id:     str
    relationship_type: RelationshipType
    field_name:        Optional[str] = None
    source_value:      Optional[Any] = None
    target_value:      Optional[Any] = None
    delta_pct:         Optional[float] = None
    confidence:        float
    reasoning:         str
    created_by:        str = "reconciler"
    created_at:        datetime = Field(default_factory=datetime.utcnow)


class DocumentNode(BaseModel):
    document_id:      str
    document_type:    str
    category:         str
    extracted_fields: dict
    confidence_score: float
    received_at:      datetime
    relationships:    list[DocumentRelationship] = []


class KnowledgeGraph(BaseModel):
    applicant_id:       str
    nodes:              list[DocumentNode]
    relationships:      list[DocumentRelationship]
    conflicts:          list[DocumentRelationship]
    confirmations:      list[DocumentRelationship]
    overall_confidence: float
    requires_review:    bool
    built_at:           datetime = Field(default_factory=datetime.utcnow)


class NavigatorAnswer(BaseModel):
    question:        str
    answer:          str
    value:           Optional[Any] = None
    confidence:      float
    citations:       list[dict]
    reasoning_path:  list[str]
    conflicts_found: list[dict]
    documents_read:  int
    requires_review: bool
