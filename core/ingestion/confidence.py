"""Field-level confidence scoring + conflict detection.

The same field (e.g. annual_income) can arrive from multiple channels with
different confidence: chat=0.80, W2=0.95, IRS transcript=0.99. We pick the
highest-confidence value, and if numeric values diverge significantly we
flag a conflict for human review.
"""
from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel, Field

from core.ingestion.events import ChannelType


SOURCE_CONFIDENCE_RANKING: dict[str, float] = {
    "IRS_TRANSCRIPT": 0.99,
    "PAYROLL_API": 0.97,
    "W2_PDF": 0.95,
    "PAYSTUB_PDF": 0.93,
    "BANK_STMT_PDF": 0.90,
    "FORM_1040_PDF": 0.90,
    "API_JSON": 0.88,
    "WEB_FORM": 0.85,
    "CHAT": 0.80,
    "EMAIL_BODY": 0.75,
    "VERBAL_STATED": 0.50,
}

NUMERIC_CONFLICT_THRESHOLD = 0.10  # >10% diff between numeric values = conflict


class FieldValue(BaseModel):
    value: Any
    confidence: float
    source: str
    source_channel: ChannelType
    extracted_at: datetime = Field(default_factory=datetime.utcnow)
    requires_verification: bool = False


class ResolvedField(BaseModel):
    chosen: FieldValue
    sources: list[FieldValue]
    has_conflict: bool = False
    conflict_reason: Optional[str] = None


class ConfidenceResolver:
    def resolve(
        self, field_name: str, values: list[FieldValue]
    ) -> ResolvedField:
        if not values:
            raise ValueError(f"resolve called with no values for {field_name}")

        ranked = sorted(values, key=lambda v: v.confidence, reverse=True)
        chosen = ranked[0]
        conflict, reason = self._detect_conflict(ranked)

        return ResolvedField(
            chosen=chosen,
            sources=values,
            has_conflict=conflict,
            conflict_reason=reason,
        )

    def _detect_conflict(
        self, ranked: list[FieldValue]
    ) -> tuple[bool, Optional[str]]:
        if len(ranked) < 2:
            return False, None

        chosen = ranked[0]
        # Only numeric fields support divergence detection.
        if not isinstance(chosen.value, (int, float)) or isinstance(chosen.value, bool):
            return False, None

        for other in ranked[1:]:
            if not isinstance(other.value, (int, float)) or isinstance(other.value, bool):
                continue
            if chosen.value == 0 and other.value == 0:
                continue
            denom = max(abs(chosen.value), abs(other.value))
            if denom == 0:
                continue
            diff = abs(chosen.value - other.value) / denom
            if diff > NUMERIC_CONFLICT_THRESHOLD:
                return True, (
                    f"{chosen.source}={chosen.value} vs "
                    f"{other.source}={other.value} "
                    f"({diff:.1%} divergence)"
                )
        return False, None
