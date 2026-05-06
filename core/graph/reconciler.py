"""DocumentReconciler — writes typed graph edges between documents.

Compares each new document against existing documents for the same applicant
and emits relationships (confirms / corroborates / contradicts). Numeric
divergence uses the same NUMERIC_CONFLICT_THRESHOLD as ConfidenceResolver,
so within-event and across-document conflict rules stay aligned.
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from core.graph.models import DocumentRelationship, RelationshipType
from core.ingestion.confidence import NUMERIC_CONFLICT_THRESHOLD

logger = logging.getLogger(__name__)


# Which field pairs to compare for each document type combination.
# Format: (type_a, type_b) -> [(field_in_a, field_in_b, weight)]
COMPARISON_MAP: dict[tuple, list[tuple]] = {
    ("W2_CURRENT", "IRS_TRANSCRIPT"):     [
        ("box1_wages",     "wages_tips_compensation", 1.0),
        ("employer_name",  "employer_name",           0.7),
        ("tax_year",       "tax_year",                0.5),
    ],
    ("W2_CURRENT", "PAYSTUB_CURRENT"):    [
        ("employer_name",  "employer_name", 0.8),
        ("box1_wages",     "annualized_ytd", 0.9),
    ],
    ("W2_CURRENT", "BANK_STATEMENT_M1"):  [
        ("box1_wages",     "annual_payroll_deposits", 0.7),
    ],
    ("W2_CURRENT", "W2_PRIOR"):           [
        ("employer_name",  "employer_name", 0.8),
    ],
    ("W2_CURRENT", "1099_NEC"):           [
        ("box1_wages",     "amount", 1.0),
    ],
    ("PAYSTUB_CURRENT", "PAYSTUB_PRIOR"): [
        ("employer_name",  "employer_name", 0.9),
        ("gross_pay",      "gross_pay",     0.6),
    ],
    ("W2_CURRENT", "W2_CURRENT"):         [],  # same doc type — skip
}


class DocumentReconciler:
    def __init__(self, postgres_store):
        self.postgres_store = postgres_store

    async def reconcile(
        self, applicant_id: str, new_doc: dict
    ) -> list[DocumentRelationship]:
        existing = await self.postgres_store.get_documents_for_applicant(applicant_id)
        relationships: list[DocumentRelationship] = []
        for existing_doc in existing:
            if existing_doc["document_id"] == new_doc["document_id"]:
                continue
            relationships.extend(self._compare_pair(applicant_id, new_doc, existing_doc))
        for rel in relationships:
            await self.postgres_store.save_relationship(rel.model_dump())
            logger.info(
                "relationship_written",
                extra={
                    "type": rel.relationship_type.value,
                    "applicant_id": applicant_id,
                    "field": rel.field_name,
                },
            )
        return relationships

    def _compare_pair(
        self, applicant_id: str, doc_a: dict, doc_b: dict
    ) -> list[DocumentRelationship]:
        type_a = doc_a.get("document_type", "")
        type_b = doc_b.get("document_type", "")
        fields_a = doc_a.get("extracted_fields") or {}
        fields_b = doc_b.get("extracted_fields") or {}

        if isinstance(fields_a, str):
            try:
                fields_a = json.loads(fields_a)
            except Exception:
                fields_a = {}
        if isinstance(fields_b, str):
            try:
                fields_b = json.loads(fields_b)
            except Exception:
                fields_b = {}

        pairs = self._get_pairs(type_a, type_b)
        results: list[DocumentRelationship] = []
        for field_a, field_b, weight in pairs:
            val_a = fields_a.get(field_a)
            val_b = fields_b.get(field_b)
            if val_a is None or val_b is None:
                continue
            rel = self._make_relationship(
                applicant_id=applicant_id,
                source_doc_id=doc_a["document_id"],
                target_doc_id=doc_b["document_id"],
                field_label=f"{field_a}↔{field_b}",
                val_a=val_a,
                val_b=val_b,
                weight=weight,
            )
            if rel:
                results.append(rel)
        return results

    def _make_relationship(
        self,
        applicant_id: str,
        source_doc_id: str,
        target_doc_id: str,
        field_label: str,
        val_a,
        val_b,
        weight: float = 1.0,
    ) -> Optional[DocumentRelationship]:
        # Numeric path — reuse NUMERIC_CONFLICT_THRESHOLD from confidence.py
        try:
            a = float(str(val_a).replace(",", "").replace("$", "").strip())
            b = float(str(val_b).replace(",", "").replace("$", "").strip())
            if max(abs(a), abs(b)) == 0:
                return None
            delta = abs(a - b) / max(abs(a), abs(b))
            if delta <= 0.05:
                rel_type = RelationshipType.CONFIRMS
                conf = 0.95 * weight
                note = f"delta {delta*100:.1f}% ≤ 5% — confirms"
            elif delta <= NUMERIC_CONFLICT_THRESHOLD:
                rel_type = RelationshipType.CORROBORATES
                conf = 0.75 * weight
                note = (
                    f"delta {delta*100:.1f}% ≤ "
                    f"{NUMERIC_CONFLICT_THRESHOLD*100:.0f}% — corroborates"
                )
            else:
                rel_type = RelationshipType.CONTRADICTS
                conf = 0.90 * weight
                note = (
                    f"delta {delta*100:.1f}% > "
                    f"{NUMERIC_CONFLICT_THRESHOLD*100:.0f}% — CONFLICT"
                )
            return DocumentRelationship(
                applicant_id=applicant_id,
                source_doc_id=source_doc_id,
                target_doc_id=target_doc_id,
                relationship_type=rel_type,
                field_name=field_label,
                source_value=val_a,
                target_value=val_b,
                delta_pct=round(delta * 100, 2),
                confidence=round(conf, 3),
                reasoning=f"{field_label}: {a:,.0f} vs {b:,.0f} — {note}",
            )
        except (ValueError, TypeError):
            # String path — rapidfuzz similarity
            from rapidfuzz import fuzz

            score = fuzz.ratio(str(val_a).lower(), str(val_b).lower()) / 100
            if score > 0.90:
                rel_type = RelationshipType.CONFIRMS
                conf = score * weight
            elif score > 0.70:
                rel_type = RelationshipType.CORROBORATES
                conf = score * 0.80 * weight
            else:
                rel_type = RelationshipType.CONTRADICTS
                conf = (1 - score) * weight
            return DocumentRelationship(
                applicant_id=applicant_id,
                source_doc_id=source_doc_id,
                target_doc_id=target_doc_id,
                relationship_type=rel_type,
                field_name=field_label,
                source_value=val_a,
                target_value=val_b,
                delta_pct=None,
                confidence=round(conf, 3),
                reasoning=f"{field_label}: fuzzy match {score:.2f}",
            )

    def _get_pairs(self, type_a: str, type_b: str) -> list[tuple]:
        result = COMPARISON_MAP.get((type_a, type_b))
        if result is not None:
            return result
        result = COMPARISON_MAP.get((type_b, type_a))
        if result is not None:
            return [(b, a, w) for a, b, w in result]
        return []
