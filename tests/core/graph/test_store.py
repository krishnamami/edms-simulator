"""Graph storage tests via the in-memory FakePostgresStore."""
from datetime import datetime

import pytest


def _rel(rid, rtype="confirms", applicant="APL-00001-P"):
    return {
        "relationship_id":   rid,
        "applicant_id":      applicant,
        "source_doc_id":     "D-A",
        "target_doc_id":     "D-B",
        "relationship_type": rtype,
        "field_name":        "x↔y",
        "source_value":      "1",
        "target_value":      "1",
        "delta_pct":         None,
        "confidence":        0.9,
        "reasoning":         "test",
        "created_by":        "reconciler",
        "created_at":        datetime.utcnow(),
    }


def _doc(doc_id, applicant="APL-00001-P"):
    return {
        "document_id":   doc_id,
        "applicant_id":  applicant,
        "document_type": "W2_CURRENT",
        "document_category": "income",
        "extracted_fields": {"box1_wages": 92400},
        "confidence_score": 0.95,
    }


@pytest.mark.asyncio
async def test_save_and_retrieve_relationship(postgres_store):
    await postgres_store.save_relationship(_rel("r1"))
    rels = await postgres_store.get_relationships_for_applicant("APL-00001-P")
    assert len(rels) == 1
    assert rels[0]["relationship_id"] == "r1"


@pytest.mark.asyncio
async def test_get_conflicts_only_returns_contradicts(postgres_store):
    await postgres_store.save_relationship(_rel("r1", "confirms"))
    await postgres_store.save_relationship(_rel("r2", "contradicts"))
    await postgres_store.save_relationship(_rel("r3", "corroborates"))
    await postgres_store.save_relationship(_rel("r4", "contradicts"))

    conflicts = await postgres_store.get_conflicts_for_applicant("APL-00001-P")
    assert len(conflicts) == 2
    assert all(c["relationship_type"] == "contradicts" for c in conflicts)


@pytest.mark.asyncio
async def test_graph_summary_correct_counts(postgres_store):
    await postgres_store.save_document(_doc("D-A"))
    await postgres_store.save_document(_doc("D-B"))
    await postgres_store.save_relationship(_rel("r1", "confirms"))
    await postgres_store.save_relationship(_rel("r2", "contradicts"))
    await postgres_store.save_relationship(_rel("r3", "corroborates"))

    summary = await postgres_store.get_graph_summary("APL-00001-P")
    assert summary["document_count"] == 2
    assert summary["relationship_count"] == 3
    assert summary["confirmation_count"] == 1
    assert summary["conflict_count"] == 1
    assert summary["requires_review"] is True
