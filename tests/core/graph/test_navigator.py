"""DocumentNavigator tests using the in-memory FakePostgresStore.

Tests intentionally do NOT call the Claude path — they exercise the
deterministic rule-based navigation. The Claude path is gated on
ANTHROPIC_API_KEY being unset by the conftest's environment setup.
"""
import os
from datetime import datetime

import pytest

from core.graph.navigator import DocumentNavigator
from core.graph.models import RelationshipType


@pytest.fixture(autouse=True)
def _clear_anthropic_key(monkeypatch):
    """Force the rule-based path even if ANTHROPIC_API_KEY happens to be set."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)


def _doc(doc_id, doc_type, applicant_id="APL-00001-P", **fields):
    return {
        "document_id":      doc_id,
        "applicant_id":     applicant_id,
        "document_type":    doc_type,
        "document_category": "income",
        "extracted_fields": fields,
        "confidence_score": 0.95,
        "received_at":      datetime.utcnow(),
    }


@pytest.fixture
def navigator(postgres_store):
    return DocumentNavigator(postgres_store)


def test_classify_income_question(navigator):
    assert navigator._classify("What is the qualifying annual income?") == "income"


def test_classify_credit_question(navigator):
    assert navigator._classify("Is the credit score above 700?") == "credit"


def test_classify_employment_question(navigator):
    assert navigator._classify("Who is the borrower's employer?") == "employment"


def test_classify_assets_question(navigator):
    assert navigator._classify("How much money is in their bank account?") == "assets"


@pytest.mark.asyncio
async def test_rule_based_returns_answer(postgres_store, navigator):
    await postgres_store.save_document(
        _doc("D-W2", "W2_CURRENT", box1_wages=92400, employer_name="Accenture LLC")
    )
    answer = await navigator.answer("APL-00001-P", "What is the annual income?")
    assert answer.confidence > 0
    assert answer.citations
    assert answer.reasoning_path
    assert answer.documents_read >= 1


@pytest.mark.asyncio
async def test_no_documents_requires_review(postgres_store, navigator):
    answer = await navigator.answer("APL-EMPTY", "What is the income?")
    assert answer.requires_review is True
    assert answer.documents_read == 0
    assert answer.confidence == 0.0


@pytest.mark.asyncio
async def test_conflicts_set_requires_review(postgres_store, navigator):
    await postgres_store.save_document(
        _doc("D-W2", "W2_CURRENT", box1_wages=92400)
    )
    await postgres_store.save_document(
        _doc("D-1099", "1099_NEC", amount=45000)
    )
    # Pre-seed a CONTRADICTS relationship
    postgres_store.relationships.append({
        "relationship_id":   "rel-1",
        "applicant_id":      "APL-00001-P",
        "source_doc_id":     "D-W2",
        "target_doc_id":     "D-1099",
        "relationship_type": "contradicts",
        "field_name":        "box1_wages↔amount",
        "source_value":      92400,
        "target_value":      45000,
        "delta_pct":         51.3,
        "confidence":        0.90,
        "reasoning":         "delta 51.3% > 10% — CONFLICT",
        "created_by":        "reconciler",
        "created_at":        datetime.utcnow(),
    })
    answer = await navigator.answer("APL-00001-P", "What is the annual income?")
    assert answer.requires_review is True
    assert answer.conflicts_found


@pytest.mark.asyncio
async def test_build_graph_correct_counts(postgres_store, navigator):
    await postgres_store.save_document(_doc("D-A", "W2_CURRENT", box1_wages=92400))
    await postgres_store.save_document(_doc("D-B", "PAYSTUB_CURRENT", gross_pay=3553))
    await postgres_store.save_document(_doc("D-C", "BANK_STATEMENT_M1", balance=12000))
    postgres_store.relationships.extend([
        {
            "relationship_id":   "r1",
            "applicant_id":      "APL-00001-P",
            "source_doc_id":     "D-A",
            "target_doc_id":     "D-B",
            "relationship_type": "confirms",
            "field_name":        "employer_name↔employer_name",
            "source_value":      "Accenture",
            "target_value":      "Accenture",
            "delta_pct":         None,
            "confidence":        0.95,
            "reasoning":         "fuzzy match 1.00",
            "created_by":        "reconciler",
            "created_at":        datetime.utcnow(),
        },
        {
            "relationship_id":   "r2",
            "applicant_id":      "APL-00001-P",
            "source_doc_id":     "D-A",
            "target_doc_id":     "D-C",
            "relationship_type": "corroborates",
            "field_name":        "box1_wages↔annual_payroll_deposits",
            "source_value":      92400,
            "target_value":      90000,
            "delta_pct":         2.6,
            "confidence":        0.75,
            "reasoning":         "close",
            "created_by":        "reconciler",
            "created_at":        datetime.utcnow(),
        },
    ])
    graph = await navigator.build_graph("APL-00001-P")
    assert len(graph.nodes) == 3
    assert len(graph.relationships) == 2
    assert len(graph.confirmations) == 1
    assert len(graph.conflicts) == 0
