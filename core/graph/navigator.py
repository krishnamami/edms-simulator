"""DocumentNavigator — answers questions by traversing the document graph.

Uses SOURCE_CONFIDENCE_RANKING from core/ingestion/confidence.py to rank
document sources. With ANTHROPIC_API_KEY set, delegates reasoning to
Claude with the graph as context. Without the key, falls back to a
deterministic rule-based path that picks the highest-confidence source.
"""
from __future__ import annotations

import json
import logging
import os

from core.graph.models import (
    DocumentNode,
    DocumentRelationship,
    KnowledgeGraph,
    NavigatorAnswer,
    RelationshipType,
)
from core.ingestion.confidence import SOURCE_CONFIDENCE_RANKING

logger = logging.getLogger(__name__)


QUESTION_ROUTING: dict[str, list[str]] = {
    "income": [
        "W2_CURRENT", "W2_PRIOR", "PAYSTUB_CURRENT",
        "IRS_TRANSCRIPT", "BANK_STATEMENT_M1", "TAX_RETURN_1040_CURRENT",
    ],
    "employment": ["W2_CURRENT", "PAYSTUB_CURRENT", "EMPLOYMENT_VERIFICATION"],
    "credit":     ["CREDIT_REPORT"],
    "identity":   ["IDENTITY_DL", "PASSPORT"],
    "assets":     ["BANK_STATEMENT_M1", "BANK_STATEMENT_M2",
                   "BANK_STATEMENT_M3", "ASSET_STATEMENT"],
    "property":   ["APPRAISAL", "PURCHASE_AGREEMENT", "TITLE_COMMITMENT"],
}


def _doc_type_to_source_key(doc_type: str) -> str:
    """Translate a document_type string into the SOURCE_CONFIDENCE_RANKING key."""
    return doc_type.replace("_CURRENT", "_PDF").replace("_PRIOR", "_PDF")


class DocumentNavigator:
    def __init__(self, postgres_store, redis_store=None):
        self.postgres_store = postgres_store
        self.redis_store = redis_store

    async def answer(self, applicant_id: str, question: str) -> NavigatorAnswer:
        graph = await self.build_graph(applicant_id)
        if os.getenv("ANTHROPIC_API_KEY"):
            try:
                return await self._claude_navigate(question, graph)
            except Exception as exc:
                logger.warning("claude_navigate_failed_falling_back: %s", exc)
                # Fall through to rule-based — keep the endpoint useful.
        return self._rule_based_navigate(question, graph)

    async def build_graph(self, applicant_id: str) -> KnowledgeGraph:
        docs = await self.postgres_store.get_documents_for_applicant(applicant_id)
        rels_raw = await self.postgres_store.get_relationships_for_applicant(applicant_id)
        nodes: list[DocumentNode] = []
        for d in docs:
            fields = d.get("extracted_fields") or {}
            if isinstance(fields, str):
                try:
                    fields = json.loads(fields)
                except Exception:
                    fields = {}
            nodes.append(DocumentNode(
                document_id=d["document_id"],
                document_type=d.get("document_type", ""),
                category=d.get("document_category", ""),
                extracted_fields=fields,
                confidence_score=float(d.get("confidence_score") or 0),
                received_at=d["received_at"],
            ))
        relationships = [DocumentRelationship(**r) for r in rels_raw]
        conflicts = [
            r for r in relationships if r.relationship_type == RelationshipType.CONTRADICTS
        ]
        confirmations = [
            r for r in relationships if r.relationship_type == RelationshipType.CONFIRMS
        ]
        overall = (
            sum(n.confidence_score for n in nodes) / len(nodes) if nodes else 0.0
        )
        return KnowledgeGraph(
            applicant_id=applicant_id,
            nodes=nodes,
            relationships=relationships,
            conflicts=conflicts,
            confirmations=confirmations,
            overall_confidence=round(overall, 3),
            requires_review=len(conflicts) > 0,
        )

    async def _claude_navigate(
        self, question: str, graph: KnowledgeGraph
    ) -> NavigatorAnswer:
        import anthropic

        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        graph_summary = {
            "applicant_id": graph.applicant_id,
            "documents": [
                {
                    "document_id": n.document_id,
                    "document_type": n.document_type,
                    "confidence": n.confidence_score,
                    "fields": n.extracted_fields,
                }
                for n in graph.nodes
            ],
            "relationships": [
                {
                    "type": r.relationship_type.value,
                    "field": r.field_name,
                    "source_value": r.source_value,
                    "target_value": r.target_value,
                    "delta_pct": r.delta_pct,
                    "reasoning": r.reasoning,
                }
                for r in graph.relationships
            ],
            "conflict_count": len(graph.conflicts),
        }
        system = """You are a mortgage document analyst.
You have a document knowledge graph with typed edges:
  confirms (delta ≤5%), corroborates (delta ≤10%), contradicts (delta >10%),
  supersedes, references.

Answer the question by reasoning through the graph.
Cite every value back to its source document.
Use the highest-confidence source as your primary answer.
Flag any contradictions explicitly.

Return JSON only — no preamble:
{
  "answer": "plain English",
  "value": <numeric or string or null>,
  "confidence": <float>,
  "citations": [{"doc_id","doc_type","field","value","confidence"}],
  "reasoning_path": ["step 1",...],
  "conflicts_found": [{"field","doc_a","val_a","doc_b","val_b"}],
  "requires_review": <bool>
}"""
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2000,
            system=system,
            messages=[{
                "role": "user",
                "content": (
                    f"Graph:\n{json.dumps(graph_summary, default=str, indent=2)}"
                    f"\n\nQuestion: {question}"
                ),
            }],
        )
        raw = response.content[0].text.strip()
        if raw.startswith("```"):
            raw = "\n".join(raw.split("\n")[1:-1])
        result = json.loads(raw)
        return NavigatorAnswer(
            question=question,
            answer=result["answer"],
            value=result.get("value"),
            confidence=result["confidence"],
            citations=result.get("citations", []),
            reasoning_path=result.get("reasoning_path", []),
            conflicts_found=result.get("conflicts_found", []),
            documents_read=len(graph.nodes),
            requires_review=result.get("requires_review", False),
        )

    def _rule_based_navigate(
        self, question: str, graph: KnowledgeGraph
    ) -> NavigatorAnswer:
        qtype = self._classify(question)
        relevant_types = QUESTION_ROUTING.get(qtype, [])
        nodes = sorted(
            [n for n in graph.nodes if n.document_type in relevant_types],
            key=lambda n: SOURCE_CONFIDENCE_RANKING.get(
                _doc_type_to_source_key(n.document_type), 0.5
            ),
            reverse=True,
        )
        if not nodes:
            return NavigatorAnswer(
                question=question,
                answer="No relevant documents found.",
                confidence=0.0,
                citations=[],
                reasoning_path=["No docs"],
                conflicts_found=[],
                documents_read=0,
                requires_review=True,
            )
        best = nodes[0]
        best_conf = SOURCE_CONFIDENCE_RANKING.get(
            _doc_type_to_source_key(best.document_type), 0.5
        )
        citations = [
            {
                "doc_id": n.document_id,
                "doc_type": n.document_type,
                "confidence": SOURCE_CONFIDENCE_RANKING.get(
                    _doc_type_to_source_key(n.document_type), 0.5
                ),
                "fields": n.extracted_fields,
            }
            for n in nodes
        ]
        conflicts = [
            {
                "field": r.field_name,
                "doc_a": r.source_doc_id,
                "val_a": r.source_value,
                "doc_b": r.target_doc_id,
                "val_b": r.target_value,
                "delta_pct": r.delta_pct,
            }
            for r in graph.conflicts
        ]
        return NavigatorAnswer(
            question=question,
            answer=(
                f"Best source: {best.document_type} (conf {best_conf:.2f}). "
                f"Fields: {json.dumps(best.extracted_fields, default=str)[:200]}"
            ),
            confidence=best_conf,
            citations=citations,
            reasoning_path=[
                f"Classified question as '{qtype}'",
                f"Found {len(nodes)} relevant documents",
                "Ranked by SOURCE_CONFIDENCE_RANKING",
                f"Best: {best.document_type}",
            ],
            conflicts_found=conflicts,
            documents_read=len(nodes),
            requires_review=len(conflicts) > 0,
        )

    def _classify(self, question: str) -> str:
        q = question.lower()
        if any(w in q for w in ["income", "salary", "wages", "earn", "pay"]):
            return "income"
        if any(w in q for w in ["employ", "job", "work", "employer"]):
            return "employment"
        if any(w in q for w in ["credit", "score", "debt", "fico"]):
            return "credit"
        if any(w in q for w in ["asset", "bank", "saving", "account"]):
            return "assets"
        if any(w in q for w in ["property", "appraisal", "value", "home"]):
            return "property"
        return "income"
