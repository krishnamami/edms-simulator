"""Incremental knowledge-graph builder.

Pulls new documents from a ``BaseEDMSConnector``, persists each one,
runs assembly + reconciler per affected entity, and updates a single
row per entity in ``entity_states`` (no versioning — last write wins).
The companion :class:`core.graph.snapshot_scheduler.SnapshotScheduler`
copies the live ``entity_states`` into ``entity_snapshots`` at EOD so
a Decision-OS replay can walk an entity's evolution day by day.

This is the canonical replacement for the old "re-assemble on every
upload" path when running against an S3 EDMS source: you tick the
builder N times per day, it pulls only what changed, and the cost
stays bounded.
"""
from __future__ import annotations

import logging
import time
from datetime import date, datetime, timezone
from typing import Optional

from core.connectors.base_connector import BaseEDMSConnector

logger = logging.getLogger(__name__)


# Required-slot catalog mirrors api/routes._REQUIRED_DOCS — duplicating
# here to keep the builder importable without dragging in the FastAPI
# router. The catalog defines what "complete" means for completeness_pct.
_REQUIRED_SLOTS: list[dict] = [
    {"item": "W-2",                  "doc_type": "W2_CURRENT",       "alternates": ["W2_PRIOR"]},
    {"item": "Pay stub",             "doc_type": "PAYSTUB_CURRENT",  "alternates": ["PAYSTUB_PRIOR"]},
    {"item": "Credit report",        "doc_type": "CREDIT_REPORT",    "alternates": []},
    {"item": "Bank statement",       "doc_type": "BANK_STATEMENT_M1", "alternates": []},
    {"item": "DL",                   "doc_type": "DRIVERS_LICENSE",  "alternates": ["IDENTITY_DL"]},
    {"item": "SSN validation",       "doc_type": "SSN_VALIDATION",   "alternates": ["IDENTITY_SSN_CARD"]},
    {"item": "OFAC clearance",       "doc_type": "OFAC_CHECK",       "alternates": ["OFAC_REPORT"]},
    {"item": "URAR",                 "doc_type": "APPRAISAL_URAR",   "alternates": []},
    {"item": "Title commitment",     "doc_type": "TITLE_COMMITMENT", "alternates": []},
    {"item": "HOI",                  "doc_type": "HOI_BINDER",       "alternates": ["HOI_DECLARATIONS"]},
    {"item": "Flood cert",           "doc_type": "FLOOD_CERT",       "alternates": []},
    {"item": "Property tax bill",    "doc_type": "PROPERTY_TAX_BILL", "alternates": []},
    {"item": "URLA",                 "doc_type": "URLA_1003",        "alternates": []},
    {"item": "Purchase agreement",   "doc_type": "PURCHASE_AGREEMENT", "alternates": []},
    {"item": "AUS findings",         "doc_type": "AUS_DU_FINDINGS",  "alternates": ["AUS_LP_FINDINGS"]},
]
_REQUIRED_TOTAL = len(_REQUIRED_SLOTS)


def _slot_received(slot: dict, have: set[str]) -> bool:
    if slot["doc_type"] in have:
        return True
    return any(alt in have for alt in (slot.get("alternates") or []))


class IncrementalGraphBuilder:
    """Single-tick driver: pull → save → reconcile → assemble → upsert."""

    def __init__(
        self,
        connector: BaseEDMSConnector,
        postgres_store,
        redis_store,
        reconciler=None,
        aggregation_service=None,
    ):
        self.connector = connector
        self.pg        = postgres_store
        self.redis     = redis_store
        # Optional: when present, full assembly fans out via the existing
        # service. When absent, the builder still saves docs + reconciles +
        # records summary state — enough for the backtest report card.
        self.reconciler          = reconciler
        self.aggregation_service = aggregation_service

    async def run_build(
        self,
        build_date: date,
        build_number: int,
        until: Optional[str] = None,
        tenant_id: str = "default",
    ) -> dict:
        """One incremental tick.

        Steps:
          1. Read watermark from the connector.
          2. Pull ``received_at`` ∈ (watermark, until] from the connector.
          3. ``save_document`` each new row (idempotent via document_id).
          4. Group by applicant_id; for each:
              a. Re-assemble (income/credit/property/asset/identity) via
                 ``AggregationService._run_assembly`` if injected.
              b. Reconcile new docs vs existing (graph edges).
              c. Compose state summary; ``upsert_entity_state``.
          5. Advance the watermark to ``max(received_at)``.
          6. Record the run in ``graph_build_runs``.
        """
        started_at = datetime.now(timezone.utc)
        t0         = time.perf_counter()

        stats = {
            "documents_pulled":  0,
            "documents_new":     0,
            "documents_skipped": 0,
            "entities_updated":  0,
            "edges_created":     0,
            "duration_ms":       0,
        }

        wm_from = await self.connector.get_watermark()
        logger.info(
            "incremental_build_start",
            extra={"build_date": str(build_date),
                   "build_number": build_number,
                   "watermark_from": wm_from,
                   "until": until},
        )

        try:
            new_docs = await self.connector.pull_documents_since(
                wm_from, until=until,
            )
        except Exception as exc:
            stats["duration_ms"] = int((time.perf_counter() - t0) * 1000)
            await self._record_run(
                build_date, build_number, wm_from, wm_from,
                stats, started_at, status="failed",
                error_details=str(exc)[:1000], tenant_id=tenant_id,
            )
            logger.error("incremental_build_pull_failed",
                         extra={"error": str(exc)[:200]})
            return stats

        stats["documents_pulled"] = len(new_docs)
        if not new_docs:
            stats["duration_ms"] = int((time.perf_counter() - t0) * 1000)
            await self._record_run(
                build_date, build_number, wm_from, wm_from,
                stats, started_at, tenant_id=tenant_id,
            )
            return stats

        # ── Step 3: persist docs ─────────────────────────────────────
        wm_to = wm_from
        affected: dict[str, dict] = {}     # applicant_id → first-doc
        for doc in new_docs:
            doc_id = doc.get("document_id")
            applicant_id = doc.get("applicant_id")
            if not doc_id or not applicant_id:
                stats["documents_skipped"] += 1
                continue

            existing = None
            try:
                existing = await self.pg.get_document(doc_id)
            except Exception:
                existing = None
            if existing and existing.get("status") == "indexed":
                stats["documents_skipped"] += 1
            else:
                save_doc = self._build_save_doc(doc)
                try:
                    await self.pg.save_document(save_doc, tenant_id=tenant_id)
                    stats["documents_new"] += 1
                except Exception as exc:
                    logger.warning(
                        "incremental_save_doc_failed",
                        extra={"document_id": doc_id, "error": str(exc)[:200]},
                    )
                    continue

            received = doc.get("received_at") or wm_to
            if received and received > (wm_to or ""):
                wm_to = received
            affected.setdefault(applicant_id, doc)

        # ── Step 4: re-assemble + reconcile + upsert per entity ─────
        for applicant_id, first_doc in affected.items():
            application_id = first_doc.get("application_id") or ""
            entity_type    = self._classify_entity(first_doc)

            # Trigger the canonical assembly path so income / credit /
            # property / asset / identity caches all refresh. The service
            # owns its own locking + Redis write-through.
            if self.aggregation_service is not None:
                try:
                    await self.aggregation_service._run_assembly(
                        applicant_id=applicant_id,
                        application_id=application_id,
                        co_applicant_id=None,
                        documents=[],     # already in PG; assembler reads
                        loan_data={},
                    )
                except Exception as exc:
                    logger.warning(
                        "incremental_assembly_failed",
                        extra={"applicant_id": applicant_id,
                               "error": str(exc)[:200]},
                    )

            # Reconcile: emit graph edges from each new doc against the
            # full doc set. Uses the same DocumentReconciler.reconcile()
            # the AggregationService uses on document upload.
            new_for_aid = [
                d for d in new_docs if d.get("applicant_id") == applicant_id
            ]
            if self.reconciler is not None and new_for_aid:
                try:
                    for d in new_for_aid:
                        save_doc = self._build_save_doc(d)
                        edges = await self.reconciler.reconcile(
                            applicant_id, save_doc,
                        )
                        for edge in edges or []:
                            try:
                                row = edge.model_dump() if hasattr(edge, "model_dump") else dict(edge)
                                await self.pg.save_relationship(
                                    row, tenant_id=tenant_id,
                                )
                                stats["edges_created"] += 1
                            except Exception as exc:
                                logger.debug(
                                    "incremental_edge_persist_failed",
                                    extra={"error": str(exc)[:200]},
                                )
                except Exception as exc:
                    logger.warning(
                        "incremental_reconcile_failed",
                        extra={"applicant_id": applicant_id,
                               "error": str(exc)[:200]},
                    )

            # ── Compose entity_states.state summary ──────────────────
            state, doc_count, completeness = await self._compose_state(
                applicant_id, application_id, tenant_id,
            )

            try:
                edge_count     = await self.pg.count_edges_for_entity(
                    applicant_id, tenant_id=tenant_id,
                )
                conflict_count = await self.pg.count_conflicts_for_entity(
                    applicant_id, tenant_id=tenant_id,
                )
            except Exception:
                edge_count = conflict_count = 0

            try:
                await self.pg.upsert_entity_state(
                    entity_id=applicant_id,
                    entity_type=entity_type,
                    application_id=application_id,
                    state=state,
                    document_count=doc_count,
                    graph_edge_count=edge_count,
                    conflict_count=conflict_count,
                    completeness_pct=completeness,
                    tenant_id=tenant_id,
                )
                stats["entities_updated"] += 1
            except Exception as exc:
                logger.warning(
                    "incremental_upsert_state_failed",
                    extra={"applicant_id": applicant_id,
                           "error": str(exc)[:200]},
                )

        # ── Step 5: advance watermark ────────────────────────────────
        if wm_to and wm_to != wm_from:
            try:
                await self.connector.set_watermark(wm_to)
            except Exception as exc:
                logger.warning("watermark_save_failed",
                               extra={"error": str(exc)[:200]})

        # ── Step 6: record the run ───────────────────────────────────
        stats["duration_ms"] = int((time.perf_counter() - t0) * 1000)
        await self._record_run(
            build_date, build_number, wm_from, wm_to,
            stats, started_at, tenant_id=tenant_id,
        )
        logger.info("incremental_build_complete", extra={
            "build_date":    str(build_date),
            "build_number":  build_number,
            **stats,
        })
        return stats

    # ------------------------------------------------------------------

    @staticmethod
    def _classify_entity(doc: dict) -> str:
        if doc.get("category") == "property":
            return "property"
        if doc.get("borrower_role") == "co_borrower":
            return "co_borrower"
        return "borrower"

    @staticmethod
    def _build_save_doc(doc: dict) -> dict:
        """Coerce the connector's flat shape to the ``save_document``
        contract — the existing PG store expects ``document_category``,
        ``borrower_role`` etc."""
        return {
            "document_id":       doc.get("document_id"),
            "applicant_id":      doc.get("applicant_id"),
            "application_id":    doc.get("application_id"),
            "document_type":     doc.get("document_type"),
            "document_category": doc.get("category") or doc.get("document_category", "income"),
            "borrower_role":     doc.get("borrower_role", "primary"),
            "s3_key":            doc.get("s3_key"),
            "status":            "indexed",
            "extracted_fields":  doc.get("extracted_fields") or {},
            "confidence_score":  doc.get("confidence_score") or 0.94,
            "extraction_method": doc.get("extraction_method") or "caller_supplied",
        }

    async def _compose_state(
        self, applicant_id: str, application_id: str, tenant_id: str,
    ) -> tuple[dict, int, float]:
        """Build the JSONB ``state`` blob from the applicant's doc set
        + assembled profiles. Returns ``(state, doc_count, completeness_pct)``."""
        try:
            docs = await self.pg.get_documents_for_applicant(
                applicant_id, tenant_id=tenant_id,
            )
        except Exception:
            docs = []
        doc_types  = sorted({d.get("document_type") for d in docs if d.get("document_type")})
        doc_count  = len(docs)
        # Slot fulfillment — required slots only (conditional slots
        # belong to the missing-documents catalog endpoint).
        have       = set(doc_types)
        filled     = sum(1 for s in _REQUIRED_SLOTS if _slot_received(s, have))
        completeness = round(filled / _REQUIRED_TOTAL * 100, 1) if _REQUIRED_TOTAL else 0.0

        income = credit = None
        try:
            income = await self.pg.get_income_profile(applicant_id, tenant_id=tenant_id)
        except Exception:
            income = None
        try:
            credit = await self.pg.get_credit_profile(applicant_id, tenant_id=tenant_id)
        except Exception:
            credit = None

        last_received = max(
            (d.get("received_at") for d in docs if d.get("received_at")),
            default=None,
        )

        state = {
            "application_id":         application_id,
            "doc_types":              doc_types,
            "required_slots_filled":  filled,
            "required_slots_total":   _REQUIRED_TOTAL,
            "completeness_pct":       completeness,
            "last_doc_received_at":   last_received,
            "qualifying_monthly":     (
                (income or {}).get("qualifying_monthly")
                or (income or {}).get("primary_borrower", {}).get("qualifying_monthly")
            ),
            "mid_score":              (credit or {}).get("mid_score"),
            "credit_band":            (credit or {}).get("credit_band"),
        }
        return state, doc_count, completeness

    async def _record_run(
        self, build_date, build_number, wm_from, wm_to,
        stats, started_at, tenant_id="default",
        status: str = "completed",
        error_details: Optional[str] = None,
    ) -> None:
        try:
            await self.pg.insert_graph_build_run(
                build_date=build_date,
                build_number=build_number,
                watermark_from=wm_from,
                watermark_to=wm_to,
                stats=stats,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc),
                status=status,
                error_details=error_details,
                tenant_id=tenant_id,
            )
        except Exception as exc:
            logger.warning(
                "graph_build_run_persist_failed",
                extra={"error": str(exc)[:200]},
            )
