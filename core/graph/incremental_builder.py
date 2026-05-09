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

import asyncio
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
            "documents_pulled":      0,
            "documents_new":         0,
            "documents_skipped":     0,
            "documents_classified":  0,    # AI-Vision step 2.4 successes
            "applications_created":  0,    # v3 step 2.0 (loan_origination)
            "entities_updated":      0,
            "edges_created":         0,
            "duration_ms":           0,
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

        # ── Step 2.0: process v3 loan_application_submitted events ───
        # The v3 simulator emits one ``loan_origination/{los_id}_
        # application.json`` per loan with ``event_type ==
        # 'loan_application_submitted'``. Process these BEFORE los_id
        # resolution so the apps + applicants exist when the rest of
        # the day's docs hit the resolver. Idempotent: PG helper checks
        # for an existing row and returns it on re-pull, so resetting
        # the watermark + replaying the bucket doesn't double-create.
        application_events = [
            d for d in new_docs
            if d.get("event_type") == "loan_application_submitted"
        ]
        legacy_ids_by_los: dict[str, dict] = {}
        if application_events:
            create_event = getattr(self.pg, "create_application_from_event", None)
            for evt in application_events:
                los_id = evt.get("los_id")
                if not los_id:
                    continue
                # Stash the legacy_ids the event carries — the builder
                # threads these into upsert_entity_state when the same
                # los_id's docs land later in this same tick.
                legacy = dict(evt.get("legacy_ids") or {})
                legacy.setdefault("los_id", los_id)
                legacy_ids_by_los[los_id] = legacy
                if create_event is None:
                    logger.debug(
                        "create_application_from_event_unavailable "
                        f"pg={type(self.pg).__name__}"
                    )
                    continue
                try:
                    result = await create_event(evt, tenant_id=tenant_id)
                    stats["applications_created"] += 1
                    logger.info(
                        f"application_created los_id={los_id} "
                        f"applicant_id={result.get('applicant_id')} "
                        f"co_applicant_id={result.get('co_applicant_id')} "
                        f"application_id={result.get('application_id')}"
                    )
                except Exception as exc:
                    logger.warning(
                        f"create_application_failed los_id={los_id} "
                        f"error_type={type(exc).__name__} "
                        f"error={str(exc)[:200]}"
                    )
            # Drop the events from new_docs — they're not real documents
            # and the persist gate would otherwise try to FK them.
            new_docs = [
                d for d in new_docs
                if d.get("event_type") != "loan_application_submitted"
            ]

        # ── Step 2.4: AI-Vision classify shared-drive scans ──────────
        # Connector synthesises ``UNKNOWN`` docs with
        # ``requires_classification=True`` for every raw scan that
        # arrived without metadata. Fetch each PDF, run Claude Vision
        # with the UNKNOWN field hint (asks for document_type +
        # los_id + borrower-identifying fields), and merge whatever
        # came back onto the doc. If Vision returned a recognisable
        # document_type and/or los_id, the doc rolls forward into the
        # los_id-resolution step below and may now resolve to a real
        # applicant; if not, it stays unclassified and falls out at
        # the persist gate (no FK violation, just a documents_skipped).
        # All-graceful: extract_with_claude returns ({}, 0.5) on any
        # missing key / disabled flag / network error.
        await self._classify_unknown_docs(new_docs, stats)

        # ── Step 2.5: resolve los_id → applicant_id ──────────────────
        # The v2 connector emits docs that carry only ``los_id`` (the
        # generators don't know which APL-XXXXX-P the API minted). Look
        # up each unique los_id once and stamp applicant_id +
        # application_id onto every doc that lacks them. Docs whose
        # los_id can't be resolved get skipped further down because
        # the persist loop refuses any doc without applicant_id. The
        # ``UNCLASSIFIED`` los_id (synthesised by shared_drive scans)
        # also ends up here and is skipped — exactly what we want.
        los_cache: dict[str, Optional[dict]] = {}
        for doc in new_docs:
            if doc.get("applicant_id"):
                continue
            los_id = doc.get("los_id")
            if not los_id:
                continue
            if los_id not in los_cache:
                try:
                    los_cache[los_id] = await self.pg.get_application_by_los_id(
                        los_id, tenant_id=tenant_id,
                    )
                except Exception as exc:
                    logger.warning(
                        "los_id_lookup_failed",
                        extra={"los_id": los_id, "error": str(exc)[:200]},
                    )
                    los_cache[los_id] = None
            app = los_cache[los_id]
            if app:
                # The role tells us which applicant_id maps in: primary →
                # applicant_id; co_borrower → co_applicant_id (with
                # primary fallback when no co_applicant exists).
                role = doc.get("borrower_role", "primary")
                if role == "co_borrower" and app.get("co_applicant_id"):
                    doc["applicant_id"] = app["co_applicant_id"]
                else:
                    doc["applicant_id"] = app["applicant_id"]
                doc["application_id"] = app["application_id"]
            else:
                logger.warning(
                    "unknown_los_id",
                    extra={"los_id": los_id,
                           "doc_id": doc.get("document_id"),
                           "channel": doc.get("source_channel")},
                )

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

            # Accumulate legacy_ids for this applicant from (a) the
            # loan_origination event's encompass IDs (when the same
            # los_id surfaced earlier in this tick) and (b) every
            # source_document_id docs in ``new_docs`` carry. PG merges
            # via JSONB ``||`` so the column grows over time.
            los_id_for_entity = first_doc.get("los_id")
            legacy_for_entity: dict = {}
            if los_id_for_entity and los_id_for_entity in legacy_ids_by_los:
                legacy_for_entity.update(legacy_ids_by_los[los_id_for_entity])
            src_ids = sorted({
                d.get("source_document_id") for d in new_docs
                if d.get("applicant_id") == applicant_id
                and d.get("source_document_id")
            })
            if src_ids:
                legacy_for_entity["source_document_ids"] = src_ids
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
                    legacy_ids=legacy_for_entity,
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

    async def _classify_unknown_docs(
        self, new_docs: list[dict], stats: dict,
    ) -> None:
        """Run Claude Vision on every doc carrying
        ``requires_classification=True``. Updates the doc in-place when
        Vision returned actionable fields:

        - ``document_type`` from the model overrides ``UNKNOWN`` so the
          downstream graph reconciler treats the doc as the right kind.
        - ``los_id`` (if visible on the doc) overrides ``UNCLASSIFIED``
          so the next step can resolve it to a real applicant.
        - All extracted fields merge into ``extracted_fields``.
        - ``extraction_method='ai_vision'`` records provenance for the
          ``/applicant/.../graph/summary`` extraction breakdown.

        Vision-failure or empty-response leaves the doc untouched —
        it still falls through to the los_id-resolution step and (with
        ``los_id='UNCLASSIFIED'``) gets skipped at the persist gate.
        """
        candidates = [d for d in new_docs if d.get("requires_classification")]
        if not candidates:
            return

        try:
            from core.documents.extractors.claude_extractor import (
                extract_with_claude,
            )
        except Exception as exc:    # pragma: no cover — import-only failure
            logger.warning(
                "vision_extractor_unavailable",
                extra={"error": str(exc)[:200]},
            )
            return

        connector_get_bytes = getattr(
            self.connector, "get_evidence_bytes", None,
        )
        if connector_get_bytes is None:
            logger.warning(
                "vision_classify_skipped reason=connector_lacks_get_evidence_bytes "
                f"connector={type(self.connector).__name__}"
            )
            return

        for doc in candidates:
            evidence_path = doc.get("evidence_file")
            if not evidence_path:
                continue
            try:
                # connector.get_evidence_bytes is sync (boto3 / Path)
                # so wrap in a thread executor to keep the event loop
                # unblocked on multi-megabyte PDFs from S3.
                pdf_bytes = await asyncio.to_thread(
                    connector_get_bytes, evidence_path,
                )
            except Exception as exc:
                logger.warning(
                    f"vision_evidence_fetch_failed "
                    f"doc_id={doc.get('document_id')} "
                    f"evidence={evidence_path} "
                    f"error_type={type(exc).__name__} "
                    f"error={str(exc)[:200]}"
                )
                continue
            if not pdf_bytes:
                continue

            extracted, conf = await extract_with_claude(pdf_bytes, "UNKNOWN")
            if not extracted:
                logger.info(
                    f"vision_classify_empty doc_id={doc.get('document_id')} "
                    f"evidence={evidence_path}"
                )
                continue

            new_type = extracted.get("document_type")
            new_los  = extracted.get("los_id")
            if new_type:
                doc["document_type"] = new_type
                # Re-derive category so the entity classifier + graph
                # downstream see a valid bucket.
                doc["category"] = doc.get("category") or "income"
            if new_los:
                doc["los_id"] = new_los
            doc["extracted_fields"] = {
                **(doc.get("extracted_fields") or {}),
                **{k: v for k, v in extracted.items()
                   if k not in ("document_type", "los_id")},
            }
            doc["extraction_method"]      = "ai_vision"
            doc["confidence_score"]       = conf
            doc["requires_classification"] = False
            stats["documents_classified"] += 1

            logger.info(
                f"vision_classified doc_id={doc.get('document_id')} "
                f"new_type={new_type or '?'} new_los_id={new_los or '?'} "
                f"fields_extracted={len(extracted)} "
                f"confidence={conf}"
            )

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
        ``borrower_role`` etc. v3 docs also carry ``source_document_id``
        + ``source_channel`` which thread through to the new
        ``document_index`` columns."""
        return {
            "document_id":         doc.get("document_id"),
            "applicant_id":        doc.get("applicant_id"),
            "application_id":      doc.get("application_id"),
            "document_type":       doc.get("document_type"),
            "document_category":   doc.get("category") or doc.get("document_category", "income"),
            "borrower_role":       doc.get("borrower_role", "primary"),
            "s3_key":              doc.get("s3_key"),
            "status":              "indexed",
            "extracted_fields":    doc.get("extracted_fields") or {},
            "confidence_score":    doc.get("confidence_score") or 0.94,
            "extraction_method":   doc.get("extraction_method") or "caller_supplied",
            "source_document_id":  doc.get("source_document_id"),
            "source_channel":      doc.get("source_channel"),
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
