"""AggregationService — central orchestrator for the EDMS pipeline.

Three event paths:
  A: APPLICATION_SUBMITTED  -> placeholder -> resolving -> assemble -> active
  B: DOCUMENT_UPLOADED      -> stale -> re-assemble -> active
  C: IDENTITY_RESOLVED      -> transition to active
"""
from datetime import datetime
from typing import Optional

import structlog

from core.aggregation.events import (
    ApplicationSubmittedEvent,
    BaseEvent,
    EventType,
)
from core.aggregation.status import GoldenRecordStatus, StatusMachine
from core.identity.resolver import IdentityResolver, IdentitySignals
from core.ingestion.events import ChannelType, NormalizedIngestEvent
from core.property.assembler import PropertyAssembler

logger = structlog.get_logger()


class AggregationService:
    def __init__(
        self,
        xref_store,
        golden_record_store,
        income_assembler,
        credit_assembler,
        redis_store,
        postgres_store,
        event_bus=None,
    ):
        self.xref_store = xref_store
        self.golden_record_store = golden_record_store
        self.resolver = IdentityResolver(xref_store, golden_record_store)
        self.income_assembler = income_assembler
        self.credit_assembler = credit_assembler
        self.property_assembler = PropertyAssembler()
        self.redis_store = redis_store
        self.postgres_store = postgres_store
        self.event_bus = event_bus
        self._published_events: list = []

    async def handle(self, event) -> dict:
        if isinstance(event, NormalizedIngestEvent):
            return await self._handle_normalized_ingest_event(event)

        handlers = {
            EventType.APPLICATION_SUBMITTED: self._handle_application_submitted,
            EventType.DOCUMENT_UPLOADED: self._handle_document_uploaded,
            EventType.IDENTITY_RESOLVED: self._handle_identity_resolved,
            EventType.PROPERTY_DOCUMENT_UPLOADED: self._handle_property_document_uploaded,
        }
        handler = handlers.get(event.event_type)
        if not handler:
            raise ValueError(f"No handler for: {event.event_type}")
        return await handler(event)

    async def _handle_normalized_ingest_event(
        self, event: NormalizedIngestEvent
    ) -> dict:
        if event.source_channel == ChannelType.API:
            signals = event.applicant_signals or {}
            extracted = event.extracted_fields or {}
            payload = {
                "los_id": signals.get("los_id") or extracted.get("los_id"),
                "borrower": {
                    "first_name": signals.get("first_name"),
                    "last_name": signals.get("last_name"),
                    "dob": signals.get("dob"),
                    "ssn_hash": signals.get("ssn_hash"),
                    "ssn_last4": signals.get("ssn_last4"),
                    "email": signals.get("email"),
                    "phone": signals.get("phone"),
                },
                "co_borrower": extracted.get("co_borrower"),
                "loan": extracted.get("loan", {}),
                "documents": extracted.get("documents", []),
            }
            inner = ApplicationSubmittedEvent(payload=payload)
            return await self._handle_application_submitted(inner)

        raise NotImplementedError(
            f"NormalizedIngestEvent handler for "
            f"{event.source_channel.value} not implemented yet"
        )

    async def _handle_application_submitted(self, event) -> dict:
        p = event.payload
        los_id = p["los_id"]
        log = logger.bind(los_id=los_id, handler="application_submitted")

        primary_signals = IdentitySignals(
            los_id=los_id,
            role="primary",
            first_name=p["borrower"]["first_name"],
            last_name=p["borrower"]["last_name"],
            dob=p["borrower"]["dob"],
            ssn_hash=p["borrower"].get("ssn_hash"),
            ssn_last4=p["borrower"].get("ssn_last4"),
            email=p["borrower"].get("email"),
        )
        primary_result = self.resolver.resolve(primary_signals)
        primary_gr = primary_result.golden_record

        # placeholder -> resolving -> active
        if primary_result.is_new_record:
            primary_gr.status = GoldenRecordStatus.PLACEHOLDER
            self.golden_record_store.save(primary_gr)

        if StatusMachine.can_transition(
            primary_gr.status, GoldenRecordStatus.RESOLVING
        ):
            primary_gr.status = GoldenRecordStatus.RESOLVING
            self.golden_record_store.save(primary_gr)

        co_applicant_id: Optional[str] = None
        if p.get("co_borrower"):
            co_signals = IdentitySignals(
                los_id=los_id,
                role="co_borrower",
                first_name=p["co_borrower"]["first_name"],
                last_name=p["co_borrower"]["last_name"],
                dob=p["co_borrower"]["dob"],
                ssn_hash=p["co_borrower"].get("ssn_hash"),
                ssn_last4=p["co_borrower"].get("ssn_last4"),
            )
            co_result = self.resolver.resolve(co_signals)
            co_applicant_id = co_result.golden_record.applicant_id
            await self.postgres_store.save_golden_record(co_result.golden_record.model_dump())
            for xref in co_result.golden_record.identity_xrefs:
                await self.postgres_store.save_xref(xref.model_dump())

        await self.postgres_store.save_golden_record(primary_gr.model_dump())
        for xref in primary_gr.identity_xrefs:
            await self.postgres_store.save_xref(xref.model_dump())

        application_id = f"APP-{los_id}"
        application = {
            "application_id": application_id,
            "applicant_id": primary_gr.applicant_id,
            "co_applicant_id": co_applicant_id,
            "los_id": los_id,
            "status": "active",
            "created_at": datetime.utcnow().isoformat(),
        }
        await self.postgres_store.save_application(application)

        await self._run_assembly(
            applicant_id=primary_gr.applicant_id,
            application_id=application_id,
            co_applicant_id=co_applicant_id,
            documents=p.get("documents", []),
            loan_data=p.get("loan", {}),
        )

        if StatusMachine.can_transition(
            primary_gr.status, GoldenRecordStatus.ACTIVE
        ):
            primary_gr.status = GoldenRecordStatus.ACTIVE
            self.golden_record_store.save(primary_gr)
        self.redis_store.set_status(primary_gr.applicant_id, "active")
        self.redis_store.set_app_lookup(
            los_id,
            {
                "application_id": application_id,
                "applicant_id": primary_gr.applicant_id,
                "co_applicant_id": co_applicant_id,
            },
        )

        self._publish(
            {
                "event_type": EventType.GOLDEN_RECORD_CREATED,
                "applicant_id": primary_gr.applicant_id,
                "application_id": application_id,
                "match_method": primary_result.match_method.value,
                "is_new_record": primary_result.is_new_record,
            }
        )
        log.info(
            "application_processed",
            applicant_id=primary_gr.applicant_id,
            match_method=primary_result.match_method.value,
        )

        return {
            "application_id": application_id,
            "applicant_id": primary_gr.applicant_id,
            "co_applicant_id": co_applicant_id,
            "status": primary_gr.status.value,
            "match_method": primary_result.match_method.value,
            "is_new_record": primary_result.is_new_record,
        }

    async def _handle_document_uploaded(self, event) -> dict:
        p = event.payload
        applicant_id = p["applicant_id"]
        gr = self.golden_record_store.find_by_applicant_id(applicant_id)
        if not gr:
            raise ValueError(f"No golden record for: {applicant_id}")

        if StatusMachine.can_transition(gr.status, GoldenRecordStatus.STALE):
            gr.status = StatusMachine.transition(
                gr.status, GoldenRecordStatus.STALE
            )
            self.golden_record_store.save(gr)
            self.redis_store.set_status(applicant_id, "stale")

        # Hydrate the application context so single-doc uploads still see the
        # full borrower picture. Without this:
        #   - co_applicant_id stayed None → co-borrower W2s were filed under
        #     the primary's applicant_id, and co-side income never assembled
        #   - documents was just this request's payload → assembling on a
        #     CREDIT_REPORT alone wiped primary qualifying back to $0
        application_id = p.get("application_id", "")
        co_applicant_id: Optional[str] = None
        loan_data: dict = {}
        if application_id:
            app = await self.postgres_store.get_application(application_id)
        else:
            app = await self.postgres_store.get_application_by_applicant(applicant_id)
        if app:
            application_id = application_id or app.get("application_id", "")
            co_applicant_id = app.get("co_applicant_id")
            loan_data = {
                "loan_amount":      app.get("loan_amount"),
                "interest_rate":    app.get("interest_rate"),
                "loan_term_months": app.get("loan_term_months"),
            }

        documents = await self._merge_request_with_indexed_docs(
            applicant_id=applicant_id,
            co_applicant_id=co_applicant_id,
            new_docs=p.get("all_documents", []),
        )

        await self._run_assembly(
            applicant_id=applicant_id,
            application_id=application_id,
            co_applicant_id=co_applicant_id,
            documents=documents,
            loan_data=loan_data,
        )

        # Belt-and-suspenders: _run_assembly already invalidates the context
        # cache when application_id is set, but if a caller skipped it, force
        # a re-assemble on the next GET /application/{id}/context. The income
        # / credit profiles we just wrote are otherwise hidden behind the
        # 30-min context TTL.
        if application_id:
            self.redis_store.invalidate_context(application_id)

        gr.status = StatusMachine.transition(
            GoldenRecordStatus.STALE, GoldenRecordStatus.ACTIVE
        )
        self.golden_record_store.save(gr)
        self.redis_store.set_status(applicant_id, "active")

        self._publish(
            {
                "event_type": EventType.PROFILE_UPDATED,
                "applicant_id": applicant_id,
                "trigger": "document_uploaded",
            }
        )
        return {
            "applicant_id": applicant_id,
            "status": "active",
            "trigger": "document_uploaded",
        }

    async def _handle_identity_resolved(self, event) -> dict:
        applicant_id = event.payload["applicant_id"]
        gr = self.golden_record_store.find_by_applicant_id(applicant_id)
        if gr and StatusMachine.can_transition(
            gr.status, GoldenRecordStatus.ACTIVE
        ):
            gr.status = GoldenRecordStatus.ACTIVE
            self.golden_record_store.save(gr)
            self.redis_store.set_status(applicant_id, "active")
        return {"applicant_id": applicant_id, "status": "active"}

    async def _run_assembly(
        self,
        applicant_id: str,
        application_id: str,
        co_applicant_id: Optional[str],
        documents: list,
        loan_data: dict,
    ):
        primary_docs = [
            d for d in documents if d.get("borrower_role") == "primary"
        ]
        co_docs = [
            d for d in documents if d.get("borrower_role") == "co_borrower"
        ]

        # Persist documents into document_index BEFORE running the credit /
        # income assemblers. The credit assembler reads CREDIT_REPORT field
        # values out of the Postgres ``extracted_fields`` jsonb column via
        # ``pg.get_documents_for_applicant`` — if we save after assembly
        # the just-arrived CREDIT_REPORT isn't visible to the assembler
        # and it falls back to a synthetic profile (the bug the live demo
        # was hitting). Reconciliation also runs here, which is fine: the
        # reconciler skips comparing a doc to itself by document_id.
        await self._persist_and_reconcile_documents(
            documents=documents,
            applicant_id=applicant_id,
            co_applicant_id=co_applicant_id,
            application_id=application_id,
        )

        # Credit reads from Postgres directly — see core/credit/assembler.py
        # for the ``extracted_fields``-only read pattern.
        primary_credit = await self.credit_assembler.assemble(
            applicant_id,
            loan_data,
            postgres_store=self.postgres_store,
        )
        co_credit = (
            await self.credit_assembler.assemble(
                co_applicant_id,
                loan_data,
                postgres_store=self.postgres_store,
            )
            if co_applicant_id
            else None
        )

        # Diagnostic: log what the assembler is about to see and what it returns.
        # If qualifying_monthly is $0, the doc shapes here are usually the cause
        # (e.g. box1_wages missing or buried under extracted_fields).
        logger.info(
            "income_assembly_inputs",
            applicant_id=applicant_id,
            application_id=application_id,
            primary_doc_count=len(primary_docs),
            co_doc_count=len(co_docs),
            primary_doc_types=[d.get("document_type") for d in primary_docs],
            primary_top_level_keys=[
                sorted(d.keys()) for d in primary_docs
            ],
        )
        profile = self.income_assembler.assemble(
            primary_docs=primary_docs,
            co_borrower_docs=co_docs,
            primary_credit=primary_credit,
            co_borrower_credit=co_credit,
            application_id=application_id,
            applicant_id=applicant_id,
            co_applicant_id=co_applicant_id,
        )
        logger.info(
            "income_assembly_result",
            applicant_id=applicant_id,
            primary_qualifying_monthly=profile.primary_borrower.get("qualifying_monthly"),
            co_qualifying_monthly=(profile.co_borrower or {}).get("qualifying_monthly") if profile.co_borrower else None,
            combined_qualifying_monthly=profile.combined_qualifying_monthly,
            primary_source_types=[
                s.get("source_type") for s in profile.primary_borrower.get("sources", [])
            ],
        )
        await self.postgres_store.save_income_profile(profile.model_dump())
        await self.postgres_store.save_credit_profile(primary_credit)
        if co_credit:
            await self.postgres_store.save_credit_profile(co_credit)
        self.redis_store.set_income_profile(applicant_id, profile.model_dump())
        self.redis_store.set_credit_profile(applicant_id, primary_credit)
        if co_credit:
            self.redis_store.set_credit_profile(co_applicant_id, co_credit)

        # The borrower layer just changed — drop the cached context so the
        # next GET /application/{id}/context re-assembles with fresh data.
        # (Document persistence + reconciliation already ran at the top of
        # this method so the credit assembler could read the new docs.)
        if application_id:
            self.redis_store.invalidate_context(application_id)
        else:
            try:
                app = await self.postgres_store.get_application_by_applicant(
                    applicant_id
                )
                if app:
                    self.redis_store.invalidate_context(app["application_id"])
            except Exception as exc:
                logger.warning("invalidate_context_failed", extra={"error": str(exc)})

    async def _merge_request_with_indexed_docs(
        self,
        applicant_id: str,
        co_applicant_id: Optional[str],
        new_docs: list,
    ) -> list:
        """Build the cumulative doc list the assembler needs.

        save_document stores the original incoming doc inside the
        ``extracted_fields`` jsonb column, so we lift those fields back to
        the top level on the way out — calculate_w2_salaried etc. read
        ``box1_wages`` directly off the doc dict.

        New docs in the request override existing rows on document_id so a
        re-upload with corrected fields wins over the stale indexed copy.
        """
        merged: dict = {}

        async def _load(aid: Optional[str]):
            if not aid:
                return
            try:
                rows = await self.postgres_store.get_documents_for_applicant(aid)
            except Exception as exc:
                logger.warning("hydrate_docs_failed", applicant_id=aid, error=str(exc))
                return
            for row in rows:
                fields = row.get("extracted_fields") or {}
                if isinstance(fields, str):
                    import json as _json
                    try:
                        fields = _json.loads(fields)
                    except Exception:
                        fields = {}
                doc_id = row.get("document_id")
                if not doc_id:
                    continue
                merged[doc_id] = {
                    **fields,
                    "document_id":       doc_id,
                    "document_type":     row.get("document_type"),
                    "document_category": row.get("document_category"),
                    "borrower_role":     row.get("borrower_role"),
                    "s3_key":            row.get("s3_key"),
                    "confidence_score":  row.get("confidence_score"),
                    "status":            row.get("status"),
                }

        await _load(applicant_id)
        await _load(co_applicant_id)

        for d in new_docs or []:
            doc_id = d.get("document_id")
            if doc_id:
                merged[doc_id] = d

        return list(merged.values())

    async def _persist_and_reconcile_documents(
        self,
        documents: list,
        applicant_id: str,
        co_applicant_id: Optional[str],
        application_id: str,
    ):
        if not documents:
            return
        from core.graph.reconciler import DocumentReconciler

        reconciler = DocumentReconciler(self.postgres_store)

        # Pre-fetch the other-borrower's current docs so the reconciler can
        # emit cross-applicant edges (joint applications). Without this the
        # primary's W2 and the co-borrower's W2 — under different applicant
        # ids since the 3c631b7 attribution fix — never get compared.
        other_docs_for_primary: list = []
        other_docs_for_co: list = []
        if co_applicant_id:
            try:
                other_docs_for_primary = await self.postgres_store.get_documents_for_applicant(
                    co_applicant_id
                )
            except Exception as exc:
                logger.warning("hydrate_co_docs_failed", extra={"error": str(exc)})
            try:
                other_docs_for_co = await self.postgres_store.get_documents_for_applicant(
                    applicant_id
                )
            except Exception as exc:
                logger.warning("hydrate_primary_docs_failed", extra={"error": str(exc)})

        # Columns on the document_index row — anything else on the incoming
        # doc dict is content that belongs in the extracted_fields jsonb.
        _METADATA_KEYS = {
            "document_id", "applicant_id", "application_id",
            "document_type", "document_category", "borrower_role",
            "s3_key", "status", "expiry_date", "is_current",
            "extracted_fields", "confidence_score",
        }

        for d in documents:
            role = d.get("borrower_role", "primary")
            doc_applicant = (
                co_applicant_id if (role == "co_borrower" and co_applicant_id) else applicant_id
            )

            # Resolve extracted_fields:
            #   1. If caller nested them under "extracted_fields", use that
            #      directly (and don't double-nest by storing the whole d).
            #   2. Otherwise treat top-level non-metadata keys as the
            #      extracted content (the demo / typical caller spread
            #      fields at the top level).
            nested = d.get("extracted_fields")
            if isinstance(nested, dict) and nested:
                extracted_fields = nested
            elif isinstance(nested, str) and nested:
                extracted_fields = nested  # asyncpg will store the JSON string
            else:
                extracted_fields = {
                    k: v for k, v in d.items() if k not in _METADATA_KEYS
                }

            # Status: "indexed" when we actually have extracted content;
            # "received" when the row is just a placeholder waiting on
            # extraction. Caller can override either way.
            has_fields = bool(
                extracted_fields if isinstance(extracted_fields, dict)
                else extracted_fields  # truthy string passes
            )
            status = d.get(
                "status",
                "indexed" if has_fields else "received",
            )

            saved_doc = {
                "document_id":       d.get("document_id"),
                "applicant_id":      doc_applicant,
                "application_id":    application_id,
                "document_type":     d.get("document_type", "UNKNOWN"),
                "document_category": d.get("document_category", "income"),
                "borrower_role":     role,
                "s3_key":            d.get("s3_key"),
                "status":            status,
                "is_current":        True,
                "extracted_fields":  extracted_fields,
                "confidence_score":  d.get("confidence_score", 0.95),
            }
            try:
                await self.postgres_store.save_document(saved_doc)
            except Exception as exc:
                logger.warning("save_document_failed", extra={"error": str(exc)})
                continue
            logger.info(
                "document_persisted",
                document_id=saved_doc["document_id"],
                document_type=saved_doc["document_type"],
                status=saved_doc["status"],
                extracted_field_count=(
                    len(extracted_fields) if isinstance(extracted_fields, dict) else 0
                ),
            )
            also_compare_with = (
                other_docs_for_co if doc_applicant == co_applicant_id
                else other_docs_for_primary
            )
            try:
                new_rels = await reconciler.reconcile(
                    doc_applicant, saved_doc,
                    also_compare_with=also_compare_with,
                )
            except Exception as exc:
                logger.warning("reconciler_failed", extra={"error": str(exc)})
                continue
            conflicts = [r for r in new_rels if r.relationship_type.value == "contradicts"]
            if conflicts:
                logger.warning(
                    "document_graph_conflict",
                    applicant_id=doc_applicant,
                    conflict_count=len(conflicts),
                    conflicts=[r.reasoning for r in conflicts],
                )
                self.redis_store.invalidate_income_profile(doc_applicant)

        # Always bust the graph cache after persisting docs — even without
        # conflicts. Otherwise /graph/summary keeps returning a stale
        # document_count from before the inserts. Use a graph-only invalidate
        # so we don't blow away the income/credit caches _run_assembly just
        # warmed (invalidate_income_profile would clobber them).
        self.redis_store.invalidate_graph_summary(applicant_id)
        if co_applicant_id:
            self.redis_store.invalidate_graph_summary(co_applicant_id)

    async def _handle_property_document_uploaded(self, event) -> dict:
        """Re-assemble a PropertyProfile after a new property doc lands.

        Loads every property doc for the given property_id, runs
        PropertyAssembler, persists the new versioned profile, and warms
        ``property:{id}`` while invalidating ``context:{application_id}``.
        """
        p = event.payload
        property_id = p["property_id"]
        log = logger.bind(property_id=property_id, handler="property_doc_uploaded")

        prop = await self.postgres_store.get_property(property_id)
        if not prop:
            raise ValueError(f"No property for: {property_id}")
        application_id = prop.get("application_id") or p.get("application_id") or ""

        property_docs = p.get("property_docs")
        if property_docs is None:
            property_docs = await self.postgres_store.get_property_docs(
                property_id
            )

        loan_data = p.get("loan_data") or {}
        if application_id and not loan_data:
            try:
                app = await self.postgres_store.get_application_by_los_id(
                    application_id
                )
            except Exception:
                app = None
            if app:
                loan_data = {
                    "loan_amount":      app.get("loan_amount"),
                    "interest_rate":    app.get("interest_rate"),
                    "loan_term_months": app.get("loan_term_months"),
                }

        profile = self.property_assembler.assemble(
            property_docs=property_docs or [],
            loan_data=loan_data,
            property_id=property_id,
            application_id=application_id,
        )
        profile_dict = profile.model_dump()

        await self.postgres_store.save_property_profile(profile_dict)
        self.redis_store.set_property_profile(property_id, profile_dict)
        if application_id:
            self.redis_store.invalidate_context(application_id)

        piti_total = (profile.piti_components.total_piti
                      if profile.piti_components else None)
        self._publish(
            {
                "event_type": EventType.PROFILE_UPDATED,
                "property_id": property_id,
                "application_id": application_id,
                "trigger": "property_document_uploaded",
                "piti_total": piti_total,
            }
        )
        log.info(
            "property_profile_updated",
            property_id=property_id,
            piti_total=piti_total,
        )
        return {
            "property_id":    property_id,
            "application_id": application_id,
            "profile":        profile_dict,
            "piti_total":     piti_total,
        }

    def _publish(self, event_data: dict):
        # Convert enum to value for JSON-friendliness in tests/event sinks.
        if isinstance(event_data.get("event_type"), EventType):
            event_data = {**event_data, "event_type": event_data["event_type"].value}
        self._published_events.append(event_data)
        if self.event_bus:
            self.event_bus.publish(event_data)

    def get_published_events(self) -> list:
        return self._published_events
