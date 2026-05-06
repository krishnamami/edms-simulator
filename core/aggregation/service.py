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

        await self._run_assembly(
            applicant_id=applicant_id,
            application_id=p.get("application_id", ""),
            co_applicant_id=None,
            documents=p.get("all_documents", []),
            loan_data={},
        )

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

        primary_credit = self.credit_assembler.generate_synthetic(
            applicant_id, loan_data
        )
        co_credit = (
            self.credit_assembler.generate_synthetic(
                co_applicant_id, loan_data
            )
            if co_applicant_id
            else None
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

        # Persist documents into document_index, then reconcile each one
        # against existing docs for the same applicant. The reconciler writes
        # typed graph edges (confirms / corroborates / contradicts).
        await self._persist_and_reconcile_documents(
            documents=documents,
            applicant_id=applicant_id,
            co_applicant_id=co_applicant_id,
            application_id=application_id,
        )

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
        for d in documents:
            role = d.get("borrower_role", "primary")
            doc_applicant = (
                co_applicant_id if (role == "co_borrower" and co_applicant_id) else applicant_id
            )
            saved_doc = {
                "document_id":       d.get("document_id"),
                "applicant_id":      doc_applicant,
                "application_id":    application_id,
                "document_type":     d.get("document_type", "UNKNOWN"),
                "document_category": d.get("document_category", "income"),
                "borrower_role":     role,
                "s3_key":            d.get("s3_key"),
                "status":            d.get("status", "received"),
                "is_current":        True,
                # Treat the incoming doc payload itself as extracted_fields —
                # it carries box1_wages / employer_name / etc. at top level.
                "extracted_fields":  d,
                "confidence_score":  d.get("confidence_score", 0.95),
            }
            try:
                await self.postgres_store.save_document(saved_doc)
            except Exception as exc:
                logger.warning("save_document_failed", extra={"error": str(exc)})
                continue
            try:
                new_rels = await reconciler.reconcile(doc_applicant, saved_doc)
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
