"""Shared pytest fixtures.

Sets environment to drive Redis/secrets into local fakes BEFORE the
core modules are imported.
"""
import os

os.environ.setdefault("USE_FAKE_REDIS", "true")
os.environ.setdefault("USE_AWS_SECRETS", "false")
os.environ.setdefault("USE_AWS_SQS", "false")
os.environ.setdefault("USE_LOCAL_STORAGE", "true")
os.environ.setdefault("API_KEY", "test_key")

import pytest  # noqa: E402

from core.aggregation.service import AggregationService  # noqa: E402
from core.credit.assembler import CreditAssembler  # noqa: E402
from core.identity.xref_store import XRefStore  # noqa: E402
from core.income.assembler import IncomeAssembler  # noqa: E402
from core.storage.redis_store import RedisStore  # noqa: E402


class FakePostgresStore:
    """In-memory stand-in for PostgresStore — for unit tests only."""

    def __init__(self):
        self.applicants: dict = {}
        self.applications: dict = {}
        self.income_profiles: dict = {}
        self.credit_profiles: dict = {}
        self.documents: list = []
        self.xrefs: list = []
        self.relationships: list = []
        self.properties: dict = {}
        self.property_profiles: dict = {}
        self.webhooks: dict = {}
        self.webhook_deliveries: list = []
        self.context_versions: list = []
        self.watermarks: dict = {}
        self.indexing_runs: dict = {}

    async def save_golden_record(self, gr, tenant_id="default"):
        # Round-trip storage so get_all_applicants / find_by_external_id can
        # observe records written through the regular pipeline.
        gr = {**gr, "tenant_id": tenant_id}
        self.applicants[gr["applicant_id"]] = gr
    async def find_by_applicant_id(self, applicant_id, tenant_id="default"): return None
    async def find_by_ssn_hash(self, ssn_hash, tenant_id="default"): return None
    async def find_by_name_dob(self, last_name, dob, tenant_id="default"): return []
    async def update_status(self, applicant_id, status): pass
    async def next_sequence(self): return 1

    async def save_xref(self, xref):
        self.xrefs.append(xref)

    async def save_application(self, app, tenant_id="default"):
        self.applications[app["application_id"]] = {**app, "tenant_id": tenant_id}

    async def get_application_by_los_id(self, los_id, tenant_id="default"):
        for app in self.applications.values():
            if app["los_id"] == los_id and app.get("tenant_id", "default") == tenant_id:
                return app
        return None

    async def create_application_from_event(self, event, tenant_id="default"):
        """In-memory mirror of the PG helper. Idempotent on los_id."""
        los_id = event["los_id"]
        existing = await self.get_application_by_los_id(los_id, tenant_id=tenant_id)
        if existing:
            return {
                "application_id":  existing["application_id"],
                "applicant_id":    existing["applicant_id"],
                "co_applicant_id": existing.get("co_applicant_id"),
                "los_id":          los_id,
            }
        seq = await self.next_sequence()
        applicant_id = f"APL-{seq:05d}-P"
        b = event["borrower"]
        await self.save_golden_record({
            "applicant_id": applicant_id, "full_name": f"{b['first_name']} {b['last_name']}",
            "first_name": b["first_name"], "last_name": b["last_name"],
            "dob": b["dob"],
            "ssn_hash": b.get("ssn_hash") or f"hash-{b.get('ssn_last4', '0000')}",
            "ssn_last4": b.get("ssn_last4"), "email": b.get("email"),
            "status": "active",
        }, tenant_id=tenant_id)
        co_applicant_id = None
        if event.get("co_borrower"):
            co_seq = await self.next_sequence()
            co_applicant_id = f"APL-{co_seq:05d}-P"
            cb = event["co_borrower"]
            await self.save_golden_record({
                "applicant_id": co_applicant_id,
                "full_name": f"{cb['first_name']} {cb['last_name']}",
                "first_name": cb["first_name"], "last_name": cb["last_name"],
                "dob": cb["dob"],
                "ssn_hash": cb.get("ssn_hash") or f"hash-{cb.get('ssn_last4', '0000')}",
                "ssn_last4": cb.get("ssn_last4"), "email": cb.get("email"),
                "status": "active",
            }, tenant_id=tenant_id)
        application_id = f"APP-{los_id}"
        await self.save_application({
            "application_id": application_id, "applicant_id": applicant_id,
            "co_applicant_id": co_applicant_id, "los_id": los_id,
            "status": "active",
        }, tenant_id=tenant_id)
        return {
            "application_id": application_id, "applicant_id": applicant_id,
            "co_applicant_id": co_applicant_id, "los_id": los_id,
        }

    async def get_application(self, application_id, tenant_id="default"):
        app = self.applications.get(application_id)
        if app and app.get("tenant_id", "default") != tenant_id:
            return None
        return app

    async def get_all_applications(self, limit=50, tenant_id="default"):
        rows = [a for a in self.applications.values()
                if a.get("tenant_id", "default") == tenant_id]
        return rows[:limit]

    async def get_raw_ingestion_for_application(self, application_id):
        # FakePostgresStore doesn't model raw_ingestion at all today —
        # callers handle the empty list path.
        return []

    async def get_application_by_applicant(self, applicant_id, tenant_id="default"):
        for app in self.applications.values():
            if (app.get("applicant_id") == applicant_id
                    or app.get("co_applicant_id") == applicant_id) \
                    and app.get("tenant_id", "default") == tenant_id:
                return app
        return None

    async def update_application_loan_data(self, application_id, loan_data):
        await self.update_application_loan_fields(application_id, loan_data)

    async def save_income_profile(self, profile, tenant_id="default"):
        applicant_id = profile["applicant_id"]
        prior = self.income_profiles.get(applicant_id)
        version = (prior.get("_version", 0) + 1) if prior else 1
        profile = {**profile, "_version": version, "tenant_id": tenant_id}
        self.income_profiles[applicant_id] = profile
        return f"profile-{applicant_id}-{version}"

    async def get_income_profile(self, applicant_id, tenant_id="default"):
        row = self.income_profiles.get(applicant_id)
        if row and row.get("tenant_id", "default") != tenant_id:
            return None
        return row

    async def save_credit_profile(self, profile, tenant_id="default"):
        self.credit_profiles[profile["applicant_id"]] = {**profile, "tenant_id": tenant_id}

    async def get_credit_profile(self, applicant_id, tenant_id="default"):
        row = self.credit_profiles.get(applicant_id)
        if row and row.get("tenant_id", "default") != tenant_id:
            return None
        return row

    async def save_document(self, doc, tenant_id="default"):
        # Mirror production's ON CONFLICT (document_id) DO UPDATE so the
        # batch indexer's save → assembler save flow doesn't double-row.
        doc = {**doc, "tenant_id": tenant_id}
        for i, existing in enumerate(self.documents):
            if existing.get("document_id") == doc.get("document_id"):
                self.documents[i] = doc
                return
        self.documents.append(doc)

    async def get_document(self, document_id):
        for d in self.documents:
            if d.get("document_id") == document_id:
                return d
        return None

    async def get_documents_for_applicant(self, applicant_id, tenant_id="default"):
        return [d for d in self.documents
                if d["applicant_id"] == applicant_id
                and d.get("tenant_id", "default") == tenant_id]

    async def get_documents_for_application(self, application_id, tenant_id="default"):
        return [d for d in self.documents
                if d.get("application_id") == application_id
                and d.get("tenant_id", "default") == tenant_id]

    async def get_documents_by_app_and_category(
        self, application_id, category, tenant_id="default",
    ):
        # Walk applications first to know primary + co; the FakePG fan-out
        # mirrors the real SQL JOIN through ``applications``.
        app = self.applications.get(application_id)
        primary = (app or {}).get("applicant_id")
        co      = (app or {}).get("co_applicant_id")
        return [
            d for d in self.documents
            if d.get("tenant_id", "default") == tenant_id
            and d.get("document_category") == category
            and d.get("is_current", True)
            and (d.get("application_id") == application_id
                 or d.get("applicant_id") in (primary, co))
        ]

    async def get_documents_by_types(
        self, applicant_id, doc_types, tenant_id="default",
    ):
        types = set(doc_types or [])
        if not types:
            return []
        return [
            d for d in self.documents
            if d.get("applicant_id") == applicant_id
            and d.get("document_type") in types
            and d.get("tenant_id", "default") == tenant_id
            and d.get("is_current", True)
        ]

    async def get_documents_for_application_by_types(
        self, application_id, doc_types, tenant_id="default",
    ):
        types = set(doc_types or [])
        if not types:
            return []
        app = self.applications.get(application_id)
        primary = (app or {}).get("applicant_id")
        co      = (app or {}).get("co_applicant_id")
        return [
            d for d in self.documents
            if d.get("tenant_id", "default") == tenant_id
            and d.get("document_type") in types
            and d.get("is_current", True)
            and (d.get("application_id") == application_id
                 or d.get("applicant_id") in (primary, co))
        ]

    async def upsert_entity_state(
        self, application_id, state_data=None, tenant_id="default",
    ):
        """v4 — keyed by ``application_id``, ``state_data`` is a dict
        whose keys map 1:1 to the entity_states columns. Mirrors the
        PG-side ``legacy_ids`` JSONB merge."""
        from datetime import datetime, timezone
        if not hasattr(self, "_entity_states"):
            self._entity_states = {}
        prior = self._entity_states.get(application_id, {})
        s = state_data or {}
        merged_legacy = dict(prior.get("legacy_ids") or {})
        merged_legacy.update(s.get("legacy_ids") or {})
        self._entity_states[application_id] = {
            **prior,
            "application_id":   application_id,
            "tenant_id":        tenant_id,
            "los_id":           s.get("los_id") or prior.get("los_id"),
            "legacy_ids":       merged_legacy,
            "borrower":         s.get("borrower")     or prior.get("borrower")     or {},
            "co_borrowers":     s.get("co_borrowers") or prior.get("co_borrowers") or [],
            "property":         s.get("property")     or prior.get("property")     or {},
            "loan_terms":       s.get("loan_terms")   or prior.get("loan_terms")   or {},
            "verifications":    s.get("verifications") or prior.get("verifications") or {},
            "mid_credit_score":              s.get("mid_credit_score"),
            "qualifying_monthly":            s.get("qualifying_monthly"),
            "co_borrower_qualifying_monthly": s.get("co_borrower_qualifying_monthly"),
            "combined_monthly_income":       s.get("combined_monthly_income"),
            "total_liquid_assets":           s.get("total_liquid_assets"),
            "appraised_value":               s.get("appraised_value"),
            "purchase_price":                s.get("purchase_price"),
            "loan_amount":                   s.get("loan_amount"),
            "interest_rate":                 s.get("interest_rate"),
            "ltv":                           s.get("ltv"),
            "dti_front":                     s.get("dti_front"),
            "dti_back":                      s.get("dti_back"),
            "piti_monthly":                  s.get("piti_monthly"),
            "monthly_obligations":           s.get("monthly_obligations"),
            "document_count":                int(s.get("document_count") or 0),
            "graph_edge_count":              int(s.get("graph_edge_count") or 0),
            "conflict_count":                int(s.get("conflict_count") or 0),
            "critical_conflict_count":       int(s.get("critical_conflict_count") or 0),
            "completeness_pct":              float(s.get("completeness_pct") or 0.0),
            "status":                        s.get("status") or "application_received",
            "income_verified":               bool(s.get("income_verified")),
            "employment_verified":           bool(s.get("employment_verified")),
            "credit_pulled":                 bool(s.get("credit_pulled")),
            "assets_verified":               bool(s.get("assets_verified")),
            "identity_complete":             bool(s.get("identity_complete")),
            "appraisal_complete":            bool(s.get("appraisal_complete")),
            "title_clear":                   bool(s.get("title_clear")),
            "insurance_bound":               bool(s.get("insurance_bound")),
            "aus_approved":                  bool(s.get("aus_approved")),
            "rate_locked":                   bool(s.get("rate_locked")),
            "conditions_cleared":            bool(s.get("conditions_cleared")),
            "clear_to_close":                bool(s.get("clear_to_close")),
            "days_in_current_status":        (int(s["days_in_current_status"])
                                              if s.get("days_in_current_status") is not None
                                              else None),
            "loan_age_days":                 (int(s["loan_age_days"])
                                              if s.get("loan_age_days") is not None
                                              else None),
            "last_updated":                  datetime.now(timezone.utc),
        }

    async def get_documents_for_application(self, application_id, tenant_id="default"):
        return [d for d in self.documents
                if d.get("application_id") == application_id
                and d.get("tenant_id", "default") == tenant_id]

    async def get_applicants_for_application(self, application_id, tenant_id="default"):
        app = await self.get_application(application_id, tenant_id=tenant_id)
        if not app:
            return []
        out = []
        for aid, role in [(app.get("applicant_id"), "primary"),
                          (app.get("co_applicant_id"), "co_borrower")]:
            if not aid:
                continue
            applicant = self.applicants.get(aid)
            if applicant:
                out.append({**applicant, "role": role})
        return out

    async def count_docs_for_application(self, application_id, tenant_id="default"):
        return len(await self.get_documents_for_application(application_id, tenant_id))

    async def count_edges_for_application(self, application_id, tenant_id="default"):
        return len([r for r in self.relationships
                    if r.get("application_id") == application_id])

    async def count_conflicts_for_application(
        self, application_id, tenant_id="default", critical_only=False,
    ):
        return len([
            r for r in self.relationships
            if r.get("application_id") == application_id
            and r.get("relationship_type") == "contradicts"
        ])

    async def update_application_verified_fields(
        self, application_id, fields, tenant_id="default",
    ):
        app = self.applications.get(application_id)
        if not app:
            return
        for k in ("verified_income", "verified_property_value",
                  "verified_assets", "verified_employer"):
            if k in fields:
                app[k] = fields[k]

    async def update_application_stated_fields(
        self, application_id, fields, tenant_id="default",
    ):
        app = self.applications.get(application_id)
        if not app:
            return
        for k in ("stated_income", "stated_property_value",
                  "stated_assets", "stated_employer"):
            if k in fields:
                app[k] = fields[k]

    async def log_entity_state_event(self, application_id, event_type,
                                     field_path=None, old_value=None,
                                     new_value=None, triggered_by=None,
                                     document_id=None, tenant_id="default"):
        if not hasattr(self, "_entity_state_events"):
            self._entity_state_events = []
        from datetime import datetime, timezone
        self._entity_state_events.append({
            "application_id": application_id,
            "event_type":     event_type,
            "field_path":     field_path,
            "old_value":      old_value,
            "new_value":      new_value,
            "triggered_by":   triggered_by,
            "document_id":    document_id,
            "created_at":     datetime.now(timezone.utc),
        })

    async def get_entity_state_events(self, application_id, limit=50,
                                      tenant_id="default"):
        evs = getattr(self, "_entity_state_events", [])
        return [e for e in evs if e["application_id"] == application_id][:limit]

    async def get_entity_state(self, application_id, tenant_id="default"):
        row = getattr(self, "_entity_states", {}).get(application_id)
        if row and row.get("tenant_id", "default") != tenant_id:
            return None
        return row

    async def count_edges_for_entity(self, applicant_id, tenant_id="default"):
        return len([r for r in self.relationships
                    if r.get("applicant_id") == applicant_id
                    and r.get("tenant_id", "default") == tenant_id])

    async def count_conflicts_for_entity(self, applicant_id, tenant_id="default"):
        return len([r for r in self.relationships
                    if r.get("applicant_id") == applicant_id
                    and r.get("relationship_type") == "contradicts"
                    and r.get("tenant_id", "default") == tenant_id])

    async def get_all_applicants(self):
        return list(self.applicants.values())

    # graph methods
    async def save_relationship(self, rel, tenant_id="default"):
        self.relationships.append({**rel, "tenant_id": tenant_id})

    async def get_relationships_for_applicant(self, applicant_id, tenant_id="default"):
        return [r for r in self.relationships
                if r["applicant_id"] == applicant_id
                and r.get("tenant_id", "default") == tenant_id]

    async def get_conflicts_for_applicant(self, applicant_id, tenant_id="default"):
        return [
            r for r in self.relationships
            if r["applicant_id"] == applicant_id
            and r["relationship_type"] == "contradicts"
            and r.get("tenant_id", "default") == tenant_id
        ]

    async def get_graph_summary(self, applicant_id, tenant_id="default"):
        docs = await self.get_documents_for_applicant(applicant_id, tenant_id)
        rels = await self.get_relationships_for_applicant(applicant_id, tenant_id)
        conflicts = [r for r in rels if r["relationship_type"] == "contradicts"]
        confirms  = [r for r in rels if r["relationship_type"] == "confirms"]
        return {
            "applicant_id":       applicant_id,
            "document_count":     len(docs),
            "relationship_count": len(rels),
            "confirmation_count": len(confirms),
            "conflict_count":     len(conflicts),
            "requires_review":    len(conflicts) > 0,
        }

    # external IDs / LOS integration
    async def find_by_external_id(self, source_system, external_id):
        for a in self.applicants.values():
            ext = a.get("external_ids") or {}
            if ext.get(source_system) == external_id:
                return a
        return None

    async def add_external_id(self, applicant_id, source_system, external_id):
        a = self.applicants.get(applicant_id)
        if a is None:
            return
        ext = a.get("external_ids") or {}
        ext[source_system] = external_id
        a["external_ids"] = ext

    async def get_application_by_external_loan_id(self, external_loan_id):
        for app in self.applications.values():
            if app.get("external_loan_id") == external_loan_id:
                return app
        return None

    async def update_application_loan_fields(self, application_id, loan_data):
        app = self.applications.get(application_id)
        if app is None:
            return
        for k in (
            "loan_amount", "interest_rate", "loan_term_months",
            "loan_purpose", "loan_type", "occupancy", "external_loan_id",
            "urla_fields",
        ):
            if loan_data.get(k) is not None:
                app[k] = loan_data[k]

    # property layer
    async def save_property(self, prop, tenant_id="default"):
        self.properties[prop["property_id"]] = {**dict(prop), "tenant_id": tenant_id}
        return prop["property_id"]

    async def get_property(self, property_id, tenant_id="default"):
        row = self.properties.get(property_id)
        if row and row.get("tenant_id", "default") != tenant_id:
            return None
        return row

    async def get_property_by_application(self, application_id, tenant_id="default"):
        for p in self.properties.values():
            if p.get("application_id") == application_id \
                    and p.get("tenant_id", "default") == tenant_id:
                return p
        return None

    async def save_property_profile(self, profile, tenant_id="default"):
        property_id = profile["property_id"]
        prior = self.property_profiles.get(property_id)
        version = (prior.get("_version", 0) + 1) if prior else 1
        self.property_profiles[property_id] = {
            **profile, "_version": version, "tenant_id": tenant_id,
        }
        return f"profile-{property_id}-{version}"

    async def get_property_profile(self, property_id, tenant_id="default"):
        row = self.property_profiles.get(property_id)
        if row and row.get("tenant_id", "default") != tenant_id:
            return None
        return row

    async def get_property_docs(self, property_id, tenant_id="default"):
        prop = self.properties.get(property_id)
        if not prop:
            return []
        application_id = prop.get("application_id")
        return [
            d for d in self.documents
            if d.get("application_id") == application_id
            and d.get("document_category") == "property"
            and d.get("tenant_id", "default") == tenant_id
        ]

    async def update_application_property(self, application_id, property_id):
        app = self.applications.get(application_id)
        if app is not None:
            app["property_id"] = property_id

    # Phase E — webhooks + context versioning
    async def get_active_webhooks(self, event_type):
        return [
            w for w in self.webhooks.values()
            if w.get("is_active", True)
            and event_type in (w.get("events") or ["context_updated"])
        ]

    async def list_webhooks(self):
        return list(self.webhooks.values())

    async def get_webhook(self, webhook_id):
        return self.webhooks.get(str(webhook_id))

    async def save_webhook(self, webhook):
        import uuid as _uuid
        new_id = str(_uuid.uuid4())
        self.webhooks[new_id] = {
            "webhook_id":     new_id,
            "name":           webhook["name"],
            "url":            webhook["url"],
            "secret":         webhook.get("secret"),
            "events":         webhook.get("events") or ["context_updated"],
            "is_active":      webhook.get("is_active", True),
            "failure_count":  0,
        }
        return new_id

    async def deactivate_webhook(self, webhook_id):
        wh = self.webhooks.get(str(webhook_id))
        if wh is not None:
            wh["is_active"] = False

    async def save_webhook_delivery(self, delivery):
        self.webhook_deliveries.append(dict(delivery))

    async def get_webhook_deliveries(self, webhook_id, limit=50):
        rows = [
            d for d in self.webhook_deliveries
            if str(d.get("webhook_id")) == str(webhook_id)
        ]
        return rows[-limit:][::-1]

    async def increment_webhook_failures(self, webhook_id):
        wh = self.webhooks.get(str(webhook_id))
        if wh is not None:
            wh["failure_count"] = (wh.get("failure_count") or 0) + 1

    async def save_context_version(self, version):
        import uuid as _uuid
        row = {
            "version_id":     str(_uuid.uuid4()),
            "application_id": version["application_id"],
            "context_data":   version["context_data"],
            "assembled_at":   version["assembled_at"],
            "trigger_event":  version.get("trigger_event"),
            "trigger_doc_id": version.get("trigger_doc_id"),
        }
        self.context_versions.append(row)
        return row["version_id"]

    async def get_context_versions(self, application_id, limit=10):
        rows = [
            v for v in self.context_versions
            if v["application_id"] == application_id
        ]
        rows.sort(key=lambda r: r.get("assembled_at") or "", reverse=True)
        return rows[:limit]

    async def get_context_at(self, application_id, timestamp):
        rows = [
            v for v in self.context_versions
            if v["application_id"] == application_id
            and (v.get("assembled_at") or "") <= timestamp
        ]
        if not rows:
            return None
        rows.sort(key=lambda r: r.get("assembled_at") or "", reverse=True)
        return rows[0]

    # incremental indexer
    async def get_watermark(self, source):
        return self.watermarks.get(source)

    async def upsert_watermark_status(self, source, status):
        wm = self.watermarks.setdefault(
            source, {"source": source, "last_indexed_at": None}
        )
        wm["status"] = status

    async def upsert_watermark_complete(
        self, source, last_indexed_at,
        files_processed, files_skipped, errors,
        run_duration_ms=None,
    ):
        status = "failed" if errors and not files_processed else "complete"
        self.watermarks[source] = {
            "source":          source,
            "last_indexed_at": last_indexed_at,
            "last_run_at":     last_indexed_at,
            "files_processed": files_processed,
            "files_skipped":   files_skipped,
            "errors":          errors,
            "status":          status,
            "run_duration_ms": run_duration_ms,
        }

    async def set_watermark_timestamp(self, source, last_indexed_at):
        wm = self.watermarks.setdefault(source, {"source": source})
        wm["last_indexed_at"] = last_indexed_at

    async def create_indexing_run(self, source, watermark_from, watermark_to):
        import uuid as _uuid
        run_id = str(_uuid.uuid4())
        self.indexing_runs[run_id] = {
            "run_id":         run_id,
            "source":         source,
            "watermark_from": watermark_from,
            "watermark_to":   watermark_to,
            "started_at":     watermark_to,
            "status":         "running",
        }
        return run_id

    async def complete_indexing_run(self, run_id, stats):
        run = self.indexing_runs.get(run_id)
        if run is None:
            return
        errors = int(stats.get("errors") or 0)
        run.update({
            "completed_at":        True,
            "files_found":         stats.get("found", 0),
            "files_processed":     stats.get("processed", 0),
            "files_skipped":       stats.get("skipped", 0),
            "applicants_affected": stats.get("applicants_affected", 0),
            "errors":              errors,
            "error_details":       stats.get("error_details") or [],
            "status": (
                "complete_with_errors"
                if errors and (stats.get("processed") or 0) > 0
                else ("failed" if errors else "complete")
            ),
        })

    async def get_indexing_runs(self, source=None, limit=50):
        rows = list(self.indexing_runs.values())
        if source:
            rows = [r for r in rows if r["source"] == source]
        return rows[:limit]

    async def get_indexing_run(self, run_id):
        return self.indexing_runs.get(str(run_id))

    async def get_table_count(self, table_name):
        # Best-effort fake — map table_name to the in-memory store length.
        mapping = {
            "applicants":              len(self.applicants),
            "applicant_identity_xref": len(self.xrefs),
            "applications":            len(self.applications),
            "income_profiles":         len(self.income_profiles),
            "credit_profiles":         len(self.credit_profiles),
            "document_index":          len(self.documents),
            "document_relationships":  len(self.relationships),
            "properties":              len(self.properties),
            "property_profiles":       len(self.property_profiles),
            "raw_ingestion":           0,
            "context_versions":        len(self.context_versions),
            "indexing_watermarks":     len(self.watermarks),
            "indexing_runs":           len(self.indexing_runs),
            "webhooks":                len(self.webhooks),
            "webhook_deliveries":      len(self.webhook_deliveries),
            "export_watermarks":       len(getattr(self, "export_watermarks", {})),
        }
        return mapping.get(table_name, 0)

    # Webhook outbox stubs — async fan-out path. Tests that exercise
    # the publisher assert against ``self.outbox`` directly; the worker
    # is exercised by integration tests with real Postgres.

    async def insert_outbox(self, webhook_id, event_type, payload,
                             application_id=None, tenant_id="default",
                             max_attempts=3):
        if not hasattr(self, "outbox"):
            self.outbox = []
        import uuid as _uuid
        from datetime import datetime, timezone
        row = {
            "id":             str(_uuid.uuid4()),
            "tenant_id":      tenant_id,
            "webhook_id":     str(webhook_id),
            "event_type":     event_type,
            "application_id": application_id,
            "payload":        payload,
            "status":         "pending",
            "attempts":       0,
            "max_attempts":   max_attempts,
            "next_retry_at":  datetime.now(timezone.utc),
            "last_error":     None,
            "created_at":     datetime.now(timezone.utc),
            "delivered_at":   None,
        }
        self.outbox.append(row)
        return row["id"]

    async def get_pending_outbox(self, limit=50):
        from datetime import datetime, timezone
        rows = [
            r for r in getattr(self, "outbox", [])
            if r["status"] == "pending"
            and r["next_retry_at"] <= datetime.now(timezone.utc)
        ]
        rows.sort(key=lambda r: r["created_at"])
        return rows[:limit]

    async def mark_outbox_delivered(self, outbox_id):
        from datetime import datetime, timezone
        for r in getattr(self, "outbox", []):
            if r["id"] == str(outbox_id):
                r["status"] = "delivered"
                r["delivered_at"] = datetime.now(timezone.utc)
                r["last_error"] = None
                return

    async def mark_outbox_retry(self, outbox_id, error, backoff_seconds=30):
        from datetime import datetime, timedelta, timezone
        for r in getattr(self, "outbox", []):
            if r["id"] == str(outbox_id):
                r["attempts"] = (r.get("attempts") or 0) + 1
                r["last_error"] = (error or "")[:1000]
                r["next_retry_at"] = (
                    datetime.now(timezone.utc) + timedelta(seconds=backoff_seconds)
                )
                if r["attempts"] >= r.get("max_attempts", 3):
                    r["status"] = "failed"
                return r
        return {}

    async def mark_outbox_failed(self, outbox_id, error):
        for r in getattr(self, "outbox", []):
            if r["id"] == str(outbox_id):
                r["status"] = "failed"
                r["attempts"] = (r.get("attempts") or 0) + 1
                r["last_error"] = (error or "")[:1000]
                return

    async def get_outbox_for_webhook(self, webhook_id, status=None, limit=20):
        rows = [
            r for r in getattr(self, "outbox", [])
            if str(r["webhook_id"]) == str(webhook_id)
        ]
        if status:
            rows = [r for r in rows if r["status"] == status]
        rows.sort(key=lambda r: r["created_at"], reverse=True)
        return rows[:limit]

    async def reset_failed_outbox(self, webhook_id):
        from datetime import datetime, timezone
        n = 0
        for r in getattr(self, "outbox", []):
            if str(r["webhook_id"]) == str(webhook_id) and r["status"] == "failed":
                r["status"] = "pending"
                r["attempts"] = 0
                r["next_retry_at"] = datetime.now(timezone.utc)
                r["last_error"] = None
                n += 1
        return n

    async def get_outbox_stats(self):
        from datetime import datetime, timedelta, timezone
        rows = getattr(self, "outbox", [])
        now = datetime.now(timezone.utc)
        pending  = [r for r in rows if r["status"] == "pending"]
        failed   = [r for r in rows if r["status"] == "failed"]
        delivered_recent = [
            r for r in rows
            if r["status"] == "delivered"
            and r.get("delivered_at")
            and r["delivered_at"] >= now - timedelta(hours=1)
        ]
        oldest_age = 0
        if pending:
            oldest = min(r["created_at"] for r in pending)
            oldest_age = int((now - oldest).total_seconds())
        return {
            "pending":                   len(pending),
            "failed":                    len(failed),
            "delivered_last_hour":       len(delivered_recent),
            "oldest_pending_age_seconds": oldest_age,
        }

    # Multi-tenancy stubs — tests don't exercise the api_keys path
    # (auth uses the legacy env-var fallback when API_KEY is set), but
    # AggregationService route paths may pass tenant_id through.

    async def get_api_key(self, api_key):
        return getattr(self, "_api_keys", {}).get(api_key)

    async def touch_api_key(self, api_key):
        pass

    async def create_api_key(self, api_key, tenant_id, name=None, scopes="read,write"):
        if not hasattr(self, "_api_keys"):
            self._api_keys = {}
        row = {"api_key": api_key, "tenant_id": tenant_id, "name": name,
               "scopes": scopes, "is_active": True}
        self._api_keys[api_key] = row
        return row

    async def list_api_keys(self, tenant_id=None):
        rows = list(getattr(self, "_api_keys", {}).values())
        if tenant_id:
            rows = [r for r in rows if r["tenant_id"] == tenant_id]
        return rows

    async def deactivate_api_key(self, api_key):
        if hasattr(self, "_api_keys") and api_key in self._api_keys:
            self._api_keys[api_key]["is_active"] = False

    async def get_tenant(self, tenant_id):
        return getattr(self, "_tenants", {}).get(tenant_id)

    async def create_tenant(self, tenant_id, name):
        if not hasattr(self, "_tenants"):
            self._tenants = {}
        row = {"tenant_id": tenant_id, "name": name, "is_active": True}
        self._tenants[tenant_id] = row
        return row

    async def list_tenants(self):
        return list(getattr(self, "_tenants", {}).values())

    # Bulk-export streams + watermarks (Interface 3) — minimal stubs so
    # service-layer tests don't blow up if a code path tries to exercise
    # them. Real coverage is the live Postgres path in api/exports.py.

    async def get_export_watermark(self, consumer, table_name):
        return getattr(self, "export_watermarks", {}).get((consumer, table_name))

    async def upsert_export_watermark(self, consumer, table_name, watermark_ts):
        if not hasattr(self, "export_watermarks"):
            self.export_watermarks = {}
        row = {
            "consumer": consumer, "table_name": table_name,
            "watermark_ts": watermark_ts,
        }
        self.export_watermarks[(consumer, table_name)] = row
        return row

    async def list_export_watermarks(self, consumer=None):
        rows = list(getattr(self, "export_watermarks", {}).values())
        if consumer:
            rows = [r for r in rows if r["consumer"] == consumer]
        return rows


@pytest.fixture
def xref_store():
    return XRefStore()


@pytest.fixture
def redis_store():
    return RedisStore()


@pytest.fixture
def postgres_store():
    return FakePostgresStore()


@pytest.fixture
def aggregation_service(xref_store, redis_store, postgres_store):
    return AggregationService(
        xref_store=xref_store,
        golden_record_store=xref_store,
        income_assembler=IncomeAssembler(),
        credit_assembler=CreditAssembler(),
        redis_store=redis_store,
        postgres_store=postgres_store,
    )
