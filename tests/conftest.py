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

    async def save_golden_record(self, gr):
        # Round-trip storage so get_all_applicants / find_by_external_id can
        # observe records written through the regular pipeline.
        self.applicants[gr["applicant_id"]] = gr
    async def find_by_applicant_id(self, applicant_id): return None
    async def find_by_ssn_hash(self, ssn_hash): return None
    async def find_by_name_dob(self, last_name, dob): return []
    async def update_status(self, applicant_id, status): pass
    async def next_sequence(self): return 1

    async def save_xref(self, xref):
        self.xrefs.append(xref)

    async def save_application(self, app):
        self.applications[app["application_id"]] = app

    async def get_application_by_los_id(self, los_id):
        for app in self.applications.values():
            if app["los_id"] == los_id:
                return app
        return None

    async def get_application(self, application_id):
        return self.applications.get(application_id)

    async def get_all_applications(self, limit=50):
        rows = list(self.applications.values())
        return rows[:limit]

    async def get_raw_ingestion_for_application(self, application_id):
        # FakePostgresStore doesn't model raw_ingestion at all today —
        # callers handle the empty list path.
        return []

    async def get_application_by_applicant(self, applicant_id):
        for app in self.applications.values():
            if app.get("applicant_id") == applicant_id \
                    or app.get("co_applicant_id") == applicant_id:
                return app
        return None

    async def update_application_loan_data(self, application_id, loan_data):
        await self.update_application_loan_fields(application_id, loan_data)

    async def save_income_profile(self, profile):
        applicant_id = profile["applicant_id"]
        prior = self.income_profiles.get(applicant_id)
        version = (prior.get("_version", 0) + 1) if prior else 1
        profile = {**profile, "_version": version}
        self.income_profiles[applicant_id] = profile
        return f"profile-{applicant_id}-{version}"

    async def get_income_profile(self, applicant_id):
        return self.income_profiles.get(applicant_id)

    async def save_credit_profile(self, profile):
        self.credit_profiles[profile["applicant_id"]] = profile

    async def get_credit_profile(self, applicant_id):
        return self.credit_profiles.get(applicant_id)

    async def save_document(self, doc):
        # Mirror production's ON CONFLICT (document_id) DO UPDATE so the
        # batch indexer's save → assembler save flow doesn't double-row.
        for i, existing in enumerate(self.documents):
            if existing.get("document_id") == doc.get("document_id"):
                self.documents[i] = doc
                return
        self.documents.append(doc)

    async def get_documents_for_applicant(self, applicant_id):
        return [d for d in self.documents if d["applicant_id"] == applicant_id]

    async def get_documents_for_application(self, application_id):
        return [d for d in self.documents if d.get("application_id") == application_id]

    async def get_all_applicants(self):
        return list(self.applicants.values())

    # graph methods
    async def save_relationship(self, rel):
        self.relationships.append(rel)

    async def get_relationships_for_applicant(self, applicant_id):
        return [r for r in self.relationships if r["applicant_id"] == applicant_id]

    async def get_conflicts_for_applicant(self, applicant_id):
        return [
            r for r in self.relationships
            if r["applicant_id"] == applicant_id
            and r["relationship_type"] == "contradicts"
        ]

    async def get_graph_summary(self, applicant_id):
        docs = await self.get_documents_for_applicant(applicant_id)
        rels = await self.get_relationships_for_applicant(applicant_id)
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
    async def save_property(self, prop):
        self.properties[prop["property_id"]] = dict(prop)
        return prop["property_id"]

    async def get_property(self, property_id):
        return self.properties.get(property_id)

    async def get_property_by_application(self, application_id):
        for p in self.properties.values():
            if p.get("application_id") == application_id:
                return p
        return None

    async def save_property_profile(self, profile):
        property_id = profile["property_id"]
        prior = self.property_profiles.get(property_id)
        version = (prior.get("_version", 0) + 1) if prior else 1
        self.property_profiles[property_id] = {**profile, "_version": version}
        return f"profile-{property_id}-{version}"

    async def get_property_profile(self, property_id):
        return self.property_profiles.get(property_id)

    async def get_property_docs(self, property_id):
        prop = self.properties.get(property_id)
        if not prop:
            return []
        application_id = prop.get("application_id")
        return [
            d for d in self.documents
            if d.get("application_id") == application_id
            and d.get("document_category") == "property"
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
        }
        return mapping.get(table_name, 0)


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
