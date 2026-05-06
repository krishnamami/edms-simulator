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
