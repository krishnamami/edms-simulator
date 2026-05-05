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
        self.applications: dict = {}
        self.income_profiles: dict = {}
        self.credit_profiles: dict = {}
        self.documents: list = []
        self.xrefs: list = []

    async def save_golden_record(self, gr): pass
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
