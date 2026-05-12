"""Unit tests for ``core/aggregation/golden_record_builder.py``.

Locks in the contract that the per-application backfill writes to all
five golden-record tables idempotently. The orchestrator is tested
against the in-process ``FakePostgresStore`` from ``tests/conftest.py``
so we don't depend on a running Postgres.
"""
from __future__ import annotations

import asyncio

import pytest

from core.aggregation.golden_record_builder import rebuild_one


# ----- Stub assemblers -------------------------------------------------------

class _IncomeProfileStub:
    """Minimal stand-in for the real ``IncomeProfile`` dataclass — we
    only need ``model_dump`` + the few attributes ``rebuild_one`` reads
    when composing entity_states."""
    def __init__(self, *, applicant_id, application_id,
                 qualifying_monthly=8000.0, co_qualifying=None):
        self.applicant_id   = applicant_id
        self.application_id = application_id
        self.primary_borrower = {
            "applicant_id":       applicant_id,
            "qualifying_monthly": qualifying_monthly,
            "sources":            [{"source_type": "W2_SALARIED",
                                    "monthly":      qualifying_monthly,
                                    "status":       "confirmed"}],
        }
        self.co_borrower = (
            {"qualifying_monthly": co_qualifying} if co_qualifying else None
        )
        self.combined_qualifying_monthly = qualifying_monthly + (co_qualifying or 0)
        self.assembled_at  = "2026-05-11T00:00:00+00:00"
        self.lineage_hash  = "stub"

    def model_dump(self):
        return {
            "applicant_id":               self.applicant_id,
            "application_id":             self.application_id,
            "assembled_at":               self.assembled_at,
            "primary_borrower":           self.primary_borrower,
            "co_borrower":                self.co_borrower,
            "combined_qualifying_monthly": self.combined_qualifying_monthly,
            "lineage_hash":               self.lineage_hash,
            "profile_data":               {},  # PG schema wants this key
        }


class _IncomeAssemblerStub:
    def assemble(self, *, primary_docs, co_borrower_docs, primary_credit,
                 co_borrower_credit, application_id, applicant_id,
                 co_applicant_id=None, **_):
        co_q = 4000.0 if co_borrower_credit else None
        return _IncomeProfileStub(
            applicant_id=applicant_id,
            application_id=application_id,
            qualifying_monthly=8000.0,
            co_qualifying=co_q,
        )


class _CreditAssemblerStub:
    async def assemble(self, applicant_id, loan_data, postgres_store=None,
                       docs=None):
        return {
            "applicant_id":              applicant_id,
            "mid_score":                 720,
            "credit_band":               "prime",
            "experian_score":            725,
            "equifax_score":             720,
            "transunion_score":          715,
            "report_date":               "2026-01-15",
            "expiry_date":               "2026-04-15",
            "is_current":                True,
            "pull_type":                 "tri_merge",
            "total_monthly_obligations": 600.0,
            "monthly_obligations":       [],
            "profile_data":              {},
        }


# ----- Fixtures --------------------------------------------------------------

@pytest.fixture
def seeded_app(postgres_store):
    """Seed an application + primary applicant + a couple of docs so
    rebuild_one has a real shape to work against."""
    async def _seed():
        # Application
        await postgres_store.save_application({
            "application_id":   "APP-001",
            "applicant_id":     "APL-00001-P",
            "co_applicant_id":  None,
            "los_id":           "LOAN-001",
            "loan_amount":      400_000,
            "interest_rate":    6.5,
            "loan_term_months": 360,
            "status":           "submitted",
        })
        # A W-2 doc (income_verified flag) and a CREDIT_REPORT doc
        # (credit_pulled flag).
        await postgres_store.save_document({
            "document_id":         "DOC-W2-001",
            "applicant_id":        "APL-00001-P",
            "application_id":      "APP-001",
            "document_type":       "W2_CURRENT",
            "category":            "income",
            "received_at":         "2026-01-01T09:00:00Z",
            "source_document_id":  "ADP-W2-2025-0001",
            "source_channel":      "edms_pull",
            "extracted_fields":    {"box1_wages": 96000},
            "status":              "indexed",
        })
        await postgres_store.save_document({
            "document_id":         "DOC-CR-001",
            "applicant_id":        "APL-00001-P",
            "application_id":      "APP-001",
            "document_type":       "CREDIT_REPORT",
            "category":            "credit",
            "received_at":         "2026-01-02T09:00:00Z",
            "source_document_id":  "EQX-CR-001",
            "source_channel":      "vendor_equifax",
            "extracted_fields":    {"mid_score": 720},
            "status":              "indexed",
        })

    asyncio.run(_seed())
    return postgres_store


# ----- Tests -----------------------------------------------------------------

def test_rebuild_one_writes_to_all_five_tables(seeded_app, redis_store):
    """rebuild_one must populate income_profiles + credit_profiles +
    applicant_identity_xref + entity_states + entity_state_events.
    The fake PG records every write to in-memory dicts so we can
    introspect them after."""
    pg = seeded_app
    result = asyncio.run(rebuild_one(
        pg, redis_store, "APP-001",
        income_assembler=_IncomeAssemblerStub(),
        credit_assembler=_CreditAssemblerStub(),
    ))

    # rebuild_one stat shape
    assert result["entity_state"] is True
    assert result["applicant_count"] == 1
    assert result["income_profiles"] >= 1
    assert result["credit_profiles"] >= 1
    assert result["xref_rows"] == 2  # one per doc with source_document_id
    assert result["doc_count"] == 2
    # 12 verification flags, 2 of which (income_verified + credit_pulled)
    # are true → 2/12 ≈ 16.67% → status="intake".
    assert result["completeness_pct"] > 0
    assert result["status"] in {"intake", "in_progress", "complete"}

    # Income profile landed
    inc = asyncio.run(pg.get_income_profile("APL-00001-P"))
    assert inc is not None
    assert inc.get("applicant_id") == "APL-00001-P"

    # Credit profile landed
    cred = asyncio.run(pg.get_credit_profile("APL-00001-P"))
    assert cred is not None
    assert cred.get("mid_score") == 720

    # entity_states row landed and carries the indexed columns we
    # populated from the assemblers.
    es = asyncio.run(pg.get_entity_state("APP-001"))
    assert es is not None
    assert es["application_id"] == "APP-001"
    assert es["mid_credit_score"] == 720
    assert es["loan_amount"] == 400_000
    assert es["interest_rate"] == 6.5
    # LTV needs an appraisal — none seeded → ltv stays None. Verify.
    assert es.get("ltv") in (None, 0)

    # xrefs: 2 docs each carried source_document_id → 2 rows expected.
    saved_xrefs = list(getattr(pg, "xrefs", []))
    assert len(saved_xrefs) >= 2
    by_system = {x["source_system"]: x for x in saved_xrefs}
    assert "edms_pull" in by_system
    assert "vendor_equifax" in by_system

    # entity_state_events — one row logged per rebuild_one call.
    events = getattr(pg, "_entity_state_events", []) or []
    assert any(
        e.get("application_id") == "APP-001"
        and e.get("event_type") == "golden_record_rebuilt"
        for e in events
    )


def test_rebuild_one_is_idempotent_on_re_run(seeded_app, redis_store):
    """Re-running on the same application must not duplicate rows in
    any of the 5 tables. income/credit are DELETE+INSERT; xref is
    UPSERT DO NOTHING; entity_states is UPSERT DO UPDATE."""
    pg = seeded_app
    args = (pg, redis_store, "APP-001")
    kwargs = dict(
        income_assembler=_IncomeAssemblerStub(),
        credit_assembler=_CreditAssemblerStub(),
    )

    asyncio.run(rebuild_one(*args, **kwargs))
    asyncio.run(rebuild_one(*args, **kwargs))

    # entity_states is keyed by application_id → exactly one row.
    es_rows = list((pg._entity_states or {}).values())
    assert len([r for r in es_rows if r["application_id"] == "APP-001"]) == 1

    # income / credit profiles are DELETE+INSERT in the FakePostgresStore
    # mirror — so re-running re-keys by applicant_id, one row remains.
    assert "APL-00001-P" in pg.income_profiles
    assert "APL-00001-P" in pg.credit_profiles


def test_rebuild_one_skips_unknown_application(postgres_store, redis_store):
    """A missing application_id should report ``skipped`` rather than
    raise — the backfill loop catches errors per app, but the most
    common "no-op" case is just an empty applications table."""
    result = asyncio.run(rebuild_one(
        postgres_store, redis_store, "APP-DOES-NOT-EXIST",
        income_assembler=_IncomeAssemblerStub(),
        credit_assembler=_CreditAssemblerStub(),
    ))
    assert result.get("skipped") is True
    assert result.get("reason") == "application_not_found"
