"""Unit tests for the 30 enrichment fields (Groups 2-9) added to
``entity_states`` so the Decision OS personas can decide on the JSONB
row alone, no extra joins.

Groups 2 + 3 (income, 9 fields) and Group 4 (identity, 5 fields) are
pure helpers in ``entity_state_builder``; Groups 5-9 (property, loan,
closer, secondary-market, management — 16 fields) are derived inside
``_compose_and_upsert_entity_state`` and asserted via the FakePG row.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from core.aggregation.entity_state_builder import (
    _classify_employment_type,
    _derive_identity_enrichment,
    _derive_income_enrichment,
)
from core.aggregation.golden_record_builder import (
    _classify_investor,
    _classify_loan_type,
    _days_since,
    _days_until,
    _llpa_grid,
    rebuild_one,
)


# ===========================================================================
# Group 2 + 3 — income enrichment + stability
# ===========================================================================


def _doc(document_type, applicant_id="APL-1", **fields):
    return {
        "document_id":      f"DOC-{document_type}",
        "applicant_id":     applicant_id,
        "application_id":   "APP-1",
        "document_type":    document_type,
        "extracted_fields": fields,
        "status":           "indexed",
    }


def test_employment_type_salaried_when_w2_present():
    assert _classify_employment_type({"W2_CURRENT"}) == "salaried"
    assert _classify_employment_type({"PAYSTUB_CURRENT"}) == "salaried"


def test_employment_type_self_employed_when_1099_or_schedule_c():
    assert _classify_employment_type({"1099_NEC"}) == "self_employed"
    assert _classify_employment_type({"SCHEDULE_C"}) == "self_employed"


def test_employment_type_retired_when_ssa_award_only():
    assert _classify_employment_type({"SSA_AWARD_LETTER"}) == "retired"


def test_employment_type_other_when_no_income_docs():
    assert _classify_employment_type({"DRIVERS_LICENSE", "OFAC_CHECK"}) == "other"


def test_income_enrichment_pulls_verified_from_w2_box1():
    docs = [_doc("W2_CURRENT", box1_wages=96_000)]
    out = _derive_income_enrichment(docs, None, "APL-1", None)
    assert out["verified_income_annual"] == 96_000
    assert out["employment_type"] == "salaried"


def test_income_enrichment_falls_back_to_employment_amount():
    """When no W2 doc carries box1_wages, mirror the already-extracted
    ``employment.income_amount`` so the persona still sees a number."""
    out = _derive_income_enrichment([], None, "APL-1", 75_000)
    assert out["verified_income_annual"] == 75_000


def test_income_enrichment_stated_from_urla_monthly_times_twelve():
    docs = [_doc("URLA_1003", monthly_income_stated=10_000,
                 employer_name="Acme Corp")]
    out = _derive_income_enrichment(docs, None, "APL-1", None)
    assert out["stated_income_annual"] == 120_000
    assert out["stated_employer"] == "Acme Corp"


def test_income_enrichment_multiple_sources_when_w2_plus_1099():
    docs = [_doc("W2_CURRENT", box1_wages=80_000),
            _doc("1099_NEC", nonemployee_compensation=20_000)]
    out = _derive_income_enrichment(docs, None, "APL-1", None)
    assert out["multiple_income_sources"] is True


def test_income_enrichment_single_source_when_only_w2_pair():
    """W2_CURRENT + W2_PRIOR is still one income channel (salaried)."""
    docs = [_doc("W2_CURRENT", box1_wages=90_000),
            _doc("W2_PRIOR",   box1_wages=85_000)]
    out = _derive_income_enrichment(docs, None, "APL-1", None)
    assert out["multiple_income_sources"] is False


def test_income_confidence_reads_existing_overall_confidence():
    income = {"primary_borrower": {"overall_confidence": 0.92}}
    out = _derive_income_enrichment([], income, "APL-1", None)
    assert out["income_confidence_score"] == 0.92


def test_income_confidence_default_85_when_missing():
    out = _derive_income_enrichment([], None, "APL-1", None)
    assert out["income_confidence_score"] == 0.85


def test_income_stability_stable_same_employer_year_over_year():
    docs = [_doc("W2_CURRENT", box1_wages=100_000, employer_name="Acme Corp"),
            _doc("W2_PRIOR",   box1_wages=95_000,  employer_name="Acme Corp")]
    out = _derive_income_enrichment(docs, None, "APL-1", None)
    assert out["income_stability"] == "stable"


def test_income_stability_new_employment_when_employer_differs():
    docs = [_doc("W2_CURRENT", box1_wages=100_000, employer_name="Acme Corp"),
            _doc("W2_PRIOR",   box1_wages=95_000,  employer_name="Initech")]
    out = _derive_income_enrichment(docs, None, "APL-1", None)
    assert out["income_stability"] == "new_employment"


def test_income_stability_insufficient_history_when_no_prior_w2():
    docs = [_doc("W2_CURRENT", box1_wages=100_000, employer_name="Acme Corp")]
    out = _derive_income_enrichment(docs, None, "APL-1", None)
    assert out["income_stability"] == "insufficient_history"
    assert out["income_trending"] == "unknown"


def test_income_trending_increasing_when_wages_up_over_3pct():
    docs = [_doc("W2_CURRENT", box1_wages=110_000, employer_name="Acme Corp"),
            _doc("W2_PRIOR",   box1_wages=100_000, employer_name="Acme Corp")]
    out = _derive_income_enrichment(docs, None, "APL-1", None)
    assert out["income_trending"] == "increasing"


def test_income_trending_decreasing_when_wages_down_over_3pct():
    docs = [_doc("W2_CURRENT", box1_wages=90_000,  employer_name="Acme Corp"),
            _doc("W2_PRIOR",   box1_wages=100_000, employer_name="Acme Corp")]
    out = _derive_income_enrichment(docs, None, "APL-1", None)
    assert out["income_trending"] == "decreasing"


def test_income_trending_flat_within_3pct():
    docs = [_doc("W2_CURRENT", box1_wages=101_000, employer_name="Acme Corp"),
            _doc("W2_PRIOR",   box1_wages=100_000, employer_name="Acme Corp")]
    out = _derive_income_enrichment(docs, None, "APL-1", None)
    assert out["income_trending"] == "flat"


def test_income_gap_in_employment_defaults_false_synthetic():
    docs = [_doc("W2_CURRENT", box1_wages=100_000, employer_name="Acme Corp")]
    out = _derive_income_enrichment(docs, None, "APL-1", None)
    assert out["gap_in_employment"] is False


# ===========================================================================
# Group 4 — identity enrichment
# ===========================================================================


def test_identity_full_kit_yields_low_fraud_score():
    docs = [_doc("OFAC_CHECK"), _doc("SSN_VALIDATION"),
            _doc("DRIVERS_LICENSE")]
    out = _derive_identity_enrichment(docs, "APL-FULL")
    assert 0.02 <= out["fraud_score"] <= 0.08
    assert out["identity_match_confidence"] == 0.98
    assert out["watchlist_match"] is False
    assert out["synthetic_identity_flag"] is False
    assert out["document_authenticity_score"] == 0.95


def test_identity_partial_kit_yields_medium_fraud_score():
    docs = [_doc("SSN_VALIDATION")]
    out = _derive_identity_enrichment(docs, "APL-PARTIAL")
    assert 0.27 <= out["fraud_score"] <= 0.33
    assert out["identity_match_confidence"] == 0.70


def test_identity_no_docs_yields_high_fraud_score():
    out = _derive_identity_enrichment([], "APL-NONE")
    assert 0.77 <= out["fraud_score"] <= 0.83
    assert out["identity_match_confidence"] == 0.0


def test_identity_fraud_score_is_deterministic_by_applicant_id():
    docs = [_doc("OFAC_CHECK"), _doc("SSN_VALIDATION")]
    a = _derive_identity_enrichment(docs, "APL-42")
    b = _derive_identity_enrichment(docs, "APL-42")
    assert a == b


# ===========================================================================
# Group 6 — loan classification helpers
# ===========================================================================


@pytest.mark.parametrize("program,expected", [
    ("Conventional 30",  "conforming"),
    ("CONV30",           "conforming"),
    ("FHA 30 fixed",     "fha"),
    ("VA 15",            "va"),
    ("Jumbo NonQM",      "jumbo"),
    ("HELOC line",       "heloc"),
    ("USDA Rural",       "usda"),
    ("",                 "other"),
    (None,               "other"),
])
def test_classify_loan_type(program, expected):
    assert _classify_loan_type(program) == expected


# ===========================================================================
# Group 8 — LLPA grid + investor eligibility
# ===========================================================================


@pytest.mark.parametrize("score,ltv,expected", [
    (760, 75, 0.0),    # top tier, low LTV
    (760, 85, 0.25),
    (720, 75, 0.25),
    (720, 85, 0.75),
    (690, 90, 1.0),
    (670, 70, 1.5),
    (640, 60, 2.5),
    (None, 75, 0.0),   # missing score → no add-on
])
def test_llpa_grid(score, ltv, expected):
    assert _llpa_grid(score, ltv) == expected


def test_investor_fannie_when_conforming_under_cap_and_aus_ok():
    assert _classify_investor("conforming", 500_000, True) == "fannie"


def test_investor_ginnie_when_fha_or_va():
    assert _classify_investor("fha", 300_000, False) == "ginnie"
    assert _classify_investor("va",  400_000, False) == "ginnie"


def test_investor_jumbo_portfolio_when_over_cap():
    assert _classify_investor("conforming", 900_000, True) == "jumbo_portfolio"


def test_investor_non_qm_when_no_other_bucket_fits():
    # Conforming but AUS not approved → falls through.
    assert _classify_investor("conforming", 500_000, False) == "non_qm"
    assert _classify_investor("other",      500_000, True)  == "non_qm"


# ===========================================================================
# Group 9 — time helpers
# ===========================================================================


def test_days_since_handles_iso_string():
    one_week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    assert _days_since(one_week_ago) in (6, 7)  # tolerate boundary


def test_days_since_handles_naive_datetime():
    one_week_ago = datetime.utcnow() - timedelta(days=7)
    assert _days_since(one_week_ago) in (6, 7)


def test_days_since_returns_none_on_bad_input():
    assert _days_since(None) is None
    assert _days_since("not-a-date") is None


def test_days_until_handles_iso_string():
    in_30_days = (datetime.now(timezone.utc) + timedelta(days=30)).date().isoformat()
    # ±1 day tolerance — _days_until uses date.today() (local) while
    # the test computes a UTC offset, and the two can differ by a day
    # near midnight depending on the runner's timezone.
    assert _days_until(in_30_days) in (29, 30, 31)


def test_days_until_negative_when_already_expired():
    # A full week back is well past any timezone-boundary ambiguity.
    a_week_ago = (datetime.now(timezone.utc) - timedelta(days=7)).date().isoformat()
    assert _days_until(a_week_ago) < 0


# ===========================================================================
# Integration — every field lands on the entity_states row
# ===========================================================================


class _IncomeProfileStub:
    def __init__(self, applicant_id, application_id):
        self.applicant_id   = applicant_id
        self.application_id = application_id
        self.primary_borrower = {
            "applicant_id":       applicant_id,
            "qualifying_monthly": 8000.0,
            "overall_confidence": 0.91,
            "sources": [{"source_type": "W2_SALARIED",
                         "monthly":      8000.0,
                         "status":       "confirmed"}],
        }
        self.co_borrower                 = None
        self.combined_qualifying_monthly = 8000.0
        self.assembled_at = "2026-05-13T00:00:00+00:00"
        self.lineage_hash = "stub"

    def model_dump(self):
        return {
            "applicant_id":               self.applicant_id,
            "application_id":             self.application_id,
            "assembled_at":               self.assembled_at,
            "primary_borrower":           self.primary_borrower,
            "co_borrower":                self.co_borrower,
            "combined_qualifying_monthly": self.combined_qualifying_monthly,
            "lineage_hash":               self.lineage_hash,
            "profile_data":               {},
        }


class _IncomeAssemblerStub:
    def assemble(self, *, application_id, applicant_id, **_):
        return _IncomeProfileStub(applicant_id, application_id)


class _CreditAssemblerStub:
    async def assemble(self, applicant_id, loan_data, postgres_store=None, docs=None):
        return {
            "applicant_id":              applicant_id,
            "mid_score":                 720,
            "credit_band":               "near-prime",
            "experian_score":            725,
            "equifax_score":             720,
            "transunion_score":          715,
            "report_date":               "2026-01-15",
            "expiry_date":               "2026-04-15",
            "is_current":                True,
            "total_monthly_obligations": 600.0,
            "monthly_obligations":       [],
            "profile_data":              {},
        }


@pytest.fixture
def rich_app(postgres_store):
    """Seed an application with W2 (current + prior), URLA, identity
    docs, title insurance, closing disclosure — enough to exercise
    every enrichment branch."""
    async def _seed():
        await postgres_store.save_application({
            "application_id":   "APP-RICH",
            "applicant_id":     "APL-RICH-P",
            "co_applicant_id":  None,
            "los_id":           "LOAN-RICH",
            "loan_amount":      400_000,
            "interest_rate":    6.5,
            "loan_term_months": 360,
            "status":           "submitted",
            "created_at":       datetime.now(timezone.utc) - timedelta(days=30),
        })
        docs = [
            _doc("W2_CURRENT",       applicant_id="APL-RICH-P",
                 box1_wages=104_000, employer_name="Acme Corp"),
            _doc("W2_PRIOR",         applicant_id="APL-RICH-P",
                 box1_wages=100_000, employer_name="Acme Corp"),
            _doc("URLA_1003",        applicant_id="APL-RICH-P",
                 monthly_income_stated=8_500,
                 employer_name="Acme Corp",
                 loan_purpose="purchase"),
            _doc("DRIVERS_LICENSE",  applicant_id="APL-RICH-P"),
            _doc("SSN_VALIDATION",   applicant_id="APL-RICH-P"),
            _doc("OFAC_CHECK",       applicant_id="APL-RICH-P"),
            _doc("CREDIT_REPORT",    applicant_id="APL-RICH-P",
                 mid_score=720),
            _doc("CLOSING_DISCLOSURE", applicant_id="APL-RICH-P"),
            # Property docs need application_id linking; build_property_state
            # queries application-scoped docs separately, but we still
            # need title_insurance to flip the closer flag.
            {**_doc("TITLE_INSURANCE", applicant_id="APL-RICH-P"),
             "document_category": "property"},
            {**_doc("APPRAISAL_URAR", applicant_id="APL-RICH-P",
                    appraised_value=450_000),
             "document_category": "property"},
            {**_doc("RATE_LOCK", applicant_id="APL-RICH-P",
                    locked_rate=6.5,
                    lock_expiry=(datetime.now(timezone.utc)
                                 + timedelta(days=30)).date().isoformat(),
                    loan_program="Conventional 30"),
             "document_category": "loan_terms"},
            {**_doc("AUS_DU_FINDINGS", applicant_id="APL-RICH-P",
                    approved=True),
             "document_category": "loan_terms"},
            {**_doc("PURCHASE_AGREEMENT", applicant_id="APL-RICH-P",
                    purchase_price=460_000),
             "document_category": "loan_terms"},
            {**_doc("TITLE_COMMITMENT", applicant_id="APL-RICH-P"),
             "document_category": "property"},
        ]
        for d in docs:
            await postgres_store.save_document(d)

    asyncio.run(_seed())
    return postgres_store


def test_rebuild_one_lands_all_37_enrichment_fields(rich_app, redis_store):
    asyncio.run(rebuild_one(
        rich_app, redis_store, "APP-RICH",
        income_assembler=_IncomeAssemblerStub(),
        credit_assembler=_CreditAssemblerStub(),
    ))
    row = asyncio.run(rich_app.get_entity_state("APP-RICH"))
    assert row is not None

    # Group 1 — credit (7 fields on borrower.credit)
    credit = row["borrower"]["credit"]
    for k in ("active_bankruptcy", "foreclosure_last_36_months",
              "thin_file", "no_derogatory_last_24_months",
              "derogatory_marks", "open_tradelines", "credit_utilization"):
        assert k in credit, f"borrower.credit missing {k}"

    # Group 2 + 3 — income (9 fields on borrower.income)
    income = row["borrower"]["income"]
    for k in ("verified_income_annual", "stated_income_annual",
              "stated_employer", "employment_type",
              "multiple_income_sources", "income_confidence_score",
              "income_stability", "income_trending", "gap_in_employment"):
        assert k in income, f"borrower.income missing {k}"
    assert income["verified_income_annual"] == 104_000
    assert income["stated_income_annual"]   == 8_500 * 12
    assert income["stated_employer"] == "Acme Corp"
    assert income["employment_type"] == "salaried"
    assert income["income_stability"] == "stable"
    assert income["income_trending"] == "increasing"

    # Group 4 — identity (5 fields on borrower.identity)
    identity = row["borrower"]["identity"]
    for k in ("fraud_score", "identity_match_confidence",
              "document_authenticity_score", "watchlist_match",
              "synthetic_identity_flag"):
        assert k in identity, f"borrower.identity missing {k}"
    # Full kit → very low fraud
    assert identity["fraud_score"] < 0.10

    # Group 5 — property (3 fields on property)
    prop = row["property"]
    for k in ("down_payment", "appraisal_disputed", "lien_status"):
        assert k in prop, f"property missing {k}"
    assert prop["down_payment"] == 60_000  # 460k - 400k
    assert prop["appraisal_disputed"] is False
    assert prop["lien_status"] in {"clear", "pending", "unknown"}

    # Group 6 + 7 + 8 — loan_terms (10 fields)
    lt = row["loan_terms"]
    for k in ("loan_type", "concurrent_rate_lock_conflict",
              "days_until_rate_lock_expiry", "proposed_payment",
              "cd_issued", "wire_instructions_received",
              "final_title_policy_received",
              "investor_eligible", "llpa_adjustment", "servicing_released"):
        assert k in lt, f"loan_terms missing {k}"
    assert lt["loan_type"]              == "conforming"
    assert lt["cd_issued"]              is True
    assert lt["final_title_policy_received"] is True
    assert lt["servicing_released"]     is True
    assert lt["investor_eligible"]      == "fannie"  # conv + under cap + aus ok
    # LTV here = 400k loan / min(460k purchase, 450k appraisal) = 88.9% > 80
    # → 720 + ltv > 80 cell of the grid → 0.75.
    assert lt["llpa_adjustment"]        == 0.75
    assert isinstance(lt["days_until_rate_lock_expiry"], int)
    assert lt["days_until_rate_lock_expiry"] > 0

    # Group 9 — top-level management (2 fields)
    assert "days_in_current_status" in row
    assert "loan_age_days"          in row
    # First backfill → status just set → days_in_current_status starts 0
    assert row["days_in_current_status"] == 0
    # 30 days seeded for created_at
    assert row["loan_age_days"] is not None
    assert 28 <= row["loan_age_days"] <= 32
