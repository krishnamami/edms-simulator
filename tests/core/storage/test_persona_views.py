"""Structural tests for the 12 Decision OS persona-context views in
``infra/schema.sql``.

These views are pure SQL; the actual semantic correctness of the
JSONB casts is validated when ``apply_schema`` runs against a real
Postgres in CI. What we lock in here is that the **names**, **column
counts**, and **key projections** stay in sync with the persona
contract — so a future refactor of ``entity_states`` JSONB can't
silently break a view without flipping a red test.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

SCHEMA_FILE = (
    Path(__file__).resolve().parents[3] / "infra" / "schema.sql"
)


# Spec: (view_name, expected_column_count, must-have aliases)
# Column counts include alias columns (everything in the SELECT list,
# one per output column). Must-have aliases pin the persona contract
# — Decision OS reads these by name.
EXPECTED_VIEWS = [
    ("vw_credit_assessment_context", 18, [
        "credit_score", "active_bankruptcy", "thin_file",
        "no_derogatory_last_24_months", "derogatory_marks",
        "open_tradelines", "credit_utilization",
    ]),
    ("vw_fraud_screening_context", 9, [
        "fraud_score", "identity_match_confidence",
        "document_authenticity_score", "watchlist_match",
        "synthetic_identity_flag",
    ]),
    ("vw_compliance_check_context", 12, [
        "all_hmda_fields_complete", "no_fair_lending_flags",
        "state_rules_passed", "fair_lending_violation",
        "missing_required_disclosures", "regulatory_ambiguity",
        "mixed_jurisdiction", "minor_data_gap",
    ]),
    ("vw_employment_reconciliation_context", 17, [
        "reconciliation_status", "continuity_coverage_pct",
        "max_gap_days", "employer_name_match_confidence",
        "stated_vs_verified_drift_pct", "employer_on_watchlist",
        "period_start", "period_end", "gross_amount", "stated_income",
    ]),
    ("vw_income_verification_context", 15, [
        "income_confidence_score", "employment_type",
        "payroll_verified", "reconciliation_status",
        "income_discrepancy_pct", "stated_income", "verified_income",
        "multiple_income_sources", "income_stability",
        "income_trending",
    ]),
    ("vw_dti_calculation_context", 12, [
        "dti", "dti_front", "existing_debt_obligations",
        "proposed_payment", "qualifying_monthly",
        "combined_monthly_income", "income_confidence",
    ]),
    ("vw_ltv_assessment_context", 12, [
        "ltv", "appraised_value", "purchase_price", "loan_amount",
        "down_payment", "appraisal_disputed", "title_status",
        "lien_dispute", "credit_band",
    ]),
    ("vw_product_eligibility_context", 10, [
        "dti_ratio", "ltv_ratio", "credit_band", "credit_score",
        "loan_type", "loan_amount", "loan_purpose",
    ]),
    ("vw_rate_pricing_context", 14, [
        "credit_score", "dti_ratio", "ltv_ratio", "interest_rate",
        "loan_type", "rate_within_normal_band",
        "no_manual_adjustments_required", "rate_exceeds_usury_limit",
        "concurrent_rate_lock_conflict", "llpa_adjustment",
        "loan_program",
    ]),
    ("vw_underwriting_decision_context", 21, [
        "borrower", "co_borrowers", "property", "loan_terms",
        "verifications", "mid_credit_score", "ltv", "dti_back",
        "dti_front", "piti_monthly", "completeness_pct",
    ]),
    ("vw_approval_routing_context", 5, [
        "applicant_id", "status", "completeness_pct",
    ]),
    ("vw_closing_readiness_context", 13, [
        "all_conditions_cleared", "cd_timing_compliant", "title_clear",
        "cd_timing_violation", "title_defect", "lien_dispute",
        "insurance_gap", "insurance_binder",
        "closing_disclosure_sent_at", "days_until_rate_lock_expiry",
    ]),
]


def _read_schema() -> str:
    """Read schema.sql and strip ``--`` line comments the same way
    ``core.storage.migrations.apply_schema`` does. Without this, free-
    text inside comments (e.g. "may be a scalar OR an array FROM ...")
    would trip our regex-based splitters."""
    raw = SCHEMA_FILE.read_text(encoding="utf-8")
    return re.sub(r"--[^\n]*", "", raw)


def _extract_view_body(schema_sql: str, view_name: str) -> str:
    """Return the SELECT body of the named view (everything from the
    CREATE OR REPLACE VIEW header through the trailing ``;``). Returns
    an empty string if the view is missing."""
    pattern = re.compile(
        rf"CREATE\s+OR\s+REPLACE\s+VIEW\s+{re.escape(view_name)}\s+AS"
        r"\s*(?P<body>.*?);",
        re.IGNORECASE | re.DOTALL,
    )
    m = pattern.search(schema_sql)
    return m.group("body") if m else ""


_FROM_RE = re.compile(r"\bFROM\b", re.IGNORECASE)


def _find_top_level_from(body: str) -> int:
    """Return the byte offset of the TOP-LEVEL ``FROM`` (depth-0, not
    inside a subquery). Returns ``-1`` if none found. Views with a
    CASE that uses ``jsonb_array_elements(...)`` carry a subquery
    FROM at depth > 0 — those must be skipped."""
    depth = 0
    for m in _FROM_RE.finditer(body):
        # Count parens up to (but not including) this match.
        depth = body.count("(", 0, m.start()) - body.count(")", 0, m.start())
        if depth == 0:
            # First depth-0 FROM. The view's outer FROM is always the
            # first one at depth 0 because the SELECT's projection
            # comes BEFORE the outer FROM. (Any earlier "FROM" in the
            # body has to live inside a depth-> 0 subquery.)
            return m.start()
    return -1


def _split_select_and_from(body: str) -> tuple[str, str]:
    """Split a view body into ``(select_projection, from_clause)``
    based on the TOP-LEVEL FROM. Subquery FROMs inside CASE
    expressions live at depth > 0 and are ignored — see
    ``_find_top_level_from``."""
    idx = _find_top_level_from(body)
    if idx < 0:
        return "", ""
    projection = body[:idx]
    rest       = body[idx + len("FROM"):].strip()
    # Drop the leading "SELECT" so the caller sees only projection cols.
    projection = re.sub(r"^\s*SELECT\s+", "", projection,
                        flags=re.IGNORECASE)
    return projection, rest


def _split_projection_top_level(projection: str) -> list[str]:
    """Walks parens-aware so a cast like ``(borrower->>'foo')::int``
    can't confuse the splitter."""
    depth = 0
    parts: list[str] = []
    cur:   list[str] = []
    for ch in projection:
        if   ch == "(": depth += 1; cur.append(ch)
        elif ch == ")": depth -= 1; cur.append(ch)
        elif ch == "," and depth == 0:
            parts.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)
    if cur and "".join(cur).strip():
        parts.append("".join(cur).strip())
    return parts


def _count_select_columns(body: str) -> int:
    projection, _ = _split_select_and_from(body)
    if not projection:
        return 0
    return len(_split_projection_top_level(projection))


@pytest.fixture(scope="module")
def schema_sql() -> str:
    return _read_schema()


@pytest.mark.parametrize(
    "view_name,expected_cols,must_have",
    EXPECTED_VIEWS,
    ids=[v[0] for v in EXPECTED_VIEWS],
)
def test_persona_view_is_defined_with_expected_shape(
    schema_sql, view_name, expected_cols, must_have,
):
    body = _extract_view_body(schema_sql, view_name)
    assert body, f"{view_name} is missing from infra/schema.sql"
    actual_cols = _count_select_columns(body)
    assert actual_cols == expected_cols, (
        f"{view_name}: expected {expected_cols} columns, got {actual_cols}"
    )
    projection, _ = _split_select_and_from(body)
    for alias in must_have:
        # A persona-contract name can land in the projection either
        # explicitly as ``<expr> AS alias`` OR as a bare column passed
        # through unchanged (e.g. ``status`` or ``ltv``). Accept both.
        as_pat   = re.compile(rf"\bAS\s+{re.escape(alias)}\b", re.IGNORECASE)
        bare_pat = re.compile(rf"\b{re.escape(alias)}\b")
        if as_pat.search(projection):
            continue
        # Bare column — must appear as its own projection element. Walk
        # the projection's top-level commas and look for an element
        # whose trailing token equals the alias.
        tokens = [p.strip().rstrip(",")
                  for p in _split_projection_top_level(projection)]
        has_bare = any(t == alias or t.split()[-1] == alias
                       for t in tokens)
        assert has_bare or bare_pat.search(projection), (
            f"{view_name} missing required column `{alias}` — Decision OS "
            "reads this by name; removing it would break a persona contract."
        )


def test_every_persona_view_reads_only_entity_states(schema_sql):
    """All 12 views must source from entity_states alone — no joins,
    no other tables. Keeps the views safe to query at any tenant scale
    (no fan-out) and means we only have to refresh one source of truth."""
    for view_name, _, _ in EXPECTED_VIEWS:
        body = _extract_view_body(schema_sql, view_name)
        _, from_clause = _split_select_and_from(body)
        assert from_clause, f"{view_name}: no FROM clause found"
        # Allow trailing whitespace; nothing else after entity_states.
        assert from_clause.lower().startswith("entity_states"), (
            f"{view_name}: must source from entity_states only "
            f"(got `{from_clause[:80]}...`)"
        )
        # No JOIN, no UNION inside the body.
        upper = body.upper()
        assert " JOIN "  not in upper, f"{view_name}: unexpected JOIN"
        assert " UNION " not in upper, f"{view_name}: unexpected UNION"


def test_views_use_create_or_replace_for_idempotent_redeploy(schema_sql):
    """Plain ``CREATE VIEW`` would error on the second ECS task boot
    because the view already exists. Every persona view must use
    ``CREATE OR REPLACE VIEW`` so ``apply_schema`` re-runs cleanly."""
    for view_name, _, _ in EXPECTED_VIEWS:
        header = re.compile(
            rf"CREATE\s+OR\s+REPLACE\s+VIEW\s+{re.escape(view_name)}\b",
            re.IGNORECASE,
        )
        assert header.search(schema_sql), (
            f"{view_name} must be declared with CREATE OR REPLACE VIEW"
        )


# ===========================================================================
# Regression guards — JSONB casts that landed array/object values in
# production must stay protected. These tests fail loudly if anyone
# unwinds the ``jsonb_typeof`` safety net while refactoring.
# ===========================================================================


_TYPEOF_GUARDED_VIEWS = [
    # (view_name, JSONB path that may be array/object/scalar)
    ("vw_credit_assessment_context",       "borrower->'credit'->'monthly_obligations'"),
    ("vw_dti_calculation_context",         "borrower->'credit'->'monthly_obligations'"),
    ("vw_employment_reconciliation_context",
                                            "borrower->'employment'->'income_amount'"),
]


@pytest.mark.parametrize(
    "view_name,path", _TYPEOF_GUARDED_VIEWS,
    ids=[f"{v}:{p}" for v, p in _TYPEOF_GUARDED_VIEWS],
)
def test_jsonb_typeof_guard_present_on_risky_cast(schema_sql, view_name, path):
    """``::float`` against a JSONB array text representation raises in
    Postgres at query time. Decision OS hit this with
    ``borrower.credit.monthly_obligations`` (a list of obligation
    rows when the credit assembler runs against a real CREDIT_REPORT
    doc) and again with ``borrower.employment.income_amount`` (raw
    extracted_fields passthrough — anything from a numeric string to
    a chaos-test "one hundred thousand"). Every cast against one of
    those paths must be wrapped in ``jsonb_typeof`` so the bad shapes
    fall through to NULL or 0 instead of erroring the whole query."""
    body = _extract_view_body(schema_sql, view_name)
    assert body, f"{view_name} missing from schema.sql"
    # ``jsonb_typeof(<path>)`` must appear at least once in the same
    # view body where ``<path>::float`` would otherwise live.
    expected = f"jsonb_typeof({path})"
    assert expected in body, (
        f"{view_name}: missing safety guard `{expected}` — a future "
        "refactor that removed the jsonb_typeof CASE would re-introduce "
        "the production query failure on real-doc monthly_obligations "
        "or chaos-case income_amount."
    )
