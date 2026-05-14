"""Structural tests for Decision OS's output + timeline tables and the
``vw_pipeline_status`` management view.

The actual SQL parses cleanly when CI applies ``infra/schema.sql`` to
a real Postgres in ``.github/workflows/ci.yaml`` — these tests just
pin the schema-level contract so a future refactor can't silently
remove a column, index, or the partial pending-human predicate that
the ops dashboard depends on.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

SCHEMA_FILE = (
    Path(__file__).resolve().parents[3] / "infra" / "schema.sql"
)


def _read_schema() -> str:
    """Strip ``--`` line comments the way ``apply_schema`` does — see
    note on the same helper in ``test_persona_views.py``."""
    return re.sub(r"--[^\n]*", "", SCHEMA_FILE.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def schema_sql() -> str:
    return _read_schema()


# ----- decision_outputs -----------------------------------------------------

_DECISION_OUTPUTS_REQUIRED_COLUMNS = [
    "id",
    "application_id",
    "decision_id",
    "wave",
    "outcome",
    "mode",
    "risk_level",
    "boundary_matched",
    "boundary_rule",
    "context_snapshot",
    "reasoning",
    "confidence",
    "upstream_decisions",
    "human_action",
    "human_override_reason",
    "human_reviewer",
    "decided_at",
    "acted_at",
    "sla_seconds",
    "actual_seconds",
    "version",
    "superseded_by",
    "tenant_id",
    "created_at",
]


def _table_body(schema_sql: str, table: str) -> str:
    """Return the column-list body of ``CREATE TABLE IF NOT EXISTS
    <table> ( ... );`` — i.e. everything between the outer parens."""
    pat = re.compile(
        rf"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+{re.escape(table)}\s*\(",
        re.IGNORECASE,
    )
    m = pat.search(schema_sql)
    if not m:
        return ""
    # Walk paren depth from the opening paren to find the matching close.
    start = m.end() - 1  # index of '('
    depth = 0
    for i in range(start, len(schema_sql)):
        ch = schema_sql[i]
        if   ch == "(": depth += 1
        elif ch == ")":
            depth -= 1
            if depth == 0:
                return schema_sql[start + 1 : i]
    return ""


def test_decision_outputs_table_is_defined(schema_sql):
    assert _table_body(schema_sql, "decision_outputs"), (
        "decision_outputs must be declared with CREATE TABLE IF NOT EXISTS"
    )


@pytest.mark.parametrize("column", _DECISION_OUTPUTS_REQUIRED_COLUMNS)
def test_decision_outputs_carries_required_column(schema_sql, column):
    body = _table_body(schema_sql, "decision_outputs")
    # Match the column name at the start of a line (allowing leading
    # whitespace) to avoid false hits from the same word appearing
    # inside another column's type or default.
    pat = re.compile(rf"^\s*{re.escape(column)}\b", re.MULTILINE)
    assert pat.search(body), (
        f"decision_outputs.{column} is missing — Decision OS writes "
        "this on every persona evaluation."
    )


def test_decision_outputs_superseded_by_self_references(schema_sql):
    """``superseded_by`` must FK back to ``decision_outputs(id)`` so
    re-decisions form a versioned chain Decision OS can walk
    backwards through audit history."""
    body = _table_body(schema_sql, "decision_outputs")
    assert re.search(
        r"superseded_by\s+UUID\s+REFERENCES\s+decision_outputs\s*\(\s*id\s*\)",
        body, re.IGNORECASE,
    ), "superseded_by must REFERENCES decision_outputs(id)"


_DECISION_OUTPUTS_REQUIRED_INDEXES = [
    "idx_decision_outputs_unique",
    "idx_decision_outputs_app",
    "idx_decision_outputs_decision",
    "idx_decision_outputs_decided",
    "idx_decision_outputs_tenant",
    "idx_decision_outputs_pending_human",
]


@pytest.mark.parametrize("idx_name", _DECISION_OUTPUTS_REQUIRED_INDEXES)
def test_decision_outputs_index_exists(schema_sql, idx_name):
    pat = re.compile(
        rf"CREATE\s+(?:UNIQUE\s+)?INDEX\s+IF\s+NOT\s+EXISTS\s+{re.escape(idx_name)}\b",
        re.IGNORECASE,
    )
    assert pat.search(schema_sql), f"{idx_name} index is missing"


def test_decision_outputs_unique_index_covers_versioned_quad(schema_sql):
    """The (application_id, decision_id, version, tenant_id) unique
    constraint is what lets re-decisions UPSERT cleanly without
    overwriting prior versions."""
    assert re.search(
        r"CREATE\s+UNIQUE\s+INDEX\s+IF\s+NOT\s+EXISTS\s+"
        r"idx_decision_outputs_unique\s+ON\s+decision_outputs\s*\(\s*"
        r"application_id\s*,\s*decision_id\s*,\s*version\s*,\s*tenant_id\s*\)",
        schema_sql, re.IGNORECASE | re.DOTALL,
    ), "idx_decision_outputs_unique must be on (application_id, decision_id, version, tenant_id)"


def test_pending_human_index_carries_partial_predicate(schema_sql):
    """``idx_decision_outputs_pending_human`` is partial — the WHERE
    clause is what keeps it small as Decision OS scales. Drop the
    predicate and the index covers every row, defeating the point."""
    pat = re.compile(
        r"idx_decision_outputs_pending_human[^;]*?"
        r"WHERE\s+human_action\s+IS\s+NULL\s+AND\s+mode\s*=\s*'human_approval'",
        re.IGNORECASE | re.DOTALL,
    )
    assert pat.search(schema_sql), (
        "pending_human index must keep its partial predicate "
        "(human_action IS NULL AND mode = 'human_approval') — "
        "otherwise it indexes every row in the table."
    )


# ----- decision_timeline ----------------------------------------------------

_DECISION_TIMELINE_REQUIRED_COLUMNS = [
    "id",
    "application_id",
    "decision_id",
    "wave",
    "from_state",
    "to_state",
    "trigger",
    "transition_at",
    "time_in_prev_state_seconds",
    "cumulative_elapsed_seconds",
    "wave_elapsed_seconds",
    "waiting_on",
    "pipeline_position",
    "tenant_id",
]


def test_decision_timeline_table_is_defined(schema_sql):
    assert _table_body(schema_sql, "decision_timeline"), (
        "decision_timeline must be declared with CREATE TABLE IF NOT EXISTS"
    )


@pytest.mark.parametrize("column", _DECISION_TIMELINE_REQUIRED_COLUMNS)
def test_decision_timeline_carries_required_column(schema_sql, column):
    body = _table_body(schema_sql, "decision_timeline")
    pat = re.compile(rf"^\s*{re.escape(column)}\b", re.MULTILINE)
    assert pat.search(body), f"decision_timeline.{column} is missing"


_DECISION_TIMELINE_REQUIRED_INDEXES = [
    "idx_decision_timeline_app",
    "idx_decision_timeline_decision",
    "idx_decision_timeline_tenant",
]


@pytest.mark.parametrize("idx_name", _DECISION_TIMELINE_REQUIRED_INDEXES)
def test_decision_timeline_index_exists(schema_sql, idx_name):
    pat = re.compile(
        rf"CREATE\s+INDEX\s+IF\s+NOT\s+EXISTS\s+{re.escape(idx_name)}\b",
        re.IGNORECASE,
    )
    assert pat.search(schema_sql), f"{idx_name} index is missing"


# ----- vw_pipeline_status ---------------------------------------------------


_PIPELINE_STATUS_REQUIRED_ALIASES = [
    "decisions_complete",
    "decisions_total",
    "pipeline_pct",
    "current_wave",
    "has_block",
    "escalate_count",
    "pending_human_review",
    "pipeline_started",
    "last_decision_at",
    "pipeline_elapsed_seconds",
]


def test_pipeline_status_view_uses_create_or_replace(schema_sql):
    """``CREATE OR REPLACE VIEW`` so apply_schema can re-run on every
    ECS boot without erroring on the second tick."""
    assert re.search(
        r"CREATE\s+OR\s+REPLACE\s+VIEW\s+vw_pipeline_status\b",
        schema_sql, re.IGNORECASE,
    ), "vw_pipeline_status must use CREATE OR REPLACE VIEW"


def test_pipeline_status_view_sources_from_decision_outputs(schema_sql):
    """The view rolls up decision_outputs — NOT entity_states. Sourcing
    from anywhere else would change its semantics entirely."""
    # Find the view body (everything from CREATE OR REPLACE through ;)
    m = re.search(
        r"CREATE\s+OR\s+REPLACE\s+VIEW\s+vw_pipeline_status\s+AS\s+(?P<body>.*?);",
        schema_sql, re.IGNORECASE | re.DOTALL,
    )
    assert m, "vw_pipeline_status view not found"
    body = m.group("body")
    # Outer FROM (depth 0). Find every FROM and pick the one at depth 0.
    depth = 0
    found_outer = False
    for fm in re.finditer(r"\bFROM\b", body, re.IGNORECASE):
        depth = body.count("(", 0, fm.start()) - body.count(")", 0, fm.start())
        if depth == 0:
            after = body[fm.end():].strip()
            assert after.lower().startswith("decision_outputs"), (
                f"vw_pipeline_status outer FROM must be decision_outputs, "
                f"got `{after[:80]}...`"
            )
            found_outer = True
            break
    assert found_outer, "no depth-0 FROM clause found in vw_pipeline_status"


@pytest.mark.parametrize("alias", _PIPELINE_STATUS_REQUIRED_ALIASES)
def test_pipeline_status_view_projects_required_alias(schema_sql, alias):
    """Decision OS reads these column names directly off the view —
    removing one would silently break the management dashboard."""
    m = re.search(
        r"CREATE\s+OR\s+REPLACE\s+VIEW\s+vw_pipeline_status\s+AS\s+(?P<body>.*?);",
        schema_sql, re.IGNORECASE | re.DOTALL,
    )
    assert m
    body = m.group("body")
    pat = re.compile(rf"\bAS\s+{re.escape(alias)}\b", re.IGNORECASE)
    assert pat.search(body), (
        f"vw_pipeline_status missing alias `{alias}` — dashboard reads this by name"
    )


def test_pipeline_status_pending_human_filter_matches_partial_index_predicate(schema_sql):
    """The view's ``pending_human_review`` count must use the same
    predicate as the partial index on decision_outputs — otherwise the
    query planner can't use the index and the count slows linearly
    with table size."""
    m = re.search(
        r"CREATE\s+OR\s+REPLACE\s+VIEW\s+vw_pipeline_status\s+AS\s+(?P<body>.*?);",
        schema_sql, re.IGNORECASE | re.DOTALL,
    )
    assert m
    body = m.group("body")
    # The FILTER clause should mention all three predicates.
    assert "human_action IS NULL"   in body
    assert "human_approval"         in body
    assert "= 'recommend'"          in body


def test_pipeline_status_uses_max_version_subquery(schema_sql):
    """The whole point of the view is "latest version per
    (app, decision)". If someone simplifies away the subquery, every
    superseded re-decision starts being double-counted."""
    m = re.search(
        r"CREATE\s+OR\s+REPLACE\s+VIEW\s+vw_pipeline_status\s+AS\s+(?P<body>.*?);",
        schema_sql, re.IGNORECASE | re.DOTALL,
    )
    assert m
    body = m.group("body")
    assert re.search(
        r"version\s*=\s*\(\s*SELECT\s+MAX\s*\(\s*version\s*\)",
        body, re.IGNORECASE,
    ), "vw_pipeline_status must pick MAX(version) per (app, decision)"
