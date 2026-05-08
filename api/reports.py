"""Operational reports — Interface 2.

Cross-loan analytical queries for ops, compliance, and dashboards.
Hits Postgres directly (these aren't single-loan reads — Redis caching
sits at the *response* layer with a 5-minute TTL keyed on the params).
Pagination is LIMIT/OFFSET; every response carries total / page /
page_size / has_next so a client can page without keeping cursor state.
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from api.routes import (
    _CONDITIONAL_DOCS,
    _REQUIRED_DOCS,
    _slot_received,
    verify_api_key,
)
from core.graph.reconciler import (
    FIELD_CONFLICT_THRESHOLDS,
    NUMERIC_CONFLICT_THRESHOLD,
)

logger = logging.getLogger(__name__)

router = APIRouter()

# 5-minute TTL on every report response. Reports aggregate across many
# rows and accept a moderate staleness window — far longer than the
# context cache (30 min) would mask freshness, far shorter than the
# borrower-side caches (4h) since ops dashboards refresh on this cadence.
_REPORT_CACHE_TTL_SECONDS = 300

# Hard caps so a client can't accidentally request a 10k-row page or a
# year-long window that scans the entire document_relationships table.
_MAX_PAGE_SIZE = 200
_DEFAULT_PAGE_SIZE = 50
_MAX_DATE_RANGE_DAYS = 90


# ---------------------------------------------------------------------------
# Validation + cache helpers
# ---------------------------------------------------------------------------


def _parse_iso(value: Optional[str], field: str) -> Optional[datetime]:
    """Parse an ISO-8601 string, raising 422 on garbage input. ``None``
    is accepted (the endpoint default-fills the window)."""
    if value is None or value == "":
        return None
    try:
        # ``fromisoformat`` accepts both "2026-05-08" and full timestamps.
        # Z suffix isn't supported pre-3.11 — strip it.
        cleaned = value[:-1] + "+00:00" if value.endswith("Z") else value
        ts = datetime.fromisoformat(cleaned)
    except (ValueError, TypeError) as exc:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid {field}: expected ISO-8601, got {value!r} ({exc})",
        )
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


def _resolve_date_range(
    date_from: Optional[str],
    date_to: Optional[str],
) -> tuple[datetime, datetime]:
    """Default to the last 30 days if either bound is missing. Reject
    windows longer than _MAX_DATE_RANGE_DAYS so callers can't accidentally
    request the entire history."""
    now = datetime.now(timezone.utc)
    end = _parse_iso(date_to, "date_to") or now
    # If end is a date-only timestamp at 00:00, bump it by a day so the
    # caller's ``date_to=2026-05-08`` actually covers that whole day.
    if end.hour == 0 and end.minute == 0 and end.second == 0 and date_to and "T" not in date_to:
        end = end + timedelta(days=1)
    start = _parse_iso(date_from, "date_from") or (end - timedelta(days=30))
    if start >= end:
        raise HTTPException(
            status_code=422,
            detail=f"date_from ({start.isoformat()}) must be < date_to ({end.isoformat()})",
        )
    if (end - start) > timedelta(days=_MAX_DATE_RANGE_DAYS):
        raise HTTPException(
            status_code=422,
            detail=f"Date range exceeds {_MAX_DATE_RANGE_DAYS} days",
        )
    return start, end


def _validate_page_size(page_size: int) -> int:
    if page_size < 1 or page_size > _MAX_PAGE_SIZE:
        raise HTTPException(
            status_code=422,
            detail=f"page_size must be 1..{_MAX_PAGE_SIZE}",
        )
    return page_size


def _validate_page(page: int) -> int:
    if page < 1:
        raise HTTPException(status_code=422, detail="page must be >= 1")
    return page


def _cache_key(endpoint: str, params: dict) -> str:
    """Deterministic cache key from endpoint + sorted-param hash. Keeps
    keys readable under MONITOR while staying short enough for Redis."""
    blob = json.dumps(params, sort_keys=True, default=str).encode()
    digest = hashlib.sha256(blob).hexdigest()[:16]
    return f"report:{endpoint}:{digest}"


async def _cache_get(redis_store, key: str) -> Optional[dict]:
    try:
        raw = await redis_store._r.get(key)
        if raw:
            return json.loads(raw)
    except Exception as exc:
        logger.warning("report_cache_get_failed", extra={"key": key, "error": str(exc)})
    return None


async def _cache_set(redis_store, key: str, payload: dict) -> None:
    try:
        await redis_store._r.setex(
            key, _REPORT_CACHE_TTL_SECONDS, json.dumps(payload, default=str)
        )
    except Exception as exc:
        logger.warning("report_cache_set_failed", extra={"key": key, "error": str(exc)})


def _paginate_envelope(total: int, page: int, page_size: int) -> dict:
    return {
        "total":     int(total),
        "page":      page,
        "page_size": page_size,
        "has_next":  (page * page_size) < int(total),
    }


def _coerce_float(value: Any) -> Optional[float]:
    """Best-effort numeric coercion. Handles plain ints/floats, asyncpg
    NUMERIC columns (returned as Decimal), and JSONB strings — including
    chaos-test garbage like ``box1_wages='one hundred ten thousand'``,
    which silently returns None instead of raising. Booleans are
    coerced to 0/1 by float() incidentally; we explicitly return None
    for them so a caller treating None-vs-zero differently isn't
    confused."""
    from decimal import Decimal
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float, Decimal)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.replace(",", "").replace("$", "").strip())
        except (ValueError, AttributeError):
            return None
    return None


def _ctx_get(ctx: Any, *path: str, default: Any = None) -> Any:
    """Walk a (possibly None) nested dict by keys, defaulting on miss."""
    if not isinstance(ctx, dict):
        return default
    cur: Any = ctx
    for key in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
    return cur if cur is not None else default


# Number of readiness flag fields tracked on ApplicationContext.readiness.
# Hard-coded so the endpoint reports a stable denominator even when the
# stored snapshot is missing newer flags. Mirrors core.context.models
# .ReadinessFlags (excluding the missing_items list).
_READINESS_FLAG_FIELDS = (
    "income_verified", "credit_pulled", "identity_verified",
    "employment_verified", "assets_verified", "identity_complete",
    "tax_docs_received", "appraisal_complete", "title_clear",
    "title_received", "insurance_bound", "flood_cert_received",
    "dti_calculable", "ltv_calculable", "aus_ready",
    "loan_application_complete", "purchase_agreement_received",
    "rate_locked", "no_critical_conflicts",
)
_READINESS_FLAGS_TOTAL = len(_READINESS_FLAG_FIELDS)


def _readiness_true_count(ctx: Any) -> int:
    readiness = _ctx_get(ctx, "readiness", default={}) or {}
    if not isinstance(readiness, dict):
        return 0
    return sum(1 for f in _READINESS_FLAG_FIELDS if bool(readiness.get(f)))


# ---------------------------------------------------------------------------
# 1. /reports/pipeline
# ---------------------------------------------------------------------------


@router.get(
    "/pipeline",
    dependencies=[Depends(verify_api_key)],
)
async def report_pipeline(
    request: Request,
    status: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(_DEFAULT_PAGE_SIZE, ge=1, le=_MAX_PAGE_SIZE),
):
    """Per-loan summary across the active pipeline."""
    page = _validate_page(page)
    page_size = _validate_page_size(page_size)
    start, end = _resolve_date_range(date_from, date_to)

    pg = request.app.state.postgres_store
    redis = request.app.state.redis_store

    cache_key = _cache_key("pipeline", {
        "status": status, "from": start.isoformat(), "to": end.isoformat(),
        "page": page, "page_size": page_size,
    })
    cached = await _cache_get(redis, cache_key)
    if cached is not None:
        return cached

    offset = (page - 1) * page_size
    total = await pg.count_pipeline_report(start, end, status)
    rows = await pg.get_pipeline_report(start, end, status, page_size, offset)

    # documents_received counts the *populated slots* against the
    # required catalog — not the raw doc count — so a loan with 43
    # rows but missing CREDIT_REPORT shows < 100% rather than 100%
    # (which a raw count would, since 43 >> 15). Slot fulfillment is
    # the same metric /reports/completeness uses; sharing it keeps
    # the two reports consistent.
    expected = len(_REQUIRED_DOCS)
    applications: list[dict] = []
    for row in rows:
        ctx = row.get("context_data")
        if isinstance(ctx, str):
            try:
                ctx = json.loads(ctx)
            except Exception:
                ctx = None

        income = row.get("income_data") or {}
        if isinstance(income, str):
            try:
                income = json.loads(income)
            except Exception:
                income = {}

        have = set(row.get("doc_types") or [])
        required_filled = sum(1 for s in _REQUIRED_DOCS if _slot_received(s, have))
        completeness_pct = round(required_filled / expected * 100, 1) \
            if expected else 0.0
        docs_received = int(row.get("docs_received") or 0)
        qualifying_monthly = (
            _coerce_float(_ctx_get(ctx, "combined_qualifying_monthly"))
            or _coerce_float(income.get("qualifying_monthly"))
            or _coerce_float(_ctx_get(income, "primary", "qualifying_monthly"))
        )

        applications.append({
            "application_id":          row.get("application_id"),
            "los_id":                  row.get("los_id"),
            "status":                  row.get("status"),
            "borrower_name":           row.get("borrower_name"),
            "co_borrower_name":        row.get("co_borrower_name"),
            "loan_amount":             _coerce_float(row.get("loan_amount")),
            "interest_rate":           _coerce_float(row.get("interest_rate")),
            "documents_received":      required_filled,
            "documents_expected":      expected,
            "completeness_pct":        completeness_pct,
            "documents_total":         docs_received,
            "readiness_flags_true":    _readiness_true_count(ctx),
            "readiness_flags_total":   _READINESS_FLAGS_TOTAL,
            "conflict_count":          int(row.get("conflict_count") or 0),
            "critical_conflict_count": int(row.get("critical_conflict_count") or 0),
            "qualifying_monthly_income": qualifying_monthly,
            "mid_credit_score":        row.get("mid_score"),
            "ltv":                     _coerce_float(_ctx_get(ctx, "ltv")),
            "front_end_dti":           _coerce_float(_ctx_get(ctx, "front_end_dti")),
            "back_end_dti":            _coerce_float(_ctx_get(ctx, "back_end_dti")),
            "created_at":              row.get("created_at"),
            "last_doc_received_at":    row.get("last_doc_received_at"),
        })

    payload = {
        "applications": applications,
        **_paginate_envelope(total, page, page_size),
        "filters": {
            "status":    status,
            "date_from": start.isoformat(),
            "date_to":   end.isoformat(),
        },
    }
    await _cache_set(redis, cache_key, payload)
    return payload


# ---------------------------------------------------------------------------
# 2. /reports/conflicts
# ---------------------------------------------------------------------------


def _is_critical_edge(edge: dict) -> bool:
    """``severity=critical`` filter — the edge's delta_pct exceeds the
    field-pair threshold from FIELD_CONFLICT_THRESHOLDS (defaulting to
    NUMERIC_CONFLICT_THRESHOLD = 10%). All ``contradicts`` edges already
    cross *some* threshold (that's how they got emitted), but the
    per-pair dict is the authoritative cutoff for "ops should look at
    this now"."""
    delta = edge.get("delta_pct")
    if delta is None:
        # Non-numeric (string-fuzzy) contradictions are inherently
        # critical — there's no delta to compare, the value diverges.
        return True
    field_combined = edge.get("field_compared") or edge.get("field_name") or ""
    field_a = field_combined.split("↔")[0] if "↔" in field_combined else field_combined
    src = edge.get("source_doc_type") or ""
    tgt = edge.get("target_doc_type") or ""
    threshold_frac = (
        FIELD_CONFLICT_THRESHOLDS.get((src, tgt, field_a))
        or FIELD_CONFLICT_THRESHOLDS.get((tgt, src, field_a))
        or NUMERIC_CONFLICT_THRESHOLD
    )
    try:
        return float(delta) >= threshold_frac * 100
    except (TypeError, ValueError):
        return True


@router.get(
    "/conflicts",
    dependencies=[Depends(verify_api_key)],
)
async def report_conflicts(
    request: Request,
    severity: str = Query("all", pattern="^(all|critical)$"),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    min_delta_pct: Optional[float] = Query(None, ge=0, le=100),
    page: int = Query(1, ge=1),
    page_size: int = Query(_DEFAULT_PAGE_SIZE, ge=1, le=_MAX_PAGE_SIZE),
):
    page = _validate_page(page)
    page_size = _validate_page_size(page_size)
    start, end = _resolve_date_range(date_from, date_to)

    pg = request.app.state.postgres_store
    redis = request.app.state.redis_store

    cache_key = _cache_key("conflicts", {
        "severity": severity, "from": start.isoformat(), "to": end.isoformat(),
        "min_delta": min_delta_pct, "page": page, "page_size": page_size,
    })
    cached = await _cache_get(redis, cache_key)
    if cached is not None:
        return cached

    if severity == "critical":
        # Apply the per-pair threshold filter in Python — doing it in
        # SQL would mean encoding the entire FIELD_CONFLICT_THRESHOLDS
        # dict as a CASE expression. Since severity=critical is rare
        # and the result set is bounded by date window, a fetch-then-
        # filter is fine. We over-fetch the page to keep the post-
        # filter slice from going under page_size.
        all_rows = await pg.get_conflicts_report(
            start, end, min_delta_pct, limit=page_size * 4, offset=0,
        )
        filtered_rows = [
            r for r in all_rows
            if _is_critical_edge({**r, "field_compared": r.get("field_name")})
        ]
        total = len(filtered_rows)
        offset = (page - 1) * page_size
        page_rows = filtered_rows[offset : offset + page_size]
    else:
        offset = (page - 1) * page_size
        total = await pg.count_conflicts_report(start, end, min_delta_pct)
        page_rows = await pg.get_conflicts_report(
            start, end, min_delta_pct, page_size, offset,
        )

    conflicts: list[dict] = []
    for r in page_rows:
        conflicts.append({
            "application_id":   r.get("application_id"),
            "los_id":           r.get("los_id"),
            "borrower_name":    r.get("borrower_name"),
            "applicant_id":     r.get("applicant_id"),
            "source_doc_type":  r.get("source_doc_type"),
            "target_doc_type":  r.get("target_doc_type"),
            "field_compared":   r.get("field_name"),
            "source_value":     r.get("source_value"),
            "target_value":     r.get("target_value"),
            "delta_pct":        _coerce_float(r.get("delta_pct")),
            "relationship_type": r.get("relationship_type"),
            "confidence":       _coerce_float(r.get("confidence")),
            "created_at":       r.get("created_at"),
        })

    payload = {
        "conflicts": conflicts,
        **_paginate_envelope(total, page, page_size),
        "filters": {
            "severity":  severity,
            "date_from": start.isoformat(),
            "date_to":   end.isoformat(),
            "min_delta_pct": min_delta_pct,
        },
    }
    await _cache_set(redis, cache_key, payload)
    return payload


# ---------------------------------------------------------------------------
# 3. /reports/completeness
# ---------------------------------------------------------------------------


@router.get(
    "/completeness",
    dependencies=[Depends(verify_api_key)],
)
async def report_completeness(
    request: Request,
    threshold: float = Query(80.0, ge=0, le=100),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(_DEFAULT_PAGE_SIZE, ge=1, le=_MAX_PAGE_SIZE),
):
    """Applications below the completeness threshold (default 80%) with
    the missing-required + missing-conditional doc lists. The threshold
    is applied in Python because completeness depends on the slot-vs-
    alternates catalog (W2_PRIOR alternates W2_CURRENT etc.) — encoding
    that in SQL would be brittle."""
    page = _validate_page(page)
    page_size = _validate_page_size(page_size)
    start, end = _resolve_date_range(date_from, date_to)

    pg = request.app.state.postgres_store
    redis = request.app.state.redis_store

    cache_key = _cache_key("completeness", {
        "threshold": threshold, "from": start.isoformat(), "to": end.isoformat(),
        "page": page, "page_size": page_size,
    })
    cached = await _cache_get(redis, cache_key)
    if cached is not None:
        return cached

    rows = await pg.get_applications_with_doc_types(start, end)

    expected = len(_REQUIRED_DOCS)
    below: list[dict] = []
    for row in rows:
        have = set(row.get("doc_types") or [])
        missing_required = [
            s["doc_type"] for s in _REQUIRED_DOCS if not _slot_received(s, have)
        ]
        missing_conditional = [
            s["doc_type"] for s in _CONDITIONAL_DOCS if not _slot_received(s, have)
        ]
        documents_received = max(expected - len(missing_required), 0)
        completeness_pct = round(documents_received / expected * 100, 1) \
            if expected else 0.0
        if completeness_pct >= threshold:
            continue
        below.append({
            "application_id":      row.get("application_id"),
            "los_id":               row.get("los_id"),
            "completeness_pct":     completeness_pct,
            "documents_received":   documents_received,
            "documents_expected":   expected,
            "missing_required":     missing_required,
            "missing_conditional":  missing_conditional,
            "created_at":           row.get("created_at"),
        })

    # Sort lowest-completeness first — that's the operations queue order.
    below.sort(key=lambda r: r["completeness_pct"])
    total = len(below)
    offset = (page - 1) * page_size
    page_rows = below[offset : offset + page_size]

    payload = {
        "applications": page_rows,
        **_paginate_envelope(total, page, page_size),
        "filters": {
            "threshold": threshold,
            "date_from": start.isoformat(),
            "date_to":   end.isoformat(),
        },
    }
    await _cache_set(redis, cache_key, payload)
    return payload


# ---------------------------------------------------------------------------
# 4. /reports/extraction-quality
# ---------------------------------------------------------------------------


@router.get(
    "/extraction-quality",
    dependencies=[Depends(verify_api_key)],
)
async def report_extraction_quality(
    request: Request,
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
):
    start, end = _resolve_date_range(date_from, date_to)

    pg = request.app.state.postgres_store
    redis = request.app.state.redis_store

    cache_key = _cache_key("extraction-quality", {
        "from": start.isoformat(), "to": end.isoformat(),
    })
    cached = await _cache_get(redis, cache_key)
    if cached is not None:
        return cached

    totals = await pg.get_extraction_method_totals(start, end)
    by_type_rows = await pg.get_extraction_method_by_doc_type(start, end)

    total = int(totals.get("total") or 0)
    deterministic = int(totals.get("deterministic") or 0)
    caller_supplied = int(totals.get("caller_supplied") or 0)
    ai_vision = int(totals.get("ai_vision") or 0)
    none_method = int(totals.get("none_method") or 0)

    by_doc_type = [
        {
            "doc_type":         r.get("document_type"),
            "total":            int(r.get("total") or 0),
            "deterministic":    int(r.get("deterministic") or 0),
            "caller_supplied":  int(r.get("caller_supplied") or 0),
            "ai_vision":        int(r.get("ai_vision") or 0),
            "none":             int(r.get("none_method") or 0),
        }
        for r in by_type_rows
    ]

    payload = {
        "total_documents": total,
        "by_method": {
            "deterministic":   deterministic,
            "caller_supplied": caller_supplied,
            "ai_vision":       ai_vision,
            "none":            none_method,
        },
        "by_doc_type": by_doc_type,
        "ai_extraction_rate":    round(ai_vision / total * 100, 1) if total else 0.0,
        "empty_extraction_rate": round(none_method / total * 100, 1) if total else 0.0,
        "period": {
            "from": start.isoformat(),
            "to":   end.isoformat(),
        },
    }
    await _cache_set(redis, cache_key, payload)
    return payload


# ---------------------------------------------------------------------------
# 5. /reports/income-verification
# ---------------------------------------------------------------------------


@router.get(
    "/income-verification",
    dependencies=[Depends(verify_api_key)],
)
async def report_income_verification(
    request: Request,
    min_delta: float = Query(10.0, ge=0, le=100),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(_DEFAULT_PAGE_SIZE, ge=1, le=_MAX_PAGE_SIZE),
):
    """Stated (URLA section 1c) vs documented (W2 box1) income mismatch.

    The URLA monthly_income_stated × 12 is compared against
    W2_CURRENT.box1_wages. ``min_delta`` is the percent-divergence
    floor: 10 means surface only loans where stated diverges from
    documented by at least 10%."""
    page = _validate_page(page)
    page_size = _validate_page_size(page_size)
    start, end = _resolve_date_range(date_from, date_to)

    pg = request.app.state.postgres_store
    redis = request.app.state.redis_store

    cache_key = _cache_key("income-verification", {
        "min_delta": min_delta, "from": start.isoformat(), "to": end.isoformat(),
        "page": page, "page_size": page_size,
    })
    cached = await _cache_get(redis, cache_key)
    if cached is not None:
        return cached

    rows = await pg.get_income_verification_data(start, end)

    discrepancies: list[dict] = []
    for r in rows:
        monthly_stated = _coerce_float(r.get("monthly_stated_raw"))
        documented_annual = _coerce_float(r.get("w2_wages_raw"))
        if not monthly_stated or not documented_annual:
            continue
        stated_annual = monthly_stated * 12
        denom = max(stated_annual, documented_annual)
        if denom <= 0:
            continue
        delta_pct = abs(stated_annual - documented_annual) / denom * 100
        if delta_pct < min_delta:
            continue
        flag = (
            "stated_above_documented" if stated_annual > documented_annual
            else "documented_above_stated"
        )
        discrepancies.append({
            "application_id":     r.get("application_id"),
            "los_id":             r.get("los_id"),
            "applicant_id":       r.get("applicant_id"),
            "borrower_name":      r.get("borrower_name"),
            "stated_monthly":     round(monthly_stated, 2),
            "stated_annual":      round(stated_annual, 2),
            "documented_monthly": round(documented_annual / 12, 2),
            "documented_annual":  round(documented_annual, 2),
            "delta_pct":          round(delta_pct, 2),
            "source_docs":        ["URLA_1003", "W2_CURRENT"],
            "flag":               flag,
        })

    discrepancies.sort(key=lambda d: d["delta_pct"], reverse=True)
    total = len(discrepancies)
    offset = (page - 1) * page_size
    page_rows = discrepancies[offset : offset + page_size]

    payload = {
        "discrepancies": page_rows,
        **_paginate_envelope(total, page, page_size),
        "filters": {
            "min_delta": min_delta,
            "date_from": start.isoformat(),
            "date_to":   end.isoformat(),
        },
    }
    await _cache_set(redis, cache_key, payload)
    return payload
