"""Bulk Export API — Interface 3.

Daily / monthly full or incremental dumps of the EDMS knowledge graph
in formats data warehouses (Snowflake, Redshift, BigQuery) ingest
directly. Every endpoint is a streaming response — JSONL or CSV — backed
by a server-side asyncpg cursor so a multi-thousand-row pull never
materializes the full result set in Python memory.

Pull cadence is encoded as ``?since=<ISO timestamp>``: omit for a full
snapshot, supply the prior pull's watermark for an incremental. The
companion ``/export/watermark`` POST/GET lets the consumer persist its
own watermark on the server so it can resume after a restart without
keeping client-side state.

Rate limit: 10 export requests per hour per API key, tracked via Redis
INCR + 1h EXPIRE.
"""
from __future__ import annotations

import csv
import io
import json
import logging
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from api.routes import verify_api_key

logger = logging.getLogger(__name__)
router = APIRouter()


# Reusable response-code documentation. The streaming endpoints don't
# carry a JSON response_model, so the 200 description here doubles as
# the OpenAPI documentation for the streamed body — that's why every
# 200 entry below mentions the media type explicitly.
_EXPORT_RESPONSES: dict = {
    401: {"description": "Missing or invalid `X-API-Key`."},
    422: {"description": "Validation error (bad format, ISO timestamp, or relationship_type / profile_type / include token)."},
    429: {"description": "Rate limit exceeded — 10 export requests per hour per API key. Returns `Retry-After` in seconds."},
}


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_RATE_LIMIT_PER_HOUR = 10
_RATE_LIMIT_TTL_SECONDS = 3600
_DEFAULT_FORMAT = "jsonl"
_VALID_FORMATS = ("jsonl", "csv")
_VALID_REL_TYPES = ("confirms", "contradicts", "corroborates", "supersedes", "references")
_VALID_PROFILE_TYPES = ("income", "credit", "all")
_CSV_FLUSH_EVERY = 100  # rows


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_iso(value: Optional[str], field: str) -> Optional[datetime]:
    if value is None or value == "":
        return None
    try:
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


def _validate_format(fmt: str) -> str:
    if fmt not in _VALID_FORMATS:
        raise HTTPException(
            status_code=422,
            detail=f"format must be one of {_VALID_FORMATS}, got {fmt!r}",
        )
    return fmt


def _json_default(o: Any):
    """JSON serializer for asyncpg/Decimal/datetime values that the
    standard ``json`` module can't handle natively."""
    from decimal import Decimal
    if isinstance(o, datetime):
        return o.isoformat()
    if isinstance(o, Decimal):
        return float(o)
    try:
        from datetime import date as _date
        if isinstance(o, _date):
            return o.isoformat()
    except Exception:
        pass
    import uuid as _uuid
    if isinstance(o, _uuid.UUID):
        return str(o)
    return str(o)


def _ensure_dict(value: Any) -> dict:
    """JSONB columns can come back from asyncpg as either dict or
    JSON-encoded string depending on whether we're hitting real Postgres
    or the in-memory fake. Normalize both to dict."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value) or {}
        except Exception:
            return {}
    return {}


async def _enforce_rate_limit(redis_store, api_key: str, endpoint: str) -> None:
    """Increment ``export_rate:{api_key}`` once per request; reject when
    the bucket already shows >= _RATE_LIMIT_PER_HOUR. The bucket lives
    for an hour from its first INCR so the next hour's quota refills
    cleanly. A redis outage (returning None / raising) MUST NOT block
    legitimate traffic — bulk exports are SLA-critical for downstream
    DWH pipelines, so we fail open on the limit and just log a warning."""
    key = f"export_rate:{api_key}"
    try:
        client = redis_store._r
        count = await client.incr(key)
        if int(count) == 1:
            await client.expire(key, _RATE_LIMIT_TTL_SECONDS)
        if int(count) > _RATE_LIMIT_PER_HOUR:
            ttl = await client.ttl(key)
            raise HTTPException(
                status_code=429,
                detail=(
                    f"Export rate limit exceeded: {_RATE_LIMIT_PER_HOUR}/hr; "
                    f"retry after {ttl}s"
                ),
                headers={"Retry-After": str(max(int(ttl or 1), 1))},
            )
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning(
            "export_rate_limit_check_failed",
            extra={"endpoint": endpoint, "error": str(exc)},
        )


def _export_headers(
    since: Optional[datetime],
    row_count_holder: list[int],
    filename_stem: str,
    fmt: str,
) -> dict:
    """Build the response-header dict. ``row_count_holder`` is a list
    we mutate during streaming so the caller can read the final count
    after the generator finishes — but we set the header at response
    construction time using the live mutable count. FastAPI snapshots
    headers at response start, so the count header is best-effort:
    it reflects 0 for streamed responses (rows are written *after* the
    headers are flushed). We surface ``X-Export-Row-Count`` as the
    intent-to-stream count via a trailer-style logging line instead."""
    suffix = "ndjson" if fmt == "jsonl" else "csv"
    return {
        "X-Export-Since":        since.isoformat() if since else "full",
        "X-Export-Generated-At": datetime.now(timezone.utc).isoformat(),
        "Content-Disposition":   (
            f'attachment; filename="{filename_stem}_'
            f'{datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")}.{suffix}"'
        ),
    }


def _media_type(fmt: str) -> str:
    return "application/x-ndjson" if fmt == "jsonl" else "text/csv"


# ---------------------------------------------------------------------------
# Streaming primitives
# ---------------------------------------------------------------------------


async def _stream_jsonl(
    rows: AsyncIterator[dict],
    transform,
    counter: list[int],
) -> AsyncIterator[str]:
    """Wrap an async-iter of raw rows with a per-row transform and emit
    JSONL. ``counter[0]`` is bumped per emitted row so callers can log
    the total when the generator drains."""
    async for raw in rows:
        record = transform(raw)
        counter[0] += 1
        yield json.dumps(record, default=_json_default, separators=(",", ":")) + "\n"


async def _stream_csv(
    rows: AsyncIterator[dict],
    transform,
    columns: list[str],
    counter: list[int],
) -> AsyncIterator[str]:
    """Emit a CSV header + one line per row. Uses ``csv.writer`` against
    a StringIO buffer that is reset every row, so quoting/escaping
    matches RFC 4180 — important for fields like extracted_fields that
    may carry commas or newlines when serialized as a JSON string."""
    buf = io.StringIO()
    writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(columns)
    yield buf.getvalue()
    buf.seek(0)
    buf.truncate(0)

    rows_since_flush = 0
    async for raw in rows:
        record = transform(raw)
        writer.writerow([_csv_cell(record.get(c)) for c in columns])
        counter[0] += 1
        rows_since_flush += 1
        if rows_since_flush >= _CSV_FLUSH_EVERY:
            yield buf.getvalue()
            buf.seek(0)
            buf.truncate(0)
            rows_since_flush = 0
    if buf.tell():
        yield buf.getvalue()


def _csv_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=_json_default, separators=(",", ":"))
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


# ---------------------------------------------------------------------------
# Per-endpoint row transforms
# ---------------------------------------------------------------------------


_ENTITY_CSV_COLUMNS = [
    "applicant_id", "application_id", "role", "name",
    "qualifying_monthly", "mid_score", "monthly_payments",
    "total_liquid", "total_retirement", "gift_funds",
    "dl_verified", "ssn_verified", "ofac_clear", "identity_complete",
    "document_count", "conflict_count", "updated_at",
]


def _entity_record(row: dict, include: set[str]) -> dict:
    """Shape one applicant row into the spec's nested entity shape.
    Drops sub-bucket keys not in ``include`` so the consumer sees only
    what they asked for."""
    income = _ensure_dict(row.get("income_data"))
    credit = _ensure_dict(row.get("credit_data"))

    primary = income.get("primary_borrower") or {}
    co = income.get("co_borrower") or {}
    if row.get("role") == "co_borrower":
        bucket = co or primary
    else:
        bucket = primary or income

    sources = sorted({
        s.get("source_type")
        for s in (bucket.get("sources") or [])
        if s.get("source_type")
    })

    obligations = credit.get("monthly_obligations")
    if isinstance(obligations, list):
        monthly_payments = sum(
            float(o.get("monthly_payment") or 0)
            for o in obligations
            if isinstance(o, dict)
        )
    else:
        try:
            monthly_payments = float(obligations) if obligations is not None else 0.0
        except (TypeError, ValueError):
            monthly_payments = 0.0

    dl_v   = bool(row.get("dl_verified"))
    ssn_v  = bool(row.get("ssn_verified"))
    ofac_v = bool(row.get("ofac_clear"))

    record = {
        "applicant_id":   row.get("applicant_id"),
        "application_id": row.get("application_id"),
        "role":           row.get("role"),
        "name":           row.get("full_name"),
        "document_count": int(row.get("document_count") or 0),
        "conflict_count": int(row.get("conflict_count") or 0),
        "updated_at":     row.get("updated_at"),
    }
    if "income" in include:
        record["income"] = {
            "qualifying_monthly": float(bucket.get("qualifying_monthly") or 0),
            "sources":            sources,
        }
    if "credit" in include:
        record["credit"] = {
            "mid_score":        int(row.get("mid_score") or 0) or None,
            "credit_band":      credit.get("credit_band"),
            "monthly_payments": round(monthly_payments, 2),
        }
    if "assets" in include:
        record["assets"] = {
            "total_liquid":     float(row.get("total_liquid") or 0),
            "total_retirement": float(row.get("total_retirement") or 0),
            "gift_funds":       float(row.get("gift_funds") or 0),
        }
    if "identity" in include:
        record["identity"] = {
            "dl_verified":  dl_v,
            "ssn_verified": ssn_v,
            "ofac_clear":   ofac_v,
            "complete":     dl_v and ssn_v and ofac_v,
        }
    return record


def _entity_csv_row(row: dict, include: set[str]) -> dict:
    rec = _entity_record(row, include={"income", "credit", "assets", "identity"})
    flat = {
        "applicant_id":   rec["applicant_id"],
        "application_id": rec["application_id"],
        "role":           rec["role"],
        "name":           rec["name"],
        "qualifying_monthly": rec.get("income", {}).get("qualifying_monthly"),
        "mid_score":          rec.get("credit", {}).get("mid_score"),
        "monthly_payments":   rec.get("credit", {}).get("monthly_payments"),
        "total_liquid":       rec.get("assets", {}).get("total_liquid"),
        "total_retirement":   rec.get("assets", {}).get("total_retirement"),
        "gift_funds":         rec.get("assets", {}).get("gift_funds"),
        "dl_verified":        rec.get("identity", {}).get("dl_verified"),
        "ssn_verified":       rec.get("identity", {}).get("ssn_verified"),
        "ofac_clear":         rec.get("identity", {}).get("ofac_clear"),
        "identity_complete":  rec.get("identity", {}).get("complete"),
        "document_count":     rec["document_count"],
        "conflict_count":     rec["conflict_count"],
        "updated_at":         rec["updated_at"],
    }
    return flat


_DOCUMENT_CSV_COLUMNS = [
    "document_id", "applicant_id", "application_id",
    "doc_type", "category", "borrower_role",
    "status", "extraction_method", "confidence_score",
    "extracted_fields", "received_at", "is_current",
]


def _document_record(row: dict) -> dict:
    fields = _ensure_dict(row.get("extracted_fields"))
    return {
        "document_id":       row.get("document_id"),
        "applicant_id":      row.get("applicant_id"),
        "application_id":    row.get("application_id"),
        "doc_type":          row.get("document_type"),
        "category":          row.get("document_category"),
        "borrower_role":     row.get("borrower_role"),
        "status":            row.get("status"),
        "extraction_method": row.get("extraction_method"),
        "confidence_score":  row.get("confidence_score"),
        "extracted_fields":  fields,
        "received_at":       row.get("received_at"),
        "is_current":        bool(row.get("is_current")),
        "expiry_date":       row.get("expiry_date"),
        "s3_key":            row.get("s3_key"),
    }


_GRAPH_CSV_COLUMNS = [
    "relationship_id", "applicant_id", "source_doc_id", "target_doc_id",
    "relationship_type", "field_compared", "source_value", "target_value",
    "delta_pct", "confidence", "created_at",
]


def _graph_record(row: dict) -> dict:
    return {
        "relationship_id":   row.get("relationship_id"),
        "applicant_id":      row.get("applicant_id"),
        "source_doc_id":     row.get("source_doc_id"),
        "target_doc_id":     row.get("target_doc_id"),
        "relationship_type": row.get("relationship_type"),
        "field_compared":    row.get("field_name"),
        "source_value":      row.get("source_value"),
        "target_value":      row.get("target_value"),
        "delta_pct":         row.get("delta_pct"),
        "confidence":        row.get("confidence"),
        "reasoning":         row.get("reasoning"),
        "created_at":        row.get("created_at"),
    }


_PROFILE_CSV_COLUMNS = [
    "profile_kind", "profile_id", "applicant_id", "application_id",
    "qualifying_monthly", "mid_score", "credit_band",
    "assembled_at", "version", "lineage_hash",
]


def _income_profile_record(row: dict) -> dict:
    data = _ensure_dict(row.get("profile_data"))
    return {
        "profile_kind":   "income",
        "profile_id":      str(row.get("profile_id")) if row.get("profile_id") else None,
        "applicant_id":    row.get("applicant_id"),
        "application_id":  row.get("application_id"),
        "qualifying_monthly": float(data.get("qualifying_monthly") or 0),
        "mid_score":       None,
        "credit_band":     None,
        "assembled_at":    row.get("assembled_at"),
        "version":         row.get("version"),
        "lineage_hash":    row.get("lineage_hash"),
        "profile_data":    data,
    }


def _credit_profile_record(row: dict) -> dict:
    data = _ensure_dict(row.get("profile_data"))
    return {
        "profile_kind":   "credit",
        "profile_id":      str(row.get("profile_id")) if row.get("profile_id") else None,
        "applicant_id":    row.get("applicant_id"),
        "application_id":  None,
        "qualifying_monthly": None,
        "mid_score":       row.get("mid_score"),
        "credit_band":     row.get("credit_band"),
        "assembled_at":    row.get("created_at"),
        "version":         None,
        "lineage_hash":    None,
        "report_date":     row.get("report_date"),
        "expiry_date":     row.get("expiry_date"),
        "profile_data":    data,
    }


_APPLICATION_CSV_COLUMNS = [
    "application_id", "los_id", "status",
    "applicant_id", "co_applicant_id",
    "borrower_name", "co_borrower_name",
    "loan_amount", "interest_rate", "loan_term_months",
    "loan_purpose", "loan_type", "occupancy",
    "document_count", "conflict_count",
    "ltv", "front_end_dti", "back_end_dti",
    "readiness_summary",
    "created_at", "updated_at",
]


def _application_record(row: dict) -> dict:
    ctx = _ensure_dict(row.get("context_data"))
    readiness = _ensure_dict(ctx.get("readiness"))
    flag_count = sum(1 for v in readiness.values() if v is True)
    flag_total = sum(1 for v in readiness.values() if isinstance(v, bool))
    return {
        "application_id":   row.get("application_id"),
        "los_id":            row.get("los_id"),
        "status":            row.get("status"),
        "applicant_id":      row.get("applicant_id"),
        "co_applicant_id":   row.get("co_applicant_id"),
        "borrower_name":     row.get("borrower_name"),
        "co_borrower_name":  row.get("co_borrower_name"),
        "loan_amount":       row.get("loan_amount"),
        "interest_rate":     row.get("interest_rate"),
        "loan_term_months":  row.get("loan_term_months"),
        "loan_purpose":      row.get("loan_purpose"),
        "loan_type":         row.get("loan_type"),
        "occupancy":         row.get("occupancy"),
        "document_count":    int(row.get("document_count") or 0),
        "conflict_count":    int(row.get("conflict_count") or 0),
        "ltv":               ctx.get("ltv"),
        "front_end_dti":     ctx.get("front_end_dti"),
        "back_end_dti":      ctx.get("back_end_dti"),
        "readiness_summary": f"{flag_count}/{flag_total}" if flag_total else "0/0",
        "readiness":         readiness,
        "created_at":        row.get("created_at"),
        "updated_at":        row.get("updated_at"),
    }


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


def _streaming_response(
    body: AsyncIterator[str],
    fmt: str,
    headers: dict,
) -> StreamingResponse:
    return StreamingResponse(
        body,
        media_type=_media_type(fmt),
        headers=headers,
    )


@router.get(
    "/entities",
    dependencies=[Depends(verify_api_key)],
    summary="Streaming export of applicant entities",
    description=(
        "One row per applicant joined with the latest income + credit + "
        "asset + identity aggregates. Returns a streaming response — JSONL "
        "(`application/x-ndjson`, one JSON object per newline) or CSV "
        "(`text/csv`). Supply `since` for an incremental export keyed on "
        "`applicants.updated_at`; omit it for a full snapshot. Use "
        "`include` to select a subset of the four sub-buckets."
    ),
    responses={
        **_EXPORT_RESPONSES,
        200: {
            "description": "Streaming JSONL or CSV body; one record per line.",
            "content": {
                "application/x-ndjson": {"example":
                    '{"applicant_id":"APL-00316-P","application_id":"APP-LOS-12345",'
                    '"role":"primary","name":"Alex Martinez","document_count":41,'
                    '"conflict_count":7,"updated_at":"2026-05-08T15:18:52+00:00",'
                    '"income":{"qualifying_monthly":10416.67,"sources":["RENTAL","W2_SALARIED"]},'
                    '"credit":{"mid_score":752,"credit_band":"prime","monthly_payments":0},'
                    '"assets":{"total_liquid":176500.0,"total_retirement":165000.0,"gift_funds":20000.0},'
                    '"identity":{"dl_verified":true,"ssn_verified":true,"ofac_clear":true,"complete":true}}\n'
                },
                "text/csv": {"example":
                    "applicant_id,application_id,role,name,qualifying_monthly,mid_score,"
                    "monthly_payments,total_liquid,total_retirement,gift_funds,"
                    "dl_verified,ssn_verified,ofac_clear,identity_complete,"
                    "document_count,conflict_count,updated_at\n"
                    "APL-00316-P,APP-LOS-12345,primary,Alex Martinez,10416.67,752,0,"
                    "176500.0,165000.0,20000.0,true,true,true,true,41,7,"
                    "2026-05-08T15:18:52+00:00\n"
                },
            },
        },
    },
)
async def export_entities(
    request: Request,
    format: str = Query(_DEFAULT_FORMAT, description="`jsonl` or `csv`."),
    since: Optional[str] = Query(None, description="Incremental cutoff (ISO 8601). Omit for a full snapshot."),
    include: str = Query(
        "income,credit,assets,identity",
        description="Comma-separated subset of `income,credit,assets,identity`. JSONL only — CSV always emits all columns.",
    ),
):
    x_api_key = request.headers.get("X-API-Key") or "anon"
    fmt = _validate_format(format)
    since_ts = _parse_iso(since, "since")

    requested = {p.strip() for p in include.split(",") if p.strip()}
    valid_buckets = {"income", "credit", "assets", "identity"}
    bad = requested - valid_buckets
    if bad:
        raise HTTPException(
            status_code=422,
            detail=f"include must be subset of {sorted(valid_buckets)}, got extra {sorted(bad)}",
        )
    include_set = requested or valid_buckets

    pg = request.app.state.postgres_store
    redis = request.app.state.redis_store
    await _enforce_rate_limit(redis, x_api_key or "anon", "entities")

    counter = [0]

    if fmt == "jsonl":
        async def gen():
            async for chunk in _stream_jsonl(
                pg.stream_entities(since=since_ts),
                lambda r: _entity_record(r, include_set),
                counter,
            ):
                yield chunk
        body = gen()
    else:
        async def gen():
            async for chunk in _stream_csv(
                pg.stream_entities(since=since_ts),
                lambda r: _entity_csv_row(r, include_set),
                _ENTITY_CSV_COLUMNS,
                counter,
            ):
                yield chunk
        body = gen()

    headers = _export_headers(since_ts, counter, "entities", fmt)
    headers["X-Export-Row-Count"] = "streaming"
    return _streaming_response(body, fmt, headers)


@router.get(
    "/documents",
    dependencies=[Depends(verify_api_key)],
    summary="Streaming export of document_index rows",
    responses=_EXPORT_RESPONSES,
)
async def export_documents(
    request: Request,
    format: str = Query(_DEFAULT_FORMAT, description="`jsonl` or `csv`."),
    since: Optional[str] = Query(None, description="Incremental cutoff (ISO 8601)."),
    doc_type: Optional[str] = Query(None, description="Filter by canonical document_type (e.g. `W2_CURRENT`)."),
    category: Optional[str] = Query(None, description="Filter by document_category (`income`, `credit`, `property`, `vendor`, …)."),
):
    x_api_key = request.headers.get("X-API-Key") or "anon"
    fmt = _validate_format(format)
    since_ts = _parse_iso(since, "since")

    pg = request.app.state.postgres_store
    redis = request.app.state.redis_store
    await _enforce_rate_limit(redis, x_api_key or "anon", "documents")

    counter = [0]
    raw_iter = pg.stream_documents(since=since_ts, doc_type=doc_type, category=category)

    if fmt == "jsonl":
        body = _stream_jsonl(raw_iter, _document_record, counter)
    else:
        body = _stream_csv(raw_iter, _document_record, _DOCUMENT_CSV_COLUMNS, counter)

    headers = _export_headers(since_ts, counter, "documents", fmt)
    headers["X-Export-Row-Count"] = "streaming"
    return _streaming_response(body, fmt, headers)


@router.get(
    "/graph",
    dependencies=[Depends(verify_api_key)],
    summary="Streaming export of document_relationships edges",
    responses=_EXPORT_RESPONSES,
)
async def export_graph(
    request: Request,
    format: str = Query(_DEFAULT_FORMAT, description="`jsonl` or `csv`."),
    since: Optional[str] = Query(None, description="Incremental cutoff (ISO 8601)."),
    relationship_type: Optional[str] = Query(
        None,
        description="Filter to one of `confirms`, `contradicts`, `corroborates`, `supersedes`, `references`.",
    ),
):
    x_api_key = request.headers.get("X-API-Key") or "anon"
    fmt = _validate_format(format)
    since_ts = _parse_iso(since, "since")
    if relationship_type and relationship_type not in _VALID_REL_TYPES:
        raise HTTPException(
            status_code=422,
            detail=f"relationship_type must be one of {_VALID_REL_TYPES}",
        )

    pg = request.app.state.postgres_store
    redis = request.app.state.redis_store
    await _enforce_rate_limit(redis, x_api_key or "anon", "graph")

    counter = [0]
    raw_iter = pg.stream_graph_edges(since=since_ts, relationship_type=relationship_type)

    if fmt == "jsonl":
        body = _stream_jsonl(raw_iter, _graph_record, counter)
    else:
        body = _stream_csv(raw_iter, _graph_record, _GRAPH_CSV_COLUMNS, counter)

    headers = _export_headers(since_ts, counter, "graph", fmt)
    headers["X-Export-Row-Count"] = "streaming"
    return _streaming_response(body, fmt, headers)


@router.get(
    "/profiles",
    dependencies=[Depends(verify_api_key)],
    summary="Streaming export of income + credit profiles",
    responses=_EXPORT_RESPONSES,
)
async def export_profiles(
    request: Request,
    format: str = Query(_DEFAULT_FORMAT, description="`jsonl` or `csv`."),
    since: Optional[str] = Query(None, description="Incremental cutoff (ISO 8601)."),
    profile_type: str = Query("all", description="`income`, `credit`, or `all` (default — emits both, each row keyed by `profile_kind`)."),
):
    x_api_key = request.headers.get("X-API-Key") or "anon"
    fmt = _validate_format(format)
    since_ts = _parse_iso(since, "since")
    if profile_type not in _VALID_PROFILE_TYPES:
        raise HTTPException(
            status_code=422,
            detail=f"profile_type must be one of {_VALID_PROFILE_TYPES}",
        )

    pg = request.app.state.postgres_store
    redis = request.app.state.redis_store
    await _enforce_rate_limit(redis, x_api_key or "anon", "profiles")

    counter = [0]

    async def merged():
        """When ``profile_type=all`` the response is income rows
        followed by credit rows, each carrying ``profile_kind`` so a
        DWH consumer can split by type at load time without keeping
        two separate URLs."""
        if profile_type in ("income", "all"):
            async for r in pg.stream_income_profiles(since=since_ts):
                yield _income_profile_record(r)
        if profile_type in ("credit", "all"):
            async for r in pg.stream_credit_profiles(since=since_ts):
                yield _credit_profile_record(r)

    if fmt == "jsonl":
        async def gen():
            async for rec in merged():
                counter[0] += 1
                yield json.dumps(rec, default=_json_default, separators=(",", ":")) + "\n"
        body = gen()
    else:
        async def passthrough():
            async for rec in merged():
                yield rec
        body = _stream_csv(
            passthrough(),
            lambda r: r,
            _PROFILE_CSV_COLUMNS,
            counter,
        )

    headers = _export_headers(since_ts, counter, "profiles", fmt)
    headers["X-Export-Row-Count"] = "streaming"
    return _streaming_response(body, fmt, headers)


@router.get(
    "/applications",
    dependencies=[Depends(verify_api_key)],
    summary="Streaming export of application-level summaries",
    responses=_EXPORT_RESPONSES,
)
async def export_applications(
    request: Request,
    format: str = Query(_DEFAULT_FORMAT, description="`jsonl` or `csv`."),
    since: Optional[str] = Query(None, description="Incremental cutoff (ISO 8601). Filters on `COALESCE(updated_at, created_at)`."),
):
    x_api_key = request.headers.get("X-API-Key") or "anon"
    fmt = _validate_format(format)
    since_ts = _parse_iso(since, "since")

    pg = request.app.state.postgres_store
    redis = request.app.state.redis_store
    await _enforce_rate_limit(redis, x_api_key or "anon", "applications")

    counter = [0]
    raw_iter = pg.stream_applications_export(since=since_ts)

    if fmt == "jsonl":
        body = _stream_jsonl(raw_iter, _application_record, counter)
    else:
        body = _stream_csv(raw_iter, _application_record, _APPLICATION_CSV_COLUMNS, counter)

    headers = _export_headers(since_ts, counter, "applications", fmt)
    headers["X-Export-Row-Count"] = "streaming"
    return _streaming_response(body, fmt, headers)


# ---------------------------------------------------------------------------
# Watermark CRUD — DWH consumers persist their last-pulled cursor here
# ---------------------------------------------------------------------------


class WatermarkRequest(BaseModel):
    consumer:  str = Field(..., min_length=1, max_length=100)
    table:     str = Field(..., min_length=1, max_length=100)
    watermark: str  # ISO-8601 timestamp


class WatermarkResponse(BaseModel):
    consumer:  str
    table:     str
    watermark: str
    updated_at: Optional[str] = None


@router.post(
    "/watermark",
    response_model=WatermarkResponse,
    dependencies=[Depends(verify_api_key)],
    summary="Set a DWH consumer's last-pull watermark",
    responses={
        401: {"description": "Missing or invalid `X-API-Key`."},
        422: {"description": "Validation error (missing fields or unparseable timestamp)."},
    },
)
async def set_watermark(request: Request, body: WatermarkRequest):
    """Persist a DWH consumer's last-successful-pull watermark."""
    ts = _parse_iso(body.watermark, "watermark")
    if ts is None:
        raise HTTPException(status_code=422, detail="watermark required")
    pg = request.app.state.postgres_store
    row = await pg.upsert_export_watermark(body.consumer, body.table, ts)
    return WatermarkResponse(
        consumer=row.get("consumer", body.consumer),
        table=row.get("table_name", body.table),
        watermark=(
            row["watermark_ts"].isoformat()
            if isinstance(row.get("watermark_ts"), datetime)
            else str(row.get("watermark_ts") or ts.isoformat())
        ),
        updated_at=(
            row["updated_at"].isoformat()
            if isinstance(row.get("updated_at"), datetime)
            else (str(row.get("updated_at")) if row.get("updated_at") else None)
        ),
    )


@router.get(
    "/watermark",
    dependencies=[Depends(verify_api_key)],
    summary="Get a single watermark for a consumer + table",
    responses={
        401: {"description": "Missing or invalid `X-API-Key`."},
        404: {"description": "No watermark recorded for this consumer/table pair."},
    },
)
async def get_watermark(
    request: Request,
    consumer: str = Query(..., min_length=1, description="DWH consumer name (e.g. `snowflake_etl`)."),
    table: str = Query(..., min_length=1, description="Logical table name (`entities`, `documents`, `graph`, `profiles`, `applications`)."),
):
    pg = request.app.state.postgres_store
    row = await pg.get_export_watermark(consumer, table)
    if not row:
        raise HTTPException(status_code=404, detail="Watermark not found")
    wm = row.get("watermark_ts")
    return {
        "consumer":  row.get("consumer", consumer),
        "table":     row.get("table_name", table),
        "watermark": wm.isoformat() if isinstance(wm, datetime) else str(wm),
        "updated_at": (
            row["updated_at"].isoformat()
            if isinstance(row.get("updated_at"), datetime)
            else (str(row.get("updated_at")) if row.get("updated_at") else None)
        ),
    }


@router.get(
    "/watermarks",
    dependencies=[Depends(verify_api_key)],
    summary="List every recorded watermark",
    responses={401: {"description": "Missing or invalid `X-API-Key`."}},
)
async def list_watermarks(
    request: Request,
    consumer: Optional[str] = Query(None, description="Optional consumer-name filter."),
):
    pg = request.app.state.postgres_store
    rows = await pg.list_export_watermarks(consumer=consumer)
    out = []
    for row in rows:
        wm = row.get("watermark_ts")
        out.append({
            "consumer":  row.get("consumer"),
            "table":     row.get("table_name"),
            "watermark": wm.isoformat() if isinstance(wm, datetime) else str(wm),
            "updated_at": (
                row["updated_at"].isoformat()
                if isinstance(row.get("updated_at"), datetime)
                else (str(row.get("updated_at")) if row.get("updated_at") else None)
            ),
        })
    return {"watermarks": out, "count": len(out)}
