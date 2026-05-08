"""EDMS Simulator routes.

Auth: X-API-Key validated against the edms/api/keys secret.
Cache pattern: Redis -> Postgres.
"""
import base64
import json
import os
import secrets as _secrets
from typing import Any, Optional

import structlog
from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse

from api.schemas import (
    ApplicantIdResponse,
    CreateLoanRequest,
    CreateLoanResponse,
    CreditProfileResponse,
    DocumentUploadRequest,
    IncomeProfileResponse,
)
from core.aggregation.events import (
    ApplicationSubmittedEvent,
    DocumentUploadedEvent,
    EventType,
    PropertyDocumentUploadedEvent,
)
from core.context.assembler import ContextAssembler
from core.indexing.batch_indexer import BatchIndexer
from core.indexing.watermark import WatermarkStore
from core.context.models import (
    ComplianceSlice,
    CreditSlice,
    FraudSlice,
    IncomeSlice,
    PropertySlice,
    ReadinessFlags,
)
from core.ingestion.adapters.vendor_aus_adapter import VendorAUSAdapter
from core.ingestion.adapters.vendor_fraud_adapter import VendorFraudAdapter
from core.ingestion.adapters.vendor_ssn_adapter import (
    VendorOFACAdapter,
    VendorSSNAdapter,
)
from core.ingestion.adapters.vendor_voe_adapter import VendorVOEAdapter
from core.property.extractors import (
    extract_appraisal_pdf,
    extract_flood_pdf,
    extract_hoi_pdf,
    extract_tax_pdf,
)
from core.property.sources import PROPERTY_CONFIDENCE
from core.graph.navigator import DocumentNavigator
from core.graph.reconciler import DocumentReconciler
from core.ingestion._claude_client import ClaudeUnavailable
from core.ingestion.adapters import (
    chat_adapter,
    csv_adapter,
    email_adapter,
    form_adapter,
    image_adapter,
    pdf_adapter,
    xml_adapter,
)
from core.ingestion.events import ChannelType
from core.ingestion.los_connector import get_connector
from core.ingestion.mismo import (
    ENCOMPASS_TO_INTERNAL,
    MISMO_TO_INTERNAL,
)
from core.ingestion.pipeline import IngestionPipeline
from core.ingestion.router import IngestRouter
from core.storage.raw_ingestion_store import RawIngestionStore

try:
    from anthropic import APIStatusError as _AnthropicAPIStatusError  # type: ignore
except Exception:  # SDK absent in some environments
    _AnthropicAPIStatusError = None  # type: ignore[assignment]


def _claude_error_to_http(exc: Exception) -> HTTPException:
    """Map an upstream Anthropic error to a 502 with a useful detail."""
    detail = getattr(exc, "message", None) or str(exc)
    return HTTPException(status_code=502, detail=f"Anthropic upstream error: {detail}")

logger = structlog.get_logger()
router = APIRouter()


DEFAULT_TENANT_ID = "default"
_API_KEY_CACHE_TTL_SECONDS = 300  # 5 minutes


class AuthContext:
    """The shape stamped onto ``request.state`` by ``verify_api_key``.

    ``tenant_id`` gates every Postgres read/write and Redis key prefix
    downstream. ``scopes`` is the comma-list from ``api_keys.scopes`` —
    routes with ``Depends(require_admin)`` enforce ``admin`` on it.
    Plain string set so future callers can do ``"write" in scopes``.
    """
    __slots__ = ("tenant_id", "scopes", "api_key", "name")

    def __init__(self, tenant_id: str, scopes: set[str], api_key: str, name: Optional[str] = None):
        self.tenant_id = tenant_id
        self.scopes = scopes
        self.api_key = api_key
        self.name = name


def _legacy_env_key() -> Optional[str]:
    """Fallback static key for tests + bootstrap. Tests set ``API_KEY``
    directly via conftest; production reads from Secrets Manager when
    the env-var is empty. Either way, a match grants the 'default'
    tenant with admin scope so 329 existing tests + dev workflows
    keep working without seeding ``api_keys`` first."""
    expected = os.getenv("API_KEY")
    if expected:
        return expected
    try:
        from core.storage.secrets import get_secrets
        keys = get_secrets().get_secret("edms/api/keys")
        return keys.get("decision_os_api_key") if isinstance(keys, dict) else None
    except Exception:
        return None


async def _lookup_api_key(request: Request, key: str) -> Optional[dict]:
    """Resolve an API key → ``{tenant_id, scopes, name}`` via Redis
    cache → Postgres. Cache hits are 5-min TTL; misses fall through to
    a single SELECT against ``api_keys``. Failures (Redis down, DB
    pool not initialised in unit tests) silently return None so the
    legacy env-var fallback can run instead of failing the request."""
    redis = getattr(request.app.state, "redis_store", None)
    cache_key = f"apikey:{key}"
    if redis is not None:
        try:
            raw = await redis._r.get(cache_key)
            if raw:
                return json.loads(raw)
        except Exception as exc:
            logger.debug("apikey_cache_get_failed", error=str(exc))

    pg = getattr(request.app.state, "postgres_store", None)
    if pg is None or not hasattr(pg, "get_api_key"):
        return None
    try:
        row = await pg.get_api_key(key)
    except Exception as exc:
        logger.debug("apikey_lookup_failed", error=str(exc))
        return None
    if not row or not row.get("is_active"):
        return None

    record = {
        "tenant_id": row["tenant_id"],
        "scopes":    row.get("scopes") or "read,write",
        "name":      row.get("name"),
    }
    if redis is not None:
        try:
            await redis._r.setex(
                cache_key,
                _API_KEY_CACHE_TTL_SECONDS,
                json.dumps(record),
            )
        except Exception as exc:
            logger.debug("apikey_cache_set_failed", error=str(exc))
    return record


async def verify_api_key(
    request: Request,
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
) -> AuthContext:
    """Resolve the inbound ``X-API-Key`` to a tenant + scopes.

    Resolution order:
      1. Postgres ``api_keys`` table (cached in Redis 5 min).
      2. Legacy static env-var fallback (``API_KEY`` / Secrets Manager).
         Matches grant the 'default' tenant with admin scope.

    Either path attaches ``tenant_id`` + ``scopes`` + ``api_key`` to
    ``request.state`` so downstream code can read them via
    ``request.state.tenant_id``. ``last_used_at`` is bumped best-effort
    after a successful DB-backed match (non-blocking).
    """
    if not x_api_key:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")

    record = await _lookup_api_key(request, x_api_key)
    if record:
        ctx = AuthContext(
            tenant_id=record["tenant_id"],
            scopes={s.strip() for s in (record.get("scopes") or "").split(",") if s.strip()},
            api_key=x_api_key,
            name=record.get("name"),
        )
        # Best-effort last-used touch — never blocks the request and
        # silently swallows pool-not-ready errors in tests.
        pg = getattr(request.app.state, "postgres_store", None)
        if pg is not None and hasattr(pg, "touch_api_key"):
            try:
                await pg.touch_api_key(x_api_key)
            except Exception:
                pass
    else:
        legacy = _legacy_env_key()
        if not legacy or x_api_key != legacy:
            raise HTTPException(status_code=401, detail="Invalid or missing API key")
        ctx = AuthContext(
            tenant_id=DEFAULT_TENANT_ID,
            scopes={"read", "write", "admin"},
            api_key=x_api_key,
            name="legacy_env",
        )

    request.state.tenant_id = ctx.tenant_id
    request.state.scopes    = ctx.scopes
    request.state.api_key   = ctx.api_key
    # Mirror onto the per-task contextvar so service + store code that
    # doesn't take a Request can read the current tenant via
    # core.tenancy.current_tenant_id() instead of threading it through
    # every method signature.
    from core.tenancy import set_tenant_id
    set_tenant_id(ctx.tenant_id)
    return ctx


async def require_admin(
    request: Request,
    auth: AuthContext = Depends(verify_api_key),
) -> AuthContext:
    """Use as ``dependencies=[Depends(require_admin)]`` on /admin routes.
    Runs ``verify_api_key`` first, then enforces ``admin`` ∈ scopes."""
    if "admin" not in auth.scopes:
        raise HTTPException(status_code=403, detail="admin scope required")
    return auth


def get_tenant_id(request: Request) -> str:
    """Read the tenant_id off ``request.state``, defaulting to 'default'
    when no auth has run (unit-test paths that bypass middleware)."""
    return getattr(request.state, "tenant_id", DEFAULT_TENANT_ID) or DEFAULT_TENANT_ID


@router.post(
    "/loans",
    response_model=CreateLoanResponse,
    dependencies=[Depends(verify_api_key)],
    summary="Create a loan application",
    description=(
        "Resolves identity for the primary borrower (and optional co-borrower) "
        "and creates the application + golden record. Subsequent document "
        "uploads attach via `POST /documents/upload` or any of the seven "
        "`/ingest/*` channels using the returned `applicant_id`."
    ),
    responses={
        401: {"description": "Missing or invalid `X-API-Key`."},
        422: {"description": "Validation error in the borrower / loan payload."},
    },
)
async def create_loan(request: Request, body: CreateLoanRequest):
    service = request.app.state.aggregation_service
    payload = body.model_dump()
    event = ApplicationSubmittedEvent(
        event_type=EventType.APPLICATION_SUBMITTED, payload=payload
    )
    result = await service.handle(event)
    return CreateLoanResponse(**result)


@router.get(
    "/loan/{los_id}/applicant-id",
    response_model=ApplicantIdResponse,
    dependencies=[Depends(verify_api_key)],
)
async def get_applicant_id(request: Request, los_id: str):
    redis_store = request.app.state.redis_store
    postgres_store = request.app.state.postgres_store

    cached = await redis_store.get_app_lookup(los_id)
    if cached:
        return ApplicantIdResponse(
            applicant_id=cached["applicant_id"],
            application_id=cached["application_id"],
            co_applicant_id=cached.get("co_applicant_id"),
            cached=True,
        )

    app = await postgres_store.get_application_by_los_id(los_id)
    if not app:
        raise HTTPException(status_code=404, detail="Application not found")

    await redis_store.set_app_lookup(
        los_id,
        {
            "application_id": app["application_id"],
            "applicant_id": app["applicant_id"],
            "co_applicant_id": app.get("co_applicant_id"),
        },
    )
    return ApplicantIdResponse(
        applicant_id=app["applicant_id"],
        application_id=app["application_id"],
        co_applicant_id=app.get("co_applicant_id"),
        cached=False,
    )


# ---------------------------------------------------------------------------
# Incremental graph endpoints — entity_states, snapshots, build runs.
# ---------------------------------------------------------------------------


@router.get(
    "/entity/{entity_id}/state",
    dependencies=[Depends(verify_api_key)],
    summary="Current state of an entity (Decision-OS read shape)",
    description=(
        "Returns the live row from ``entity_states`` (write-through "
        "from the incremental graph builder). This is what Decision OS "
        "reads when it needs the current shape of a borrower / "
        "co-borrower / property without re-assembling on the read path."
    ),
    responses={
        401: {"description": "Missing or invalid `X-API-Key`."},
        404: {"description": "No state recorded for this entity."},
    },
)
async def get_entity_state(request: Request, entity_id: str):
    pg  = request.app.state.postgres_store
    tid = get_tenant_id(request)
    row = await pg.get_entity_state(entity_id, tenant_id=tid)
    if not row:
        raise HTTPException(status_code=404, detail="Entity not found")
    return row


@router.get(
    "/entity/{entity_id}/timeline",
    dependencies=[Depends(verify_api_key)],
    summary="EOD snapshot timeline for an entity",
    description=(
        "Returns every ``entity_snapshots`` row for this entity ordered "
        "by ``snapshot_date`` ascending — the lineage view used by "
        "audit / replay tools. One row per simulated day."
    ),
    responses={
        401: {"description": "Missing or invalid `X-API-Key`."},
        404: {"description": "No snapshots recorded for this entity yet."},
    },
)
async def get_entity_timeline_endpoint(request: Request, entity_id: str):
    pg  = request.app.state.postgres_store
    tid = get_tenant_id(request)
    rows = await pg.get_entity_timeline(entity_id, tenant_id=tid)
    if not rows:
        raise HTTPException(status_code=404, detail="No snapshots for entity")
    return {
        "entity_id": entity_id,
        "snapshots": rows,
        "count":     len(rows),
    }


@router.get(
    "/graph/build-runs",
    dependencies=[Depends(verify_api_key)],
    summary="Builder execution log",
    description=(
        "Returns ``graph_build_runs`` rows in (build_date, build_number) "
        "order. The watermark trail (``watermark_from`` → "
        "``watermark_to``) shows where the incremental pull advanced on "
        "each tick; the per-build deltas show docs pulled / new / "
        "skipped + entities updated + edges created. Useful for ops "
        "dashboards + post-mortems."
    ),
    responses={
        401: {"description": "Missing or invalid `X-API-Key`."},
        422: {"description": "Validation error in date_from / date_to."},
    },
)
async def list_graph_build_runs(
    request: Request,
    date_from: Optional[str] = None,
    date_to:   Optional[str] = None,
    limit:     int = 100,
):
    from datetime import date as _date, datetime as _dt, timedelta as _td
    pg  = request.app.state.postgres_store
    tid = get_tenant_id(request)
    today = _dt.utcnow().date()
    df = _date.fromisoformat(date_from) if date_from else (today - _td(days=60))
    dt = _date.fromisoformat(date_to)   if date_to   else today
    rows = await pg.get_graph_build_runs(df, dt, tenant_id=tid, limit=limit)
    return {
        "build_runs": rows,
        "count":      len(rows),
        "filters":    {"date_from": str(df), "date_to": str(dt)},
    }


@router.get(
    "/graph/watermark",
    dependencies=[Depends(verify_api_key)],
    summary="Current S3 EDMS connector watermark",
    description=(
        "Returns the most-recent ``last_indexed_at`` for the "
        "``s3_edms_connector`` source. Shows how far the incremental "
        "pull has advanced — gap to NOW() = backlog."
    ),
    responses={401: {"description": "Missing or invalid `X-API-Key`."}},
)
async def get_graph_watermark(request: Request):
    pg = request.app.state.postgres_store
    row = await pg.get_watermark("s3_edms_connector")
    last = (row or {}).get("last_indexed_at")
    return {
        "source":          "s3_edms_connector",
        "last_indexed_at": last.isoformat() if hasattr(last, "isoformat") else last,
        "status":          (row or {}).get("status", "idle"),
    }


# ---------------------------------------------------------------------------
# Config-driven scheduler — status / manual trigger / hot-reload
# ---------------------------------------------------------------------------


def _require_engine(request: Request):
    engine = getattr(request.app.state, "schedule_engine", None)
    if engine is None:
        raise HTTPException(
            status_code=503,
            detail=(
                "Schedule engine not configured. Place "
                "config/schedule.yaml on the container or set "
                "SCHEDULE_CONFIG_PATH; ENABLE_SCHEDULE_ENGINE=true "
                "additionally enables the polling loop."
            ),
        )
    return engine


@router.get(
    "/scheduler/status",
    dependencies=[Depends(verify_api_key)],
    summary="Scheduler config + last-run / next-fire snapshot",
    description=(
        "Returns the parsed YAML config plus, for every build + "
        "snapshot job, the last successful run time and the next "
        "scheduled fire time (computed from the cron expression). "
        "Returns 503 when no schedule.yaml has been loaded."
    ),
    responses={
        401: {"description": "Missing or invalid `X-API-Key`."},
        503: {"description": "Schedule engine not configured."},
    },
)
async def scheduler_status(request: Request):
    engine = _require_engine(request)
    out = engine.status()
    out["loop_running"] = bool(getattr(request.app.state, "schedule_engine_task", None))
    return out


from pydantic import BaseModel as _BaseModel, Field as _Field  # noqa: E402

class _SchedulerTriggerBody(_BaseModel):
    job: str = _Field(..., min_length=1, max_length=100,
                      description="Job name from schedule.yaml (e.g. `morning_build`, `eod_snapshot`).")


@router.post(
    "/scheduler/trigger",
    dependencies=[Depends(verify_api_key)],
    summary="Manually fire a scheduled job",
    description=(
        "Bypasses the cron-due check and runs the named build or "
        "snapshot immediately. Useful for ad-hoc backfills + smoke "
        "testing the wiring without waiting on the next cron tick."
    ),
    responses={
        401: {"description": "Missing or invalid `X-API-Key`."},
        404: {"description": "Job name not found in schedule.yaml."},
        503: {"description": "Schedule engine not configured."},
    },
)
async def scheduler_trigger(request: Request, body: _SchedulerTriggerBody):
    engine = _require_engine(request)
    result = await engine.trigger_job(body.job)
    if isinstance(result, dict) and result.get("error", "").startswith("unknown job"):
        raise HTTPException(status_code=404, detail=result["error"])
    return {"job": body.job, "result": result}


@router.post(
    "/scheduler/reload",
    dependencies=[Depends(verify_api_key)],
    summary="Re-read schedule.yaml without restarting the container",
    description=(
        "Re-loads the YAML config in place. Returns the new status "
        "block so the operator can confirm the new cron expressions / "
        "feature flags landed. Existing in-flight jobs continue under "
        "the old config; the next polling tick uses the reloaded one."
    ),
    responses={
        401: {"description": "Missing or invalid `X-API-Key`."},
        503: {"description": "Schedule engine not configured."},
    },
)
async def scheduler_reload(request: Request):
    engine = _require_engine(request)
    try:
        engine.reload()
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to reload schedule.yaml: {exc}",
        )
    out = engine.status()
    out["loop_running"] = bool(getattr(request.app.state, "schedule_engine_task", None))
    out["reloaded"]     = True
    return out


@router.get(
    "/applicant/{applicant_id}/income-profile",
    response_model=IncomeProfileResponse,
    dependencies=[Depends(verify_api_key)],
    summary="Assembled income profile (Redis → Postgres fallback)",
    responses={
        401: {"description": "Missing or invalid `X-API-Key`."},
        404: {"description": "No income profile found for this applicant."},
    },
)
async def get_income_profile(request: Request, applicant_id: str):
    redis_store = request.app.state.redis_store
    postgres_store = request.app.state.postgres_store
    tid = get_tenant_id(request)

    cached = await redis_store.get_income_profile(applicant_id, tenant_id=tid)
    if cached:
        return IncomeProfileResponse(
            applicant_id=applicant_id, profile=cached, cached=True,
            source="cache", data=cached,
        )

    profile = await postgres_store.get_income_profile(applicant_id, tenant_id=tid)
    if not profile:
        raise HTTPException(status_code=404, detail="Income profile not found")
    await redis_store.set_income_profile(applicant_id, profile, tenant_id=tid)
    return IncomeProfileResponse(
        applicant_id=applicant_id, profile=profile, cached=False,
        source="postgres", data=profile,
    )


@router.get(
    "/applicant/{applicant_id}/credit-profile",
    response_model=CreditProfileResponse,
    dependencies=[Depends(verify_api_key)],
    summary="Assembled credit profile (Redis → Postgres fallback)",
    responses={
        401: {"description": "Missing or invalid `X-API-Key`."},
        404: {"description": "No credit profile found for this applicant."},
    },
)
async def get_credit_profile(request: Request, applicant_id: str):
    redis_store = request.app.state.redis_store
    postgres_store = request.app.state.postgres_store
    tid = get_tenant_id(request)

    cached = await redis_store.get_credit_profile(applicant_id, tenant_id=tid)
    if cached:
        return CreditProfileResponse(
            applicant_id=applicant_id, profile=cached, cached=True,
            source="cache", data=cached,
        )

    profile = await postgres_store.get_credit_profile(applicant_id, tenant_id=tid)
    if not profile:
        raise HTTPException(status_code=404, detail="Credit profile not found")
    await redis_store.set_credit_profile(applicant_id, profile, tenant_id=tid)
    return CreditProfileResponse(
        applicant_id=applicant_id, profile=profile, cached=False,
        source="postgres", data=profile,
    )


async def _upload_documents_impl(request: Request, body: DocumentUploadRequest):
    service = request.app.state.aggregation_service
    event = DocumentUploadedEvent(
        event_type=EventType.DOCUMENT_UPLOADED,
        payload=body.model_dump(),
    )
    return await service.handle(event)


@router.post(
    "/documents/upload",
    dependencies=[Depends(verify_api_key)],
    summary="Upload one or more documents for an applicant",
    description=(
        "Persists each document to S3 + `document_index`, runs the indexer "
        "(structured-text / Claude-Vision fallback), reconciles against the "
        "existing graph, and re-assembles the affected income / credit / "
        "asset / property / context layers. Idempotent on `document_id`."
    ),
    responses={
        401: {"description": "Missing or invalid `X-API-Key`."},
        422: {"description": "Validation error in the documents payload."},
    },
)
async def upload_documents(request: Request, body: DocumentUploadRequest):
    return await _upload_documents_impl(request, body)


@router.post(
    "/loans/document",
    dependencies=[Depends(verify_api_key)],
    summary="Alias for /documents/upload (legacy callers)",
    responses={
        401: {"description": "Missing or invalid `X-API-Key`."},
        422: {"description": "Validation error in the documents payload."},
    },
)
async def upload_documents_loans_alias(request: Request, body: DocumentUploadRequest):
    return await _upload_documents_impl(request, body)


# ---------------------------------------------------------------------------
# Universal ingestion (Phase C: all adapters wired)
# ---------------------------------------------------------------------------


def _next_question_for(missing: list[str]) -> Optional[str]:
    if not missing:
        return None
    field = missing[0]
    pretty = field.replace("_", " ")
    return f"Could you share your {pretty}?"


def _build_pipeline(request: Request) -> IngestionPipeline:
    """Per-request pipeline; reuses the app.state singletons for s3 +
    postgres, default-constructs RawIngestionStore (stateless)."""
    return IngestionPipeline(
        postgres_store=request.app.state.postgres_store,
        redis_store=request.app.state.redis_store,
        s3_client=request.app.state.s3_client,
        raw_store=getattr(request.app.state, "raw_store", None) or RawIngestionStore(),
    )


def _claude_or_anthropic_to_http(exc: Exception) -> HTTPException:
    if isinstance(exc, ClaudeUnavailable):
        return HTTPException(status_code=503, detail=str(exc))
    if _AnthropicAPIStatusError and isinstance(exc, _AnthropicAPIStatusError):
        return _claude_error_to_http(exc)
    return HTTPException(status_code=500, detail=str(exc))


@router.post("/ingest/pdf", dependencies=[Depends(verify_api_key)])
async def ingest_pdf(
    request: Request,
    file: UploadFile = File(...),
    applicant_id: Optional[str] = Form(None),
    borrower_role: str = Form("primary"),
):
    body = await file.read()
    pipeline = _build_pipeline(request)
    result = await pipeline.ingest(
        channel=ChannelType.PDF_UPLOAD,
        payload=body,
        applicant_id=applicant_id,
        filename=file.filename,
    )
    return {
        **result["event"].model_dump(),
        "ingest_id": result["ingest_id"],
        "raw_s3_key": result["raw_s3_key"],
    }


@router.post("/ingest/image", dependencies=[Depends(verify_api_key)])
async def ingest_image(
    request: Request,
    file: UploadFile = File(...),
    applicant_id: Optional[str] = Form(None),
    borrower_role: str = Form("primary"),
):
    body = await file.read()
    pipeline = _build_pipeline(request)
    try:
        result = await pipeline.ingest(
            channel=ChannelType.IMAGE_UPLOAD,
            payload=body,
            applicant_id=applicant_id,
            filename=file.filename,
        )
    except (ClaudeUnavailable, Exception) as exc:
        if isinstance(exc, ClaudeUnavailable) or (
            _AnthropicAPIStatusError and isinstance(exc, _AnthropicAPIStatusError)
        ):
            raise _claude_or_anthropic_to_http(exc)
        raise
    return {
        **result["event"].model_dump(),
        "ingest_id": result["ingest_id"],
        "raw_s3_key": result["raw_s3_key"],
    }


@router.post("/ingest/email", dependencies=[Depends(verify_api_key)])
async def ingest_email(request: Request, payload: dict):
    pipeline = _build_pipeline(request)
    try:
        result = await pipeline.ingest(
            channel=ChannelType.EMAIL,
            payload=payload,
        )
    except ClaudeUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        if _AnthropicAPIStatusError and isinstance(exc, _AnthropicAPIStatusError):
            raise _claude_error_to_http(exc)
        raise
    events = result["event"]
    attachments_count = max(0, len(events) - 1)
    return {
        "ingest_id": result["ingest_id"],
        "raw_s3_key": result["raw_s3_key"],
        "events": [e.model_dump() for e in events],
        "documents_processed": attachments_count,
    }


@router.post("/ingest/chat", dependencies=[Depends(verify_api_key)])
async def ingest_chat(request: Request, payload: dict):
    messages = payload.get("messages") or []
    applicant_id = payload.get("applicant_id")
    pipeline = _build_pipeline(request)
    try:
        result = await pipeline.ingest(
            channel=ChannelType.CHAT,
            payload=messages,
            applicant_id=applicant_id,
        )
    except ClaudeUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        if _AnthropicAPIStatusError and isinstance(exc, _AnthropicAPIStatusError):
            raise _claude_error_to_http(exc)
        raise
    event = result["event"]
    return {
        "extracted": event.extracted_fields,
        "missing_fields": event.missing_fields,
        "documents_needed": event.documents_needed,
        "overall_confidence": event.confidence,
        "applicant_id": applicant_id,
        "next_question_suggestion": _next_question_for(event.missing_fields),
        "event": event.model_dump(),
        "ingest_id": result["ingest_id"],
        "raw_s3_key": result["raw_s3_key"],
    }


@router.post("/ingest/form", dependencies=[Depends(verify_api_key)])
async def ingest_form(request: Request, payload: dict):
    pipeline = _build_pipeline(request)
    result = await pipeline.ingest(channel=ChannelType.FORM, payload=payload)
    return {
        **result["event"].model_dump(),
        "ingest_id": result["ingest_id"],
        "raw_s3_key": result["raw_s3_key"],
    }


@router.post("/ingest/csv", dependencies=[Depends(verify_api_key)])
async def ingest_csv(request: Request, file: UploadFile = File(...)):
    body = await file.read()
    pipeline = _build_pipeline(request)
    result = await pipeline.ingest(
        channel=ChannelType.CSV_BATCH,
        payload=body,
        filename=file.filename,
    )
    events, report = result["event"]
    return {
        "ingest_id": result["ingest_id"],
        "raw_s3_key": result["raw_s3_key"],
        **report,
        "applicants": [e.applicant_signals for e in events],
    }


@router.post("/ingest/xml", dependencies=[Depends(verify_api_key)])
async def ingest_xml(request: Request, file: UploadFile = File(...)):
    body = await file.read()
    pipeline = _build_pipeline(request)
    result = await pipeline.ingest(
        channel=ChannelType.XML,
        payload=body,
        filename=file.filename,
    )
    return {
        **result["event"].model_dump(),
        "ingest_id": result["ingest_id"],
        "raw_s3_key": result["raw_s3_key"],
    }


# ---------------------------------------------------------------------------
# Phase A: raw_ingestion observability
# ---------------------------------------------------------------------------


@router.get(
    "/applicant/{applicant_id}/raw-ingestion",
    dependencies=[Depends(verify_api_key)],
)
async def list_raw_ingestion(request: Request, applicant_id: str):
    raw_store = getattr(request.app.state, "raw_store", None) or RawIngestionStore()
    rows = await raw_store.get_for_applicant(applicant_id)
    state = await raw_store.get_pipeline_state(applicant_id)
    return {
        "applicant_id":   applicant_id,
        "pipeline_state": state,
        "ingestions":     rows,
    }


@router.get(
    "/ingest/{ingest_id}/raw",
    dependencies=[Depends(verify_api_key)],
)
async def get_raw_ingestion(request: Request, ingest_id: str):
    raw_store = getattr(request.app.state, "raw_store", None) or RawIngestionStore()
    row = await raw_store.get(ingest_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"no raw ingestion {ingest_id}")
    return row


@router.post(
    "/ingest/{ingest_id}/reprocess",
    dependencies=[Depends(verify_api_key)],
)
async def reprocess_raw_ingestion(request: Request, ingest_id: str):
    pipeline = _build_pipeline(request)
    try:
        result = await pipeline.reprocess(ingest_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except ClaudeUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        if _AnthropicAPIStatusError and isinstance(exc, _AnthropicAPIStatusError):
            raise _claude_error_to_http(exc)
        raise
    event = result["event"]
    summary = (
        event.model_dump() if hasattr(event, "model_dump")
        else {"events_count": len(event) if isinstance(event, (list, tuple)) else 0}
    )
    return {
        "ingest_id":   result["ingest_id"],
        "status":      result["status"],
        "raw_s3_key":  result["raw_s3_key"],
        "result":      summary,
    }


@router.get(
    "/pipeline/failed",
    dependencies=[Depends(verify_api_key)],
)
async def list_failed_ingestions(request: Request, limit: int = 50):
    raw_store = getattr(request.app.state, "raw_store", None) or RawIngestionStore()
    rows = await raw_store.get_failed(limit=limit)
    return {"count": len(rows), "ingestions": rows}


# ---------------------------------------------------------------------------
# Document knowledge graph
# ---------------------------------------------------------------------------


@router.get(
    "/applicant/{applicant_id}/graph/summary",
    dependencies=[Depends(verify_api_key)],
)
async def get_graph_summary(request: Request, applicant_id: str):
    redis = request.app.state.redis_store
    cached = await redis.get_graph_summary(applicant_id)
    if cached:
        return {"source": "cache", "data": cached}
    pg = request.app.state.postgres_store
    summary = await pg.get_graph_summary(applicant_id)
    await redis.set_graph_summary(applicant_id, summary)
    return {"source": "database", "data": summary}


@router.get(
    "/applicant/{applicant_id}/graph",
    dependencies=[Depends(verify_api_key)],
)
async def get_knowledge_graph(request: Request, applicant_id: str):
    pg = request.app.state.postgres_store
    navigator = DocumentNavigator(pg)
    graph = await navigator.build_graph(applicant_id)
    return graph.model_dump()


@router.get(
    "/applicant/{applicant_id}/conflicts",
    dependencies=[Depends(verify_api_key)],
)
async def get_conflicts(request: Request, applicant_id: str):
    pg = request.app.state.postgres_store
    conflicts = await pg.get_conflicts_for_applicant(applicant_id)
    return {
        "applicant_id": applicant_id,
        "conflict_count": len(conflicts),
        "conflicts": conflicts,
    }


@router.post(
    "/applicant/{applicant_id}/navigate",
    dependencies=[Depends(verify_api_key)],
)
async def navigate(request: Request, applicant_id: str, body: dict):
    question = body.get("question", "")
    if not question:
        raise HTTPException(status_code=400, detail="question field required")
    pg = request.app.state.postgres_store
    redis = request.app.state.redis_store
    navigator = DocumentNavigator(pg, redis)
    answer = await navigator.answer(applicant_id, question)
    return answer.model_dump()


@router.post(
    "/applicant/{applicant_id}/reconcile",
    dependencies=[Depends(verify_api_key)],
)
async def reconcile_applicant(request: Request, applicant_id: str):
    pg = request.app.state.postgres_store
    docs = await pg.get_documents_for_applicant(applicant_id)
    reconciler = DocumentReconciler(pg)
    total_rels = 0
    total_conflicts = 0
    for doc in docs:
        rels = await reconciler.reconcile(applicant_id, doc)
        total_rels += len(rels)
        total_conflicts += sum(
            1 for r in rels if r.relationship_type.value == "contradicts"
        )
    return {
        "applicant_id": applicant_id,
        "relationships_created": total_rels,
        "conflicts_found": total_conflicts,
    }


# ---------------------------------------------------------------------------
# Attribute index — query single fields across an applicant's documents
# (Build: comprehensive indexing)
# ---------------------------------------------------------------------------


@router.get(
    "/applicant/{applicant_id}/field/{field_name}",
    dependencies=[Depends(verify_api_key)],
)
async def get_field_across_documents(
    request: Request, applicant_id: str, field_name: str,
):
    """Highest-confidence value for a field across every indexed document
    for the applicant, plus the full source list and a max_delta_pct so
    callers can see at a glance whether sources agree."""
    pg = request.app.state.postgres_store
    sources = await pg.get_all_field_values(applicant_id, field_name)
    if not sources:
        raise HTTPException(
            status_code=404,
            detail=f"no document for {applicant_id} has field {field_name!r}",
        )
    best = await pg.get_highest_confidence_field(applicant_id, field_name)

    numeric_values: list[float] = []
    for s in sources:
        try:
            numeric_values.append(float(s.get("field_value")))
        except (TypeError, ValueError):
            continue
    max_delta_pct: Optional[float] = None
    has_conflict = False
    if len(numeric_values) >= 2:
        peak = max(abs(v) for v in numeric_values)
        if peak > 0:
            spread = max(numeric_values) - min(numeric_values)
            max_delta_pct = round(spread / peak * 100, 2)
            has_conflict = max_delta_pct > 10.0

    # Surface the extraction_method of the best-source document at the
    # top level so callers can decide how confident to be without
    # digging into best_value's row dict. Falls back to "none" when no
    # best document was found (shouldn't happen given the 404 above
    # but keeps the contract honest).
    best_method = (best or {}).get("extraction_method") if isinstance(best, dict) else None
    return {
        "field_name":        field_name,
        "applicant_id":      applicant_id,
        "best_value":        best,
        "extraction_method": best_method or "none",
        "all_sources":       sources,
        "has_conflict":      has_conflict,
        "max_delta_pct":     max_delta_pct,
    }


@router.get(
    "/applicant/{applicant_id}/documents/{category}",
    dependencies=[Depends(verify_api_key)],
)
async def list_documents_by_category(
    request: Request, applicant_id: str, category: str,
):
    """All indexed documents for an applicant in a given category
    (income | credit | asset | property | identity | compliance)."""
    pg = request.app.state.postgres_store
    docs = await pg.get_documents_by_category(applicant_id, category)
    return {
        "applicant_id":   applicant_id,
        "category":       category,
        "document_count": len(docs),
        "documents":      docs,
    }


@router.get(
    "/application/{application_id}/graph/full",
    dependencies=[Depends(verify_api_key)],
)
async def get_full_graph(request: Request, application_id: str):
    """Complete knowledge graph for an application — primary + co
    applicant nodes, every edge, plus a confidence + conflict summary."""
    pg = request.app.state.postgres_store
    app = await pg.get_application(application_id)
    if not app:
        raise HTTPException(status_code=404, detail="application not found")

    navigator = DocumentNavigator(pg)
    primary_graph = await navigator.build_graph(app["applicant_id"])
    payload: dict = {
        "application_id": application_id,
        "primary":        primary_graph.model_dump(),
        "co_borrower":    None,
    }
    co_id = app.get("co_applicant_id")
    if co_id:
        co_graph = await navigator.build_graph(co_id)
        payload["co_borrower"] = co_graph.model_dump()
    payload["conflict_summary"] = {
        "primary_conflicts": len(primary_graph.conflicts),
        "co_conflicts": (
            len(payload["co_borrower"]["conflicts"])
            if payload["co_borrower"] else 0
        ),
    }
    return payload


# ---------------------------------------------------------------------------
# Phase 0: MISMO compatibility — universal LOS endpoints
# ---------------------------------------------------------------------------


@router.post("/ingest/los", dependencies=[Depends(verify_api_key)])
async def ingest_los(request: Request, body: dict):
    """Universal LOS document receiver.

    Body shape::

        {
          "source_system": "encompass" | "mismo_34" | ...,
          "payload": { ... whatever the LOS sends ... }
        }

    The connector translates the payload to the internal model. If the
    LOS loan number maps to an existing application, the document is
    persisted into ``document_index`` and reconciled against the
    applicant's other docs. Otherwise the translated event is returned
    with ``status=pending_loan_creation`` so the caller can submit the
    loan first via ``POST /loans/from-los``.
    """
    source_system = body.get("source_system") or ""
    payload = body.get("payload") or {}
    if not source_system or not payload:
        raise HTTPException(
            status_code=400,
            detail="body must include source_system and payload",
        )
    connector = get_connector(source_system)
    translated = connector.translate_document(payload)

    pg = request.app.state.postgres_store
    external_loan_id = translated.get("external_loan_id")
    application = (
        await pg.get_application_by_external_loan_id(external_loan_id)
        if external_loan_id else None
    )

    if not application:
        return {
            "status": "pending_loan_creation",
            "document_type_detected": translated["document_type"],
            "applicant_id": None,
            "external_loan_id": external_loan_id,
            "translated": translated,
        }

    import uuid as _uuid
    applicant_id = application["applicant_id"]
    document_id = (
        translated.get("external_doc_id")
        or f"DOC-{source_system}-{_uuid.uuid4().hex[:12]}"
    )
    doc = {
        "document_id":      document_id,
        "applicant_id":     applicant_id,
        "application_id":   application["application_id"],
        "document_type":    translated["document_type"],
        "document_category": translated["document_category"],
        "borrower_role":    "primary",
        "s3_key":           None,
        "status":           "received",
        "is_current":       True,
        "extracted_fields": translated["extracted_fields"],
        "confidence_score": translated["confidence_score"],
    }
    try:
        await pg.save_document(doc)
        new_rels = await DocumentReconciler(pg).reconcile(applicant_id, doc)
    except Exception as exc:
        logger.warning("ingest_los_persist_failed", extra={"error": str(exc)})
        return {
            "status": "translation_only",
            "document_type_detected": translated["document_type"],
            "applicant_id": applicant_id,
            "external_loan_id": external_loan_id,
            "translated": translated,
            "error": str(exc),
        }

    return {
        "status": "persisted",
        "ingest_id": document_id,
        "document_type_detected": translated["document_type"],
        "applicant_id": applicant_id,
        "application_id": application["application_id"],
        "external_loan_id": external_loan_id,
        "relationships_created": len(new_rels),
        "translated": translated,
    }


@router.post("/loans/from-los", dependencies=[Depends(verify_api_key)])
async def create_loan_from_los(request: Request, body: dict):
    """Create a loan from a LOS-shaped payload.

    Translates via the connector, then drives the existing
    APPLICATION_SUBMITTED pipeline. Stores the LOS's loan number on the
    new application row and merges any external IDs onto the applicant.
    """
    source_system = body.get("source_system") or ""
    payload = body.get("payload") or {}
    if not source_system or not payload:
        raise HTTPException(
            status_code=400,
            detail="body must include source_system and payload",
        )
    connector = get_connector(source_system)
    translated = connector.translate_loan(payload)

    inner_payload = {
        "los_id":      translated["los_id"],
        "borrower":    translated["borrower"],
        "co_borrower": translated.get("co_borrower"),
        "loan":        {
            "loan_amount": (translated["loan"] or {}).get("loan_amount"),
            "credit_band": (translated["loan"] or {}).get("credit_band", "near-prime"),
        },
        "documents":   [],
    }
    service = request.app.state.aggregation_service
    event = ApplicationSubmittedEvent(
        event_type=EventType.APPLICATION_SUBMITTED, payload=inner_payload
    )
    result = await service.handle(event)

    pg = request.app.state.postgres_store
    external_loan_id = translated.get("los_id")
    loan_terms = translated.get("loan") or {}
    try:
        await pg.update_application_loan_fields(
            application_id=result["application_id"],
            loan_data={
                "loan_amount":      loan_terms.get("loan_amount"),
                "interest_rate":    loan_terms.get("interest_rate"),
                "loan_term_months": loan_terms.get("loan_term_months"),
                "loan_purpose":     loan_terms.get("loan_purpose"),
                "loan_type":        loan_terms.get("loan_type"),
                "external_loan_id": external_loan_id,
                "urla_fields":      translated.get("urla_fields") or {},
            },
        )
        for sys_name, ext_id in (translated.get("external_ids") or {}).items():
            await pg.add_external_id(result["applicant_id"], sys_name, ext_id)
    except Exception as exc:
        logger.warning("loans_from_los_patch_failed", extra={"error": str(exc)})

    return {
        "applicant_id":     result["applicant_id"],
        "co_applicant_id":  result.get("co_applicant_id"),
        "application_id":   result["application_id"],
        "external_loan_id": external_loan_id,
        "match_method":     result["match_method"],
        "is_new_record":    result["is_new_record"],
        "source_system":    source_system,
    }


@router.get(
    "/resolve/external/{source_system}/{external_id}",
    dependencies=[Depends(verify_api_key)],
)
async def resolve_external(
    request: Request, source_system: str, external_id: str
):
    """Reverse-lookup: given a real LOS loan number / contact id, return
    the simulator's internal ids."""
    pg = request.app.state.postgres_store
    application = await pg.get_application_by_external_loan_id(external_id)
    if application:
        return {
            "applicant_id":     application["applicant_id"],
            "co_applicant_id":  application.get("co_applicant_id"),
            "application_id":   application["application_id"],
            "los_id":           application.get("los_id"),
            "external_loan_id": application.get("external_loan_id"),
            "matched_via":      "applications.external_loan_id",
        }
    applicant = await pg.find_by_external_id(source_system, external_id)
    if applicant:
        return {
            "applicant_id":   applicant["applicant_id"],
            "external_ids":   applicant.get("external_ids", {}),
            "matched_via":    "applicants.external_ids",
        }
    raise HTTPException(
        status_code=404,
        detail=f"no record found for {source_system}/{external_id}",
    )


# ---------------------------------------------------------------------------
# Phase B: property layer
# ---------------------------------------------------------------------------


_PROPERTY_EXTRACTORS = {
    "APPRAISAL_URAR":    extract_appraisal_pdf,
    "APPRAISAL_UPDATE":  extract_appraisal_pdf,
    "APPRAISAL_DESK":    extract_appraisal_pdf,
    "APPRAISAL_FIELD":   extract_appraisal_pdf,
    "HOI_BINDER":        extract_hoi_pdf,
    "HOI_DECLARATIONS":  extract_hoi_pdf,
    "FLOOD_CERT":        extract_flood_pdf,
    "PROPERTY_TAX_BILL": extract_tax_pdf,
}

_REQUIRED_PROPERTY_DOC_TYPES = [
    "APPRAISAL_URAR",
    "TITLE_COMMITMENT",
    "HOI_BINDER",
    "FLOOD_CERT",
    "PROPERTY_TAX_BILL",
]


@router.post("/properties", dependencies=[Depends(verify_api_key)])
async def create_property(request: Request, body: dict):
    """Create a property record for an application.

    Body shape::

        {
          "application_id": "APP-LOS-001",
          "address": {
            "line1": "123 Main St", "line2": null, "city": "...", "state": "CA",
            "zip_code": "94105"
          },
          "property_type": "single_family",
          "units": 1,
          "year_built": 2005,
          "sqft": 2400
        }
    """
    application_id = body.get("application_id")
    address = body.get("address") or {}
    property_type = body.get("property_type", "single_family")
    if not application_id:
        raise HTTPException(status_code=400, detail="application_id required")
    if not address.get("line1") or not address.get("city") \
            or not address.get("state") or not address.get("zip_code"):
        raise HTTPException(
            status_code=400,
            detail="address.line1, address.city, address.state, address.zip_code required",
        )

    import uuid as _uuid
    pg = request.app.state.postgres_store
    property_id = body.get("property_id") or f"PROP-{_uuid.uuid4().hex[:12]}"
    prop = {
        "property_id":    property_id,
        "application_id": application_id,
        "address_line1":  address["line1"],
        "address_line2":  address.get("line2"),
        "city":           address["city"],
        "state":          address["state"],
        "zip_code":       address["zip_code"],
        "property_type":  property_type,
        "units":          int(body.get("units", 1)),
        "year_built":     body.get("year_built"),
        "sqft":           body.get("sqft"),
        "status":         "pending",
    }
    await pg.save_property(prop)
    try:
        await pg.update_application_property(application_id, property_id)
    except Exception as exc:
        logger.warning("update_application_property_failed", extra={"error": str(exc)})
    return {"property_id": property_id, "status": "pending"}


@router.get(
    "/property/{property_id}/profile",
    dependencies=[Depends(verify_api_key)],
)
async def get_property_profile(request: Request, property_id: str):
    redis = request.app.state.redis_store
    pg = request.app.state.postgres_store

    cached = await redis.get_property_profile(property_id)
    if cached:
        return {"source": "cache", "data": cached}

    profile = await pg.get_property_profile(property_id)
    if not profile:
        raise HTTPException(status_code=404, detail="Property profile not found")
    await redis.set_property_profile(property_id, profile)
    return {"source": "database", "data": profile}


@router.get(
    "/property/{property_id}/pipeline-state",
    dependencies=[Depends(verify_api_key)],
)
async def get_property_pipeline_state(request: Request, property_id: str):
    pg = request.app.state.postgres_store
    prop = await pg.get_property(property_id)
    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")

    docs = await pg.get_property_docs(property_id)
    received_types = sorted({d.get("document_type") for d in docs if d.get("document_type")})
    missing = [t for t in _REQUIRED_PROPERTY_DOC_TYPES if t not in received_types]

    profile = await pg.get_property_profile(property_id)
    piti_ready = bool(profile and profile.get("piti_components"))
    ltv_ready = bool(profile and profile.get("appraised_value"))

    return {
        "property_id":        property_id,
        "documents_received": received_types,
        "documents_missing":  missing,
        "piti_ready":         piti_ready,
        "ltv_ready":          ltv_ready,
        "profile":            profile,
    }


@router.post("/ingest/property-doc", dependencies=[Depends(verify_api_key)])
async def ingest_property_doc(
    request: Request,
    file: UploadFile = File(...),
    property_id: str = Form(...),
    document_type: str = Form(...),
):
    """Upload a property PDF, extract, persist, and trigger reassembly."""
    body = await file.read()

    pg = request.app.state.postgres_store
    s3 = request.app.state.s3_client
    prop = await pg.get_property(property_id)
    if not prop:
        raise HTTPException(status_code=404, detail=f"property {property_id} not found")
    application_id = prop.get("application_id") or ""
    applicant_id = None
    if application_id:
        try:
            app = await pg.get_application_by_los_id(application_id.replace("APP-", "")) \
                if application_id.startswith("APP-") else None
        except Exception:
            app = None
        # Also try direct lookup since application_id == APP-{los_id}
        if not app:
            try:
                app = await pg.get_application_by_los_id(application_id)
            except Exception:
                app = None
        if app:
            applicant_id = app.get("applicant_id")

    extractor = _PROPERTY_EXTRACTORS.get(document_type)
    if extractor is None:
        extracted_fields, confidence = ({}, PROPERTY_CONFIDENCE.get(document_type, 0.7))
    else:
        try:
            extracted_fields, conf_from_extractor = extractor(body)
        except Exception as exc:
            logger.warning("property_extract_failed", extra={"error": str(exc)})
            extracted_fields, conf_from_extractor = ({}, 0.0)
        # Use the document-type's catalog confidence as the floor — the
        # extractor confidence is the recovery rate, not the source weight.
        confidence = max(
            conf_from_extractor,
            PROPERTY_CONFIDENCE.get(document_type, 0.7),
        )

    import uuid as _uuid
    document_id = f"PROPDOC-{_uuid.uuid4().hex[:12]}"
    s3_key = s3.upload_document(
        application_id=application_id or "no-app",
        category="property",
        document_id=document_id,
        content=body,
        extension="pdf",
        content_type="application/pdf",
    )

    if applicant_id:
        try:
            await pg.save_document({
                "document_id":      document_id,
                "applicant_id":     applicant_id,
                "application_id":   application_id,
                "document_type":    document_type,
                "document_category": "property",
                "borrower_role":    "primary",
                "s3_key":           s3_key,
                "status":           "received",
                "is_current":       True,
                "extracted_fields": extracted_fields,
                "confidence_score": confidence,
            })
        except Exception as exc:
            logger.warning("property_save_document_failed", extra={"error": str(exc)})

    service = request.app.state.aggregation_service
    new_doc_payload = {
        "document_id":      document_id,
        "document_type":    document_type,
        "document_category": "property",
        "extracted_fields": extracted_fields,
        "confidence_score": confidence,
    }
    docs = await pg.get_property_docs(property_id) or []
    if not any(d.get("document_id") == document_id for d in docs):
        docs.append(new_doc_payload)

    event = PropertyDocumentUploadedEvent(
        payload={
            "property_id":    property_id,
            "application_id": application_id,
            "property_docs":  docs,
        }
    )
    result = await service.handle(event)
    return {
        "property_id":    property_id,
        "document_id":    document_id,
        "document_type":  document_type,
        "s3_key":         s3_key,
        "confidence":     confidence,
        "extracted":      extracted_fields,
        "piti_total":     result.get("piti_total"),
        "profile":        result.get("profile"),
    }


@router.get("/mismo/doc-types", dependencies=[Depends(verify_api_key)])
async def mismo_doc_types():
    """Return the supported MISMO 3.4 + Encompass type mappings.

    Useful for an LOS integration team to discover what types we
    recognise without running test traffic.
    """
    return {
        "mismo_34": MISMO_TO_INTERNAL,
        "encompass": ENCOMPASS_TO_INTERNAL,
        "totals": {
            "mismo": len(MISMO_TO_INTERNAL),
            "encompass": len(ENCOMPASS_TO_INTERNAL),
        },
    }


# ---------------------------------------------------------------------------
# Phase C: application context — single-call assembly for Decision OS
# ---------------------------------------------------------------------------


def _context_assembler(request: Request) -> ContextAssembler:
    return ContextAssembler(
        postgres_store=request.app.state.postgres_store,
        redis_store=request.app.state.redis_store,
    )


@router.get(
    "/application/{application_id}/context",
    dependencies=[Depends(verify_api_key)],
    summary="Single-call ApplicationContext for Decision OS",
    description=(
        "The unified read shape Decision OS consumes per application. "
        "Folds borrower (income + credit + assets + identity), property "
        "(PITI + LTV), vendor checks, and readiness flags into one envelope. "
        "Cached at `context:{application_id}` for 30 minutes; layer changes "
        "invalidate the cache so the next read recomputes."
    ),
    responses={
        200: {
            "description": "Cached or freshly-assembled application context.",
            "content": {"application/json": {"example": {
                "source": "cache",
                "data": {
                    "application_id":   "APP-LOS-12345",
                    "los_id":            "LOS-12345",
                    "loan_amount":       360000,
                    "primary": {
                        "applicant_id":       "APL-00316-P",
                        "full_name":          "Alex Martinez",
                        "role":                "primary",
                        "qualifying_monthly": 10416.67,
                        "mid_score":           752,
                    },
                    "co_borrower": {
                        "applicant_id":       "APL-00317-C",
                        "full_name":          "Pat Martinez",
                        "role":                "co_borrower",
                        "qualifying_monthly": 9483.33,
                    },
                    "combined_qualifying_monthly": 19900.0,
                    "front_end_dti":               16.67,
                    "back_end_dti":                21.15,
                    "ltv":                          80.0,
                    "readiness": {
                        "income_verified": True, "credit_pulled": True,
                        "appraisal_complete": True, "ltv_calculable": True,
                        "dti_calculable": True, "aus_ready": True,
                        "no_critical_conflicts": False,
                    },
                    "graph_summary": {"document_count": 41, "conflict_count": 7},
                    "requires_review": True,
                },
            }}},
        },
        401: {"description": "Missing or invalid `X-API-Key`."},
        404: {"description": "Application not found."},
    },
)
async def get_application_context(request: Request, application_id: str):
    """The single endpoint Decision OS calls — folded borrower + property
    + readiness + DTI/LTV. Redis cache → re-assemble if stale."""
    redis = request.app.state.redis_store
    tid = get_tenant_id(request)
    cached = await redis.get_application_context(application_id, tenant_id=tid)
    if cached:
        return {"source": "cache", "data": cached}

    assembler = _context_assembler(request)
    try:
        ctx = await assembler.assemble(application_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return {"source": "database", "data": ctx.model_dump()}


@router.get(
    "/application/{application_id}/readiness",
    dependencies=[Depends(verify_api_key)],
    summary="Readiness flags only",
    description="Lightweight 19-flag readiness view for AUS-ready polling.",
    responses={
        401: {"description": "Missing or invalid `X-API-Key`."},
        404: {"description": "Application not found."},
    },
)
async def get_application_readiness(request: Request, application_id: str):
    """Lightweight readiness flags for "are we ready for AUS?" polling."""
    redis = request.app.state.redis_store
    cached = await redis.get_application_context(application_id)
    if cached:
        return {
            "application_id": application_id,
            "readiness":      cached.get("readiness") or {},
            "source":         "cache",
        }
    assembler = _context_assembler(request)
    try:
        ctx = await assembler.assemble(application_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return {
        "application_id": application_id,
        "readiness":      ctx.readiness.model_dump(),
        "source":         "database",
    }


@router.post(
    "/application/{application_id}/refresh-context",
    dependencies=[Depends(verify_api_key)],
)
async def refresh_application_context(request: Request, application_id: str):
    """Force re-assembly even if cached. Useful after a batch upload."""
    redis = request.app.state.redis_store
    await redis.invalidate_context(application_id)
    assembler = _context_assembler(request)
    try:
        ctx = await assembler.assemble(application_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return {"source": "database", "data": ctx.model_dump()}


@router.get(
    "/application/{application_id}/dti",
    dependencies=[Depends(verify_api_key)],
)
async def get_application_dti(request: Request, application_id: str):
    """DTI breakdown — derives PITI, income, and obligations from context."""
    redis = request.app.state.redis_store
    cached = await redis.get_application_context(application_id)
    if not cached:
        assembler = _context_assembler(request)
        try:
            ctx = await assembler.assemble(application_id)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc))
        cached = ctx.model_dump()

    property_block = cached.get("property") or {}
    return {
        "application_id":              application_id,
        "front_end_dti":               cached.get("front_end_dti"),
        "back_end_dti":                cached.get("back_end_dti"),
        "piti_total":                  property_block.get("piti_total"),
        "combined_qualifying_monthly": cached.get("combined_qualifying_monthly"),
        "total_obligations":           cached.get("total_monthly_obligations"),
    }


# ---------------------------------------------------------------------------
# Phase D: vendor return adapters
# ---------------------------------------------------------------------------


def _vendor_adapter_for(vendor_type: str, vendor: str):
    vendor_type = (vendor_type or "").lower()
    vendor = (vendor or "").lower()
    if vendor_type == "aus":
        return VendorAUSAdapter()
    if vendor_type == "fraud":
        return VendorFraudAdapter()
    if vendor_type == "voe":
        return VendorVOEAdapter()
    if vendor_type == "ssn":
        return VendorSSNAdapter()
    if vendor_type == "ofac":
        return VendorOFACAdapter()
    if vendor_type == "flood":
        # The flood vendor return is a simple JSON; reuse the property
        # FLOOD_CERT shape directly via a passthrough adapter.
        return None
    raise HTTPException(
        status_code=400,
        detail=f"Unknown vendor_type: {vendor_type!r}",
    )


def _vendor_payload_for_aus(payload: dict) -> dict:
    """Translate the wrapped /ingest/vendor-return body into the AUS
    adapter's expected shape. Accepts both ``response.xml_content`` and
    a top-level ``xml_content``."""
    response = payload.get("response") or {}
    xml = response.get("xml_content") or payload.get("xml_content") or ""
    return {
        "aus_type":       (payload.get("vendor") or "DU").upper(),
        "xml_content":    xml,
        "applicant_id":   payload.get("applicant_id"),
        "application_id": payload.get("application_id"),
    }


def _vendor_payload_passthrough(payload: dict) -> dict:
    return {
        "vendor":         payload.get("vendor"),
        "response":       payload.get("response") or {},
        "applicant_id":   payload.get("applicant_id"),
        "application_id": payload.get("application_id"),
    }


@router.post("/ingest/vendor-return", dependencies=[Depends(verify_api_key)])
async def ingest_vendor_return(request: Request, body: dict):
    """Universal vendor-return receiver.

    Body shape::

        {
          "vendor_type":   "aus" | "fraud" | "voe" | "ssn" | "ofac" | "flood",
          "vendor":        "du" | "lp" | "socure" | "lexisnexis" | "twn" |
                           "equifax_voe" | "ssa" | "ofac",
          "response":      { ...vendor JSON or {xml_content: str} },
          "application_id": "APP-...",
          "applicant_id":   "APL-..."
        }
    """
    vendor_type = (body.get("vendor_type") or "").lower()
    application_id = body.get("application_id")
    applicant_id = body.get("applicant_id")
    if not vendor_type or not application_id:
        raise HTTPException(
            status_code=400,
            detail="body must include vendor_type and application_id",
        )

    pg = request.app.state.postgres_store
    redis = request.app.state.redis_store

    if vendor_type == "flood":
        # Flood returns reuse the FLOOD_CERT shape directly — no adapter,
        # the response itself is the extracted_fields blob.
        fields = body.get("response") or {}
        document_type = "FLOOD_CERT"
        confidence = 0.99
    else:
        adapter = _vendor_adapter_for(vendor_type, body.get("vendor") or "")
        adapter_payload = (
            _vendor_payload_for_aus(body) if vendor_type == "aus"
            else _vendor_payload_passthrough(body)
        )
        event = adapter.process(adapter_payload)
        fields = event.extracted_fields
        document_type = event.document_type
        confidence = event.confidence

    import uuid as _uuid
    document_id = f"VENDOR-{_uuid.uuid4().hex[:12]}"
    if applicant_id:
        try:
            await pg.save_document({
                "document_id":      document_id,
                "applicant_id":     applicant_id,
                "application_id":   application_id,
                "document_type":    document_type,
                "document_category": "vendor",
                "borrower_role":    "primary",
                "s3_key":           None,
                "status":           "received",
                "is_current":       True,
                "extracted_fields": fields,
                "confidence_score": confidence,
            })
        except Exception as exc:
            logger.warning("vendor_save_document_failed", extra={"error": str(exc)})

    # Drop the cached context — next /context call recomputes including
    # the freshly-landed vendor return.
    await redis.invalidate_context(application_id)

    return {
        "ingest_id":             document_id,
        "document_type":         document_type,
        "vendor_checks_updated": True,
        "extracted":             fields,
    }


@router.get(
    "/application/{application_id}/vendor-checks",
    dependencies=[Depends(verify_api_key)],
)
async def get_application_vendor_checks(
    request: Request, application_id: str
):
    redis = request.app.state.redis_store
    cached = await redis.get_application_context(application_id)
    if cached and cached.get("vendor_checks") is not None:
        return {
            "application_id": application_id,
            "vendor_checks":  cached.get("vendor_checks") or {},
            "source":         "cache",
        }
    assembler = _context_assembler(request)
    try:
        ctx = await assembler.assemble(application_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return {
        "application_id": application_id,
        "vendor_checks":  ctx.vendor_checks,
        "source":         "database",
    }


@router.post(
    "/application/{application_id}/run-vendor-checks",
    dependencies=[Depends(verify_api_key)],
)
async def run_vendor_checks(request: Request, application_id: str):
    """Demo path — generate synthetic vendor responses, run every
    adapter, and re-assemble the context. Useful for end-to-end tests."""
    from core.ingestion.adapters import vendor_synthetic

    pg = request.app.state.postgres_store
    redis = request.app.state.redis_store

    app = await pg.get_application(application_id)
    if not app:
        raise HTTPException(status_code=404, detail="Application not found")
    applicant_id = app["applicant_id"]

    cached = await redis.get_application_context(application_id) or {}
    primary = (cached.get("primary") or {}) if cached else {}
    property_block = cached.get("property") if cached else None
    credit_score = primary.get("mid_score") or 720
    front_dti = cached.get("front_end_dti") or 38.0
    ltv = cached.get("ltv") or 80.0
    flood_zone = (property_block or {}).get("flood_zone") or "X"
    employer_name = "Acme Corp"
    annual_salary = 96_000.0

    # Build synthetic returns and route each through its adapter
    submissions = [
        {
            "vendor_type": "aus",
            "vendor":      "du",
            "response":    {"xml_content": vendor_synthetic.generate_du_response(
                credit_score=credit_score,
                dti=front_dti,
                ltv=ltv,
                loan_type=app.get("loan_type") or "conventional",
            )},
        },
        {
            "vendor_type": "fraud",
            "vendor":      "socure",
            "response":    vendor_synthetic.generate_fraud_response(applicant_id, "low"),
        },
        {
            "vendor_type": "voe",
            "vendor":      "twn",
            "response":    vendor_synthetic.generate_voe_response(
                employer_name, annual_salary
            ),
        },
        {
            "vendor_type": "ssn",
            "vendor":      "ssa",
            "response":    vendor_synthetic.generate_ssn_response(verified=True),
        },
        {
            "vendor_type": "ofac",
            "vendor":      "ofac",
            "response":    vendor_synthetic.generate_ofac_response(hit=False),
        },
        {
            "vendor_type": "flood",
            "vendor":      "fema",
            "response":    vendor_synthetic.generate_flood_response(flood_zone),
        },
    ]

    submitted = []
    for sub in submissions:
        body = {
            **sub,
            "application_id": application_id,
            "applicant_id":   applicant_id,
        }
        try:
            result = await ingest_vendor_return(request, body)
            submitted.append({
                "vendor_type":   sub["vendor_type"],
                "document_type": result["document_type"],
                "ingest_id":     result["ingest_id"],
            })
        except HTTPException as exc:
            submitted.append({
                "vendor_type": sub["vendor_type"],
                "error":       exc.detail,
            })

    await redis.invalidate_context(application_id)
    ctx = await _context_assembler(request).assemble(application_id)
    return {
        "application_id": application_id,
        "submitted":      submitted,
        "vendor_checks":  ctx.vendor_checks,
        "readiness":      ctx.readiness.model_dump(),
    }


# ---------------------------------------------------------------------------
# Phase E: persona slices, webhooks, context versioning, missing-docs
# ---------------------------------------------------------------------------


async def _ctx_dict(request: Request, application_id: str) -> dict:
    """Return the cached context dict, falling through to a fresh
    assembly when nothing is cached. Raises 404 if the application
    doesn't exist."""
    redis = request.app.state.redis_store
    cached = await redis.get_application_context(application_id)
    if cached:
        return cached
    assembler = _context_assembler(request)
    try:
        ctx = await assembler.assemble(application_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return ctx.model_dump()


# ---- persona slices --------------------------------------------------------


@router.get(
    "/application/{application_id}/context/income",
    dependencies=[Depends(verify_api_key)],
)
async def get_income_slice(request: Request, application_id: str):
    ctx = await _ctx_dict(request, application_id)
    primary = ctx.get("primary") or {}
    co = ctx.get("co_borrower") or {}
    slice_ = IncomeSlice(
        application_id=application_id,
        primary_qualifying_monthly=float(primary.get("qualifying_monthly") or 0),
        primary_income_sources=primary.get("income_sources") or [],
        primary_income_confidence=float(primary.get("income_confidence") or 0),
        primary_income_verified=bool(primary.get("income_verified", False)),
        co_borrower_qualifying=(
            float(co.get("qualifying_monthly")) if co else None
        ),
        combined_qualifying_monthly=float(ctx.get("combined_qualifying_monthly") or 0),
        dti_calculable=bool((ctx.get("readiness") or {}).get("dti_calculable", False)),
        front_end_dti=ctx.get("front_end_dti"),
        back_end_dti=ctx.get("back_end_dti"),
        income_requires_review=bool(primary.get("income_requires_review", False)),
        assembled_at=ctx.get("assembled_at", ""),
    )
    return slice_.model_dump()


@router.get(
    "/application/{application_id}/context/credit",
    dependencies=[Depends(verify_api_key)],
)
async def get_credit_slice(request: Request, application_id: str):
    ctx = await _ctx_dict(request, application_id)
    primary = ctx.get("primary") or {}
    co = ctx.get("co_borrower") or {}
    derog = bool(primary.get("derogatory_marks") or 0)
    slice_ = CreditSlice(
        application_id=application_id,
        primary_mid_score=int(primary.get("mid_score") or 0),
        primary_credit_band=primary.get("credit_band") or "",
        primary_obligations=float(primary.get("monthly_obligations") or 0),
        co_borrower_mid_score=(int(co.get("mid_score")) if co else None),
        qualifying_score_used=int(ctx.get("qualifying_score_used") or 0),
        total_obligations=float(ctx.get("total_monthly_obligations") or 0),
        derogatory_flags=derog,
        assembled_at=ctx.get("assembled_at", ""),
    )
    return slice_.model_dump()


@router.get(
    "/application/{application_id}/context/property",
    dependencies=[Depends(verify_api_key)],
)
async def get_property_slice(request: Request, application_id: str):
    ctx = await _ctx_dict(request, application_id)
    prop = ctx.get("property") or {}
    readiness = ctx.get("readiness") or {}
    slice_ = PropertySlice(
        application_id=application_id,
        appraised_value=prop.get("appraised_value"),
        ltv=prop.get("ltv") if prop else ctx.get("ltv"),
        piti_total=prop.get("piti_total"),
        piti_breakdown=prop.get("piti_components"),
        flood_zone=prop.get("flood_zone"),
        condition_rating=prop.get("condition_rating"),
        appraisal_complete=bool(readiness.get("appraisal_complete", False)),
        requires_review=bool(ctx.get("requires_review", False)),
        assembled_at=ctx.get("assembled_at", ""),
    )
    return slice_.model_dump()


@router.get(
    "/application/{application_id}/context/compliance",
    dependencies=[Depends(verify_api_key)],
)
async def get_compliance_slice(request: Request, application_id: str):
    ctx = await _ctx_dict(request, application_id)
    pg = request.app.state.postgres_store
    aus = (ctx.get("vendor_checks") or {}).get("aus_findings") or {}
    app_row = await pg.get_application(application_id)
    hmda = (app_row or {}).get("hmda_fields") or {}
    if isinstance(hmda, str):
        import json as _json
        try:
            hmda = _json.loads(hmda)
        except Exception:
            hmda = {}
    slice_ = ComplianceSlice(
        application_id=application_id,
        readiness=ReadinessFlags(**(ctx.get("readiness") or {})),
        missing_items=(ctx.get("readiness") or {}).get("missing_items") or [],
        aus_recommendation=aus.get("recommendation"),
        hmda_fields=hmda,
        requires_review=bool(ctx.get("requires_review", False)),
        assembled_at=ctx.get("assembled_at", ""),
    )
    return slice_.model_dump()


@router.get(
    "/application/{application_id}/context/fraud",
    dependencies=[Depends(verify_api_key)],
)
async def get_fraud_slice(request: Request, application_id: str):
    ctx = await _ctx_dict(request, application_id)
    vc = ctx.get("vendor_checks") or {}
    slice_ = FraudSlice(
        application_id=application_id,
        fraud_score=vc.get("fraud_score"),
        fraud_band=vc.get("fraud_band"),
        ssn_valid=vc.get("ssn_valid"),
        ofac_clear=vc.get("ofac_clear"),
        employment_verified=vc.get("employment_verified"),
        requires_review=bool(vc.get("fraud_requires_review")),
        assembled_at=ctx.get("assembled_at", ""),
    )
    return slice_.model_dump()


# ---- webhooks --------------------------------------------------------------


@router.post("/webhooks", dependencies=[Depends(verify_api_key)])
async def register_webhook(request: Request, body: dict):
    if not body.get("name") or not body.get("url"):
        raise HTTPException(
            status_code=400, detail="name and url required"
        )
    pg = request.app.state.postgres_store
    webhook_id = await pg.save_webhook({
        "name":   body["name"],
        "url":    body["url"],
        "secret": body.get("secret"),
        "events": body.get("events") or ["context_updated"],
    })
    return {
        "webhook_id": webhook_id,
        "name":       body["name"],
        "url":        body["url"],
        "is_active":  True,
    }


@router.get("/webhooks", dependencies=[Depends(verify_api_key)])
async def list_webhooks(request: Request):
    pg = request.app.state.postgres_store
    rows = await pg.list_webhooks()
    return {"count": len(rows), "webhooks": rows}


@router.delete(
    "/webhooks/{webhook_id}", dependencies=[Depends(verify_api_key)]
)
async def deactivate_webhook(request: Request, webhook_id: str):
    pg = request.app.state.postgres_store
    await pg.deactivate_webhook(webhook_id)
    return {"webhook_id": webhook_id, "is_active": False}


@router.get(
    "/webhooks/{webhook_id}/deliveries",
    dependencies=[Depends(verify_api_key)],
    summary="Outbox + audit deliveries for a webhook",
    description=(
        "Surfaces the async-outbox view first (every enqueued delivery "
        "with its `pending` / `delivered` / `failed` state, attempt "
        "count, last error, and timestamps). Use `?status=pending` / "
        "`failed` / `delivered` to filter. The legacy `webhook_deliveries` "
        "audit history is preserved under `audit_history` so older "
        "consumers don't break."
    ),
)
async def list_webhook_deliveries(
    request: Request,
    webhook_id: str,
    status: Optional[str] = None,
    page_size: int = 20,
    audit_limit: int = 50,
):
    if status and status not in {"pending", "delivered", "failed"}:
        raise HTTPException(
            status_code=422,
            detail="status must be one of pending|delivered|failed",
        )
    pg = request.app.state.postgres_store
    outbox = await pg.get_outbox_for_webhook(
        webhook_id, status=status, limit=page_size,
    )
    audit  = await pg.get_webhook_deliveries(webhook_id, limit=audit_limit)
    return {
        "webhook_id":     webhook_id,
        "count":          len(outbox),
        "deliveries":     outbox,
        "audit_history":  audit,
        "filters":        {"status": status, "page_size": page_size},
    }


@router.post(
    "/webhooks/{webhook_id}/retry-failed",
    dependencies=[Depends(verify_api_key)],
    summary="Reset every failed outbox row for this webhook to pending",
    description=(
        "Operator-driven recovery hook: when a subscriber comes back "
        "online after an outage, this resets all of its `failed` outbox "
        "rows back to `pending` with `attempts=0` and "
        "`next_retry_at=NOW()`. The delivery worker picks them up on "
        "the next tick. Returns the count of rows reset."
    ),
)
async def retry_failed_deliveries(request: Request, webhook_id: str):
    pg = request.app.state.postgres_store
    reset_count = await pg.reset_failed_outbox(webhook_id)
    return {
        "webhook_id":  webhook_id,
        "reset_count": reset_count,
    }


# ---- context versioning ----------------------------------------------------


@router.get(
    "/application/{application_id}/context/history",
    dependencies=[Depends(verify_api_key)],
)
async def get_context_history(
    request: Request, application_id: str, limit: int = 10
):
    pg = request.app.state.postgres_store
    rows = await pg.get_context_versions(application_id, limit=limit)
    versions = [
        {
            "version_id":     r.get("version_id"),
            "assembled_at":   r.get("assembled_at"),
            "trigger_event":  r.get("trigger_event"),
            "trigger_doc_id": r.get("trigger_doc_id"),
        }
        for r in rows
    ]
    return {"application_id": application_id, "versions": versions}


@router.get(
    "/application/{application_id}/context/at/{timestamp}",
    dependencies=[Depends(verify_api_key)],
)
async def get_context_at_timestamp(
    request: Request, application_id: str, timestamp: str
):
    pg = request.app.state.postgres_store
    row = await pg.get_context_at(application_id, timestamp)
    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"no context version at or before {timestamp}",
        )
    return {
        "application_id": application_id,
        "timestamp":      timestamp,
        "version":        {
            "version_id":     row.get("version_id"),
            "assembled_at":   row.get("assembled_at"),
            "trigger_event":  row.get("trigger_event"),
            "trigger_doc_id": row.get("trigger_doc_id"),
        },
        "context":        row.get("context_data"),
    }


# ---- missing documents -----------------------------------------------------
#
# The catalog is the authoritative list of every doc type a complete
# mortgage file is expected to carry. Required slots gate close;
# conditional slots only apply when the loan / property / borrower meets
# a stated rule (self-employed, gift funds, coastal flood zone, etc.).
# A slot is "received" when document_index has at least one row whose
# canonical document_type matches the slot's doc_type or any of its
# ``alternates`` (e.g. W2_CURRENT slot accepts W2_PRIOR; AUS slot
# accepts DU OR LP findings).


_REQUIRED_DOCS = [
    # Income — at least one current proof. W2_PRIOR satisfies if no
    # current W2 yet (e.g. mid-year hire).
    {"item": "W-2 — current year",            "category": "income",     "doc_type": "W2_CURRENT",       "alternates": ["W2_PRIOR"]},
    {"item": "Pay stub — most recent",        "category": "income",     "doc_type": "PAYSTUB_CURRENT",  "alternates": ["PAYSTUB_PRIOR"]},
    # Credit
    {"item": "Credit report",                 "category": "credit",     "doc_type": "CREDIT_REPORT"},
    # Asset
    {"item": "Bank statement — month 1",      "category": "asset",      "doc_type": "BANK_STATEMENT_M1"},
    # Identity / KYC
    {"item": "Driver's license",              "category": "identity",   "doc_type": "IDENTITY_DL"},
    {"item": "SSN validation",                "category": "identity",   "doc_type": "SSN_VALIDATION",   "alternates": ["IDENTITY_SSN_CARD"]},
    {"item": "OFAC clearance",                "category": "identity",   "doc_type": "OFAC_REPORT"},
    # Property
    {"item": "URAR appraisal",                "category": "property",   "doc_type": "APPRAISAL_URAR"},
    {"item": "Title commitment",              "category": "property",   "doc_type": "TITLE_COMMITMENT"},
    {"item": "Homeowner's insurance binder",  "category": "property",   "doc_type": "HOI_BINDER",       "alternates": ["HOI_DECLARATIONS"]},
    {"item": "Flood certificate",             "category": "property",   "doc_type": "FLOOD_CERT"},
    {"item": "Property tax bill",             "category": "property",   "doc_type": "PROPERTY_TAX_BILL"},
    # Loan terms
    {"item": "URLA / 1003",                   "category": "loan_terms", "doc_type": "URLA_1003"},
    {"item": "Purchase agreement",            "category": "loan_terms", "doc_type": "PURCHASE_AGREEMENT"},
    # Vendor — DU or LP satisfies. Either AUS_LP_FINDINGS or
    # AUS_DU_FINDINGS in have closes the slot.
    {"item": "AUS findings",                  "category": "vendor",     "doc_type": "AUS_DU_FINDINGS",  "alternates": ["AUS_LP_FINDINGS"]},
]


_CONDITIONAL_DOCS = [
    {"item": "IRS wage & income transcript",  "category": "income",     "doc_type": "IRS_TRANSCRIPT",
     "reason": "required if self-employed or income > $100k"},
    {"item": "Form 1040",                     "category": "income",     "doc_type": "TAX_RETURN_1040_CURRENT",
     "reason": "required if self-employed"},
    {"item": "Schedule C",                    "category": "income",     "doc_type": "SCHEDULE_C",
     "reason": "required if self-employed (sole proprietor)"},
    {"item": "Schedule E",                    "category": "income",     "doc_type": "SCHEDULE_E",
     "reason": "required if rental income claimed"},
    {"item": "Gift letter",                   "category": "asset",      "doc_type": "GIFT_LETTER",
     "reason": "required if gift funds used for down payment"},
    {"item": "Wind / hail insurance",         "category": "property",   "doc_type": "WIND_HAIL_INSURANCE",
     "reason": "required if property in TX/FL coastal counties"},
    {"item": "Wood-destroying-organism (WDO) report", "category": "property", "doc_type": "PEST_WDO_INSPECTION",
     "reason": "required by state (FL, TX, LA, etc.)"},
    {"item": "Well & septic inspection",      "category": "property",   "doc_type": "WELL_SEPTIC_INSPECTION",
     "reason": "required if rural property"},
    {"item": "HOA certification",             "category": "property",   "doc_type": "HOA_CERT",
     "reason": "required if condo or PUD"},
]


def _slot_received(slot: dict, have: set) -> bool:
    """Return True when ``have`` contains the slot's doc_type or any of
    its alternates."""
    if slot["doc_type"] in have:
        return True
    for alt in slot.get("alternates") or []:
        if alt in have:
            return True
    return False


@router.get(
    "/application/{application_id}/missing-documents",
    dependencies=[Depends(verify_api_key)],
)
async def get_missing_documents(request: Request, application_id: str):
    """Return a comprehensive checklist of every doc type the loan file
    is expected to carry. Required slots gate close; conditional slots
    return with the rule that triggers them so the caller can decide
    whether they apply."""
    from core.ingestion.mismo import canonicalize_doc_type

    pg = request.app.state.postgres_store
    docs = await pg.get_documents_for_application(application_id)
    have: set[str] = {canonicalize_doc_type(d.get("document_type")) for d in docs}
    have.discard(None)
    # The borrower-required income / asset / identity docs are usually
    # rowed against the applicant rather than the application — pull
    # those too.
    app_row = await pg.get_application(application_id)
    if app_row:
        try:
            applicant_docs = await pg.get_documents_for_applicant(
                app_row["applicant_id"]
            )
        except Exception:
            applicant_docs = []
        have |= {canonicalize_doc_type(d.get("document_type"))
                 for d in applicant_docs}
        have.discard(None)
        # Co-applicant docs too — joint applications often file a single
        # set of identity / asset docs under the co-borrower.
        co_aid = app_row.get("co_applicant_id")
        if co_aid:
            try:
                co_docs = await pg.get_documents_for_applicant(co_aid)
            except Exception:
                co_docs = []
            have |= {canonicalize_doc_type(d.get("document_type"))
                     for d in co_docs}
            have.discard(None)

    required_missing = [
        s for s in _REQUIRED_DOCS if not _slot_received(s, have)
    ]
    conditional_missing = [
        s for s in _CONDITIONAL_DOCS if not _slot_received(s, have)
    ]

    total_expected = len(_REQUIRED_DOCS)
    total_received = total_expected - len(required_missing)
    completeness_pct = round(total_received / total_expected * 100, 1) \
        if total_expected else 0.0

    return {
        "application_id":     application_id,
        "required":           required_missing,
        "conditional":        conditional_missing,
        "received":           sorted(have),
        "total_expected":     total_expected,
        "total_received":     total_received,
        "completeness_pct":   completeness_pct,
    }


# ---------------------------------------------------------------------------
# Phase F: pipeline observability — dashboard, pipeline-state, timeline
# ---------------------------------------------------------------------------


async def _get_borrower_name(pg, applicant_id: str) -> str:
    if not applicant_id:
        return "—"
    try:
        gr = await pg.find_by_applicant_id(applicant_id)
    except Exception:
        gr = None
    return (gr or {}).get("full_name") or applicant_id


def _flag_html(val: bool) -> str:
    cls = "green" if val else "red"
    sym = "&#10003;" if val else "&#10007;"  # ✓ / ✗
    return f'<span class="{cls}">{sym}</span>'


def _render_dashboard(summaries: list) -> str:
    rows = ""
    for s in summaries:
        front_dti = s.get("front_dti")
        ltv = s.get("ltv")
        dti_class = "red" if (front_dti or 0) > 43 else (
            "yellow" if (front_dti or 0) > 36 else "green"
        )
        ltv_class = "red" if (ltv or 0) > 95 else (
            "yellow" if (ltv or 0) > 80 else "green"
        )
        row_class = "review" if s.get("requires_review") else ""
        rows += (
            f'<tr class="{row_class}" data-href="/application/{s["application_id"]}/pipeline-state">'
            f'<td><code>{s["application_id"]}</code></td>'
            f'<td>{s.get("los_id","")}</td>'
            f'<td>{s.get("borrower_name","")}</td>'
            f'<td>{_flag_html(s.get("income_verified"))}</td>'
            f'<td>{_flag_html(s.get("credit_pulled"))}</td>'
            f'<td>{_flag_html(s.get("appraisal_done"))}</td>'
            f'<td>{_flag_html(s.get("aus_ready"))}</td>'
            f'<td class="{dti_class}">'
            f'{f"{front_dti:.1f}%" if front_dti is not None else "&mdash;"}'
            f'</td>'
            f'<td class="{ltv_class}">'
            f'{f"{ltv:.1f}%" if ltv is not None else "&mdash;"}'
            f'</td>'
            f'<td class="{"red" if s.get("conflicts",0) > 0 else "green"}">'
            f'{s.get("conflicts",0)}</td>'
            f'<td class="{"red" if s.get("requires_review") else "green"}">'
            f'{"&#9888; Review" if s.get("requires_review") else "&#10003; OK"}'
            f'</td>'
            f'</tr>'
        )
    total = len(summaries)
    aus_ready = sum(1 for s in summaries if s.get("aus_ready"))
    review_n  = sum(1 for s in summaries if s.get("requires_review"))
    missing_n = sum(1 for s in summaries if s.get("missing_count", 0) > 0)

    empty_row = (
        '<tr><td colspan="11" style="text-align:center;color:#8b949e">'
        'No applications yet. POST /loans or run scripts/watch_pipeline.py.'
        '</td></tr>'
    )
    return f"""<!DOCTYPE html>
<html>
<head>
  <title>EDMS Pipeline Dashboard</title>
  <meta http-equiv="refresh" content="15">
  <style>
    body {{ font-family: monospace; background: #0d1117; color: #c9d1d9; padding: 20px; }}
    h1 {{ color: #58a6ff; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
    th {{ background: #161b22; color: #8b949e; padding: 8px 12px; text-align: left;
          border-bottom: 1px solid #30363d; font-size: 11px; text-transform: uppercase; }}
    td {{ padding: 8px 12px; border-bottom: 1px solid #21262d; font-size: 12px; }}
    tr:hover {{ background: #161b22; cursor: pointer; }}
    tr.review {{ background: #2d1a1a; }}
    .green {{ color: #3fb950; }}
    .yellow {{ color: #d29922; }}
    .red {{ color: #f85149; }}
    .stats {{ display: flex; gap: 20px; margin: 20px 0; }}
    .stat {{ background: #161b22; border: 1px solid #30363d; border-radius: 6px;
             padding: 12px 20px; }}
    .stat-val {{ font-size: 24px; font-weight: bold; color: #58a6ff; }}
    .stat-lbl {{ font-size: 11px; color: #8b949e; margin-top: 4px; }}
    code {{ background: #161b22; padding: 2px 6px; border-radius: 3px;
            font-size: 11px; color: #79c0ff; }}
  </style>
</head>
<body>
  <h1>EDMS Pipeline Dashboard</h1>
  <p style="color:#8b949e; font-size:12px">
    Auto-refreshes every 15 seconds &nbsp;|&nbsp;
    {total} applications &nbsp;|&nbsp;
    Click any row for detail JSON.
  </p>
  <div class="stats">
    <div class="stat"><div class="stat-val">{total}</div><div class="stat-lbl">Total Applications</div></div>
    <div class="stat"><div class="stat-val green">{aus_ready}</div><div class="stat-lbl">AUS Ready</div></div>
    <div class="stat"><div class="stat-val red">{review_n}</div><div class="stat-lbl">Requires Review</div></div>
    <div class="stat"><div class="stat-val yellow">{missing_n}</div><div class="stat-lbl">Missing Documents</div></div>
  </div>
  <table>
    <thead>
      <tr>
        <th>Application ID</th><th>LOS ID</th><th>Borrower</th>
        <th>Income</th><th>Credit</th><th>Appraisal</th><th>AUS</th>
        <th>Front DTI</th><th>LTV</th><th>Conflicts</th><th>Status</th>
      </tr>
    </thead>
    <tbody>{rows if rows else empty_row}</tbody>
  </table>
  <script>
    document.querySelectorAll("tr[data-href]").forEach(r => {{
      r.addEventListener("click", () => window.location = r.dataset.href);
    }});
  </script>
</body>
</html>"""


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Live HTML dashboard. No auth — read-only summary view; safe to
    leave open in a browser tab.
    """
    pg = request.app.state.postgres_store
    redis = request.app.state.redis_store
    apps = await pg.get_all_applications(limit=50)

    summaries: list = []
    for app in apps:
        ctx = (await redis.get_application_context(app["application_id"])) or {}
        primary = ctx.get("primary") or {}
        summaries.append({
            "application_id":  app["application_id"],
            "los_id":          app.get("los_id", "") or "",
            "borrower_name":   await _get_borrower_name(pg, app.get("applicant_id")),
            "status":          app.get("status", "active"),
            "income_verified": bool(primary.get("income_verified")),
            "credit_pulled":   (primary.get("mid_score") or 0) > 300,
            "appraisal_done":  bool(ctx.get("property")),
            "aus_ready":       bool((ctx.get("readiness") or {}).get("aus_ready")),
            "front_dti":       ctx.get("front_end_dti"),
            "ltv":             ctx.get("ltv"),
            "conflicts":       (ctx.get("graph_summary") or {}).get("conflict_count", 0),
            "requires_review": bool(ctx.get("requires_review")),
            "missing_count":   len((ctx.get("readiness") or {}).get("missing_items") or []),
        })
    return HTMLResponse(_render_dashboard(summaries))


# ---- pipeline-state --------------------------------------------------------


@router.get(
    "/application/{application_id}/pipeline-state",
    dependencies=[Depends(verify_api_key)],
)
async def application_pipeline_state(request: Request, application_id: str):
    pg = request.app.state.postgres_store
    redis = request.app.state.redis_store
    raw_store = getattr(request.app.state, "raw_store", None) or RawIngestionStore()

    app = await pg.get_application(application_id)
    if not app:
        raise HTTPException(status_code=404, detail="Application not found")

    ctx = await redis.get_application_context(application_id)
    if not ctx:
        try:
            ctx_obj = await ContextAssembler(pg, redis).assemble(application_id)
            ctx = ctx_obj.model_dump()
        except Exception:
            ctx = {}

    # Per-borrower roll-up
    borrowers: list = []
    for role, applicant_id in (
        ("primary", app.get("applicant_id")),
        ("co_borrower", app.get("co_applicant_id")),
    ):
        if not applicant_id:
            continue
        gr = await pg.find_by_applicant_id(applicant_id)
        try:
            docs = await pg.get_documents_for_applicant(applicant_id)
        except Exception:
            docs = []
        try:
            raw_state = await raw_store.get_pipeline_state(applicant_id)
        except Exception:
            raw_state = {"received": 0, "extracting": 0, "indexed": 0,
                         "failed": 0, "reprocessing": 0, "total": 0}
        income = await pg.get_income_profile(applicant_id) or {}
        credit = await pg.get_credit_profile(applicant_id) or {}
        section_key = "co_borrower" if role == "co_borrower" else "primary_borrower"
        section = (income.get(section_key) or {}) if income else {}
        borrowers.append({
            "applicant_id":   applicant_id,
            "full_name":      (gr or {}).get("full_name") or applicant_id,
            "role":           role,
            "raw_ingestion":  raw_state,
            "documents":      [
                {
                    "document_id":   d.get("document_id"),
                    "document_type": d.get("document_type"),
                    "confidence":    d.get("confidence_score"),
                    "received_at":   str(d.get("received_at") or ""),
                }
                for d in docs
            ],
            "income": {
                "qualifying_monthly": section.get("qualifying_monthly"),
                "confidence":         section.get("overall_confidence"),
                "assembled_at":       income.get("assembled_at"),
            },
            "credit": {
                "mid_score":   credit.get("mid_score"),
                "band":        credit.get("credit_band"),
                "obligations": credit.get("total_monthly_obligations"),
            },
            "redis_keys": {
                "income": await redis.key_state(f"income:{applicant_id}"),
                "credit": await redis.key_state(f"credit:{applicant_id}"),
                "status": await redis.key_state(f"status:{applicant_id}"),
            },
        })

    # Property roll-up
    property_block = None
    property_id = app.get("property_id")
    if property_id:
        prop = await pg.get_property(property_id)
        try:
            prop_docs = await pg.get_property_docs(property_id)
        except Exception:
            prop_docs = []
        profile = await pg.get_property_profile(property_id) or {}
        piti = profile.get("piti_components") or {}
        appraised = profile.get("appraised_value")
        loan_amount = app.get("loan_amount")
        ltv = (
            round(float(loan_amount) / float(appraised) * 100, 2)
            if appraised and loan_amount else None
        )
        address = ""
        if prop:
            address = (
                f"{prop.get('address_line1','')}, "
                f"{prop.get('city','')}, "
                f"{prop.get('state','')}"
            )
        property_block = {
            "property_id":  property_id,
            "address":      address,
            "documents":    [
                {"type": d.get("document_type"),
                 "confidence": d.get("confidence_score")}
                for d in prop_docs
            ],
            "profile": {
                "appraised_value": appraised,
                "piti_total":      piti.get("total_piti"),
                "ltv":             ltv,
                "flood_zone":      profile.get("flood_zone"),
            },
            "redis_key": await redis.key_state(f"property:{property_id}"),
        }

    # Graph
    primary_id = app.get("applicant_id")
    try:
        graph_summary = await pg.get_graph_summary(primary_id) if primary_id else {}
    except Exception:
        graph_summary = {}
    try:
        rels = (
            await pg.get_relationships_for_applicant(primary_id)
            if primary_id else []
        )
    except Exception:
        rels = []
    edges = [
        {
            "type":         r.get("relationship_type"),
            "field":        r.get("field_name"),
            "source_value": r.get("source_value"),
            "target_value": r.get("target_value"),
            "delta_pct":    r.get("delta_pct"),
            "confidence":   r.get("confidence"),
        }
        for r in rels[:25]
    ]
    graph_block = {
        "node_count":     graph_summary.get("document_count", 0),
        "edge_count":     graph_summary.get("relationship_count", 0),
        "conflict_count": graph_summary.get("conflict_count", 0),
        "edges":          edges,
    }

    vendor_checks = ctx.get("vendor_checks") or {}
    readiness = ctx.get("readiness") or {}
    ctx_key_state = await redis.key_state(f"context:{application_id}")
    context_block = {
        "present":         ctx_key_state.get("present", False),
        "ttl_seconds":     ctx_key_state.get("ttl_seconds"),
        "front_end_dti":   ctx.get("front_end_dti"),
        "back_end_dti":    ctx.get("back_end_dti"),
        "ltv":             ctx.get("ltv"),
        "requires_review": bool(ctx.get("requires_review")),
    }

    pipeline_complete = bool(
        readiness.get("income_verified")
        and readiness.get("credit_pulled")
        and readiness.get("appraisal_complete")
        and readiness.get("insurance_bound")
        and readiness.get("aus_ready")
        and not ctx.get("requires_review")
    )

    return {
        "application_id": application_id,
        "application": {
            "los_id":       app.get("los_id"),
            "status":       app.get("status"),
            "loan_amount":  app.get("loan_amount"),
            "loan_type":    app.get("loan_type"),
            "loan_purpose": app.get("loan_purpose"),
            "created_at":   str(app.get("created_at") or ""),
        },
        "borrowers":     borrowers,
        "property":      property_block,
        "graph":         graph_block,
        "vendor_checks": vendor_checks,
        "context":       context_block,
        "readiness":     readiness,
        "pipeline_complete": pipeline_complete,
    }


# ---- timeline --------------------------------------------------------------


@router.get(
    "/application/{application_id}/timeline",
    dependencies=[Depends(verify_api_key)],
)
async def application_timeline(request: Request, application_id: str):
    pg = request.app.state.postgres_store
    app = await pg.get_application(application_id)
    if not app:
        raise HTTPException(status_code=404, detail="Application not found")

    events: list = []
    events.append({
        "timestamp":   str(app.get("created_at") or ""),
        "event_type":  "application_submitted",
        "layer":       "borrower",
        "description": f"Application created — LOS: {app.get('los_id','')}",
    })

    try:
        raw = await pg.get_raw_ingestion_for_application(application_id)
    except Exception:
        raw = []
    for r in raw:
        size = r.get("raw_size_bytes") or 0
        events.append({
            "timestamp":   str(r.get("received_at") or ""),
            "event_type":  "document_received",
            "layer":       "document",
            "description": (
                f"{(r.get('source_channel') or '').upper()} — "
                f"{r.get('filename') or 'unnamed'} ({size:,} bytes)"
            ),
        })
        if r.get("extracted_at"):
            events.append({
                "timestamp":   str(r["extracted_at"]),
                "event_type":  "extraction_complete",
                "layer":       "document",
                "description": (
                    f"Extracted {r.get('document_id') or '?'} "
                    f"status={r.get('status')}"
                ),
            })

    primary_id = app.get("applicant_id")
    if primary_id:
        try:
            rels = await pg.get_relationships_for_applicant(primary_id)
        except Exception:
            rels = []
        for rel in rels:
            reasoning = (rel.get("reasoning") or "")[:100]
            field = rel.get("field_name") or "?"
            rtype = rel.get("relationship_type") or "edge"
            events.append({
                "timestamp":   str(rel.get("created_at") or ""),
                "event_type":  f"graph_edge_{rtype}",
                "layer":       "graph",
                "description": f"{rtype.upper()}: {field} — {reasoning}",
            })

    try:
        versions = await pg.get_context_versions(application_id, limit=50)
    except Exception:
        versions = []
    for v in versions:
        version_id = str(v.get("version_id") or "")
        events.append({
            "timestamp":   str(v.get("assembled_at") or ""),
            "event_type":  "context_assembled",
            "layer":       "context",
            "description": (
                f"Context v{version_id[:8]} — trigger: "
                f"{v.get('trigger_event') or 'manual'}"
            ),
        })

    events.sort(key=lambda e: str(e.get("timestamp") or ""))
    return {"application_id": application_id, "events": events}


# ---------------------------------------------------------------------------
# Incremental indexer: status, run, runs history, watermark override
# ---------------------------------------------------------------------------


def _build_indexer(request: Request) -> BatchIndexer:
    return BatchIndexer(
        postgres_store=request.app.state.postgres_store,
        redis_store=request.app.state.redis_store,
        aggregation_service=request.app.state.aggregation_service,
        s3_client=getattr(request.app.state, "s3_client", None),
    )


@router.get("/indexing/status", dependencies=[Depends(verify_api_key)])
async def indexing_status(request: Request, source: str = "s3"):
    pg = request.app.state.postgres_store
    wm = await pg.get_watermark(source)
    runs = await pg.get_indexing_runs(source=source, limit=1)
    last_run = runs[0] if runs else None
    return {
        "source":          source,
        "last_indexed_at": (wm or {}).get("last_indexed_at"),
        "last_run_at":     (wm or {}).get("last_run_at"),
        "status":          (wm or {}).get("status") or "idle",
        "files_processed": (wm or {}).get("files_processed") or 0,
        "files_skipped":   (wm or {}).get("files_skipped") or 0,
        "errors":          (wm or {}).get("errors") or 0,
        "last_run":        last_run,
    }


@router.post("/indexing/run", dependencies=[Depends(verify_api_key)])
async def indexing_run(request: Request, body: dict | None = None):
    body = body or {}
    source = body.get("source", "s3")
    dry_run = bool(body.get("dry_run", False))
    indexer = _build_indexer(request)
    stats = await indexer.run(source=source, dry_run=dry_run)
    return stats


@router.get("/indexing/runs", dependencies=[Depends(verify_api_key)])
async def indexing_runs(
    request: Request, source: Optional[str] = None, limit: int = 50
):
    pg = request.app.state.postgres_store
    rows = await pg.get_indexing_runs(source=source, limit=limit)
    return {"count": len(rows), "runs": rows}


@router.get(
    "/indexing/runs/{run_id}", dependencies=[Depends(verify_api_key)]
)
async def indexing_run_detail(request: Request, run_id: str):
    pg = request.app.state.postgres_store
    row = await pg.get_indexing_run(run_id)
    if not row:
        raise HTTPException(status_code=404, detail="indexing run not found")
    return row


_ADMIN_ALLOWED_TABLES = {
    "applicants", "applicant_identity_xref", "applications",
    "income_profiles", "credit_profiles",
    "document_index", "document_relationships",
    "properties", "property_profiles",
    "raw_ingestion", "context_versions",
    "indexing_watermarks", "indexing_runs",
    "webhooks", "webhook_deliveries",
    "mismo_doc_type_registry", "los_connectors",
}


@router.get(
    "/admin/table-count/{table_name}", dependencies=[Depends(verify_api_key)]
)
async def admin_table_count(request: Request, table_name: str):
    if table_name not in _ADMIN_ALLOWED_TABLES:
        raise HTTPException(
            status_code=400,
            detail=f"table {table_name!r} not in allowed list",
        )
    pg = request.app.state.postgres_store
    try:
        count = await pg.get_table_count(table_name)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return {"table": table_name, "count": count}


@router.put("/indexing/watermark", dependencies=[Depends(verify_api_key)])
async def indexing_set_watermark(request: Request, body: dict):
    """Manually move the watermark. ⚠ resets which files get re-indexed
    on the next run. Use carefully.

    Body: ``{"source": "s3", "timestamp": "2026-05-06T00:00:00Z"}``
    """
    source = body.get("source", "s3")
    timestamp = body.get("timestamp")
    if not timestamp:
        raise HTTPException(status_code=400, detail="timestamp required")
    pg = request.app.state.postgres_store
    store = WatermarkStore(pg)
    from datetime import datetime as _dt
    try:
        ts = _dt.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HTTPException(
            status_code=400, detail=f"invalid timestamp: {exc}"
        )
    await store.set_timestamp(source, ts)
    wm = await pg.get_watermark(source)
    return {
        "source": source,
        "last_indexed_at": (wm or {}).get("last_indexed_at"),
    }
