"""Tenant + API-key admin endpoints.

All endpoints require the caller's API key to carry the ``admin`` scope
(enforced via ``Depends(require_admin)``). The development key
``edms_dev_key`` is seeded with ``read,write,admin`` so the local
workflow + the existing 329 tests can administer freely; production
deployments should provision narrower keys.
"""
from __future__ import annotations

import logging
import secrets as _secrets
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from api.routes import require_admin

logger = logging.getLogger(__name__)
router = APIRouter()


# ---------------------------------------------------------------------------
# Request / response shapes
# ---------------------------------------------------------------------------


class CreateTenantRequest(BaseModel):
    tenant_id: str = Field(..., min_length=1, max_length=50,
                            description="Stable lower_snake identifier (e.g. `acme_lending`).")
    name:      str = Field(..., min_length=1, max_length=200,
                            description="Human-friendly display name.")


class TenantResponse(BaseModel):
    tenant_id:  str
    name:       str
    is_active:  bool = True
    created_at: Optional[str] = None


class CreateApiKeyRequest(BaseModel):
    tenant_id: str = Field(..., min_length=1, max_length=50)
    name:      Optional[str] = Field(None, max_length=100,
                                      description="Operator note (e.g. `Production Key`).")
    scopes:    str = Field("read,write",
                            description="Comma-separated scope list. Use `read,write,admin` for an operator key.")


class ApiKeyResponse(BaseModel):
    api_key:   str
    tenant_id: str
    name:      Optional[str] = None
    scopes:    str
    is_active: bool = True
    created_at: Optional[str] = None


class ApiKeyMaskedResponse(BaseModel):
    """Listing shape — masks the secret so an operator's screen-share
    doesn't accidentally leak a production key."""
    api_key_masked: str
    tenant_id:      str
    name:           Optional[str] = None
    scopes:         str
    is_active:      bool
    created_at:     Optional[str] = None
    last_used_at:   Optional[str] = None


def _mask(api_key: str) -> str:
    if not api_key:
        return ""
    if len(api_key) <= 8:
        return "***"
    return f"{api_key[:4]}…{api_key[-4:]}"


def _row_str(value) -> Optional[str]:
    if value is None:
        return None
    try:
        return value.isoformat()  # datetime
    except AttributeError:
        return str(value)


# ---------------------------------------------------------------------------
# Tenants
# ---------------------------------------------------------------------------


@router.post(
    "/tenants",
    response_model=TenantResponse,
    summary="Create a new tenant",
    dependencies=[Depends(require_admin)],
    responses={
        401: {"description": "Missing or invalid `X-API-Key`."},
        403: {"description": "`admin` scope required."},
        422: {"description": "tenant_id or name failed validation."},
    },
)
async def create_tenant(request: Request, body: CreateTenantRequest):
    pg = request.app.state.postgres_store
    existing = await pg.get_tenant(body.tenant_id)
    if existing:
        # Idempotent: re-creating with the same id is a no-op rather than
        # an error; admin scripts can run repeatedly without 409 noise.
        return TenantResponse(
            tenant_id=existing["tenant_id"],
            name=existing.get("name", body.name),
            is_active=bool(existing.get("is_active", True)),
            created_at=_row_str(existing.get("created_at")),
        )
    row = await pg.create_tenant(body.tenant_id, body.name)
    return TenantResponse(
        tenant_id=row["tenant_id"],
        name=row.get("name", body.name),
        is_active=bool(row.get("is_active", True)),
        created_at=_row_str(row.get("created_at")),
    )


@router.get(
    "/tenants",
    summary="List tenants",
    dependencies=[Depends(require_admin)],
    responses={
        401: {"description": "Missing or invalid `X-API-Key`."},
        403: {"description": "`admin` scope required."},
    },
)
async def list_tenants(request: Request):
    pg = request.app.state.postgres_store
    rows = await pg.list_tenants()
    return {
        "tenants": [
            {
                "tenant_id":  r["tenant_id"],
                "name":       r.get("name"),
                "is_active":  bool(r.get("is_active", True)),
                "created_at": _row_str(r.get("created_at")),
            }
            for r in rows
        ],
        "count": len(rows),
    }


# ---------------------------------------------------------------------------
# API keys
# ---------------------------------------------------------------------------


@router.post(
    "/api-keys",
    response_model=ApiKeyResponse,
    summary="Provision a new API key for a tenant",
    dependencies=[Depends(require_admin)],
    responses={
        200: {
            "description": (
                "API key generated. The plain-text key is returned ONCE — "
                "store it in the consumer's secret manager immediately. "
                "Subsequent listings only show a masked form."
            ),
        },
        401: {"description": "Missing or invalid `X-API-Key`."},
        403: {"description": "`admin` scope required."},
        404: {"description": "Tenant does not exist — create it first."},
    },
)
async def create_api_key(request: Request, body: CreateApiKeyRequest):
    pg = request.app.state.postgres_store
    tenant = await pg.get_tenant(body.tenant_id)
    if not tenant:
        raise HTTPException(
            status_code=404,
            detail=f"Tenant not found: {body.tenant_id}. Create via POST /admin/tenants first.",
        )
    new_key = f"edms_{_secrets.token_urlsafe(32)}"
    row = await pg.create_api_key(
        api_key=new_key,
        tenant_id=body.tenant_id,
        name=body.name,
        scopes=body.scopes,
    )
    if not row:
        raise HTTPException(
            status_code=500,
            detail="Failed to provision API key — DB returned no row.",
        )
    return ApiKeyResponse(
        api_key=new_key,
        tenant_id=row["tenant_id"],
        name=row.get("name"),
        scopes=row.get("scopes", body.scopes),
        is_active=bool(row.get("is_active", True)),
        created_at=_row_str(row.get("created_at")),
    )


@router.get(
    "/api-keys",
    summary="List API keys (masked)",
    dependencies=[Depends(require_admin)],
    responses={
        401: {"description": "Missing or invalid `X-API-Key`."},
        403: {"description": "`admin` scope required."},
    },
)
async def list_api_keys(
    request: Request,
    tenant_id: Optional[str] = None,
):
    pg = request.app.state.postgres_store
    rows = await pg.list_api_keys(tenant_id=tenant_id)
    return {
        "api_keys": [
            {
                "api_key_masked": _mask(r["api_key"]),
                "tenant_id":      r["tenant_id"],
                "name":           r.get("name"),
                "scopes":         r.get("scopes", ""),
                "is_active":      bool(r.get("is_active", True)),
                "created_at":     _row_str(r.get("created_at")),
                "last_used_at":   _row_str(r.get("last_used_at")),
            }
            for r in rows
        ],
        "count": len(rows),
    }


@router.delete(
    "/api-keys/{api_key}",
    summary="Deactivate an API key",
    dependencies=[Depends(require_admin)],
    responses={
        401: {"description": "Missing or invalid `X-API-Key`."},
        403: {"description": "`admin` scope required."},
        404: {"description": "API key not found."},
    },
)
async def deactivate_api_key(request: Request, api_key: str):
    pg = request.app.state.postgres_store
    existing = await pg.get_api_key(api_key)
    if not existing:
        raise HTTPException(status_code=404, detail="API key not found")
    await pg.deactivate_api_key(api_key)
    # Bust the auth cache so the deactivation takes effect on the very
    # next request instead of waiting up to 5 min for the cache to
    # expire. Best-effort — failures here are logged but never block.
    redis = request.app.state.redis_store
    try:
        await redis._r.delete(f"apikey:{api_key}")
    except Exception as exc:
        logger.warning("apikey_cache_evict_failed", extra={"error": str(exc)})
    return {
        "api_key_masked": _mask(api_key),
        "tenant_id":      existing["tenant_id"],
        "is_active":      False,
    }


@router.post(
    "/reset",
    summary="DESTRUCTIVE — wipe all loan data for a clean slate",
    description=(
        "TRUNCATEs every loan-data table (entity_states, snapshots, "
        "events, document_index, document_relationships, "
        "graph_build_runs, applications, applicants, "
        "indexing_watermarks) and FLUSHDB on Redis so the next build "
        "starts from zero. Tenants + api_keys + webhooks are "
        "preserved. Use BEFORE swapping the connector source to a new "
        "simulation tree, or after a generator change that produces "
        "different applicant_ids.\n\n"
        "**This is a hard reset** — there is no undo. The operation "
        "is logged but no backup is taken."
    ),
    dependencies=[Depends(require_admin)],
    responses={
        200: {"description": "Reset complete; row counts after truncate."},
        401: {"description": "Missing or invalid `X-API-Key`."},
        403: {"description": "`admin` scope required."},
    },
)
async def admin_reset(request: Request):
    pg    = request.app.state.postgres_store
    redis = request.app.state.redis_store
    # Order matters because of FK constraints: child tables before
    # parents. CASCADE on the tail handles anything we forget.
    tables = (
        "entity_state_events",
        "entity_snapshots",
        "entity_states",
        "graph_build_runs",
        "indexing_watermarks",
        "document_relationships",
        "document_index",
        "income_profiles",
        "credit_profiles",
        "applications",
        "applicant_identity_xref",
        "applicants",
    )
    truncated: list = []
    failed:    list = []
    from core.storage import db
    for t in tables:
        try:
            await db.execute(f"TRUNCATE TABLE {t} CASCADE")
            truncated.append(t)
        except Exception as exc:
            failed.append({"table": t, "error": str(exc)[:200]})
            logger.warning(
                f"admin_reset_truncate_failed table={t} "
                f"error_type={type(exc).__name__} error={str(exc)[:200]}"
            )
    # Reset the applicant sequence so APL-00001-P starts fresh.
    try:
        await db.execute("ALTER SEQUENCE applicant_sequence RESTART WITH 1")
    except Exception as exc:
        logger.warning(
            f"admin_reset_sequence_reset_failed error={str(exc)[:200]}"
        )
    # FLUSHDB Redis — best-effort; a Redis blip shouldn't fail the
    # whole reset since the next build re-warms caches.
    redis_flushed = False
    try:
        await redis._r.flushdb()
        redis_flushed = True
    except Exception as exc:
        logger.warning(
            f"admin_reset_redis_flush_failed error={str(exc)[:200]}"
        )

    logger.info(
        f"admin_reset_complete tenant={getattr(request.state, 'tenant_id', '?')} "
        f"truncated={len(truncated)} failed={len(failed)} "
        f"redis_flushed={redis_flushed}"
    )
    return {
        "status":            "reset_complete",
        "tables_truncated":  len(truncated),
        "truncated":         truncated,
        "failed":            failed,
        "redis_flushed":     redis_flushed,
    }


# =====================================================================
# Golden-record backfill — POST /admin/rebuild-golden-records (202'd to
# background) + GET /admin/rebuild-golden-records/status. See
# core/aggregation/golden_record_builder.py for the per-application
# orchestrator that backs both endpoints.
# =====================================================================
import asyncio as _asyncio
from datetime import datetime as _dt, timezone as _tz

# In-process guard so a second POST on the same replica can't spawn a
# duplicate background task. Cross-replica safety comes from the PG-
# backed watermark + idempotent UPSERTs — two replicas racing produces
# correct (if wasteful) writes.
_REBUILD_LOCK = _asyncio.Lock()


async def _run_rebuild_bg(pg, redis, tenant_id: str, force: bool) -> None:
    """Background task launched by POST. Swallows top-level exceptions
    into the watermark row so the asyncio future doesn't surface an
    uncaught traceback in CloudWatch."""
    from core.aggregation.golden_record_builder import (
        run_backfill, read_backfill_state, write_backfill_state,
    )
    try:
        await run_backfill(pg, redis, tenant_id=tenant_id, force=force)
    except Exception as exc:
        logger.error(
            f"rebuild_golden_records_bg_failed tenant={tenant_id} "
            f"error_type={type(exc).__name__} error={str(exc)[:300]}"
        )
        state = await read_backfill_state(tenant_id=tenant_id)
        state["status"]       = "failed"
        state["completed_at"] = _dt.now(_tz.utc)
        state.setdefault("errors", []).append({
            "application_id": None,
            "error":          f"{type(exc).__name__}: {str(exc)[:400]}",
            "at":             _dt.now(_tz.utc).isoformat(),
        })
        await write_backfill_state(state, tenant_id=tenant_id)


@router.post(
    "/rebuild-golden-records",
    summary="Rebuild income / credit / xref / entity_states from already-indexed docs",
    description=(
        "Backfill orchestrator. Reads every application in the tenant "
        "ORDER BY application_id ASC, re-runs the income + credit "
        "assemblers against the docs currently in ``document_index``, "
        "writes ``income_profiles`` / ``credit_profiles`` / "
        "``applicant_identity_xref`` / ``entity_states`` for each, and "
        "logs an ``entity_state_events`` row.\n\n"
        "**Restartable.** Progress is committed per application to "
        "``golden_record_backfill_state``. On crash, the next POST "
        "resumes after the last committed ``application_id`` — passing "
        "``?force=true`` resets the watermark and re-UPSERTs every row "
        "from scratch.\n\n"
        "Returns immediately with HTTP 202; poll "
        "``GET /admin/rebuild-golden-records/status`` for progress."
    ),
    status_code=202,
    dependencies=[Depends(require_admin)],
    responses={
        202: {"description": "Background task accepted."},
        401: {"description": "Missing or invalid `X-API-Key`."},
        403: {"description": "`admin` scope required."},
        409: {"description": "A backfill is already running."},
    },
)
async def rebuild_golden_records(request: Request, force: bool = False):
    pg    = request.app.state.postgres_store
    redis = request.app.state.redis_store
    tenant_id = getattr(request.state, "tenant_id", "default") or "default"

    from core.aggregation.golden_record_builder import (
        read_backfill_state, count_applications,
    )

    async with _REBUILD_LOCK:
        existing = await read_backfill_state(tenant_id=tenant_id)
        if existing.get("status") == "running" and not force:
            return {
                "status":         "already_running",
                "completed":      existing.get("completed_count") or 0,
                "total":          existing.get("total_count") or 0,
                "last_completed": existing.get("last_completed_application_id"),
                "started_at":     (existing.get("started_at").isoformat()
                                   if hasattr(existing.get("started_at"), "isoformat")
                                   else existing.get("started_at")),
            }
        total = await count_applications(tenant_id)
        _asyncio.create_task(_run_rebuild_bg(pg, redis, tenant_id, force))
        logger.info(
            f"rebuild_golden_records_started tenant={tenant_id} "
            f"force={force} total_applications={total}"
        )
        return {
            "status":    "started",
            "force":     bool(force),
            "total":     total,
            "tenant_id": tenant_id,
            "message":   "Poll GET /admin/rebuild-golden-records/status for progress.",
        }


@router.get(
    "/rebuild-golden-records/status",
    summary="Poll the running golden-record backfill",
    description=(
        "Returns the current state of the most recent backfill (or "
        "``{'status': 'not_started'}`` if one has never run for this "
        "tenant). Updated by the background task after every "
        "application, so a 5-second poll cadence reflects progress "
        "within a few apps."
    ),
    dependencies=[Depends(require_admin)],
    responses={
        200: {"description": "Current backfill state."},
        401: {"description": "Missing or invalid `X-API-Key`."},
        403: {"description": "`admin` scope required."},
    },
)
async def rebuild_golden_records_status(request: Request):
    tenant_id = getattr(request.state, "tenant_id", "default") or "default"
    from core.aggregation.golden_record_builder import read_backfill_state
    s = await read_backfill_state(tenant_id=tenant_id)

    def _iso(v):
        return v.isoformat() if hasattr(v, "isoformat") else v

    started   = s.get("started_at")
    completed = s.get("completed_at")
    elapsed   = None
    if started:
        try:
            sd = started if isinstance(started, _dt) else _dt.fromisoformat(
                str(started).replace("Z", "+00:00")
            )
            end = completed if isinstance(completed, _dt) else (
                _dt.fromisoformat(str(completed).replace("Z", "+00:00"))
                if completed else _dt.now(_tz.utc)
            )
            elapsed = int((end - sd).total_seconds())
        except Exception:
            elapsed = None

    return {
        "status":            s.get("status") or "not_started",
        "completed":         int(s.get("completed_count") or 0),
        "total":             int(s.get("total_count") or 0),
        "last_completed":    s.get("last_completed_application_id"),
        "errors":            s.get("errors") or [],
        "started_at":        _iso(started),
        "completed_at":      _iso(completed),
        "elapsed_seconds":   elapsed,
        "tenant_id":         tenant_id,
    }
