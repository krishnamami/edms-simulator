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
