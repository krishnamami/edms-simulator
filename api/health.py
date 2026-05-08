"""Liveness (/health) and readiness (/ready) endpoints."""
from fastapi import APIRouter, Request

from api.schemas import HealthResponse, ReadyResponse

health_router = APIRouter(tags=["System"])


@health_router.get(
    "/health",
    response_model=HealthResponse,
    summary="Liveness probe",
    description="Returns 200 if the FastAPI process is up. No upstream dependencies are checked.",
)
async def health():
    return HealthResponse(status="ok", version="0.1.0")


@health_router.get(
    "/ready",
    response_model=ReadyResponse,
    summary="Readiness probe (Postgres + Redis)",
    description=(
        "Returns 200 in both healthy and degraded states; the body's "
        "`status` field is `ok` when both Postgres and Redis are reachable, "
        "`degraded` otherwise. Use the `postgres` / `redis` booleans to "
        "diagnose which dependency is down."
    ),
)
async def ready(request: Request):
    pg_ok = False
    redis_ok = False
    try:
        from core.storage import db
        await db.fetchval("SELECT 1")
        pg_ok = True
    except Exception:
        pg_ok = False
    try:
        store = getattr(request.app.state, "redis_store", None)
        redis_ok = bool(store and store.ping())
    except Exception:
        redis_ok = False
    status = "ok" if pg_ok and redis_ok else "degraded"
    return ReadyResponse(status=status, postgres=pg_ok, redis=redis_ok)
