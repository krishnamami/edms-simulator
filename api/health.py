"""Liveness (/health) and readiness (/ready) endpoints."""
from fastapi import APIRouter, Request

from api.schemas import HealthResponse, ReadyResponse

health_router = APIRouter(tags=["health"])


@health_router.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse(status="ok", version="0.1.0")


@health_router.get("/ready", response_model=ReadyResponse)
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
