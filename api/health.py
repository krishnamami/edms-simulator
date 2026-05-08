"""Liveness (/health) and readiness (/ready) endpoints."""
import logging

from fastapi import APIRouter, Request

from api.schemas import HealthResponse, ReadyResponse

logger = logging.getLogger(__name__)

health_router = APIRouter(tags=["System"])


@health_router.get(
    "/health",
    summary="Liveness probe + webhook outbox stats",
    description=(
        "Returns 200 if the FastAPI process is up. The body now also "
        "includes `webhook_outbox` queue stats — pending/failed counts, "
        "deliveries in the last hour, and the oldest pending row's age. "
        "If `oldest_pending_age_seconds` climbs above the worker's "
        "polling interval × max_attempts, the delivery worker is "
        "falling behind."
    ),
)
async def health(request: Request):
    base = HealthResponse(status="ok", version="0.1.0").model_dump()
    pg = getattr(request.app.state, "postgres_store", None)
    if pg is None or not hasattr(pg, "get_outbox_stats"):
        return base
    try:
        base["webhook_outbox"] = await pg.get_outbox_stats()
    except Exception as exc:
        # Health must never fail loudly because a stats query hiccupped.
        logger.warning("health_outbox_stats_failed", extra={"error": str(exc)[:200]})
        base["webhook_outbox"] = {"error": "stats_unavailable"}
    return base


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
