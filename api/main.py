"""FastAPI app entry point. Lifespan-managed dependency wiring."""
import logging
import os
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.stdlib.add_log_level,
        structlog.processors.JSONRenderer(),
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)
logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    from core.aggregation.service import AggregationService
    from core.credit.assembler import CreditAssembler
    from core.identity.xref_store import XRefStore
    from core.income.assembler import IncomeAssembler
    from core.storage import db
    from core.storage.postgres_store import PostgresStore
    from core.storage.redis_store import RedisStore
    from core.storage.s3_client import S3Client

    try:
        await db.get_pool()
    except Exception as e:
        logging.warning("aurora_pool_not_available_at_startup: %s", e)

    app.state.redis_store = RedisStore()
    app.state.postgres_store = PostgresStore()
    app.state.s3_client = S3Client()
    app.state.xref_store = XRefStore()

    # Hydrate the in-memory XRefStore from Postgres so that applicant_id
    # sequence + SSN / source-id lookups survive across restarts. Without
    # this, the first POST /loans after a redeploy collides on
    # APL-00001-P and silently overwrites an existing applicant via
    # save_golden_record's ON CONFLICT DO UPDATE.
    try:
        loaded, max_seq = await app.state.xref_store.hydrate_from_postgres(
            app.state.postgres_store
        )
        logging.info(
            "xref_store_hydrated", extra={"applicants": loaded, "max_seq": max_seq}
        )
    except Exception as e:
        logging.warning("xref_store_hydration_failed: %s", e)

    app.state.aggregation_service = AggregationService(
        xref_store=app.state.xref_store,
        golden_record_store=app.state.xref_store,
        income_assembler=IncomeAssembler(),
        credit_assembler=CreditAssembler(),
        redis_store=app.state.redis_store,
        postgres_store=app.state.postgres_store,
    )

    # Incremental indexer background scheduler. Off by default; flip on
    # in production via ENABLE_SCHEDULER=true. Avoids spurious S3 calls
    # in local development and keeps tests deterministic.
    app.state.scheduler = None
    if os.getenv("ENABLE_SCHEDULER", "false").lower() == "true":
        try:
            from apscheduler.schedulers.asyncio import AsyncIOScheduler

            from core.indexing.batch_indexer import BatchIndexer

            interval = int(os.getenv("INDEX_INTERVAL_MINUTES", "15"))

            async def _scheduled_index():
                try:
                    indexer = BatchIndexer(
                        postgres_store=app.state.postgres_store,
                        redis_store=app.state.redis_store,
                        aggregation_service=app.state.aggregation_service,
                        s3_client=app.state.s3_client,
                    )
                    stats = await indexer.run(source="s3")
                    logging.info(
                        "scheduled_index_complete",
                        extra={k: stats.get(k) for k in (
                            "found", "processed", "skipped",
                            "applicants_affected", "errors", "run_id",
                        )},
                    )
                except Exception as exc:
                    logging.error(
                        "scheduled_index_failed", extra={"error": str(exc)}
                    )

            sched = AsyncIOScheduler()
            sched.add_job(
                _scheduled_index,
                "interval",
                minutes=interval,
                id="incremental_indexer",
                max_instances=1,
                coalesce=True,
            )
            sched.start()
            app.state.scheduler = sched
            logging.info(
                "scheduler_started",
                extra={"interval_minutes": interval},
            )
        except Exception as exc:
            logging.warning("scheduler_start_failed: %s", exc)

    yield
    try:
        if getattr(app.state, "scheduler", None):
            app.state.scheduler.shutdown(wait=False)
    except Exception:
        pass
    try:
        await db.close_pool()
    except Exception:
        pass


# OpenAPI tag groups. Order here drives the section order in /docs and
# /redoc; the descriptions appear under each group heading. Keep these
# in sync with the per-route tags= values — Swagger UI silently puts
# any unknown tag at the bottom under its raw name.
OPENAPI_TAGS = [
    {
        "name": "Application",
        "description": (
            "**Interface 1 — real-time per-entity context.** Single-entity "
            "reads + writes used by Decision OS to ingest one borrower or one "
            "document and read the assembled context, readiness flags, and "
            "knowledge-graph view. Backed by Redis (4h income/credit, 30m "
            "context) with Postgres as the source of truth."
        ),
    },
    {
        "name": "Reports",
        "description": (
            "**Interface 2 — aggregated cross-loan analytics.** Paginated, "
            "filtered queries for ops, compliance, and dashboards. Pipeline "
            "summary, conflicts, completeness, extraction quality, "
            "stated-vs-documented income. 5-minute Redis cache on every "
            "response."
        ),
    },
    {
        "name": "Export",
        "description": (
            "**Interface 3 — bulk JSONL/CSV streams for data warehouses.** "
            "Server-side cursor streaming so a Snowflake/Redshift/BigQuery "
            "consumer can pull tens of thousands of rows without buffering. "
            "Full snapshots and `?since=<ts>` incremental dumps. Per-consumer "
            "watermarks let pipelines resume from their last successful pull. "
            "Rate-limited to 10 requests / hour / API key."
        ),
    },
    {
        "name": "System",
        "description": (
            "Operational endpoints: liveness, readiness, indexing run "
            "control, watermark admin, webhook subscription management, and "
            "the public dashboard."
        ),
    },
    {
        "name": "Admin",
        "description": (
            "Multi-tenancy administration. Create tenants, provision per-"
            "tenant API keys, and manage their lifecycle. All endpoints "
            "require an API key with the `admin` scope."
        ),
    },
]

API_DESCRIPTION = """\
Document indexing, entity aggregation, and knowledge-graph service for
mortgage lending. Three consumption interfaces sit on top of the same
underlying borrower / property / vendor data layer:

- **Application API** — real-time, per-entity context for decision systems
- **Report API**      — aggregated analytics for ops + compliance dashboards
- **Bulk Export API** — streaming JSONL/CSV exports for data warehouses

## Authentication

Every endpoint except `/health`, `/ready`, `/dashboard`, `/docs`,
`/redoc`, and `/openapi.json` requires an `X-API-Key` header. The local
development key is `edms_dev_key` (override via the `EDMS_API_KEY` /
`API_KEY` env var).

## Common error codes

| Code | Meaning |
|------|---------|
| 401  | Missing / invalid `X-API-Key` |
| 404  | Application / applicant / document not found |
| 422  | Request validation failed (bad ISO timestamp, page size out of range, …) |
| 429  | Rate limit exceeded (Bulk Export only — 10 req/hr/key) |
| 502  | Upstream Anthropic API error during chat / image / email ingestion |
| 503  | `ANTHROPIC_API_KEY` not configured for a Claude-only path |
"""


app = FastAPI(
    title="EDMS Knowledge Graph API",
    description=API_DESCRIPTION,
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_url="/openapi.json",
    openapi_tags=OPENAPI_TAGS,
    contact={
        "name": "EDMS Platform Team",
        "url": "https://github.com/krishnamami/edms-simulator",
    },
    license_info={"name": "Proprietary"},
    lifespan=lifespan,
)

from api.admin import router as admin_router  # noqa: E402
from api.exports import router as exports_router  # noqa: E402
from api.health import health_router  # noqa: E402
from api.middleware import RequestMiddleware  # noqa: E402
from api.reports import router as reports_router  # noqa: E402
from api.routes import router  # noqa: E402

app.add_middleware(RequestMiddleware)
app.include_router(router)
app.include_router(health_router)
app.include_router(reports_router, prefix="/reports", tags=["Reports"])
app.include_router(exports_router, prefix="/export", tags=["Export"])
app.include_router(admin_router, prefix="/admin", tags=["Admin"])


# OpenAPI tag classifier — centralized so we don't hand-edit tags on
# all 60+ routes in api/routes.py. Path-pattern → tag mapping. The
# "Reports" / "Export" entries are redundant (those routers already
# have tags from include_router), but listing them keeps the rules
# self-documenting if someone adds a new path under those prefixes.
_TAG_RULES: list[tuple[str, str]] = [
    ("/reports/",            "Reports"),
    ("/export/",             "Export"),
    ("/admin/",              "Admin"),
    # System: ops + observability + indexing + webhooks
    ("/health",              "System"),
    ("/ready",               "System"),
    ("/dashboard",           "System"),
    ("/admin/",              "System"),
    ("/indexing/",           "System"),
    ("/webhooks",            "System"),
    ("/pipeline/failed",     "System"),
    ("/mismo/",              "System"),
    # Everything else is the per-entity Application API
]


def _classify_path(path: str) -> str:
    for prefix, tag in _TAG_RULES:
        if path.startswith(prefix) or path == prefix.rstrip("/"):
            return tag
    return "Application"


def custom_openapi():
    """Override the default OpenAPI generator so each operation lands
    in the right Swagger UI / Redoc section regardless of how its
    decorator was written. Runs once and caches; clear
    ``app.openapi_schema = None`` if you need to regenerate."""
    if app.openapi_schema:
        return app.openapi_schema
    from fastapi.openapi.utils import get_openapi
    schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
        tags=OPENAPI_TAGS,
        contact=app.contact,
        license_info=app.license_info,
    )
    # Document the X-API-Key security scheme once at the top level so
    # Swagger UI's "Authorize" button picks it up. Then mark every
    # operation as requiring it — except the public /health, /ready,
    # /dashboard, and the docs endpoints themselves.
    components = schema.setdefault("components", {})
    schemes = components.setdefault("securitySchemes", {})
    schemes["ApiKeyAuth"] = {
        "type": "apiKey",
        "in":   "header",
        "name": "X-API-Key",
        "description": (
            "Static per-environment API key. The local-dev key is "
            "`edms_dev_key`; production reads `edms/api/keys` from "
            "AWS Secrets Manager. Send on every request as the "
            "`X-API-Key` header."
        ),
    }
    public_paths = {"/health", "/ready", "/dashboard",
                    "/docs", "/redoc", "/openapi.json"}
    for path, methods in schema.get("paths", {}).items():
        for method, op in methods.items():
            if not isinstance(op, dict):
                continue
            tag = _classify_path(path)
            op["tags"] = [tag]
            if path not in public_paths:
                op.setdefault("security", [{"ApiKeyAuth": []}])
    app.openapi_schema = schema
    return schema


app.openapi = custom_openapi
