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


app = FastAPI(title="EDMS Simulator", version="0.1.0", lifespan=lifespan)

from api.exports import router as exports_router  # noqa: E402
from api.health import health_router  # noqa: E402
from api.middleware import RequestMiddleware  # noqa: E402
from api.reports import router as reports_router  # noqa: E402
from api.routes import router  # noqa: E402

app.add_middleware(RequestMiddleware)
app.include_router(router)
app.include_router(health_router)
app.include_router(reports_router, prefix="/reports", tags=["reports"])
app.include_router(exports_router, prefix="/export", tags=["exports"])
