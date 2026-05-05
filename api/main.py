"""FastAPI app entry point. Lifespan-managed dependency wiring."""
import logging
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
    app.state.aggregation_service = AggregationService(
        xref_store=app.state.xref_store,
        golden_record_store=app.state.xref_store,
        income_assembler=IncomeAssembler(),
        credit_assembler=CreditAssembler(),
        redis_store=app.state.redis_store,
        postgres_store=app.state.postgres_store,
    )
    yield
    try:
        await db.close_pool()
    except Exception:
        pass


app = FastAPI(title="EDMS Simulator", version="0.1.0", lifespan=lifespan)

from api.health import health_router  # noqa: E402
from api.middleware import RequestMiddleware  # noqa: E402
from api.routes import router  # noqa: E402

app.add_middleware(RequestMiddleware)
app.include_router(router)
app.include_router(health_router)
