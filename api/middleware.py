"""Per-request structlog middleware. Mirrors ai_document_decision_engine."""
import time
import uuid

import structlog
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

logger = structlog.get_logger()


class RequestMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        request_id = request.headers.get("X-Request-ID", str(uuid.uuid4()))
        log = logger.bind(
            request_id=request_id,
            method=request.method,
            path=request.url.path,
        )
        start = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception as e:
            log.error("request_failed", error=str(e))
            raise
        elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
        log.info(
            "request_done",
            status_code=response.status_code,
            elapsed_ms=elapsed_ms,
        )
        response.headers["X-Request-ID"] = request_id
        return response
