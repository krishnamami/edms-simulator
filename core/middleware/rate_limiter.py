"""Per-API-key rate limiting backed by Redis INCR + EXPIRE.

Three tiers:
- ``application`` — single-loan reads + writes that Decision OS calls
  hot-path. Generous: 1000 req / minute / key.
- ``reports``     — analytics queries that fan out across many rows.
  Tighter: 100 req / minute / key.
- ``export``      — bulk JSONL/CSV streams against the warehouse.
  Strict: 10 req / hour / key (these can each pull 100k+ rows).

Limits are per ``X-API-Key`` value, not per tenant — that lets one
tenant provision a high-volume production key alongside a low-volume
dev key without them sharing a quota.

The middleware skips public paths (`/health`, `/ready`, `/docs`,
`/redoc`, `/openapi.json`, `/dashboard`) and the entire `/admin/*`
surface (operators administering tenants shouldn't trip a quota).

Failure mode: any Redis error is treated as "fail open" — bulk
exports + report dashboards are SLA-critical for downstream pipelines,
and a Redis outage shouldn't black-hole legitimate traffic. The error
is logged at WARNING; the request proceeds without rate-limit headers.
"""
from __future__ import annotations

import json
import logging
import time
from typing import Optional

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

logger = logging.getLogger(__name__)


# (max_requests, window_seconds) per group.
RATE_LIMITS: dict[str, tuple[int, int]] = {
    "application": (1000, 60),
    "reports":     (100,  60),
    "export":      (10,   3600),
}

# Path-prefix → group classifier. First match wins; the catch-all
# "application" tier covers /loans, /documents, /applicant, /application,
# /ingest, /properties, /webhooks, /loan, /resolve, /pipeline, etc.
_GROUP_RULES: list[tuple[str, str]] = [
    ("/reports/", "reports"),
    ("/export/",  "export"),
]

# Paths that bypass rate limiting entirely. Includes the public docs
# endpoints (no key needed), the health probes (called by load balancers
# at sub-second intervals), and the admin surface (operator scope).
_BYPASS_PREFIXES: tuple[str, ...] = (
    "/health",
    "/ready",
    "/docs",
    "/redoc",
    "/openapi.json",
    "/dashboard",
    "/admin/",
)


def classify_path(path: str) -> Optional[str]:
    """Return the rate-limit group for ``path``, or ``None`` if the
    path bypasses rate limiting entirely."""
    for prefix in _BYPASS_PREFIXES:
        if path == prefix.rstrip("/") or path.startswith(prefix):
            return None
    for prefix, group in _GROUP_RULES:
        if path.startswith(prefix):
            return group
    return "application"


async def check_rate_limit(
    api_key: str,
    group: str,
    redis_store,
) -> dict:
    """Atomic INCR + EXPIRE. Returns a dict with the limit metadata so
    the caller can build response headers + the 429 body.

    Schema:
      {
        "allowed":    bool,         # False ⇒ caller should 429
        "limit":      int,          # tier max
        "count":      int,          # post-INCR counter
        "remaining":  int,          # max(limit - count, 0)
        "reset_in":   int,          # seconds until the bucket resets
        "reset_at":   int,          # unix timestamp at which it resets
        "window":     int,          # tier window seconds (for diagnostics)
      }

    Fail-open: any Redis exception returns ``allowed=True`` with empty
    counters so the caller treats it as a no-op rather than rejecting
    legitimate traffic during a Redis outage.
    """
    limit, window = RATE_LIMITS[group]
    key = f"rate:{api_key}:{group}"
    now = int(time.time())
    try:
        count = int(await redis_store._r.incr(key))
        # Set expiry only on the first INCR — re-setting on every call
        # would slide the window forever and the bucket would never
        # reset. Concurrent first-INCRs racing here is fine: SET EX is
        # idempotent on the second call.
        if count == 1:
            await redis_store._r.expire(key, window)
            ttl = window
        else:
            ttl = int(await redis_store._r.ttl(key))
            if ttl < 0:
                # Key existed but had no expiry (edge case from manual
                # SET) — re-arm so we don't accumulate forever.
                await redis_store._r.expire(key, window)
                ttl = window
    except Exception as exc:
        logger.warning(
            "rate_limit_check_failed_fail_open",
            extra={"key": key, "error": str(exc)},
        )
        return {
            "allowed":   True,
            "limit":     limit,
            "count":     0,
            "remaining": limit,
            "reset_in":  window,
            "reset_at":  now + window,
            "window":    window,
        }

    return {
        "allowed":   count <= limit,
        "limit":     limit,
        "count":     count,
        "remaining": max(limit - count, 0),
        "reset_in":  ttl,
        "reset_at":  now + ttl,
        "window":    window,
    }


class RateLimitMiddleware(BaseHTTPMiddleware):
    """ASGI middleware that gates every authenticated request through
    :func:`check_rate_limit`.

    The check fires AFTER path classification but BEFORE the route
    handler runs. Identifier is the raw ``X-API-Key`` header value —
    rate-limited even if the key turns out to be invalid downstream
    (otherwise an attacker can spam random keys to harvest validation
    timing). For requests with no key, the bypass paths still let
    /health / /docs through; everything else falls through to the
    auth dependency which 401s without consuming quota.
    """

    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        group = classify_path(path)
        if group is None:
            return await call_next(request)

        api_key = request.headers.get("X-API-Key")
        if not api_key:
            # Defer to auth — it will 401. Don't consume rate budget on
            # a no-key request; that lets a misconfigured caller flap
            # without filling a phantom bucket.
            return await call_next(request)

        redis = getattr(request.app.state, "redis_store", None)
        if redis is None:
            return await call_next(request)

        info = await check_rate_limit(api_key, group, redis)
        if not info["allowed"]:
            # 429 with retry_after + the standard rate-limit headers.
            return JSONResponse(
                status_code=429,
                content={
                    "detail":      "Rate limit exceeded",
                    "retry_after": info["reset_in"],
                    "limit":       info["limit"],
                    "window":      info["window"],
                },
                headers=_rate_headers(info, retry_after=True),
            )

        response: Response = await call_next(request)
        for k, v in _rate_headers(info).items():
            response.headers[k] = v
        return response


def _rate_headers(info: dict, retry_after: bool = False) -> dict:
    """Build the X-RateLimit-* (and optionally Retry-After) headers
    from a check_rate_limit result. Empty dict on the fail-open path
    where ``count`` is 0 — that signals "rate limit not enforced this
    request" so consumers don't see misleading values."""
    if not info or info.get("count", 0) == 0 and info.get("limit", 0) == 0:
        return {}
    out = {
        "X-RateLimit-Limit":     str(info["limit"]),
        "X-RateLimit-Remaining": str(info["remaining"]),
        "X-RateLimit-Reset":     str(info["reset_at"]),
    }
    if retry_after:
        out["Retry-After"] = str(max(info["reset_in"], 1))
    return out
