"""Per-request tenant_id resolution via contextvars.

The ``X-API-Key`` middleware (``verify_api_key`` in api/routes.py) sets
``CURRENT_TENANT_ID`` on every authenticated request. Service-layer and
store-layer code that needs the tenant for an SQL filter or a Redis
key prefix reads it via :func:`current_tenant_id` instead of having
the parameter threaded down 4-5 stack frames.

contextvars are per-asyncio-task — every FastAPI request runs as its
own Task, so concurrent requests don't share state. Tests that don't
go through the auth path get the default ``"default"`` tenant, which
matches every domain table's column default.
"""
from __future__ import annotations

from contextvars import ContextVar

DEFAULT_TENANT_ID = "default"

CURRENT_TENANT_ID: ContextVar[str] = ContextVar(
    "current_tenant_id", default=DEFAULT_TENANT_ID
)


def set_tenant_id(tenant_id: str) -> None:
    """Set the current task's tenant_id. Called from auth at request entry."""
    CURRENT_TENANT_ID.set(tenant_id or DEFAULT_TENANT_ID)


def current_tenant_id() -> str:
    """Return the current task's tenant_id, defaulting to 'default' when
    no auth has run (unit tests, internal background jobs)."""
    return CURRENT_TENANT_ID.get() or DEFAULT_TENANT_ID
