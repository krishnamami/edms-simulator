"""Schema auto-migration applied at API startup.

Reads ``infra/schema.sql`` and executes each statement against the
connected Postgres pool. Every CREATE / ALTER / INSERT in that file is
written ``IF NOT EXISTS`` (or ``ON CONFLICT DO NOTHING`` for seeds), so
a re-run on an already-migrated DB is a no-op — no destructive paths.

Why on every startup:
- The container image bakes ``infra/schema.sql`` in via ``COPY . .``.
  Whenever GitHub Actions deploys a new image to ECS, the new task
  running here picks up any added DDL the moment it boots — no
  separate "apply_schema one-off task" step.
- ``apply_schema.py`` still exists for the rare case where an
  out-of-band migration is needed without restarting the service.

Failure mode: an individual statement's error is logged but never
blocks startup. ``already exists`` is silently bucketed as ``skipped``;
anything else is bucketed as ``error`` and surfaced in the structured
log so an operator can dig in. The API still comes up so we don't
black-hole a deploy on a single bad ALTER.
"""
from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Optional

from core.storage import db

logger = logging.getLogger(__name__)


SCHEMA_FILE = Path(__file__).resolve().parents[2] / "infra" / "schema.sql"


def split_statements(sql: str) -> list[str]:
    """Strip ``--`` line comments first (they may legally contain ``;``),
    then split on ``;``. Naive about dollar-quoted PL/pgSQL bodies — our
    schema has none, so this is fine."""
    cleaned = re.sub(r"--[^\n]*", "", sql)
    return [s.strip() for s in cleaned.split(";") if s.strip()]


async def apply_schema(schema_path: Optional[Path] = None) -> dict:
    """Apply every DDL/INSERT statement in ``schema_path`` (defaults to
    the repo's ``infra/schema.sql``) to the connected pool.

    Returns a dict ``{ok, skipped, errors, total}`` so the caller can
    log a one-line summary. Never raises — partial application is
    acceptable since every statement is idempotent.
    """
    path = Path(schema_path) if schema_path else SCHEMA_FILE
    if not path.exists():
        logger.warning(
            "schema_migration_skipped_missing_file", extra={"path": str(path)}
        )
        return {"ok": 0, "skipped": 0, "errors": 0, "total": 0}

    statements = split_statements(path.read_text(encoding="utf-8"))
    if not statements:
        return {"ok": 0, "skipped": 0, "errors": 0, "total": 0}

    pool = await db.get_pool()
    ok = skipped = errors = 0
    first_error: Optional[str] = None

    async with pool.acquire() as conn:
        for stmt in statements:
            try:
                await conn.execute(stmt)
                ok += 1
            except Exception as exc:
                msg = str(exc).split("\n")[0]
                if "already exists" in msg.lower():
                    skipped += 1
                else:
                    errors += 1
                    if first_error is None:
                        first_error = f"{stmt[:80]} -- {msg}"

    summary = {
        "ok":      ok,
        "skipped": skipped,
        "errors":  errors,
        "total":   len(statements),
    }
    if errors:
        logger.warning(
            "schema_migration_completed_with_errors",
            extra={**summary, "first_error": first_error},
        )
    else:
        logger.info("schema_migration_applied", extra=summary)
    return summary
