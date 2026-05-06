"""Apply infra/schema.sql to the production database.

Designed to run as a one-off ECS Fargate task so it has VPC reachability
to the private RDS endpoint. Locally this only works if the RDS is also
reachable (e.g. via SSH tunnel or VPN).

Usage:
    USE_AWS_SECRETS=true python scripts/apply_schema.py

The container image already has ``infra/schema.sql`` baked in (Dockerfile
``COPY . .``), so no S3 fetch is required. Statements are split on the
semicolon delimiter and applied individually so a ``relation already
exists`` from a prior partial apply doesn't abort the whole run.
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

# Allow ``python scripts/apply_schema.py`` from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.storage import db  # noqa: E402


SCHEMA_FILE = Path(__file__).resolve().parents[1] / "infra" / "schema.sql"


def split_statements(sql: str) -> list[str]:
    """Strip ``--`` line comments first (they may legally contain ``;``),
    then split on ``;``. Still naive about PL/pgSQL bodies, but our
    schema doesn't have any."""
    import re

    # Drop line comments. Each `--` runs to end-of-line.
    cleaned = re.sub(r"--[^\n]*", "", sql)
    return [s.strip() for s in cleaned.split(";") if s.strip()]


async def apply() -> int:
    schema = SCHEMA_FILE.read_text()
    statements = split_statements(schema)
    print(f"Loaded {len(statements)} statements from {SCHEMA_FILE}")
    print(f"USE_AWS_SECRETS={os.getenv('USE_AWS_SECRETS')!r}")

    pool = await db.get_pool()
    ok_count = 0
    skip_count = 0
    err_count = 0

    async with pool.acquire() as conn:
        for stmt in statements:
            preview = " ".join(stmt[:80].split())
            try:
                await conn.execute(stmt)
                print(f"OK:   {preview}")
                ok_count += 1
            except Exception as exc:
                msg = str(exc).split("\n")[0]
                # "already exists" is benign — table/index was already there.
                if "already exists" in msg.lower():
                    print(f"SKIP: {preview} -- {msg}")
                    skip_count += 1
                else:
                    print(f"FAIL: {preview} -- {msg}")
                    err_count += 1

    await db.close_pool()
    print(f"\nDone: {ok_count} OK, {skip_count} SKIPPED (idempotent), {err_count} FAILED")
    return 1 if err_count > 0 else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(apply()))
