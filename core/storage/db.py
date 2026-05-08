"""asyncpg connection pool to Aurora/RDS Postgres."""
import logging
import os
from typing import Optional

import asyncpg

from core.storage.secrets import get_secrets

logger = logging.getLogger(__name__)
_pool: Optional[asyncpg.Pool] = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await _create_pool()
    return _pool


async def _create_pool() -> asyncpg.Pool:
    secrets = get_secrets()
    creds = secrets.get_secret("edms/aurora/credentials")
    dsn = (
        f"postgresql://{creds['username']}:{creds['password']}"
        f"@{creds['host']}:{creds['port']}/{creds['database']}"
    )
    # RDS Postgres parameter groups commonly enable rds.force_ssl=1, so
    # connections without TLS are rejected at pg_hba. In production we
    # request encryption; locally we don't (the docker-compose Postgres
    # has no TLS termination).
    use_ssl = os.getenv("USE_AWS_SECRETS", "false").lower() == "true"
    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=2,
        max_size=20,
        max_inactive_connection_lifetime=300,
        command_timeout=30,
        ssl="require" if use_ssl else None,
    )
    logger.info(
        "aurora_pool_created",
        extra={"host": creds["host"], "ssl": "require" if use_ssl else "off"},
    )
    return pool


async def close_pool():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


async def execute(query: str, *args):
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.execute(query, *args)


async def fetch(query: str, *args) -> list:
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch(query, *args)


async def fetchrow(query: str, *args):
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow(query, *args)


async def fetchval(query: str, *args):
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchval(query, *args)


async def stream(query: str, *args, prefetch: int = 500):
    """Async-iterate a query's rows via a server-side cursor.

    asyncpg requires the cursor to live inside a transaction; the
    transaction is committed when the generator exits. Prefetch=500
    keeps round-trip count reasonable while bounding memory at ~one
    page of rows. Used by the bulk-export streaming endpoints so
    multi-thousand-row dumps never load the whole result set into
    Python memory.
    """
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            async for row in conn.cursor(query, *args, prefetch=prefetch):
                yield row
