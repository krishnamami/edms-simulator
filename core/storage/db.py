"""asyncpg connection pool to Aurora Postgres (via RDS Proxy in production)."""
import logging
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
    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=2,
        max_size=20,
        max_inactive_connection_lifetime=300,
        command_timeout=30,
    )
    logger.info("aurora_pool_created", extra={"host": creds["host"]})
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
