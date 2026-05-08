"""ElastiCache Redis store with three-mode support.

- USE_FAKE_REDIS=true   -> fakeredis.aioredis (CI/unit tests)
- ENVIRONMENT=local     -> local Redis (docker-compose)
- ENVIRONMENT=production-> ElastiCache via secrets manager

All methods are async. The underlying client is ``redis.asyncio.Redis``
so a setex / get / delete never blocks the event loop — important
because every FastAPI request handler is async and a sync client would
serialize concurrent traffic behind redis round-trips.
"""
import json
import logging
import os
from typing import Optional

from core.storage.secrets import get_secrets

logger = logging.getLogger(__name__)
_client = None


def get_redis():
    global _client
    if _client is None:
        _client = _create_client()
    return _client


def _create_client():
    if os.getenv("USE_FAKE_REDIS", "false").lower() == "true":
        from fakeredis import aioredis as fake_aioredis
        logger.info("redis_using_fakeredis_async")
        return fake_aioredis.FakeRedis(decode_responses=True)

    secrets = get_secrets()
    creds = secrets.get_secret("edms/redis/endpoint")
    from redis import asyncio as redis_asyncio
    # ElastiCache TransitEncryptionEnabled requires ssl=True on the client.
    # Trigger TLS on either an explicit REDIS_SSL=true (test/local override)
    # or ENVIRONMENT=production (default in the ECS task definition).
    use_ssl = (
        os.getenv("REDIS_SSL", "false").lower() == "true"
        or os.getenv("ENVIRONMENT", "local") == "production"
    )
    client = redis_asyncio.Redis(
        host=creds["host"],
        port=creds["port"],
        password=creds.get("password") or None,
        decode_responses=True,
        ssl=use_ssl,
        socket_connect_timeout=5,
        socket_timeout=5,
        retry_on_timeout=True,
    )
    logger.info("redis_connected", extra={"host": creds["host"], "ssl": use_ssl})
    return client


class RedisStore:
    TTL_INCOME_PROFILE = 14400   # 4 hours
    TTL_CREDIT_PROFILE = 14400   # 4 hours
    TTL_GOLDEN_STATUS = 86400    # 24 hours
    TTL_APP_LOOKUP = 43200       # 12 hours
    TTL_GRAPH_SUMMARY = 3600     # 1 hour
    TTL_PROPERTY_PROFILE = 14400  # 4 hours
    TTL_APPLICATION_CONTEXT = 1800  # 30 minutes
    TTL_BORROWER_SNAPSHOT   = 7200  # 2 hours

    def __init__(self, client=None):
        self._r = client or get_redis()

    @staticmethod
    def _k(tenant_id: str, key: str) -> str:
        """Tenant-namespace a cache key. Every key the app reads or writes
        is prefixed with ``{tenant_id}:`` so two tenants can never share
        a cache slot. Existing pre-multi-tenant deployments effectively
        cold-start their cache once after this lands — that's intentional
        and harmless (cold reads fall through to Postgres).
        """
        return f"{tenant_id or 'default'}:{key}"

    async def set_income_profile(
        self, applicant_id: str, profile: dict, ttl: Optional[int] = None,
        tenant_id: str = "default",
    ) -> bool:
        try:
            await self._r.setex(
                self._k(tenant_id, f"income:{applicant_id}"),
                ttl or self.TTL_INCOME_PROFILE,
                json.dumps(profile, default=str),
            )
            return True
        except Exception as e:
            logger.warning("redis_set_income_failed", extra={"error": str(e)})
            return False

    async def get_income_profile(
        self, applicant_id: str, tenant_id: str = "default",
    ) -> Optional[dict]:
        try:
            raw = await self._r.get(self._k(tenant_id, f"income:{applicant_id}"))
            return json.loads(raw) if raw else None
        except Exception as e:
            logger.warning("redis_get_income_failed", extra={"error": str(e)})
            return None

    async def set_credit_profile(
        self, applicant_id: str, profile: dict, ttl: Optional[int] = None,
        tenant_id: str = "default",
    ) -> bool:
        try:
            await self._r.setex(
                self._k(tenant_id, f"credit:{applicant_id}"),
                ttl or self.TTL_CREDIT_PROFILE,
                json.dumps(profile, default=str),
            )
            return True
        except Exception as e:
            logger.warning("redis_set_credit_failed", extra={"error": str(e)})
            return False

    async def get_credit_profile(
        self, applicant_id: str, tenant_id: str = "default",
    ) -> Optional[dict]:
        try:
            raw = await self._r.get(self._k(tenant_id, f"credit:{applicant_id}"))
            return json.loads(raw) if raw else None
        except Exception as e:
            logger.warning("redis_get_credit_failed", extra={"error": str(e)})
            return None

    async def set_status(
        self, applicant_id: str, status: str, tenant_id: str = "default",
    ):
        try:
            await self._r.setex(
                self._k(tenant_id, f"status:{applicant_id}"),
                self.TTL_GOLDEN_STATUS, status,
            )
        except Exception as e:
            logger.warning("redis_set_status_failed", extra={"error": str(e)})

    async def get_status(
        self, applicant_id: str, tenant_id: str = "default",
    ) -> Optional[str]:
        try:
            return await self._r.get(self._k(tenant_id, f"status:{applicant_id}"))
        except Exception:
            return None

    async def set_app_lookup(
        self, los_id: str, data: dict, tenant_id: str = "default",
    ):
        try:
            await self._r.setex(
                self._k(tenant_id, f"app_los:{los_id}"),
                self.TTL_APP_LOOKUP,
                json.dumps(data, default=str),
            )
        except Exception as e:
            logger.warning("redis_set_app_failed", extra={"error": str(e)})

    async def get_app_lookup(
        self, los_id: str, tenant_id: str = "default",
    ) -> Optional[dict]:
        try:
            raw = await self._r.get(self._k(tenant_id, f"app_los:{los_id}"))
            return json.loads(raw) if raw else None
        except Exception:
            return None

    async def ping(self) -> bool:
        try:
            return bool(await self._r.ping())
        except Exception:
            return False

    async def key_state(
        self, key: str, tenant_id: str = "default",
    ) -> dict:
        """Return ``{"present": bool, "ttl_seconds": int|None}`` for the
        tenant-prefixed key. Pass the un-prefixed logical key (e.g.
        ``income:APL-001``); we add ``{tenant_id}:`` here so callers
        don't have to know the namespacing scheme.
        """
        full_key = self._k(tenant_id, key)
        try:
            ttl = int(await self._r.ttl(full_key))
        except Exception as e:
            logger.warning("redis_ttl_failed", extra={"error": str(e)})
            return {"present": False, "ttl_seconds": None}
        if ttl == -2:
            return {"present": False, "ttl_seconds": None}
        return {"present": True, "ttl_seconds": ttl if ttl >= 0 else None}

    # ---------------- per-applicant assembly lock -----------------
    #
    # Serialize concurrent _run_assembly invocations for one applicant so
    # two near-simultaneous /documents/upload calls don't each compute
    # income from a partial doc set and race the last set_income_profile.
    # SET NX EX acquire + plain DEL release. The 30s TTL is a crash-
    # safety net: if the holder dies mid-assembly the lock auto-expires
    # so the next request isn't permanently blocked.

    _LOCK_TTL_SECONDS = 30

    async def try_acquire_assembly_lock(
        self, applicant_id: str, ttl: Optional[int] = None,
        tenant_id: str = "default",
    ) -> bool:
        key = self._k(tenant_id, f"assembly_lock:{applicant_id}")
        try:
            return bool(
                await self._r.set(
                    key, "1", nx=True, ex=ttl or self._LOCK_TTL_SECONDS
                )
            )
        except Exception as e:
            logger.warning("redis_lock_acquire_failed", extra={"error": str(e)})
            return False

    async def release_assembly_lock(
        self, applicant_id: str, tenant_id: str = "default",
    ) -> None:
        key = self._k(tenant_id, f"assembly_lock:{applicant_id}")
        try:
            await self._r.delete(key)
        except Exception as e:
            logger.warning("redis_lock_release_failed", extra={"error": str(e)})

    # ---------------- document knowledge graph -----------------

    async def invalidate_income_profile(
        self, applicant_id: str, tenant_id: str = "default",
    ) -> None:
        """Drop income + graph caches for an applicant — call after a
        reconciler conflict so the next read recomputes."""
        try:
            await self._r.delete(self._k(tenant_id, f"income:{applicant_id}"))
            await self._r.delete(self._k(tenant_id, f"graph:{applicant_id}"))
        except Exception as e:
            logger.warning("redis_invalidate_failed", extra={"error": str(e)})

    async def invalidate_graph_summary(
        self, applicant_id: str, tenant_id: str = "default",
    ) -> None:
        try:
            await self._r.delete(self._k(tenant_id, f"graph:{applicant_id}"))
        except Exception as e:
            logger.warning("redis_invalidate_graph_failed", extra={"error": str(e)})

    async def set_graph_summary(
        self, applicant_id: str, summary: dict, tenant_id: str = "default",
    ) -> bool:
        try:
            await self._r.setex(
                self._k(tenant_id, f"graph:{applicant_id}"),
                self.TTL_GRAPH_SUMMARY,
                json.dumps(summary, default=str),
            )
            return True
        except Exception as e:
            logger.warning("redis_set_graph_failed", extra={"error": str(e)})
            return False

    async def get_graph_summary(
        self, applicant_id: str, tenant_id: str = "default",
    ) -> Optional[dict]:
        try:
            raw = await self._r.get(self._k(tenant_id, f"graph:{applicant_id}"))
            return json.loads(raw) if raw else None
        except Exception:
            return None

    # ---------------- property profiles (Phase B) -----------------

    async def set_property_profile(
        self, property_id: str, profile: dict, ttl: Optional[int] = None,
        tenant_id: str = "default",
    ) -> bool:
        try:
            await self._r.setex(
                self._k(tenant_id, f"property:{property_id}"),
                ttl or self.TTL_PROPERTY_PROFILE,
                json.dumps(profile, default=str),
            )
            return True
        except Exception as e:
            logger.warning("redis_set_property_failed", extra={"error": str(e)})
            return False

    async def get_property_profile(
        self, property_id: str, tenant_id: str = "default",
    ) -> Optional[dict]:
        try:
            raw = await self._r.get(self._k(tenant_id, f"property:{property_id}"))
            return json.loads(raw) if raw else None
        except Exception as e:
            logger.warning("redis_get_property_failed", extra={"error": str(e)})
            return None

    async def invalidate_property_profile(
        self, property_id: str, tenant_id: str = "default",
    ) -> None:
        try:
            await self._r.delete(self._k(tenant_id, f"property:{property_id}"))
        except Exception as e:
            logger.warning(
                "redis_invalidate_property_failed", extra={"error": str(e)}
            )

    async def invalidate_application_context(
        self, application_id: str, tenant_id: str = "default",
    ) -> None:
        """Backwards-compatible alias for :meth:`invalidate_context`."""
        await self.invalidate_context(application_id, tenant_id=tenant_id)

    # ---------------- application context (Phase C) -----------------

    async def set_application_context(
        self, application_id: str, ctx: dict, ttl: Optional[int] = None,
        tenant_id: str = "default",
    ) -> bool:
        try:
            await self._r.setex(
                self._k(tenant_id, f"context:{application_id}"),
                ttl or self.TTL_APPLICATION_CONTEXT,
                json.dumps(ctx, default=str),
            )
            return True
        except Exception as e:
            logger.warning("redis_set_context_failed", extra={"error": str(e)})
            return False

    async def get_application_context(
        self, application_id: str, tenant_id: str = "default",
    ) -> Optional[dict]:
        try:
            raw = await self._r.get(self._k(tenant_id, f"context:{application_id}"))
            return json.loads(raw) if raw else None
        except Exception as e:
            logger.warning("redis_get_context_failed", extra={"error": str(e)})
            return None

    async def invalidate_context(
        self, application_id: str, tenant_id: str = "default",
    ) -> None:
        """Drop the cached application context so the next read recomputes."""
        try:
            await self._r.delete(self._k(tenant_id, f"context:{application_id}"))
        except Exception as e:
            logger.warning(
                "redis_invalidate_context_failed", extra={"error": str(e)}
            )

    # ---------------- asset summary (per-applicant) -----------------
    #
    # Aggregated view of every asset doc the borrower has supplied: liquid
    # bank balances, brokerage, retirement, gift funds. Recomputed on
    # write-through whenever an asset-category doc lands; readers (slices,
    # context, /readiness) get a single-key fetch instead of scanning
    # document_index. Same TTL as income (4h) — these are computed from
    # the same source-of-truth and should refresh on the same cadence.

    async def set_asset_summary(
        self, applicant_id: str, summary: dict, ttl: Optional[int] = None,
        tenant_id: str = "default",
    ) -> bool:
        try:
            await self._r.setex(
                self._k(tenant_id, f"asset:{applicant_id}"),
                ttl or self.TTL_INCOME_PROFILE,
                json.dumps(summary, default=str),
            )
            return True
        except Exception as e:
            logger.warning("redis_set_asset_failed", extra={"error": str(e)})
            return False

    async def get_asset_summary(
        self, applicant_id: str, tenant_id: str = "default",
    ) -> Optional[dict]:
        try:
            raw = await self._r.get(self._k(tenant_id, f"asset:{applicant_id}"))
            return json.loads(raw) if raw else None
        except Exception as e:
            logger.warning("redis_get_asset_failed", extra={"error": str(e)})
            return None

    async def invalidate_asset_summary(
        self, applicant_id: str, tenant_id: str = "default",
    ) -> None:
        try:
            await self._r.delete(self._k(tenant_id, f"asset:{applicant_id}"))
        except Exception as e:
            logger.warning("redis_invalidate_asset_failed", extra={"error": str(e)})

    # ---------------- identity summary (per-applicant) -----------------
    #
    # Aggregated view of every identity / KYC artifact the borrower has on
    # file: driver's license, SSN validation, OFAC clearance. Same TTL as
    # the golden-record status (24h) — identity rarely changes within a
    # session, and a stale read is harmless because the source-of-truth
    # rows in document_index are the gating data; this cache only powers
    # fast reads on the readiness / compliance slices.

    async def set_identity_summary(
        self, applicant_id: str, summary: dict, ttl: Optional[int] = None,
        tenant_id: str = "default",
    ) -> bool:
        try:
            await self._r.setex(
                self._k(tenant_id, f"identity:{applicant_id}"),
                ttl or self.TTL_GOLDEN_STATUS,
                json.dumps(summary, default=str),
            )
            return True
        except Exception as e:
            logger.warning("redis_set_identity_failed", extra={"error": str(e)})
            return False

    async def get_identity_summary(
        self, applicant_id: str, tenant_id: str = "default",
    ) -> Optional[dict]:
        try:
            raw = await self._r.get(self._k(tenant_id, f"identity:{applicant_id}"))
            return json.loads(raw) if raw else None
        except Exception as e:
            logger.warning("redis_get_identity_failed", extra={"error": str(e)})
            return None

    async def invalidate_identity_summary(
        self, applicant_id: str, tenant_id: str = "default",
    ) -> None:
        try:
            await self._r.delete(self._k(tenant_id, f"identity:{applicant_id}"))
        except Exception as e:
            logger.warning(
                "redis_invalidate_identity_failed", extra={"error": str(e)}
            )

    # ---------------- entity_states write-through -----------------
    #
    # The /documents/upload path now upserts entity_states for every
    # affected entity (borrower / co_borrower / property / loan_terms)
    # at the end of AggregationService._run_assembly. The state JSONB
    # is also mirrored into Redis under ``entity:{entity_id}`` so
    # /entity/{id}/state can serve from cache without a PG hit. The
    # 1-hour TTL matches typical decision-engine refresh cadence —
    # short enough that stale reads heal fast, long enough to absorb
    # a burst of assembly fan-out.

    TTL_ENTITY_STATE = 3600  # 1 hour

    async def set_entity_state(
        self, entity_id: str, state_json: str,
        ttl: Optional[int] = None,
        tenant_id: str = "default",
    ) -> bool:
        """Cache an entity_states.state payload as a JSON string. The
        caller passes the already-serialized string so we don't double-
        encode (and any non-JSON-safe values it contains are already
        the caller's problem)."""
        try:
            await self._r.setex(
                self._k(tenant_id, f"entity:{entity_id}"),
                ttl or self.TTL_ENTITY_STATE,
                state_json,
            )
            return True
        except Exception as e:
            logger.warning("redis_set_entity_state_failed",
                           extra={"error": str(e)})
            return False

    async def get_entity_state(
        self, entity_id: str, tenant_id: str = "default",
    ) -> Optional[str]:
        try:
            return await self._r.get(self._k(tenant_id, f"entity:{entity_id}"))
        except Exception:
            return None

    async def invalidate_entity_state(
        self, entity_id: str, tenant_id: str = "default",
    ) -> None:
        try:
            await self._r.delete(self._k(tenant_id, f"entity:{entity_id}"))
        except Exception as e:
            logger.warning("redis_invalidate_entity_state_failed",
                           extra={"error": str(e)})
