"""ElastiCache Redis store with three-mode support.

- USE_FAKE_REDIS=true   -> fakeredis (CI/unit tests)
- ENVIRONMENT=local     -> local Redis (docker-compose)
- ENVIRONMENT=production-> ElastiCache via secrets manager
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
        import fakeredis
        logger.info("redis_using_fakeredis")
        return fakeredis.FakeRedis(decode_responses=True)

    secrets = get_secrets()
    creds = secrets.get_secret("edms/redis/endpoint")
    import redis
    # ElastiCache TransitEncryptionEnabled requires ssl=True on the client.
    # Trigger TLS on either an explicit REDIS_SSL=true (test/local override)
    # or ENVIRONMENT=production (default in the ECS task definition).
    use_ssl = (
        os.getenv("REDIS_SSL", "false").lower() == "true"
        or os.getenv("ENVIRONMENT", "local") == "production"
    )
    client = redis.Redis(
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

    def set_income_profile(
        self, applicant_id: str, profile: dict, ttl: Optional[int] = None
    ) -> bool:
        try:
            self._r.setex(
                f"income:{applicant_id}",
                ttl or self.TTL_INCOME_PROFILE,
                json.dumps(profile, default=str),
            )
            return True
        except Exception as e:
            logger.warning("redis_set_income_failed", extra={"error": str(e)})
            return False

    def get_income_profile(self, applicant_id: str) -> Optional[dict]:
        try:
            raw = self._r.get(f"income:{applicant_id}")
            return json.loads(raw) if raw else None
        except Exception as e:
            logger.warning("redis_get_income_failed", extra={"error": str(e)})
            return None

    def set_credit_profile(
        self, applicant_id: str, profile: dict, ttl: Optional[int] = None
    ) -> bool:
        try:
            self._r.setex(
                f"credit:{applicant_id}",
                ttl or self.TTL_CREDIT_PROFILE,
                json.dumps(profile, default=str),
            )
            return True
        except Exception as e:
            logger.warning("redis_set_credit_failed", extra={"error": str(e)})
            return False

    def get_credit_profile(self, applicant_id: str) -> Optional[dict]:
        try:
            raw = self._r.get(f"credit:{applicant_id}")
            return json.loads(raw) if raw else None
        except Exception as e:
            logger.warning("redis_get_credit_failed", extra={"error": str(e)})
            return None

    def set_status(self, applicant_id: str, status: str):
        try:
            self._r.setex(
                f"status:{applicant_id}", self.TTL_GOLDEN_STATUS, status
            )
        except Exception as e:
            logger.warning("redis_set_status_failed", extra={"error": str(e)})

    def get_status(self, applicant_id: str) -> Optional[str]:
        try:
            return self._r.get(f"status:{applicant_id}")
        except Exception:
            return None

    def set_app_lookup(self, los_id: str, data: dict):
        try:
            self._r.setex(
                f"app_los:{los_id}",
                self.TTL_APP_LOOKUP,
                json.dumps(data, default=str),
            )
        except Exception as e:
            logger.warning("redis_set_app_failed", extra={"error": str(e)})

    def get_app_lookup(self, los_id: str) -> Optional[dict]:
        try:
            raw = self._r.get(f"app_los:{los_id}")
            return json.loads(raw) if raw else None
        except Exception:
            return None

    def ping(self) -> bool:
        try:
            return bool(self._r.ping())
        except Exception:
            return False

    # ---------------- document knowledge graph -----------------

    def invalidate_income_profile(self, applicant_id: str) -> None:
        """Drop income + graph caches for an applicant — call after a
        reconciler conflict so the next read recomputes."""
        try:
            self._r.delete(f"income:{applicant_id}")
            self._r.delete(f"graph:{applicant_id}")
        except Exception as e:
            logger.warning("redis_invalidate_failed", extra={"error": str(e)})

    def set_graph_summary(self, applicant_id: str, summary: dict) -> bool:
        try:
            self._r.setex(
                f"graph:{applicant_id}",
                self.TTL_GRAPH_SUMMARY,
                json.dumps(summary, default=str),
            )
            return True
        except Exception as e:
            logger.warning("redis_set_graph_failed", extra={"error": str(e)})
            return False

    def get_graph_summary(self, applicant_id: str) -> Optional[dict]:
        try:
            raw = self._r.get(f"graph:{applicant_id}")
            return json.loads(raw) if raw else None
        except Exception:
            return None

    # ---------------- property profiles (Phase B) -----------------

    def set_property_profile(
        self, property_id: str, profile: dict, ttl: Optional[int] = None
    ) -> bool:
        try:
            self._r.setex(
                f"property:{property_id}",
                ttl or self.TTL_PROPERTY_PROFILE,
                json.dumps(profile, default=str),
            )
            return True
        except Exception as e:
            logger.warning("redis_set_property_failed", extra={"error": str(e)})
            return False

    def get_property_profile(self, property_id: str) -> Optional[dict]:
        try:
            raw = self._r.get(f"property:{property_id}")
            return json.loads(raw) if raw else None
        except Exception as e:
            logger.warning("redis_get_property_failed", extra={"error": str(e)})
            return None

    def invalidate_property_profile(self, property_id: str) -> None:
        try:
            self._r.delete(f"property:{property_id}")
        except Exception as e:
            logger.warning(
                "redis_invalidate_property_failed", extra={"error": str(e)}
            )

    def invalidate_application_context(self, application_id: str) -> None:
        """Backwards-compatible alias for :meth:`invalidate_context`."""
        self.invalidate_context(application_id)

    # ---------------- application context (Phase C) -----------------

    def set_application_context(
        self, application_id: str, ctx: dict, ttl: Optional[int] = None
    ) -> bool:
        try:
            self._r.setex(
                f"context:{application_id}",
                ttl or self.TTL_APPLICATION_CONTEXT,
                json.dumps(ctx, default=str),
            )
            return True
        except Exception as e:
            logger.warning("redis_set_context_failed", extra={"error": str(e)})
            return False

    def get_application_context(self, application_id: str) -> Optional[dict]:
        try:
            raw = self._r.get(f"context:{application_id}")
            return json.loads(raw) if raw else None
        except Exception as e:
            logger.warning("redis_get_context_failed", extra={"error": str(e)})
            return None

    def invalidate_context(self, application_id: str) -> None:
        """Drop the cached application context so the next read recomputes."""
        try:
            self._r.delete(f"context:{application_id}")
        except Exception as e:
            logger.warning(
                "redis_invalidate_context_failed", extra={"error": str(e)}
            )
