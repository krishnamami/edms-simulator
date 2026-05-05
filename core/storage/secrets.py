"""AWS Secrets Manager helper.

Falls back to environment variables when USE_AWS_SECRETS=false (local/dev/test).
"""
import json
import os
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)


class SecretsManager:
    def __init__(self):
        self.use_aws = os.getenv("USE_AWS_SECRETS", "false").lower() == "true"
        self.region = os.getenv("AWS_REGION", "us-east-1")
        self._client = None
        self._cache: dict = {}

    @property
    def client(self):
        if not self._client and self.use_aws:
            import boto3
            self._client = boto3.client("secretsmanager", region_name=self.region)
        return self._client

    def get_secret(self, secret_name: str) -> dict:
        if secret_name in self._cache:
            return self._cache[secret_name]
        if not self.use_aws:
            result = self._from_env(secret_name)
        else:
            try:
                response = self.client.get_secret_value(SecretId=secret_name)
                result = json.loads(response["SecretString"])
                logger.info("secret_loaded", extra={"secret": secret_name})
            except Exception as e:
                logger.error(
                    "secret_load_failed",
                    extra={"secret": secret_name, "error": str(e)},
                )
                result = self._from_env(secret_name)
        self._cache[secret_name] = result
        return result

    def _from_env(self, secret_name: str) -> dict:
        mappings = {
            "edms/aurora/credentials": {
                "host": os.getenv("DB_HOST", "localhost"),
                "port": int(os.getenv("DB_PORT", "5432")),
                "database": os.getenv("DB_NAME", "edms"),
                "username": os.getenv("DB_USER", "edms"),
                "password": os.getenv("DB_PASSWORD", "edms_dev"),
            },
            "edms/redis/endpoint": {
                "host": os.getenv("REDIS_HOST", "localhost"),
                "port": int(os.getenv("REDIS_PORT", "6379")),
                "password": os.getenv("REDIS_PASSWORD", ""),
            },
            "edms/api/keys": {
                "decision_os_api_key": os.getenv("API_KEY", "edms_dev_key"),
            },
        }
        return mappings.get(secret_name, {})


@lru_cache(maxsize=1)
def get_secrets() -> SecretsManager:
    return SecretsManager()
