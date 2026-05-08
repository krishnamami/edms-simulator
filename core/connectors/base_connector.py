"""Abstract EDMS-connector contract.

Concrete connectors (S3, Encompass REST, Encompass DB, etc.) implement
this. The incremental graph builder uses only this interface so the
upstream source of truth can be swapped without touching the builder.
"""
from __future__ import annotations

from typing import Optional


class BaseEDMSConnector:
    """Abstract base. Subclass + override the three methods.

    Convention:
    - ``pull_documents_since`` returns docs with ``received_at > watermark``.
      Optional ``until`` param caps the upper bound — used by the
      backtest harness to simulate a fixed-clock build tick.
    - Watermarks are ISO-8601 UTC strings stored alongside the
      connector's identity ("source" name) so a fresh deploy resumes
      from the last successful pull rather than re-importing the world.
    """

    SOURCE_NAME: str = "abstract"

    async def pull_documents_since(
        self, watermark: str, until: Optional[str] = None,
    ) -> list[dict]:
        raise NotImplementedError

    async def get_watermark(self) -> str:
        raise NotImplementedError

    async def set_watermark(self, timestamp: str) -> None:
        raise NotImplementedError
