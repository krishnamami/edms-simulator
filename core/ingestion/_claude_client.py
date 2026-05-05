"""Shared Anthropic client.

Adapters that need Claude (chat/image/email) import from here so the SDK
import is lazy and they all use the same model id. When ANTHROPIC_API_KEY
is unset, get_client() returns None — callers either raise or fall back.
"""
from __future__ import annotations

import os
from typing import Any, Optional

CLAUDE_MODEL_ID = "claude-sonnet-4-6"

_client: Optional[Any] = None


def is_available() -> bool:
    return bool(os.getenv("ANTHROPIC_API_KEY"))


def get_client() -> Optional[Any]:
    global _client
    if not is_available():
        return None
    if _client is None:
        from anthropic import Anthropic  # lazy import
        _client = Anthropic()
    return _client


def reset_client_for_tests() -> None:
    """Drop the cached singleton — used by tests that patch the SDK."""
    global _client
    _client = None


class ClaudeUnavailable(RuntimeError):
    """Raised when an adapter requires Claude but no API key is configured."""
