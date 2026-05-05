"""Minimal fake of the Anthropic client for adapter tests.

The real SDK returns response objects with a `.content` list whose items
have `.type` and `.text`. We only emit text blocks.
"""
from __future__ import annotations


class _FakeTextBlock:
    type = "text"

    def __init__(self, text: str):
        self.text = text


class _FakeResponse:
    def __init__(self, text: str):
        self.content = [_FakeTextBlock(text)]


class FakeClaudeClient:
    """Always returns the same text. Records the args of the last call."""

    def __init__(self, response_text: str):
        self.response_text = response_text
        self.last_call: dict | None = None
        self.messages = self  # client.messages.create(...)

    def create(self, **kwargs):
        self.last_call = kwargs
        return _FakeResponse(self.response_text)
