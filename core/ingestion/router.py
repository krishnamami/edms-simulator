"""IngestRouter — sniffs incoming payloads and dispatches to channel adapters.

Detection is deliberately content-based, not metadata-based: we may receive
bytes from a multipart upload, a JSON dict from a webhook, or a list of
chat messages from a websocket. We don't trust caller-supplied content-type.
"""
from typing import Any

from core.ingestion.adapters import api_adapter
from core.ingestion.events import ChannelType, NormalizedIngestEvent


class IngestRouter:
    def detect_channel(self, payload: Any) -> ChannelType:
        if isinstance(payload, (bytes, bytearray)):
            head = bytes(payload[:8])
            if head.startswith(b"%PDF"):
                return ChannelType.PDF_UPLOAD
            if head.startswith(b"\xff\xd8\xff"):  # JPEG
                return ChannelType.IMAGE_UPLOAD
            if head.startswith(b"\x89PNG\r\n\x1a\n"):  # PNG
                return ChannelType.IMAGE_UPLOAD
            if head.startswith(b"II*\x00") or head.startswith(b"MM\x00*"):  # TIFF LE/BE
                return ChannelType.IMAGE_UPLOAD
            if head.lstrip().startswith(b"<?xml"):
                return ChannelType.XML
            return ChannelType.CSV_BATCH

        if isinstance(payload, list):
            if payload and all(
                isinstance(m, dict) and "role" in m and "content" in m
                for m in payload
            ):
                return ChannelType.CHAT
            raise ValueError("Unrecognized list payload (expected chat messages)")

        if isinstance(payload, dict):
            if "form_type" in payload and "fields" in payload:
                return ChannelType.FORM
            if "from" in payload and "subject" in payload and "body" in payload:
                return ChannelType.EMAIL
            if "los_id" in payload:
                return ChannelType.API
            raise ValueError("Unrecognized dict payload")

        raise TypeError(f"Unsupported payload type: {type(payload).__name__}")

    def route(
        self, payload: Any, channel_type: ChannelType
    ) -> NormalizedIngestEvent:
        if channel_type == ChannelType.API:
            return api_adapter.adapt(payload)
        raise NotImplementedError(
            f"Adapter for {channel_type.value} not implemented yet"
        )
