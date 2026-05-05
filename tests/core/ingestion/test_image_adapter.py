"""Image adapter tests — mocked Claude vision client."""
import json

import pytest

from core.ingestion._claude_client import ClaudeUnavailable
from core.ingestion.adapters import image_adapter
from core.ingestion.events import ChannelType
from tests.core.ingestion._fakes import FakeClaudeClient


def test_detects_jpeg_media_type():
    assert image_adapter._media_type(b"\xff\xd8\xff\xe0\x00\x10JFIF...") == "image/jpeg"


def test_detects_png_media_type():
    assert image_adapter._media_type(b"\x89PNG\r\n\x1a\n....") == "image/png"


def test_detects_tiff_media_type():
    assert image_adapter._media_type(b"II*\x00....") == "image/tiff"


def test_unknown_format_raises():
    with pytest.raises(ValueError):
        image_adapter._media_type(b"random bytes")


def test_extracts_dl_fields_from_mocked_vision():
    canned = json.dumps({
        "document_type": "DRIVERS_LICENSE",
        "first_name": "James",
        "last_name": "Okafor",
        "dob": "1982-07-14",
        "dl_number": "D1234567",
        "state": "CA",
        "expiry": "2028-05-01",
        "confidence": 0.93,
    })
    client = FakeClaudeClient(canned)

    # Minimal-but-valid JPG bytes (magic header is enough for media-type detection)
    jpg_bytes = b"\xff\xd8\xff\xe0" + b"\x00" * 20

    event = image_adapter.adapt(jpg_bytes, client=client)
    assert event.source_channel == ChannelType.IMAGE_UPLOAD
    assert event.document_type == "DRIVERS_LICENSE"
    assert event.confidence == 0.93
    assert event.applicant_signals["first_name"] == "James"
    assert event.applicant_signals["last_name"] == "Okafor"
    assert event.applicant_signals["dob"] == "1982-07-14"


def test_raises_when_no_client_and_no_key(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with pytest.raises(ClaudeUnavailable):
        image_adapter.adapt(b"\xff\xd8\xff\xe0...")
