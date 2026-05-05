"""Tests for IngestRouter.detect_channel and route()."""
import pytest

from core.ingestion.events import ChannelType
from core.ingestion.router import IngestRouter


@pytest.fixture
def router():
    return IngestRouter()


def test_detects_pdf_from_header(router):
    assert router.detect_channel(b"%PDF-1.7\n%\xe2\xe3\xcf\xd3 ...") == ChannelType.PDF_UPLOAD


def test_detects_jpeg_image(router):
    assert router.detect_channel(b"\xff\xd8\xff\xe0\x00\x10JFIF...") == ChannelType.IMAGE_UPLOAD


def test_detects_png_image(router):
    assert router.detect_channel(b"\x89PNG\r\n\x1a\n....") == ChannelType.IMAGE_UPLOAD


def test_detects_tiff_image(router):
    assert router.detect_channel(b"II*\x00....") == ChannelType.IMAGE_UPLOAD
    assert router.detect_channel(b"MM\x00*....") == ChannelType.IMAGE_UPLOAD


def test_detects_xml_from_header(router):
    assert router.detect_channel(b'<?xml version="1.0"?><root/>') == ChannelType.XML


def test_unknown_bytes_fall_through_to_csv(router):
    """CSV is the bytes fallback — broker bulk uploads aren't always sniffable."""
    csv = b"first_name,last_name,annual_income\nJames,Okafor,92000\n"
    assert router.detect_channel(csv) == ChannelType.CSV_BATCH


def test_detects_chat_from_message_list(router):
    msgs = [
        {"role": "user", "content": "hi"},
        {"role": "assistant", "content": "hello"},
    ]
    assert router.detect_channel(msgs) == ChannelType.CHAT


def test_detects_email_from_dict_shape(router):
    payload = {
        "from": "borrower@example.com",
        "subject": "W2 attached",
        "body": "see attached",
        "attachments": [],
    }
    assert router.detect_channel(payload) == ChannelType.EMAIL


def test_detects_form_from_form_type_field(router):
    payload = {"form_type": "URLA_1003", "fields": {"first_name": "James"}}
    assert router.detect_channel(payload) == ChannelType.FORM


def test_detects_api_from_los_id(router):
    payload = {"los_id": "LOS-001", "borrower": {}}
    assert router.detect_channel(payload) == ChannelType.API


def test_unknown_dict_raises(router):
    with pytest.raises(ValueError):
        router.detect_channel({"random": "blob"})


def test_unsupported_type_raises(router):
    with pytest.raises(TypeError):
        router.detect_channel(42)


def test_route_api_returns_normalized_event(router):
    payload = {
        "los_id": "LOS-X1",
        "borrower": {"first_name": "James", "last_name": "Okafor", "dob": "1982-07-14"},
        "loan": {"credit_band": "prime"},
        "documents": [],
    }
    event = router.route(payload, ChannelType.API)
    assert event.source_channel == ChannelType.API
    assert event.confidence == 1.0
    assert event.requires_verification is False
    assert event.applicant_signals["first_name"] == "James"
    assert event.applicant_signals["los_id"] == "LOS-X1"


def test_route_chat_without_key_raises_claude_unavailable(router, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    from core.ingestion._claude_client import ClaudeUnavailable
    with pytest.raises(ClaudeUnavailable):
        router.route([{"role": "user", "content": "hi"}], ChannelType.CHAT)
