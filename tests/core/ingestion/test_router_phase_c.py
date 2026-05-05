"""Router dispatch with all Phase-C adapters wired."""
import base64
from datetime import date

import pytest

from core.documents.generators.w2_generator import generate_w2
from core.ingestion.events import ChannelType
from core.ingestion.router import IngestRouter


@pytest.fixture
def router():
    return IngestRouter()


def test_routes_pdf_to_pdf_adapter(router):
    pdf, _ = generate_w2(
        employee_name="James Okafor",
        employee_ssn_last4="4729",
        employee_address="X",
        employer_name="Accenture LLC",
        employer_ein="123456789",
        employer_address="X",
        tax_year=2024,
        box1_wages=92400,
    )
    event = router.route(pdf, ChannelType.PDF_UPLOAD)
    assert event.source_channel == ChannelType.PDF_UPLOAD
    assert event.document_type == "W2"


def test_routes_xml_irs_transcript(router):
    xml = b"""<?xml version="1.0"?><TaxTranscript><AGI>92400</AGI></TaxTranscript>"""
    event = router.route(xml, ChannelType.XML)
    assert event.document_type == "IRS_4506C"
    assert event.confidence == 0.99


def test_routes_form(router):
    event = router.route(
        {
            "form_type": "CONTACT_FORM",
            "fields": {"first_name": "A", "last_name": "B", "email": "a@b.com"},
        },
        ChannelType.FORM,
    )
    assert event.confidence == 0.90


def test_routes_csv_returns_events_and_report(router):
    csv = (
        b"first_name,last_name,annual_income\n"
        b"James,Okafor,92400\n"
    )
    events, report = router.route(csv, ChannelType.CSV_BATCH)
    assert report["processed"] == 1
    assert events[0].source_channel == ChannelType.CSV_BATCH


def test_routes_email_returns_list(router, monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    events = router.route(
        {
            "from": "x@y.com",
            "subject": "W2 attached",
            "body": "see attached",
            "attachments": [],
        },
        ChannelType.EMAIL,
    )
    assert isinstance(events, list)
    assert events[0].source_channel == ChannelType.EMAIL
