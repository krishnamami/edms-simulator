"""Email adapter tests — body extraction + attachments."""
import base64
import json
from datetime import date

from core.documents.generators.paystub_generator import generate_paystub
from core.documents.generators.w2_generator import generate_w2
from core.ingestion.adapters import email_adapter
from core.ingestion.events import ChannelType
from tests.core.ingestion._fakes import FakeClaudeClient


def test_subject_hint_detection():
    assert email_adapter._hint_from_subject("W2 attached") == "W2"
    assert email_adapter._hint_from_subject("Please find pay stub") == "PAYSTUB"
    assert email_adapter._hint_from_subject("Bank Statement for review") == "BANK_STATEMENT"
    assert email_adapter._hint_from_subject("random") is None


def test_body_event_without_claude_falls_back_to_low_confidence(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    events = email_adapter.adapt({
        "from": "borrower@example.com",
        "subject": "W2 attached",
        "body": "Hi, attaching my W2 for the application.",
        "attachments": [],
    })
    assert len(events) == 1
    body = events[0]
    assert body.source_channel == ChannelType.EMAIL
    assert body.document_type == "W2"
    assert body.confidence < 0.50
    assert body.requires_verification is True


def test_processes_pdf_attachment_through_pdf_adapter(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    pdf_bytes, _ = generate_w2(
        employee_name="James Okafor",
        employee_ssn_last4="4729",
        employee_address="100 Main",
        employer_name="Accenture LLC",
        employer_ein="123456789",
        employer_address="1 Corp",
        tax_year=2024,
        box1_wages=92400.00,
    )
    events = email_adapter.adapt({
        "from": "borrower@example.com",
        "subject": "W2 attached",
        "body": "see attached",
        "attachments": [
            {"filename": "w2.pdf", "content_base64": base64.b64encode(pdf_bytes).decode()},
        ],
    })
    assert len(events) == 2  # body + 1 attachment
    pdf_event = next(e for e in events if e.source_channel == ChannelType.PDF_UPLOAD)
    assert pdf_event.document_type == "W2"
    assert pdf_event.extracted_fields.get("box1_wages") == 92400.00


def test_multiple_attachments_each_get_an_event(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    w2_bytes, _ = generate_w2(
        employee_name="A B", employee_ssn_last4="1", employee_address="X",
        employer_name="X", employer_ein="999999999", employer_address="X",
        tax_year=2023, box1_wages=50_000,
    )
    pay_bytes, _ = generate_paystub(
        employer_name="X", employee_name="A B", employee_ssn_last4="1",
        pay_period_start=date(2024, 1, 1), pay_period_end=date(2024, 1, 14),
        pay_date=date(2024, 1, 17), gross_pay=1923.08, ytd_gross=1923.08,
    )
    events = email_adapter.adapt({
        "from": "x@y.com", "subject": "docs", "body": "",
        "attachments": [
            {"filename": "w2.pdf",      "content_base64": base64.b64encode(w2_bytes).decode()},
            {"filename": "paystub.pdf", "content_base64": base64.b64encode(pay_bytes).decode()},
        ],
    })
    pdf_events = [e for e in events if e.source_channel == ChannelType.PDF_UPLOAD]
    assert len(pdf_events) == 2
    types = {e.document_type for e in pdf_events}
    assert types == {"W2", "PAYSTUB"}


def test_body_extraction_with_mocked_claude():
    client = FakeClaudeClient(json.dumps({
        "annual_income": 92000,
        "employer": "Accenture",
        "employment_type": "W2_SALARIED",
    }))
    events = email_adapter.adapt(
        {
            "from": "x@y.com",
            "subject": "Income summary",
            "body": "I make $92,000 at Accenture.",
            "attachments": [],
        },
        client=client,
    )
    body = events[0]
    assert body.confidence >= 0.70
    assert body.extracted_fields["annual_income"] == 92000
    assert body.extracted_fields["employer"] == "Accenture"
