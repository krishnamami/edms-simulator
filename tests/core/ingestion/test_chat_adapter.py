"""Chat adapter — mocked client tests + key-gated live test."""
import json
import os

import pytest

from core.ingestion._claude_client import ClaudeUnavailable
from core.ingestion.adapters import chat_adapter
from core.ingestion.events import ChannelType
from tests.core.ingestion._fakes import FakeClaudeClient


def _claude_response(**overrides) -> str:
    base = {
        "primary_borrower": {
            "first_name": "James",
            "last_name": "Okafor",
            "email": "james.okafor@email.com",
            "phone": None,
            "dob": None,
            "employer": "Accenture",
            "employment_type": "W2_SALARIED",
            "annual_income_stated": 92000,
            "income_sources": [
                {"type": "W2_SALARIED", "amount": 92000, "frequency": "annual", "confidence": 0.85},
                {"type": "RENTAL", "amount": 1800, "frequency": "monthly", "confidence": 0.72},
            ],
        },
        "co_borrower": {
            "first_name": "Sarah",
            "last_name": "Okafor",
            "employer": "Dell",
            "annual_income_stated": 56000,
            "confidence": 0.80,
        },
        "assets_mentioned": [],
        "liabilities_mentioned": [],
        "property_info": {"address": None, "purchase_price": None, "loan_amount": 385000},
        "missing_fields": ["dob", "ssn_last4", "phone"],
        "documents_needed": ["W2", "PAYSTUB", "DRIVERS_LICENSE"],
        "overall_confidence": 0.83,
    }
    base.update(overrides)
    return json.dumps(base)


def test_extracts_w2_salary_and_employer():
    client = FakeClaudeClient(_claude_response())
    event = chat_adapter.adapt(
        [
            {"role": "user", "content": "I make $92,000 a year at Accenture"},
        ],
        client=client,
    )
    assert event.source_channel == ChannelType.CHAT
    assert event.requires_verification is True
    pb = event.extracted_fields["primary_borrower"]
    assert pb["employer"] == "Accenture"
    assert pb["annual_income_stated"] == 92000
    assert pb["employment_type"] == "W2_SALARIED"


def test_extracts_rental_income():
    client = FakeClaudeClient(_claude_response())
    event = chat_adapter.adapt(
        [{"role": "user", "content": "rental brings in about $1,800 a month"}],
        client=client,
    )
    sources = event.extracted_fields["primary_borrower"]["income_sources"]
    rental = next(s for s in sources if s["type"] == "RENTAL")
    assert rental["amount"] == 1800
    assert rental["frequency"] == "monthly"
    assert rental["confidence"] < 0.80, "vague amount should carry lower confidence"


def test_extracts_co_borrower():
    client = FakeClaudeClient(_claude_response())
    event = chat_adapter.adapt(
        [{"role": "user", "content": "my wife Sarah makes $56k at Dell"}],
        client=client,
    )
    cb = event.extracted_fields["co_borrower"]
    assert cb["first_name"] == "Sarah"
    assert cb["employer"] == "Dell"
    assert cb["annual_income_stated"] == 56000


def test_marks_all_chat_fields_requires_verification():
    client = FakeClaudeClient(_claude_response())
    event = chat_adapter.adapt(
        [{"role": "user", "content": "I make $92k"}],
        client=client,
    )
    assert event.requires_verification is True


def test_returns_missing_fields_and_documents_needed():
    client = FakeClaudeClient(_claude_response())
    event = chat_adapter.adapt(
        [{"role": "user", "content": "I want a mortgage"}],
        client=client,
    )
    assert "dob" in event.missing_fields
    assert "W2" in event.documents_needed


def test_handles_multiple_income_sources():
    client = FakeClaudeClient(_claude_response())
    event = chat_adapter.adapt([{"role": "user", "content": "..."}], client=client)
    types = {s["type"] for s in event.extracted_fields["primary_borrower"]["income_sources"]}
    assert {"W2_SALARIED", "RENTAL"}.issubset(types)


def test_strips_markdown_fences_in_response():
    fenced = "```json\n" + _claude_response() + "\n```"
    client = FakeClaudeClient(fenced)
    event = chat_adapter.adapt([{"role": "user", "content": "..."}], client=client)
    assert event.extracted_fields["primary_borrower"]["employer"] == "Accenture"


def test_passes_applicant_id_through_signals():
    client = FakeClaudeClient(_claude_response())
    event = chat_adapter.adapt(
        [{"role": "user", "content": "..."}],
        applicant_id="APL-00001-P",
        client=client,
    )
    assert event.applicant_signals.get("applicant_id") == "APL-00001-P"


def test_raises_when_no_client_and_no_key(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    with pytest.raises(ClaudeUnavailable):
        chat_adapter.adapt([{"role": "user", "content": "hi"}])


@pytest.mark.skipif(
    not os.getenv("ANTHROPIC_API_KEY"),
    reason="ANTHROPIC_API_KEY not set; live API test skipped",
)
def test_live_chat_extraction_returns_valid_event():
    event = chat_adapter.adapt(
        [
            {"role": "user", "content": "I make $92,000 a year at Accenture"},
            {"role": "user", "content": "my wife Sarah makes $56k at Dell"},
        ]
    )
    assert event.source_channel == ChannelType.CHAT
    assert event.requires_verification is True
    assert isinstance(event.extracted_fields, dict)
