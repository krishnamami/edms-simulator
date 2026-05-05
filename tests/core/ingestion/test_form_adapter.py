"""Form adapter tests — required-field validation and shape."""
import pytest

from core.ingestion.adapters import form_adapter
from core.ingestion.events import ChannelType


def test_urla_1003_complete_has_no_missing():
    event = form_adapter.adapt({
        "form_type": "URLA_1003",
        "fields": {
            "first_name": "James",
            "last_name": "Okafor",
            "dob": "1982-07-14",
            "ssn_last4": "4729",
            "employer": "Accenture",
            "annual_income": 92400,
            "address": "100 Main St, San Francisco, CA",
        },
    })
    assert event.source_channel == ChannelType.FORM
    assert event.confidence == 0.90
    assert event.requires_verification is False
    assert event.missing_fields == []
    assert event.applicant_signals["first_name"] == "James"


def test_urla_1003_missing_fields_reported():
    event = form_adapter.adapt({
        "form_type": "URLA_1003",
        "fields": {"first_name": "James", "last_name": "Okafor"},
    })
    missing = set(event.missing_fields)
    assert {"dob", "ssn_last4", "employer", "annual_income", "address"}.issubset(missing)


def test_unknown_form_type_raises():
    with pytest.raises(ValueError):
        form_adapter.adapt({"form_type": "UNRELATED", "fields": {}})


def test_contact_form_minimal_required_set():
    event = form_adapter.adapt({
        "form_type": "CONTACT_FORM",
        "fields": {"first_name": "James", "last_name": "Okafor", "email": "j@e.com"},
    })
    assert event.missing_fields == []
    assert event.extracted_fields["form_type"] == "CONTACT_FORM"
