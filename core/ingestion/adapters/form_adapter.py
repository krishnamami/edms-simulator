"""Web-form adapter — deterministic field mapping with required-field validation.

Confidence is fixed at 0.90: structured but self-reported, so above CHAT
(0.80) and below verified documents (0.95+).
"""
from __future__ import annotations

from typing import Optional

from core.ingestion.events import ChannelType, NormalizedIngestEvent


REQUIRED_FIELDS: dict[str, list[str]] = {
    "URLA_1003": [
        "first_name", "last_name", "dob", "ssn_last4",
        "employer", "annual_income", "address",
    ],
    "INCOME_FORM":  ["annual_income", "employer"],
    "ASSET_FORM":   ["bank_name", "account_balance"],
    "CONTACT_FORM": ["first_name", "last_name", "email"],
}


def adapt(
    payload: dict,
    *,
    applicant_id: Optional[str] = None,
    borrower_role: str = "primary",
) -> NormalizedIngestEvent:
    form_type = payload.get("form_type")
    fields = payload.get("fields") or {}

    if form_type not in REQUIRED_FIELDS:
        raise ValueError(
            f"Unknown form_type: {form_type!r}. "
            f"Expected one of {sorted(REQUIRED_FIELDS)}"
        )

    required = REQUIRED_FIELDS[form_type]
    missing = [f for f in required if not fields.get(f)]

    signals = {
        "first_name": fields.get("first_name"),
        "last_name": fields.get("last_name"),
        "dob": fields.get("dob"),
        "ssn_last4": fields.get("ssn_last4"),
        "email": fields.get("email"),
        "phone": fields.get("phone"),
        "role": borrower_role,
    }
    if applicant_id:
        signals["applicant_id"] = applicant_id

    return NormalizedIngestEvent(
        source_channel=ChannelType.FORM,
        document_type=form_type,
        applicant_signals=signals,
        extracted_fields={"form_type": form_type, **fields},
        confidence=0.90,
        requires_verification=False,
        missing_fields=missing,
    )
