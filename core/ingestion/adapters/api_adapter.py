"""API adapter — wraps the existing JSON loan payload.

The JSON POST /loans body is already structured; the adapter just lifts
the borrower signals into applicant_signals and surfaces the documents
list as extracted_fields["documents"].
"""
from core.ingestion.events import ChannelType, NormalizedIngestEvent


def adapt(payload: dict) -> NormalizedIngestEvent:
    borrower = payload.get("borrower", {}) or {}
    signals = {
        "first_name": borrower.get("first_name"),
        "last_name": borrower.get("last_name"),
        "dob": borrower.get("dob"),
        "ssn_hash": borrower.get("ssn_hash"),
        "ssn_last4": borrower.get("ssn_last4"),
        "email": borrower.get("email"),
        "phone": borrower.get("phone"),
        "los_id": payload.get("los_id"),
        "role": "primary",
    }
    return NormalizedIngestEvent(
        source_channel=ChannelType.API,
        applicant_signals=signals,
        extracted_fields={
            "loan": payload.get("loan", {}),
            "documents": payload.get("documents", []),
            "co_borrower": payload.get("co_borrower"),
            "los_id": payload.get("los_id"),
        },
        confidence=1.0,
        requires_verification=False,
    )
