"""Vendor SSA SSN-validation + Treasury OFAC sanctions return adapters."""
from __future__ import annotations

from core.ingestion.events import ChannelType, NormalizedIngestEvent

SSN_CONFIDENCE = 0.99
OFAC_CONFIDENCE = 0.99


class VendorSSNAdapter:
    def process(self, payload: dict) -> NormalizedIngestEvent:
        """SSA Consent-Based SSN Verification (CBSV) response."""
        response = payload.get("response") or {}
        fields = {
            "vendor":       "ssa",
            "ssn_valid":    bool(response.get("verified", False)),
            "name_match":   bool(response.get("nameMatch", False)),
            "dob_match":    bool(response.get("dobMatch", False)),
            "death_record": bool(response.get("deathRecord", False)),
            "verified_at":  response.get("verificationDate"),
        }
        return NormalizedIngestEvent(
            source_channel=ChannelType.API,
            applicant_signals={
                "applicant_id":   payload.get("applicant_id"),
                "application_id": payload.get("application_id"),
            },
            document_type="SSN_VALIDATION",
            extracted_fields=fields,
            confidence=SSN_CONFIDENCE,
            requires_verification=False,
            missing_fields=[],
            documents_needed=[],
        )


class VendorOFACAdapter:
    def process(self, payload: dict) -> NormalizedIngestEvent:
        """Treasury OFAC sanctions check response."""
        response = payload.get("response") or {}
        hit = bool(response.get("hit", False))
        fields = {
            "vendor":      "ofac",
            "ofac_clear":  not hit,
            "hit_count":   int(response.get("hitCount", 0)),
            "matches":     response.get("matches") or [],
            "checked_at":  response.get("checkedAt"),
        }
        return NormalizedIngestEvent(
            source_channel=ChannelType.API,
            applicant_signals={
                "applicant_id":   payload.get("applicant_id"),
                "application_id": payload.get("application_id"),
            },
            document_type="OFAC_REPORT",
            extracted_fields=fields,
            confidence=OFAC_CONFIDENCE,
            requires_verification=False,
            missing_fields=[],
            documents_needed=[],
        )
