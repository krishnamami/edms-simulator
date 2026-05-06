"""Vendor fraud / KYC return adapter (Socure + LexisNexis Instant ID)."""
from __future__ import annotations

from core.ingestion.events import ChannelType, NormalizedIngestEvent

SOCURE_CONFIDENCE = 0.95
LEXISNEXIS_CONFIDENCE = 0.93


class VendorFraudAdapter:
    """Translate a fraud / identity check JSON response into a
    NormalizedIngestEvent."""

    def process(self, payload: dict) -> NormalizedIngestEvent:
        """payload shape::

            {
              "vendor":   "socure" | "lexisnexis",
              "response": { ...vendor JSON... },
              "applicant_id":   "APL-...",
              "application_id": "APP-..."
            }
        """
        vendor = (payload.get("vendor") or "socure").lower()
        response = payload.get("response") or {}

        if vendor == "socure":
            fields = self._parse_socure(response)
            confidence = SOCURE_CONFIDENCE
        else:
            fields = self._parse_lexisnexis(response)
            confidence = LEXISNEXIS_CONFIDENCE

        return NormalizedIngestEvent(
            source_channel=ChannelType.API,
            applicant_signals={
                "applicant_id":   payload.get("applicant_id"),
                "application_id": payload.get("application_id"),
            },
            document_type="FRAUD_REPORT",
            extracted_fields=fields,
            confidence=confidence,
            requires_verification=False,
            missing_fields=[],
            documents_needed=[],
        )

    # ------------------------------------------------------------------

    def _parse_socure(self, response: dict) -> dict:
        scores = response.get("scores") or []
        fraud_score = next(
            (s.get("score") for s in scores if s.get("name") == "fraud"),
            response.get("fraudScore"),
        )
        kyc = response.get("kyc") or {}
        fraud_score_f = float(fraud_score) if fraud_score is not None else None
        return {
            "vendor":         "socure",
            "fraud_score":    fraud_score_f,
            "risk_band":      self._fraud_band(fraud_score_f),
            "kyc_pass":       (kyc.get("reasonCodes") or []) == [],
            "reason_codes":   kyc.get("reasonCodes") or [],
            "document_verification": response.get("documentVerification") or {},
            "email_risk":     (response.get("emailRisk")   or {}).get("score"),
            "phone_risk":     (response.get("phoneRisk")   or {}).get("score"),
            "address_risk":   (response.get("addressRisk") or {}).get("score"),
            "decision_status": response.get("decisionStatus"),
        }

    def _parse_lexisnexis(self, response: dict) -> dict:
        score = response.get("riskScore") or response.get("RiskScore")
        score_f = float(score) if score is not None else None
        return {
            "vendor":     "lexisnexis",
            "risk_score": score_f,
            "fraud_score": score_f,
            "risk_band":  self._fraud_band(score_f),
            "kyc_pass":   bool(response.get("kycPass", False)),
            "alerts":     response.get("alerts") or [],
            "ofac_hit":   bool(response.get("ofacHit", False)),
        }

    def _fraud_band(self, score) -> str:
        if score is None:
            return "unknown"
        score = float(score)
        if score >= 0.85:
            return "high_risk"
        if score >= 0.60:
            return "medium_risk"
        return "low_risk"

    @staticmethod
    def requires_review(fields: dict) -> bool:
        return (fields.get("risk_band") or "low_risk") in (
            "high_risk", "medium_risk"
        )
