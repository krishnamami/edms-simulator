"""Vendor verification-of-employment return adapter (TWN + Equifax VOE)."""
from __future__ import annotations

from core.ingestion.events import ChannelType, NormalizedIngestEvent

VOE_CONFIDENCE = 0.97


class VendorVOEAdapter:
    def process(self, payload: dict) -> NormalizedIngestEvent:
        """payload shape::

            {
              "vendor":   "twn" | "equifax_voe",
              "response": { ... },
              "applicant_id":   "APL-...",
              "application_id": "APP-..."
            }
        """
        vendor = (payload.get("vendor") or "twn").lower()
        response = payload.get("response") or {}

        if vendor == "twn":
            fields = self._parse_twn(response)
        else:
            fields = self._parse_equifax_voe(response)

        return NormalizedIngestEvent(
            source_channel=ChannelType.API,
            applicant_signals={
                "applicant_id":   payload.get("applicant_id"),
                "application_id": payload.get("application_id"),
            },
            document_type="EMPLOYMENT_VERIFICATION",
            extracted_fields=fields,
            confidence=VOE_CONFIDENCE,
            requires_verification=False,
            missing_fields=[],
            documents_needed=[],
        )

    # ------------------------------------------------------------------

    def _parse_twn(self, response: dict) -> dict:
        emps = response.get("employments") or [{}]
        sals = response.get("salaries") or [{}]
        emp = emps[0] if emps else {}
        salary = sals[0] if sals else {}
        status = (emp.get("employmentStatus") or "").upper()
        return {
            "vendor":              "twn",
            "employer_name":       emp.get("employerName"),
            "employment_status":   status,
            "hire_date":           emp.get("hireDate") or emp.get("originalHireDate"),
            "termination_date":    emp.get("terminationDate"),
            "position_title":      emp.get("positionTitle"),
            "base_pay_annual":     salary.get("basePayAnnual") or salary.get("annualSalary"),
            "base_pay_hourly":     salary.get("basePayHourly"),
            "pay_frequency":       salary.get("payFrequency"),
            "employment_verified": status == "A",
            "verified_at":         response.get("reportDate"),
        }

    def _parse_equifax_voe(self, response: dict) -> dict:
        currently = (response.get("currentlyEmployed") or "").lower()
        return {
            "vendor":              "equifax_voe",
            "employer_name":       response.get("employerName"),
            "employment_status":   response.get("currentlyEmployed") or "",
            "hire_date":           response.get("hireDate"),
            "annual_salary":       response.get("annualSalary"),
            "employment_verified": currently == "yes",
            "verified_at":         response.get("verificationDate"),
        }
