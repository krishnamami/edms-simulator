"""Vendor AUS (Automated Underwriting) return adapter.

Handles Fannie Mae Desktop Underwriter (DU) and Freddie Mac Loan Product
Advisor (LP) XML findings. Both formats vary by version + namespace; we
detect by the ``aus_type`` field on the inbound payload and fall back to
namespace-tolerant XPath when parsing.
"""
from __future__ import annotations

import logging
import xml.etree.ElementTree as ET

from core.ingestion.events import ChannelType, NormalizedIngestEvent

logger = logging.getLogger(__name__)

AUS_CONFIDENCE = 0.99


class VendorAUSAdapter:
    """Translate a DU or LP XML findings payload into a NormalizedIngestEvent."""

    def process(self, payload: dict) -> NormalizedIngestEvent:
        """payload shape::

            {
              "aus_type":      "DU" | "LP",
              "xml_content":   "<...>",        # raw XML body
              "application_id": "APP-...",
              "applicant_id":   "APL-..."
            }
        """
        aus_type = (payload.get("aus_type") or "DU").upper()
        xml_content = payload.get("xml_content", "") or ""
        if aus_type == "DU":
            fields = self._parse_du(xml_content)
        else:
            fields = self._parse_lp(xml_content)

        doc_type = "AUS_DU_FINDINGS" if aus_type == "DU" else "AUS_LP_FINDINGS"
        return NormalizedIngestEvent(
            source_channel=ChannelType.API,
            applicant_signals={
                "applicant_id":   payload.get("applicant_id"),
                "application_id": payload.get("application_id"),
            },
            document_type=doc_type,
            extracted_fields=fields,
            confidence=AUS_CONFIDENCE,
            requires_verification=False,
            missing_fields=[],
            documents_needed=[],
        )

    # ------------------------------------------------------------------
    # DU / LP parsers
    # ------------------------------------------------------------------

    def _parse_du(self, xml_content: str) -> dict:
        fields: dict = {
            "aus_type":          "DU",
            "recommendation":    None,
            "eligible_products": [],
            "risk_factors":      [],
            "findings_text":     "",
            "casefile_id":       None,
            "version":           None,
        }
        if not xml_content:
            return fields
        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError as exc:
            logger.warning("du_parse_error", extra={"error": str(exc)})
            return fields

        ns = {"du": "http://www.fanniemae.com/du"}

        rec = (
            root.findtext(
                ".//du:RECOMMENDATION/du:RecommendationDescription",
                namespaces=ns,
            )
            or root.findtext(".//RecommendationDescription")
            or root.findtext(".//Recommendation")
        )
        fields["recommendation"] = rec

        casefile = (
            root.findtext(".//du:CasefileIdentifier", namespaces=ns)
            or root.findtext(".//CasefileIdentifier")
        )
        fields["casefile_id"] = casefile

        risk_nodes = (
            root.findall(".//du:RISK_FACTOR", namespaces=ns)
            or root.findall(".//RISK_FACTOR")
        )
        for factor in risk_nodes:
            desc = (
                factor.findtext("Description")
                or factor.findtext("du:Description", namespaces=ns)
            )
            if desc:
                fields["risk_factors"].append(desc.strip())

        product_nodes = (
            root.findall(".//du:ELIGIBLE_PRODUCT", namespaces=ns)
            or root.findall(".//ELIGIBLE_PRODUCT")
        )
        for product in product_nodes:
            name = product.findtext("ProductName") or (product.text or "").strip()
            if name:
                fields["eligible_products"].append(name)

        fields["findings_text"] = ET.tostring(root, encoding="unicode")[:500]
        return fields

    def _parse_lp(self, xml_content: str) -> dict:
        fields: dict = {
            "aus_type":       "LP",
            "recommendation": None,
            "risk_class":     None,
            "findings":       [],
            "key_data_id":    None,
        }
        if not xml_content:
            return fields
        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError as exc:
            logger.warning("lp_parse_error", extra={"error": str(exc)})
            return fields

        rec = (
            root.findtext(".//LPARecommendation")
            or root.findtext(".//Recommendation")
            or root.findtext(".//DecisionType")
        )
        fields["recommendation"] = rec

        key_id = (
            root.findtext(".//KeyDataIdentifier")
            or root.findtext(".//CaseIdentifier")
        )
        fields["key_data_id"] = key_id

        fields["risk_class"] = (
            root.findtext(".//RiskClass")
            or root.findtext(".//RiskClassification")
        )
        return fields

    @staticmethod
    def is_approved(fields: dict) -> bool:
        rec = (fields.get("recommendation") or "").lower()
        return any(word in rec for word in ("approve", "accept", "eligible"))
