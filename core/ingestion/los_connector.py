"""LOS (Loan Origination System) connectors.

Each connector translates an LOS-shaped payload into the simulator's
internal model. The :class:`LOSConnector` base class holds the shared
translation logic; subclasses just implement ``_extract_*`` for their
specific payload shape.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Optional

from core.ingestion.confidence import SOURCE_CONFIDENCE_RANKING
from core.ingestion.mismo import MISMOMapper

logger = logging.getLogger(__name__)


# When we know the internal type, this map picks the right confidence
# bucket from SOURCE_CONFIDENCE_RANKING. Anything else defaults to
# API_JSON (0.88).
_TYPE_TO_CONFIDENCE_KEY: dict[str, str] = {
    "IRS_TRANSCRIPT":     "IRS_TRANSCRIPT",
    "W2_CURRENT":         "W2_PDF",
    "PAYSTUB_CURRENT":    "PAYSTUB_PDF",
    "BANK_STATEMENT_M1":  "BANK_STMT_PDF",
    "AUS_DU_FINDINGS":    "PAYROLL_API",
    "AUS_LP_FINDINGS":    "PAYROLL_API",
}


class LOSConnector(ABC):
    """Translate LOS-shaped payloads into the simulator's internal model."""

    source_system: str = "generic"
    display_name:  str = "Generic LOS"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def translate_document(self, los_payload: dict) -> dict:
        """Translate a LOS document payload into a dict ready for ingestion."""
        external_type = self._extract_doc_type(los_payload)
        internal_type = MISMOMapper.to_internal_type(
            external_type, self.source_system
        )
        if not internal_type:
            content_text = los_payload.get("content_text", "")
            internal_type = (
                MISMOMapper.detect_type_from_content(content_text) or "UNKNOWN"
            )
            if internal_type == "UNKNOWN":
                logger.warning(
                    "unknown_doc_type",
                    extra={
                        "source_system": self.source_system,
                        "external_type": external_type,
                    },
                )

        raw_fields = self._extract_fields(los_payload) or {}
        internal_fields = MISMOMapper.map_fields(internal_type, raw_fields)

        return {
            "document_type":     internal_type,
            "document_category": MISMOMapper.get_document_category(internal_type),
            "extracted_fields":  internal_fields,
            "source_system":     self.source_system,
            "external_doc_id":   self._extract_doc_id(los_payload),
            "external_loan_id":  self._extract_loan_id(los_payload),
            "confidence_score":  self._base_confidence(internal_type),
        }

    def translate_loan(self, los_payload: dict) -> dict:
        """Translate a LOS loan payload to the simulator's POST /loans shape."""
        loan_id = self._extract_loan_id(los_payload)
        return {
            "los_id":        loan_id,
            "borrower":      self._extract_borrower(los_payload),
            "co_borrower":   self._extract_co_borrower(los_payload),
            "loan":          self._extract_loan_terms(los_payload),
            "external_ids":  {self.source_system: loan_id} if loan_id else {},
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _base_confidence(self, internal_type: str) -> float:
        key = _TYPE_TO_CONFIDENCE_KEY.get(internal_type, "API_JSON")
        return SOURCE_CONFIDENCE_RANKING.get(key, 0.88)

    # ------------------------------------------------------------------
    # Subclass contract
    # ------------------------------------------------------------------

    @abstractmethod
    def _extract_doc_type(self, payload: dict) -> str: ...

    @abstractmethod
    def _extract_doc_id(self, payload: dict) -> str: ...

    @abstractmethod
    def _extract_loan_id(self, payload: dict) -> str: ...

    @abstractmethod
    def _extract_fields(self, payload: dict) -> dict: ...

    @abstractmethod
    def _extract_borrower(self, payload: dict) -> dict: ...

    @abstractmethod
    def _extract_co_borrower(self, payload: dict) -> Optional[dict]: ...

    @abstractmethod
    def _extract_loan_terms(self, payload: dict) -> dict: ...


# ---------------------------------------------------------------------------
# Concrete connectors
# ---------------------------------------------------------------------------


class EncompassConnector(LOSConnector):
    """Connector for ICE Encompass.

    Encompass is camelCase or PascalCase depending on which API endpoint
    you hit; the extractors look in both shapes.
    """

    source_system = "encompass"
    display_name  = "ICE Encompass"

    def _extract_doc_type(self, payload: dict) -> str:
        return (
            payload.get("documentType")
            or payload.get("DocumentType")
            or payload.get("type")
            or ""
        )

    def _extract_doc_id(self, payload: dict) -> str:
        return (
            payload.get("documentId")
            or payload.get("DocumentId")
            or payload.get("id")
            or ""
        )

    def _extract_loan_id(self, payload: dict) -> str:
        return (
            payload.get("loanNumber")
            or payload.get("LoanNumber")
            or payload.get("loanId")
            or ""
        )

    def _extract_fields(self, payload: dict) -> dict:
        return (
            payload.get("fields")
            or payload.get("extractedData")
            or payload.get("data")
            or {}
        )

    def _extract_borrower(self, payload: dict) -> dict:
        b = payload.get("borrower") or payload.get("Borrower") or {}
        ssn = b.get("taxIdentificationIdentifier") or ""
        return {
            "first_name": b.get("firstName") or b.get("FirstName", ""),
            "last_name":  b.get("lastName") or b.get("LastName", ""),
            "dob":        b.get("birthDate") or b.get("BirthDate", ""),
            "ssn_last4":  ssn[-4:] if ssn else "",
            "email":      b.get("emailAddressText") or b.get("email", ""),
        }

    def _extract_co_borrower(self, payload: dict) -> Optional[dict]:
        cb = payload.get("coBorrower") or payload.get("CoBorrower")
        if not cb:
            return None
        ssn = cb.get("taxIdentificationIdentifier") or ""
        return {
            "first_name": cb.get("firstName", ""),
            "last_name":  cb.get("lastName", ""),
            "dob":        cb.get("birthDate", ""),
            "ssn_last4":  ssn[-4:] if ssn else "",
        }

    def _extract_loan_terms(self, payload: dict) -> dict:
        loan = payload.get("loanInformation") or payload.get("loan") or {}
        return {
            "loan_amount":      loan.get("loanAmount") or loan.get("BaseLoanAmount"),
            "loan_type":        loan.get("loanType", "conventional"),
            "loan_purpose":     loan.get("loanPurpose", "purchase"),
            "interest_rate":    loan.get("noteRatePercent"),
            "loan_term_months": loan.get("loanTermMonths", 360),
        }


class GenericMISMOConnector(LOSConnector):
    """Generic connector for any LOS that emits MISMO 3.4 JSON/XML.

    Works as a baseline for BytePro, OpenClose, Finastra, MeridianLink.
    Subclass and override individual extractors when an LOS deviates.
    """

    source_system = "mismo_34"
    display_name  = "Generic MISMO 3.4"

    def _extract_doc_type(self, payload: dict) -> str:
        return (
            payload.get("DataPointName")
            or payload.get("DocumentType")
            or payload.get("MISMOType")
            or ""
        )

    def _extract_doc_id(self, payload: dict) -> str:
        return (
            payload.get("DocumentIdentifier")
            or payload.get("documentId")
            or ""
        )

    def _extract_loan_id(self, payload: dict) -> str:
        return (
            payload.get("LoanIdentifier")
            or payload.get("loanId")
            or ""
        )

    def _extract_fields(self, payload: dict) -> dict:
        return payload.get("DataPoints") or payload.get("fields") or {}

    def _extract_borrower(self, payload: dict) -> dict:
        b = payload.get("BORROWER") or payload.get("Borrower") or {}
        n = b.get("NAME") or b.get("IndividualName") or {}
        ssn = b.get("TaxpayerIdentifierValue") or ""
        return {
            "first_name": n.get("FirstName", ""),
            "last_name":  n.get("LastName", ""),
            "dob":        b.get("BirthDate", ""),
            "ssn_last4":  ssn[-4:] if ssn else "",
        }

    def _extract_co_borrower(self, payload: dict) -> Optional[dict]:
        # MISMO models co-borrowers via PARTY/Role nesting which varies
        # per LOS — leave to a real per-LOS subclass to override.
        return None

    def _extract_loan_terms(self, payload: dict) -> dict:
        loan = payload.get("LOAN") or {}
        return {
            "loan_amount":  loan.get("NoteAmount"),
            "loan_type":    loan.get("MortgageType", "conventional"),
            "loan_purpose": loan.get("LoanPurposeType", "purchase"),
        }


_CONNECTOR_REGISTRY: dict[str, type[LOSConnector]] = {
    "encompass":    EncompassConnector,
    "mismo_34":     GenericMISMOConnector,
    "byteprocloud": GenericMISMOConnector,
    "openclose":    GenericMISMOConnector,
    "meridianlink": GenericMISMOConnector,
}


def get_connector(source_system: str) -> LOSConnector:
    """Factory — returns the matching connector for a source system.

    Unknown systems default to the generic MISMO 3.4 connector so that
    the universal endpoint still produces useful output.
    """
    cls = _CONNECTOR_REGISTRY.get((source_system or "").lower(), GenericMISMOConnector)
    return cls()
