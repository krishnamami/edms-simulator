"""Tests for the MISMO mapper + LOS connectors (Phase 0)."""
from core.ingestion.los_connector import EncompassConnector, get_connector
from core.ingestion.mismo import (
    ENCOMPASS_TO_INTERNAL,
    INTERNAL_TO_MISMO,
    MISMO_TO_INTERNAL,
    MISMOMapper,
)


def test_mismo_to_internal_w2():
    assert MISMOMapper.to_internal_type("W2") == "W2_CURRENT"


def test_mismo_to_internal_appraisal():
    assert (
        MISMOMapper.to_internal_type("UniformResidentialAppraisalReport")
        == "APPRAISAL_URAR"
    )


def test_encompass_to_internal_paystub():
    assert (
        MISMOMapper.to_internal_type("Pay Stub", "encompass") == "PAYSTUB_CURRENT"
    )


def test_field_mapping_w2():
    fields = {"WagesAmount": 92400, "EmployerName": "Accenture"}
    result = MISMOMapper.map_fields("W2_CURRENT", fields)
    assert result["box1_wages"] == 92400
    assert result["employer_name"] == "Accenture"


def test_content_detection_w2():
    text = "Wage and Tax Statement Box 1 Wages 92400"
    assert MISMOMapper.detect_type_from_content(text) == "W2_CURRENT"


def test_content_detection_falls_through_to_none():
    assert MISMOMapper.detect_type_from_content("just some random text") is None


def test_encompass_connector_translate():
    connector = get_connector("encompass")
    assert isinstance(connector, EncompassConnector)
    payload = {
        "loanNumber":   "ENH-2024-98765",
        "documentId":   "DOC-A-1",
        "documentType": "W-2",
        "fields":       {"WagesAmount": 92400, "EmployerName": "Accenture LLC"},
    }
    result = connector.translate_document(payload)
    assert result["document_type"] == "W2_CURRENT"
    assert result["external_loan_id"] == "ENH-2024-98765"
    assert result["external_doc_id"] == "DOC-A-1"
    assert result["extracted_fields"]["box1_wages"] == 92400
    assert result["extracted_fields"]["employer_name"] == "Accenture LLC"
    assert result["document_category"] == "income"
    assert result["confidence_score"] >= 0.85


def test_unknown_connector_falls_back_to_generic():
    connector = get_connector("never-heard-of-this-LOS")
    # Default is GenericMISMOConnector
    assert connector.source_system == "mismo_34"


def test_document_category_mapping():
    assert MISMOMapper.get_document_category("W2_CURRENT") == "income"
    assert MISMOMapper.get_document_category("APPRAISAL_URAR") == "property"
    assert MISMOMapper.get_document_category("CREDIT_REPORT") == "credit"
    assert MISMOMapper.get_document_category("FLOOD_CERT") == "property"
    assert MISMOMapper.get_document_category("BANK_STATEMENT_M1") == "asset"
    assert MISMOMapper.get_document_category("URLA_1003") == "loan"
    assert MISMOMapper.get_document_category("IDENTITY_DL") == "identity"
    assert MISMOMapper.get_document_category("AUS_DU_FINDINGS") == "compliance"
    # Unknown internal types fall back to "loan"
    assert MISMOMapper.get_document_category("WHATEVER_NEW_TYPE") == "loan"


def test_reverse_mapping_complete():
    """Every internal type in MISMO_TO_INTERNAL has a back-edge in INTERNAL_TO_MISMO."""
    for mismo_name, internal_name in MISMO_TO_INTERNAL.items():
        assert internal_name in INTERNAL_TO_MISMO, (
            f"missing reverse map for {internal_name}"
        )
        assert INTERNAL_TO_MISMO[internal_name] == mismo_name


def test_encompass_table_keys_unique_in_internal_space():
    """Encompass labels can be aliases (Pay Stub vs Paystub both map to
    PAYSTUB_CURRENT) so we don't require a unique reverse map there —
    just sanity-check that every Encompass mapping is to a known internal type."""
    known_internal = set(MISMO_TO_INTERNAL.values())
    for label, internal in ENCOMPASS_TO_INTERNAL.items():
        assert internal in known_internal, f"{label} -> {internal} not in MISMO map"


def test_generic_mismo_connector_translate():
    connector = get_connector("mismo_34")
    payload = {
        "DataPointName":      "W2",
        "LoanIdentifier":     "LN-12345",
        "DocumentIdentifier": "DOC-99",
        "DataPoints":         {
            "WagesAmount":  92400,
            "EmployerName": "Dell Technologies",
        },
    }
    result = connector.translate_document(payload)
    assert result["document_type"] == "W2_CURRENT"
    assert result["external_loan_id"] == "LN-12345"
    assert result["external_doc_id"] == "DOC-99"
    assert result["extracted_fields"]["box1_wages"] == 92400
    assert result["source_system"] == "mismo_34"


def test_translate_loan_returns_external_ids():
    connector = get_connector("encompass")
    payload = {
        "loanNumber": "ENH-2024-98765",
        "borrower": {
            "firstName": "James", "lastName": "Okafor",
            "birthDate": "1982-07-14",
            "taxIdentificationIdentifier": "123456789",
            "emailAddressText": "james@example.com",
        },
        "loanInformation": {
            "loanAmount":      385000,
            "loanType":        "conventional",
            "loanPurpose":     "purchase",
            "noteRatePercent": 7.125,
            "loanTermMonths":  360,
        },
    }
    result = connector.translate_loan(payload)
    assert result["los_id"] == "ENH-2024-98765"
    assert result["external_ids"] == {"encompass": "ENH-2024-98765"}
    assert result["borrower"]["first_name"] == "James"
    assert result["borrower"]["ssn_last4"] == "6789"
    # ssn_hash MUST be populated whenever the full SSN is present, otherwise
    # multiple "" hashes collide on idx_applicant_ssn UNIQUE constraint.
    assert result["borrower"]["ssn_hash"], "ssn_hash must be a non-empty digest"
    assert len(result["borrower"]["ssn_hash"]) == 64  # sha256 hex
    assert result["loan"]["loan_amount"] == 385000
    assert result["loan"]["interest_rate"] == 7.125


def test_translate_loan_distinct_ssns_produce_distinct_hashes():
    """Two different SSNs must produce different ssn_hash values."""
    connector = get_connector("encompass")
    def _payload(ssn):
        return {
            "loanNumber": f"L-{ssn[-4:]}",
            "borrower": {
                "firstName": "A", "lastName": "B", "birthDate": "1980-01-01",
                "taxIdentificationIdentifier": ssn,
            },
            "loanInformation": {"loanAmount": 1, "loanType": "conventional", "loanPurpose": "purchase"},
        }
    a = connector.translate_loan(_payload("111223333"))
    b = connector.translate_loan(_payload("999887766"))
    assert a["borrower"]["ssn_hash"] != b["borrower"]["ssn_hash"]
    assert a["borrower"]["ssn_hash"] and b["borrower"]["ssn_hash"]


def test_translate_loan_no_ssn_leaves_hash_empty():
    """No SSN -> empty hash. (DB unique constraint still bites if
    multiple such rows arrive, but the behaviour is explicit.)"""
    connector = get_connector("encompass")
    payload = {
        "loanNumber": "L-NOSSN",
        "borrower": {"firstName": "A", "lastName": "B", "birthDate": "1980-01-01"},
        "loanInformation": {"loanAmount": 1, "loanType": "conventional", "loanPurpose": "purchase"},
    }
    result = connector.translate_loan(payload)
    assert result["borrower"]["ssn_hash"] == ""
    assert result["borrower"]["ssn_last4"] == ""
