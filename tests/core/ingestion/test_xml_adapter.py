"""XML adapter tests — IRS 4506-C and MISMO 3.4 detection + extraction."""
from core.ingestion.adapters import xml_adapter
from core.ingestion.events import ChannelType


_IRS_TRANSCRIPT = b"""<?xml version="1.0"?>
<TaxTranscript>
  <TaxYear>2023</TaxYear>
  <FilingStatus>Single</FilingStatus>
  <AGI>92400</AGI>
  <Wages>92400</Wages>
  <Taxpayer>
    <Name>James Okafor</Name>
    <SSNLast4>4729</SSNLast4>
  </Taxpayer>
</TaxTranscript>
"""

_MISMO_DOC = b"""<?xml version="1.0"?>
<MESSAGE xmlns="http://www.mismo.org/residential/2009/schemas">
  <DEAL>
    <PARTIES>
      <PARTY>
        <INDIVIDUAL>
          <NAME>
            <FirstName>James</FirstName>
            <LastName>Okafor</LastName>
          </NAME>
        </INDIVIDUAL>
      </PARTY>
    </PARTIES>
    <LOAN>
      <BaseLoanAmount>385000</BaseLoanAmount>
      <LoanIdentifier>LOAN-001</LoanIdentifier>
    </LOAN>
  </DEAL>
</MESSAGE>
"""


def test_irs_transcript_extracts_with_high_confidence():
    event = xml_adapter.adapt(_IRS_TRANSCRIPT)
    assert event.source_channel == ChannelType.XML
    assert event.document_type == "IRS_4506C"
    assert event.confidence == 0.99
    assert event.extracted_fields["agi"] == 92400.0
    assert event.extracted_fields["wages"] == 92400.0
    assert event.extracted_fields["filing_status"] == "Single"
    assert event.extracted_fields["tax_year"] == "2023"
    assert event.applicant_signals["first_name"] == "James"
    assert event.applicant_signals["last_name"] == "Okafor"


def test_mismo_extracts_loan_and_names():
    event = xml_adapter.adapt(_MISMO_DOC)
    assert event.document_type == "MISMO"
    assert event.extracted_fields["loan_amount"] == 385000.0
    assert event.extracted_fields["loan_identifier"] == "LOAN-001"
    assert event.applicant_signals["first_name"] == "James"
    assert event.applicant_signals["last_name"] == "Okafor"


def test_unknown_xml_low_confidence():
    event = xml_adapter.adapt(b"<?xml version='1.0'?><Random><Foo>1</Foo></Random>")
    assert event.document_type == "UNKNOWN_XML"
    assert event.confidence < 0.5
    assert event.requires_verification is True


def test_invalid_xml_raises_value_error():
    import pytest
    with pytest.raises(ValueError):
        xml_adapter.adapt(b"not even close to xml")
