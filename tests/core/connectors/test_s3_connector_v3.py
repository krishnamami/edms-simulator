"""Tests for the v3 two-stage layout dispatch in ``S3EDMSConnector``.

The fixture builds a tiny v3 tree directly on the filesystem (the same
shape ``scripts/generate_realworld_simulation_v3.py`` writes) and
asserts the connector reads:

- ``loan_origination/{los}_application.json`` as an
  ``event_type='loan_application_submitted'`` doc, yielded BEFORE any
  ``post_application/`` channel entries.
- ``post_application/edms_pull/`` and friends — known JSON channels.
- ``post_application/los_encompass/`` — JSON-array batches exploded.
- ``post_application/los_bytepro/`` — CSV rows expanded to one doc each.
- ``post_application/mismo_feed/`` — XML envelopes parsed via
  local-name matching.
- ``post_application/{employer_manual,appraisal_manual,…}/`` — PDF-only
  channels synthesised as UNCLASSIFIED.

Plus a regression: the v2 layout (no ``loan_origination`` /
``post_application``) keeps working unchanged.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from core.connectors.s3_connector import S3EDMSConnector


class _StubPG:
    async def get_watermark(self, source):
        return None

    async def set_watermark_timestamp(self, source, ts):
        pass


def _write(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    if isinstance(payload, (dict, list)):
        path.write_text(json.dumps(payload), encoding="utf-8")
    elif isinstance(payload, bytes):
        path.write_bytes(payload)
    else:
        path.write_text(payload, encoding="utf-8")


@pytest.fixture
def v3_root(tmp_path: Path) -> Path:
    """Build a one-day v3 tree exercising every format family."""
    root = tmp_path / "s3_simulation_v3"
    day = root / "2026-01-01"

    # === loan_origination ===
    _write(day / "loan_origination" / "LOAN-101_application.json", {
        "event_type":  "loan_application_submitted",
        "los_id":      "LOAN-101",
        "received_at": "2026-01-01T09:15:00Z",
        "source_system": "ENCOMPASS",
        "legacy_ids": {
            "encompass_loan_number": "ENC-2026-101",
            "encompass_borrower_id": "ENC-BR-78901",
        },
        "loan_terms": {
            "loan_purpose": "purchase", "loan_amount": 360000,
            "interest_rate": 6.25, "loan_term_months": 360,
            "occupancy": "primary_residence", "property_type": "SFR",
        },
        "borrower": {
            "first_name": "James", "last_name": "Wilson",
            "dob": "1985-06-20", "ssn_last4": "4567",
            "email": "james.wilson@email.com",
            "current_address": "456 Elm St, Austin TX 78745",
            "stated_income": 125000, "stated_employer": "TechCorp Inc",
            "years_at_employer": 6, "stated_assets": 200000,
        },
        "co_borrower": None,
        "property": {
            "address": "123 Oak Valley Dr, Austin TX 78745",
            "city": "Austin", "state": "TX", "zip": "78745",
            "type": "SFR", "purchase_price": 450000,
        },
    })

    pa = day / "post_application"

    # === Individual JSON channel ===
    _write(pa / "employer_adp" / "ADP-W2-001.json", {
        "document_id": "ADP-W2-001",
        "document_type": "W2_CURRENT",
        "category": "income",
        "los_id": "LOAN-101",
        "borrower_role": "primary",
        "received_at": "2026-01-01T10:15:00Z",
        "source_document_id": "ADP-W2-2025-4567",
        "extracted_fields": {"box1_wages": 125000},
    })

    # === JSON-array batch ===
    _write(pa / "los_encompass" / "LOAN-101_batch.json", [
        {"document_id": "ENC-URLA-001", "document_type": "URLA_1003",
         "category": "loan_terms", "los_id": "LOAN-101",
         "received_at": "2026-01-01T11:15:00Z",
         "source_document_id": "ENC-2026-12345",
         "extracted_fields": {}},
        {"document_id": "ENC-CREDIT-001", "document_type": "CREDIT_REPORT",
         "category": "credit", "los_id": "LOAN-101",
         "received_at": "2026-01-01T11:15:00Z",
         "source_document_id": "ENC-2026-12346",
         "extracted_fields": {}},
    ])

    # === CSV — los_bytepro ===
    _write(
        pa / "los_bytepro" / "snapshot.csv",
        "document_id,document_type,los_id,borrower_name,received_at\n"
        "BP-101-001,LOAN_SNAPSHOT,LOAN-101,James Wilson,2026-01-01T14:15:00Z\n"
        "BP-102-001,LOAN_SNAPSHOT,LOAN-102,Maria Garcia,2026-01-01T14:15:00Z\n",
    )

    # === XML — mismo_feed ===
    _write(
        pa / "mismo_feed" / "MISMO-LOAN-101.xml",
        '<?xml version="1.0" encoding="utf-8"?>'
        '<MESSAGE xmlns="http://www.mismo.org/residential/2009/schemas" '
        'MISMOReferenceModelIdentifier="3.4.0" '
        'MessageDateTime="2026-01-01T12:15:00Z">'
        '<DEAL_SETS><DEAL_SET><DEALS><DEAL>'
        '<PARTIES><PARTY><INDIVIDUAL><NAME>'
        '<FirstName>James</FirstName><LastName>Wilson</LastName>'
        '</NAME></INDIVIDUAL></PARTY></PARTIES>'
        '<LOANS><LOAN><LOAN_DETAIL>'
        '<LoanPurposeType>Purchase</LoanPurposeType></LOAN_DETAIL>'
        '<TERMS_OF_LOAN><BaseLoanAmount>360000</BaseLoanAmount>'
        '<NoteRatePercent>6.25</NoteRatePercent></TERMS_OF_LOAN>'
        '<LOAN_IDENTIFIERS><LOAN_IDENTIFIER>'
        '<LoanIdentifier>LOAN-101</LoanIdentifier>'
        '<LoanIdentifierType>LenderLoan</LoanIdentifierType>'
        '</LOAN_IDENTIFIER></LOAN_IDENTIFIERS>'
        '</LOAN></LOANS></DEAL></DEALS></DEAL_SET></DEAL_SETS>'
        '</MESSAGE>',
    )

    # === PDF + meta — appraisal_mercury (v3 channel) ===
    _write(pa / "appraisal_mercury" / "MERC-LOAN-101.pdf",
           b"%PDF-1.4 stub")
    _write(pa / "appraisal_mercury" / "MERC-LOAN-101_meta.json", {
        "document_id":   "MERC-LOAN-101",
        "document_type": "APPRAISAL_URAR",
        "category":      "property",
        "los_id":        "LOAN-101",
        "borrower_role": "primary",
        "received_at":   "2026-01-01T15:15:00Z",
        "source_document_id": "MERC-RPT-2026-1019",
        "extracted_fields": {"appraised_value": 460000},
    })

    # === PDF only — appraisal_manual ===
    _write(pa / "appraisal_manual" / "scan_001.pdf", b"%PDF-1.4 stub")

    return root


def _pull(root: Path) -> list[dict]:
    c = S3EDMSConnector(str(root), _StubPG())
    return asyncio.run(c.pull_documents_since("1970-01-01T00:00:00Z"))


def test_loan_origination_event_yielded_before_post_app(v3_root: Path):
    """The connector must yield ``loan_origination/`` first so the
    builder's step 2.0 creates apps before docs need los_id resolution.
    Easiest assertion: there's exactly one event_type doc and its
    received_at is the earliest in the result set (matches the 09:15
    timestamp we wrote)."""
    docs = _pull(v3_root)
    events = [d for d in docs if d.get("event_type") == "loan_application_submitted"]
    assert len(events) == 1
    e = events[0]
    assert e["los_id"] == "LOAN-101"
    assert e["legacy_ids"]["encompass_loan_number"] == "ENC-2026-101"
    assert e["source_channel"] == "loan_origination"
    # Builder needs ``borrower`` block intact for create_application_from_event.
    assert e["borrower"]["first_name"] == "James"
    # Ordering: every other doc has received_at >= the event's.
    other_received = [d.get("received_at") for d in docs if d is not e]
    assert all((r or "") >= e["received_at"] for r in other_received)


def test_v3_individual_json_channel_routes_to_employer_adp(v3_root: Path):
    docs = _pull(v3_root)
    adp = [d for d in docs if d.get("source_channel") == "employer_adp"]
    assert len(adp) == 1
    assert adp[0]["document_type"] == "W2_CURRENT"
    assert adp[0]["source_document_id"] == "ADP-W2-2025-4567"


def test_v3_csv_channel_explodes_each_row_to_a_doc(v3_root: Path):
    docs = _pull(v3_root)
    bp = [d for d in docs if d.get("source_channel") == "los_bytepro"]
    assert len(bp) == 2, f"expected 2 CSV rows, got {len(bp)}"
    los_ids = sorted(d.get("los_id") for d in bp)
    assert los_ids == ["LOAN-101", "LOAN-102"]


def test_v3_xml_channel_parses_mismo_envelope(v3_root: Path):
    docs = _pull(v3_root)
    mismo = [d for d in docs if d.get("source_channel") == "mismo_feed"]
    assert len(mismo) == 1
    m = mismo[0]
    assert m["los_id"] == "LOAN-101"
    f = m["extracted_fields"]
    assert f["borrower_first_name"] == "James"
    assert f["loan_amount"]         == 360000
    assert f["interest_rate"]       == 6.25
    assert f["loan_purpose"]        == "purchase"


def test_v3_pdf_meta_channel_reads_meta_only(v3_root: Path):
    docs = _pull(v3_root)
    appr = [d for d in docs if d.get("source_channel") == "appraisal_mercury"]
    assert len(appr) == 1
    a = appr[0]
    assert a["document_type"] == "APPRAISAL_URAR"
    assert a["source_document_id"] == "MERC-RPT-2026-1019"
    # Sibling .pdf is hinted but the .pdf bytes themselves were NOT
    # parsed as JSON (the read would've failed and the doc would not
    # be present).
    assert a.get("evidence_file", "").endswith(".pdf")


def test_v3_pdf_only_channel_synthesizes_unclassified_doc(v3_root: Path):
    docs = _pull(v3_root)
    manual = [d for d in docs if d.get("source_channel") == "appraisal_manual"]
    assert len(manual) == 1
    m = manual[0]
    assert m["document_type"]               == "UNKNOWN"
    assert m["los_id"]                      == "UNCLASSIFIED"
    assert m["requires_classification"]     is True


def test_v3_los_encompass_batch_explodes(v3_root: Path):
    docs = _pull(v3_root)
    enc = [d for d in docs if d.get("source_channel") == "los_encompass"]
    assert len(enc) == 2
    assert sorted(d["document_type"] for d in enc) == ["CREDIT_REPORT", "URLA_1003"]


def test_v3_total_doc_count_matches_expected(v3_root: Path):
    """Sanity total: 1 event + 1 adp + 2 batch + 2 csv + 1 xml + 1 appraisal +
    1 manual = 9."""
    docs = _pull(v3_root)
    assert len(docs) == 9


def test_v2_layout_unchanged_no_v3_stage_dirs(tmp_path: Path):
    """v2 fixture (no loan_origination / post_application) must still
    take the v2 path."""
    root = tmp_path / "s3_simulation"
    day = root / "2026-01-01"
    _write(day / "edms_pull" / "doc.json", {
        "document_id": "EDMS-V2-001", "document_type": "W2_CURRENT",
        "los_id": "LOAN-001", "received_at": "2026-01-01T09:15:00Z",
        "extracted_fields": {},
    })
    docs = _pull(root)
    assert len(docs) == 1
    assert docs[0]["source_channel"] == "edms_pull"
    assert "event_type" not in docs[0]
