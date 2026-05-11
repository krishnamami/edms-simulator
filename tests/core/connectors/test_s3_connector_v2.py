"""Unit tests for the channel-segmented v2 layout dispatch in
``S3EDMSConnector``. Builds a temp directory mirroring the v2 tree and
asserts every channel format produces the right shape.

The legacy (per-applicant) layout is also exercised so the
backwards-compat fallback stays wired."""
from __future__ import annotations

import asyncio
import json
from datetime import date
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
def v2_root(tmp_path: Path) -> Path:
    """Build a one-day v2 tree with at least one doc per channel."""
    root = tmp_path / "s3_simulation_v2"
    day = root / "2026-01-01"

    # 1. Individual JSON channels
    _write(day / "edms_pull" / "EDMS-W2-001.json", {
        "document_id": "EDMS-W2-001",
        "document_type": "W2_CURRENT",
        "category": "income",
        "los_id": "LOAN-101",
        "borrower_role": "primary",
        "received_at": "2026-01-01T09:15:00Z",
        "extracted_fields": {"box1_wages": 125000},
    })
    _write(day / "vendor_equifax" / "voe_LOAN-101.json", {
        "document_id": "EFX-VOE-001",
        "document_type": "VOE_TWN",
        "category": "employment",
        "los_id": "LOAN-101",
        "borrower_role": "primary",
        "received_at": "2026-01-01T11:15:00Z",
        "extracted_fields": {"employment_status": "Active"},
    })
    _write(day / "vendor_corelogic" / "avm_LOAN-101.json", {
        "document_id": "CL-AVM-001",
        "document_type": "AVM_REPORT",
        "category": "property",
        "los_id": "LOAN-101",
        "received_at": "2026-01-01T15:15:00Z",
        "extracted_fields": {"avm_value": 450000},
    })
    _write(day / "ai_chat" / "chat_LOAN-101.json", {
        "document_id": "CHAT-001",
        "document_type": "CREDIT_EXPLANATION",
        "category": "credit",
        "los_id": "LOAN-101",
        "received_at": "2026-01-01T19:15:00Z",
        "extracted_fields": {},
    })

    # 2. Batch JSON array — must explode into N docs.
    _write(day / "los_encompass" / "LOAN-101_batch_2026-01-01.json", [
        {"document_id": "ENC-URLA-001",   "document_type": "URLA_1003",
         "category": "loan_terms", "los_id": "LOAN-101",
         "received_at": "2026-01-01T10:15:00Z", "extracted_fields": {}},
        {"document_id": "ENC-CREDIT-001", "document_type": "CREDIT_REPORT",
         "category": "credit", "los_id": "LOAN-101",
         "received_at": "2026-01-01T10:15:00Z", "extracted_fields": {}},
        {"document_id": "ENC-AUS-001",    "document_type": "AUS_DU_FINDINGS",
         "category": "vendor", "los_id": "LOAN-101",
         "received_at": "2026-01-01T10:15:00Z", "extracted_fields": {}},
    ])

    # 3. Meta-pair channels — raw .pdf + _meta.json. Connector reads
    # ONLY the meta.json, attaches an evidence_file hint to the sibling
    # binary.
    for ch in ("email_inbox", "borrower_portal", "vendor_title"):
        _write(day / ch / f"{ch.upper()}-LOAN101.pdf",
               b"%PDF-1.4 stub")  # bytes don't matter
        _write(day / ch / f"{ch.upper()}-LOAN101_meta.json", {
            "document_id":   f"{ch.upper()}-LOAN101",
            "document_type": "PURCHASE_AGREEMENT" if ch == "email_inbox"
                              else ("DRIVERS_LICENSE" if ch == "borrower_portal"
                                    else "TITLE_COMMITMENT"),
            "category":      "property" if ch == "vendor_title"
                              else ("identity" if ch == "borrower_portal" else "loan_terms"),
            "los_id":        "LOAN-101",
            "received_at":   "2026-01-01T14:15:00Z",
            "extracted_fields": {"k": "v"},
        })

    # 4. Raw-scan channel — no metadata. Connector synthesises an
    # UNKNOWN doc per binary file.
    _write(day / "shared_drive" / "scan_20260101-1015.pdf", b"%PDF-1.4 stub")

    return root


def _pull(root: Path) -> list[dict]:
    c = S3EDMSConnector(str(root), _StubPG())
    return asyncio.run(c.pull_documents_since("1970-01-01T00:00:00Z"))


def test_individual_json_channels_yield_one_doc_per_file(v2_root: Path):
    docs = _pull(v2_root)
    by_chan = {}
    for d in docs:
        by_chan.setdefault(d.get("source_channel"), []).append(d)
    for chan in ("edms_pull", "vendor_equifax", "vendor_corelogic", "ai_chat"):
        assert len(by_chan.get(chan, [])) == 1, (
            f"channel {chan} expected 1 doc, got {len(by_chan.get(chan, []))}"
        )


def test_batch_arrays_are_exploded(v2_root: Path):
    docs = _pull(v2_root)
    enc = [d for d in docs if d.get("source_channel") == "los_encompass"]
    assert len(enc) == 3, "los_encompass batch JSON should explode into 3 docs"
    types = sorted(d["document_type"] for d in enc)
    assert types == ["AUS_DU_FINDINGS", "CREDIT_REPORT", "URLA_1003"]


def test_meta_pair_channels_read_meta_only_with_evidence_hint(v2_root: Path):
    docs = _pull(v2_root)
    for chan in ("email_inbox", "borrower_portal", "vendor_title"):
        rows = [d for d in docs if d.get("source_channel") == chan]
        assert len(rows) == 1, (
            f"{chan} should yield exactly 1 doc (one pair) — got {len(rows)}"
        )
        d = rows[0]
        assert d.get("evidence_file"), f"{chan} doc missing evidence_file hint"
        assert d["evidence_file"].endswith(".pdf"), (
            f"{chan} evidence hint should point at the raw .pdf, "
            f"got {d['evidence_file']!r}"
        )
        # The .pdf binary itself must NOT be parsed as JSON; the only
        # JSON we read is the _meta.json. Validate the read succeeded
        # by checking a known field landed.
        assert d.get("extracted_fields", {}).get("k") == "v"


def test_shared_drive_synthesizes_unclassified_doc(v2_root: Path):
    docs = _pull(v2_root)
    scans = [d for d in docs if d.get("source_channel") == "shared_drive"]
    assert len(scans) == 1
    s = scans[0]
    assert s["document_type"] == "UNKNOWN"
    assert s["los_id"] == "UNCLASSIFIED"
    assert s["requires_classification"] is True
    assert s["status"] == "pending_classification"
    assert s["received_at"].startswith("2026-01-01T")
    assert s["evidence_file"].endswith(".pdf")


def test_v1_legacy_layout_falls_back_to_recursive_scan(tmp_path: Path):
    """A date folder without any known-channel sub-dir is treated as
    legacy: every .json under it is read as a single doc."""
    root = tmp_path / "s3_simulation"
    _write(root / "2026-01-01" / "LOS-001" / "W2.json", {
        "document_id":      "LEGACY-W2-001",
        "document_type":    "W2_CURRENT",
        "applicant_id":     "APL-001-P",
        "received_at":      "2026-01-01T09:00:00Z",
        "extracted_fields": {},
    })
    _write(root / "2026-01-01" / "LOS-002" / "PAYSTUB.json", {
        "document_id":      "LEGACY-PS-001",
        "document_type":    "PAYSTUB_CURRENT",
        "applicant_id":     "APL-002-P",
        "received_at":      "2026-01-01T10:00:00Z",
        "extracted_fields": {},
    })
    docs = _pull(root)
    assert len(docs) == 2
    assert all(d.get("source_channel") == "legacy" for d in docs)


def test_watermark_filter_excludes_pre_window_docs(v2_root: Path):
    """A watermark mid-day must drop earlier docs."""
    c = S3EDMSConnector(str(v2_root), _StubPG())
    docs = asyncio.run(c.pull_documents_since("2026-01-01T12:00:00Z"))
    received = [d.get("received_at") for d in docs]
    # Only the 14:15 (meta-pair) and 15:15 / 19:15 (corelogic + ai_chat)
    # docs should remain. The 09:15, 10:15, 11:15 ones are filtered out.
    assert all(r > "2026-01-01T12:00:00Z" for r in received), received


def test_v4_pull_cap_returns_first_n_and_no_eod_marker(tmp_path: Path):
    """v4.4 — when a folder carries more docs than the per-build cap,
    the connector returns the first N (sorted by received_at) and
    does NOT append an EOD marker — the watermark advances mid-day so
    the next build re-picks the same folder."""
    from core.connectors.s3_connector import _PULL_CAP_PER_BUILD
    root = tmp_path / "s3_simulation_v4"
    day  = root / "2026-01-01"
    # 250 docs is comfortably > the 200 cap.
    for i in range(_PULL_CAP_PER_BUILD + 50):
        _write(day / "edms_pull" / f"EDMS-{i:04d}.json", {
            "document_id":      f"EDMS-{i:04d}",
            "document_type":    "W2_CURRENT",
            "category":         "income",
            "los_id":           "LOAN-101",
            "borrower_role":    "primary",
            "received_at":      f"2026-01-01T{(i // 60) % 24:02d}:{i % 60:02d}:00Z",
            "extracted_fields": {},
        })
    c = S3EDMSConnector(str(root), _StubPG())
    docs = asyncio.run(c.pull_documents_since("1970-01-01T00:00:00Z"))
    assert len(docs) == _PULL_CAP_PER_BUILD, (
        f"expected exactly {_PULL_CAP_PER_BUILD} docs after the cap, "
        f"got {len(docs)}"
    )
    assert not any(d.get("_eod_marker") for d in docs), (
        "EOD marker must not appear when the cap was hit — wm should "
        "stay mid-day so the next build re-picks the same folder"
    )
    # Sorted ascending by received_at — the LATEST returned doc is the
    # 200th (mid-day, not end-of-day).
    received = [d["received_at"] for d in docs]
    assert received == sorted(received), "docs must be sorted"


def test_v4_eod_marker_appended_when_folder_drained_with_more_folders(tmp_path: Path):
    """v4.4 — when a folder has <= cap docs AND there's a later folder
    in window, the connector appends an in-band ``_eod_marker`` with
    ``received_at`` = start of next day so the builder advances the
    watermark past the drained folder. Without the marker the picker
    would re-loop on the same exhausted day forever."""
    root = tmp_path / "s3_simulation_v4_eod"
    # Two date folders; pull from epoch picks day-1 and should mark
    # it as drained so the next build moves to day-2.
    _write(root / "2026-01-01" / "edms_pull" / "DOC-001.json", {
        "document_id":      "DOC-001",
        "document_type":    "W2_CURRENT",
        "category":         "income",
        "los_id":           "LOAN-101",
        "borrower_role":    "primary",
        "received_at":      "2026-01-01T09:15:00Z",
        "extracted_fields": {},
    })
    _write(root / "2026-01-02" / "edms_pull" / "DOC-002.json", {
        "document_id":      "DOC-002",
        "document_type":    "W2_CURRENT",
        "category":         "income",
        "los_id":           "LOAN-101",
        "borrower_role":    "primary",
        "received_at":      "2026-01-02T09:15:00Z",
        "extracted_fields": {},
    })
    c = S3EDMSConnector(str(root), _StubPG())
    docs = asyncio.run(c.pull_documents_since("1970-01-01T00:00:00Z"))
    real_docs = [d for d in docs if not d.get("_eod_marker")]
    markers   = [d for d in docs if d.get("_eod_marker")]
    assert len(real_docs) == 1, (
        f"expected exactly one real doc (day-1), got {len(real_docs)}"
    )
    assert real_docs[0]["document_id"] == "DOC-001"
    assert len(markers) == 1, (
        f"expected one EOD marker when folder drained with more folders ahead, "
        f"got {len(markers)}"
    )
    assert markers[0]["received_at"].startswith("2026-01-02T00:00:00"), (
        f"marker received_at must be start of next day; got {markers[0]['received_at']}"
    )


def test_v4_eod_marker_NOT_appended_on_last_folder(tmp_path: Path):
    """v4.4 — when the drained folder is the last one in window, do
    NOT append a marker. Let the catch-up loop see the empty pull on
    the next build and stop naturally."""
    root = tmp_path / "s3_simulation_v4_last"
    _write(root / "2026-01-01" / "edms_pull" / "DOC-001.json", {
        "document_id":      "DOC-001",
        "document_type":    "W2_CURRENT",
        "category":         "income",
        "los_id":           "LOAN-101",
        "borrower_role":    "primary",
        "received_at":      "2026-01-01T09:15:00Z",
        "extracted_fields": {},
    })
    c = S3EDMSConnector(str(root), _StubPG())
    docs = asyncio.run(c.pull_documents_since("1970-01-01T00:00:00Z"))
    assert not any(d.get("_eod_marker") for d in docs), (
        "no marker should be appended when there are no folders ahead"
    )


def test_sibling_pdf_files_are_ignored_by_dispatch(v2_root: Path):
    """``edms_pull/`` and ``los_encompass/`` may carry sibling raw
    ``.pdf`` evidence files alongside the JSON record (added by
    ``generate_realworld_simulation.py`` for format-aware doc types).
    The connector must read only the JSON and skip the binaries —
    otherwise the count would double when the renderer runs."""
    edms_dir = v2_root / "2026-01-01" / "edms_pull"
    # Drop a sibling .pdf next to the existing JSON record.
    fake_pdf = edms_dir / "EDMS-W2-001.pdf"
    fake_pdf.write_bytes(b"%PDF-1.4 stub")

    c = S3EDMSConnector(str(v2_root), _StubPG())
    docs = asyncio.run(c.pull_documents_since("1970-01-01T00:00:00Z"))
    edms_docs = [d for d in docs if d.get("source_channel") == "edms_pull"]
    # Still exactly 1 doc — the sibling .pdf must NOT have been parsed
    # as a JSON record.
    assert len(edms_docs) == 1, (
        f"sibling .pdf leaked into the pull (found {len(edms_docs)} edms docs)"
    )
    assert edms_docs[0]["document_id"] == "EDMS-W2-001"
