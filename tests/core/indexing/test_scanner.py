"""S3Scanner tests — local-storage backed."""
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from core.indexing.s3_scanner import S3Scanner


def _set_mtime(path: Path, dt: datetime) -> None:
    ts = dt.timestamp()
    os.utime(path, (ts, ts))


def _drop(root: Path, key: str, body: bytes = b"%PDF-1.4 fake") -> Path:
    target = root / key
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(body)
    return target


def test_scanner_skips_old_files(tmp_path):
    root = tmp_path
    f = _drop(root, "loans/LOS-001/income/w2_current.pdf")
    _set_mtime(f, datetime(2026, 1, 1, tzinfo=timezone.utc))

    scanner = S3Scanner(use_local=True, local_path=str(root))
    since = datetime(2026, 5, 1, tzinfo=timezone.utc)
    docs = scanner.scan_new(since=since)
    assert docs == []


def test_scanner_returns_new_files(tmp_path):
    root = tmp_path
    f = _drop(root, "loans/LOS-001/income/w2_current.pdf")
    _set_mtime(f, datetime(2026, 6, 1, tzinfo=timezone.utc))

    scanner = S3Scanner(use_local=True, local_path=str(root))
    since = datetime(2026, 5, 1, tzinfo=timezone.utc)
    docs = scanner.scan_new(since=since)
    assert len(docs) == 1
    d = docs[0]
    assert d.los_id == "LOS-001"
    assert d.category == "income"
    assert d.filename == "w2_current.pdf"
    assert d.doc_type == "W2_CURRENT"


def test_scanner_groups_by_los(tmp_path):
    root = tmp_path
    fa = _drop(root, "loans/LOS-001/income/w2_current.pdf")
    fb = _drop(root, "loans/LOS-001/asset/bank_statement.pdf")
    fc = _drop(root, "loans/LOS-002/property/appraisal_urar.pdf")
    later = datetime(2026, 6, 1, tzinfo=timezone.utc)
    for f in (fa, fb, fc):
        _set_mtime(f, later)

    scanner = S3Scanner(use_local=True, local_path=str(root))
    since = datetime(2026, 5, 1, tzinfo=timezone.utc)
    docs = scanner.scan_new(since=since)
    groups = scanner.group_by_los(docs)
    assert set(groups.keys()) == {"LOS-001", "LOS-002"}
    assert len(groups["LOS-001"]) == 2
    assert len(groups["LOS-002"]) == 1


def test_scanner_filename_doc_types(tmp_path):
    root = tmp_path
    cases = [
        ("loans/L1/income/w2_current.pdf",      "W2_CURRENT"),
        ("loans/L1/income/paystub_current.pdf", "PAYSTUB_CURRENT"),
        ("loans/L1/asset/bank_statement.pdf",   "BANK_STATEMENT_M1"),
        ("loans/L1/credit/credit_report.pdf",   "CREDIT_REPORT"),
        ("loans/L1/property/appraisal_urar.pdf", "APPRAISAL_URAR"),
        ("loans/L1/property/hoi_binder.pdf",    "HOI_BINDER"),
        ("loans/L1/property/flood_cert.pdf",    "FLOOD_CERT"),
    ]
    later = datetime(2026, 6, 1, tzinfo=timezone.utc)
    for key, _ in cases:
        f = _drop(root, key)
        _set_mtime(f, later)

    scanner = S3Scanner(use_local=True, local_path=str(root))
    docs = scanner.scan_new(since=datetime(2026, 5, 1, tzinfo=timezone.utc))
    by_filename = {d.filename: d.doc_type for d in docs}
    for key, expected in cases:
        filename = key.split("/")[-1]
        assert by_filename[filename] == expected
