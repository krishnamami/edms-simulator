"""CSV bulk-upload adapter.

Broker uploads arrive with inconsistent header casing — this adapter
normalizes via an alias map, then emits one NormalizedIngestEvent per row
plus a validation report. Per-row errors are collected without halting.
"""
from __future__ import annotations

import csv
import io
from typing import Optional

from core.ingestion.events import ChannelType, NormalizedIngestEvent


HEADER_ALIASES: dict[str, list[str]] = {
    "first_name":     ["firstname", "fname", "first", "givenname"],
    "last_name":      ["lastname", "lname", "last", "surname"],
    "dob":            ["dob", "dateofbirth", "birthdate", "birthday"],
    "ssn_last4":      ["ssnlast4", "ssn4", "lastfourssn"],
    "email":          ["email", "emailaddress"],
    "phone":          ["phone", "phonenumber", "mobile", "cell"],
    "employer":       ["employer", "company", "employername"],
    "annual_income":  ["annualincome", "income", "salary", "annualsalary"],
    "los_id":         ["losid", "loanid", "applicationid", "los"],
}

REQUIRED_FIELDS = ["first_name", "last_name"]


def _norm(s: str) -> str:
    return "".join(c for c in s.lower() if c.isalnum())


def _build_header_map(raw_headers: list[str]) -> dict[int, str]:
    """Map column index -> canonical field name (or None if unrecognized)."""
    canonical_for: dict[str, str] = {}
    for canonical, aliases in HEADER_ALIASES.items():
        for a in aliases + [canonical]:
            canonical_for[_norm(a)] = canonical

    out: dict[int, str] = {}
    for idx, hdr in enumerate(raw_headers):
        key = canonical_for.get(_norm(hdr))
        if key:
            out[idx] = key
    return out


def adapt(
    csv_bytes: bytes,
) -> tuple[list[NormalizedIngestEvent], dict]:
    text = csv_bytes.decode("utf-8-sig", errors="replace")
    reader = csv.reader(io.StringIO(text))

    rows = list(reader)
    if not rows:
        return [], {"processed": 0, "failed": 0, "errors": []}

    header_map = _build_header_map(rows[0])
    events: list[NormalizedIngestEvent] = []
    errors: list[dict] = []
    processed = 0
    failed = 0

    for row_num, row in enumerate(rows[1:], start=2):
        record: dict = {}
        for idx, value in enumerate(row):
            field = header_map.get(idx)
            if field:
                record[field] = value.strip() if isinstance(value, str) else value

        missing = [f for f in REQUIRED_FIELDS if not record.get(f)]
        if missing:
            failed += 1
            errors.append({"row": row_num, "reason": f"missing required: {missing}"})
            continue

        # Coerce annual_income to float when present
        if record.get("annual_income"):
            try:
                record["annual_income"] = float(
                    str(record["annual_income"]).replace(",", "").replace("$", "")
                )
            except (TypeError, ValueError):
                failed += 1
                errors.append({
                    "row": row_num,
                    "reason": f"annual_income not numeric: {record['annual_income']!r}",
                })
                continue

        signals = {
            "first_name": record.get("first_name"),
            "last_name": record.get("last_name"),
            "dob": record.get("dob"),
            "ssn_last4": record.get("ssn_last4"),
            "email": record.get("email"),
            "phone": record.get("phone"),
            "los_id": record.get("los_id"),
            "role": "primary",
        }
        events.append(NormalizedIngestEvent(
            source_channel=ChannelType.CSV_BATCH,
            applicant_signals=signals,
            extracted_fields=record,
            confidence=0.85,
            requires_verification=False,
        ))
        processed += 1

    report = {
        "processed": processed,
        "failed": failed,
        "errors": errors,
    }
    return events, report
