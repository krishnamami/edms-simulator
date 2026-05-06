#!/usr/bin/env python3
"""simulate_s3_edms.py — drop files into the S3 / local_storage layout
that the incremental indexer scans, then trigger /indexing/run and
show the deltas.

Behaviour matches the production indexer: only files modified after
the watermark are processed.

Usage:
    python scripts/simulate_s3_edms.py
        Drops all docs for one synthetic loan, triggers the indexer,
        prints applicants_affected.

    python scripts/simulate_s3_edms.py --watch
        Drops new docs every 30s across multiple loans for ~5 minutes.

    python scripts/simulate_s3_edms.py --dry-run
        Hits POST /indexing/run with dry_run=true — shows what *would*
        be indexed without touching document_index.

The script does NOT touch real AWS S3; everything lands in the
local_storage path that the local docker-compose API reads from.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import date
from pathlib import Path
from typing import Optional

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.documents.generators.bank_stmt_generator import generate_bank_statement  # noqa: E402
from core.documents.generators.paystub_generator import generate_paystub  # noqa: E402
from core.documents.generators.w2_generator import generate_w2  # noqa: E402
from core.property.generators.appraisal_generator import generate_appraisal  # noqa: E402
from core.property.generators.flood_cert_generator import generate_flood_cert  # noqa: E402
from core.property.generators.hoi_generator import generate_hoi_binder  # noqa: E402

GREEN = "\033[92m"
YELLOW = "\033[93m"
CYAN = "\033[96m"
RED = "\033[91m"
BOLD = "\033[1m"
RESET = "\033[0m"


def _api_url() -> str:
    return os.getenv("EDMS_API_URL", "http://localhost:8001")


def _api_key() -> str:
    return os.getenv("EDMS_API_KEY", "edms_dev_key")


def _local_loans_root() -> Path:
    base = Path(os.getenv("LOCAL_STORAGE_PATH", "./local_storage"))
    return base / "loans"


# ---------------------------------------------------------------------------


def banner(msg: str) -> None:
    print(f"\n{BOLD}{YELLOW}═══ {msg} ═══{RESET}")


def info(msg: str) -> None:
    print(f"  {CYAN}     {RESET} {msg}")


def ok(msg: str) -> None:
    print(f"  {GREEN}[OK]{RESET}  {msg}")


def fail(msg: str) -> None:
    print(f"  {RED}[!!]{RESET}  {msg}")


# ---------------------------------------------------------------------------


def drop_file(los_id: str, category: str, filename: str, body: bytes) -> str:
    """Write a file into local_storage/loans/{los_id}/{category}/{filename}.

    Returns the relative S3-style key (``loans/...``).
    """
    base = _local_loans_root()
    target_dir = base / los_id / category
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / filename
    target.write_bytes(body)
    key = f"loans/{los_id}/{category}/{filename}"
    info(f"dropped {key}  ({len(body):,} bytes)")
    return key


def ensure_application(client: httpx.Client, los_id: str,
                        first_name: str, last_name: str,
                        ssn_last4: str, dob: str = "1985-01-01") -> dict:
    """Create the application via POST /loans if it doesn't exist."""
    r = client.get(f"/loan/{los_id}/applicant-id")
    if r.status_code == 200:
        body = r.json()
        info(f"application exists: {body['application_id']}")
        return body
    payload = {
        "los_id": los_id,
        "borrower": {
            "first_name": first_name, "last_name": last_name,
            "dob": dob,
            "ssn_hash": f"sha256:sim-{los_id}",
            "ssn_last4": ssn_last4,
            "email": f"{first_name.lower()}.{last_name.lower()}@example.com",
        },
        "loan": {"loan_amount": 320_000, "credit_band": "near-prime"},
        "documents": [],
    }
    r = client.post("/loans", json=payload)
    r.raise_for_status()
    body = r.json()
    ok(f"created application {body['application_id']} applicant={body['applicant_id']}")
    return body


def trigger_indexing(client: httpx.Client, dry_run: bool = False) -> dict:
    r = client.post("/indexing/run", json={"source": "s3", "dry_run": dry_run})
    r.raise_for_status()
    stats = r.json()
    info(
        f"indexer: found={stats.get('found')}  processed={stats.get('processed')}  "
        f"skipped={stats.get('skipped')}  affected={stats.get('applicants_affected')}  "
        f"errors={stats.get('errors')}"
    )
    return stats


def print_status(client: httpx.Client) -> None:
    r = client.get("/indexing/status")
    if r.status_code == 200:
        body = r.json()
        info(
            f"status: last_indexed_at={body.get('last_indexed_at')}  "
            f"status={body.get('status')}"
        )


# ---------------------------------------------------------------------------


def scenario_single(client: httpx.Client, dry_run: bool) -> None:
    los_id = "LOS-SIM-001"
    banner("Scenario 1 — single loan, all docs at once")
    ensure_application(
        client, los_id, "Alice", "Anderson", "0001"
    )

    pdf, _ = generate_w2(
        employee_name="Alice Anderson", employee_ssn_last4="0001",
        employee_address="100 Main St", employer_name="Acme Inc",
        employer_ein="123456789", employer_address="1 Corp Way",
        tax_year=date.today().year - 1, box1_wages=92_400,
    )
    drop_file(los_id, "income", "w2_current.pdf", pdf)

    pdf, _ = generate_paystub(
        employee_name="Alice Anderson",
        employee_address="100 Main St",
        employer_name="Acme Inc",
        employer_address="1 Corp Way",
        pay_period_start="2025-01-01",
        pay_period_end="2025-01-15",
        pay_date="2025-01-20",
        gross_pay=3_846,
        ytd_gross=15_385,
    )
    drop_file(los_id, "income", "paystub_current.pdf", pdf)

    pdf, _ = generate_bank_statement(
        bank_name="Chase",
        account_holder="Alice Anderson",
        account_number_masked="****1234",
        months=["2025-01"],
        opening_balance=10_000,
        closing_balance=12_500,
    )
    drop_file(los_id, "asset", "bank_statement_jan.pdf", pdf)

    print_status(client)
    banner("Trigger indexer")
    trigger_indexing(client, dry_run=dry_run)
    print_status(client)


def scenario_property(client: httpx.Client, dry_run: bool) -> None:
    los_id = "LOS-SIM-002"
    banner("Scenario 2 — second loan, property docs only")
    ensure_application(
        client, los_id, "Bob", "Brown", "0002", dob="1980-06-15"
    )
    pdf, _ = generate_appraisal(
        property_address="500 Elm St\nAustin TX 78701",
        appraised_value=525_000, condition_rating="C2",
    )
    drop_file(los_id, "property", "appraisal_urar.pdf", pdf)
    pdf, _ = generate_hoi_binder(
        insured_name="Bob Brown",
        property_address="500 Elm St\nAustin TX 78701",
        annual_premium=1_650,
    )
    drop_file(los_id, "property", "hoi_binder.pdf", pdf)
    pdf, _ = generate_flood_cert(
        property_address="500 Elm St\nAustin TX 78701",
        flood_zone="X",
    )
    drop_file(los_id, "property", "flood_cert.pdf", pdf)

    banner("Trigger indexer (Alice's docs were already indexed — should be skipped)")
    stats = trigger_indexing(client, dry_run=dry_run)
    if stats.get("applicants_affected") == 1:
        ok("only Bob was re-assembled — incremental run worked")
    else:
        info(f"applicants_affected={stats.get('applicants_affected')} "
             f"(expected 1 — Alice's docs predate the watermark and were skipped)")


def scenario_watch(client: httpx.Client) -> None:
    """Drop one new doc every 30 seconds for ~5 minutes."""
    banner("--watch mode: dropping a new doc every 30s for ~5 minutes")
    los_ids = ["LOS-SIM-WATCH-A", "LOS-SIM-WATCH-B", "LOS-SIM-WATCH-C"]
    for i, los_id in enumerate(los_ids):
        ensure_application(
            client, los_id,
            ["Cara", "Devon", "Eli"][i],
            "Watcher", f"00{10+i}",
        )

    rounds = 0
    max_rounds = 10
    while rounds < max_rounds:
        rounds += 1
        target_los = los_ids[rounds % len(los_ids)]
        pdf, _ = generate_w2(
            employee_name=f"Round {rounds}",
            employee_ssn_last4="9999",
            employee_address="X", employer_name=f"Acme R{rounds}",
            employer_ein="123456789", employer_address="X",
            tax_year=date.today().year - 1,
            box1_wages=80_000 + rounds * 1_000,
        )
        drop_file(target_los, "income", f"w2_round{rounds}.pdf", pdf)
        info(f"round {rounds} → triggering indexer")
        trigger_indexing(client)
        print_status(client)
        time.sleep(30)


# ---------------------------------------------------------------------------


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--watch", action="store_true",
                    help="continuously drop new docs every 30s")
    ap.add_argument("--dry-run", action="store_true",
                    help="trigger indexer with dry_run=true (no document_index writes)")
    args = ap.parse_args()

    print(f"{BOLD}simulate_s3_edms{RESET}  api={_api_url()}  storage={_local_loans_root()}")

    headers = {"X-API-Key": _api_key()}
    with httpx.Client(base_url=_api_url(), headers=headers, timeout=60) as client:
        try:
            if args.watch:
                scenario_watch(client)
            else:
                scenario_single(client, dry_run=args.dry_run)
                # Pause briefly so the second scenario's files have a
                # clearly later mtime than the watermark we just set.
                time.sleep(2)
                scenario_property(client, dry_run=args.dry_run)
        except httpx.HTTPError as exc:
            fail(f"HTTP error: {exc}")
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
