#!/usr/bin/env python3
"""watch_pipeline.py — show data flowing through every storage layer.

Usage:
    python scripts/watch_pipeline.py
        # default: hit local API at http://localhost:8001 with edms_dev_key

    python scripts/watch_pipeline.py --live
        # production ALB; pulls API key from edms/api/keys via boto3

    python scripts/watch_pipeline.py --applicant APL-00001-P
        # observe an existing applicant; skip the upload step

The script generates a W2 PDF (Phase B reportlab), posts it to
``POST /ingest/pdf``, then prints what landed in S3 + raw_ingestion +
document_index + document_relationships + income_profiles + Redis.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.documents.generators.w2_generator import generate_w2  # noqa: E402


GREEN, YELLOW, CYAN, MAGENTA, BOLD, RESET = (
    "\033[92m", "\033[93m", "\033[96m", "\033[95m", "\033[1m", "\033[0m"
)


def step(n: int, title: str) -> None:
    print(f"\n{BOLD}{YELLOW}{'='*72}{RESET}")
    print(f"{BOLD}{YELLOW}  STEP {n}: {title}{RESET}")
    print(f"{BOLD}{YELLOW}{'='*72}{RESET}")


def info(msg: str) -> None:
    print(f"  {CYAN}      {RESET} {msg}")


def ok(msg: str) -> None:
    print(f"  {GREEN}[PASS]{RESET} {msg}")


def warn(msg: str) -> None:
    print(f"  {YELLOW}[NOTE]{RESET} {msg}")


def show_json(label: str, data, max_lines: int = 40) -> None:
    print(f"\n  {CYAN}{label}:{RESET}")
    lines = json.dumps(data, indent=2, default=str).split("\n")
    for line in lines[:max_lines]:
        print(f"    {line}")
    if len(lines) > max_lines:
        print(f"    ... ({len(lines)-max_lines} more lines)")


def get_api_key(live: bool) -> str:
    if not live:
        return "edms_dev_key"
    import boto3
    import os as _os
    _os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    secret = boto3.client("secretsmanager").get_secret_value(
        SecretId="edms/api/keys"
    )
    return json.loads(secret["SecretString"])["decision_os_api_key"]


def get_url(live: bool) -> str:
    if live:
        return "http://edms-simulator-alb-1374683374.us-east-1.elb.amazonaws.com"
    return "http://localhost:8001"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true", help="hit production ALB")
    ap.add_argument("--applicant", help="observe an existing applicant_id (skip upload)")
    args = ap.parse_args()

    url = get_url(args.live)
    api_key = get_api_key(args.live)
    headers = {"X-API-Key": api_key}

    print(f"\n{BOLD}watch_pipeline — {('LIVE prod' if args.live else 'local')} @ {url}{RESET}")

    # Health check first.
    r = httpx.get(f"{url}/health", timeout=10)
    r.raise_for_status()
    ok(f"API alive: {r.json()}")

    # ──────────────────────────────────────────────────────────
    # STEP 0: Generate (or skip if --applicant)
    # ──────────────────────────────────────────────────────────
    if args.applicant:
        applicant_id = args.applicant
        ingest_id = None
        raw_s3_key = None
        warn(f"--applicant set; skipping upload. Observing {applicant_id}.")
    else:
        step(1, "Generate W2 PDF + POST /ingest/pdf")
        pdf_bytes, w2_meta = generate_w2(
            employee_name="James Okafor",
            employee_ssn_last4="4729",
            employee_address="100 Mission St\nSan Francisco, CA 94105",
            employer_name="Accenture LLC",
            employer_ein="123456789",
            employer_address="1 Corporate Way",
            tax_year=date.today().year - 1,
            box1_wages=92400.00,
        )
        info(f"generated W2 PDF ({len(pdf_bytes):,} bytes)")
        info(f"  box1_wages = ${w2_meta['box1_wages']:,.2f}")
        info(f"  employer   = {w2_meta['employer_name']}")

        applicant_id = "APL-WATCH-DEMO"
        files = {"file": ("w2.pdf", pdf_bytes, "application/pdf")}
        r = httpx.post(
            f"{url}/ingest/pdf",
            files=files,
            data={"applicant_id": applicant_id, "borrower_role": "primary"},
            headers=headers,
            timeout=60,
        )
        r.raise_for_status()
        body = r.json()
        ingest_id = body["ingest_id"]
        raw_s3_key = body["raw_s3_key"]
        ok(f"ingest_id={ingest_id}")
        info(f"raw_s3_key={raw_s3_key}")
        info(f"document_type_detected={body.get('document_type')}")
        info(f"confidence={body.get('confidence')}")

    # ──────────────────────────────────────────────────────────
    # STEP 2: raw_ingestion row
    # ──────────────────────────────────────────────────────────
    step(2, f"GET /ingest/{ingest_id}/raw" if ingest_id
            else "GET /applicant/.../raw-ingestion (existing applicant)")

    if ingest_id:
        r = httpx.get(f"{url}/ingest/{ingest_id}/raw", headers=headers, timeout=10)
        r.raise_for_status()
        show_json("raw_ingestion row", r.json(), max_lines=30)

    # ──────────────────────────────────────────────────────────
    # STEP 3: pipeline state for the applicant
    # ──────────────────────────────────────────────────────────
    step(3, f"GET /applicant/{applicant_id}/raw-ingestion — pipeline rollup")
    r = httpx.get(
        f"{url}/applicant/{applicant_id}/raw-ingestion",
        headers=headers, timeout=10,
    )
    r.raise_for_status()
    pipeline_body = r.json()
    show_json("pipeline_state", pipeline_body["pipeline_state"], max_lines=20)
    info(f"ingestions count: {len(pipeline_body['ingestions'])}")

    # ──────────────────────────────────────────────────────────
    # STEP 4: graph summary
    # ──────────────────────────────────────────────────────────
    step(4, f"GET /applicant/{applicant_id}/graph/summary")
    r = httpx.get(
        f"{url}/applicant/{applicant_id}/graph/summary",
        headers=headers, timeout=15,
    )
    if r.status_code == 200:
        show_json("graph summary", r.json(), max_lines=15)
    else:
        warn(f"graph endpoint returned HTTP {r.status_code}: {r.text[:200]}")

    # ──────────────────────────────────────────────────────────
    # STEP 5: income / credit profile
    # ──────────────────────────────────────────────────────────
    step(5, f"GET /applicant/{applicant_id}/income-profile")
    r = httpx.get(
        f"{url}/applicant/{applicant_id}/income-profile",
        headers=headers, timeout=10,
    )
    if r.status_code == 200:
        show_json("income profile", r.json(), max_lines=20)
    else:
        warn(f"no income profile yet (HTTP {r.status_code})")

    # ──────────────────────────────────────────────────────────
    # STEP 6: pipeline conflicts (if any)
    # ──────────────────────────────────────────────────────────
    step(6, f"GET /applicant/{applicant_id}/conflicts")
    r = httpx.get(
        f"{url}/applicant/{applicant_id}/conflicts",
        headers=headers, timeout=10,
    )
    if r.status_code == 200:
        body = r.json()
        if body.get("conflict_count", 0) > 0:
            warn(f"{body['conflict_count']} conflicts present")
            show_json("conflicts", body, max_lines=20)
        else:
            ok("no conflicts")
    else:
        warn(f"conflicts endpoint returned {r.status_code}")

    # ──────────────────────────────────────────────────────────
    # STEP 7: failed pipeline rollup (admin)
    # ──────────────────────────────────────────────────────────
    step(7, "GET /pipeline/failed — system-wide failures")
    r = httpx.get(f"{url}/pipeline/failed", headers=headers, timeout=10)
    if r.status_code == 200:
        body = r.json()
        if body.get("count", 0) == 0:
            ok("no failed ingestions across the system")
        else:
            warn(f"{body['count']} failed ingestion(s) need attention")
            show_json("failed sample", body, max_lines=20)
    else:
        warn(f"pipeline/failed returned {r.status_code}")

    print(f"\n{BOLD}{GREEN}done.{RESET}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
