#!/usr/bin/env python3
"""watch_pipeline.py — show data flowing through every storage layer.

Usage:
    python scripts/watch_pipeline.py
        Default: hit local API at http://localhost:8001 with edms_dev_key.
        Generates a W2 PDF, uploads it, and walks the pipeline.

    python scripts/watch_pipeline.py --live
        Production ALB; pulls API key from edms/api/keys via boto3.

    python scripts/watch_pipeline.py --applicant APL-00001-P
        Observe an existing applicant; skip the upload step.

    python scripts/watch_pipeline.py --application APP-LOS-PROD-001
        Watch an existing application end-to-end (pipeline-state + timeline).

    python scripts/watch_pipeline.py --upload /path/to/w2.pdf --type W2_CURRENT
        Upload a real PDF (any property or borrower doc) and watch
        extraction + assembly. ``--type`` is the document_type; for
        property docs it routes to /ingest/property-doc, otherwise
        /ingest/pdf.

    python scripts/watch_pipeline.py --full
        Full mortgage scenario: borrower + co-borrower placeholder +
        appraisal + HOI + flood + property tax + AUS findings, with the
        assembled context shown after each step.

Exit code: 0 if every [PASS] line passed, 1 otherwise.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path
from typing import Optional

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from core.documents.generators.w2_generator import generate_w2  # noqa: E402

GREEN, YELLOW, CYAN, MAGENTA, RED, BOLD, RESET = (
    "\033[92m", "\033[93m", "\033[96m", "\033[95m", "\033[91m",
    "\033[1m", "\033[0m",
)

# ---- output helpers --------------------------------------------------------


_FAIL_COUNT = 0


def step(n: int, title: str) -> None:
    bar = "═" * 72
    print(f"\n{BOLD}{YELLOW}{bar}{RESET}")
    print(f"{BOLD}{YELLOW}STEP {n} — {title}{RESET}")
    print(f"{BOLD}{YELLOW}{bar}{RESET}")


def info(msg: str) -> None:
    print(f"  {CYAN}     {RESET} {msg}")


def ok(msg: str) -> None:
    print(f"  {GREEN}[PASS]{RESET} {msg}")


def fail(msg: str) -> None:
    global _FAIL_COUNT
    _FAIL_COUNT += 1
    print(f"  {RED}[FAIL]{RESET} {msg}")


def warn(msg: str) -> None:
    print(f"  {YELLOW}[NOTE]{RESET} {msg}")


def show_json(label: str, data, max_lines: int = 30) -> None:
    print(f"\n  {CYAN}{label}:{RESET}")
    text = json.dumps(data, indent=2, default=str)
    for line in text.split("\n")[:max_lines]:
        print(f"    {line}")
    if text.count("\n") + 1 > max_lines:
        print(f"    ... ({text.count(chr(10))+1-max_lines} more lines)")


# ---- env / connection ------------------------------------------------------


def get_url(live: bool) -> str:
    if live:
        return os.getenv(
            "EDMS_API_URL",
            "http://edms-simulator-alb-1374683374.us-east-1.elb.amazonaws.com",
        )
    return "http://localhost:8001"


def get_api_key(live: bool) -> str:
    if not live:
        return os.getenv("EDMS_API_KEY", "edms_dev_key")
    import boto3
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    secret = boto3.client("secretsmanager").get_secret_value(
        SecretId="edms/api/keys"
    )
    return json.loads(secret["SecretString"])["decision_os_api_key"]


# ---- pipeline summary helpers ---------------------------------------------


def print_context(client: httpx.Client, application_id: str) -> Optional[dict]:
    r = client.get(f"/application/{application_id}/context")
    if r.status_code != 200:
        warn(f"context: HTTP {r.status_code} {r.text[:200]}")
        return None
    body = r.json()
    data = body.get("data") if "data" in body else body
    primary = (data or {}).get("primary") or {}
    prop = (data or {}).get("property") or {}
    readiness = (data or {}).get("readiness") or {}
    print(f"  {CYAN}context:{RESET}")
    print(f"    application_id   = {data.get('application_id')}")
    print(f"    los_id           = {data.get('los_id')}")
    print(
        f"    primary income   = "
        f"${(primary.get('qualifying_monthly') or 0):,.2f}/mo  "
        f"confidence={primary.get('income_confidence'):.2f}  "
        f"verified={primary.get('income_verified')}"
        if primary.get('income_confidence') is not None
        else "    primary income   = (no data)"
    )
    print(
        f"    credit           = mid={primary.get('mid_score')} "
        f"band={primary.get('credit_band')} "
        f"obligations=${(primary.get('monthly_obligations') or 0):,.2f}"
    )
    if prop:
        print(
            f"    property         = appraised="
            f"${(prop.get('appraised_value') or 0):,.0f}  "
            f"piti=${(prop.get('piti_total') or 0):,.0f}  "
            f"zone={prop.get('flood_zone')}"
        )
    print(
        f"    DTI front/back   = "
        f"{data.get('front_end_dti')}% / {data.get('back_end_dti')}%   "
        f"LTV={data.get('ltv')}%"
    )
    print(
        f"    readiness        = aus_ready={readiness.get('aus_ready')}  "
        f"missing={readiness.get('missing_items') or []}"
    )
    print(
        f"    requires_review  = {data.get('requires_review')}"
    )
    return data


def print_pipeline_state(
    client: httpx.Client, application_id: str
) -> Optional[dict]:
    r = client.get(f"/application/{application_id}/pipeline-state")
    if r.status_code != 200:
        warn(f"pipeline-state: HTTP {r.status_code} {r.text[:200]}")
        return None
    state = r.json()
    print(f"  {CYAN}pipeline-state:{RESET}")
    for b in state.get("borrowers", []):
        rk = b.get("redis_keys") or {}
        print(
            f"    {b.get('role')} {b.get('full_name')}: "
            f"{len(b.get('documents', []))} docs  "
            f"income_ttl={rk.get('income',{}).get('ttl_seconds')}  "
            f"credit_ttl={rk.get('credit',{}).get('ttl_seconds')}"
        )
    prop = state.get("property")
    if prop:
        rk = prop.get("redis_key") or {}
        print(
            f"    property {prop.get('property_id')}: "
            f"docs={len(prop.get('documents', []))}  "
            f"profile={prop.get('profile')}  "
            f"redis_ttl={rk.get('ttl_seconds')}"
        )
    g = state.get("graph") or {}
    print(
        f"    graph: nodes={g.get('node_count')} "
        f"edges={g.get('edge_count')} "
        f"conflicts={g.get('conflict_count')}"
    )
    ctx = state.get("context") or {}
    print(
        f"    context: present={ctx.get('present')}  "
        f"ttl={ctx.get('ttl_seconds')}  "
        f"requires_review={ctx.get('requires_review')}"
    )
    return state


# ---- mode handlers ---------------------------------------------------------


def mode_full(client: httpx.Client) -> None:
    """Drive a complete mortgage scenario end-to-end."""
    from core.property.generators.appraisal_generator import generate_appraisal
    from core.property.generators.flood_cert_generator import generate_flood_cert
    from core.property.generators.hoi_generator import generate_hoi_binder
    from core.property.generators.tax_bill_generator import generate_tax_bill

    los_id = "LOS-WATCH-FULL"
    application_id = f"APP-{los_id}"

    step(1, "Create application — POST /loans (borrower layer)")
    body = {
        "los_id": los_id,
        "borrower": {
            "first_name": "James", "last_name": "Okafor",
            "dob": "1985-07-12",
            "ssn_hash": "sha256:watch-pipeline",
            "ssn_last4": "4729",
            "email": "james.okafor@example.com", "phone": "+15555550100",
        },
        "loan": {"loan_amount": 385000, "credit_band": "near-prime"},
        "documents": [],
    }
    r = client.post("/loans", json=body)
    if r.status_code != 200:
        fail(f"POST /loans returned {r.status_code}: {r.text[:200]}")
        return
    created = r.json()
    applicant_id = created["applicant_id"]
    ok(f"primary applicant_id={applicant_id} application_id={created['application_id']}")
    application_id = created["application_id"]

    step(2, "Upload W2 PDF — /ingest/pdf")
    pdf, _meta = generate_w2(
        employee_name="James Okafor",
        employee_ssn_last4="4729",
        employee_address="100 Mission St\nSan Francisco, CA 94105",
        employer_name="Accenture LLC",
        employer_ein="123456789",
        employer_address="1 Corporate Way",
        tax_year=date.today().year - 1,
        box1_wages=92400.00,
    )
    files = {"file": ("w2.pdf", pdf, "application/pdf")}
    r = client.post(
        "/ingest/pdf", files=files,
        data={"applicant_id": applicant_id, "borrower_role": "primary"},
    )
    if r.status_code == 200:
        ok(f"W2 ingested ingest_id={r.json().get('ingest_id')}")
    else:
        fail(f"W2 ingest failed: {r.status_code} {r.text[:200]}")

    step(3, "Create property — POST /properties")
    r = client.post(
        "/properties",
        json={
            "application_id": application_id,
            "address": {
                "line1": "123 Main St", "city": "Austin",
                "state": "TX", "zip_code": "78701",
            },
            "property_type": "single_family",
            "year_built": 2010, "sqft": 2200,
        },
    )
    if r.status_code != 200:
        fail(f"POST /properties failed {r.status_code}: {r.text[:200]}")
        return
    property_id = r.json()["property_id"]
    ok(f"property_id={property_id}")

    step(4, "Upload appraisal — /ingest/property-doc APPRAISAL_URAR")
    appraisal_pdf, _ = generate_appraisal(
        property_address="123 Main St\nAustin, TX 78701",
        appraised_value=485_000, condition_rating="C2",
    )
    r = client.post(
        "/ingest/property-doc",
        files={"file": ("appraisal.pdf", appraisal_pdf, "application/pdf")},
        data={"property_id": property_id, "document_type": "APPRAISAL_URAR"},
    )
    if r.status_code == 200:
        ok(f"appraisal extracted: appraised_value="
           f"${r.json().get('extracted', {}).get('appraised_value', 0):,.0f}")
    else:
        fail(f"appraisal upload failed {r.status_code}: {r.text[:200]}")

    step(5, "Upload HOI binder — /ingest/property-doc HOI_BINDER")
    hoi_pdf, _ = generate_hoi_binder(
        insured_name="James Okafor",
        property_address="123 Main St\nAustin, TX 78701",
        annual_premium=1_800,
    )
    r = client.post(
        "/ingest/property-doc",
        files={"file": ("hoi.pdf", hoi_pdf, "application/pdf")},
        data={"property_id": property_id, "document_type": "HOI_BINDER"},
    )
    if r.status_code == 200:
        ok(f"HOI extracted: annual_premium="
           f"${r.json().get('extracted', {}).get('annual_premium', 0):,.0f}")
    else:
        fail(f"HOI upload failed {r.status_code}: {r.text[:200]}")

    step(6, "Upload flood cert — /ingest/property-doc FLOOD_CERT")
    flood_pdf, _ = generate_flood_cert(
        property_address="123 Main St\nAustin, TX 78701", flood_zone="X",
    )
    r = client.post(
        "/ingest/property-doc",
        files={"file": ("flood.pdf", flood_pdf, "application/pdf")},
        data={"property_id": property_id, "document_type": "FLOOD_CERT"},
    )
    if r.status_code == 200:
        ok(f"flood zone={r.json().get('extracted', {}).get('flood_zone')}")
    else:
        fail(f"flood upload failed {r.status_code}")

    step(7, "Upload property tax bill — PROPERTY_TAX_BILL")
    tax_pdf, _ = generate_tax_bill(
        property_address="123 Main St\nAustin, TX 78701",
        owner_name="James Okafor", annual_tax=7_500, tax_year=2024,
    )
    r = client.post(
        "/ingest/property-doc",
        files={"file": ("tax.pdf", tax_pdf, "application/pdf")},
        data={"property_id": property_id, "document_type": "PROPERTY_TAX_BILL"},
    )
    if r.status_code == 200:
        ok(f"tax annual=${r.json().get('extracted', {}).get('annual_tax', 0):,.0f}")
    else:
        fail(f"tax upload failed {r.status_code}")

    step(8, "Run vendor checks — POST /run-vendor-checks")
    r = client.post(f"/application/{application_id}/run-vendor-checks")
    if r.status_code == 200:
        body = r.json()
        ok(f"vendor checks: {len(body.get('submitted', []))} submitted")
        info(f"readiness.aus_ready={body['readiness'].get('aus_ready')}")
    else:
        fail(f"run-vendor-checks failed {r.status_code}: {r.text[:200]}")

    step(9, "Final context")
    print_context(client, application_id)

    step(10, "Pipeline state + timeline")
    print_pipeline_state(client, application_id)
    r = client.get(f"/application/{application_id}/timeline")
    if r.status_code == 200:
        info(f"timeline events: {len(r.json().get('events') or [])}")


def mode_application(client: httpx.Client, application_id: str) -> None:
    step(1, f"GET /application/{application_id}/pipeline-state")
    print_pipeline_state(client, application_id)
    step(2, f"GET /application/{application_id}/context")
    print_context(client, application_id)
    step(3, f"GET /application/{application_id}/timeline")
    r = client.get(f"/application/{application_id}/timeline")
    if r.status_code == 200:
        events = r.json().get("events") or []
        ok(f"{len(events)} events on timeline")
        for ev in events[-10:]:
            info(f"  {ev.get('timestamp')} [{ev.get('layer')}] {ev.get('description')}")
    else:
        fail(f"timeline failed {r.status_code}: {r.text[:200]}")


def mode_upload(client: httpx.Client, path: str, doc_type: str,
                 applicant_id: Optional[str], property_id: Optional[str]) -> None:
    step(1, f"Read {path}")
    raw = Path(path).read_bytes()
    info(f"size={len(raw):,} bytes; type={doc_type}")

    if doc_type and doc_type.upper() in (
        "APPRAISAL_URAR", "APPRAISAL_UPDATE", "APPRAISAL_DESK",
        "HOI_BINDER", "FLOOD_CERT", "PROPERTY_TAX_BILL", "TITLE_COMMITMENT",
    ):
        if not property_id:
            fail("property doc upload requires --property-id")
            return
        step(2, f"POST /ingest/property-doc {doc_type}")
        r = client.post(
            "/ingest/property-doc",
            files={"file": (Path(path).name, raw, "application/pdf")},
            data={"property_id": property_id, "document_type": doc_type},
        )
    else:
        step(2, "POST /ingest/pdf")
        r = client.post(
            "/ingest/pdf",
            files={"file": (Path(path).name, raw, "application/pdf")},
            data={
                "applicant_id": applicant_id or "",
                "borrower_role": "primary",
            },
        )
    if r.status_code != 200:
        fail(f"upload failed {r.status_code}: {r.text[:200]}")
        return
    show_json("response", r.json())
    ok("upload accepted")


def mode_default(client: httpx.Client, applicant_id: Optional[str]) -> None:
    """Existing single-W2 path — kept for backwards compatibility."""
    step(0, "Health check")
    r = client.get("/health")
    if r.status_code == 200:
        ok(f"API alive: {r.json()}")
    else:
        fail(f"/health returned {r.status_code}")
        return

    if applicant_id:
        warn(f"--applicant set; skipping upload. Observing {applicant_id}.")
        ingest_id = None
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
        r = client.post(
            "/ingest/pdf", files=files,
            data={"applicant_id": applicant_id, "borrower_role": "primary"},
        )
        if r.status_code != 200:
            fail(f"W2 ingest failed {r.status_code}: {r.text[:200]}")
            return
        body = r.json()
        ingest_id = body["ingest_id"]
        ok(f"ingest_id={ingest_id}")
        info(f"raw_s3_key={body.get('raw_s3_key')}")
        info(f"document_type_detected={body.get('document_type')}")

    if ingest_id:
        step(2, f"GET /ingest/{ingest_id}/raw")
        r = client.get(f"/ingest/{ingest_id}/raw")
        if r.status_code == 200:
            show_json("raw_ingestion row", r.json(), max_lines=20)
        else:
            warn(f"raw lookup returned {r.status_code}")

    step(3, f"GET /applicant/{applicant_id}/raw-ingestion")
    r = client.get(f"/applicant/{applicant_id}/raw-ingestion")
    if r.status_code == 200:
        body = r.json()
        show_json("pipeline_state", body.get("pipeline_state") or {}, max_lines=15)
        info(f"ingestions count: {len(body.get('ingestions') or [])}")
    else:
        warn(f"raw-ingestion returned {r.status_code}")

    step(4, f"GET /applicant/{applicant_id}/graph/summary")
    r = client.get(f"/applicant/{applicant_id}/graph/summary")
    if r.status_code == 200:
        show_json("graph summary", r.json(), max_lines=15)
    else:
        warn(f"graph endpoint {r.status_code}")

    step(5, f"GET /applicant/{applicant_id}/income-profile")
    r = client.get(f"/applicant/{applicant_id}/income-profile")
    if r.status_code == 200:
        show_json("income profile", r.json(), max_lines=20)
    else:
        warn(f"no income profile yet ({r.status_code})")


# ---- main ------------------------------------------------------------------


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--live", action="store_true",
                    help="Hit production ALB instead of local docker-compose.")
    ap.add_argument("--applicant", help="Observe an existing applicant_id (skip upload).")
    ap.add_argument("--application", help="Watch an existing application_id.")
    ap.add_argument("--upload", help="Path to a PDF to upload.")
    ap.add_argument("--type", help="Document type for --upload (e.g. APPRAISAL_URAR).")
    ap.add_argument("--property-id", help="Property ID for property-doc uploads.")
    ap.add_argument("--full", action="store_true",
                    help="Drive the complete mortgage scenario end-to-end.")
    args = ap.parse_args()

    url = get_url(args.live)
    api_key = get_api_key(args.live)
    headers = {"X-API-Key": api_key}

    print(
        f"\n{BOLD}watch_pipeline — {('LIVE prod' if args.live else 'local')} "
        f"@ {url}{RESET}"
    )

    with httpx.Client(base_url=url, headers=headers, timeout=60) as client:
        try:
            if args.full:
                mode_full(client)
            elif args.application:
                mode_application(client, args.application)
            elif args.upload:
                mode_upload(
                    client, args.upload, (args.type or "").upper(),
                    args.applicant, args.property_id,
                )
            else:
                mode_default(client, args.applicant)
        except httpx.HTTPError as exc:
            fail(f"HTTP error: {exc}")

    print()
    if _FAIL_COUNT:
        print(f"{BOLD}{RED}{_FAIL_COUNT} step(s) FAILED{RESET}\n")
        return 1
    print(f"{BOLD}{GREEN}all steps PASSED{RESET}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
