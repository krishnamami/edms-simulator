#!/usr/bin/env python3
"""EDMS 10-Loan Simulation against real AWS or local docker-compose.

Drops generated PDFs into S3 (or local_storage), creates applications via
``POST /loans``, ingests documents via ``/loans/document`` (income/credit/
asset) or ``/ingest/property-doc`` (property docs), and prints a full
report covering S3 + Postgres + Redis + graph + ApplicationContext.

Usage:
    python scripts/run_simulation.py --batch 1
    python scripts/run_simulation.py --batch 2
    python scripts/run_simulation.py --report
    python scripts/run_simulation.py --reset

    Add --live to hit the production ALB + real S3 + RDS + ElastiCache.
"""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
from datetime import date
from typing import Optional

import httpx

# Reconfigure stdout to UTF-8 so Unicode arrows in graph edge reasoning
# don't crash on Windows cp1252.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from core.documents.generators.bank_stmt_generator import generate_bank_statement  # noqa: E402
from core.documents.generators.credit_report_generator import generate_credit_report  # noqa: E402
from core.documents.generators.paystub_generator import generate_paystub  # noqa: E402
from core.documents.generators.w2_generator import generate_w2  # noqa: E402
from core.property.generators.appraisal_generator import generate_appraisal  # noqa: E402
from core.property.generators.flood_cert_generator import generate_flood_cert  # noqa: E402
from core.property.generators.hoi_generator import generate_hoi_binder  # noqa: E402
from core.property.generators.tax_bill_generator import generate_tax_bill  # noqa: E402

# ── Config ─────────────────────────────────────────────────────────────────

PROD_URL  = os.getenv(
    "EDMS_API_URL_LIVE",
    "http://edms-simulator-alb-1374683374.us-east-1.elb.amazonaws.com",
)
LOCAL_URL = os.getenv("EDMS_API_URL", "http://localhost:8001")
S3_BUCKET = os.getenv("AWS_S3_BUCKET", "edms-simulator-loans")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
STATE_FILE = pathlib.Path("simulation_state.json")

GREEN, YELLOW, CYAN, RED, BOLD, RESET = (
    "\033[92m", "\033[93m", "\033[96m", "\033[91m", "\033[1m", "\033[0m"
)

# Hard cap on every HTTP call so a slow / wedged endpoint can't freeze
# the whole simulation. Bumped via EDMS_SIM_TIMEOUT.
HTTP_TIMEOUT = float(os.getenv("EDMS_SIM_TIMEOUT", "10"))
DEBUG = False


def _safe(s) -> str:
    """Strip non-ASCII so Windows cp1252 console doesn't crash on
    extracted field names / reconciler reasoning containing arrows."""
    return str(s).encode("ascii", "replace").decode("ascii")


def ok(msg):    print(f"  {GREEN}[PASS]{RESET} {_safe(msg)}")
def fail(msg):  print(f"  {RED}[FAIL]{RESET} {_safe(msg)}")
def info(msg):  print(f"  {CYAN}  ->  {RESET} {_safe(msg)}")
def warn(msg):  print(f"  {YELLOW}[WARN]{RESET} {_safe(msg)}")


def section(title):
    print(f"\n{BOLD}{'='*70}{RESET}")
    print(f"{BOLD}  {title}{RESET}")
    print(f"{BOLD}{'='*70}{RESET}")


def subsection(title):
    print(f"\n{CYAN}  -- {title} --{RESET}")


# ── Application + document fixtures ────────────────────────────────────────

APPLICATIONS: dict[str, dict] = {
    "LOS-SIM-001": {
        "borrower":    {"first_name": "James",   "last_name": "Okafor",    "dob": "1982-07-14",
                        "ssn_hash": "sha256:sim-001-pri", "ssn_last4": "4729",
                        "email": "james.okafor@example.com"},
        "co_borrower": {"first_name": "Sarah",   "last_name": "Okafor",    "dob": "1985-03-22",
                        "ssn_hash": "sha256:sim-001-co", "ssn_last4": "8821",
                        "email": "sarah.okafor@example.com"},
        "loan":        {"loan_amount": 385_000, "credit_band": "near-prime"},
        "property":    {"line1": "123 Oak Street", "city": "Austin",  "state": "TX", "zip_code": "78701",
                        "property_type": "single_family"},
    },
    "LOS-SIM-002": {
        "borrower":    {"first_name": "Michael", "last_name": "Chen",      "dob": "1978-11-30",
                        "ssn_hash": "sha256:sim-002-pri", "ssn_last4": "3316"},
        "co_borrower": {"first_name": "Linda",   "last_name": "Chen",      "dob": "1980-05-15",
                        "ssn_hash": "sha256:sim-002-co", "ssn_last4": "7742"},
        "loan":        {"loan_amount": 520_000, "credit_band": "prime"},
    },
    "LOS-SIM-003": {
        "borrower":    {"first_name": "Maria",   "last_name": "Rodriguez", "dob": "1975-08-20",
                        "ssn_hash": "sha256:sim-003-pri", "ssn_last4": "5583"},
        "co_borrower": {"first_name": "Carlos",  "last_name": "Rodriguez", "dob": "1973-12-10",
                        "ssn_hash": "sha256:sim-003-co", "ssn_last4": "2291"},
        "loan":        {"loan_amount": 415_000, "credit_band": "prime"},
        "property":    {"line1": "456 Maple Ave", "city": "Dallas", "state": "TX", "zip_code": "75201",
                        "property_type": "single_family"},
    },
    "LOS-SIM-004": {
        "borrower":    {"first_name": "David",    "last_name": "Kim",      "dob": "1980-04-05",
                        "ssn_hash": "sha256:sim-004-pri", "ssn_last4": "6674"},
        "co_borrower": {"first_name": "Jennifer", "last_name": "Kim",      "dob": "1982-09-18",
                        "ssn_hash": "sha256:sim-004-co", "ssn_last4": "4418"},
        "loan":        {"loan_amount": 680_000, "credit_band": "prime"},
    },
    "LOS-SIM-005": {
        "borrower":    {"first_name": "Robert",  "last_name": "Johnson",   "dob": "1965-02-28",
                        "ssn_hash": "sha256:sim-005-pri", "ssn_last4": "8835"},
        "co_borrower": {"first_name": "Lisa",    "last_name": "Johnson",   "dob": "1967-06-14",
                        "ssn_hash": "sha256:sim-005-co", "ssn_last4": "1127"},
        "loan":        {"loan_amount": 295_000, "credit_band": "near-prime"},
        "property":    {"line1": "789 Pine Rd", "city": "Houston", "state": "TX", "zip_code": "77001",
                        "property_type": "single_family"},
    },
    "LOS-SIM-006": {
        "borrower":    {"first_name": "Ahmed",   "last_name": "Hassan",    "dob": "1983-10-12",
                        "ssn_hash": "sha256:sim-006-pri", "ssn_last4": "3362"},
        "co_borrower": {"first_name": "Fatima",  "last_name": "Hassan",    "dob": "1985-07-25",
                        "ssn_hash": "sha256:sim-006-co", "ssn_last4": "9954"},
        "loan":        {"loan_amount": 445_000, "credit_band": "prime"},
    },
    "LOS-SIM-007": {
        "borrower":    {"first_name": "Emily",   "last_name": "Davis",     "dob": "1990-01-08",
                        "ssn_hash": "sha256:sim-007-pri", "ssn_last4": "7716"},
        "co_borrower": {"first_name": "Mark",    "last_name": "Davis",     "dob": "1988-11-22",
                        "ssn_hash": "sha256:sim-007-co", "ssn_last4": "5538"},
        "loan":        {"loan_amount": 365_000, "credit_band": "near-prime"},
    },
    "LOS-SIM-008": {
        "borrower":    {"first_name": "William", "last_name": "Brown",     "dob": "1972-05-17",
                        "ssn_hash": "sha256:sim-008-pri", "ssn_last4": "2293"},
        "co_borrower": {"first_name": "Susan",   "last_name": "Brown",     "dob": "1974-09-03",
                        "ssn_hash": "sha256:sim-008-co", "ssn_last4": "8871"},
        "loan":        {"loan_amount": 310_000, "credit_band": "near-prime"},
    },
    "LOS-SIM-009": {
        "borrower":    {"first_name": "Sofia",   "last_name": "Martinez",  "dob": "1987-03-29",
                        "ssn_hash": "sha256:sim-009-pri", "ssn_last4": "6649"},
        "co_borrower": {"first_name": "Juan",    "last_name": "Martinez",  "dob": "1985-12-11",
                        "ssn_hash": "sha256:sim-009-co", "ssn_last4": "3315"},
        "loan":        {"loan_amount": 425_000, "credit_band": "prime"},
    },
    "LOS-SIM-010": {
        "borrower":    {"first_name": "Thomas",  "last_name": "Wilson",    "dob": "1969-08-06",
                        "ssn_hash": "sha256:sim-010-pri", "ssn_last4": "4482"},
        "co_borrower": {"first_name": "Mary",    "last_name": "Wilson",    "dob": "1971-04-19",
                        "ssn_hash": "sha256:sim-010-co", "ssn_last4": "7763"},
        "loan":        {"loan_amount": 550_000, "credit_band": "prime"},
    },
}


BATCH_1_DOCS: dict[str, list[dict]] = {
    "LOS-SIM-001": [
        {"type": "W2_CURRENT",       "role": "primary",     "category": "income",
         "data": {"employer_name": "Accenture LLC", "box1_wages": 92_400,
                  "tax_year": 2023, "ssn_last4": "4729"}},
        {"type": "W2_CURRENT",       "role": "co_borrower", "category": "income",
         "data": {"employer_name": "Dell Technologies", "box1_wages": 56_200,
                  "tax_year": 2023, "ssn_last4": "8821"}},
        {"type": "CREDIT_REPORT",    "role": "primary",     "category": "credit",
         "data": {"mid_score": 723, "credit_band": "near-prime",
                  "total_monthly_obligations": 944, "derogatory_marks": 0}},
        {"type": "APPRAISAL_URAR",   "role": "primary",     "category": "property",
         "data": {"appraised_value": 485_000, "condition_rating": "C2",
                  "appraisal_date": "2026-05-01",
                  "property_address": "123 Oak Street, Austin TX"}},
    ],
    "LOS-SIM-002": [
        {"type": "W2_CURRENT",       "role": "primary",     "category": "income",
         "data": {"employer_name": "Google LLC", "box1_wages": 145_000,
                  "tax_year": 2023, "ssn_last4": "3316"}},
        {"type": "PAYSTUB_CURRENT",  "role": "primary",     "category": "income",
         "data": {"employer_name": "Google LLC", "ytd_gross": 60_416,
                  "gross_pay": 12_083, "pay_period_end": "2026-04-30"}},
        {"type": "BANK_STATEMENT_M1", "role": "primary",    "category": "asset",
         "data": {"closing_balance": 85_000, "avg_monthly_deposits": 12_500,
                  "balance": 85_000, "account_type": "checking"}},
    ],
    "LOS-SIM-003": [
        {"type": "W2_CURRENT",       "role": "primary",     "category": "income",
         "data": {"employer_name": "Amazon.com Inc", "box1_wages": 118_000,
                  "tax_year": 2023, "ssn_last4": "5583"}},
        {"type": "W2_CURRENT",       "role": "co_borrower", "category": "income",
         "data": {"employer_name": "Texas Health Resources", "box1_wages": 72_000,
                  "tax_year": 2023, "ssn_last4": "2291"}},
        {"type": "APPRAISAL_URAR",   "role": "primary",     "category": "property",
         "data": {"appraised_value": 520_000, "condition_rating": "C1",
                  "appraisal_date": "2026-05-02",
                  "property_address": "456 Maple Ave, Dallas TX"}},
    ],
    "LOS-SIM-004": [
        {"type": "TAX_RETURN_1040_CURRENT", "role": "primary", "category": "income",
         "data": {"agi": 185_000, "net_income_after_addbacks": 165_000,
                  "has_schedule_c": True, "tax_year": 2023}},
        {"type": "1099_NEC",         "role": "primary",     "category": "income",
         "data": {"amount": 185_000, "payer_name": "Various Clients LLC",
                  "tax_year": 2023}},
    ],
    "LOS-SIM-005": [
        {"type": "W2_CURRENT",       "role": "primary",     "category": "income",
         "data": {"employer_name": "Shell Oil Company", "box1_wages": 88_000,
                  "tax_year": 2023, "ssn_last4": "8835"}},
        {"type": "SSA_AWARD_LETTER", "role": "co_borrower", "category": "income",
         "data": {"monthly_benefit": 2100, "is_non_taxable": True}},
        {"type": "APPRAISAL_URAR",   "role": "primary",     "category": "property",
         "data": {"appraised_value": 395_000, "condition_rating": "C3",
                  "appraisal_date": "2026-05-03",
                  "property_address": "789 Pine Rd, Houston TX"}},
    ],
    "LOS-SIM-006": [
        {"type": "W2_CURRENT",       "role": "primary",     "category": "income",
         "data": {"employer_name": "ExxonMobil Corp", "box1_wages": 125_000,
                  "tax_year": 2023, "ssn_last4": "3362"}},
        {"type": "W2_CURRENT",       "role": "co_borrower", "category": "income",
         "data": {"employer_name": "UT Health System", "box1_wages": 68_000,
                  "tax_year": 2023, "ssn_last4": "9954"}},
        {"type": "BANK_STATEMENT_M1", "role": "primary",    "category": "asset",
         "data": {"closing_balance": 42_000, "avg_monthly_deposits": 16_000,
                  "balance": 42_000, "account_type": "checking"}},
    ],
    # 008 / 009 / 010: created by main() with empty list so no docs ship.
}

BATCH_2_DOCS: dict[str, list[dict]] = {
    "LOS-SIM-001": [
        {"type": "PAYSTUB_CURRENT",  "role": "primary", "category": "income",
         "data": {"employer_name": "Accenture LLC", "ytd_gross": 38_500,
                  "gross_pay": 7_700, "pay_period_end": "2026-04-30"}},
        {"type": "HOI_BINDER",       "role": "primary", "category": "property",
         "data": {"annual_premium": 1_800, "monthly_premium": 150,
                  "carrier_name": "State Farm", "policy_number": "SF-2026-001",
                  "dwelling_coverage": 485_000}},
        {"type": "FLOOD_CERT",       "role": "primary", "category": "property",
         "data": {"flood_zone": "X", "sfha": False,
                  "flood_insurance_required": False,
                  "determination_date": "2026-05-05",
                  "firm_panel": "48453C0480J"}},
    ],
    "LOS-SIM-003": [
        {"type": "HOI_BINDER",       "role": "primary", "category": "property",
         "data": {"annual_premium": 2_100, "monthly_premium": 175,
                  "carrier_name": "Allstate", "policy_number": "AL-2026-003",
                  "dwelling_coverage": 520_000}},
        {"type": "FLOOD_CERT",       "role": "primary", "category": "property",
         "data": {"flood_zone": "AE", "sfha": True,
                  "flood_insurance_required": True,
                  "determination_date": "2026-05-05",
                  "firm_panel": "48113C0325G"}},
    ],
    "LOS-SIM-005": [
        {"type": "PAYSTUB_CURRENT",  "role": "primary", "category": "income",
         "data": {"employer_name": "Shell Oil Company", "ytd_gross": 36_666,
                  "gross_pay": 7_333, "pay_period_end": "2026-04-30"}},
        {"type": "HOI_BINDER",       "role": "primary", "category": "property",
         "data": {"annual_premium": 1_560, "monthly_premium": 130,
                  "carrier_name": "USAA", "policy_number": "USAA-2026-005",
                  "dwelling_coverage": 395_000}},
        {"type": "FLOOD_CERT",       "role": "primary", "category": "property",
         "data": {"flood_zone": "X", "sfha": False,
                  "flood_insurance_required": False,
                  "determination_date": "2026-05-05",
                  "firm_panel": "48201C1050L"}},
    ],
    "LOS-SIM-007": [
        {"type": "W2_CURRENT", "role": "primary", "category": "income",
         "data": {"employer_name": "Apple Inc", "box1_wages": 135_000,
                  "tax_year": 2023, "ssn_last4": "7716"}},
        {"type": "W2_CURRENT", "role": "co_borrower", "category": "income",
         "data": {"employer_name": "Microsoft Corp", "box1_wages": 115_000,
                  "tax_year": 2023, "ssn_last4": "5538"}},
    ],
}


_PROPERTY_DOC_TYPES = {
    "APPRAISAL_URAR", "APPRAISAL_UPDATE", "APPRAISAL_DESK", "APPRAISAL_FIELD",
    "HOI_BINDER", "HOI_DECLARATIONS", "FLOOD_CERT",
    "PROPERTY_TAX_BILL", "TITLE_COMMITMENT",
}


# ── PDF generation (adapt fixture data → real generator signatures) ─────────


def _name_for(los_id: str, role: str) -> str:
    person = APPLICATIONS[los_id][
        "co_borrower" if role == "co_borrower" else "borrower"
    ]
    return f"{person['first_name']} {person['last_name']}"


def _ssn_last4_for(los_id: str, role: str) -> str:
    person = APPLICATIONS[los_id][
        "co_borrower" if role == "co_borrower" else "borrower"
    ]
    return person.get("ssn_last4", "0000")


def generate_doc_bytes(los_id: str, doc: dict) -> bytes:
    doc_type = doc["type"]
    data = doc["data"]
    role = doc.get("role", "primary")
    name = _name_for(los_id, role)
    ssn4 = _ssn_last4_for(los_id, role)

    if DEBUG:
        import time as _t
        gstart = _t.time()
        print(f"  {CYAN}gen{RESET}  {doc_type} ({los_id}/{role})", flush=True)

    def _emit(pdf_bytes: bytes) -> bytes:
        if DEBUG:
            ms = (_t.time() - gstart) * 1000
            print(f"  {CYAN}gen{RESET}  -> {len(pdf_bytes):,}b  {ms:.0f}ms",
                  flush=True)
        return pdf_bytes

    pdf_bytes: Optional[bytes] = None
    try:
        if doc_type in ("W2_CURRENT", "W2_PRIOR"):
            pdf_bytes, _ = generate_w2(
                employee_name=name,
                employee_ssn_last4=data.get("ssn_last4") or ssn4,
                employee_address="100 Main St\nCity, ST 00000",
                employer_name=data.get("employer_name", "Employer Inc"),
                employer_ein="123456789",
                employer_address="1 Corp Way",
                tax_year=int(data.get("tax_year", date.today().year - 1)),
                box1_wages=float(data.get("box1_wages", 60_000)),
            )
        elif doc_type == "PAYSTUB_CURRENT":
            end = data.get("pay_period_end") or "2026-04-30"
            end_d = date.fromisoformat(end)
            pdf_bytes, _ = generate_paystub(
                employer_name=data.get("employer_name", "Employer Inc"),
                employee_name=name,
                employee_ssn_last4=ssn4,
                pay_period_start=end_d.replace(day=1),
                pay_period_end=end_d,
                pay_date=end_d,
                gross_pay=float(data.get("gross_pay", 5_000)),
                ytd_gross=float(data.get("ytd_gross", 20_000)),
            )
        elif doc_type in ("BANK_STATEMENT_M1", "BANK_STATEMENT_M2"):
            pdf_bytes, _ = generate_bank_statement(
                bank_name="Sample Bank",
                account_holder=name,
                account_number="XXXX1234",
                statement_end_date=date.today(),
                starting_balance=float(data.get("closing_balance", 5_000)),
                seed=hash(los_id) & 0xFFFF,
            )
        elif doc_type == "CREDIT_REPORT":
            profile = {
                "applicant_id": los_id,
                "experian_score": int(data.get("mid_score", 700)) - 5,
                "equifax_score":  int(data.get("mid_score", 700)),
                "transunion_score": int(data.get("mid_score", 700)) + 5,
                "mid_score":      int(data.get("mid_score", 700)),
                "credit_band":    data.get("credit_band", "near-prime"),
                "open_tradelines": 5,
                "revolving_utilization": 0.30,
                "total_monthly_obligations": float(data.get("total_monthly_obligations", 500)),
                "monthly_obligations": [
                    {"type": "auto", "monthly_payment":
                        float(data.get("total_monthly_obligations", 500)),
                     "creditor": "Auto Lender"},
                ],
                "derogatory_marks": int(data.get("derogatory_marks", 0)),
                "hard_inquiries_12mo": 1,
                "report_date": "2026-05-01",
            }
            pdf_bytes, _ = generate_credit_report(applicant_name=name, profile=profile)
        elif doc_type in ("APPRAISAL_URAR", "APPRAISAL_UPDATE",
                           "APPRAISAL_DESK", "APPRAISAL_FIELD"):
            pdf_bytes, _ = generate_appraisal(
                property_address=data.get("property_address", "123 Main St\nCity, ST 00000"),
                appraised_value=float(data.get("appraised_value", 500_000)),
                condition_rating=data.get("condition_rating", "C3"),
                effective_date=data.get("appraisal_date", "2026-05-01"),
            )
        elif doc_type == "HOI_BINDER":
            pdf_bytes, _ = generate_hoi_binder(
                insured_name=name,
                property_address=data.get("property_address",
                                           "123 Main St\nCity, ST 00000"),
                annual_premium=float(data.get("annual_premium", 1_800)),
                policy_number=data.get("policy_number", "POL-1"),
                dwelling_coverage=float(data.get("dwelling_coverage", 500_000)),
                carrier_name=data.get("carrier_name", "Carrier Inc"),
            )
        elif doc_type == "FLOOD_CERT":
            pdf_bytes, _ = generate_flood_cert(
                property_address=data.get("property_address",
                                           "123 Main St\nCity, ST 00000"),
                flood_zone=data.get("flood_zone", "X"),
                determination_date=data.get("determination_date", "2026-05-05"),
                firm_panel=data.get("firm_panel", "48000C0000A"),
            )
        elif doc_type == "PROPERTY_TAX_BILL":
            pdf_bytes, _ = generate_tax_bill(
                property_address=data.get("property_address",
                                           "123 Main St\nCity, ST 00000"),
                owner_name=name,
                annual_tax=float(data.get("annual_tax", 6_000)),
                tax_year=int(data.get("tax_year", 2024)),
            )
    except Exception as exc:
        warn(f"generator failed for {doc_type}: {exc}")
        pdf_bytes = None

    if pdf_bytes is None:
        # Fallback — any doc type without a real generator (1099, SSA, tax return)
        body = json.dumps(
            {"document_type": doc_type, **data}, default=str, indent=2
        )
        pdf_bytes = b"%PDF-1.4\n" + body.encode() + b"\n%%EOF"
    return _emit(pdf_bytes)


# ── S3 / local upload ──────────────────────────────────────────────────────


def upload_to_s3(application_id: str, los_id: str, doc: dict,
                  live: bool, s3_client) -> tuple[str, int]:
    doc_id = f"DOC-{los_id}-{doc['type']}-{doc['role']}"
    s3_key = f"loans/{application_id}/{doc['category']}/{doc_id}.pdf"
    pdf_bytes = generate_doc_bytes(los_id, doc)
    size = len(pdf_bytes)

    if live and s3_client:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=pdf_bytes,
            ContentType="application/pdf",
            ServerSideEncryption="aws:kms",
            Metadata={
                "los_id": los_id, "application_id": application_id,
                "document_type": doc["type"], "borrower_role": doc["role"],
                "simulation": "true",
            },
        )
    else:
        local_path = pathlib.Path(
            os.getenv("LOCAL_STORAGE_PATH", "./local_storage")
        ) / s3_key
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(pdf_bytes)

    return s3_key, size


# ── API helpers ─────────────────────────────────────────────────────────────


def api(method: str, path: str, api_url: str, api_key: str, **kwargs):
    headers = {"X-API-Key": api_key}
    if "json" in kwargs:
        headers["Content-Type"] = "application/json"
    timeout = kwargs.pop("timeout", HTTP_TIMEOUT)
    if DEBUG:
        import time as _t
        start = _t.time()
        print(f"  {YELLOW}>>>{RESET} {method} {path}", flush=True)
    try:
        r = httpx.request(
            method, f"{api_url}{path}",
            headers=headers, timeout=timeout, **kwargs,
        )
    except httpx.HTTPError as exc:
        if DEBUG:
            print(f"  {RED}!!!{RESET} {method} {path}  -> {exc}", flush=True)
        raise
    if DEBUG:
        ms = (_t.time() - start) * 1000
        print(
            f"  {YELLOW}<<<{RESET} {method} {path}  "
            f"-> {r.status_code}  {ms:.0f}ms",
            flush=True,
        )
    return r


def ingest_property_doc(client_url: str, api_key: str, *,
                         property_id: str, doc_type: str,
                         pdf_bytes: bytes, filename: str) -> bool:
    """Property docs go through the dedicated multipart endpoint."""
    headers = {"X-API-Key": api_key}
    if DEBUG:
        import time as _t
        start = _t.time()
        print(f"  {YELLOW}>>>{RESET} POST /ingest/property-doc  "
              f"({doc_type}, {len(pdf_bytes):,}b)", flush=True)
    try:
        r = httpx.post(
            f"{client_url}/ingest/property-doc",
            headers=headers,
            files={"file": (filename, pdf_bytes, "application/pdf")},
            data={"property_id": property_id, "document_type": doc_type},
            timeout=HTTP_TIMEOUT,
        )
    except httpx.HTTPError as exc:
        if DEBUG:
            print(f"  {RED}!!!{RESET} property-doc -> {exc}", flush=True)
        return False
    if DEBUG:
        ms = (_t.time() - start) * 1000
        print(f"  {YELLOW}<<<{RESET} POST /ingest/property-doc  "
              f"-> {r.status_code}  {ms:.0f}ms", flush=True)
    return r.status_code in (200, 201)


def ingest_borrower_docs_batch(
    api_url: str, api_key: str, *,
    applicant_id: str, application_id: str,
    los_id: str, docs: list[dict],
) -> bool:
    """Ingest a batch of borrower-side docs in ONE /loans/document call.

    /loans/document fires DocumentUploadedEvent which calls
    `_run_assembly(documents=all_documents)`. If we send each doc in its
    own request, each event re-assembles with only that one doc and the
    final income_profile reflects only the LAST doc — wiping earlier
    results. Batching keeps the assembler's view of "everything I have
    for this applicant" consistent.
    """
    if not docs:
        return True
    body = {
        "applicant_id":   applicant_id,
        "application_id": application_id,
        "all_documents": [
            {
                "document_id":      f"DOC-{los_id}-{d['type']}-{d['role']}",
                "document_type":    d["type"],
                "document_category": d["category"],
                "borrower_role":    d["role"],
                **d.get("data", {}),
            }
            for d in docs
        ],
    }
    r = api("POST", "/loans/document", api_url, api_key, json=body)
    return r.status_code in (200, 201)


# ── Batch runner ───────────────────────────────────────────────────────────


def run_batch(batch_num: int, batch_docs: dict, created_apps: dict,
              api_url: str, api_key: str, live: bool, s3_client) -> None:

    section(
        f"BATCH {batch_num} — {'6 apps + partial docs' if batch_num == 1 else '4 apps + 1 new + property docs'}"
    )

    for los_id in sorted(batch_docs.keys()):
        docs = batch_docs[los_id]

        # --- Create application if not already created ---
        if los_id not in created_apps:
            app_def = APPLICATIONS[los_id]
            print(f"\n  {BOLD}Creating {los_id} -- "
                  f"{app_def['borrower']['first_name']} {app_def['borrower']['last_name']} + "
                  f"{app_def['co_borrower']['first_name']} {app_def['co_borrower']['last_name']}{RESET}")

            r = api("POST", "/loans", api_url, api_key, json={
                "los_id":      los_id,
                "borrower":    app_def["borrower"],
                "co_borrower": app_def.get("co_borrower"),
                "loan":        app_def["loan"],
                "documents":   [],
            })
            if r.status_code != 200:
                fail(f"create {los_id} -> {r.status_code} {r.text[:160]}")
                continue
            result = r.json()
            entry = {
                "application_id":  result["application_id"],
                "applicant_id":    result["applicant_id"],
                "co_applicant_id": result.get("co_applicant_id"),
                "match_method":    result.get("match_method", "?"),
                "is_new_record":   result.get("is_new_record", True),
            }
            ok(f"application {result['application_id']}  applicant={result['applicant_id']}  "
               f"co={result.get('co_applicant_id') or '-'}")

            if app_def.get("property"):
                r2 = api("POST", "/properties", api_url, api_key, json={
                    "application_id": result["application_id"],
                    "address": {
                        "line1": app_def["property"]["line1"],
                        "city":  app_def["property"]["city"],
                        "state": app_def["property"]["state"],
                        "zip_code": app_def["property"]["zip_code"],
                    },
                    "property_type": app_def["property"]["property_type"],
                })
                if r2.status_code in (200, 201):
                    entry["property_id"] = r2.json().get("property_id")
                    info(f"property {entry['property_id']}  "
                         f"{app_def['property']['line1']}, {app_def['property']['city']} "
                         f"{app_def['property']['state']}")
                else:
                    warn(f"property create {los_id}: {r2.status_code} {r2.text[:120]}")
            created_apps[los_id] = entry

        app_info = created_apps[los_id]
        application_id = app_info["application_id"]
        applicant_id   = app_info["applicant_id"]
        property_id    = app_info.get("property_id")

        if not docs:
            info(f"{los_id}: created, no documents this batch")
            continue

        print(f"\n  {BOLD}{los_id} -- {len(docs)} document(s){RESET}")

        # Upload every doc to S3 first (one round-trip per file).
        for doc in docs:
            s3_key, size = upload_to_s3(application_id, los_id, doc, live, s3_client)
            info(f"S3 {s3_key} ({size:,} bytes)")

        # Property docs: route through /ingest/property-doc one at a time —
        # each one triggers PropertyDocumentUploadedEvent + re-assembly.
        prop_docs = [d for d in docs if d["type"] in _PROPERTY_DOC_TYPES]
        for doc in prop_docs:
            role_label = f"[{doc['role'][:3].upper()}]"
            if not property_id:
                warn(f"{role_label} {doc['type']}: no property_id on application")
                continue
            pdf_bytes = generate_doc_bytes(los_id, doc)
            if ingest_property_doc(
                api_url, api_key,
                property_id=property_id, doc_type=doc["type"],
                pdf_bytes=pdf_bytes,
                filename=f"{doc['type'].lower()}.pdf",
            ):
                ok(f"{role_label} {doc['type']} -> /ingest/property-doc")
            else:
                warn(f"{role_label} {doc['type']} -> /ingest/property-doc failed")

        # Borrower docs: batch by applicant role so the income assembler
        # sees the FULL set of docs for that applicant in one event.
        borrower_docs = [d for d in docs if d["type"] not in _PROPERTY_DOC_TYPES]
        co_id = app_info.get("co_applicant_id")
        groups = {
            "primary":     [d for d in borrower_docs if d["role"] == "primary"],
            "co_borrower": [d for d in borrower_docs if d["role"] == "co_borrower"],
        }
        for role, doc_list in groups.items():
            if not doc_list:
                continue
            target_id = co_id if role == "co_borrower" and co_id else applicant_id
            ok_label = f"[{role[:3].upper()}]"
            types_str = ", ".join(d["type"] for d in doc_list)
            if ingest_borrower_docs_batch(
                api_url, api_key,
                applicant_id=target_id,
                application_id=application_id,
                los_id=los_id, docs=doc_list,
            ):
                ok(f"{ok_label} {types_str} -> /loans/document")
            else:
                warn(f"{ok_label} {types_str} -> /loans/document failed")


# ── Report ─────────────────────────────────────────────────────────────────


def _flag(val: bool) -> str:
    return f"{GREEN}OK{RESET}" if val else f"{RED}--{RESET}"


def show_report(created_apps: dict, api_url: str, api_key: str,
                live: bool, s3_client) -> None:
    section("REPORT")

    for los_id in sorted(APPLICATIONS.keys()):
        if los_id not in created_apps:
            print(f"\n  {YELLOW}{los_id}: NOT CREATED{RESET}")
            continue
        info_row = created_apps[los_id]
        application_id = info_row["application_id"]
        applicant_id   = info_row["applicant_id"]
        co_id          = info_row.get("co_applicant_id")
        app_def        = APPLICATIONS[los_id]

        print(f"\n{BOLD}{CYAN}{'-'*70}{RESET}")
        print(f"{BOLD}{CYAN}  {los_id}  ->  {application_id}{RESET}")
        print(f"  primary:    {app_def['borrower']['first_name']} {app_def['borrower']['last_name']} ({applicant_id})")
        if co_id:
            print(f"  co-borrower: {app_def['co_borrower']['first_name']} {app_def['co_borrower']['last_name']} ({co_id})")
        print(f"{BOLD}{CYAN}{'-'*70}{RESET}")

        # --- S3 files ---
        subsection("S3 files")
        files = _list_files(application_id, live, s3_client)
        if files:
            for key, size in files:
                print(f"    {key}  ({size:,} bytes)")
        else:
            print("    (no files)")

        # --- Postgres rollups ---
        subsection("Postgres + graph")
        gs = api("GET", f"/applicant/{applicant_id}/graph/summary",
                  api_url, api_key)
        gs_body = gs.json().get("data", gs.json()) if gs.status_code == 200 else {}
        print(f"    document_count:    {gs_body.get('document_count', 0)}")
        print(f"    relationships:     {gs_body.get('relationship_count', 0)}")
        print(f"    confirms:          {gs_body.get('confirmation_count', 0)}")
        print(f"    conflicts:         {gs_body.get('conflict_count', 0)}")

        inc = api("GET", f"/applicant/{applicant_id}/income-profile",
                  api_url, api_key)
        if inc.status_code == 200:
            ib = inc.json().get("data", inc.json())
            qm = ib.get("combined_qualifying_monthly")
            if qm is None:
                qm = (ib.get("primary_borrower") or {}).get("qualifying_monthly", 0)
            print(f"    income.qualifying: ${float(qm or 0):,.0f}/mo  "
                  f"source={inc.json().get('source','?')}")
        else:
            print(f"    income.qualifying: pending (HTTP {inc.status_code})")

        cr = api("GET", f"/applicant/{applicant_id}/credit-profile",
                  api_url, api_key)
        if cr.status_code == 200:
            cb = cr.json().get("data", cr.json())
            print(f"    credit:            mid={cb.get('mid_score')}  "
                  f"band={cb.get('credit_band')}  "
                  f"obligations=${float(cb.get('total_monthly_obligations') or 0):,.0f}/mo")
        else:
            print(f"    credit:            pending")

        # --- Graph edges ---
        subsection("Graph edges")
        ge = api("GET", f"/applicant/{applicant_id}/graph", api_url, api_key)
        if ge.status_code == 200:
            edges = ge.json().get("relationships") or ge.json().get("edges") or []
            if edges:
                for r in edges[:20]:
                    rt = r.get("relationship_type", "?")
                    color = (GREEN if rt == "confirms" else
                             YELLOW if rt == "corroborates" else
                             RED if rt == "contradicts" else CYAN)
                    field = _safe(r.get("field_name", "?"))[:20]
                    print(f"    {color}{rt:13}{RESET}  field={field:<20}  "
                          f"delta={r.get('delta_pct')}  conf={r.get('confidence')}")
            else:
                print("    (no edges yet)")
        else:
            print(f"    HTTP {ge.status_code}")

        # --- Application context ---
        subsection("Application context")
        ctx_resp = api("GET", f"/application/{application_id}/context",
                        api_url, api_key)
        if ctx_resp.status_code == 200:
            data = ctx_resp.json().get("data", ctx_resp.json())
            rd = data.get("readiness") or {}
            missing = rd.get("missing_items") or []
            print(f"    income_verified:    {_flag(rd.get('income_verified'))}")
            print(f"    credit_pulled:      {_flag(rd.get('credit_pulled'))}")
            print(f"    appraisal_complete: {_flag(rd.get('appraisal_complete'))}")
            print(f"    insurance_bound:    {_flag(rd.get('insurance_bound'))}")
            print(f"    flood_cert:         {_flag(rd.get('flood_cert_received'))}")
            print(f"    aus_ready:          {_flag(rd.get('aus_ready'))}")
            if data.get("front_end_dti") is not None:
                v = data["front_end_dti"]
                color = RED if v > 43 else YELLOW if v > 36 else GREEN
                print(f"    front_end_dti:      {color}{v:.1f}%{RESET}")
            if data.get("ltv") is not None:
                v = data["ltv"]
                color = RED if v > 95 else YELLOW if v > 80 else GREEN
                print(f"    ltv:                {color}{v:.1f}%{RESET}")
            if data.get("requires_review"):
                print(f"    requires_review:    {RED}YES{RESET}")
            else:
                print(f"    requires_review:    {GREEN}NO{RESET}")
            if missing:
                print(f"    missing_items:      {YELLOW}{', '.join(missing)}{RESET}")
            else:
                print(f"    missing_items:      {GREEN}none{RESET}")
        else:
            print(f"    context unavailable (HTTP {ctx_resp.status_code})")

    # --- Summary table ---
    section("SUMMARY")
    print(f"\n  {'LOS ID':<14} {'Borrower':<22} {'Docs':>5} {'Edges':>6} {'Income':>11}  Status")
    print(f"  {'-'*14} {'-'*22} {'-'*5} {'-'*6} {'-'*11}  {'-'*30}")
    complete: list[str] = []
    partial:  list[str] = []
    no_docs:  list[str] = []
    for los_id in sorted(APPLICATIONS.keys()):
        app_def = APPLICATIONS[los_id]
        name = f"{app_def['borrower']['first_name']} {app_def['borrower']['last_name']}"
        if los_id not in created_apps:
            print(f"  {los_id:<14} {name:<22} {'-':>5} {'-':>6} {'-':>11}  not created")
            no_docs.append(los_id)
            continue
        info_row = created_apps[los_id]
        applicant_id = info_row["applicant_id"]
        application_id = info_row["application_id"]

        gs = api("GET", f"/applicant/{applicant_id}/graph/summary",
                  api_url, api_key)
        gs_body = gs.json().get("data", gs.json()) if gs.status_code == 200 else {}
        doc_count  = gs_body.get("document_count", 0)
        edge_count = gs_body.get("relationship_count", 0)

        inc = api("GET", f"/applicant/{applicant_id}/income-profile",
                  api_url, api_key)
        income_str = "pending"
        if inc.status_code == 200:
            ib = inc.json().get("data", inc.json())
            qm = ib.get("combined_qualifying_monthly")
            if qm is None:
                qm = (ib.get("primary_borrower") or {}).get("qualifying_monthly", 0)
            income_str = f"${float(qm or 0):,.0f}/mo"

        ctx_resp = api("GET", f"/application/{application_id}/context",
                        api_url, api_key)
        if ctx_resp.status_code == 200:
            data = ctx_resp.json().get("data", ctx_resp.json())
            missing = (data.get("readiness") or {}).get("missing_items") or []
            req = data.get("requires_review")
            if not missing:
                status = f"{GREEN}COMPLETE{RESET}"
                complete.append(los_id)
            elif req:
                status = f"{RED}REVIEW ({len(missing)} missing){RESET}"
                partial.append(los_id)
            else:
                status = f"{YELLOW}PARTIAL ({len(missing)} missing){RESET}"
                partial.append(los_id)
        else:
            status = f"{YELLOW}PARTIAL (no context){RESET}"
            (partial if doc_count > 0 else no_docs).append(los_id)

        print(f"  {los_id:<14} {name:<22} {doc_count:>5} {edge_count:>6} {income_str:>11}  {status}")

    print(
        f"\n  {GREEN}COMPLETE ({len(complete)}){RESET}: "
        f"{', '.join(complete) or 'none'}"
    )
    print(
        f"  {YELLOW}PARTIAL ({len(partial)}){RESET}:  "
        f"{', '.join(partial) or 'none'}"
    )
    print(
        f"  {RED}NO DOCS ({len(no_docs)}){RESET}:  "
        f"{', '.join(no_docs) or 'none'}"
    )

    # --- Postgres row counts via /admin/table-count ---
    section("POSTGRES ROW COUNTS")
    print(f"  {'Table':<28} {'Rows':>8}")
    print(f"  {'-'*28} {'-'*8}")
    for t in (
        "applicants", "applicant_identity_xref", "applications",
        "income_profiles", "credit_profiles",
        "document_index", "document_relationships",
        "properties", "property_profiles",
        "raw_ingestion", "context_versions",
        "indexing_watermarks", "indexing_runs",
    ):
        r = api("GET", f"/admin/table-count/{t}", api_url, api_key)
        c = r.json().get("count") if r.status_code == 200 else "?"
        print(f"  {t:<28} {str(c):>8}")

    print(f"\n{BOLD}{'='*70}{RESET}")
    print(
        f"{BOLD}  Done.  Dashboard: "
        f"{(PROD_URL if live else LOCAL_URL)}/dashboard{RESET}"
    )
    print(f"{BOLD}{'='*70}{RESET}\n")


def _list_files(application_id: str, live: bool, s3_client) -> list[tuple[str, int]]:
    if live and s3_client:
        out: list[tuple[str, int]] = []
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(
            Bucket=S3_BUCKET, Prefix=f"loans/{application_id}/"
        ):
            for obj in page.get("Contents", []) or []:
                out.append((obj["Key"], int(obj["Size"])))
        return out
    base = pathlib.Path(os.getenv("LOCAL_STORAGE_PATH", "./local_storage"))
    root = base / "loans" / application_id
    if not root.exists():
        return []
    return [
        (str(f.relative_to(base)).replace("\\", "/"), f.stat().st_size)
        for f in sorted(root.rglob("*")) if f.is_file()
    ]


# ── Pre-flight ─────────────────────────────────────────────────────────────


def preflight(api_url: str, api_key: str, live: bool, s3_client) -> bool:
    section("PRE-FLIGHT CHECKS")
    try:
        r = httpx.get(f"{api_url}/health", timeout=HTTP_TIMEOUT)
    except httpx.HTTPError as exc:
        fail(f"API unreachable: {exc}")
        return False
    if r.status_code != 200:
        fail(f"/health -> {r.status_code}")
        return False
    ok(f"API alive at {api_url}")

    try:
        r2 = httpx.get(f"{api_url}/ready", timeout=HTTP_TIMEOUT)
    except httpx.HTTPError:
        warn("/ready unreachable")
        return True
    if r2.status_code == 200:
        ready = r2.json()
        ok(f"postgres={ready.get('postgres')}  redis={ready.get('redis')}")
        if not (ready.get("postgres") and ready.get("redis")):
            warn("postgres or redis is degraded — continuing anyway")
    if live and s3_client:
        try:
            s3_client.head_bucket(Bucket=S3_BUCKET)
            ok(f"S3 bucket {S3_BUCKET} reachable")
        except Exception as exc:
            fail(f"S3 head_bucket: {exc}")
            return False
    return True


# ── Main ───────────────────────────────────────────────────────────────────


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--batch", type=int, choices=[1, 2])
    p.add_argument("--report", action="store_true")
    p.add_argument("--reset", action="store_true")
    p.add_argument("--live", action="store_true")
    p.add_argument("--debug", action="store_true",
                    help="Verbose: print before/after every API call + "
                          "generator with timing.")
    args = p.parse_args()

    global DEBUG
    DEBUG = args.debug

    api_url = PROD_URL if args.live else LOCAL_URL
    api_key = os.getenv("EDMS_API_KEY", "edms_dev_key")

    s3_client = None
    if args.live:
        import boto3
        s3_client = boto3.client("s3", region_name=AWS_REGION)

    if not preflight(api_url, api_key, args.live, s3_client):
        return 1

    if args.reset:
        STATE_FILE.unlink(missing_ok=True)
        ok("state file removed")
        return 0

    created_apps: dict = {}
    if STATE_FILE.exists():
        created_apps = json.loads(STATE_FILE.read_text())
        info(f"loaded state: {len(created_apps)} apps")

    if args.report:
        show_report(created_apps, api_url, api_key, args.live, s3_client)
        return 0

    if args.batch == 1:
        for los_id in ("LOS-SIM-008", "LOS-SIM-009", "LOS-SIM-010"):
            BATCH_1_DOCS.setdefault(los_id, [])
        run_batch(1, BATCH_1_DOCS, created_apps, api_url, api_key,
                   args.live, s3_client)
        STATE_FILE.write_text(json.dumps(created_apps, indent=2))
        ok("batch 1 saved")
        show_report(created_apps, api_url, api_key, args.live, s3_client)
    elif args.batch == 2:
        if not created_apps:
            fail("no state — run --batch 1 first")
            return 1
        run_batch(2, BATCH_2_DOCS, created_apps, api_url, api_key,
                   args.live, s3_client)
        STATE_FILE.write_text(json.dumps(created_apps, indent=2))
        ok("batch 2 saved")
        show_report(created_apps, api_url, api_key, args.live, s3_client)
    else:
        p.print_help()
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
