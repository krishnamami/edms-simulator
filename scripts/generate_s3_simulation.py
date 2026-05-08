"""Generate 50 days of mortgage-loan documents for the backtest harness.

Produces a directory tree the S3EDMSConnector can walk:
    local_storage/s3_simulation/
        2026-01-01/
            LOS-001/W2_CURRENT.json
            LOS-002/URLA_1003.json
        2026-01-02/
            ...

Each .json is a single document with ``extracted_fields`` already
populated — this simulates what an EDMS connector returns. No PDFs.

5 loans with realistic arrival patterns:
    LOS-001 — clean salaried, 13 docs across 20 days, intraday burst Day 8
    LOS-002 — self-employed, 19 docs trickling across 40 days
    LOS-003 — joint application, primary + co-borrower staggered, 35 days
    LOS-004 — problem loan, corrections + appraisal gap, never closes
    LOS-005 — fast close, 22 docs in 12 days, intraday burst Day 5

The values are internally consistent per loan so the assemblers,
reconciler, and conflict thresholds all see the right shape.

Usage:
    python scripts/generate_s3_simulation.py
    python scripts/generate_s3_simulation.py --out /tmp/sim --start 2026-03-01

When ``S3_SIMULATION_BUCKET`` is set, the script can be extended to
upload to a real S3 bucket — the local-filesystem path is the
implementation here; the S3 boto3 path is left as a stub.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# Allow ``python scripts/...`` from the repo root.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


DEFAULT_OUT = "local_storage/s3_simulation"
DEFAULT_START_DATE = "2026-01-01"
DEFAULT_NUM_DAYS = 50  # Jan 1 → Feb 19 inclusive

# Per-loan stable identity facts. Keep these consistent with the
# extracted_fields below so the reconciler doesn't fire spurious
# contradicts edges (e.g. employer_name fuzzy match on PAYSTUB vs W2).
LOAN_PROFILES: dict = {
    "LOS-001": {
        "primary_name": "Alex Martinez", "primary_dob": "1985-06-20",
        "primary_ssn4": "4567",          "co_name": None,
        "income_annual": 125000, "credit_mid": 752,
        "purchase_price": 450000, "appraised_value": 462000,
        "employer": "TechCorp Inc",
    },
    "LOS-002": {
        "primary_name": "Maria Rivera",  "primary_dob": "1978-11-03",
        "primary_ssn4": "8821",          "co_name": None,
        "income_annual": 109000,  # 42 W2 + 67 SE
        "credit_mid": 698,
        "purchase_price": 379000, "appraised_value": 385000,
        "employer": "Self-Employed",
    },
    "LOS-003": {
        "primary_name": "Jordan Chen",   "primary_dob": "1989-04-15",
        "primary_ssn4": "2299",
        "co_name": "Riley Chen",         "co_dob": "1990-09-22",
        "co_ssn4": "1188",
        "income_annual": 163000,         # 95 + 68
        "primary_income": 95000,         "co_income": 68000,
        "credit_mid": 740,               "co_credit_mid": 720,
        "purchase_price": 520000, "appraised_value": 525000,
        "employer": "BigBank",           "co_employer": "RetailCo",
    },
    "LOS-004": {
        "primary_name": "Sam Davis",     "primary_dob": "1982-02-08",
        "primary_ssn4": "9933",          "co_name": None,
        "income_annual": 88000, "credit_mid": 680,
        "purchase_price": 365000, "appraised_value": 340000,  # gap
        "employer": "RegionalCorp",
        # The loan also corrects the W2 wages on Day 15.
        "wages_initial": 78000, "wages_corrected": 88000,
        # Second appraisal Day 18.
        "appraised_value_v2": 355000,
    },
    "LOS-005": {
        "primary_name": "Quinn Patel",   "primary_dob": "1990-07-12",
        "primary_ssn4": "5544",          "co_name": None,
        "income_annual": 155000, "credit_mid": 780,
        "purchase_price": 600000, "appraised_value": 615000,
        "employer": "FinTechCo",
    },
}


# Arrival schedule: (day_offset, hour, doc_type, role). Day_offset is
# 1-indexed (Day 1 = START_DATE). Same-day multiple entries simulate
# the intraday-update case.
SCHEDULE: dict = {
    "LOS-001": [
        # Clean salaried — Day 1 application, complete by Day 20.
        (1,  9, "URLA_1003",         "primary"),
        (1, 10, "W2_CURRENT",        "primary"),
        (1, 11, "CREDIT_REPORT",     "primary"),
        (2,  9, "PAYSTUB_CURRENT",   "primary"),
        (2, 14, "BANK_STATEMENT_M1", "primary"),
        (3, 10, "BANK_STATEMENT_M2", "primary"),
        (5,  9, "VOE_TWN",           "primary"),
        (5, 11, "DRIVERS_LICENSE",   "primary"),
        (7, 14, "APPRAISAL_URAR",    "primary"),
        # Day 8 intraday burst — 4 docs at staggered times.
        (8,  9, "TITLE_COMMITMENT",  "primary"),
        (8, 10, "HOI_BINDER",        "primary"),
        (8, 14, "SSN_VALIDATION",    "primary"),
        (8, 16, "OFAC_CHECK",        "primary"),
        (10, 9, "FLOOD_CERT",        "primary"),
        (10, 11, "PROPERTY_TAX_BILL", "primary"),
        (12, 14, "PURCHASE_AGREEMENT", "primary"),
        (15, 10, "AUS_DU_FINDINGS",  "primary"),
        (18, 13, "RATE_LOCK",        "primary"),
        (20, 11, "TITLE_INSURANCE",  "primary"),
    ],
    "LOS-002": [
        # Self-employed — slow, 40-day arc.
        (1,  9, "URLA_1003",                  "primary"),
        (3, 11, "W2_CURRENT",                 "primary"),
        (5, 10, "SCHEDULE_C",                 "primary"),
        (5, 14, "FORM_1040",                  "primary"),
        (8,  9, "IRS_TRANSCRIPT",             "primary"),
        (8, 11, "K1_SCHEDULE",                "primary"),
        (10, 9, "BANK_STATEMENT_M1",          "primary"),
        (10, 10, "BANK_STATEMENT_M2",         "primary"),
        (12, 14, "CREDIT_REPORT",             "primary"),
        (15,  9, "1099_NEC",                  "primary"),
        (15, 11, "1099_NEC",                  "primary"),  # second payer
        (18, 14, "APPRAISAL_URAR",            "primary"),
        (22, 10, "GIFT_LETTER",               "primary"),
        (22, 11, "RETIREMENT_ACCOUNT",        "primary"),
        (25,  9, "PURCHASE_AGREEMENT",        "primary"),
        (25, 14, "TITLE_COMMITMENT",          "primary"),
        (30, 10, "AUS_DU_FINDINGS",           "primary"),
        (35,  9, "RATE_LOCK",                 "primary"),
        (35, 11, "HOI_BINDER",                "primary"),
        (35, 14, "FLOOD_CERT",                "primary"),
        (40, 10, "TITLE_INSURANCE",           "primary"),
    ],
    "LOS-003": [
        # Co-borrower — staggered income docs.
        (2,  9, "URLA_1003",         "primary"),
        (4,  9, "W2_CURRENT",        "primary"),
        (4, 10, "CREDIT_REPORT",     "primary"),
        (5, 11, "W2_CURRENT",        "co_borrower"),
        (7,  9, "PAYSTUB_CURRENT",   "primary"),
        (7, 10, "PAYSTUB_CURRENT",   "co_borrower"),
        (10, 9, "BANK_STATEMENT_M1", "primary"),
        (10, 11, "DRIVERS_LICENSE",  "primary"),
        (12, 14, "SSN_VALIDATION",   "primary"),
        (12, 15, "OFAC_CHECK",       "primary"),
        (15,  9, "APPRAISAL_URAR",   "primary"),
        (15, 11, "VOE_TWN",          "primary"),
        (20, 10, "PURCHASE_AGREEMENT", "primary"),
        (20, 14, "HOI_BINDER",       "primary"),
        (25,  9, "TITLE_COMMITMENT", "primary"),
        (25, 14, "FLOOD_CERT",       "primary"),
        (30, 10, "AUS_DU_FINDINGS",  "primary"),
        (30, 14, "RATE_LOCK",        "primary"),
        (35, 11, "TITLE_INSURANCE",  "primary"),
    ],
    "LOS-004": [
        # Problem loan — corrections + appraisal gap; never closes.
        (3,  9, "URLA_1003",         "primary"),
        (3, 10, "W2_CURRENT",        "primary"),     # initial wages
        (3, 11, "CREDIT_REPORT",     "primary"),
        (6, 10, "PAYSTUB_CURRENT",   "primary"),
        (10, 14, "APPRAISAL_URAR",   "primary"),     # value $340k
        (12,  9, "PURCHASE_AGREEMENT", "primary"),   # price $365k — gap
        (15, 11, "W2_CURRENT",       "primary"),     # CORRECTED wages
        (18, 14, "APPRAISAL_URAR",   "primary"),     # 2nd appraisal $355k
        (22, 10, "CREDIT_EXPLANATION", "primary"),
        (25, 14, "BANK_STATEMENT_M1", "primary"),
        (30, 11, "AUS_DU_FINDINGS",  "primary"),     # Refer
        # No rate lock, no title — left incomplete.
    ],
    "LOS-005": [
        # Fast close — 22 docs in 12 days.
        # Day 5 intraday burst — 5 docs in one morning.
        (5,  8, "URLA_1003",         "primary"),
        (5,  9, "W2_CURRENT",        "primary"),
        (5, 10, "W2_PRIOR",          "primary"),
        (5, 11, "PAYSTUB_CURRENT",   "primary"),
        (5, 13, "CREDIT_REPORT",     "primary"),
        (6,  9, "BANK_STATEMENT_M1", "primary"),
        (6, 10, "BANK_STATEMENT_M2", "primary"),
        (6, 14, "DRIVERS_LICENSE",   "primary"),
        (7,  9, "VOE_TWN",           "primary"),
        (7, 10, "SSN_VALIDATION",    "primary"),
        (7, 11, "OFAC_CHECK",        "primary"),
        (8,  9, "APPRAISAL_URAR",    "primary"),
        (8, 14, "PURCHASE_AGREEMENT", "primary"),
        (9,  9, "TITLE_COMMITMENT",  "primary"),
        (9, 10, "HOI_BINDER",        "primary"),
        (9, 14, "FLOOD_CERT",        "primary"),
        (10,  9, "PROPERTY_TAX_BILL", "primary"),
        (10, 14, "AUS_DU_FINDINGS",  "primary"),
        (12,  9, "RATE_LOCK",        "primary"),
        (12, 14, "TITLE_INSURANCE",  "primary"),
    ],
}


def _applicant_id(los_id: str, role: str) -> str:
    """Stable applicant_id derived from los_id + role. The backtest
    runner POSTs /loans for each unique LOS at run time which assigns
    real APL-NNNNN-P ids; the connector stamps these placeholder ids
    on each doc so a re-resolution by the runner can map them."""
    suffix = "P" if role == "primary" else "C"
    return f"APL-{los_id}-{suffix}"


def _doc_id(los_id: str, doc_type: str, role: str, day: int, hour: int) -> str:
    return f"DOC-{los_id}-{doc_type}-{role}-D{day:02d}-{hour:02d}"


def _extracted_fields(los_id: str, doc_type: str, role: str, day: int) -> dict:
    """Returns ``extracted_fields`` keyed to be consistent with the
    rest of the loan's docs. Per-loan branching keeps W2 wages,
    appraisal value, etc. internally consistent so the reconciler
    doesn't fire spurious contradicts."""
    p = LOAN_PROFILES[los_id]
    income = p["income_annual"]
    if los_id == "LOS-003" and role == "co_borrower":
        income = p["co_income"]
    elif los_id == "LOS-003" and role == "primary":
        income = p["primary_income"]
    if los_id == "LOS-004" and doc_type == "W2_CURRENT" and day < 15:
        wages = p["wages_initial"]
    elif los_id == "LOS-004" and doc_type == "W2_CURRENT" and day >= 15:
        wages = p["wages_corrected"]
    else:
        wages = income

    employer = p.get("employer")
    if los_id == "LOS-003" and role == "co_borrower":
        employer = p.get("co_employer", employer)
    name = p["primary_name"]
    if role == "co_borrower" and p.get("co_name"):
        name = p["co_name"]
    ssn4 = p["primary_ssn4"]
    if role == "co_borrower" and p.get("co_ssn4"):
        ssn4 = p["co_ssn4"]
    credit = p["credit_mid"]
    if los_id == "LOS-003" and role == "co_borrower":
        credit = p.get("co_credit_mid", credit)

    if doc_type in ("W2_CURRENT", "W2_PRIOR"):
        return {
            "box1_wages":    wages,
            "tax_year":      "2024" if doc_type == "W2_PRIOR" else "2025",
            "employer_name": employer,
            "employee_name": name,
            "ssn_last4":     ssn4,
        }
    if doc_type == "PAYSTUB_CURRENT":
        return {
            "ytd_gross":      round(wages * 0.42, 2),
            "gross_pay":      round(wages / 12, 2),
            "net_pay":        round(wages / 12 * 0.72, 2),
            "pay_period_end": "2026-04-30",
            "employer_name":  employer,
            "employee_name":  name,
            "annualized_ytd": wages,
        }
    if doc_type == "CREDIT_REPORT":
        return {
            "experian_score":     credit + 8,
            "equifax_score":      credit,
            "transunion_score":   credit - 7,
            "mid_score":          credit,
            "credit_band":        "prime" if credit >= 740 else (
                                  "near-prime" if credit >= 670 else "subprime"),
            "tradeline_count":    10,
            "total_monthly_obligations": 1450,
        }
    if doc_type.startswith("BANK_STATEMENT"):
        return {
            "bank_name":        "Chase Bank",
            "account_holder":   name,
            "ending_balance":   62000 if "M1" in doc_type else 58500,
            "months_count":     1,
        }
    if doc_type == "RETIREMENT_ACCOUNT":
        return {
            "account_type":         "401k",
            "balance":              165000,
            "vested_balance":       148500,
            "institution":          "Fidelity",
        }
    if doc_type == "GIFT_LETTER":
        return {
            "gift_amount":        20000,
            "donor_name":         "Family",
            "donor_relationship": "parent",
            "borrower_name":      name,
        }
    if doc_type == "URLA_1003":
        return {
            "loan_purpose":            "purchase",
            "loan_amount":             round(p["purchase_price"] * 0.8),
            "interest_rate":           6.50,
            "loan_term_months":        360,
            "property_type":           "SFR",
            "occupancy":               "primary",
            "borrower_name":           p["primary_name"],
            "borrower_ssn_last4":      p["primary_ssn4"],
            "borrower_dob":            p["primary_dob"],
            "co_borrower_name":        p.get("co_name"),
            "monthly_income_stated":   round(income / 12),
        }
    if doc_type == "PURCHASE_AGREEMENT":
        return {
            "purchase_price":      p["purchase_price"],
            "earnest_money":       5000,
            "closing_date":        "2026-07-15",
            "buyer_name":          name,
            "seller_name":         "Sample Seller",
        }
    if doc_type == "APPRAISAL_URAR":
        # LOS-004 second appraisal lands on Day 18 with the higher value.
        if los_id == "LOS-004" and day >= 18:
            value = p["appraised_value_v2"]
        else:
            value = p["appraised_value"]
        return {
            "appraised_value":   value,
            "property_type":     "SFR",
            "condition_rating":  "C3",
            "appraisal_date":    "2026-04-15",
        }
    if doc_type == "RATE_LOCK":
        return {
            "locked_rate":   6.50,
            "lock_expiry":   "2026-07-30",
            "lock_days":     60,
            "loan_amount":   round(p["purchase_price"] * 0.8),
            "loan_program":  "Conv 30yr fixed",
        }
    if doc_type == "TITLE_COMMITMENT":
        return {"title_commitment_id": f"TC-{los_id}", "lender_name": "EDMS Mortgage"}
    if doc_type == "TITLE_INSURANCE":
        return {"policy_number": f"TI-{los_id}", "coverage_amount": round(p["purchase_price"] * 0.8)}
    if doc_type == "HOI_BINDER":
        return {"annual_premium": 1800, "carrier": "StateFarm",
                "dwelling_coverage": p["appraised_value"], "deductible": 2500}
    if doc_type == "FLOOD_CERT":
        return {"flood_zone": "X", "sfha": False,
                "flood_insurance_required": False, "panel_number": "48453C0440K"}
    if doc_type == "PROPERTY_TAX_BILL":
        return {"annual_tax": round(p["appraised_value"] * 0.018),
                "assessed_value": round(p["appraised_value"] * 0.93),
                "tax_year": "2025"}
    if doc_type == "DRIVERS_LICENSE":
        return {"full_name": name, "license_number": f"TX-{ssn4}{day:02d}",
                "dob": p["primary_dob"], "state": "TX",
                "expiration": "2029-06-20"}
    if doc_type == "SSN_VALIDATION":
        return {"ssn_valid": True, "ssn_last4": ssn4, "match_score": 1.0}
    if doc_type == "OFAC_CHECK":
        return {"ofac_clear": True, "checked_at": "2026-01-08", "match_count": 0}
    if doc_type == "VOE_TWN":
        return {"employment_status": "A", "employer_name": employer,
                "base_pay_annual": income, "income_amount": income,
                "employment_verified": True}
    if doc_type == "AUS_DU_FINDINGS":
        if los_id == "LOS-004":
            return {"approved": False, "recommendation": "refer_with_caution",
                    "qualifying_income": income, "ltv": 92.0, "dti": 48.5}
        return {"approved": True, "recommendation": "approve_eligible",
                "qualifying_income": income,
                "ltv": round(p["purchase_price"] * 0.8 / p["appraised_value"] * 100, 1),
                "dti": 32.5}
    if doc_type == "SCHEDULE_C":
        return {"net_profit": 67000, "gross_receipts": 110000,
                "tax_year": "2025"}
    if doc_type == "FORM_1040":
        return {"agi": 109000, "total_income": 110000,
                "wages_line1": 42000, "schedule_c_income": 67000,
                "tax_year": "2025", "filing_status": "Single"}
    if doc_type == "IRS_TRANSCRIPT":
        return {"agi": 109000, "wages_salaries": 42000,
                "tax_year": "2025", "filing_status": "Single",
                "self_employment_income": 67000}
    if doc_type == "K1_SCHEDULE":
        return {"ordinary_income": 8500, "interest_income": 250,
                "partnership_name": "Maria Holdings LLC", "tax_year": "2025"}
    if doc_type == "1099_NEC":
        return {"nonemployee_compensation": 33500,
                "payer_name":        "ConsultingCo",
                "payer_tin":         "98-7654321",
                "recipient_name":    name,
                "tax_year":          "2025",
                "form_type":         "NEC"}
    if doc_type == "CREDIT_EXPLANATION":
        return {"explanation_type": "late_payment",
                "creditor":         "Capital One",
                "reason":           "medical hardship 2024-Q2",
                "resolved":         True}
    return {}


def _category_for(doc_type: str) -> str:
    if doc_type in {"URLA_1003", "PURCHASE_AGREEMENT", "RATE_LOCK"}:
        return "loan_terms"
    if doc_type in {"APPRAISAL_URAR", "TITLE_COMMITMENT", "TITLE_INSURANCE",
                    "HOI_BINDER", "FLOOD_CERT", "PROPERTY_TAX_BILL"}:
        return "property"
    if doc_type in {"CREDIT_REPORT", "CREDIT_EXPLANATION"}:
        return "credit"
    if doc_type in {"BANK_STATEMENT_M1", "BANK_STATEMENT_M2",
                    "RETIREMENT_ACCOUNT", "GIFT_LETTER"}:
        return "asset"
    if doc_type in {"DRIVERS_LICENSE", "SSN_VALIDATION", "OFAC_CHECK"}:
        return "identity"
    if doc_type in {"AUS_DU_FINDINGS", "VOE_TWN"}:
        return "vendor"
    return "income"


def build_doc(
    los_id: str, doc_type: str, role: str,
    day: int, hour: int, start_date: date,
) -> dict:
    received_at = datetime.combine(
        start_date + timedelta(days=day - 1),
        datetime.min.time().replace(hour=hour, minute=15),
        tzinfo=timezone.utc,
    )
    return {
        "document_id":       _doc_id(los_id, doc_type, role, day, hour),
        "document_type":     doc_type,
        "category":          _category_for(doc_type),
        "applicant_id":      _applicant_id(los_id, role),
        "application_id":    f"APP-{los_id}",
        "los_id":            los_id,
        "borrower_role":     role,
        "source":            "encompass",
        "received_at":       received_at.isoformat().replace("+00:00", "Z"),
        "extracted_fields":  _extracted_fields(los_id, doc_type, role, day),
    }


def write(out: Path, doc: dict, day: int, start_date: date) -> Path:
    folder_date = start_date + timedelta(days=day - 1)
    folder = out / folder_date.isoformat() / doc["los_id"]
    folder.mkdir(parents=True, exist_ok=True)
    # Use document_id as filename so multiple same-doc-type entries on the
    # same day (e.g. LOS-002 two 1099_NECs) don't overwrite each other.
    path = folder / f"{doc['document_id']}.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2, default=str)
    return path


def generate(out_dir: Path, start_date: date, num_days: int, clean: bool) -> dict:
    if clean and out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    written = 0
    by_loan: dict = {los: 0 for los in LOAN_PROFILES}
    for los_id, schedule in SCHEDULE.items():
        for day, hour, doc_type, role in schedule:
            if day > num_days:
                continue
            doc = build_doc(los_id, doc_type, role, day, hour, start_date)
            write(out_dir, doc, day, start_date)
            written += 1
            by_loan[los_id] += 1

    return {"total": written, "by_loan": by_loan, "out": str(out_dir)}


def _parse_s3_dest(dest: str) -> tuple[str, str]:
    """``s3://bucket/prefix[/]`` → ``(bucket, "prefix")`` (no trailing
    slash)."""
    raw = dest[len("s3://"):].lstrip("/")
    parts = raw.split("/", 1)
    bucket = parts[0]
    prefix = parts[1].strip("/") if len(parts) > 1 else ""
    return bucket, prefix


def upload_to_s3(local_dir: Path, dest: str, dry_run: bool = False) -> dict:
    """Walk every ``.json`` under ``local_dir`` and PUT it under
    ``s3://bucket/prefix/<relative-path>``. Mirrors the date-folder
    layout the connector expects so a single
    ``generate_s3_simulation.py --s3 s3://edms-simulator-loans/s3_simulation``
    populates the production bucket exactly the same way the local
    backtest harness uses it."""
    import boto3  # imported lazily — local-only runs don't need it.

    bucket, prefix = _parse_s3_dest(dest)
    s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))

    uploaded = 0
    bytes_total = 0
    for root, _dirs, files in os.walk(local_dir):
        for fn in files:
            if not fn.endswith(".json"):
                continue
            path = Path(root) / fn
            rel = path.relative_to(local_dir).as_posix()
            key = f"{prefix}/{rel}" if prefix else rel
            size = path.stat().st_size
            if dry_run:
                print(f"  [dry-run] would put: s3://{bucket}/{key} ({size} B)")
            else:
                with path.open("rb") as f:
                    s3.put_object(
                        Bucket=bucket,
                        Key=key,
                        Body=f.read(),
                        ContentType="application/json",
                    )
            uploaded += 1
            bytes_total += size

    return {
        "uploaded": uploaded,
        "bucket":   bucket,
        "prefix":   prefix,
        "bytes":    bytes_total,
        "dry_run":  dry_run,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out",   default=DEFAULT_OUT)
    ap.add_argument("--start", default=DEFAULT_START_DATE)
    ap.add_argument("--days",  type=int, default=DEFAULT_NUM_DAYS)
    ap.add_argument("--clean", action="store_true",
                    help="rm -rf the output dir before writing")
    ap.add_argument("--s3", default=None,
                    help="after generating locally, upload to this s3:// URL "
                         "(e.g. s3://edms-simulator-loans/s3_simulation)")
    ap.add_argument("--s3-only", action="store_true",
                    help="skip local generation; upload an existing local "
                         "tree (--out) to --s3")
    ap.add_argument("--dry-run", action="store_true",
                    help="print S3 PUTs without executing them")
    args = ap.parse_args()

    start = datetime.fromisoformat(args.start).date()
    out   = Path(args.out)

    if not args.s3_only:
        summary = generate(out, start, args.days, clean=args.clean)
        print(f"Wrote {summary['total']} docs to {summary['out']}")
        for los_id, n in summary["by_loan"].items():
            print(f"  {los_id}: {n} docs")

    if args.s3:
        if not args.s3.startswith("s3://"):
            print(f"ERROR: --s3 must be an s3:// URL, got {args.s3!r}",
                  file=sys.stderr)
            sys.exit(2)
        print(f"\nUploading to {args.s3} {'(dry-run) ' if args.dry_run else ''}…")
        s3_summary = upload_to_s3(out, args.s3, dry_run=args.dry_run)
        verb = "would upload" if args.dry_run else "uploaded"
        print(f"  {verb} {s3_summary['uploaded']} files "
              f"({s3_summary['bytes']:,} B) → "
              f"s3://{s3_summary['bucket']}/{s3_summary['prefix']}/")


if __name__ == "__main__":
    main()
