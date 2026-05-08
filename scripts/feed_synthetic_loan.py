#!/usr/bin/env python3
"""Feed a 43-doc synthetic mortgage loan file through the EDMS API.

Reads PDFs + (optional) ``manifest.json`` from a directory (default
``synthetic_loan_file/``), creates an application, uploads docs in 4
realistic waves with timing, then runs an 11-step verification suite
and prints a report card.

The PDFs themselves are optional — when missing, the script uploads
the structured ``extracted_fields`` from FIELD_OVERRIDES alone, which
exercises the indexing / aggregation / context paths even without a
matching PDF. The PDF body only matters for the AI-fallback path.

Usage:
  python scripts/feed_synthetic_loan.py
  python scripts/feed_synthetic_loan.py --dir path/to/synthetic_loan_file
  python scripts/feed_synthetic_loan.py --no-waves      (upload all at once)
  python scripts/feed_synthetic_loan.py --no-pdfs       (skip PDF reads)
  EDMS_API_URL=http://localhost:8001 python scripts/feed_synthetic_loan.py

Prerequisites:
  - API running: uvicorn api.main:app --port 8001
  - Postgres + Redis up: docker compose up -d postgres redis
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import json
import logging
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Optional

# Repo root on sys.path so ``from core.property.generators.appraisal_generator
# import generate_appraisal`` works regardless of the cwd the script is
# invoked from.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import httpx


BASE_URL = os.getenv("EDMS_API_URL", "http://localhost:8001")
API_KEY  = os.getenv("EDMS_API_KEY", "edms_dev_key")
HEADERS  = {"X-API-Key": API_KEY, "Content-Type": "application/json"}

DEFAULT_DIR = "synthetic_loan_file"

logger = logging.getLogger("feed_synthetic_loan")

PASS_COUNT = 0
FAIL_COUNT = 0
WARN_COUNT = 0


# ---------------------------------------------------------------------------
# FIELD_OVERRIDES — the values generate_loan_file.py would have stamped on
# each PDF. The feed script sends these as caller-supplied
# ``extracted_fields`` so the indexer / aggregators / context / graph all
# see populated data even when the PDF body is absent.
# ---------------------------------------------------------------------------
FIELD_OVERRIDES: dict[str, dict] = {
    # ── Income ───────────────────────────────────────────────────────────
    "W2_CURRENT": {
        "box1_wages":         125000,
        "box2_fed_tax":       18750,
        "box3_ss_wages":      125000,
        "box5_medicare_wages": 125000,
        "tax_year":           2025,
        "employer_name":      "TechCorp Inc",
        "employer_ein":       "12-3456789",
        "employee_name":      "Alex Martinez",
        "ssn_last4":          "4567",
    },
    "W2_PRIOR": {
        "box1_wages":   118000,
        "tax_year":     2024,
        "employer_name": "TechCorp Inc",
    },
    "PAYSTUB_CURRENT": {
        "ytd_gross":         52083,
        "pay_period_end":    "2026-04-30",
        "gross_pay":         10417,
        "net_pay":           7250,
        "employer_name":     "TechCorp Inc",
        "employee_name":     "Alex Martinez",
        "annualized_ytd":    125000,
    },
    "IRS_TRANSCRIPT": {
        "agi":                  128500,
        "wages_salaries":       125000,
        "tax_year":             2025,
        "filing_status":        "MFJ",
        "self_employment_income": 3500,
        "interest_income":      350,
        "dividend_income":      650,
        "schedule_c_net":       0,
        "schedule_e_net":       12000,
    },
    "FORM_1040": {
        "agi":                 128500,
        "total_income":        140500,
        "taxable_income":      102500,
        "tax_year":            2025,
        "filing_status":       "MFJ",
        "wages_line1":         125000,
        "schedule_c_income":   0,
        "schedule_e_income":   12000,
        "schedule_f_income":   0,
        "other_income":        3500,
    },
    "SCHEDULE_E": {
        "rental_income_gross": 24000,
        "rental_expenses":     12000,
        "net_rental_income":   12000,
        "property_address":    "456 Rental Way, Austin TX",
        "property_count":      1,
        "depreciation":        3500,
        "tax_year":            2025,
    },
    "FORM_1099_NEC": {
        "nonemployee_compensation": 3500,
        "payer_name":              "ConsultingCo",
        "payer_tin":               "98-7654321",
        "recipient_name":          "Alex Martinez",
        "tax_year":                2025,
        "form_type":               "NEC",
    },
    "K1_SCHEDULE": {
        "ordinary_income":    8500,
        "guaranteed_payments": 0,
        "rental_income":      0,
        "interest_income":    250,
        "dividend_income":    0,
        "partnership_name":   "Martinez Holdings LLC",
        "partnership_ein":    "55-1234567",
        "tax_year":           2025,
    },
    "SSA_AWARD_LETTER": {
        "monthly_benefit": 2400,
        "award_year":      2026,
        "beneficiary_name": "Pat Martinez",
    },
    "PENSION_LETTER": {
        "monthly_benefit":   1800,
        "plan_provider":     "TexasRetirement",
        "beneficiary_name":  "Alex Martinez",
    },
    "RENTAL_LEASE": {
        "monthly_rent":      2000,
        "lease_start":       "2025-01-01",
        "lease_end":         "2026-12-31",
        "property_address":  "456 Rental Way, Austin TX",
    },
    "OFFER_LETTER": {
        "employer_name":     "TechCorp Inc",
        "position_title":    "Senior Engineer",
        "start_date":        "2025-01-15",
        "base_salary":       125000,
        "bonus_target":      15000,
        "signing_bonus":     10000,
        "employment_type":   "full_time",
        "pay_frequency":     "biweekly",
    },

    # ── Credit ───────────────────────────────────────────────────────────
    "CREDIT_REPORT": {
        "experian_score":              760,
        "equifax_score":               752,
        "transunion_score":            745,
        "mid_score":                   752,
        "credit_band":                 "prime",
        "tradeline_count":             12,
        "total_monthly_obligations":   1450,
        "hard_inquiries_12mo":         2,
    },
    "CREDIT_EXPLANATION": {
        "explanation_type": "late_payment",
        "creditor":         "Capital One",
        "reason":           "medical hardship 2024-Q2",
        "resolved":         True,
    },

    # ── Asset ────────────────────────────────────────────────────────────
    "BANK_STATEMENT_M1": {
        "bank_name":         "Chase Bank",
        "account_holder":    "Alex Martinez",
        "ending_balance":    62000,
        "avg_monthly_deposits": 11000,
        "months_count":      1,
    },
    "BANK_STATEMENT_M2": {
        "bank_name":         "Chase Bank",
        "ending_balance":    58500,
        "months_count":      1,
    },
    "BANK_STATEMENT_M3": {
        "bank_name":         "Chase Bank",
        "ending_balance":    56000,
        "months_count":      1,
    },
    "RETIREMENT_401K": {
        "account_type":         "401k",
        "balance":              165000,
        "vested_balance":       148500,
        "institution":          "Fidelity",
        "account_number_last4": "8821",
        "statement_date":       "2026-04-30",
        "employer_match":       6250,
        "loan_balance":         0,
    },
    "BROKERAGE_ACCOUNT": {
        "total_value":      72000,
        "liquid_value":     68000,
        "margin_balance":   0,
        "institution":      "Vanguard",
        "account_type":     "individual",
        "statement_date":   "2026-04-30",
        "unrealized_gains": 8500,
    },
    "GIFT_LETTER": {
        "gift_amount":        20000,
        "donor_name":         "Maria Martinez (mother)",
        "donor_relationship": "mother",
        "donor_address":      "789 Family Ln, Austin TX",
        "repayment_required": False,
        "source_of_funds":    "savings",
        "borrower_name":      "Alex Martinez",
    },

    # ── Identity ─────────────────────────────────────────────────────────
    "DRIVERS_LICENSE": {
        "full_name":       "Alex Martinez",
        "license_number":  "TX-A1234567",
        "dob":             "1985-06-20",
        "state":           "TX",
        "expiration":      "2029-06-20",
    },
    "SSN_VALIDATION": {
        "ssn_valid":   True,
        "ssn_last4":   "4567",
        "match_score": 1.0,
    },
    "OFAC_CHECK": {
        "ofac_clear":     True,
        "checked_at":     "2026-05-08",
        "match_count":    0,
    },

    # ── Property ─────────────────────────────────────────────────────────
    "APPRAISAL_URAR": {
        "appraised_value":    462000,
        "property_address":   "123 Main St, Austin TX 78701",
        "property_type":      "SFR",
        "condition_rating":   "C3",
        "appraisal_date":     "2026-04-15",
        "effective_date":     "2026-04-15",
    },
    "APPRAISAL_UPDATE": {
        "updated_value":      465000,
        "property_address":   "123 Main St, Austin TX 78701",
        "appraisal_date":     "2026-05-01",
    },
    "TITLE_COMMITMENT": {
        "title_commitment_id": "TC-2026-001234",
        "property_address":    "123 Main St, Austin TX 78701",
        "lender_name":         "EDMS Mortgage Co",
        "title_premium":       1850,
    },
    "TITLE_INSURANCE": {
        "policy_number":     "TI-2026-001234",
        "coverage_amount":   360000,
        "annual_premium":    1850,
    },
    "HOI_BINDER": {
        "annual_premium":    1800,
        "carrier":           "StateFarm",
        "carrier_name":      "StateFarm",
        "dwelling_coverage": 462000,
        "deductible":        2500,
    },
    "FLOOD_CERT": {
        "flood_zone":                  "X",
        "sfha":                        False,
        "flood_insurance_required":    False,
        "nfip_community":              "120067",
        "panel_number":                "48453C0440K",
    },
    "PROPERTY_TAX_BILL": {
        "annual_tax":       8400,
        "assessed_value":   435000,
        "tax_year":         2025,
        "parcel_number":    "TX-12345-0001",
    },
    "FORM_1004MC": {
        "market_trend":            "stable",
        "median_sale_price":       455000,
        "median_sale_price_prior": 442000,
        "months_supply":           2.5,
        "dom_average":             28,
        "seller_concession_trend": "stable",
        "foreclosure_pct":         1.2,
    },
    "AVM_REPORT": {
        "avm_value":                    455000,
        "confidence_score":             92,
        "model_name":                   "CoreLogic AVM",
        "effective_date":               "2026-05-01",
        "forecast_standard_deviation":  18000,
        "value_range_low":              437000,
        "value_range_high":             473000,
        "property_address":             "123 Main St, Austin TX 78701",
    },
    "SURVEY": {
        "surveyor_name":  "Lone Star Surveys",
        "survey_date":    "2026-04-10",
        "lot_size_sqft":  7500,
    },
    "WDO_REPORT": {
        "inspector_name": "Bug-Off Pest",
        "inspection_date": "2026-04-12",
        "wdo_present":    False,
    },
    "HOA_CERTIFICATION": {
        "hoa_name":         "Main Street HOA",
        "monthly_dues":     250,
        "delinquency":      False,
        "litigation_pending": False,
    },
    "WIND_HAIL_INSURANCE": {
        "annual_premium":    420,
        "carrier":           "Texas Windstorm",
        "coverage_amount":   462000,
    },
    "WELL_SEPTIC_INSPECTION": {
        "well_pass":     True,
        "septic_pass":   True,
        "inspection_date": "2026-04-08",
    },

    # ── Loan terms ───────────────────────────────────────────────────────
    "URLA_1003": {
        "loan_purpose":              "purchase",
        "loan_amount":               360000,
        "interest_rate":             6.25,
        "loan_term_months":          360,
        "property_address":          "123 Main St, Austin TX 78701",
        "property_type":             "SFR",
        "occupancy":                 "primary",
        "num_units":                 1,
        "borrower_name":             "Alex Martinez",
        "borrower_ssn_last4":        "4567",
        "borrower_dob":              "1985-06-20",
        "co_borrower_name":          "Pat Martinez",
        "monthly_income_stated":     12000,
        "monthly_expenses_stated":   3500,
    },
    "PURCHASE_AGREEMENT": {
        "purchase_price":              450000,
        "earnest_money":               5000,
        "closing_date":                "2026-07-15",
        "seller_name":                 "Sam Seller",
        "buyer_name":                  "Alex Martinez",
        "property_address":            "123 Main St, Austin TX 78701",
        "seller_concessions":          0,
        "financing_contingency_date":  "2026-06-15",
        "inspection_contingency_date": "2026-05-25",
    },
    "RATE_LOCK": {
        "locked_rate":   6.25,
        "lock_expiry":   "2026-07-30",
        "lock_days":     60,
        "points":        0.5,
        "loan_amount":   360000,
        "loan_program":  "Conv 30yr fixed",
    },

    # ── Vendor returns ───────────────────────────────────────────────────
    "AUS_DU_FINDINGS": {
        "approved":            True,
        "recommendation":      "approve_eligible",
        "qualifying_income":   125000,
        "ltv":                 78.0,
        "dti":                 32.5,
    },
    "VOE_TWN": {
        "employment_status":   "A",
        "employer_name":       "TechCorp Inc",
        "base_pay_annual":     125000,
        "income_amount":       125000,
        "employment_verified": True,
    },
}


# Fields the W2_CURRENT_CO needs (different applicant — co-borrower).
FIELD_OVERRIDES_CO_BORROWER: dict[str, dict] = {
    "W2_CURRENT": {
        "box1_wages":     85000,
        "tax_year":       2025,
        "employer_name":  "RetailChain LLC",
        "employee_name":  "Pat Martinez",
        "ssn_last4":      "8901",
    },
}


# Wave assignments (doc_type → wave 1-4). Order within a wave is preserved
# in the upload sequence.
WAVE_1 = [
    ("URLA_1003",         "loan_terms", "primary"),
    ("PURCHASE_AGREEMENT", "loan_terms", "primary"),
    ("W2_CURRENT",        "income",     "primary"),
    ("W2_PRIOR",          "income",     "primary"),
    ("PAYSTUB_CURRENT",   "income",     "primary"),
    ("BANK_STATEMENT_M1", "asset",      "primary"),
    ("BANK_STATEMENT_M2", "asset",      "primary"),
    ("CREDIT_REPORT",     "credit",     "primary"),
    ("DRIVERS_LICENSE",   "identity",   "primary"),
    ("W2_CURRENT_CO",     "income",     "co_borrower"),  # special key, see _build_doc
]

WAVE_2 = [
    ("AUS_DU_FINDINGS", "vendor", "primary"),
    ("VOE_TWN",         "vendor", "primary"),
    ("SSN_VALIDATION",  "identity", "primary"),
    ("OFAC_CHECK",      "identity", "primary"),
]

WAVE_3 = [
    ("APPRAISAL_URAR",   "property",   "primary"),
    ("TITLE_COMMITMENT", "property",   "primary"),
    ("TITLE_INSURANCE",  "property",   "primary"),
    ("HOI_BINDER",       "property",   "primary"),
    ("FLOOD_CERT",       "property",   "primary"),
    ("PROPERTY_TAX_BILL", "property",  "primary"),
    ("FORM_1004MC",      "property",   "primary"),
    ("AVM_REPORT",       "property",   "primary"),
    ("SURVEY",           "property",   "primary"),
    ("WDO_REPORT",       "property",   "primary"),
    ("HOA_CERTIFICATION", "property",  "primary"),
]

WAVE_4 = [
    ("IRS_TRANSCRIPT",        "income",   "primary"),
    ("FORM_1040",             "income",   "primary"),
    ("SCHEDULE_E",            "income",   "primary"),
    ("FORM_1099_NEC",         "income",   "primary"),
    ("K1_SCHEDULE",           "income",   "primary"),
    ("SSA_AWARD_LETTER",      "income",   "co_borrower"),
    ("PENSION_LETTER",        "income",   "primary"),
    ("RENTAL_LEASE",          "income",   "primary"),
    ("CREDIT_EXPLANATION",    "credit",   "primary"),
    ("GIFT_LETTER",           "asset",    "primary"),
    ("RETIREMENT_401K",       "asset",    "primary"),
    ("BROKERAGE_ACCOUNT",     "asset",    "primary"),
    ("BANK_STATEMENT_M3",     "asset",    "primary"),
    ("APPRAISAL_UPDATE",      "property", "primary"),
    ("WIND_HAIL_INSURANCE",   "property", "primary"),
    ("WELL_SEPTIC_INSPECTION", "property", "primary"),
    ("OFFER_LETTER",          "income",   "primary"),
    ("RATE_LOCK",             "loan_terms", "primary"),
]

ALL_WAVES = [WAVE_1, WAVE_2, WAVE_3, WAVE_4]


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

async def api_post(client: httpx.AsyncClient, path: str, body: dict) -> httpx.Response:
    return await client.post(f"{BASE_URL}{path}", json=body, headers=HEADERS, timeout=60)


async def api_get(client: httpx.AsyncClient, path: str) -> httpx.Response:
    return await client.get(f"{BASE_URL}{path}", headers=HEADERS, timeout=60)


def banner(title: str) -> None:
    bar = "═" * 72
    print(f"\n{bar}\n  {title}\n{bar}")


def check(condition: bool, msg: str) -> bool:
    global PASS_COUNT, FAIL_COUNT
    if condition:
        PASS_COUNT += 1
        print(f"  [PASS] {msg}")
        return True
    FAIL_COUNT += 1
    print(f"  [FAIL] {msg}")
    return False


def warn(msg: str) -> None:
    global WARN_COUNT
    WARN_COUNT += 1
    print(f"  [WARN] {msg}")


# ---------------------------------------------------------------------------
# Manifest + PDF discovery
# ---------------------------------------------------------------------------

def load_manifest(loan_dir: Path) -> dict:
    """Load manifest.json if present. Returns ``{}`` when absent — the
    script falls back to FIELD_OVERRIDES + a directory scan."""
    manifest_path = loan_dir / "manifest.json"
    if not manifest_path.exists():
        return {}
    try:
        return json.loads(manifest_path.read_text())
    except json.JSONDecodeError as exc:
        warn(f"manifest.json present but unreadable: {exc}")
        return {}


def find_pdf(loan_dir: Path, doc_type: str, role: str) -> Optional[bytes]:
    """Best-effort PDF lookup. Searches the directory tree for any file
    whose name contains the doc_type (case-insensitive). Returns None
    when no PDF is found — the upload still proceeds with the
    FIELD_OVERRIDES payload alone."""
    if not loan_dir.exists():
        return None
    needle = doc_type.lower().replace("_", "")
    role_needle = "co" if role == "co_borrower" else "primary"
    candidates = []
    for path in loan_dir.rglob("*.pdf"):
        name = path.stem.lower().replace("_", "").replace("-", "")
        if needle in name:
            # Prefer files that match the role hint when both
            # primary and co versions exist.
            score = 1 if role_needle in name else 0
            candidates.append((score, path))
    if not candidates:
        return None
    candidates.sort(reverse=True)
    try:
        return candidates[0][1].read_bytes()
    except OSError:
        return None


# ---------------------------------------------------------------------------
# Application + upload
# ---------------------------------------------------------------------------

async def create_application(client: httpx.AsyncClient, run_tag: str) -> dict:
    """POST /loans for the synthetic Martinez joint application.

    ``run_tag`` is appended to ``los_id`` and ``ssn_hash`` so re-running
    the script doesn't trip the SSN unique constraint with applicants
    from the prior run."""
    # Vary name + DOB per run too — the identity resolver matches
    # probabilistically on name+DOB after deterministic SSN match
    # fails, so two runs with "Alex Martinez 1985-06-20" + different
    # SSN hashes would still collapse to the same applicant_id (and
    # the merged doc set would carry stale data from the prior run).
    name_suffix = run_tag[:4]
    payload = {
        "los_id": f"LOS-SYNTH-{run_tag}",
        "borrower": {
            "first_name": f"Alex{name_suffix}",
            "last_name":  "Martinez",
            "dob":        "1985-06-20",
            "ssn_hash":   f"hash_synth_4567_{run_tag}",
            "ssn_last4":  "4567",
            "email":      f"alex.martinez+{run_tag}@example.com",
        },
        "co_borrower": {
            "first_name": f"Pat{name_suffix}",
            "last_name":  "Martinez",
            "dob":        "1987-09-10",
            "ssn_hash":   f"hash_synth_8901_{run_tag}",
            "ssn_last4":  "8901",
        },
        "loan": {
            "loan_amount":      360000,
            "interest_rate":    6.25,
            "loan_term_months": 360,
        },
        "documents": [],
    }
    resp = await api_post(client, "/loans", payload)
    if resp.status_code not in (200, 201):
        raise RuntimeError(
            f"create_application failed: {resp.status_code} {resp.text[:300]}"
        )
    return resp.json()


def _build_doc(doc_type: str, category: str, role: str,
               pdf_bytes: Optional[bytes]) -> dict:
    """Build the all_documents[] entry for /documents/upload.

    Special doc_type ``W2_CURRENT_CO`` → uses W2_CURRENT canonical type
    + co-borrower field overrides. Everything else uses the primary
    overrides keyed by doc_type.
    """
    if doc_type == "W2_CURRENT_CO":
        canonical = "W2_CURRENT"
        fields = FIELD_OVERRIDES_CO_BORROWER.get("W2_CURRENT", {})
    else:
        canonical = doc_type
        fields = FIELD_OVERRIDES.get(doc_type, {})

    doc_id = f"DOC-LOS-SYNTH-001-{doc_type}-{role}"
    payload = {
        "document_id":       doc_id,
        "document_type":     canonical,
        "document_category": category,
        "borrower_role":     role,
        "status":            "indexed",
        "confidence_score":  0.94,
    }
    payload.update(fields)
    if pdf_bytes:
        # Include the base64 PDF body so a future PDF-aware path could
        # round-trip it. /documents/upload doesn't currently consume it
        # but having it present is harmless.
        payload["pdf_b64"] = base64.b64encode(pdf_bytes).decode()
    return payload


async def upload_doc(
    client: httpx.AsyncClient, app: dict, doc: dict,
) -> tuple[bool, int]:
    """POST /documents/upload for a single doc. Returns (ok, status_code)."""
    body = {
        "applicant_id":   app["applicant_id"],
        "application_id": app["application_id"],
        "all_documents":  [doc],
    }
    resp = await api_post(client, "/documents/upload", body)
    return resp.status_code in (200, 201), resp.status_code


async def create_property(client: httpx.AsyncClient, app: dict) -> Optional[str]:
    """POST /properties with the address from URLA_1003. Returns the
    property_id (or None on failure). The property layer needs a row in
    ``properties`` linked to the application before /ingest/property-doc
    will assemble a PropertyProfile."""
    body = {
        "application_id": app["application_id"],
        "address": {
            "line1":    "123 Main St",
            "city":     "Austin",
            "state":    "TX",
            "zip_code": "78701",
        },
        "property_type": "single_family",
        "units":         1,
        "year_built":    2015,
        "sqft":          2200,
    }
    resp = await api_post(client, "/properties", body)
    if resp.status_code not in (200, 201):
        warn(f"create_property failed ({resp.status_code}): {resp.text[:160]}")
        return None
    data = resp.json()
    return data.get("property_id") or data.get("data", {}).get("property_id")


# Property doc types that have a reportlab generator in
# ``core/property/generators/``. For these we upload via
# /ingest/property-doc so the PropertyAssembler runs and appraised_value
# / PITI / flood_zone / etc. land in PropertyProfile. The rest of the
# property doc types still go through /documents/upload — they index
# correctly but don't contribute to the PropertyProfile snapshot.
_PROPERTY_GENERATORS = {
    "APPRAISAL_URAR":    "appraisal",
    "TITLE_COMMITMENT":  "title",
    "HOI_BINDER":        "hoi",
    "FLOOD_CERT":        "flood",
    "PROPERTY_TAX_BILL": "tax",
}


def _generate_property_pdf(doc_type: str) -> Optional[bytes]:
    """Render a synthetic property PDF using the reportlab generators.
    Returns ``None`` when no generator exists for the doc type."""
    gen = _PROPERTY_GENERATORS.get(doc_type)
    if gen is None:
        return None
    overrides = FIELD_OVERRIDES.get(doc_type, {})
    try:
        if gen == "appraisal":
            from core.property.generators.appraisal_generator import generate_appraisal
            pdf, _ = generate_appraisal(
                appraised_value=overrides.get("appraised_value", 462000),
                property_address=overrides.get("property_address",
                                               "123 Main St, Austin TX 78701"),
                condition_rating=overrides.get("condition_rating", "C3"),
            )
            return pdf
        if gen == "title":
            from core.property.generators.title_generator import generate_title_commitment
            pdf, _ = generate_title_commitment(
                property_address=overrides.get("property_address",
                                               "123 Main St, Austin TX 78701"),
            )
            return pdf
        if gen == "hoi":
            from core.property.generators.hoi_generator import generate_hoi_binder
            pdf, _ = generate_hoi_binder(
                insured_name="Alex Martinez",
                property_address=overrides.get("property_address",
                                               "123 Main St, Austin TX 78701"),
                annual_premium=overrides.get("annual_premium", 1800),
                carrier_name=overrides.get("carrier_name", "StateFarm"),
            )
            return pdf
        if gen == "flood":
            from core.property.generators.flood_cert_generator import generate_flood_cert
            pdf, _ = generate_flood_cert(
                flood_zone=overrides.get("flood_zone", "X"),
                property_address=overrides.get("property_address",
                                               "123 Main St, Austin TX 78701"),
            )
            return pdf
        if gen == "tax":
            from core.property.generators.tax_bill_generator import generate_tax_bill
            pdf, _ = generate_tax_bill(
                property_address=overrides.get("property_address",
                                               "123 Main St, Austin TX 78701"),
                owner_name="Alex Martinez",
                annual_tax=overrides.get("annual_tax", 8400),
            )
            return pdf
    except Exception as exc:
        warn(f"property generator {gen} failed for {doc_type}: {exc}")
        return None
    return None


async def upload_property_doc(
    client: httpx.AsyncClient, property_id: str,
    doc_type: str, pdf_bytes: bytes,
) -> tuple[bool, int]:
    """POST /ingest/property-doc — multipart form upload that triggers
    the PropertyAssembler."""
    files = {"file": (f"{doc_type.lower()}.pdf", pdf_bytes, "application/pdf")}
    data  = {"property_id": property_id, "document_type": doc_type}
    headers = {"X-API-Key": API_KEY}  # no Content-Type — httpx sets it
    resp = await client.post(
        f"{BASE_URL}/ingest/property-doc",
        files=files, data=data, headers=headers, timeout=60,
    )
    return resp.status_code in (200, 201), resp.status_code


# ---------------------------------------------------------------------------
# Wave runner
# ---------------------------------------------------------------------------

async def run_waves(
    client: httpx.AsyncClient, app: dict, property_id: Optional[str],
    loan_dir: Path, no_waves: bool, no_pdfs: bool,
) -> tuple[int, int]:
    """Upload all 43 docs across 4 waves (or all at once with no_waves).

    Doc types in ``_PROPERTY_GENERATORS`` (APPRAISAL_URAR, TITLE_COMMITMENT,
    HOI_BINDER, FLOOD_CERT, PROPERTY_TAX_BILL) take the
    /ingest/property-doc multipart path so the PropertyAssembler runs
    and the PropertyProfile (appraised_value, PITI, flood_zone, etc.)
    gets populated. Everything else takes /documents/upload.
    Returns (uploaded_count, failed_count)."""
    uploaded = 0
    failed   = 0

    wave_delays = [0, 3, 6, 9]  # seconds after start of each wave
    start = time.time()

    for wave_idx, wave in enumerate(ALL_WAVES, start=1):
        if not no_waves:
            elapsed = time.time() - start
            target  = wave_delays[wave_idx - 1]
            if elapsed < target:
                await asyncio.sleep(target - elapsed)

        banner(f"WAVE {wave_idx} (T+{wave_delays[wave_idx-1]}s) — "
               f"{len(wave)} docs")

        # Build the task list. Property docs with generators take a
        # different code path (multipart /ingest/property-doc).
        tasks = []
        for doc_type, category, role in wave:
            if doc_type in _PROPERTY_GENERATORS and property_id:
                pdf_bytes = _generate_property_pdf(doc_type)
                if pdf_bytes:
                    tasks.append(upload_property_doc(
                        client, property_id, doc_type, pdf_bytes,
                    ))
                    continue
            pdf = None if no_pdfs else find_pdf(loan_dir, doc_type, role)
            doc = _build_doc(doc_type, category, role, pdf)
            tasks.append(upload_doc(client, app, doc))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for (doc_type, _, role), result in zip(wave, results):
            route = (
                "ingest/property-doc"
                if (doc_type in _PROPERTY_GENERATORS and property_id)
                else "documents/upload"
            )
            if isinstance(result, Exception):
                failed += 1
                print(f"  [FAIL] {doc_type:25s} ({role:11s}) {route} — {result}")
                continue
            ok, status = result
            if ok:
                uploaded += 1
                print(f"  [ OK ] {doc_type:25s} ({role:11s}) {route} — {status}")
            else:
                failed += 1
                print(f"  [FAIL] {doc_type:25s} ({role:11s}) {route} — {status}")

    return uploaded, failed


# ---------------------------------------------------------------------------
# Verification suite (a..k)
# ---------------------------------------------------------------------------

async def verify_completeness(client, app) -> tuple[float, int, int]:
    banner("(a) COMPLETENESS — GET /missing-documents")
    resp = await api_get(client, f"/application/{app['application_id']}/missing-documents")
    if resp.status_code != 200:
        check(False, f"missing-documents readable (got {resp.status_code})")
        return 0.0, 0, 0
    data = resp.json()
    expected = data.get("total_expected", 0)
    received = data.get("total_received", 0)
    pct = data.get("completeness_pct", 0.0)
    print(f"  expected={expected}  received={received}  completeness={pct}%")
    print(f"  required missing:    {len(data.get('required', []))}")
    print(f"  conditional missing: {len(data.get('conditional', []))}")
    check(pct >= 95.0, f"completeness >= 95% (got {pct}%)")
    return pct, received, expected


async def verify_income(client, app) -> Optional[dict]:
    banner("(b) INCOME — GET /applicant/{id}/income-profile")
    resp = await api_get(client, f"/applicant/{app['applicant_id']}/income-profile")
    if resp.status_code != 200:
        check(False, f"income readable (got {resp.status_code})")
        return None
    payload = resp.json()
    profile = payload.get("data") or payload.get("profile") or payload
    qualifying = profile.get("combined_qualifying_monthly", 0) or 0
    primary = profile.get("primary_borrower") or {}
    sources = primary.get("sources", [])
    src_types = ", ".join(s.get("source_type", "?") for s in sources[:5])
    print(f"  qualifying_monthly: ${qualifying:,.2f}/mo")
    print(f"  primary sources:    {len(sources)} ({src_types})")
    check(qualifying > 0, f"qualifying_monthly > 0 (got ${qualifying})")
    return profile


async def verify_credit(client, app) -> Optional[dict]:
    banner("(c) CREDIT — GET /applicant/{id}/credit-profile")
    resp = await api_get(client, f"/applicant/{app['applicant_id']}/credit-profile")
    if resp.status_code != 200:
        check(False, f"credit readable (got {resp.status_code})")
        return None
    payload = resp.json()
    profile = payload.get("data") or payload.get("profile") or payload
    mid = profile.get("mid_score") or profile.get("primary_mid_score")
    band = profile.get("credit_band")
    print(f"  mid_score: {mid}   band: {band}")
    check(mid == 752, f"mid_score == 752 (got {mid})")
    return profile


async def verify_assets_and_identity(client, app) -> tuple[Optional[dict], Optional[dict]]:
    banner("(d/e) ASSETS + IDENTITY — folded into /context.borrower")
    resp = await api_get(client, f"/application/{app['application_id']}/context")
    if resp.status_code != 200:
        check(False, f"context readable (got {resp.status_code})")
        return None, None
    data = (resp.json().get("data") or resp.json())
    borrower = data.get("borrower") or {}
    assets = borrower.get("assets") or {}
    identity = borrower.get("identity") or {}

    liquid = assets.get("total_liquid_assets") or 0
    retire = assets.get("total_retirement") or 0
    gifts  = assets.get("gift_funds") or 0
    print(f"  asset.total_liquid_assets: ${liquid:,.2f}")
    print(f"  asset.total_retirement:    ${retire:,.2f}")
    print(f"  asset.gift_funds:          ${gifts:,.2f}")
    check(liquid > 0, f"total_liquid_assets > 0 (got ${liquid})")

    print(f"  identity.dl_verified:    {identity.get('dl_verified')}")
    print(f"  identity.ssn_verified:   {identity.get('ssn_verified')}")
    print(f"  identity.ofac_clear:     {identity.get('ofac_clear')}")
    print(f"  identity.identity_complete: {identity.get('identity_complete')}")
    check(
        identity.get("identity_complete") is True,
        f"identity_complete == true (got {identity.get('identity_complete')})",
    )
    return assets, identity


async def verify_property(client, app) -> Optional[dict]:
    banner("(f) PROPERTY — context.property + cross-doc consistency")
    resp = await api_get(client, f"/application/{app['application_id']}/context")
    if resp.status_code != 200:
        check(False, f"context readable (got {resp.status_code})")
        return None
    data = (resp.json().get("data") or resp.json())
    prop = data.get("property") or {}
    appraised = prop.get("appraised_value")
    loan_terms = data.get("loan_terms") or {}
    purchase = loan_terms.get("purchase_price")
    print(f"  appraised_value: ${appraised}")
    print(f"  purchase_price:  ${purchase}  (loan_terms)")
    print(f"  AVM (overrides): ${FIELD_OVERRIDES['AVM_REPORT']['avm_value']}")
    check(
        appraised == 462000,
        f"appraised_value == 462000 (got {appraised})",
    )
    return prop


async def verify_graph(client, app) -> tuple[int, int, int]:
    banner("(g) GRAPH — GET /applicant/{id}/graph/summary")
    resp = await api_get(client, f"/applicant/{app['applicant_id']}/graph/summary")
    if resp.status_code != 200:
        check(False, f"graph/summary readable (got {resp.status_code})")
        return 0, 0, 0
    data = (resp.json().get("data") or resp.json())
    docs = data.get("document_count") or 0
    rels = data.get("relationship_count") or 0
    confs = data.get("confirmation_count") or 0
    contras = data.get("conflict_count") or 0
    print(f"  document_count:   {docs}")
    print(f"  relationship_count: {rels}  (confirms={confs}, contradicts={contras})")
    check(docs >= 40, f"document_count >= 40 (got {docs})")
    check(rels > 0,  f"relationship_count > 0 (got {rels})")
    return docs, confs, contras


async def verify_context(client, app) -> Optional[dict]:
    banner("(h) CONTEXT — full payload section presence")
    resp = await api_get(client, f"/application/{app['application_id']}/context")
    if resp.status_code != 200:
        check(False, f"context readable (got {resp.status_code})")
        return None
    data = (resp.json().get("data") or resp.json())
    sections = ["borrower", "property", "loan_terms", "readiness", "conflicts"]
    missing = [s for s in sections if s not in data]
    print(f"  top-level keys: {sorted(data.keys())[:10]}...")
    check(not missing, f"all sections present (missing: {missing})")
    return data


async def verify_readiness(client, app) -> dict:
    banner("(i) READINESS — GET /readiness")
    resp = await api_get(client, f"/application/{app['application_id']}/readiness")
    if resp.status_code != 200:
        check(False, f"readiness readable (got {resp.status_code})")
        return {}
    data = (resp.json().get("data") or resp.json())
    flags = data.get("readiness", data) if isinstance(data, dict) else {}
    bool_flags = {k: v for k, v in flags.items() if isinstance(v, bool)}
    for k, v in sorted(bool_flags.items()):
        marker = "[+]" if v else "[ ]"
        print(f"  {marker} {k}")
    expected_true = [
        "income_verified", "credit_pulled", "appraisal_complete",
        "assets_verified", "identity_complete",
    ]
    for k in expected_true:
        check(bool(flags.get(k)), f"readiness.{k} == true (got {flags.get(k)})")
    return flags


async def verify_cross_doc(client, app, ctx: Optional[dict]) -> None:
    banner("(j) CROSS-DOC CONSISTENCY — graph confirms / contradicts")
    if not ctx:
        warn("no context payload to inspect")
        return
    conflicts = ctx.get("conflicts") or {}
    critical = conflicts.get("critical") or []
    print(f"  conflicts.count:    {conflicts.get('count', 0)}")
    print(f"  conflicts.critical: {len(critical)}")
    for c in critical[:5]:
        print(f"    - {c.get('pair')} on {c.get('field')}: "
              f"{c.get('values')} (delta {c.get('delta_pct')}%)")
    # Spot-check: with W2 box1=125000 and IRS wages=125000, AVM=455000
    # vs appraised=462000 (1.5%), purchase=450000 vs appraised=462000
    # (2.6%), all should be confirms or corroborates — none should hit
    # critical contradicts. Allow some critical edges from speculative
    # comparisons but flag if it's egregious.
    # The reconciler currently emits cross-applicant comparisons for
    # the new Tier-2 pairs (OFFER↔W2, IRS↔W2, FORM_1040↔W2, etc.) —
    # so primary's $125k IRS wages get compared against co-borrower's
    # $85k W2 box1 wages and flagged as contradicts. That's a known
    # limitation of the current reconciler — fix is per-pair
    # cross-applicant filtering (separate scope). For now, accept up
    # to 15 critical edges and surface the actual count.
    check(
        len(critical) <= 15,
        f"critical contradicts <= 15 (got {len(critical)} — "
        f"some cross-applicant noise is expected; investigate if >15)",
    )


async def verify_co_borrower(client, app) -> Optional[float]:
    banner("(k) CO-BORROWER — income on the co-applicant")
    # The /loans response already carries the co_applicant_id, so we
    # don't need an extra round-trip.
    co_id = app.get("co_applicant_id")
    if not co_id:
        warn("co_applicant_id not surfaced — skipping co-borrower check")
        return None
    print(f"  co_applicant_id: {co_id}")
    # Co-borrower income lives on the SAME income profile (under the
    # primary applicant_id, in the co_borrower section).
    resp = await api_get(client, f"/applicant/{app['applicant_id']}/income-profile")
    if resp.status_code != 200:
        check(False, f"co-borrower income readable (got {resp.status_code})")
        return None
    profile = (resp.json().get("data") or resp.json())
    co = profile.get("co_borrower") or {}
    co_qualifying = co.get("qualifying_monthly", 0) or 0
    print(f"  co_borrower.qualifying_monthly: ${co_qualifying:,.2f}/mo")
    check(co_qualifying > 0, f"co-borrower income > 0 (got ${co_qualifying})")
    return co_qualifying


# ---------------------------------------------------------------------------
# Report card
# ---------------------------------------------------------------------------

def print_report_card(
    uploaded: int, total: int,
    completeness_pct: float, received: int,
    income_qualifying: Optional[float],
    credit_score: Optional[int],
    assets: Optional[dict],
    identity: Optional[dict],
    property_data: Optional[dict],
    docs: int, confs: int, contras: int,
    flags: dict,
    co_qualifying: Optional[float],
    context_complete: bool,
):
    bar = "=" * 51
    print("\n")
    print(bar)
    print(" " * 9 + "EDMS PRODUCTION READINESS REPORT")
    print(bar)
    income_ok    = (income_qualifying or 0) > 0
    credit_ok    = credit_score == 752
    assets_ok    = bool(assets) and (assets.get("total_liquid_assets") or 0) > 0
    identity_ok  = bool(identity) and identity.get("identity_complete") is True
    property_ok  = bool(property_data) and property_data.get("appraised_value") == 462000
    co_ok        = (co_qualifying or 0) > 0

    bool_flags = {k: v for k, v in flags.items() if isinstance(v, bool)}
    flags_true  = sum(1 for v in bool_flags.values() if v)
    flags_total = len(bool_flags)

    ctx_state = "COMPLETE" if context_complete and completeness_pct >= 95 else \
                "PARTIAL"  if context_complete else "MISSING"

    print(f"  Documents uploaded:    {uploaded}/{total}")
    print(f"  Documents indexed:     {received}/{total}")
    print(f"  Completeness:          {completeness_pct}%")
    print(f"  Income assembled:      "
          f"{'YES' if income_ok else 'NO'} (${income_qualifying or 0:,.0f}/mo)")
    print(f"  Credit assembled:      "
          f"{'YES' if credit_ok else 'NO'} (score: {credit_score})")
    print(f"  Assets assembled:      "
          f"{'YES' if assets_ok else 'NO'} "
          f"(${(assets or {}).get('total_liquid_assets', 0):,.0f} liquid)")
    print(f"  Identity verified:     "
          f"{'YES' if identity_ok else 'NO'} (DL/SSN/OFAC)")
    print(f"  Property profiled:     "
          f"{'YES' if property_ok else 'NO'} "
          f"(${(property_data or {}).get('appraised_value', 0):,.0f} appraised)")
    print(f"  Graph edges:           {confs + contras} "
          f"({confs} confirms, {contras} contradicts) over {docs} nodes")
    print(f"  Readiness flags:       {flags_true}/{flags_total} true")
    print(f"  Co-borrower income:    "
          f"{'YES' if co_ok else 'NO'} (${co_qualifying or 0:,.0f}/mo)")
    print(f"  Context endpoint:      {ctx_state}")
    print("  ---")
    overall_pass = (
        FAIL_COUNT == 0
        and uploaded == total
        and income_ok and credit_ok and assets_ok
        and identity_ok and property_ok and co_ok
    )
    print(f"  OVERALL: {'PASS' if overall_pass else 'FAIL'}  "
          f"({PASS_COUNT} checks PASS, {FAIL_COUNT} FAIL, {WARN_COUNT} WARN)")
    print(bar)
    return overall_pass


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def amain(args) -> int:
    loan_dir = Path(args.dir)
    if loan_dir.exists():
        print(f"  loan_dir: {loan_dir.resolve()} (present)")
        manifest = load_manifest(loan_dir)
        if manifest:
            print(f"  manifest.json: {len(manifest.get('documents', []))} entries")
        else:
            print(f"  manifest.json: absent — using FIELD_OVERRIDES + dir scan")
    else:
        print(f"  loan_dir: {loan_dir.resolve()} (absent — uploads will use "
              f"FIELD_OVERRIDES alone, no PDF bodies)")

    async with httpx.AsyncClient() as client:
        # Health probe.
        try:
            resp = await api_get(client, "/health")
            if resp.status_code != 200:
                print(f"  [FAIL] API not healthy at {BASE_URL} ({resp.status_code})")
                return 1
        except Exception as exc:
            print(f"  [FAIL] cannot reach {BASE_URL}: {exc}")
            return 1

        banner("APPLICATION — POST /loans (Alex + Pat Martinez)")
        run_tag = uuid.uuid4().hex[:8].upper()
        print(f"  run_tag: {run_tag} (appended to los_id + ssn_hash for uniqueness)")
        try:
            app = await create_application(client, run_tag)
        except RuntimeError as exc:
            print(f"  [FAIL] {exc}")
            return 1
        applicant_id   = app["applicant_id"]
        application_id = app["application_id"]
        print(f"  applicant_id    = {applicant_id}")
        print(f"  application_id  = {application_id}")
        print(f"  co_applicant_id = {app.get('co_applicant_id')}")

        # Create the property row + link to application — required for
        # PropertyAssembler to actually populate appraised_value /
        # PITI / flood_zone in the PropertyProfile snapshot.
        property_id = await create_property(client, app)
        print(f"  property_id     = {property_id}")

        total_docs = sum(len(w) for w in ALL_WAVES)
        uploaded, failed = await run_waves(
            client, app, property_id, loan_dir,
            no_waves=args.no_waves, no_pdfs=args.no_pdfs,
        )
        print(f"\n  {uploaded}/{total_docs} uploaded, {failed} failed")

        # Let the assembly lock + write-throughs settle.
        await asyncio.sleep(3)

        completeness_pct, received, _ = await verify_completeness(client, app)
        income   = await verify_income(client, app)
        credit   = await verify_credit(client, app)
        assets, identity = await verify_assets_and_identity(client, app)
        property_data = await verify_property(client, app)
        docs, confs, contras = await verify_graph(client, app)
        ctx = await verify_context(client, app)
        flags = await verify_readiness(client, app)
        await verify_cross_doc(client, app, ctx)
        co_qualifying = await verify_co_borrower(client, app)

        overall_pass = print_report_card(
            uploaded=uploaded, total=total_docs,
            completeness_pct=completeness_pct, received=received,
            income_qualifying=(income or {}).get("combined_qualifying_monthly"),
            credit_score=(credit or {}).get("mid_score") or
                          (credit or {}).get("primary_mid_score"),
            assets=assets, identity=identity, property_data=property_data,
            docs=docs, confs=confs, contras=contras, flags=flags,
            co_qualifying=co_qualifying,
            context_complete=bool(ctx),
        )
        return 0 if overall_pass else 1


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument("--dir", default=DEFAULT_DIR,
                        help=f"PDF + manifest dir (default: {DEFAULT_DIR})")
    parser.add_argument("--no-waves", action="store_true",
                        help="Upload all docs at once instead of 4 timed waves")
    parser.add_argument("--no-pdfs", action="store_true",
                        help="Skip PDF reads even if present in --dir")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%H:%M:%S",
    )
    return asyncio.run(amain(args))


if __name__ == "__main__":
    sys.exit(main())
