"""Generate 50 days of realistic mortgage-loan documents arriving across
nine source channels for ten diverse loans.

Output tree (one date folder per simulated day, one channel sub-folder
per source-system pattern)::

    local_storage/s3_simulation_v2/
        2026-01-01/
            edms_pull/             ← {document_id}.json (one doc each)
            email_inbox/           ← {name}.pdf.b64 + {name}_meta.json pairs
            borrower_portal/       ← {name}.{pdf,jpg}.b64 + {name}_meta.json
            los_encompass/         ← {los_id}_batch_{date}.json (JSON array)
            vendor_equifax/        ← {type}_{los_id}_{date}.json
            vendor_corelogic/      ← {type}_{los_id}_{date}.json
            vendor_title/          ← {name}.pdf.b64 + {name}_meta.json
            shared_drive/          ← scan_{ts}.pdf.b64 (NO metadata)
            ai_chat/               ← chat_{los_id}_{date}.json
        2026-01-02/
            ...

Documents intentionally vary in shape:

- ``edms_pull/`` — structured JSON only (FileNet/EDMS connector pull)
- ``email_inbox/`` — base64-encoded PDF + meta JSON pair
- ``borrower_portal/`` — same pair shape but with ``uploaded_by`` field
- ``los_encompass/`` — batched JSON array (multiple docs in one file)
- ``vendor_equifax/`` / ``vendor_corelogic/`` — single JSON per call
- ``vendor_title/`` — PDF + meta pair (title companies email PDFs)
- ``shared_drive/`` — raw PDF, NO metadata (system must AI-classify)
- ``ai_chat/`` — chat transcript JSON with extracted_fields

The loan IDs (``LOAN-101``..``LOAN-110``) carry no embedded
applicant_id — the system resolves los_id → applicant_id at upload
time so re-runs against a long-lived prod DB stay idempotent.

CLI:
    python scripts/generate_realworld_simulation.py             # generate locally
    python scripts/generate_realworld_simulation.py --clean     # rm -rf first
    python scripts/generate_realworld_simulation.py --upload    # also `aws s3 sync` to S3
    python scripts/generate_realworld_simulation.py --start 2026-03-01 --days 30
"""
from __future__ import annotations

import argparse
import base64
import io
import json
import os
import shutil
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# Format-aware PDF renderers (W2/paystub/bank/title/credit/appraisal each
# have 2-3 layout variants per real-world institution). The dispatcher
# falls back to a generic title+kv layout for doc types without a
# custom renderer (rate lock, HOI binder, gift letter, etc.).
from scripts import pdf_formats  # noqa: E402


DEFAULT_OUT       = "local_storage/s3_simulation_v2"
DEFAULT_S3_TARGET = "s3://edms-simulator-loans/s3_simulation_v2/"
DEFAULT_START     = "2026-01-01"
DEFAULT_DAYS      = 50


# ===========================================================================
# Loan profiles — 10 borrowers covering the most common mortgage scenarios.
# Income / credit / property values stay internally consistent per loan so
# the reconciler doesn't fire spurious contradicts.
# ===========================================================================


LOAN_PROFILES: dict = {
    "LOAN-101": {
        "primary_name":   "James Wilson",
        "primary_dob":    "1985-04-12",
        "primary_ssn4":   "1011",
        "income":         125000,
        "credit_mid":     752,
        "purchase_price": 450000,
        "appraised":      462000,
        "city":           "Austin",
        "state":          "TX",
        "employer":       "TechCorp Inc",
        "employer_ein":   "12-3456789",
        "bank":           "Chase Bank",
        "occupancy":      "primary_residence",
        "loan_purpose":   "purchase",
        "scenario":       "clean_salaried",
    },
    "LOAN-102": {
        "primary_name":   "Maria Garcia",
        "primary_dob":    "1979-08-22",
        "primary_ssn4":   "1022",
        "income":         109000,
        "wages_w2":       8500,
        "income_1099":    100500,    # 67k from one payer + 33.5k from another
        "credit_mid":     698,
        "purchase_price": 380000,
        "appraised":      385000,
        "city":           "San Antonio",
        "state":          "TX",
        "employer":       "Self-Employed",
        "bank":           "Wells Fargo",
        "occupancy":      "primary_residence",
        "loan_purpose":   "purchase",
        "scenario":       "self_employed",
    },
    "LOAN-103": {
        "primary_name":   "David Kim",
        "primary_dob":    "1988-02-15",
        "primary_ssn4":   "1033",
        "co_name":        "Sarah Kim",
        "co_dob":         "1989-11-03",
        "co_ssn4":        "1034",
        "income":         195000,
        "primary_income": 110000,
        "co_income":      85000,
        "credit_mid":     740,
        "co_credit_mid":  720,
        "purchase_price": 620000,
        "appraised":      625000,
        "city":           "Round Rock",
        "state":          "TX",
        "employer":       "Oracle",
        "co_employer":    "HealthSys",
        "bank":           "Bank of America",
        "occupancy":      "primary_residence",
        "loan_purpose":   "purchase",
        "scenario":       "joint_dual_income",
    },
    "LOAN-104": {
        "primary_name":   "Robert Johnson",
        "primary_dob":    "1955-07-19",
        "primary_ssn4":   "1044",
        "income":         50400,    # 4,200 / mo × 12
        "pension_monthly": 2800,
        "ssa_monthly":    1400,
        "credit_mid":     790,
        "purchase_price": 290000,
        "appraised":      295000,
        "city":           "Georgetown",
        "state":          "TX",
        "employer":       "Retired",
        "bank":           "Frost Bank",
        "occupancy":      "primary_residence",
        "loan_purpose":   "purchase",
        "scenario":       "retired_fixed_income",
    },
    "LOAN-105": {
        "primary_name":   "Amanda Chen",
        "primary_dob":    "1992-12-08",
        "primary_ssn4":   "1055",
        "income":         78000,
        "credit_mid":     715,
        "purchase_price": 350000,
        "appraised":      355000,
        "city":           "Pflugerville",
        "state":          "TX",
        "employer":       "Hill Country Nonprofit",
        "bank":           "Capital One",
        "gift_amount":    25000,
        "donor_name":     "Robert & Linda Chen",
        "occupancy":      "primary_residence",
        "loan_purpose":   "purchase",
        "scenario":       "first_time_gift",
    },
    "LOAN-106": {
        "primary_name":   "Carlos Rivera",
        "primary_dob":    "1981-03-17",
        "primary_ssn4":   "1066",
        "income":         158000,
        "wages_w2":       140000,
        "rental_income":  18000,
        "credit_mid":     760,
        "purchase_price": 425000,
        "appraised":      430000,
        "city":           "Dallas",
        "state":          "TX",
        "employer":       "Dell Technologies",
        "bank":           "Chase Bank",
        "occupancy":      "investment_property",
        "loan_purpose":   "purchase",
        "scenario":       "investment_rental",
    },
    "LOAN-107": {
        "primary_name":   "Jennifer Brown",
        "primary_dob":    "1980-06-25",
        "primary_ssn4":   "1077",
        "co_name":        "Mike Brown",
        "co_dob":         "1979-09-14",
        "co_ssn4":        "1078",
        "income":         165000,
        "primary_income": 90000,
        "co_income":      75000,
        "credit_mid":     735,
        "co_credit_mid":  748,
        "purchase_price": 0,         # refi — no purchase
        "current_balance": 295000,
        "appraised":      485000,
        "city":           "Austin",
        "state":          "TX",
        "employer":       "Indeed",
        "co_employer":    "AMD",
        "bank":           "USAA",
        "occupancy":      "primary_residence",
        "loan_purpose":   "refinance_rate_term",
        "scenario":       "refinance",
    },
    "LOAN-108": {
        "primary_name":   "Priya Patel",
        "primary_dob":    "1990-01-30",
        "primary_ssn4":   "1088",
        "income":         155000,
        "credit_mid":     770,
        "purchase_price": 500000,
        "appraised":      510000,
        "city":           "Frisco",
        "state":          "TX",
        "employer":       "Atlassian",
        "bank":           "Citibank",
        "visa_status":    "H1B",
        "occupancy":      "primary_residence",
        "loan_purpose":   "purchase",
        "scenario":       "h1b_visa_holder",
    },
    "LOAN-109": {
        "primary_name":   "Thomas O'Brien",
        "primary_dob":    "1983-10-04",
        "primary_ssn4":   "1099",
        "income":         112000,
        "wages_w2":       88000,
        "alimony_monthly": 2000,
        "credit_mid":     680,
        "purchase_price": 310000,
        "appraised":      315000,
        "city":           "Cedar Park",
        "state":          "TX",
        "employer":       "RegionalSoft",
        "bank":           "Wells Fargo",
        "occupancy":      "primary_residence",
        "loan_purpose":   "purchase",
        "scenario":       "post_divorce_alimony",
    },
    "LOAN-110": {
        "primary_name":   "Lisa Zhang",
        "primary_dob":    "1986-07-11",
        "primary_ssn4":   "1100",
        "co_name":        "Wei Zhang",
        "co_dob":         "1985-04-29",
        "co_ssn4":        "1101",
        "income":         202000,
        "primary_income": 130000,
        "co_income":      72000,
        "credit_mid":     755,
        "co_credit_mid":  740,
        "purchase_price": 475000,
        "appraised":      480000,
        "city":           "Austin",
        "state":          "TX",
        "employer":       "Google",
        "co_employer":    "Indeed",
        "bank":           "Chase Bank",
        "property_type":  "condo",
        "hoa_monthly":    420,
        "occupancy":      "primary_residence",
        "loan_purpose":   "purchase",
        "scenario":       "condo_hoa_heavy",
    },
}


# ===========================================================================
# Schedule — what arrives, on which day, via which channel, for each loan.
#
# Row shape: (day, hour, channel, doc_type | list_of_doc_types, role, extras)
#   - When ``channel == "los_encompass"`` and ``doc_type`` is a list, the
#     entries are bundled into a single batch JSON file.
#   - ``extras`` carries per-event hints (e.g. {"sender": "..."}, custom
#     extracted_fields overrides). Optional; defaults to ``{}``.
# ===========================================================================


SCHEDULE: dict = {
    "LOAN-101": [
        (1,  10, "los_encompass", ["URLA_1003", "CREDIT_REPORT"], "primary", {}),
        (2,   9, "edms_pull",     "W2_CURRENT",        "primary", {"source": "ADP_PAYROLL", "source_institution": "ADP Inc"}),
        (3,   9, "edms_pull",     "W2_PRIOR",          "primary", {"source": "ADP_PAYROLL", "source_institution": "ADP Inc"}),
        (3,  10, "edms_pull",     "PAYSTUB_CURRENT",   "primary", {"source": "ADP_PAYROLL", "source_institution": "ADP Inc"}),
        (4,  15, "vendor_corelogic", "AVM_REPORT",     "primary", {}),
        (4,  16, "vendor_corelogic", "FLOOD_CERT",     "primary", {}),
        (5,  10, "edms_pull",     "BANK_STATEMENT_M1", "primary", {"source": "CHASE_BANK", "source_institution": "Chase Bank"}),
        (5,  20, "borrower_portal", "DRIVERS_LICENSE", "primary", {"format": "jpg"}),
        (6,  10, "edms_pull",     "BANK_STATEMENT_M2", "primary", {"source": "CHASE_BANK", "source_institution": "Chase Bank"}),
        (7,  11, "vendor_equifax", "VOE_TWN",          "primary", {}),
        (7,  12, "vendor_equifax", "SSN_VALIDATION",   "primary", {}),
        (8,   9, "edms_pull",     "APPRAISAL_URAR",    "primary", {"source": "MERCURY_AMC", "source_institution": "Mercury Network"}),
        (8,  10, "vendor_title",  "TITLE_COMMITMENT",  "primary", {}),
        (8,  11, "borrower_portal", "OFAC_CHECK",      "primary", {}),
        (8,  14, "edms_pull",     "HOI_BINDER",        "primary", {"source": "STATEFARM", "source_institution": "StateFarm"}),
        (10,  9, "edms_pull",     "FLOOD_CERT",        "primary", {"source": "CORELOGIC", "source_institution": "CoreLogic"}),
        (10, 10, "edms_pull",     "PROPERTY_TAX_BILL", "primary", {"source": "TRAVIS_TAX", "source_institution": "Travis County"}),
        (12, 14, "email_inbox",   "PURCHASE_AGREEMENT", "primary", {"sender": "james.wilson@email.com", "subject": "Signed purchase agreement"}),
        (15, 10, "los_encompass", ["AUS_DU_FINDINGS"], "primary", {}),
        (18, 13, "los_encompass", ["RATE_LOCK"],       "primary", {}),
        (20, 11, "vendor_title",  "TITLE_INSURANCE",   "primary", {}),
    ],
    "LOAN-102": [
        (1,   9, "los_encompass", ["URLA_1003"],       "primary", {}),
        (3,  11, "edms_pull",     "W2_CURRENT",        "primary", {"source": "ADP_PAYROLL", "source_institution": "ADP Inc"}),
        (5,  10, "email_inbox",   "SCHEDULE_C",        "primary", {"sender": "maria.garcia@email.com", "subject": "2025 Schedule C"}),
        (5,  11, "email_inbox",   "TAX_RETURN_1040_CURRENT", "primary", {"sender": "maria.garcia@email.com", "subject": "2025 Form 1040"}),
        (8,   9, "edms_pull",     "IRS_TRANSCRIPT",    "primary", {"source": "IRS_IVES", "source_institution": "IRS IVES"}),
        (8,  10, "email_inbox",   "K1_SCHEDULE",       "primary", {"sender": "maria.garcia@email.com", "subject": "K-1 from partnership"}),
        (10,  9, "edms_pull",     "BANK_STATEMENT_M1", "primary", {"source": "WELLS_FARGO", "source_institution": "Wells Fargo"}),
        (10, 10, "edms_pull",     "BANK_STATEMENT_M2", "primary", {"source": "WELLS_FARGO", "source_institution": "Wells Fargo"}),
        (12, 14, "los_encompass", ["CREDIT_REPORT"],   "primary", {}),
        # Day 15 — TWO 1099s in the same email_inbox folder.
        (15,  9, "email_inbox",   "1099_NEC",          "primary", {"sender": "ar@consultco.com", "subject": "1099-NEC for 2025", "payer_name": "ConsultCo", "amount": 67000, "doc_id_suffix": "consultco"}),
        (15, 11, "email_inbox",   "1099_NEC",          "primary", {"sender": "billing@designhub.com", "subject": "1099-NEC", "payer_name": "DesignHub", "amount": 33500, "doc_id_suffix": "designhub"}),
        (18, 14, "edms_pull",     "APPRAISAL_URAR",    "primary", {"source": "MERCURY_AMC", "source_institution": "Mercury Network"}),
        (22, 10, "borrower_portal", "GIFT_LETTER",     "primary", {}),
        (22, 11, "borrower_portal", "RETIREMENT_ACCOUNT", "primary", {}),
        (25,  9, "email_inbox",   "PURCHASE_AGREEMENT", "primary", {"sender": "agent@kw.com", "subject": "Signed contract — 1234 Broadway"}),
        (25, 14, "vendor_title",  "TITLE_COMMITMENT",  "primary", {}),
        (30, 10, "los_encompass", ["AUS_DU_FINDINGS"], "primary", {}),
        (35,  9, "los_encompass", ["RATE_LOCK"],       "primary", {}),
        (35, 11, "edms_pull",     "HOI_BINDER",        "primary", {"source": "ALLSTATE", "source_institution": "Allstate"}),
        (35, 14, "edms_pull",     "FLOOD_CERT",        "primary", {"source": "CORELOGIC", "source_institution": "CoreLogic"}),
        (40, 10, "vendor_title",  "TITLE_INSURANCE",   "primary", {}),
    ],
    "LOAN-103": [
        (2,   9, "los_encompass", ["URLA_1003"],       "primary",     {}),
        (4,   9, "edms_pull",     "W2_CURRENT",        "primary",     {"source": "ORACLE_PAYROLL", "source_institution": "Oracle"}),
        (4,  10, "los_encompass", ["CREDIT_REPORT"],   "primary",     {}),
        (5,  11, "edms_pull",     "W2_CURRENT",        "co_borrower", {"source": "HEALTHSYS_PAY", "source_institution": "HealthSys"}),
        (7,   9, "edms_pull",     "PAYSTUB_CURRENT",   "primary",     {"source": "ORACLE_PAYROLL", "source_institution": "Oracle"}),
        (7,  10, "edms_pull",     "PAYSTUB_CURRENT",   "co_borrower", {"source": "HEALTHSYS_PAY", "source_institution": "HealthSys"}),
        (10,  9, "edms_pull",     "BANK_STATEMENT_M1", "primary",     {"source": "BOA",  "source_institution": "Bank of America"}),
        (10, 11, "borrower_portal", "DRIVERS_LICENSE", "primary",     {"format": "jpg"}),
        # Day 12 — 4 identity docs in one cluster.
        (12,  9, "borrower_portal", "DRIVERS_LICENSE", "co_borrower", {"format": "jpg"}),
        (12, 10, "vendor_equifax", "SSN_VALIDATION",   "primary",     {}),
        (12, 11, "vendor_equifax", "SSN_VALIDATION",   "co_borrower", {}),
        (12, 14, "vendor_equifax", "OFAC_CHECK",       "primary",     {}),
        (15,  9, "edms_pull",     "APPRAISAL_URAR",    "primary",     {"source": "MERCURY_AMC", "source_institution": "Mercury Network"}),
        (15, 11, "vendor_equifax", "VOE_TWN",          "primary",     {}),
        (15, 13, "vendor_equifax", "VOE_TWN",          "co_borrower", {}),
        (20, 10, "email_inbox",   "PURCHASE_AGREEMENT", "primary",    {"sender": "david.kim@email.com", "subject": "Signed PA"}),
        (20, 14, "edms_pull",     "HOI_BINDER",        "primary",     {"source": "STATEFARM", "source_institution": "StateFarm"}),
        (25,  9, "vendor_title",  "TITLE_COMMITMENT",  "primary",     {}),
        (25, 14, "vendor_corelogic", "FLOOD_CERT",     "primary",     {}),
        (30, 10, "los_encompass", ["AUS_DU_FINDINGS", "RATE_LOCK"], "primary", {}),
        (35, 11, "vendor_title",  "TITLE_INSURANCE",   "primary",     {}),
    ],
    "LOAN-104": [
        (3,   9, "los_encompass", ["URLA_1003"],       "primary", {}),
        (5,  10, "edms_pull",     "SSA_AWARD_LETTER",  "primary", {"source": "SSA_GOV", "source_institution": "Social Security Administration"}),
        (5,  11, "edms_pull",     "PENSION_LETTER",    "primary", {"source": "PENSION_PLAN", "source_institution": "Texas Retirement"}),
        (7,   9, "edms_pull",     "BANK_STATEMENT_M1", "primary", {"source": "FROST",  "source_institution": "Frost Bank"}),
        (7,  10, "edms_pull",     "BANK_STATEMENT_M2", "primary", {"source": "FROST",  "source_institution": "Frost Bank"}),
        (10,  9, "los_encompass", ["CREDIT_REPORT"],   "primary", {}),
        (10, 11, "borrower_portal", "DRIVERS_LICENSE", "primary", {"format": "jpg"}),
        (15, 10, "edms_pull",     "APPRAISAL_URAR",    "primary", {"source": "MERCURY_AMC", "source_institution": "Mercury Network"}),
        (20, 11, "email_inbox",   "PURCHASE_AGREEMENT", "primary", {"sender": "robert.johnson@email.com", "subject": "Signed contract"}),
        (25,  9, "vendor_title",  "TITLE_COMMITMENT",  "primary", {}),
        (25, 10, "edms_pull",     "HOI_BINDER",        "primary", {"source": "FARMERS",  "source_institution": "Farmers Insurance"}),
        (25, 11, "edms_pull",     "FLOOD_CERT",        "primary", {"source": "CORELOGIC", "source_institution": "CoreLogic"}),
        (25, 12, "edms_pull",     "PROPERTY_TAX_BILL", "primary", {"source": "WMSON_TAX", "source_institution": "Williamson County"}),
        (30, 10, "los_encompass", ["AUS_DU_FINDINGS", "RATE_LOCK"], "primary", {}),
        (35, 11, "vendor_title",  "TITLE_INSURANCE",   "primary", {}),
    ],
    "LOAN-105": [
        (3,  10, "los_encompass", ["URLA_1003"],       "primary", {}),
        (5,   9, "edms_pull",     "W2_CURRENT",        "primary", {"source": "NPRO_PAYROLL", "source_institution": "Hill Country Nonprofit"}),
        (5,  10, "edms_pull",     "PAYSTUB_CURRENT",   "primary", {"source": "NPRO_PAYROLL", "source_institution": "Hill Country Nonprofit"}),
        (7,   9, "edms_pull",     "BANK_STATEMENT_M1", "primary", {"source": "CAP_ONE", "source_institution": "Capital One"}),
        (7,  10, "edms_pull",     "BANK_STATEMENT_M2", "primary", {"source": "CAP_ONE", "source_institution": "Capital One"}),
        (8,  11, "los_encompass", ["CREDIT_REPORT"],   "primary", {}),
        (10, 11, "borrower_portal", "DRIVERS_LICENSE", "primary", {"format": "jpg"}),
        (10, 12, "vendor_equifax", "SSN_VALIDATION",   "primary", {}),
        (12, 10, "email_inbox",   "GIFT_LETTER",       "primary", {"sender": "amanda.chen@gmail.com", "subject": "Gift letter from my parents"}),
        (12, 11, "edms_pull",     "GIFT_FUNDS_TRAIL",  "primary", {"source": "CAP_ONE", "source_institution": "Capital One"}),
        (15, 11, "vendor_equifax", "VOE_TWN",          "primary", {}),
        (18,  9, "edms_pull",     "APPRAISAL_URAR",    "primary", {"source": "MERCURY_AMC", "source_institution": "Mercury Network"}),
        (20, 10, "email_inbox",   "PURCHASE_AGREEMENT", "primary", {"sender": "amanda.chen@gmail.com", "subject": "Signed contract"}),
        (25,  9, "vendor_title",  "TITLE_COMMITMENT",  "primary", {}),
        (25, 10, "edms_pull",     "HOI_BINDER",        "primary", {"source": "ALLSTATE", "source_institution": "Allstate"}),
        (25, 11, "edms_pull",     "FLOOD_CERT",        "primary", {"source": "CORELOGIC", "source_institution": "CoreLogic"}),
        (30, 10, "los_encompass", ["AUS_DU_FINDINGS", "RATE_LOCK"], "primary", {}),
        (35, 11, "vendor_title",  "TITLE_INSURANCE",   "primary", {}),
    ],
    "LOAN-106": [
        (2,   9, "los_encompass", ["URLA_1003"],       "primary", {}),
        (4,   9, "edms_pull",     "W2_CURRENT",        "primary", {"source": "DELL_PAYROLL", "source_institution": "Dell Technologies"}),
        (6,  10, "email_inbox",   "RENTAL_LEASE",      "primary", {"sender": "carlos.rivera@email.com", "subject": "Existing rental lease"}),
        (6,  11, "email_inbox",   "SCHEDULE_E",        "primary", {"sender": "carlos.rivera@email.com", "subject": "2025 Schedule E"}),
        (8,   9, "edms_pull",     "BANK_STATEMENT_M1", "primary", {"source": "CHASE_BANK", "source_institution": "Chase Bank"}),
        (8,  10, "edms_pull",     "BANK_STATEMENT_M2", "primary", {"source": "CHASE_BANK", "source_institution": "Chase Bank"}),
        (8,  11, "edms_pull",     "BANK_STATEMENT_M3", "primary", {"source": "CHASE_BANK", "source_institution": "Chase Bank"}),
        (10, 11, "los_encompass", ["CREDIT_REPORT"],   "primary", {}),
        (12,  9, "edms_pull",     "APPRAISAL_URAR",    "primary", {"source": "MERCURY_AMC", "source_institution": "Mercury Network"}),
        (15, 10, "vendor_equifax", "VOE_TWN",          "primary", {}),
        (20, 11, "email_inbox",   "PURCHASE_AGREEMENT", "primary", {"sender": "carlos.rivera@email.com", "subject": "Investment property contract"}),
        (25,  9, "vendor_title",  "TITLE_COMMITMENT",  "primary", {}),
        (25, 10, "edms_pull",     "HOI_BINDER",        "primary", {"source": "TRAVELERS", "source_institution": "Travelers"}),
        (25, 11, "edms_pull",     "FLOOD_CERT",        "primary", {"source": "CORELOGIC", "source_institution": "CoreLogic"}),
        (30, 10, "los_encompass", ["AUS_DU_FINDINGS"], "primary", {}),
        (35, 11, "los_encompass", ["RATE_LOCK"],       "primary", {}),
        (40, 11, "vendor_title",  "TITLE_INSURANCE",   "primary", {}),
    ],
    # LOAN-107 is a refinance — NO purchase agreement, NO flood cert; tests
    # the missing-doc path.
    "LOAN-107": [
        (1,   9, "los_encompass", ["URLA_1003"],       "primary",     {}),
        (3,   9, "edms_pull",     "W2_CURRENT",        "primary",     {"source": "INDEED_PAYROLL", "source_institution": "Indeed"}),
        (3,  10, "edms_pull",     "W2_CURRENT",        "co_borrower", {"source": "AMD_PAYROLL", "source_institution": "AMD"}),
        (3,  11, "edms_pull",     "PAYSTUB_CURRENT",   "primary",     {"source": "INDEED_PAYROLL", "source_institution": "Indeed"}),
        (3,  12, "edms_pull",     "PAYSTUB_CURRENT",   "co_borrower", {"source": "AMD_PAYROLL", "source_institution": "AMD"}),
        (5,  10, "edms_pull",     "MORTGAGE_PAYOFF",   "primary",     {"source": "USAA",  "source_institution": "USAA Federal Savings"}),
        (7,   9, "los_encompass", ["CREDIT_REPORT"],   "primary",     {}),
        (7,  11, "edms_pull",     "BANK_STATEMENT_M1", "primary",     {"source": "USAA",  "source_institution": "USAA"}),
        (10,  9, "edms_pull",     "APPRAISAL_URAR",    "primary",     {"source": "MERCURY_AMC", "source_institution": "Mercury Network"}),
        (12, 11, "vendor_equifax", "VOE_TWN",          "primary",     {}),
        (12, 13, "vendor_equifax", "VOE_TWN",          "co_borrower", {}),
        (15, 10, "borrower_portal", "DRIVERS_LICENSE", "primary",     {"format": "jpg"}),
        (15, 11, "borrower_portal", "DRIVERS_LICENSE", "co_borrower", {"format": "jpg"}),
        (18, 10, "los_encompass", ["AUS_DU_FINDINGS"], "primary",     {}),
        (20, 11, "los_encompass", ["RATE_LOCK"],       "primary",     {}),
        (22,  9, "vendor_title",  "TITLE_COMMITMENT",  "primary",     {}),
        (25, 11, "vendor_title",  "TITLE_INSURANCE",   "primary",     {}),
    ],
    "LOAN-108": [
        (2,   9, "los_encompass", ["URLA_1003"],       "primary", {}),
        (4,   9, "edms_pull",     "W2_CURRENT",        "primary", {"source": "ATLASSIAN_PAY", "source_institution": "Atlassian"}),
        (4,  10, "edms_pull",     "PAYSTUB_CURRENT",   "primary", {"source": "ATLASSIAN_PAY", "source_institution": "Atlassian"}),
        (5,  10, "borrower_portal", "VISA_H1B",        "primary", {"format": "jpg"}),
        (5,  11, "borrower_portal", "EAD_CARD",        "primary", {"format": "jpg"}),
        (5,  12, "borrower_portal", "PASSPORT",        "primary", {"format": "jpg"}),
        (7,  10, "edms_pull",     "BANK_STATEMENT_M1", "primary", {"source": "CITI",  "source_institution": "Citibank"}),
        (7,  11, "edms_pull",     "BANK_STATEMENT_M2", "primary", {"source": "CITI",  "source_institution": "Citibank"}),
        (10,  9, "los_encompass", ["CREDIT_REPORT"],   "primary", {}),
        (12, 11, "vendor_equifax", "VOE_TWN",          "primary", {}),
        (15,  9, "edms_pull",     "APPRAISAL_URAR",    "primary", {"source": "MERCURY_AMC", "source_institution": "Mercury Network"}),
        (20, 10, "email_inbox",   "PURCHASE_AGREEMENT", "primary", {"sender": "priya.patel@email.com", "subject": "Signed PA"}),
        (25,  9, "vendor_title",  "TITLE_COMMITMENT",  "primary", {}),
        (25, 10, "edms_pull",     "HOI_BINDER",        "primary", {"source": "ALLSTATE", "source_institution": "Allstate"}),
        (25, 11, "edms_pull",     "FLOOD_CERT",        "primary", {"source": "CORELOGIC", "source_institution": "CoreLogic"}),
        (30, 10, "los_encompass", ["AUS_DU_FINDINGS", "RATE_LOCK"], "primary", {}),
        (35, 11, "vendor_title",  "TITLE_INSURANCE",   "primary", {}),
    ],
    "LOAN-109": [
        (3,   9, "los_encompass", ["URLA_1003"],       "primary", {}),
        (5,  10, "edms_pull",     "W2_CURRENT",        "primary", {"source": "REGSOFT_PAY", "source_institution": "RegionalSoft"}),
        (7,  10, "email_inbox",   "DIVORCE_DECREE",    "primary", {"sender": "thomas.obrien@email.com", "subject": "Final divorce decree"}),
        (7,  11, "email_inbox",   "ALIMONY_ORDER",     "primary", {"sender": "thomas.obrien@email.com", "subject": "Alimony court order"}),
        (10,  9, "edms_pull",     "BANK_STATEMENT_M1", "primary", {"source": "WELLS_FARGO", "source_institution": "Wells Fargo"}),
        (10, 10, "edms_pull",     "BANK_STATEMENT_M2", "primary", {"source": "WELLS_FARGO", "source_institution": "Wells Fargo"}),
        (10, 19, "ai_chat",       "CREDIT_EXPLANATION", "primary", {}),
        (12, 11, "los_encompass", ["CREDIT_REPORT"],   "primary", {}),
        (15, 10, "edms_pull",     "ALIMONY_RECEIPT_HISTORY", "primary", {"source": "WELLS_FARGO", "source_institution": "Wells Fargo"}),
        (15, 11, "vendor_equifax", "VOE_TWN",          "primary", {}),
        (18,  9, "edms_pull",     "APPRAISAL_URAR",    "primary", {"source": "MERCURY_AMC", "source_institution": "Mercury Network"}),
        (20, 11, "email_inbox",   "PURCHASE_AGREEMENT", "primary", {"sender": "thomas.obrien@email.com", "subject": "Contract"}),
        (25,  9, "vendor_title",  "TITLE_COMMITMENT",  "primary", {}),
        (25, 10, "edms_pull",     "HOI_BINDER",        "primary", {"source": "STATEFARM", "source_institution": "StateFarm"}),
        (25, 11, "edms_pull",     "FLOOD_CERT",        "primary", {"source": "CORELOGIC", "source_institution": "CoreLogic"}),
        (30, 10, "los_encompass", ["AUS_DU_FINDINGS", "RATE_LOCK"], "primary", {}),
        (35, 11, "vendor_title",  "TITLE_INSURANCE",   "primary", {}),
    ],
    "LOAN-110": [
        (1,   9, "los_encompass", ["URLA_1003"],       "primary",     {}),
        (3,   9, "edms_pull",     "W2_CURRENT",        "primary",     {"source": "GOOGLE_PAYROLL", "source_institution": "Google"}),
        (3,  10, "edms_pull",     "W2_CURRENT",        "co_borrower", {"source": "INDEED_PAYROLL", "source_institution": "Indeed"}),
        (5,   9, "edms_pull",     "PAYSTUB_CURRENT",   "primary",     {"source": "GOOGLE_PAYROLL", "source_institution": "Google"}),
        (5,  10, "edms_pull",     "PAYSTUB_CURRENT",   "co_borrower", {"source": "INDEED_PAYROLL", "source_institution": "Indeed"}),
        (7,  10, "edms_pull",     "BANK_STATEMENT_M1", "primary",     {"source": "CHASE_BANK", "source_institution": "Chase Bank"}),
        (10, 11, "los_encompass", ["CREDIT_REPORT"],   "primary",     {}),
        (12, 10, "edms_pull",     "APPRAISAL_URAR_1073", "primary",   {"source": "MERCURY_AMC", "source_institution": "Mercury Network"}),
        (15, 10, "edms_pull",     "HOA_CERT",          "primary",     {"source": "HOA_MGMT", "source_institution": "Skyline HOA Management"}),
        (15, 11, "edms_pull",     "HOA_INSURANCE_MASTER", "primary",  {"source": "HOA_MGMT", "source_institution": "Skyline HOA Management"}),
        (15, 12, "edms_pull",     "CONDO_QUESTIONNAIRE", "primary",   {"source": "HOA_MGMT", "source_institution": "Skyline HOA Management"}),
        (18, 11, "vendor_equifax", "VOE_TWN",          "primary",     {}),
        (18, 12, "vendor_equifax", "VOE_TWN",          "co_borrower", {}),
        (20, 11, "email_inbox",   "PURCHASE_AGREEMENT", "primary",    {"sender": "lisa.zhang@email.com", "subject": "Condo PA signed"}),
        (22, 10, "borrower_portal", "DRIVERS_LICENSE", "primary",     {"format": "jpg"}),
        (22, 11, "borrower_portal", "DRIVERS_LICENSE", "co_borrower", {"format": "jpg"}),
        (25,  9, "vendor_title",  "TITLE_COMMITMENT",  "primary",     {}),
        (28, 10, "edms_pull",     "HOI_BINDER_HO6",    "primary",     {"source": "ALLSTATE", "source_institution": "Allstate"}),
        (28, 11, "edms_pull",     "FLOOD_CERT",        "primary",     {"source": "CORELOGIC", "source_institution": "CoreLogic"}),
        (30, 10, "los_encompass", ["AUS_DU_FINDINGS", "RATE_LOCK"], "primary", {}),
        (35, 11, "vendor_title",  "TITLE_INSURANCE",   "primary",     {}),
    ],
}


# Unclassified scans dropped on the shared drive — system needs AI-vision
# to classify these. ``(day, hour, scan_text)``.
SHARED_DRIVE_DROPS: list = [
    (4,  16, "Loan officer's notes — meeting with James Wilson, recommend full doc package"),
    (14, 11, "Scanned divorce decree fragment — found in physical mail"),
    (27, 15, "Random property tax bill — borrower brought to office"),
    (44, 10, "Wet signature — purchase agreement copy mailed by seller's agent"),
]


# ===========================================================================
# Doc-content factory — extracted_fields keyed by doc_type, internally
# consistent with the loan profile.
# ===========================================================================


def _extracted_fields(
    doc_type: str, los_id: str, role: str, day: int, extras: dict,
) -> dict:
    p = LOAN_PROFILES[los_id]
    income = p.get("income", 0)
    credit = p["credit_mid"]
    if role == "co_borrower" and p.get("co_name"):
        income = p.get("co_income", income)
        credit = p.get("co_credit_mid", credit)
    elif p.get("primary_income"):
        income = p["primary_income"]

    employer = p.get("employer", "Self-Employed")
    if role == "co_borrower" and p.get("co_employer"):
        employer = p["co_employer"]
    name = p.get("co_name") if (role == "co_borrower" and p.get("co_name")) else p["primary_name"]
    ssn4 = p.get("co_ssn4") if (role == "co_borrower" and p.get("co_ssn4")) else p["primary_ssn4"]

    if doc_type in ("W2_CURRENT", "W2_PRIOR"):
        return {
            "box1_wages":    p.get("wages_w2", income),
            "box2_fed_tax":  round(income * 0.15),
            "box3_ss_wages": p.get("wages_w2", income),
            "tax_year":      "2024" if doc_type == "W2_PRIOR" else "2025",
            "employer_name": employer,
            "employer_ein":  p.get("employer_ein", "00-0000000"),
            "employee_name": name,
            "ssn_last4":     ssn4,
        }
    if doc_type == "PAYSTUB_CURRENT":
        return {
            "ytd_gross":      round(income * 0.42, 2),
            "gross_pay":      round(income / 12, 2),
            "net_pay":        round(income / 12 * 0.72, 2),
            "pay_period_end": "2026-04-30",
            "employer_name":  employer,
            "employee_name":  name,
        }
    if doc_type == "CREDIT_REPORT":
        return {
            "experian_score":     credit + 8,
            "equifax_score":      credit,
            "transunion_score":   credit - 7,
            "mid_score":          credit,
            "credit_band":        "prime" if credit >= 740 else (
                                  "near-prime" if credit >= 670 else "subprime"),
            "tradeline_count":    14,
            "total_monthly_obligations": 1450,
            "hard_inquiries_12mo": 2,
        }
    if doc_type.startswith("BANK_STATEMENT") or doc_type == "GIFT_FUNDS_TRAIL":
        suffix = doc_type.split("_")[-1] if "_" in doc_type else "M1"
        bal_map = {"M1": 62000, "M2": 58500, "M3": 56000}
        bal = bal_map.get(suffix, 60000)
        return {
            "bank_name":      p.get("bank", "Chase Bank"),
            "account_holder": name,
            "ending_balance": bal,
            "avg_monthly_deposits": round(income / 12 * 0.95, 2),
            "months_count":   1,
        }
    if doc_type == "RETIREMENT_ACCOUNT":
        return {"account_type": "401k", "balance": 145000, "vested_balance": 138000,
                "institution": "Fidelity"}
    if doc_type == "GIFT_LETTER":
        return {
            "gift_amount":        p.get("gift_amount", 25000),
            "donor_name":         p.get("donor_name", "Family"),
            "donor_relationship": "parent",
            "repayment_required": False,
            "borrower_name":      name,
        }
    if doc_type == "URLA_1003":
        return {
            "loan_purpose":          p.get("loan_purpose", "purchase"),
            "loan_amount":           round((p["purchase_price"] or p.get("current_balance", 0)) * 0.8) or p.get("current_balance", 0),
            "interest_rate":         6.50,
            "loan_term_months":      360,
            "occupancy":             p.get("occupancy", "primary_residence"),
            "borrower_name":         p["primary_name"],
            "borrower_dob":          p["primary_dob"],
            "borrower_ssn_last4":    p["primary_ssn4"],
            "co_borrower_name":      p.get("co_name"),
            "monthly_income_stated": round(income / 12),
            "subject_property_city": p["city"],
            "subject_property_state": p["state"],
        }
    if doc_type == "PURCHASE_AGREEMENT":
        return {
            "purchase_price":    p["purchase_price"],
            "earnest_money":     5000,
            "closing_date":      "2026-07-15",
            "buyer_name":        p["primary_name"],
            "seller_name":       "Sample Seller",
        }
    if doc_type in ("APPRAISAL_URAR", "APPRAISAL_URAR_1073"):
        return {
            "appraised_value": p.get("appraised", p["purchase_price"] + 5000),
            "property_type":   p.get("property_type", "SFR"),
            "condition_rating": "C3",
            "appraisal_form":  "1073" if doc_type.endswith("1073") else "URAR",
        }
    if doc_type == "AVM_REPORT":
        return {
            "avm_value":        round(p.get("appraised", p["purchase_price"]) * 0.985),
            "confidence_score": 0.87,
            "model":            "CoreLogic Total Home Value",
        }
    if doc_type == "RATE_LOCK":
        return {
            "locked_rate":  6.50,
            "lock_expiry":  "2026-07-30",
            "lock_days":    60,
            "loan_amount":  round((p["purchase_price"] or p.get("current_balance", 0)) * 0.8) or p.get("current_balance", 0),
            "loan_program": "Conv 30yr fixed",
        }
    if doc_type == "TITLE_COMMITMENT":
        return {"title_commitment_id": f"TC-{los_id}", "lender_name": "EDMS Mortgage"}
    if doc_type == "TITLE_INSURANCE":
        return {"policy_number": f"TI-{los_id}", "coverage_amount": round(p["purchase_price"] * 0.8) if p["purchase_price"] else p.get("current_balance", 0)}
    if doc_type in ("HOI_BINDER", "HOI_BINDER_HO6"):
        return {
            "annual_premium":   1800,
            "carrier":          extras.get("source_institution", "StateFarm"),
            "dwelling_coverage": p.get("appraised", p["purchase_price"]),
            "deductible":       2500,
            "policy_form":      "HO6" if doc_type.endswith("HO6") else "HO3",
        }
    if doc_type == "FLOOD_CERT":
        return {"flood_zone": "X", "sfha": False,
                "flood_insurance_required": False, "panel_number": "48453C0440K"}
    if doc_type == "PROPERTY_TAX_BILL":
        return {
            "annual_tax":     round(p.get("appraised", p["purchase_price"]) * 0.018),
            "assessed_value": round(p.get("appraised", p["purchase_price"]) * 0.93),
            "tax_year":       "2025",
        }
    if doc_type == "DRIVERS_LICENSE":
        return {
            "dl_number":   f"TX-{ssn4}{day:04d}",
            "state":       "TX",
            "expiry_date": "2028-06-15",
            "name_match":  True,
            "full_name":   name,
            "dob":         p["primary_dob"] if role == "primary" else p.get("co_dob", p["primary_dob"]),
        }
    if doc_type == "SSN_VALIDATION":
        return {"ssn_valid": True, "ssn_last4": ssn4, "match_score": 1.0}
    if doc_type == "OFAC_CHECK":
        return {"ofac_clear": True, "checked_at": "2026-01-08", "match_count": 0}
    if doc_type == "VOE_TWN":
        return {
            "employer_name":       employer,
            "employment_status":   "Active",
            "hire_date":           "2019-03-01",
            "income_amount":       income,
            "position":            "Senior Engineer" if "TechCorp" in employer or "Oracle" in employer else "Employee",
            "employment_verified": True,
        }
    if doc_type == "AUS_DU_FINDINGS":
        return {
            "approved":           p.get("scenario") != "self_employed",
            "recommendation":     "approve_eligible" if p.get("scenario") != "self_employed" else "refer_with_caution",
            "qualifying_income":  income,
            "ltv":                round((p["purchase_price"] or p.get("current_balance", 0)) * 0.8 / max(p["appraised"], 1) * 100, 1),
            "dti":                32.5,
            "case_id":            f"DU-{los_id}-001",
        }
    if doc_type == "SCHEDULE_C":
        return {
            "net_profit":      p.get("income_1099", 67000),
            "gross_receipts":  p.get("income_1099", 67000) + 18000,
            "tax_year":        "2025",
        }
    if doc_type == "TAX_RETURN_1040_CURRENT":
        return {
            "agi":               p.get("income", 109000),
            "total_income":      p.get("income", 109000) + 1500,
            "wages_line1":       p.get("wages_w2", 8500),
            "schedule_c_income": p.get("income_1099", 67000),
            "tax_year":          "2025",
            "filing_status":     "Single",
        }
    if doc_type == "IRS_TRANSCRIPT":
        return {
            "agi":               p.get("income", 109000),
            "wages_salaries":    p.get("wages_w2", 8500),
            "self_employment_income": p.get("income_1099", 67000),
            "tax_year":          "2025",
            "filing_status":     "Single",
        }
    if doc_type == "K1_SCHEDULE":
        return {
            "ordinary_income":   8500,
            "interest_income":   250,
            "partnership_name":  f"{name.split()[0]} Holdings LLC",
            "tax_year":          "2025",
        }
    if doc_type == "1099_NEC":
        return {
            "nonemployee_compensation": extras.get("amount", 67000),
            "payer_name":               extras.get("payer_name", "ConsultCo"),
            "payer_tin":                "98-7654321",
            "recipient_name":           name,
            "tax_year":                 "2025",
            "form_type":                "NEC",
        }
    if doc_type == "SSA_AWARD_LETTER":
        return {
            "monthly_benefit":   p.get("ssa_monthly", 1400),
            "award_year":        2026,
            "beneficiary_name":  name,
        }
    if doc_type == "PENSION_LETTER":
        return {
            "monthly_benefit":   p.get("pension_monthly", 2800),
            "plan_provider":     "Texas Retirement",
            "beneficiary_name":  name,
        }
    if doc_type == "RENTAL_LEASE":
        return {
            "monthly_rent":      1500,
            "lease_start":       "2025-01-01",
            "lease_end":         "2026-12-31",
            "property_address":  "456 Rental Way, Austin TX",
        }
    if doc_type == "SCHEDULE_E":
        return {
            "rental_income_gross": p.get("rental_income", 18000),
            "rental_expenses":     6000,
            "net_rental_income":   p.get("rental_income", 18000) - 6000,
            "tax_year":            "2025",
        }
    if doc_type == "MORTGAGE_PAYOFF":
        return {
            "current_balance":  p.get("current_balance", 295000),
            "payoff_through":   "2026-02-15",
            "lender":           "USAA Federal Savings",
            "loan_number":      f"PRIOR-{los_id}",
        }
    if doc_type == "CREDIT_EXPLANATION":
        return {
            "explanation_type": "late_payment",
            "creditor":         "Chase Visa",
            "reason":           "divorce_proceedings",
            "resolved":         True,
            "occurred":         "2024-12",
        }
    if doc_type == "DIVORCE_DECREE":
        return {"decree_date": "2024-09-15", "court": "Travis County District Court"}
    if doc_type == "ALIMONY_ORDER":
        return {
            "monthly_amount": p.get("alimony_monthly", 2000),
            "duration_months": 60,
            "court_order_id":  "ALIM-2024-09812",
        }
    if doc_type == "ALIMONY_RECEIPT_HISTORY":
        return {
            "months_received": 12,
            "monthly_amount":  p.get("alimony_monthly", 2000),
            "stable":          True,
        }
    if doc_type in ("VISA_H1B", "EAD_CARD", "PASSPORT"):
        return {
            "document_type":   doc_type,
            "holder_name":     name,
            "expiry_date":     "2027-08-15",
            "country":         "India" if doc_type == "PASSPORT" else None,
            "visa_status":     p.get("visa_status", "H1B"),
        }
    if doc_type == "HOA_CERT":
        return {
            "hoa_name":          "Skyline Condo HOA",
            "monthly_dues":      p.get("hoa_monthly", 420),
            "delinquency":       False,
            "litigation_pending": False,
            "reserves_funded_pct": 78,
        }
    if doc_type == "HOA_INSURANCE_MASTER":
        return {"carrier": "Travelers", "annual_premium": 42000, "deductible": 10000}
    if doc_type == "CONDO_QUESTIONNAIRE":
        return {"warrantable": True, "owner_occupied_pct": 82, "fha_approved": False}
    return {"placeholder": True, "doc_type": doc_type}


# ===========================================================================
# PDF + base64 helpers — dispatch through ``pdf_formats`` so each loan
# gets a format-appropriate layout (ADP / Paychex / Gusto W-2, Chase /
# Wells / BOA bank statements, etc.). Multi-page docs fan out within
# the renderer; the meta.json and the rendered PDF stay in lockstep.
# ===========================================================================


def _pdf_b64(
    doc_type: str, fields: dict, los_id: str, role: str = "primary",
) -> str:
    pdf_bytes = pdf_formats.make_pdf(doc_type, fields, los_id, role)
    return base64.b64encode(pdf_bytes).decode("ascii")


# ===========================================================================
# Per-channel writers
# ===========================================================================


def _doc_id(channel: str, los_id: str, doc_type: str, day: int, hour: int,
            suffix: str = "") -> str:
    prefix_map = {
        "edms_pull":         "EDMS",
        "email_inbox":       "EMAIL",
        "borrower_portal":   "PORTAL",
        "los_encompass":     "ENC",
        "vendor_equifax":    "EFX",
        "vendor_corelogic":  "CL",
        "vendor_title":      "TITLE",
        "shared_drive":      "DRIVE",
        "ai_chat":           "CHAT",
    }
    prefix = prefix_map.get(channel, channel.upper()[:5])
    base = f"{prefix}-2026-{los_id}-{doc_type}-D{day:02d}-{hour:02d}"
    return f"{base}-{suffix}" if suffix else base


def _category_for(doc_type: str) -> str:
    if doc_type in {"URLA_1003", "PURCHASE_AGREEMENT", "RATE_LOCK", "MORTGAGE_PAYOFF"}:
        return "loan_terms"
    if doc_type.startswith("APPRAISAL") or doc_type in {
        "TITLE_COMMITMENT", "TITLE_INSURANCE", "HOI_BINDER", "HOI_BINDER_HO6",
        "FLOOD_CERT", "PROPERTY_TAX_BILL", "AVM_REPORT", "HOA_CERT",
        "HOA_INSURANCE_MASTER", "CONDO_QUESTIONNAIRE",
    }:
        return "property"
    if doc_type in {"CREDIT_REPORT", "CREDIT_EXPLANATION"}:
        return "credit"
    if doc_type in {"BANK_STATEMENT_M1", "BANK_STATEMENT_M2", "BANK_STATEMENT_M3",
                    "RETIREMENT_ACCOUNT", "GIFT_LETTER", "GIFT_FUNDS_TRAIL"}:
        return "asset"
    if doc_type in {"DRIVERS_LICENSE", "SSN_VALIDATION", "OFAC_CHECK",
                    "VISA_H1B", "EAD_CARD", "PASSPORT"}:
        return "identity"
    if doc_type in {"AUS_DU_FINDINGS", "AUS_LP_FINDINGS"}:
        return "vendor"
    if doc_type in {"VOE_TWN", "VOE_EQUIFAX"}:
        return "employment"
    if doc_type in {"DIVORCE_DECREE", "ALIMONY_ORDER", "ALIMONY_RECEIPT_HISTORY"}:
        return "legal"
    return "income"


def _received_at(start_date: date, day: int, hour: int) -> str:
    ts = datetime.combine(
        start_date + timedelta(days=day - 1),
        datetime.min.time().replace(hour=hour, minute=15),
        tzinfo=timezone.utc,
    )
    return ts.isoformat().replace("+00:00", "Z")


def _channel_dir(out: Path, day: int, channel: str, start_date: date) -> Path:
    folder = (out / (start_date + timedelta(days=day - 1)).isoformat() / channel)
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def _write_edms_pull(out, los_id, doc_type, role, day, hour, extras, start_date):
    folder = _channel_dir(out, day, "edms_pull", start_date)
    fields = _extracted_fields(doc_type, los_id, role, day, extras)
    doc = {
        "document_id":        _doc_id("edms_pull", los_id, doc_type, day, hour),
        "document_type":      doc_type,
        "category":           _category_for(doc_type),
        "los_id":             los_id,
        "borrower_role":      role,
        "source_system":      extras.get("source", "EDMS"),
        "source_institution": extras.get("source_institution", "EDMS"),
        "source_channel":     "edms_pull",
        "received_at":        _received_at(start_date, day, hour),
        "extracted_fields":   fields,
    }
    with (folder / f"{doc['document_id']}.json").open("w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2, default=str)
    written = 1
    # When a format-aware renderer exists for this doc type (W-2, paystub,
    # bank stmt, credit report, appraisal, title), also drop a sibling
    # ``.pdf.b64`` so the rendered PDF is available for AI-Vision
    # verification + visual review. The connector keys on ``.json`` and
    # ignores the sibling, so this is purely additive — the JSON record
    # remains the source of truth indexed by the connector.
    fmt = pdf_formats.format_for(doc_type, los_id, role)
    if fmt is not None:
        pdf_bytes = pdf_formats.make_pdf(doc_type, fields, los_id, role)
        sibling = folder / f"{doc['document_id']}.pdf.b64"
        sibling.write_text(
            base64.b64encode(pdf_bytes).decode("ascii"), encoding="ascii",
        )
        written += 1
    return written


def _write_email_inbox(out, los_id, doc_type, role, day, hour, extras, start_date):
    folder = _channel_dir(out, day, "email_inbox", start_date)
    suffix = extras.get("doc_id_suffix", "")
    doc_id = _doc_id("email_inbox", los_id, doc_type, day, hour, suffix)
    fields = _extracted_fields(doc_type, los_id, role, day, extras)
    pdf_b64 = _pdf_b64(doc_type, fields, los_id, role)
    base = f"{doc_id}_email"
    (folder / f"{base}.pdf.b64").write_text(pdf_b64, encoding="ascii")
    meta = {
        "document_id":         doc_id,
        "los_id":              los_id,
        "source_system":       "EMAIL_INBOX",
        "source_channel":      "email_inbox",
        "sender":              extras.get("sender", "borrower@email.com"),
        "subject":             extras.get("subject", f"{doc_type} attached"),
        "received_at":         _received_at(start_date, day, hour),
        "attachment_filename": f"{doc_type.lower()}.pdf",
        "document_type":       doc_type,
        "category":            _category_for(doc_type),
        "borrower_role":       role,
        "extracted_fields":    fields,
    }
    with (folder / f"{base}_meta.json").open("w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, default=str)
    return 2


def _write_borrower_portal(out, los_id, doc_type, role, day, hour, extras, start_date):
    folder = _channel_dir(out, day, "borrower_portal", start_date)
    doc_id = _doc_id("borrower_portal", los_id, doc_type, day, hour)
    fields = _extracted_fields(doc_type, los_id, role, day, extras)
    fmt = extras.get("format", "pdf")
    base = f"{doc_id}_upload"
    pdf_b64 = _pdf_b64(doc_type, fields, los_id, role)
    (folder / f"{base}.{fmt}.b64").write_text(pdf_b64, encoding="ascii")
    p = LOAN_PROFILES[los_id]
    uploaded_by = (
        p.get("co_name", p["primary_name"]) if role == "co_borrower"
        else p["primary_name"]
    ).lower().replace(" ", ".") + "@email.com"
    meta = {
        "document_id":       doc_id,
        "los_id":            los_id,
        "source_system":     "BORROWER_PORTAL",
        "source_channel":    "borrower_portal",
        "uploaded_by":       uploaded_by,
        "received_at":       _received_at(start_date, day, hour),
        "original_filename": f"{doc_type.lower()}.{fmt}",
        "document_type":     doc_type,
        "category":          _category_for(doc_type),
        "borrower_role":     role,
        "extracted_fields":  fields,
    }
    with (folder / f"{base}_meta.json").open("w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, default=str)
    return 2


def _write_los_encompass_batch(out, los_id, doc_types, role, day, hour, extras,
                                start_date):
    folder = _channel_dir(out, day, "los_encompass", start_date)
    folder_date = (start_date + timedelta(days=day - 1)).isoformat()
    docs = []
    for i, dt in enumerate(doc_types):
        docs.append({
            "document_id":      _doc_id("los_encompass", los_id, dt, day, hour, str(i)),
            "document_type":    dt,
            "category":         _category_for(dt),
            "los_id":           los_id,
            "borrower_role":    role,
            "source_system":    "ENCOMPASS",
            "source_channel":   "los_encompass",
            "received_at":      _received_at(start_date, day, hour + i * 0),  # bundle stamps
            "extracted_fields": _extracted_fields(dt, los_id, role, day, extras),
        })
    fname = f"{los_id}_batch_{folder_date}_h{hour:02d}.json"
    with (folder / fname).open("w", encoding="utf-8") as f:
        json.dump(docs, f, indent=2, default=str)
    written = 1
    # For each batch entry that has a format-aware renderer (CREDIT_REPORT
    # is the main one — Encompass batches it together with URLA + AUS
    # findings), drop a sibling ``.pdf.b64`` named by document_id so the
    # rendered PDF is on disk for AI-Vision verification + visual review.
    # Connector ignores .pdf.b64 (suffix mismatch on the json scan), so
    # this stays additive.
    for d in docs:
        fmt = pdf_formats.format_for(d["document_type"], los_id, role)
        if fmt is None:
            continue
        pdf_bytes = pdf_formats.make_pdf(
            d["document_type"], d["extracted_fields"], los_id, role,
        )
        sibling = folder / f"{d['document_id']}.pdf.b64"
        sibling.write_text(
            base64.b64encode(pdf_bytes).decode("ascii"), encoding="ascii",
        )
        written += 1
    return written


def _write_vendor_equifax(out, los_id, doc_type, role, day, hour, extras, start_date):
    folder = _channel_dir(out, day, "vendor_equifax", start_date)
    doc_id = _doc_id("vendor_equifax", los_id, doc_type, day, hour)
    doc = {
        "document_id":        doc_id,
        "document_type":      doc_type,
        "category":           _category_for(doc_type),
        "los_id":             los_id,
        "borrower_role":      role,
        "source_system":      "EQUIFAX_TWN",
        "source_institution": "The Work Number",
        "source_channel":     "vendor_equifax",
        "received_at":        _received_at(start_date, day, hour),
        "extracted_fields":   _extracted_fields(doc_type, los_id, role, day, extras),
    }
    fname = f"{doc_type.lower()}_{los_id}_d{day:02d}_h{hour:02d}.json"
    with (folder / fname).open("w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2, default=str)
    return 1


def _write_vendor_corelogic(out, los_id, doc_type, role, day, hour, extras, start_date):
    folder = _channel_dir(out, day, "vendor_corelogic", start_date)
    doc_id = _doc_id("vendor_corelogic", los_id, doc_type, day, hour)
    doc = {
        "document_id":        doc_id,
        "document_type":      doc_type,
        "category":           _category_for(doc_type),
        "los_id":             los_id,
        "borrower_role":      role,
        "source_system":      "CORELOGIC",
        "source_institution": "CoreLogic",
        "source_channel":     "vendor_corelogic",
        "received_at":        _received_at(start_date, day, hour),
        "extracted_fields":   _extracted_fields(doc_type, los_id, role, day, extras),
    }
    fname = f"{doc_type.lower()}_{los_id}_d{day:02d}_h{hour:02d}.json"
    with (folder / fname).open("w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2, default=str)
    return 1


def _write_vendor_title(out, los_id, doc_type, role, day, hour, extras, start_date):
    folder = _channel_dir(out, day, "vendor_title", start_date)
    doc_id = _doc_id("vendor_title", los_id, doc_type, day, hour)
    fields = _extracted_fields(doc_type, los_id, role, day, extras)
    pdf_b64 = _pdf_b64(doc_type, fields, los_id, role)
    base = f"{doc_id}_title"
    (folder / f"{base}.pdf.b64").write_text(pdf_b64, encoding="ascii")
    meta = {
        "document_id":         doc_id,
        "los_id":              los_id,
        "source_system":       "FIRST_AMERICAN",
        "source_institution":  "First American Title",
        "source_channel":      "vendor_title",
        "sender":              "title@firstam.com",
        "subject":             f"{doc_type} for {los_id}",
        "received_at":         _received_at(start_date, day, hour),
        "attachment_filename": f"{doc_type.lower()}.pdf",
        "document_type":       doc_type,
        "category":            _category_for(doc_type),
        "borrower_role":       role,
        "extracted_fields":    fields,
    }
    with (folder / f"{base}_meta.json").open("w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, default=str)
    return 2


def _write_shared_drive(out, day, hour, scan_text, variant_idx, start_date):
    """Render a shared-drive scan with a real-world artifact (rotation /
    landscape / two-docs-per-page / faded photocopy) — variant_idx
    cycles through the four artifact styles deterministically."""
    folder = _channel_dir(out, day, "shared_drive", start_date)
    ts = (start_date + timedelta(days=day - 1)).strftime("%Y%m%d") + f"-{hour:02d}{15:02d}"
    pdf_bytes = pdf_formats.make_shared_drive_scan(scan_text, variant_idx)
    pdf_b64 = base64.b64encode(pdf_bytes).decode("ascii")
    (folder / f"scan_{ts}.pdf.b64").write_text(pdf_b64, encoding="ascii")
    return 1


def _write_ai_chat(out, los_id, doc_type, role, day, hour, extras, start_date):
    folder = _channel_dir(out, day, "ai_chat", start_date)
    doc_id = _doc_id("ai_chat", los_id, doc_type, day, hour)
    fields = _extracted_fields(doc_type, los_id, role, day, extras)
    transcript = (
        "Borrower: I had a late payment on my Chase card in Dec 2024 because I was "
        "going through a divorce — money was tight that month and I missed the due "
        "date. It's been current since Jan 2025."
    )
    folder_date = (start_date + timedelta(days=day - 1)).isoformat()
    doc = {
        "document_id":      doc_id,
        "document_type":    doc_type,
        "category":         _category_for(doc_type),
        "los_id":           los_id,
        "borrower_role":    role,
        "source_system":    "AI_CHATBOT",
        "source_channel":   "ai_chat",
        "received_at":      _received_at(start_date, day, hour),
        "chat_transcript":  transcript,
        "extracted_fields": fields,
    }
    fname = f"chat_{los_id}_{folder_date}_h{hour:02d}.json"
    with (folder / fname).open("w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2, default=str)
    return 1


# ===========================================================================
# Orchestration
# ===========================================================================


def generate(
    out_dir: Path, start_date: date, num_days: int, clean: bool,
) -> dict:
    if clean and out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    files_total = 0
    by_channel: dict = {}
    by_loan: dict = {los: 0 for los in LOAN_PROFILES}

    def _bump(channel: str, n: int, los_id: str):
        nonlocal files_total
        files_total += n
        by_channel[channel] = by_channel.get(channel, 0) + n
        by_loan[los_id] = by_loan.get(los_id, 0) + n

    for los_id, schedule in SCHEDULE.items():
        for day, hour, channel, doc_type, role, extras in schedule:
            if day > num_days:
                continue
            extras = extras or {}
            if channel == "edms_pull":
                _bump("edms_pull",
                      _write_edms_pull(out_dir, los_id, doc_type, role, day, hour, extras, start_date),
                      los_id)
            elif channel == "email_inbox":
                _bump("email_inbox",
                      _write_email_inbox(out_dir, los_id, doc_type, role, day, hour, extras, start_date),
                      los_id)
            elif channel == "borrower_portal":
                _bump("borrower_portal",
                      _write_borrower_portal(out_dir, los_id, doc_type, role, day, hour, extras, start_date),
                      los_id)
            elif channel == "los_encompass":
                doc_types = doc_type if isinstance(doc_type, list) else [doc_type]
                _bump("los_encompass",
                      _write_los_encompass_batch(out_dir, los_id, doc_types, role, day, hour, extras, start_date),
                      los_id)
            elif channel == "vendor_equifax":
                _bump("vendor_equifax",
                      _write_vendor_equifax(out_dir, los_id, doc_type, role, day, hour, extras, start_date),
                      los_id)
            elif channel == "vendor_corelogic":
                _bump("vendor_corelogic",
                      _write_vendor_corelogic(out_dir, los_id, doc_type, role, day, hour, extras, start_date),
                      los_id)
            elif channel == "vendor_title":
                _bump("vendor_title",
                      _write_vendor_title(out_dir, los_id, doc_type, role, day, hour, extras, start_date),
                      los_id)
            elif channel == "ai_chat":
                _bump("ai_chat",
                      _write_ai_chat(out_dir, los_id, doc_type, role, day, hour, extras, start_date),
                      los_id)
            else:
                print(f"  WARN unknown channel: {channel}")

    # Sprinkle unclassified shared-drive scans across the window. The
    # variant index cycles 0..3 → rotated / landscape / two-doc / faded
    # so the first run hits all four real-world scanner artifacts.
    for variant_idx, (day, hour, scan_text) in enumerate(SHARED_DRIVE_DROPS):
        if day > num_days:
            continue
        n = _write_shared_drive(
            out_dir, day, hour, scan_text, variant_idx, start_date,
        )
        files_total += n
        by_channel["shared_drive"] = by_channel.get("shared_drive", 0) + n

    return {
        "files_total":  files_total,
        "by_channel":   by_channel,
        "by_loan":      by_loan,
        "out_dir":      str(out_dir),
        "date_folders": num_days,
    }


def s3_sync(local_dir: Path, s3_target: str, dry_run: bool = False) -> int:
    """Run ``aws s3 sync`` as a subprocess. Returns exit code."""
    cmd = ["aws", "s3", "sync", str(local_dir), s3_target]
    if dry_run:
        cmd.append("--dryrun")
    print(f"\n+ {' '.join(cmd)}")
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.stdout:
        # Trim noisy "(dryrun) upload: ..." lines but keep the tail summary.
        lines = proc.stdout.splitlines()
        print("\n".join(lines[-30:]))
    if proc.returncode != 0:
        print(proc.stderr, file=sys.stderr)
    return proc.returncode


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out",   default=DEFAULT_OUT)
    ap.add_argument("--start", default=DEFAULT_START)
    ap.add_argument("--days",  type=int, default=DEFAULT_DAYS)
    ap.add_argument("--clean", action="store_true",
                    help="rm -rf the output dir before writing")
    ap.add_argument("--upload", action="store_true",
                    help=f"after generating, run `aws s3 sync` to {DEFAULT_S3_TARGET}")
    ap.add_argument("--s3-target", default=DEFAULT_S3_TARGET,
                    help="override the S3 sync destination")
    ap.add_argument("--dry-run-s3", action="store_true",
                    help="pass --dryrun to aws s3 sync")
    args = ap.parse_args()

    start = datetime.fromisoformat(args.start).date()
    out   = Path(args.out)
    summary = generate(out, start, args.days, clean=args.clean)

    print(f"\nWrote {summary['files_total']} files to {summary['out_dir']}")
    print(f"  Date folders: {summary['date_folders']} (start={start})")
    print(f"  By channel:")
    for ch, n in sorted(summary["by_channel"].items(), key=lambda x: -x[1]):
        print(f"    {ch:18s} {n}")
    print(f"  By loan:")
    for los_id, n in summary["by_loan"].items():
        prof = LOAN_PROFILES[los_id]
        print(f"    {los_id} ({prof['scenario']:25s} — {prof['primary_name']:18s}): {n}")

    if args.upload:
        rc = s3_sync(out, args.s3_target, dry_run=args.dry_run_s3)
        sys.exit(rc)


if __name__ == "__main__":
    main()
