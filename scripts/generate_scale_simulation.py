"""Scale simulation: 22 loan profiles × 9,000 loans × 60-day timeline.

Generates the most comprehensive mortgage-document corpus the
simulator has produced — 22 distinct loan profiles (Conv / FHA / VA /
USDA / Jumbo / NonQM / HELOC / Construction etc.) at 100 loans/day
across 90 days = 9,000 total. Each loan emits ~30 documents across
50+ source channels over a 60-day post-app timeline (with random
±2-day jitter), so the v3 connector + builder pull ~270,000 docs +
~250,000 entity_states snapshots.

100 loans (the first 100) get full real-binary-PDF generation
(borrower portal DLs, appraisals, title commitments, HOI binders,
closing disclosures, manual scans). The other 8,900 are JSON-only —
the connector + builder behaviour is identical, but disk + upload
costs stay manageable.

Layout (matches the v3 connector's two-stage dispatch)::

    local_storage/s3_scale_simulation/
        loans_manifest.json                      ← every loan + profile
        2026-01-01/
            loan_origination/                    ← URLA events
            post_application/{channel}/          ← all post-app docs
        ...

CLI::

    python scripts/generate_scale_simulation.py --days 5 --apps-per-day 10 --clean   # smoke
    python scripts/generate_scale_simulation.py --clean                              # full 9,000
    python scripts/generate_scale_simulation.py --upload                             # also aws s3 sync
"""
from __future__ import annotations

import argparse
import csv as csv_mod
import io
import json
import os
import random
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import pdf_formats  # noqa: E402
from scripts.generate_realworld_simulation_v3 import (  # noqa: E402
    CHANNEL_FORMAT,
    _SOURCE_ID_FMTS,
    _SOURCE_SYSTEM_NAME,
    _category_for,
    _source_id,
)


DEFAULT_OUT       = "local_storage/s3_scale_simulation"
DEFAULT_S3_TARGET = "s3://edms-simulator-loans/s3_scale_simulation/"
DEFAULT_START     = "2026-01-01"
DEFAULT_DAYS      = 90
DEFAULT_APPS_PER_DAY = 100
PDF_LOAN_COUNT    = 100   # first N loans get real PDFs


# ===========================================================================
# 22 loan profiles — distribution sums to 1.00 within ±0.01.
# ===========================================================================

PROFILES: dict = {
    "salaried_clean": {
        "pct": 0.22, "loan_type": "Conv",
        "property_type": ["SFR", "Townhome"],
        "income_range": (80000, 180000),
        "purchase_range": (350000, 550000),
        "credit_range": (720, 780), "ltv_range": (75, 80),
        "co_borrowers": 0, "income_type": "w2_salaried",
        "employer_channel": ["employer_adp", "employer_workday"],
        "title_channel": ["title_first_american"],
        "appraisal_channel": ["appraisal_mercury"],
        "insurance_channel": ["insurance_statefarm"],
        "required_docs": [
            "W2_CURRENT", "W2_PRIOR", "PAYSTUB_CURRENT",
            "CREDIT_REPORT", "BANK_STATEMENT_M1", "BANK_STATEMENT_M2",
            "VOE_TWN", "DRIVERS_LICENSE", "SSN_VALIDATION", "OFAC_CHECK",
            "IRS_TRANSCRIPT", "APPRAISAL_URAR", "TITLE_COMMITMENT",
            "TITLE_INSURANCE", "HOI_BINDER", "FLOOD_CERT",
            "PROPERTY_TAX_BILL", "PURCHASE_AGREEMENT", "RATE_LOCK",
            "AUS_DU_FINDINGS", "URLA_1003",
        ],
    },
    "salaried_joint": {
        "pct": 0.13, "loan_type": "Conv", "property_type": ["SFR"],
        "income_range": (60000, 130000),
        "purchase_range": (400000, 800000),
        "credit_range": (700, 760), "ltv_range": (75, 85),
        "co_borrowers": 1, "income_type": "w2_salaried",
        "employer_channel": ["employer_workday", "employer_gusto"],
        "co_employer_channel": ["employer_gusto", "employer_paychex"],
        "co_income_range": (45000, 100000),
        "co_credit_range": (680, 740),
        "title_channel": ["title_first_american"],
        "appraisal_channel": ["appraisal_mercury"],
        "insurance_channel": ["insurance_statefarm"],
        "required_docs": [
            "W2_CURRENT", "PAYSTUB_CURRENT",
            "CREDIT_REPORT", "BANK_STATEMENT_M1",
            "VOE_TWN", "DRIVERS_LICENSE", "SSN_VALIDATION",
            "OFAC_CHECK", "IRS_TRANSCRIPT", "APPRAISAL_URAR",
            "TITLE_COMMITMENT", "TITLE_INSURANCE", "HOI_BINDER",
            "PURCHASE_AGREEMENT", "RATE_LOCK",
            "AUS_DU_FINDINGS", "URLA_1003",
        ],
    },
    "self_employed_sole": {
        "pct": 0.07, "loan_type": "Conv",
        "property_type": ["SFR", "Condo"],
        "income_range": (70000, 150000),
        "purchase_range": (300000, 500000),
        "credit_range": (680, 740), "ltv_range": (80, 85),
        "co_borrowers": 0, "income_type": "self_employed_2yr_avg",
        "employer_channel": ["employer_paychex", "employer_manual"],
        "title_channel": ["title_chicago"],
        "appraisal_channel": ["appraisal_corelogic_amc"],
        "insurance_channel": ["insurance_allstate"],
        "has_mi": True,
        "required_docs": [
            "W2_CURRENT", "TAX_RETURN_1040_CURRENT", "TAX_RETURN_1040_PRIOR",
            "SCHEDULE_C", "1099_NEC", "K1_SCHEDULE",
            "CREDIT_REPORT", "BANK_STATEMENT_M1", "BANK_STATEMENT_M2",
            "BANK_STATEMENT_M3", "IRS_TRANSCRIPT", "VOE_TWN",
            "APPRAISAL_URAR", "TITLE_COMMITMENT", "TITLE_INSURANCE",
            "HOI_BINDER", "MI_CERTIFICATE",
            "PURCHASE_AGREEMENT", "RATE_LOCK", "AUS_DU_FINDINGS", "URLA_1003",
        ],
    },
    "self_employed_w2": {
        "pct": 0.04, "loan_type": "Conv", "property_type": ["SFR"],
        "income_range": (90000, 200000),
        "purchase_range": (350000, 600000),
        "credit_range": (700, 750), "ltv_range": (80, 80),
        "co_borrowers": 0, "income_type": "self_employed_2yr_avg",
        "employer_channel": ["employer_adp", "employer_manual"],
        "required_docs": [
            "W2_CURRENT", "TAX_RETURN_1040_CURRENT", "TAX_RETURN_1040_PRIOR",
            "SCHEDULE_C", "1099_NEC",
            "CREDIT_REPORT", "BANK_STATEMENT_M1", "BANK_STATEMENT_M2",
            "IRS_TRANSCRIPT", "APPRAISAL_URAR", "TITLE_COMMITMENT",
            "TITLE_INSURANCE", "HOI_BINDER",
            "PURCHASE_AGREEMENT", "RATE_LOCK", "AUS_DU_FINDINGS", "URLA_1003",
        ],
    },
    "first_time_buyer": {
        "pct": 0.10, "loan_type": "Conv",
        "property_type": ["SFR", "Townhome", "Condo"],
        "income_range": (55000, 90000),
        "purchase_range": (250000, 400000),
        "credit_range": (680, 720), "ltv_range": (90, 95),
        "co_borrowers": 0, "income_type": "w2_salaried",
        "employer_channel": ["employer_gusto"],
        "insurance_channel": ["insurance_allstate"],
        "has_mi": True, "has_gift": True,
        "required_docs": [
            "W2_CURRENT", "PAYSTUB_CURRENT", "CREDIT_REPORT",
            "BANK_STATEMENT_M1", "BANK_STATEMENT_M2",
            "GIFT_LETTER", "GIFT_DONOR_BANK_STATEMENT",
            "VOE_TWN", "DRIVERS_LICENSE", "SSN_VALIDATION", "OFAC_CHECK",
            "IRS_TRANSCRIPT", "APPRAISAL_URAR", "TITLE_COMMITMENT",
            "TITLE_INSURANCE", "HOI_BINDER", "MI_CERTIFICATE",
            "PURCHASE_AGREEMENT", "RATE_LOCK", "AUS_DU_FINDINGS", "URLA_1003",
        ],
    },
    "non_qm_bank_stmt": {
        "pct": 0.01, "loan_type": "NonQM", "property_type": ["SFR"],
        "income_range": (100000, 200000),
        "purchase_range": (400000, 700000),
        "credit_range": (700, 740), "ltv_range": (70, 75),
        "co_borrowers": 0, "income_type": "self_employed_2yr_avg",
        "required_docs": (
            ["BANK_STATEMENT_M1", "BANK_STATEMENT_M2", "BANK_STATEMENT_M3"]
            + ["CREDIT_REPORT", "APPRAISAL_URAR", "TITLE_COMMITMENT",
               "TITLE_INSURANCE", "HOI_BINDER",
               "PURCHASE_AGREEMENT", "URLA_1003"]
        ),
    },
    "construction": {
        "pct": 0.01, "loan_type": "Construction", "property_type": ["SFR"],
        "income_range": (120000, 250000),
        "purchase_range": (500000, 900000),
        "credit_range": (720, 760), "ltv_range": (80, 80),
        "co_borrowers": 0, "income_type": "w2_salaried",
        "required_docs": [
            "W2_CURRENT", "CREDIT_REPORT",
            "BANK_STATEMENT_M1", "BANK_STATEMENT_M2",
            "VOE_TWN", "APPRAISAL_URAR", "TITLE_COMMITMENT",
            "HOI_BINDER", "URLA_1003", "RATE_LOCK", "AUS_DU_FINDINGS",
        ],
    },
    "h1b_foreign": {
        "pct": 0.03, "loan_type": "Conv", "property_type": ["SFR"],
        "income_range": (120000, 200000),
        "purchase_range": (400000, 600000),
        "credit_range": (740, 780), "ltv_range": (80, 80),
        "co_borrowers": 0, "income_type": "w2_salaried",
        "employer_channel": ["employer_workday"],
        "is_visa_holder": True,
        "required_docs": [
            "W2_CURRENT", "PAYSTUB_CURRENT", "CREDIT_REPORT",
            "BANK_STATEMENT_M1", "VOE_TWN",
            "VISA_H1B", "EAD_CARD", "PASSPORT", "I94",
            "DRIVERS_LICENSE", "SSN_VALIDATION", "OFAC_CHECK",
            "IRS_TRANSCRIPT", "APPRAISAL_URAR", "TITLE_COMMITMENT",
            "TITLE_INSURANCE", "HOI_BINDER",
            "PURCHASE_AGREEMENT", "RATE_LOCK", "AUS_DU_FINDINGS", "URLA_1003",
        ],
    },
    "retired_fixed": {
        "pct": 0.04, "loan_type": "Conv", "property_type": ["SFR"],
        "income_range": (3500, 6000), "income_is_monthly": True,
        "purchase_range": (200000, 350000),
        "credit_range": (760, 800), "ltv_range": (70, 80),
        "co_borrowers": 0, "income_type": "fixed_income",
        "title_channel": ["title_stewart"],
        "appraisal_channel": ["appraisal_manual"],
        "required_docs": [
            "SSA_AWARD_LETTER", "PENSION_LETTER",
            "CREDIT_REPORT", "BANK_STATEMENT_M1", "BANK_STATEMENT_M2",
            "DRIVERS_LICENSE", "SSN_VALIDATION", "OFAC_CHECK",
            "IRS_TRANSCRIPT", "APPRAISAL_URAR", "TITLE_COMMITMENT",
            "TITLE_INSURANCE", "HOI_BINDER",
            "PURCHASE_AGREEMENT", "RATE_LOCK", "AUS_DU_FINDINGS", "URLA_1003",
        ],
    },
    "investment_property": {
        "pct": 0.05, "loan_type": "Conv",
        "property_type": ["SFR", "Duplex"],
        "income_range": (100000, 180000),
        "purchase_range": (300000, 500000),
        "credit_range": (720, 760), "ltv_range": (70, 75),
        "co_borrowers": 0, "income_type": "w2_salaried",
        "employer_channel": ["employer_adp"],
        "occupancy": "investment",
        "required_docs": [
            "W2_CURRENT", "PAYSTUB_CURRENT", "CREDIT_REPORT",
            "BANK_STATEMENT_M1", "BANK_STATEMENT_M2", "BANK_STATEMENT_M3",
            "SCHEDULE_E", "RENTAL_LEASE", "VOE_TWN",
            "IRS_TRANSCRIPT", "TAX_RETURN_1040_CURRENT",
            "APPRAISAL_URAR", "TITLE_COMMITMENT", "TITLE_INSURANCE",
            "HOI_BINDER", "PURCHASE_AGREEMENT", "RATE_LOCK",
            "AUS_DU_FINDINGS", "URLA_1003",
        ],
    },
    "jumbo": {
        "pct": 0.03, "loan_type": "Jumbo", "property_type": ["SFR"],
        "income_range": (250000, 500000),
        "purchase_range": (800000, 1500000),
        "credit_range": (740, 780), "ltv_range": (65, 80),
        "co_borrowers": 0, "income_type": "w2_salaried",
        "employer_channel": ["employer_workday"],
        "required_docs": [
            "W2_CURRENT", "W2_PRIOR", "PAYSTUB_CURRENT",
            "CREDIT_REPORT", "BANK_STATEMENT_M1", "BANK_STATEMENT_M2",
            "BANK_STATEMENT_M3",
            "VOE_TWN", "IRS_TRANSCRIPT", "TAX_RETURN_1040_CURRENT",
            "APPRAISAL_URAR",
            "TITLE_COMMITMENT", "TITLE_INSURANCE", "HOI_BINDER",
            "PURCHASE_AGREEMENT", "RATE_LOCK", "AUS_DU_FINDINGS", "URLA_1003",
        ],
    },
    "multi_unit": {
        "pct": 0.01, "loan_type": "Conv", "property_type": ["2-4unit"],
        "income_range": (120000, 200000),
        "purchase_range": (400000, 800000),
        "credit_range": (720, 750), "ltv_range": (75, 75),
        "co_borrowers": 0, "income_type": "w2_salaried",
        "occupancy": "investment",
        "required_docs": [
            "W2_CURRENT", "CREDIT_REPORT", "BANK_STATEMENT_M1",
            "SCHEDULE_E", "RENTAL_LEASE",
            "VOE_TWN", "APPRAISAL_URAR", "TITLE_COMMITMENT",
            "TITLE_INSURANCE", "HOI_BINDER",
            "PURCHASE_AGREEMENT", "RATE_LOCK", "AUS_DU_FINDINGS", "URLA_1003",
        ],
    },
    "refi_rate_term": {
        "pct": 0.07, "loan_type": "Conv", "property_type": ["SFR"],
        "income_range": (80000, 180000),
        "purchase_range": None,
        "appraised_range": (350000, 600000),
        "credit_range": (720, 760), "ltv_range": (65, 80),
        "co_borrowers": 0, "income_type": "w2_salaried",
        "purpose": "refinance",
        "title_channel": ["title_stewart"],
        "required_docs": [
            "W2_CURRENT", "PAYSTUB_CURRENT", "CREDIT_REPORT",
            "BANK_STATEMENT_M1", "VOE_TWN", "APPRAISAL_URAR",
            "TITLE_COMMITMENT", "TITLE_INSURANCE", "HOI_BINDER",
            "MORTGAGE_PAYOFF", "PAYMENT_HISTORY_24MO", "ESCROW_ANALYSIS",
            "RATE_LOCK", "AUS_DU_FINDINGS", "URLA_1003",
        ],
    },
    "refi_cash_out": {
        "pct": 0.03, "loan_type": "Conv", "property_type": ["SFR"],
        "income_range": (90000, 200000),
        "purchase_range": None,
        "appraised_range": (400000, 700000),
        "credit_range": (700, 740), "ltv_range": (75, 80),
        "co_borrowers": 0, "income_type": "w2_salaried",
        "purpose": "refinance_cash_out",
        "has_subordinate": True,
        "required_docs": [
            "W2_CURRENT", "PAYSTUB_CURRENT", "CREDIT_REPORT",
            "BANK_STATEMENT_M1", "BANK_STATEMENT_M2", "VOE_TWN",
            "APPRAISAL_URAR", "TITLE_COMMITMENT", "TITLE_INSURANCE",
            "MORTGAGE_PAYOFF",
            "RATE_LOCK", "AUS_DU_FINDINGS", "URLA_1003",
        ],
    },
    "fha": {
        "pct": 0.04, "loan_type": "FHA",
        "property_type": ["SFR", "Condo"],
        "income_range": (45000, 80000),
        "purchase_range": (200000, 350000),
        "credit_range": (620, 680), "ltv_range": (96, 96),
        "co_borrowers": 0, "income_type": "w2_salaried",
        "employer_channel": ["employer_gusto", "employer_paychex"],
        "has_mi": True, "mi_type": "FHA_UFMIP",
        "required_docs": [
            "W2_CURRENT", "PAYSTUB_CURRENT", "CREDIT_REPORT",
            "BANK_STATEMENT_M1", "VOE_TWN",
            "DRIVERS_LICENSE", "SSN_VALIDATION", "OFAC_CHECK",
            "IRS_TRANSCRIPT", "APPRAISAL_URAR",
            "TITLE_COMMITMENT", "TITLE_INSURANCE", "HOI_BINDER",
            "MI_CERTIFICATE", "PURCHASE_AGREEMENT", "RATE_LOCK",
            "AUS_DU_FINDINGS", "URLA_1003",
        ],
    },
    "va": {
        "pct": 0.02, "loan_type": "VA", "property_type": ["SFR"],
        "income_range": (60000, 120000),
        "purchase_range": (250000, 450000),
        "credit_range": (640, 720), "ltv_range": (100, 100),
        "co_borrowers": 0, "income_type": "w2_salaried",
        "required_docs": [
            "W2_CURRENT", "PAYSTUB_CURRENT", "CREDIT_REPORT",
            "BANK_STATEMENT_M1", "VOE_TWN", "VA_COE",
            "DRIVERS_LICENSE", "SSN_VALIDATION", "OFAC_CHECK",
            "APPRAISAL_URAR", "TITLE_COMMITMENT",
            "TITLE_INSURANCE", "HOI_BINDER",
            "PURCHASE_AGREEMENT", "RATE_LOCK", "AUS_DU_FINDINGS", "URLA_1003",
        ],
    },
    "divorced_alimony": {
        "pct": 0.02, "loan_type": "Conv", "property_type": ["SFR"],
        "income_range": (70000, 100000),
        "purchase_range": (250000, 400000),
        "credit_range": (660, 700), "ltv_range": (80, 85),
        "co_borrowers": 0, "income_type": "w2_salaried",
        "has_alimony": True, "alimony_range": (1500, 3000),
        "employer_channel": ["employer_paychex"],
        "title_channel": ["title_stewart"],
        "appraisal_channel": ["appraisal_manual"],
        "insurance_channel": ["insurance_manual_drop"],
        "has_mi": True,
        "required_docs": [
            "W2_CURRENT", "PAYSTUB_CURRENT", "CREDIT_REPORT",
            "BANK_STATEMENT_M1", "BANK_STATEMENT_M2", "VOE_TWN",
            "DIVORCE_DECREE", "ALIMONY_RECEIPT_HISTORY",
            "TAX_RETURN_1040_CURRENT",
            "DRIVERS_LICENSE", "SSN_VALIDATION", "OFAC_CHECK",
            "APPRAISAL_URAR", "TITLE_COMMITMENT", "TITLE_INSURANCE",
            "HOI_BINDER", "MI_CERTIFICATE",
            "PURCHASE_AGREEMENT", "RATE_LOCK", "AUS_DU_FINDINGS", "URLA_1003",
        ],
    },
    "usda_rural": {
        "pct": 0.01, "loan_type": "USDA", "property_type": ["SFR"],
        "income_range": (45000, 75000),
        "purchase_range": (150000, 250000),
        "credit_range": (640, 700), "ltv_range": (100, 100),
        "co_borrowers": 0, "income_type": "w2_salaried",
        "required_docs": [
            "W2_CURRENT", "PAYSTUB_CURRENT", "CREDIT_REPORT",
            "BANK_STATEMENT_M1", "VOE_TWN",
            "APPRAISAL_URAR", "TITLE_COMMITMENT",
            "TITLE_INSURANCE", "HOI_BINDER",
            "PURCHASE_AGREEMENT", "RATE_LOCK",
            "AUS_DU_FINDINGS", "URLA_1003",
        ],
    },
    "heloc_2nd": {
        "pct": 0.02, "loan_type": "HELOC", "property_type": ["SFR"],
        "income_range": (100000, 200000),
        "purchase_range": None,
        "appraised_range": (400000, 700000),
        "credit_range": (720, 760), "ltv_range": (80, 90),
        "co_borrowers": 0, "income_type": "w2_salaried",
        "purpose": "heloc",
        "has_subordinate": True,
        "required_docs": [
            "W2_CURRENT", "PAYSTUB_CURRENT", "CREDIT_REPORT",
            "BANK_STATEMENT_M1", "VOE_TWN", "APPRAISAL_URAR",
            "TITLE_COMMITMENT", "TITLE_INSURANCE",
            "URLA_1003",
        ],
    },
    "non_occupant_cosigner": {
        "pct": 0.01, "loan_type": "Conv",
        "property_type": ["SFR", "Condo"],
        "income_range": (50000, 70000),
        "purchase_range": (250000, 400000),
        "credit_range": (680, 720), "ltv_range": (90, 95),
        "co_borrowers": 1, "income_type": "w2_salaried",
        "co_role": "non_occupant_cosigner",
        "co_income_range": (80000, 150000),
        "co_credit_range": (740, 780),
        "has_mi": True,
        "required_docs": [
            "W2_CURRENT", "PAYSTUB_CURRENT",
            "CREDIT_REPORT", "BANK_STATEMENT_M1", "VOE_TWN",
            "DRIVERS_LICENSE", "SSN_VALIDATION",
            "APPRAISAL_URAR", "TITLE_COMMITMENT", "TITLE_INSURANCE",
            "HOI_BINDER", "MI_CERTIFICATE",
            "PURCHASE_AGREEMENT", "RATE_LOCK", "AUS_DU_FINDINGS", "URLA_1003",
        ],
    },
    "commission_bonus": {
        "pct": 0.03, "loan_type": "Conv", "property_type": ["SFR"],
        "income_range": (80000, 150000),
        "purchase_range": (350000, 600000),
        "credit_range": (720, 760), "ltv_range": (80, 80),
        "co_borrowers": 0, "income_type": "base_plus_commission",
        "commission_range": (20000, 60000),
        "employer_channel": ["employer_adp", "employer_workday"],
        "required_docs": [
            "W2_CURRENT", "W2_PRIOR", "PAYSTUB_CURRENT",
            "COMMISSION_HISTORY", "CREDIT_REPORT",
            "BANK_STATEMENT_M1", "BANK_STATEMENT_M2",
            "VOE_TWN", "IRS_TRANSCRIPT",
            "APPRAISAL_URAR", "TITLE_COMMITMENT", "TITLE_INSURANCE",
            "HOI_BINDER", "PURCHASE_AGREEMENT", "RATE_LOCK",
            "AUS_DU_FINDINGS", "URLA_1003",
        ],
    },
    "renovation_203k": {
        "pct": 0.01, "loan_type": "FHA_203k", "property_type": ["SFR"],
        "income_range": (70000, 120000),
        "purchase_range": (200000, 400000),
        "credit_range": (640, 700), "ltv_range": (96, 96),
        "co_borrowers": 0, "income_type": "w2_salaried",
        "has_mi": True, "mi_type": "FHA_UFMIP",
        "required_docs": [
            "W2_CURRENT", "PAYSTUB_CURRENT", "CREDIT_REPORT",
            "BANK_STATEMENT_M1", "VOE_TWN",
            "TITLE_COMMITMENT", "TITLE_INSURANCE", "HOI_BINDER",
            "MI_CERTIFICATE", "PURCHASE_AGREEMENT", "RATE_LOCK",
            "AUS_DU_FINDINGS", "URLA_1003",
        ],
    },
}

SCENARIO_FLAGS = {"has_employment_gap": 0.03, "is_flood_zone_a": 0.05}

CITIES = [
    ("Austin",      "TX", 0.25),
    ("Dallas",      "TX", 0.20),
    ("Houston",     "TX", 0.20),
    ("San Antonio", "TX", 0.15),
    ("Round Rock",  "TX", 0.05),
    ("Frisco",      "TX", 0.05),
    ("Cedar Park",  "TX", 0.05),
    ("Pflugerville","TX", 0.05),
]

FIRST_NAMES = [
    "James","Maria","David","Robert","Amanda","Carlos","Jennifer",
    "Priya","Thomas","Lisa","Wei","Sarah","Michael","Emily","Daniel",
    "Sophia","Andrew","Olivia","Matthew","Ava","Joseph","Mia","Joshua",
    "Isabella","Benjamin","Charlotte","Ryan","Harper","Tyler","Evelyn",
]
LAST_NAMES = [
    "Wilson","Garcia","Kim","Johnson","Chen","Rivera","Brown","Patel",
    "O'Brien","Zhang","Smith","Jones","Lee","Martinez","Lopez","Davis",
    "Miller","Anderson","Taylor","Thomas","Jackson","White","Harris",
    "Clark","Lewis","Walker","Hall","Allen","Young","King",
]


# ===========================================================================
# Helpers
# ===========================================================================


def _pick_weighted(choices: list) -> str:
    """choices: [(value, weight), ...] — pick by weight."""
    total = sum(w for _, w in choices)
    r = random.uniform(0, total)
    cum = 0
    for v, w in choices:
        cum += w
        if r <= cum:
            return v
    return choices[-1][0]


def _city_state() -> tuple[str, str]:
    return (lambda c: (c[0], c[1]))(_pick_weighted(
        [((c, s), w) for c, s, w in CITIES]
    ))


def _pick_profile() -> str:
    """Pick a profile by its pct weight."""
    return _pick_weighted([(name, p["pct"]) for name, p in PROFILES.items()])


def _gen_borrower(rng: random.Random, profile: dict) -> dict:
    """Generate a randomized borrower record per profile."""
    inc = rng.randint(*profile["income_range"])
    if profile.get("income_is_monthly"):
        inc = inc * 12
    credit = rng.randint(*profile["credit_range"])
    first = rng.choice(FIRST_NAMES)
    last  = rng.choice(LAST_NAMES)
    yob   = rng.randint(1950, 1995)
    mob   = rng.randint(1, 12)
    dob_d = rng.randint(1, 28)
    dob   = f"{yob:04d}-{mob:02d}-{dob_d:02d}"
    return {
        "first_name":     first,
        "last_name":      last,
        "full_name":      f"{first} {last}",
        "dob":            dob,
        "ssn_last4":      f"{rng.randint(1000, 9999):04d}",
        "email":          f"{first.lower()}.{last.lower().replace(chr(39),'')}@email.com",
        "phone":          f"{rng.randint(200,999)}-{rng.randint(100,999)}-{rng.randint(1000,9999):04d}",
        "income":         inc,
        "credit_mid":     credit,
        "employer":       _pick_employer(rng, profile),
    }


_EMPLOYERS = [
    "TechCorp Inc","Oracle","HealthSys","Indeed","AMD","Google",
    "Atlassian","Dell Technologies","Self-Employed","RegionalSoft",
    "Hill Country Nonprofit","StateFarm","Allstate","Travelers",
]


def _pick_employer(rng: random.Random, profile: dict) -> str:
    if profile.get("income_type") == "fixed_income":
        return "Retired"
    if profile.get("income_type") == "self_employed_2yr_avg":
        return rng.choice(["Self-Employed", "Sole Proprietor LLC",
                            "Independent Consultant LLC"])
    return rng.choice(_EMPLOYERS)


def _gen_loan(rng: random.Random, profile_name: str, los_id: str,
              app_date: date) -> dict:
    """Build a complete loan record from a profile + RNG."""
    profile = PROFILES[profile_name]
    borrower = _gen_borrower(rng, profile)

    co = None
    if profile.get("co_borrowers", 0) > 0:
        co_profile = {
            **profile,
            "income_range": profile.get("co_income_range", profile["income_range"]),
            "credit_range": profile.get("co_credit_range", profile["credit_range"]),
        }
        co = _gen_borrower(rng, co_profile)
        co["last_name"] = borrower["last_name"]   # married couples share last name
        co["full_name"] = f"{co['first_name']} {co['last_name']}"
        co["email"]     = (
            f"{co['first_name'].lower()}.{co['last_name'].lower()}@email.com"
        )

    is_refi = profile.get("purpose", "").startswith("refinance") or \
              profile.get("purpose") == "heloc"
    if is_refi:
        appraised = rng.randint(*profile["appraised_range"])
        purchase  = 0
    else:
        purchase  = rng.randint(*profile["purchase_range"])
        appraised = int(purchase * rng.uniform(1.00, 1.05))

    ltv = rng.uniform(*profile["ltv_range"])
    if is_refi:
        loan_amount = int(appraised * ltv / 100)
    else:
        loan_amount = int(min(appraised, purchase) * ltv / 100)

    rate = round(rng.uniform(6.0, 7.0), 3)

    city, state = _city_state()

    has_employment_gap = rng.random() < SCENARIO_FLAGS["has_employment_gap"]
    is_flood_zone_a    = rng.random() < SCENARIO_FLAGS["is_flood_zone_a"]

    ssn_hash = f"unique_{los_id}_{borrower['ssn_last4']}"
    co_ssn_hash = f"unique_{los_id}C_{co['ssn_last4']}" if co else None

    return {
        "los_id":           los_id,
        "profile":          profile_name,
        "app_date":         app_date.isoformat(),
        "borrower":         borrower,
        "co_borrower":      co,
        "loan_type":        profile["loan_type"],
        "property_type":    rng.choice(profile["property_type"]),
        "purpose":          profile.get("purpose",
                                ("refinance" if is_refi else "purchase")),
        "occupancy":        profile.get("occupancy", "primary_residence"),
        "is_refi":          is_refi,
        "purchase_price":   purchase if not is_refi else 0,
        "appraised_value":  appraised,
        "loan_amount":      loan_amount,
        "ltv_pct":          round(ltv, 2),
        "interest_rate":    rate,
        "city":             city,
        "state":            state,
        "subject_address":  f"{rng.randint(100,9999)} "
                             f"{rng.choice(['Oak','Maple','Pine','Elm','Cedar'])} "
                             f"{rng.choice(['St','Dr','Ln','Way','Pl'])}, "
                             f"{city} {state}",
        "ssn_hash":         ssn_hash,
        "co_ssn_hash":      co_ssn_hash,
        "has_employment_gap": has_employment_gap,
        "is_flood_zone_a":    is_flood_zone_a,
        "has_pdfs":         False,    # toggled per-loan in main()
        "channels": {
            "employer":  rng.choice(profile.get("employer_channel",
                                                ["employer_adp"])),
            "co_employer": rng.choice(profile.get("co_employer_channel",
                                                  ["employer_gusto"])) if co else None,
            "title":     rng.choice(profile.get("title_channel",
                                                ["title_first_american"])),
            "appraisal": rng.choice(profile.get("appraisal_channel",
                                                ["appraisal_mercury"])),
            "insurance": rng.choice(profile.get("insurance_channel",
                                                ["insurance_statefarm"])),
        },
    }


# ===========================================================================
# Per-doc field generation (parameterised on the loan dict, not the
# 10 hand-built profiles the v3 generator carries).
# ===========================================================================


def _doc_fields(doc_type: str, loan: dict, role: str = "primary",
                rng: random.Random | None = None) -> dict:
    """Doc-type → extracted_fields dict, scaled to the loan's randomised
    income/credit/property values. Mirrors the field shape the v3
    generator emits so the connector + builder don't need any
    awareness of which generator wrote the doc."""
    rng = rng or random.Random()
    b = loan["co_borrower"] if (role == "co_borrower" and loan["co_borrower"]) \
        else loan["borrower"]
    income   = b["income"]
    credit   = b["credit_mid"]
    employer = b["employer"]
    name     = b["full_name"]
    ssn4     = b["ssn_last4"]
    appraised = loan["appraised_value"]
    purchase  = loan["purchase_price"] or appraised
    loan_amt  = loan["loan_amount"]

    if doc_type in ("W2_CURRENT", "W2_PRIOR"):
        return {
            "box1_wages":     income,
            "box2_fed_tax":   round(income * 0.15),
            "box3_ss_wages":  income,
            "tax_year":       "2024" if doc_type == "W2_PRIOR" else "2025",
            "employer_name":  employer,
            "employer_ein":   "12-3456789",
            "employee_name":  name,
            "ssn_last4":      ssn4,
        }
    if doc_type == "PAYSTUB_CURRENT":
        return {
            "ytd_gross":      round(income * 0.42, 2),
            "gross_pay":      round(income / 12, 2),
            "net_pay":        round(income / 12 * 0.72, 2),
            "pay_period_end": "2026-04-30",
            "pay_frequency":  "monthly",
            "employer_name":  employer,
            "employee_name":  name,
        }
    if doc_type == "URLA_1003":
        return {
            "loan_purpose":           loan["purpose"],
            "loan_amount":            loan_amt,
            "interest_rate":          loan["interest_rate"],
            "loan_term_months":       360,
            "occupancy":              loan["occupancy"],
            "borrower_name":          loan["borrower"]["full_name"],
            "borrower_dob":           loan["borrower"]["dob"],
            "borrower_ssn_last4":     loan["borrower"]["ssn_last4"],
            "co_borrower_name":       (loan["co_borrower"] or {}).get("full_name"),
            "monthly_income_stated":  round(income / 12),
            "subject_property_address": loan["subject_address"],
            "subject_property_city":  loan["city"],
            "subject_property_state": loan["state"],
        }
    if doc_type == "CREDIT_REPORT":
        return {
            "experian_score":   credit + 8,
            "equifax_score":    credit,
            "transunion_score": credit - 7,
            "mid_score":        credit,
            "credit_band":      "prime" if credit >= 740 else
                                 ("near-prime" if credit >= 670 else "subprime"),
            "tradeline_count":  rng.randint(8, 20),
            "total_monthly_obligations": rng.randint(800, 2500),
            "hard_inquiries_12mo": rng.randint(0, 4),
        }
    if doc_type.startswith("BANK_STATEMENT") or doc_type == "GIFT_FUNDS_TRAIL":
        bal_base = max(income / 12 * 4, 25000)
        bal = round(bal_base * rng.uniform(0.85, 1.15), 2)
        gift_amt = (
            int(loan_amt * 0.05)
            if "GIFT_LETTER" in PROFILES.get(loan["profile"], {}).get(
                "required_docs", [])
            else None
        )
        largest = (
            gift_amt if (doc_type == "BANK_STATEMENT_M1" and gift_amt)
            else round(income / 12 * 0.4)
        )
        return {
            "bank_name":            "Chase Bank",
            "account_holder":       name,
            "ending_balance":       bal,
            "avg_monthly_deposits": round(income / 12 * 0.95, 2),
            "largest_deposit":      largest,
            "months_count":         1,
        }
    if doc_type == "GIFT_LETTER":
        return {
            "gift_amount":        int(loan_amt * 0.05),
            "donor_name":         "Family",
            "donor_relationship": "parent",
            "repayment_required": False,
            "borrower_name":      name,
        }
    if doc_type == "GIFT_DONOR_BANK_STATEMENT":
        return {
            "donor_name":         "Family",
            "withdrawal_amount":  int(loan_amt * 0.05),
            "withdrawal_date":    "2026-01-08",
        }
    if doc_type == "PURCHASE_AGREEMENT":
        return {
            "purchase_price":   purchase,
            "earnest_money":    5000,
            "closing_date":     "2026-07-15",
            "buyer_name":       loan["borrower"]["full_name"],
            "seller_name":      "Sample Seller",
            "property_address": loan["subject_address"],
        }
    if doc_type in ("APPRAISAL_URAR", "APPRAISAL_URAR_1073"):
        return {
            "appraised_value":  appraised,
            "property_address": loan["subject_address"],
            "property_type":    loan["property_type"],
            "condition_rating": "C3",
            "year_built":       2008,
            "gla_sqft":         rng.randint(1500, 3500),
            "bedrooms":         rng.randint(2, 5),
            "bathrooms":        rng.choice([1.5, 2.0, 2.5, 3.0]),
            "comparable_1_price": round(appraised * 0.97),
            "comparable_2_price": round(appraised * 1.03),
            "comparable_3_price": round(appraised * 0.99),
            "effective_date":   "2026-04-15",
        }
    if doc_type == "AVM_REPORT":
        return {
            "avm_value":        round(appraised * 0.985),
            "confidence_score": 0.87,
            "model_name":       "CoreLogic Total Home Value",
            "effective_date":   "2026-01-15",
        }
    if doc_type == "RATE_LOCK":
        return {
            "locked_rate":  loan["interest_rate"],
            "lock_expiry":  "2026-07-30",
            "lock_days":    60,
            "loan_amount":  loan_amt,
            "loan_program": "Conv 30yr fixed",
        }
    if doc_type == "TITLE_COMMITMENT":
        return {
            "commitment_number": f"TC-{loan['los_id']}",
            "lender_name":       "EDMS Mortgage",
            "policy_amount":     loan_amt,
            "effective_date":    "2026-04-01",
            "vesting":           "Fee Simple",
            "exceptions_count":  rng.randint(2, 7),
            "tax_lien_clear":    True,
            "judgment_lien_clear": True,
        }
    if doc_type == "TITLE_INSURANCE":
        return {
            "policy_number":   f"TI-{loan['los_id']}",
            "policy_amount":   loan_amt,
            "coverage_amount": loan_amt,
            "effective_date":  "2026-05-15",
            "insured_name":    loan["borrower"]["full_name"],
        }
    if doc_type in ("HOI_BINDER", "HOI_BINDER_HO6"):
        return {
            "policy_number":     f"HOI-{loan['los_id']}",
            "annual_premium":    rng.randint(1200, 2400),
            "carrier":           "StateFarm",
            "coverage_dwelling": appraised,
            "deductible":        2500,
            "policy_form":       "HO6" if doc_type.endswith("HO6") else "HO3",
            "effective_date":    "2026-05-15",
        }
    if doc_type == "FLOOD_CERT":
        zone = "AE" if loan.get("is_flood_zone_a") else "X"
        return {
            "flood_zone":          zone,
            "requires_insurance":  zone in ("A", "AE", "V", "VE"),
            "firm_panel":          "48453C0440K",
            "determination_date":  "2026-01-20",
        }
    if doc_type == "FLOOD_INSURANCE":
        return {
            "policy_number":      f"NFIP-{loan['los_id']}",
            "annual_premium":     rng.randint(1500, 3500),
            "carrier":            "NFIP",
            "coverage_amount":    appraised,
            "effective_date":     "2026-05-15",
        }
    if doc_type == "PROPERTY_TAX_BILL":
        return {
            "annual_tax":     round(appraised * 0.018),
            "assessed_value": round(appraised * 0.93),
            "tax_year":       "2025",
            "property_address": loan["subject_address"],
        }
    if doc_type == "DRIVERS_LICENSE":
        return {
            "dl_number":   f"TX-{ssn4}{rng.randint(1000,9999):04d}",
            "state":       "TX",
            "expiry_date": "2028-06-15",
            "name":        name,
            "dob":         b["dob"],
        }
    if doc_type == "SSN_VALIDATION":
        return {"ssn_valid": True, "name_match": True, "dob_match": True,
                "deceased_indicator": False}
    if doc_type == "OFAC_CHECK":
        return {"ofac_clear": True, "sdn_match": False, "pep_match": False,
                "adverse_media": False}
    if doc_type == "VOE_TWN":
        return {
            "employer_name":     employer,
            "employment_status": "Active",
            "hire_date":         "2019-03-01",
            "income_amount":     income,
            "income_frequency":  "annual",
            "position":          "Senior Engineer",
            "employment_verified": True,
            "verification_date": "2026-01-15",
        }
    if doc_type == "AUS_DU_FINDINGS":
        return {
            "recommendation":   "approve_eligible" if credit >= 700 else "refer_with_caution",
            "risk_class":       "low" if credit >= 740 else "moderate",
            "casefile_id":      f"DU-{loan['los_id']}-001",
            "conditions_count": rng.randint(0, 5),
            "qualifying_income": income,
            "ltv":              loan["ltv_pct"],
            "dti":              round(rng.uniform(28, 42), 1),
        }
    if doc_type == "SCHEDULE_C":
        net = round(income * 0.6)
        return {
            "gross_receipts": round(income * 1.4),
            "total_expenses": round(income * 0.4),
            "net_profit":     net,
            "business_name":  f"{b['first_name']} Studio LLC",
            "tax_year":       "2025",
        }
    if doc_type == "SCHEDULE_E":
        return {
            "rental_income_gross": rng.randint(15000, 30000),
            "rental_expenses":     rng.randint(4000, 9000),
            "net_rental_income":   rng.randint(10000, 22000),
            "property_count":      1,
            "tax_year":            "2025",
        }
    if doc_type == "TAX_RETURN_1040_CURRENT":
        return {
            "agi":               income,
            "total_income":      income + 1500,
            "wages_line1":       round(income * 0.4),
            "schedule_c_income": round(income * 0.6),
            "depreciation":      rng.randint(2000, 5000),
            "tax_year":          "2025",
            "filing_status":     "Single",
        }
    if doc_type == "TAX_RETURN_1040_PRIOR":
        return {
            "agi":               round(income * rng.uniform(0.85, 1.0)),
            "total_income":      round(income * rng.uniform(0.85, 1.0)),
            "wages_line1":       round(income * 0.4),
            "schedule_c_income": round(income * 0.55),
            "depreciation":      rng.randint(2500, 5500),
            "tax_year":          "2024",
            "filing_status":     "Single",
        }
    if doc_type == "IRS_TRANSCRIPT":
        return {
            "agi":                     income,
            "wages_salaries":          round(income * 0.5),
            "self_employment_income":  round(income * 0.5),
            "tax_year":                "2025",
            "filing_status":           "Single",
        }
    if doc_type == "K1_SCHEDULE":
        return {
            "ordinary_income":     rng.randint(5000, 15000),
            "guaranteed_payments": rng.randint(3000, 10000),
            "partnership_name":    f"{b['first_name']} Holdings LLC",
            "tax_year":            "2025",
        }
    if doc_type == "1099_NEC":
        return {
            "nonemployee_compensation": rng.randint(20000, 70000),
            "payer_name":               rng.choice(["ConsultCo", "DesignHub",
                                                     "FreelanceCorp"]),
            "payer_tin":                "98-7654321",
            "recipient_name":           name,
            "tax_year":                 "2025",
            "form_type":                "NEC",
        }
    if doc_type == "SSA_AWARD_LETTER":
        return {
            "monthly_benefit":   1400 + rng.randint(0, 500),
            "effective_date":    "2026-01-01",
            "benefit_type":      "retirement",
            "beneficiary_name":  name,
        }
    if doc_type == "PENSION_LETTER":
        return {
            "monthly_benefit":   2500 + rng.randint(0, 1000),
            "employer_name":     "Texas Retirement",
            "retirement_date":   "2024-11-01",
            "benefit_type":      "defined_benefit",
            "beneficiary_name":  name,
        }
    if doc_type == "DIVORCE_DECREE":
        amt = rng.randint(*PROFILES[loan["profile"]].get("alimony_range",
                                                          (1500, 3000)))
        return {
            "decree_date":          "2024-01-15",
            "court":                "Travis County District Court",
            "alimony_amount":       amt,
            "alimony_frequency":    "monthly",
            "remaining_years":      rng.randint(3, 7),
            "child_support_amount": 0,
        }
    if doc_type == "ALIMONY_RECEIPT_HISTORY":
        return {
            "months_received": 12,
            "monthly_amount":  rng.randint(*PROFILES[loan["profile"]].get(
                "alimony_range", (1500, 3000))),
            "stable":          True,
        }
    if doc_type == "RENTAL_LEASE":
        return {
            "monthly_rent":      rng.randint(1500, 2500),
            "lease_start":       "2025-01-01",
            "lease_end":         "2026-12-31",
            "tenant_name":       "Tenant Redacted",
            "property_address":  "456 Rental Way",
        }
    if doc_type == "MORTGAGE_PAYOFF":
        return {
            "current_balance":  loan_amt + rng.randint(5000, 30000),
            "payoff_through":   "2026-02-15",
            "lender":           "USAA Federal Savings",
            "loan_number":      f"PRIOR-{loan['los_id']}",
        }
    if doc_type == "PAYMENT_HISTORY_24MO":
        return {
            "months_reviewed":  24,
            "late_payments":    0,
            "current":          True,
            "monthly_payment":  round(loan_amt * 0.006),
            "current_rate":     4.875,
            "lender":           "USAA Federal Savings",
        }
    if doc_type == "ESCROW_ANALYSIS":
        return {
            "current_balance":  rng.randint(2000, 5000),
            "annual_taxes":     round(appraised * 0.018),
            "annual_insurance": rng.randint(1200, 2400),
            "monthly_escrow":   rng.randint(500, 900),
        }
    if doc_type in ("VISA_H1B", "EAD_CARD", "PASSPORT", "I94"):
        return {
            "document_type":  doc_type,
            "holder_name":    name,
            "expiry_date":    "2027-08-15",
            "country":        "India" if doc_type == "PASSPORT" else None,
            "visa_status":    "H1B",
        }
    if doc_type == "MI_CERTIFICATE":
        rate = (
            0.005 if loan["ltv_pct"] <= 90
            else (0.008 if loan["ltv_pct"] <= 95 else 0.01)
        )
        return {
            "certificate_number": f"MI-{loan['los_id']}",
            "coverage_pct":       25 if loan["ltv_pct"] > 90 else 12,
            "monthly_premium":    round(loan_amt * rate / 12),
            "carrier":            "MGIC",
            "effective_date":     "2026-05-15",
        }
    if doc_type == "VA_COE":
        return {
            "entitlement_amount": 144000,
            "service_branch":     "Army",
            "discharge_status":   "honorable",
            "issue_date":         "2026-01-08",
        }
    if doc_type == "COMMISSION_HISTORY":
        return {
            "two_year_average": rng.randint(*PROFILES[loan["profile"]].get(
                "commission_range", (20000, 60000))),
            "trending":         rng.choice(["increasing", "stable"]),
            "tax_year":         "2025",
        }
    if doc_type == "EMPLOYMENT_GAP_LETTER":
        return {
            "reason":     rng.choice(["relocation", "education",
                                       "family_care", "medical_leave"]),
            "gap_start":  "2024-03-01",
            "gap_end":    "2024-09-15",
            "borrower_name": name,
        }
    return {"placeholder": True, "doc_type": doc_type}


# ===========================================================================
# Per-loan timeline — maps required_docs → (day, channel, doc_type, role).
# Random ±2-day jitter per event.
# ===========================================================================


_DAY_BAND = {
    # Each doc type lands in a base day; the per-loan jitter adds ±2.
    "URLA_1003":              1,
    "CREDIT_REPORT":          1,
    "CREDIT_REPORT_CO":       2,
    "W2_CURRENT":             2,
    "W2_PRIOR":               3,
    "W2_CURRENT_CO":          4,
    "PAYSTUB_CURRENT":        3,
    "PAYSTUB_CURRENT_CO":     4,
    "DRIVERS_LICENSE":        4,
    "DRIVERS_LICENSE_CO":     5,
    "SSN_VALIDATION":         5,
    "SSN_VALIDATION_CO":      6,
    "OFAC_CHECK":             5,
    "OFAC_CHECK_CO":          6,
    "VOE_TWN":                7,
    "VOE_TWN_CO":             8,
    "VOE_TWN_COSIGNER":       7,
    "AVM_REPORT":             6,
    "FLOOD_CERT":             6,
    "FLOOD_INSURANCE":        11,
    "IRS_TRANSCRIPT":         8,
    "TAX_RETURN_1040_CURRENT": 9,
    "TAX_RETURN_1040_PRIOR":   9,
    "SCHEDULE_C":             10,
    "SCHEDULE_E":             10,
    "K1_SCHEDULE":            10,
    "1099_NEC":               12,
    "BANK_STATEMENT_M1":      10,
    "BANK_STATEMENT_M2":      11,
    "BANK_STATEMENT_M3":      12,
    "GIFT_LETTER":            12,
    "GIFT_DONOR_BANK_STATEMENT": 13,
    "APPRAISAL_URAR":         10,
    "TITLE_COMMITMENT":       10,
    "HOI_BINDER":             10,
    "PROPERTY_TAX_BILL":      12,
    "PURCHASE_AGREEMENT":     20,
    "VISA_H1B":               5,
    "EAD_CARD":               5,
    "PASSPORT":               6,
    "I94":                    6,
    "VA_COE":                 4,
    "DIVORCE_DECREE":         7,
    "ALIMONY_RECEIPT_HISTORY": 10,
    "RENTAL_LEASE":           6,
    "SSA_AWARD_LETTER":       5,
    "PENSION_LETTER":         5,
    "COMMISSION_HISTORY":     6,
    "EMPLOYMENT_GAP_LETTER":  6,
    "AUS_DU_FINDINGS":        18,
    "RATE_LOCK":              25,
    "MI_CERTIFICATE":         28,
    "TITLE_INSURANCE":        30,
    "MORTGAGE_PAYOFF":        5,
    "PAYMENT_HISTORY_24MO":   5,
    "ESCROW_ANALYSIS":        6,
    "CLOSING_DISCLOSURE":     35,
    "WIRE_INSTRUCTIONS":      36,
    "SETTLEMENT_STATEMENT":   38,
}


_CHANNEL_FOR_TYPE = {
    # Most doc types route through a fixed channel; some are loan-specific.
    "URLA_1003":              "los_encompass_batch",
    "CREDIT_REPORT":          "los_encompass_batch",
    "AUS_DU_FINDINGS":        "los_encompass_solo",
    "RATE_LOCK":              "los_encompass_solo",
    "DRIVERS_LICENSE":        "borrower_portal",
    "PURCHASE_AGREEMENT":     "email_inbox",
    "GIFT_LETTER":            "email_inbox",
    "GIFT_DONOR_BANK_STATEMENT": "email_inbox",
    "DIVORCE_DECREE":         "email_inbox",
    "ALIMONY_RECEIPT_HISTORY": "edms_pull",
    "SCHEDULE_C":             "irs_manual",
    "SCHEDULE_E":             "irs_manual",
    "TAX_RETURN_1040_CURRENT": "irs_manual",
    "TAX_RETURN_1040_PRIOR":   "irs_manual",
    "K1_SCHEDULE":            "irs_manual",
    "1099_NEC":               "email_inbox",
    "VOE_TWN":                "vendor_equifax",
    "SSN_VALIDATION":         "vendor_lexisnexis",
    "OFAC_CHECK":             "vendor_lexisnexis",
    "AVM_REPORT":             "vendor_corelogic",
    "FLOOD_CERT":             "vendor_corelogic",
    "FLOOD_INSURANCE":        "insurance_flood_nfip",
    "IRS_TRANSCRIPT":         "irs_ives",
    "BANK_STATEMENT_M1":      "vendor_finicity",
    "BANK_STATEMENT_M2":      "vendor_finicity",
    "BANK_STATEMENT_M3":      "vendor_finicity",
    "RENTAL_LEASE":           "email_inbox",
    "PROPERTY_TAX_BILL":      "edms_pull",
    "SSA_AWARD_LETTER":       "ssa_gov",
    "PENSION_LETTER":         "edms_pull",   # JSON channel — pension as JSON
    "COMMISSION_HISTORY":     "edms_pull",
    "VA_COE":                 "va_gov",
    "EMPLOYMENT_GAP_LETTER":  "borrower_portal",
    "MI_CERTIFICATE":         None,           # routed by has_mi/mi_provider
    "MORTGAGE_PAYOFF":        "servicer_current",
    "PAYMENT_HISTORY_24MO":   "servicer_current",
    "ESCROW_ANALYSIS":        "servicer_current",
    "VISA_H1B":               "borrower_portal",
    "EAD_CARD":               "borrower_portal",
    "PASSPORT":               "borrower_portal",
    "I94":                    "borrower_portal",
    "CLOSING_DISCLOSURE":     "closing_agent",
    "WIRE_INSTRUCTIONS":      "closing_agent",
    "SETTLEMENT_STATEMENT":   "closing_agent",
}


def _resolve_channel(doc_type: str, loan: dict, role: str = "primary") -> str:
    """Return the channel a doc lands in for this loan + role."""
    fixed = _CHANNEL_FOR_TYPE.get(doc_type)
    if fixed:
        if fixed == "los_encompass_batch":
            return "los_encompass"
        if fixed == "los_encompass_solo":
            return "los_encompass"
        return fixed
    if doc_type in ("W2_CURRENT", "W2_PRIOR", "PAYSTUB_CURRENT"):
        if role == "co_borrower":
            return loan["channels"]["co_employer"] or "employer_gusto"
        return loan["channels"]["employer"]
    if doc_type == "APPRAISAL_URAR" or doc_type == "APPRAISAL_URAR_1073":
        return loan["channels"]["appraisal"]
    if doc_type in ("TITLE_COMMITMENT", "TITLE_INSURANCE"):
        return loan["channels"]["title"]
    if doc_type in ("HOI_BINDER", "HOI_BINDER_HO6"):
        return loan["channels"]["insurance"]
    if doc_type == "MI_CERTIFICATE":
        return "vendor_mi_mgic"
    return "edms_pull"


def _build_events(loan: dict, rng: random.Random) -> list:
    """Return list of (sim_day, hour, channel, doc_type, role, fields)."""
    profile = PROFILES[loan["profile"]]
    docs: list = []
    co_present = loan["co_borrower"] is not None
    base_required = list(profile["required_docs"])
    # Add gap letter if scenario flag triggered
    if loan["has_employment_gap"]:
        base_required.append("EMPLOYMENT_GAP_LETTER")
    # Add flood insurance if zone A
    if loan["is_flood_zone_a"]:
        if "FLOOD_INSURANCE" not in base_required:
            base_required.append("FLOOD_INSURANCE")

    seen: set = set()
    for dt in base_required:
        if dt in seen:
            continue
        seen.add(dt)
        base_day = _DAY_BAND.get(dt, 15)
        day = max(0, base_day + rng.randint(-2, 2))
        hour = rng.choice([8, 9, 10, 11, 14, 15, 16, 17])
        role = "primary"
        channel = _resolve_channel(dt, loan, role)
        docs.append((day, hour, channel, dt, role,
                     _doc_fields(dt, loan, role, rng)))

        # Generate co-borrower mirror docs for income-bearing types
        if co_present and dt in (
            "W2_CURRENT", "PAYSTUB_CURRENT", "VOE_TWN",
            "DRIVERS_LICENSE", "SSN_VALIDATION", "OFAC_CHECK",
            "CREDIT_REPORT",
        ):
            day_co = max(0, base_day + 1 + rng.randint(-1, 2))
            hour_co = rng.choice([8, 9, 10, 11, 14, 15, 16, 17])
            chan_co = _resolve_channel(dt, loan, "co_borrower")
            docs.append((day_co, hour_co, chan_co, dt, "co_borrower",
                         _doc_fields(dt, loan, "co_borrower", rng)))
    return docs


# ===========================================================================
# File writers — minimal versions; one JSON per doc, group los_encompass
# into a daily array per loan.
# ===========================================================================


def _channel_dir(out: Path, app_date: date, day_offset: int,
                 channel: str) -> Path:
    target_date = app_date + timedelta(days=day_offset)
    if channel == "loan_origination":
        folder = out / target_date.isoformat() / "loan_origination"
    else:
        folder = out / target_date.isoformat() / "post_application" / channel
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def _build_envelope(loan: dict, doc_type: str, channel: str, fields: dict,
                    received_at: str, role: str, doc_seq: int,
                    suffix: str = "") -> dict:
    doc_id = f"{channel.upper()[:8]}-{loan['los_id']}-{doc_type}-{doc_seq:04d}"
    if suffix:
        doc_id += f"-{suffix}"
    return {
        "document_id":        doc_id,
        "document_type":      doc_type,
        "category":           _category_for(doc_type),
        "los_id":              loan["los_id"],
        "borrower_role":      role,
        "source_system":      _SOURCE_SYSTEM_NAME.get(channel, channel.upper()),
        "source_channel":     channel,
        "source_document_id": _source_id(channel, doc_seq, dt=received_at[:10].replace("-","")),
        "received_at":        received_at,
        "extracted_fields":   fields,
    }


def _write_origination(out: Path, loan: dict, app_date: date) -> int:
    folder = _channel_dir(out, app_date, 0, "loan_origination")
    co = None
    cb = loan["co_borrower"]
    if cb:
        co = {
            "first_name":      cb["first_name"], "last_name": cb["last_name"],
            "dob":             cb["dob"],
            "ssn_last4":       cb["ssn_last4"], "ssn_hash": loan["co_ssn_hash"],
            "email":           cb["email"],
            "stated_income":   cb["income"], "stated_employer": cb["employer"],
        }
    event = {
        "event_type":   "loan_application_submitted",
        "los_id":       loan["los_id"],
        "received_at":  f"{app_date.isoformat()}T09:15:00Z",
        "source_system": "ENCOMPASS",
        "legacy_ids": {
            "encompass_loan_number": f"ENC-{loan['los_id']}",
            "encompass_borrower_id": f"ENC-BR-{loan['los_id']}",
        },
        "loan_terms": {
            "loan_purpose":     loan["purpose"],
            "loan_amount":      loan["loan_amount"],
            "interest_rate":    loan["interest_rate"],
            "loan_term_months": 360,
            "occupancy":        loan["occupancy"],
            "property_type":    loan["property_type"],
        },
        "borrower": {
            "first_name":      loan["borrower"]["first_name"],
            "last_name":       loan["borrower"]["last_name"],
            "dob":             loan["borrower"]["dob"],
            "ssn_last4":       loan["borrower"]["ssn_last4"],
            "ssn_hash":        loan["ssn_hash"],
            "email":           loan["borrower"]["email"],
            "phone":           loan["borrower"]["phone"],
            "current_address": loan["subject_address"],
            "stated_income":   loan["borrower"]["income"],
            "stated_employer": loan["borrower"]["employer"],
            "years_at_employer": 5,
            "stated_assets":   round(loan["borrower"]["income"] * 1.5),
        },
        "co_borrower": co,
        "property": {
            "address":         loan["subject_address"],
            "city":            loan["city"],
            "state":           loan["state"],
            "zip":             "78745",
            "county":          "Travis",
            "type":            loan["property_type"],
            "purchase_price":  loan["purchase_price"],
            "estimated_value": loan["appraised_value"],
        },
    }
    fname = f"{loan['los_id']}_application.json"
    (folder / fname).write_text(
        json.dumps(event, indent=2, default=str), encoding="utf-8",
    )
    return 1


def _write_loan_events(out: Path, loan: dict, app_date: date,
                       rng: random.Random) -> int:
    """Walk the per-loan event list and write one file per event."""
    events = _build_events(loan, rng)
    files_written = 0
    encompass_batches: dict = {}    # date_key → list of doc envelopes
    seq_counter = {"n": 1}

    for day, hour, channel, doc_type, role, fields in events:
        seq = seq_counter["n"]
        seq_counter["n"] += 1
        target_date = app_date + timedelta(days=day)
        received_at = f"{target_date.isoformat()}T{hour:02d}:15:00Z"
        env = _build_envelope(loan, doc_type, channel, fields,
                              received_at, role, seq)

        if channel == "los_encompass":
            # Bundle into a single batch JSON per (loan, date).
            key = (target_date, hour // 4)
            encompass_batches.setdefault(key, []).append(env)
            continue

        fmt = CHANNEL_FORMAT.get(channel, "json")
        folder = _channel_dir(out, app_date, day, channel)

        if fmt == "json":
            path = folder / f"{env['document_id']}.json"
            path.write_text(json.dumps(env, indent=2, default=str),
                            encoding="utf-8")
            files_written += 1
        elif fmt in ("pdf_meta", "borrower_portal", "email_inbox"):
            # JSON-shape metadata is preserved; PDFs only for the
            # 100 PDF-bearing loans (toggled via has_pdfs flag).
            base = env["document_id"]
            (folder / f"{base}_meta.json").write_text(
                json.dumps({**env, "attachment_filename": f"{doc_type.lower()}.pdf"},
                           indent=2, default=str),
                encoding="utf-8",
            )
            files_written += 1
            if loan.get("has_pdfs"):
                pdf_bytes = pdf_formats.make_pdf(
                    doc_type, fields, loan["los_id"], role,
                )
                (folder / f"{base}.pdf").write_bytes(pdf_bytes)
                files_written += 1
        elif fmt == "pdf_only":
            # Manual-drop channels — JSON sidecar so the connector
            # has SOMETHING to read at scale; the 100 PDF loans get a
            # real PDF too.
            (folder / f"{env['document_id']}.json").write_text(
                json.dumps(env, indent=2, default=str), encoding="utf-8",
            )
            files_written += 1
            if loan.get("has_pdfs"):
                pdf_bytes = pdf_formats.make_pdf(
                    doc_type, fields, loan["los_id"], role,
                )
                (folder / f"{env['document_id']}.pdf").write_bytes(pdf_bytes)
                files_written += 1
        else:
            # csv / xml — write JSON for simplicity at scale.
            (folder / f"{env['document_id']}.json").write_text(
                json.dumps(env, indent=2, default=str), encoding="utf-8",
            )
            files_written += 1

    # Flush los_encompass batches as JSON arrays.
    for (target_date, _quartile), docs in encompass_batches.items():
        day_offset = (target_date - app_date).days
        folder = _channel_dir(out, app_date, day_offset, "los_encompass")
        fname = f"{loan['los_id']}_batch_{target_date.isoformat()}.json"
        (folder / fname).write_text(
            json.dumps(docs, indent=2, default=str), encoding="utf-8",
        )
        files_written += 1

    return files_written


# ===========================================================================
# Orchestration
# ===========================================================================


def generate(
    out_dir: Path, start_date: date, days: int, apps_per_day: int,
    pdf_count: int, clean: bool, seed: int = 42,
) -> dict:
    rng = random.Random(seed)
    if clean and out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    total_loans = days * apps_per_day
    pdf_loans   = min(pdf_count, total_loans)

    manifest: list = []
    files_total = 0
    by_profile: dict = {}
    by_loan_type: dict = {}
    by_pdf: dict = {True: 0, False: 0}
    flagged_gap = 0
    flagged_flood = 0

    loan_idx = 0
    for d in range(days):
        app_date = start_date + timedelta(days=d)
        for _ in range(apps_per_day):
            loan_idx += 1
            los_id = f"LOAN-{app_date.strftime('%Y%m%d')}-{loan_idx:05d}"
            profile = _pick_profile()
            loan = _gen_loan(rng, profile, los_id, app_date)
            loan["has_pdfs"] = (loan_idx <= pdf_loans)
            by_pdf[loan["has_pdfs"]] += 1
            by_profile[profile] = by_profile.get(profile, 0) + 1
            by_loan_type[loan["loan_type"]] = by_loan_type.get(
                loan["loan_type"], 0) + 1
            if loan["has_employment_gap"]:
                flagged_gap += 1
            if loan["is_flood_zone_a"]:
                flagged_flood += 1
            manifest.append({
                "los_id":     los_id,
                "profile":    profile,
                "loan_type":  loan["loan_type"],
                "has_pdfs":   loan["has_pdfs"],
                "app_date":   app_date.isoformat(),
                "city":       loan["city"],
                "purpose":    loan["purpose"],
                "ltv_pct":    loan["ltv_pct"],
                "credit_mid": loan["borrower"]["credit_mid"],
            })
            files_total += _write_origination(out_dir, loan, app_date)
            files_total += _write_loan_events(out_dir, loan, app_date,
                                              rng)

    # loans_manifest.json at root
    (out_dir / "loans_manifest.json").write_text(
        json.dumps({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_loans": total_loans,
            "pdf_loans":   pdf_loans,
            "by_profile":  by_profile,
            "by_loan_type": by_loan_type,
            "scenario_flags": {
                "employment_gap": flagged_gap,
                "flood_zone_a":   flagged_flood,
            },
            "loans": manifest,
        }, indent=2, default=str),
        encoding="utf-8",
    )

    return {
        "files_total":  files_total,
        "total_loans":  total_loans,
        "pdf_loans":    pdf_loans,
        "by_profile":   by_profile,
        "by_loan_type": by_loan_type,
        "flagged_gap":  flagged_gap,
        "flagged_flood": flagged_flood,
        "out_dir":      str(out_dir),
    }


def s3_sync(local_dir: Path, s3_target: str) -> int:
    cmd = ["aws", "s3", "sync", str(local_dir), s3_target,
           "--sse", "AES256", "--delete"]
    print(f"\n+ {' '.join(cmd)}")
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.stdout:
        print("\n".join(proc.stdout.splitlines()[-15:]))
    if proc.returncode != 0:
        print(proc.stderr, file=sys.stderr)
    return proc.returncode


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out",        default=DEFAULT_OUT)
    ap.add_argument("--start",      default=DEFAULT_START)
    ap.add_argument("--days",       type=int, default=DEFAULT_DAYS)
    ap.add_argument("--apps-per-day", type=int, default=DEFAULT_APPS_PER_DAY)
    ap.add_argument("--pdf-count",  type=int, default=PDF_LOAN_COUNT)
    ap.add_argument("--clean",      action="store_true")
    ap.add_argument("--upload",     action="store_true")
    ap.add_argument("--dry-run",    action="store_true",
                    help="print stats without writing files")
    ap.add_argument("--seed",       type=int, default=42)
    ap.add_argument("--s3-target",  default=DEFAULT_S3_TARGET)
    args = ap.parse_args()

    start = datetime.fromisoformat(args.start).date()
    out   = Path(args.out)

    if args.dry_run:
        total = args.days * args.apps_per_day
        print(f"DRY RUN: would generate {total} loans, "
              f"~{total*30} docs, {min(args.pdf_count, total)} PDF loans.")
        return

    print(f"Generating {args.days * args.apps_per_day} loans "
          f"({args.apps_per_day}/day × {args.days} days), "
          f"{min(args.pdf_count, args.days * args.apps_per_day)} with PDFs...")
    t0 = datetime.now()
    summary = generate(
        out, start, args.days, args.apps_per_day, args.pdf_count,
        clean=args.clean, seed=args.seed,
    )
    elapsed = (datetime.now() - t0).total_seconds()
    print(f"\nWrote {summary['files_total']:,} files to {summary['out_dir']} "
          f"in {elapsed:.1f}s")
    print(f"  Total loans:   {summary['total_loans']:,}")
    print(f"  PDF loans:     {summary['pdf_loans']:,}")
    print(f"  Empl-gap loans: {summary['flagged_gap']}")
    print(f"  Flood-A loans:  {summary['flagged_flood']}")
    print(f"  By profile (top 8):")
    for name, n in sorted(summary["by_profile"].items(),
                          key=lambda x: -x[1])[:8]:
        print(f"    {name:30s} {n:>5}")
    print(f"  By loan type:")
    for kt, n in sorted(summary["by_loan_type"].items(),
                        key=lambda x: -x[1]):
        print(f"    {kt:14s} {n:>5}")

    if args.upload:
        rc = s3_sync(out, args.s3_target)
        sys.exit(rc)


if __name__ == "__main__":
    main()
