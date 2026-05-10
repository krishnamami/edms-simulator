"""Generate v3 mortgage-loan simulation — full lifecycle, every channel.

Replaces the v2 generator with a comprehensive layout that mirrors how
documents actually arrive at a real mortgage lender: a Stage-1 loan-
origination event (the URLA submission) and Stage-2 post-application
docs that trickle in from 50+ source channels over the next 20-45 days.

Output tree::

    local_storage/s3_simulation_v3/
        loans_config.json                       ← bootstrap config (all 10 loans)
        2026-01-01/
            loan_origination/                   ← Stage 1: application events
                LOAN-101_application.json
                LOAN-102_application.json
                ...
            post_application/                   ← Stage 2: every other channel
                edms_pull/
                los_encompass/                  ← JSON array (batch)
                los_bytepro/                    ← CSV
                mismo_feed/                     ← XML
                employer_adp/ employer_paychex/ employer_gusto/ employer_workday/
                employer_manual/                ← PDF only (no meta — needs AI Vision)
                vendor_equifax/ vendor_experian/ vendor_transunion/
                vendor_lexisnexis/ vendor_finicity/ vendor_plaid/
                vendor_corelogic/ vendor_mi_mgic/ vendor_mi_radian/
                appraisal_mercury/ appraisal_corelogic_amc/ appraisal_manual/
                title_first_american/ title_chicago/ title_stewart/ title_manual_drop/
                insurance_statefarm/ insurance_allstate/ insurance_flood_nfip/
                insurance_wind_hail/ insurance_condo_ho6/ insurance_manual_drop/
                irs_ives/ irs_manual/ ssa_gov/ va_gov/
                closing_agent/ servicer_current/
                borrower_portal/ email_inbox/ ai_chat/
                hoa_management/ condo_project/
                conditions_response/ corrections/
                compliance/ loan_officer_notes/
                shared_drive/ attorney_legal/
        2026-01-02/
            ...

Each post-application doc carries ``los_id`` (no hardcoded
applicant_id; the EDMS API mints that at /loans time) and a
``source_document_id`` that mirrors the system's own ID format
(e.g. ``ADP-W2-2025-4567``, ``TWN-CASE-12345``,
``FA-TC-2026-78901``, ``MERC-RPT-2026-001``) so a downstream
reconciliation can stitch back to the legacy system.

CLI::

    python scripts/generate_realworld_simulation_v3.py             # generate locally
    python scripts/generate_realworld_simulation_v3.py --clean     # rm -rf first
    python scripts/generate_realworld_simulation_v3.py --upload    # aws s3 sync to S3
    python scripts/generate_realworld_simulation_v3.py --days 30 --loans 5
"""
from __future__ import annotations

import argparse
import csv as csv_mod
import io
import json
import os
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import pdf_formats  # noqa: E402


DEFAULT_OUT       = "local_storage/s3_simulation_v3"
DEFAULT_S3_TARGET = "s3://edms-simulator-loans/s3_simulation_v3/"
DEFAULT_START     = "2026-01-01"
DEFAULT_DAYS      = 50
DEFAULT_LOANS     = 10


# ===========================================================================
# Per-channel format dispatch — drives the writer that handles each event.
#   "json"             → individual JSON file (most system feeds)
#   "json_array"       → JSON array (Encompass batch)
#   "csv"              → CSV with headers (BytePro)
#   "xml"              → MISMO 3.4 envelope
#   "pdf_meta"         → real binary .pdf + sibling _meta.json
#   "pdf_only"         → real binary .pdf, NO metadata (forces AI Vision)
#   "borrower_portal"  → pdf+meta with uploaded_by hint
#   "email_inbox"      → pdf+meta with sender/subject hints
# ===========================================================================


CHANNEL_FORMAT: dict = {
    "loan_origination":         "json",
    "edms_pull":                "json",
    "los_encompass":            "json_array",
    "los_bytepro":              "csv",
    "los_openclose":            "json",
    "mismo_feed":               "xml",
    "employer_adp":             "json",
    "employer_paychex":         "json",
    "employer_gusto":           "json",
    "employer_workday":         "json",
    "employer_manual":          "pdf_only",
    "vendor_equifax":           "json",
    "vendor_experian":          "json",
    "vendor_transunion":        "json",
    "vendor_lexisnexis":        "json",
    "vendor_finicity":          "json",
    "vendor_plaid":             "json",
    "vendor_corelogic":         "json",
    "vendor_mi_mgic":           "json",
    "vendor_mi_radian":         "json",
    "appraisal_mercury":        "pdf_meta",
    "appraisal_corelogic_amc":  "pdf_meta",
    "appraisal_manual":         "pdf_only",
    "title_first_american":     "pdf_meta",
    "title_chicago":            "pdf_meta",
    "title_stewart":            "pdf_meta",
    "title_manual_drop":        "pdf_only",
    "insurance_statefarm":      "pdf_meta",
    "insurance_allstate":       "pdf_meta",
    "insurance_flood_nfip":     "pdf_meta",
    "insurance_wind_hail":      "pdf_meta",
    "insurance_condo_ho6":      "pdf_meta",
    "insurance_manual_drop":    "pdf_only",
    "irs_ives":                 "json",
    "irs_manual":               "pdf_only",
    "ssa_gov":                  "json",
    "va_gov":                   "json",
    "closing_agent":            "pdf_meta",
    "servicer_current":         "json",
    "borrower_portal":          "borrower_portal",
    "email_inbox":              "email_inbox",
    "ai_chat":                  "json",
    "hoa_management":           "pdf_meta",
    "condo_project":            "pdf_meta",
    "conditions_response":      "pdf_meta",
    "corrections":              "pdf_meta",
    "compliance":               "json",
    "loan_officer_notes":       "json",
    "shared_drive":             "pdf_only",
    "attorney_legal":           "pdf_meta",
}


# ===========================================================================
# Source-system ID generators — the ID format each upstream emits.
# ===========================================================================


_SOURCE_ID_FMTS: dict = {
    "loan_origination":         "ENC-2026-{seq:03d}",
    "edms_pull":                "EDMS-{seq:06d}",
    "los_encompass":            "ENC-2026-{seq:05d}",
    "los_bytepro":              "BP-{seq:06d}",
    "los_openclose":            "OC-{seq:06d}",
    "mismo_feed":               "MISMO-{seq:06d}",
    "employer_adp":             "ADP-{dt}-2025-{seq:04d}",
    "employer_paychex":         "PAYCHEX-{dt}-2025-{seq:04d}",
    "employer_gusto":           "GUSTO-{dt}-2025-{seq:04d}",
    "employer_workday":         "WD-{dt}-2025-{seq:04d}",
    "employer_manual":          "MANUAL-EMP-{seq:04d}",
    "vendor_equifax":           "EFX-{dt}-{seq:06d}",
    "vendor_experian":          "EXP-{dt}-{seq:06d}",
    "vendor_transunion":        "TU-{dt}-{seq:06d}",
    "vendor_lexisnexis":        "LN-{seq:08d}",
    "vendor_finicity":          "FIN-{seq:08d}",
    "vendor_plaid":             "PLAID-{seq:08d}",
    "vendor_corelogic":         "CL-{dt}-{seq:06d}",
    "vendor_mi_mgic":           "MGIC-{seq:07d}",
    "vendor_mi_radian":         "RAD-{seq:07d}",
    "appraisal_mercury":        "MERC-RPT-2026-{seq:04d}",
    "appraisal_corelogic_amc":  "CL-AMC-2026-{seq:04d}",
    "appraisal_manual":         "MANUAL-APR-{seq:04d}",
    "title_first_american":     "FA-TC-2026-{seq:05d}",
    "title_chicago":            "CT-2026-{seq:05d}",
    "title_stewart":            "STW-2026-{seq:05d}",
    "title_manual_drop":        "MANUAL-TI-{seq:04d}",
    "insurance_statefarm":      "SF-{seq:08d}",
    "insurance_allstate":       "ALLST-{seq:08d}",
    "insurance_flood_nfip":     "NFIP-{seq:08d}",
    "insurance_wind_hail":      "WH-TX-{seq:06d}",
    "insurance_condo_ho6":      "HO6-{seq:08d}",
    "insurance_manual_drop":    "MANUAL-INS-{seq:04d}",
    "irs_ives":                 "IVES-{seq:08d}",
    "irs_manual":               "BORROWER-IRS-{seq:04d}",
    "ssa_gov":                  "SSA-{ssn4}-2026",
    "va_gov":                   "VA-COE-{seq:06d}",
    "closing_agent":            "CD-2026-{seq:05d}",
    "servicer_current":         "SERV-{seq:08d}",
    "borrower_portal":          "PORTAL-{seq:06d}",
    "email_inbox":              "EMAIL-{seq:06d}",
    "ai_chat":                  "CHAT-{seq:06d}",
    "hoa_management":           "HOA-{seq:06d}",
    "condo_project":            "CONDO-{seq:06d}",
    "conditions_response":      "COND-RESP-{seq:06d}",
    "corrections":              "CORR-{seq:06d}",
    "compliance":               "TRID-{seq:06d}",
    "loan_officer_notes":       "LO-NOTE-{seq:06d}",
    "shared_drive":             "SCAN-{seq:06d}",
    "attorney_legal":           "LEGAL-{seq:06d}",
}


def _source_id(channel: str, seq: int, dt: str = "", ssn4: str = "") -> str:
    fmt = _SOURCE_ID_FMTS.get(channel, f"{channel.upper()}-{{seq:06d}}")
    return fmt.format(seq=seq, dt=dt, ssn4=ssn4)


_SOURCE_SYSTEM_NAME: dict = {
    "edms_pull":                "EDMS_FILENET",
    "los_encompass":            "ENCOMPASS",
    "los_bytepro":              "BYTEPRO",
    "los_openclose":            "OPENCLOSE",
    "mismo_feed":               "MISMO_3.4",
    "employer_adp":             "ADP_PAYROLL",
    "employer_paychex":         "PAYCHEX_PAYROLL",
    "employer_gusto":           "GUSTO_PAYROLL",
    "employer_workday":         "WORKDAY_PAYROLL",
    "employer_manual":          "MANUAL_EMPLOYER",
    "vendor_equifax":           "EQUIFAX",
    "vendor_experian":          "EXPERIAN",
    "vendor_transunion":        "TRANSUNION",
    "vendor_lexisnexis":        "LEXISNEXIS",
    "vendor_finicity":          "FINICITY",
    "vendor_plaid":             "PLAID",
    "vendor_corelogic":         "CORELOGIC",
    "vendor_mi_mgic":           "MGIC",
    "vendor_mi_radian":         "RADIAN",
    "appraisal_mercury":        "MERCURY_NETWORK",
    "appraisal_corelogic_amc":  "CORELOGIC_AMC",
    "appraisal_manual":         "MANUAL_APPRAISER",
    "title_first_american":     "FIRST_AMERICAN",
    "title_chicago":            "CHICAGO_TITLE",
    "title_stewart":            "STEWART_TITLE",
    "title_manual_drop":        "MANUAL_TITLE",
    "insurance_statefarm":      "STATEFARM",
    "insurance_allstate":       "ALLSTATE",
    "insurance_flood_nfip":     "NFIP",
    "insurance_wind_hail":      "TWIA",
    "insurance_condo_ho6":      "ALLSTATE_HO6",
    "insurance_manual_drop":    "MANUAL_INSURANCE",
    "irs_ives":                 "IRS_IVES",
    "irs_manual":               "MANUAL_IRS",
    "ssa_gov":                  "SSA_GOV",
    "va_gov":                   "VA_GOV",
    "closing_agent":            "CLOSING_AGENT",
    "servicer_current":         "SERVICER",
    "borrower_portal":          "BORROWER_PORTAL",
    "email_inbox":              "EMAIL_INBOX",
    "ai_chat":                  "AI_CHATBOT",
    "hoa_management":           "HOA_MGMT",
    "condo_project":            "CONDO_PROJECT",
    "conditions_response":      "UW_CONDITIONS",
    "corrections":              "CORRECTIONS",
    "compliance":               "COMPLIANCE",
    "loan_officer_notes":       "LO_NOTES",
    "shared_drive":             "SHARED_DRIVE",
    "attorney_legal":           "ATTORNEY",
}


# ===========================================================================
# Loan profiles — 10 borrowers + per-loan channel mapping (which payroll
# system, which credit bureau, which AMC, etc.) so the timeline builder
# only emits docs from channels the loan actually uses.
# ===========================================================================


LOAN_PROFILES: dict = {
    "LOAN-101": {
        "primary_name":   "James Wilson",
        "primary_first":  "James", "primary_last": "Wilson",
        "primary_dob":    "1985-01-01", "primary_ssn4": "1001",
        "primary_ssn_hash": "unique_LOAN101_1001",
        "primary_email":  "james.wilson@email.com",
        "primary_phone":  "512-555-0101",
        "primary_address":"456 Elm St, Austin TX 78745",
        "income":         125000, "stated_income": 125000,
        "credit_mid":     752,
        "purchase_price": 450000, "appraised": 460000,
        "loan_amount":    360000, "ltv_pct": 80, "interest_rate": 6.25,
        "city": "Austin", "state": "TX", "zip": "78745", "county": "Travis",
        "subject_address": "123 Oak Valley Dr, Austin TX 78745",
        "employer":       "TechCorp Inc", "employer_ein": "12-3456789",
        "years_employer": 6,
        "bank":           "Chase Bank",
        "occupancy":      "primary_residence",
        "loan_purpose":   "purchase",
        "scenario":       "clean_salaried",
        "primary_payroll":"adp", "co_payroll": None,
        "credit_bureau":  "equifax",
        "title_company":  "first_american",
        "appraisal_amc":  "mercury",
        "insurance_carrier": "statefarm",
        "bank_verify":    "finicity",
        "needs_mi":       False, "mi_provider": None,
        "is_refi":        False, "is_condo": False,
        "is_investment":  False, "is_visa_holder": False,
        "needs_wind_hail":False,
        "stated_assets":  200000,
    },
    "LOAN-102": {
        "primary_name":   "Maria Garcia",
        "primary_first":  "Maria", "primary_last": "Garcia",
        "primary_dob":    "1982-02-02", "primary_ssn4": "1002",
        "primary_ssn_hash": "unique_LOAN102_1002",
        "primary_email":  "maria.garcia@email.com",
        "primary_phone":  "210-555-0102",
        "primary_address":"789 Sunset Blvd, San Antonio TX 78201",
        "income":         109000, "stated_income": 109000,
        "wages_w2":       42000, "income_1099": 67000,
        "credit_mid":     698,
        "purchase_price": 380000, "appraised": 385000,
        "loan_amount":    304000, "ltv_pct": 80, "interest_rate": 6.625,
        "city": "San Antonio", "state": "TX", "zip": "78201", "county": "Bexar",
        "subject_address":"321 Mission Ridge, San Antonio TX 78201",
        "employer":       "Self-Employed",
        "years_employer": 8,
        "bank":           "Wells Fargo",
        "occupancy":      "primary_residence",
        "loan_purpose":   "purchase",
        "scenario":       "self_employed",
        "primary_payroll":"paychex", "co_payroll": None,
        "credit_bureau":  "experian",
        "title_company":  "chicago",
        "appraisal_amc":  "corelogic_amc",
        "insurance_carrier": "allstate",
        "bank_verify":    "plaid",
        "needs_mi":       True, "mi_provider": "mgic",
        "is_refi":        False, "is_condo": False,
        "is_investment":  False, "is_visa_holder": False,
        "needs_wind_hail":False,
        "stated_assets":  85000,
    },
    "LOAN-103": {
        "primary_name":   "David Kim",
        "primary_first":  "David", "primary_last": "Kim",
        "primary_dob":    "1980-03-03", "primary_ssn4": "1003",
        "primary_ssn_hash": "unique_LOAN103_1003",
        "primary_email":  "david.kim@email.com",
        "primary_phone":  "512-555-0103",
        "primary_address":"888 Forest Ln, Round Rock TX 78664",
        "co_name":        "Sarah Kim",
        "co_first":       "Sarah", "co_last": "Kim",
        "co_dob":         "1983-03-13", "co_ssn4": "1013",
        "co_ssn_hash":    "unique_LOAN103C_1013",
        "co_email":       "sarah.kim@email.com",
        "income":         195000, "stated_income": 195000,
        "primary_income": 110000, "co_income": 85000,
        "credit_mid":     740, "co_credit_mid": 720,
        "purchase_price": 620000, "appraised": 625000,
        "loan_amount":    496000, "ltv_pct": 80, "interest_rate": 6.5,
        "city": "Round Rock", "state": "TX", "zip": "78664", "county": "Williamson",
        "subject_address":"42 Greenview Way, Round Rock TX 78664",
        "employer":       "Oracle", "employer_ein": "94-2253743",
        "co_employer":    "HealthSys",
        "years_employer": 7,
        "bank":           "Bank of America",
        "occupancy":      "primary_residence",
        "loan_purpose":   "purchase",
        "scenario":       "joint_dual_income",
        "primary_payroll":"workday", "co_payroll": "gusto",
        "credit_bureau":  "equifax", "co_credit_bureau": "transunion",
        "title_company":  "first_american",
        "appraisal_amc":  "mercury",
        "insurance_carrier": "statefarm",
        "bank_verify":    "finicity",
        "needs_mi":       False, "mi_provider": None,
        "is_refi":        False, "is_condo": False,
        "is_investment":  False, "is_visa_holder": False,
        "needs_wind_hail":False,
        "stated_assets":  340000,
    },
    "LOAN-104": {
        "primary_name":   "Robert Johnson",
        "primary_first":  "Robert", "primary_last": "Johnson",
        "primary_dob":    "1958-04-04", "primary_ssn4": "1004",
        "primary_ssn_hash": "unique_LOAN104_1004",
        "primary_email":  "robert.johnson@email.com",
        "primary_phone":  "512-555-0104",
        "primary_address":"100 Pinewood Dr, Georgetown TX 78626",
        "income":         50400, "stated_income": 50400,
        "pension_monthly":2800, "ssa_monthly": 1400,
        "credit_mid":     790,
        "purchase_price": 290000, "appraised": 295000,
        "loan_amount":    232000, "ltv_pct": 80, "interest_rate": 6.375,
        "city": "Georgetown", "state": "TX", "zip": "78626", "county": "Williamson",
        "subject_address":"55 Sunset Ridge, Georgetown TX 78626",
        "employer":       "Retired",
        "bank":           "Frost Bank",
        "occupancy":      "primary_residence",
        "loan_purpose":   "purchase",
        "scenario":       "retired_fixed_income",
        "primary_payroll":"manual",   # pension letter only
        "co_payroll":     None,
        "credit_bureau":  "equifax",
        "title_company":  "stewart",
        "appraisal_amc":  "manual",
        "insurance_carrier": "statefarm",
        "bank_verify":    None,
        "needs_mi":       False, "mi_provider": None,
        "is_refi":        False, "is_condo": False,
        "is_investment":  False, "is_visa_holder": False,
        "needs_wind_hail":False,
        "stated_assets":  225000,
    },
    "LOAN-105": {
        "primary_name":   "Amanda Chen",
        "primary_first":  "Amanda", "primary_last": "Chen",
        "primary_dob":    "1995-05-05", "primary_ssn4": "1005",
        "primary_ssn_hash": "unique_LOAN105_1005",
        "primary_email":  "amanda.chen@gmail.com",
        "primary_phone":  "512-555-0105",
        "primary_address":"22 Garden St, Pflugerville TX 78660",
        "income":         78000, "stated_income": 78000,
        "credit_mid":     715,
        "purchase_price": 350000, "appraised": 355000,
        "loan_amount":    322000, "ltv_pct": 92, "interest_rate": 6.75,
        "city": "Pflugerville", "state": "TX", "zip": "78660", "county": "Travis",
        "subject_address":"909 Magnolia Ln, Pflugerville TX 78660",
        "employer":       "Hill Country Nonprofit",
        "bank":           "Chase Bank",
        "gift_amount":    25000, "donor_name": "Robert & Linda Chen",
        "occupancy":      "primary_residence",
        "loan_purpose":   "purchase",
        "scenario":       "first_time_gift",
        "primary_payroll":"gusto", "co_payroll": None,
        "credit_bureau":  "equifax",
        "title_company":  "first_american",
        "appraisal_amc":  "mercury",
        "insurance_carrier": "allstate",
        "bank_verify":    "finicity",
        "needs_mi":       True, "mi_provider": "radian",
        "is_refi":        False, "is_condo": False,
        "is_investment":  False, "is_visa_holder": False,
        "needs_wind_hail":False,
        "stated_assets":  35000,
    },
    "LOAN-106": {
        "primary_name":   "Carlos Rivera",
        "primary_first":  "Carlos", "primary_last": "Rivera",
        "primary_dob":    "1978-06-06", "primary_ssn4": "1006",
        "primary_ssn_hash": "unique_LOAN106_1006",
        "primary_email":  "carlos.rivera@email.com",
        "primary_phone":  "214-555-0106",
        "primary_address":"77 Liberty Way, Dallas TX 75201",
        "income":         158000, "stated_income": 158000,
        "wages_w2":       140000, "rental_income": 18000,
        "credit_mid":     760,
        "purchase_price": 425000, "appraised": 430000,
        "loan_amount":    318750, "ltv_pct": 75, "interest_rate": 6.875,
        "city": "Dallas", "state": "TX", "zip": "75201", "county": "Dallas",
        "subject_address":"1700 Crescent Ave, Dallas TX 75201",
        "employer":       "Dell Technologies",
        "bank":           "Chase Bank",
        "occupancy":      "investment_property",
        "loan_purpose":   "purchase",
        "scenario":       "investment_rental",
        "primary_payroll":"adp", "co_payroll": None,
        "credit_bureau":  "equifax",
        "title_company":  "chicago",
        "appraisal_amc":  "corelogic_amc",
        "insurance_carrier": "statefarm",
        "bank_verify":    "finicity",
        "needs_mi":       False, "mi_provider": None,
        "is_refi":        False, "is_condo": False,
        "is_investment":  True,  "is_visa_holder": False,
        "needs_wind_hail":True,   # TX coast wind/hail
        "stated_assets":  450000,
    },
    "LOAN-107": {
        "primary_name":   "Jennifer Brown",
        "primary_first":  "Jennifer", "primary_last": "Brown",
        "primary_dob":    "1984-07-07", "primary_ssn4": "1007",
        "primary_ssn_hash": "unique_LOAN107_1007",
        "primary_email":  "jennifer.brown@email.com",
        "primary_phone":  "512-555-0107",
        "primary_address":"304 Maple Dr, Austin TX 78704",
        "co_name":        "Mike Brown",
        "co_first":       "Mike", "co_last": "Brown",
        "co_dob":         "1982-07-17", "co_ssn4": "1017",
        "co_ssn_hash":    "unique_LOAN107C_1017",
        "co_email":       "mike.brown@email.com",
        "income":         165000, "stated_income": 165000,
        "primary_income": 95000, "co_income": 70000,
        "credit_mid":     735, "co_credit_mid": 748,
        "purchase_price": 0,            # refi — no purchase
        "current_balance":320000, "appraised": 550000,
        "loan_amount":    400000, "ltv_pct": 73, "interest_rate": 6.5,
        "city": "Austin", "state": "TX", "zip": "78704", "county": "Travis",
        "subject_address":"304 Maple Dr, Austin TX 78704",
        "employer":       "Indeed", "co_employer": "AMD",
        "years_employer": 9,
        "bank":           "USAA Federal Savings",
        "occupancy":      "primary_residence",
        "loan_purpose":   "refinance_rate_term",
        "scenario":       "refinance",
        "primary_payroll":"workday", "co_payroll": "adp",
        "credit_bureau":  "equifax",
        "title_company":  "stewart",
        "appraisal_amc":  "mercury",
        "insurance_carrier": "statefarm",
        "bank_verify":    None,        # existing customer; statements via servicer
        "needs_mi":       False, "mi_provider": None,
        "is_refi":        True,  "is_condo": False,
        "is_investment":  False, "is_visa_holder": False,
        "needs_wind_hail":False,
        "stated_assets":  280000,
    },
    "LOAN-108": {
        "primary_name":   "Priya Patel",
        "primary_first":  "Priya", "primary_last": "Patel",
        "primary_dob":    "1990-08-08", "primary_ssn4": "1008",
        "primary_ssn_hash": "unique_LOAN108_1008",
        "primary_email":  "priya.patel@email.com",
        "primary_phone":  "972-555-0108",
        "primary_address":"15 Innovation Blvd, Frisco TX 75033",
        "income":         155000, "stated_income": 155000,
        "credit_mid":     770,
        "purchase_price": 500000, "appraised": 510000,
        "loan_amount":    400000, "ltv_pct": 80, "interest_rate": 6.5,
        "city": "Frisco", "state": "TX", "zip": "75033", "county": "Collin",
        "subject_address":"888 Heritage Pl, Frisco TX 75033",
        "employer":       "TechMega Corp",
        "bank":           "Citibank",
        "visa_status":    "H1B",
        "occupancy":      "primary_residence",
        "loan_purpose":   "purchase",
        "scenario":       "h1b_visa_holder",
        "primary_payroll":"workday", "co_payroll": None,
        "credit_bureau":  "equifax",
        "title_company":  "first_american",
        "appraisal_amc":  "mercury",
        "insurance_carrier": "allstate",
        "bank_verify":    "finicity",
        "needs_mi":       False, "mi_provider": None,
        "is_refi":        False, "is_condo": False,
        "is_investment":  False, "is_visa_holder": True,
        "needs_wind_hail":False,
        "stated_assets":  175000,
    },
    "LOAN-109": {
        "primary_name":   "Thomas O'Brien",
        "primary_first":  "Thomas", "primary_last": "O'Brien",
        "primary_dob":    "1976-09-09", "primary_ssn4": "1009",
        "primary_ssn_hash": "unique_LOAN109_1009",
        "primary_email":  "thomas.obrien@email.com",
        "primary_phone":  "512-555-0109",
        "primary_address":"500 River Run, Cedar Park TX 78613",
        "income":         112000, "stated_income": 112000,
        "wages_w2":       88000, "alimony_monthly": 2000,
        "credit_mid":     680,
        "purchase_price": 310000, "appraised": 315000,
        "loan_amount":    248000, "ltv_pct": 80, "interest_rate": 7.0,
        "city": "Cedar Park", "state": "TX", "zip": "78613", "county": "Williamson",
        "subject_address":"212 Bluebonnet Way, Cedar Park TX 78613",
        "employer":       "RegionalSoft",
        "bank":           "Wells Fargo",
        "occupancy":      "primary_residence",
        "loan_purpose":   "purchase",
        "scenario":       "post_divorce_alimony",
        "primary_payroll":"paychex", "co_payroll": None,
        "credit_bureau":  "experian",
        "title_company":  "stewart",
        "appraisal_amc":  "manual",
        "insurance_carrier": "manual_drop",
        "bank_verify":    None,
        "needs_mi":       True, "mi_provider": "mgic",
        "is_refi":        False, "is_condo": False,
        "is_investment":  False, "is_visa_holder": False,
        "needs_wind_hail":False,
        "needs_attorney_legal": True,
        "stated_assets":  60000,
    },
    "LOAN-110": {
        "primary_name":   "Lisa Zhang",
        "primary_first":  "Lisa", "primary_last": "Zhang",
        "primary_dob":    "1988-10-10", "primary_ssn4": "1010",
        "primary_ssn_hash": "unique_LOAN110_1010",
        "primary_email":  "lisa.zhang@email.com",
        "primary_phone":  "512-555-0110",
        "primary_address":"700 Lakefront Pl, Austin TX 78701",
        "co_name":        "Wei Zhang",
        "co_first":       "Wei", "co_last": "Zhang",
        "co_dob":         "1986-10-20", "co_ssn4": "1020",
        "co_ssn_hash":    "unique_LOAN110C_1020",
        "co_email":       "wei.zhang@email.com",
        "income":         202000, "stated_income": 202000,
        "primary_income": 130000, "co_income": 72000,
        "credit_mid":     755, "co_credit_mid": 740,
        "purchase_price": 475000, "appraised": 480000,
        "loan_amount":    380000, "ltv_pct": 80, "interest_rate": 6.5,
        "city": "Austin", "state": "TX", "zip": "78701", "county": "Travis",
        "subject_address":"100 Congress Ave Unit 1402, Austin TX 78701",
        "employer":       "Google", "co_employer": "Indeed",
        "years_employer": 5,
        "bank":           "Chase Bank",
        "property_type":  "condo", "hoa_monthly": 450,
        "occupancy":      "primary_residence",
        "loan_purpose":   "purchase",
        "scenario":       "condo_hoa_heavy",
        "primary_payroll":"adp", "co_payroll": "paychex",
        "credit_bureau":  "equifax",
        "title_company":  "chicago",
        "appraisal_amc":  "corelogic_amc",
        "insurance_carrier": "condo_ho6",
        "bank_verify":    "finicity",
        "needs_mi":       False, "mi_provider": None,
        "is_refi":        False, "is_condo": True,
        "is_investment":  False, "is_visa_holder": False,
        "needs_wind_hail":False,
        "stated_assets":  410000,
    },
}


# ===========================================================================
# Field generators — extracted_fields per (doc_type, profile, role).
# ===========================================================================


def _income_for(profile: dict, role: str) -> tuple[str, str, int]:
    """Return ``(name, employer, annual_income)`` for the given role."""
    if role == "co_borrower" and profile.get("co_name"):
        return (
            profile["co_name"],
            profile.get("co_employer", profile.get("employer", "Self-Employed")),
            profile.get("co_income", profile.get("income", 0)),
        )
    name = profile["primary_name"]
    employer = profile.get("employer", "Self-Employed")
    income = profile.get("primary_income", profile.get("income", 0))
    return name, employer, income


def _ssn_for(profile: dict, role: str) -> str:
    if role == "co_borrower" and profile.get("co_ssn4"):
        return profile["co_ssn4"]
    return profile.get("primary_ssn4", "0000")


def _extracted_fields(
    doc_type: str, profile: dict, los_id: str, role: str,
    day: int, extras: dict,
) -> dict:
    """Doc-type → field dict. Reuses the v2 field model and extends for
    v3-specific doc types (closing_disclosure, condo_questionnaire,
    conditions_response, attorney_legal, mi_certificate, etc.). Internal
    consistency with the loan profile is preserved so the reconciler
    doesn't fire spurious contradicts."""
    name, employer, income = _income_for(profile, role)
    ssn4 = _ssn_for(profile, role)
    purchase = profile["purchase_price"]
    appraised = profile.get("appraised", purchase + 5000)
    loan_amt  = profile.get("loan_amount", round(purchase * 0.8) if purchase else profile.get("current_balance", 0))

    if doc_type in ("W2_CURRENT", "W2_PRIOR"):
        return {
            "box1_wages":     profile.get("wages_w2", income),
            "box2_fed_tax":   round(income * 0.15),
            "box3_ss_wages":  profile.get("wages_w2", income),
            "tax_year":       "2024" if doc_type == "W2_PRIOR" else "2025",
            "employer_name":  employer,
            "employer_ein":   profile.get("employer_ein", "00-0000000"),
            "employee_name":  name,
            "ssn_last4":      ssn4,
        }
    if doc_type == "PAYSTUB_CURRENT":
        return {
            "ytd_gross":       round(income * 0.42, 2),
            "gross_pay":       round(income / 12, 2),
            "net_pay":         round(income / 12 * 0.72, 2),
            "pay_period_end":  "2026-04-30",
            "pay_frequency":   "monthly",
            "employer_name":   employer,
            "employee_name":   name,
        }
    if doc_type == "URLA_1003":
        return {
            "loan_purpose":          profile.get("loan_purpose", "purchase"),
            "loan_amount":           loan_amt,
            "interest_rate":         profile.get("interest_rate", 6.5),
            "loan_term_months":      360,
            "occupancy":             profile.get("occupancy", "primary_residence"),
            "borrower_name":         profile["primary_name"],
            "borrower_dob":          profile["primary_dob"],
            "borrower_ssn_last4":    profile["primary_ssn4"],
            "co_borrower_name":      profile.get("co_name"),
            "monthly_income_stated": round(profile.get("income", 0) / 12),
            "subject_property_address": profile["subject_address"],
            "subject_property_city": profile["city"],
            "subject_property_state":profile["state"],
        }
    if doc_type == "CREDIT_REPORT":
        mid = profile.get("co_credit_mid") if role == "co_borrower" else profile["credit_mid"]
        mid = mid or profile["credit_mid"]
        return {
            "experian_score":   mid + 8,
            "equifax_score":    mid,
            "transunion_score": mid - 7,
            "mid_score":        mid,
            "credit_band":      "prime" if mid >= 740 else ("near-prime" if mid >= 670 else "subprime"),
            "tradeline_count":  14,
            "total_monthly_obligations": 1450,
            "hard_inquiries_12mo": 2,
        }
    if doc_type.startswith("BANK_STATEMENT") or doc_type == "GIFT_FUNDS_TRAIL":
        suffix = doc_type.rsplit("_", 1)[-1]
        bal_map = {"M1": 62000, "M2": 58500, "M3": 56000}
        bal = bal_map.get(suffix, 60000)
        # Gap-12 (gift verification chain): the M1 statement's
        # ``largest_deposit`` lights up the third step of the chain
        # when it matches the gift amount the gift letter declares.
        largest_dep = (
            profile.get("gift_amount", 0)
            if (suffix == "M1" and profile.get("gift_amount"))
            else round(income / 12 * 0.4, 2)
        )
        return {
            "bank_name":            profile.get("bank", "Chase Bank"),
            "account_holder":       name,
            "ending_balance":       bal,
            "avg_monthly_deposits": round(income / 12 * 0.95, 2),
            "largest_deposit":      largest_dep,
            "months_count":         1,
        }
    if doc_type == "GIFT_DONOR_BANK_STATEMENT":
        return {
            "donor_name":      profile.get("donor_name", "Family"),
            "withdrawal_amount": profile.get("gift_amount", 25000),
            "withdrawal_date": "2026-01-08",
            "donor_balance_before": 85000,
            "donor_balance_after":  85000 - profile.get("gift_amount", 25000),
        }
    if doc_type == "GIFT_LETTER":
        return {
            "gift_amount":        profile.get("gift_amount", 25000),
            "donor_name":         profile.get("donor_name", "Family"),
            "donor_relationship": "parent",
            "repayment_required": False,
            "borrower_name":      name,
        }
    if doc_type == "PURCHASE_AGREEMENT":
        return {
            "purchase_price":   purchase,
            "earnest_money":    5000,
            "closing_date":     "2026-07-15",
            "buyer_name":       profile["primary_name"],
            "seller_name":      "Sample Seller",
            "property_address": profile["subject_address"],
        }
    if doc_type in ("APPRAISAL_URAR", "APPRAISAL_URAR_1073"):
        return {
            "appraised_value":  appraised,
            "property_address": profile["subject_address"],
            "property_type":    "condo" if profile.get("is_condo") else "SFR",
            "condition_rating": "C3",
            "appraisal_form":   "1073" if doc_type.endswith("1073") else "URAR",
            "year_built":       2008,
            "gla_sqft":         2150,
            "bedrooms":         3,
            "bathrooms":        2.5,
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
            "locked_rate":  profile.get("interest_rate", 6.5),
            "lock_expiry":  "2026-07-30",
            "lock_days":    60,
            "loan_amount":  loan_amt,
            "loan_program": "Conv 30yr fixed",
        }
    if doc_type == "TITLE_COMMITMENT":
        return {
            "commitment_number": _source_id(extras.get("__channel", "title_first_american"),
                                            extras.get("__seq", 1)),
            "lender_name":       "EDMS Mortgage",
            "policy_amount":     loan_amt,
            "effective_date":    "2026-04-01",
            "vesting":           "Fee Simple",
            "exceptions_count":  5,
            "tax_lien_clear":    True,
            "judgment_lien_clear": True,
        }
    if doc_type == "TITLE_INSURANCE":
        return {
            "policy_number":   _source_id(extras.get("__channel", "title_first_american"),
                                          extras.get("__seq", 1)),
            "policy_amount":   loan_amt,
            "coverage_amount": loan_amt,
            "effective_date":  "2026-05-15",
            "insured_name":    profile["primary_name"],
        }
    if doc_type in ("HOI_BINDER", "HOI_BINDER_HO6"):
        return {
            "policy_number":     _source_id(extras.get("__channel", "insurance_statefarm"),
                                            extras.get("__seq", 1)),
            "annual_premium":    1800,
            "carrier":           extras.get("source_institution", "StateFarm"),
            "coverage_dwelling": appraised,
            "deductible":        2500,
            "policy_form":       "HO6" if doc_type.endswith("HO6") else "HO3",
            "effective_date":    "2026-05-15",
        }
    if doc_type == "FLOOD_CERT":
        return {
            "flood_zone":              "X",
            "requires_insurance":      False,
            "firm_panel":              "48453C0440K",
            "determination_date":      "2026-01-20",
        }
    if doc_type == "PROPERTY_TAX_BILL":
        return {
            "annual_tax":     round(appraised * 0.018),
            "assessed_value": round(appraised * 0.93),
            "tax_year":       "2025",
            "property_address": profile["subject_address"],
        }
    if doc_type == "DRIVERS_LICENSE":
        dob = profile["primary_dob"] if role == "primary" else profile.get("co_dob", profile["primary_dob"])
        return {
            "dl_number":   f"TX-{ssn4}{day:04d}",
            "state":       "TX",
            "expiry_date": "2028-06-15",
            "name":        name,
            "dob":         dob,
        }
    if doc_type == "SSN_VALIDATION":
        return {
            "ssn_valid":           True,
            "name_match":          True,
            "dob_match":           True,
            "deceased_indicator":  False,
        }
    if doc_type == "OFAC_CHECK":
        return {
            "ofac_clear":     True,
            "sdn_match":      False,
            "pep_match":      False,
            "adverse_media":  False,
        }
    if doc_type == "VOE_TWN":
        return {
            "employer_name":        employer,
            "employment_status":    "Active",
            "hire_date":            "2019-03-01",
            "income_amount":        income,
            "income_frequency":     "annual",
            "position":             "Senior Engineer" if employer in ("Oracle", "TechCorp Inc", "Google") else "Employee",
            "employment_verified":  True,
            "verification_date":    "2026-01-15",
        }
    if doc_type == "AUS_DU_FINDINGS":
        return {
            "recommendation":    "approve_eligible" if profile.get("scenario") != "self_employed" else "refer_with_caution",
            "risk_class":        "low" if profile["credit_mid"] >= 740 else "moderate",
            "casefile_id":       f"DU-{los_id}-001",
            "conditions_count":  3,
            "qualifying_income": income,
            "ltv":               profile.get("ltv_pct", 80),
            "dti":               32.5,
        }
    if doc_type == "SCHEDULE_C":
        return {
            "gross_receipts": profile.get("income_1099", 67000) + 18000,
            "total_expenses": 18000,
            "net_profit":     profile.get("income_1099", 67000),
            "business_name":  f"{name.split()[0]} Studio LLC",
            "tax_year":       "2025",
        }
    if doc_type == "SCHEDULE_E":
        rental = profile.get("rental_income", 18000)
        return {
            "rental_income_gross": rental,
            "rental_expenses":     6000,
            "net_rental_income":   rental - 6000,
            "property_count":      1,
            "tax_year":            "2025",
        }
    if doc_type == "TAX_RETURN_1040_CURRENT":
        return {
            "agi":               profile.get("income", 109000),
            "total_income":      profile.get("income", 109000) + 1500,
            "wages_line1":       profile.get("wages_w2", profile.get("income", 0)),
            "schedule_c_income": profile.get("income_1099", 0),
            "schedule_c_net":    profile.get("income_1099", 0),
            # Depreciation addback for the 2-year average (Gap 1).
            "depreciation":      3500 if profile.get("scenario") == "self_employed" else 0,
            "tax_year":          "2025",
            "filing_status":     "Single",
        }
    if doc_type == "TAX_RETURN_1040_PRIOR":
        # Prior-year tax return for the 2-year-average self-employed
        # calc (Gap 1). Slight delta from current year so the
        # ``trending`` field comes out non-trivial.
        prior_factor = 0.9 if profile.get("scenario") == "self_employed" else 1.0
        return {
            "agi":               round(profile.get("income", 109000) * prior_factor),
            "total_income":      round((profile.get("income", 109000) + 1500) * prior_factor),
            "wages_line1":       round(profile.get("wages_w2", profile.get("income", 0)) * prior_factor),
            "schedule_c_income": round(profile.get("income_1099", 0) * prior_factor),
            "schedule_c_net":    round(profile.get("income_1099", 0) * prior_factor),
            "depreciation":      4000 if profile.get("scenario") == "self_employed" else 0,
            "tax_year":          "2024",
            "filing_status":     "Single",
        }
    if doc_type == "IRS_TRANSCRIPT":
        return {
            "agi":                     profile.get("income", 109000),
            "wages_salaries":          profile.get("wages_w2", profile.get("income", 0)),
            "self_employment_income":  profile.get("income_1099", 0),
            "tax_year":                "2025",
            "filing_status":           "Single",
        }
    if doc_type == "K1_SCHEDULE":
        return {
            "ordinary_income":     8500,
            "interest_income":     250,
            "partnership_name":    f"{name.split()[0]} Holdings LLC",
            "tax_year":            "2025",
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
            "monthly_benefit":   profile.get("ssa_monthly", 1400),
            "effective_date":    "2026-01-01",
            "benefit_type":      "retirement",
            "beneficiary_name":  name,
        }
    if doc_type == "PENSION_LETTER":
        return {
            "monthly_benefit":   profile.get("pension_monthly", 2800),
            "employer_name":     "Texas Retirement",
            "retirement_date":   "2024-11-01",
            "benefit_type":      "defined_benefit",
            "beneficiary_name":  name,
        }
    if doc_type == "RENTAL_LEASE":
        return {
            "monthly_rent":      1500,
            "lease_start":       "2025-01-01",
            "lease_end":         "2026-12-31",
            "tenant_name":       "Tenant Name Redacted",
            "property_address":  "456 Rental Way, Austin TX",
        }
    if doc_type == "MORTGAGE_PAYOFF":
        return {
            "current_balance":  profile.get("current_balance", 295000),
            "payoff_through":   "2026-02-15",
            "lender":           "USAA Federal Savings",
            "loan_number":      f"PRIOR-{los_id}",
        }
    if doc_type == "PAYMENT_HISTORY_24MO":
        return {
            "months_reviewed":  24,
            "late_payments":    0,
            "current":          True,
            "monthly_payment":  1850,   # current mortgage P&I+T+I (Gap 10)
            "current_rate":     4.875,
            "lender":           "USAA Federal Savings",
        }
    if doc_type == "ESCROW_ANALYSIS":
        return {
            "current_balance":  3200,
            "annual_taxes":     6800,
            "annual_insurance": 2100,
            "monthly_escrow":   742,
        }
    if doc_type == "DIVORCE_DECREE":
        return {
            "decree_date":          "2024-01-15",
            "court":                "Travis County District Court",
            "alimony_amount":       profile.get("alimony_monthly", 2000),
            "alimony_frequency":    "monthly",
            # Gap-2: alimony only counts as income with 3+ years
            # remaining; pin to 5 so the income block's gate passes.
            "remaining_years":      5,
            "child_support_amount": 0,
            "division_of_assets":   "per attached property settlement",
        }
    if doc_type == "ALIMONY_ORDER":
        return {
            "monthly_amount": profile.get("alimony_monthly", 2000),
            "duration_months": 60,
            "court_order_id":  "ALIM-2024-09812",
        }
    if doc_type == "ALIMONY_RECEIPT_HISTORY":
        return {
            "months_received": 12,
            "monthly_amount":  profile.get("alimony_monthly", 2000),
            "stable":          True,
        }
    if doc_type in ("VISA_H1B", "EAD_CARD", "PASSPORT", "I94"):
        return {
            "document_type":  doc_type,
            "holder_name":    name,
            "expiry_date":    "2027-08-15",
            "country":        "India" if doc_type == "PASSPORT" else None,
            "visa_status":    profile.get("visa_status", "H1B"),
        }
    if doc_type == "HOA_CERT":
        return {
            "hoa_name":              "Skyline Condo HOA",
            "monthly_dues":          profile.get("hoa_monthly", 450),
            "special_assessments":   0,
            "reserve_balance":       180000,
            "litigation_pending":    False,
            "delinquency":           False,
        }
    if doc_type == "HOA_BUDGET":
        return {"annual_budget": 540000, "reserve_pct": 18}
    if doc_type == "HOA_INSURANCE_MASTER":
        return {"carrier": "Travelers", "annual_premium": 42000, "deductible": 10000}
    if doc_type == "HOA_RESERVE_STUDY":
        return {"reserve_funded_pct": 78, "study_date": "2025-06-01", "next_due": "2030-06-01"}
    if doc_type == "CONDO_QUESTIONNAIRE":
        return {
            "total_units":          240,
            "owner_occupied_pct":   82,
            "reserve_balance":      180000,
            "litigation_pending":   False,
            "insurance_adequate":   True,
            "warrantable":          True,
            "fha_approved":         False,
        }
    if doc_type == "CONDO_PERS_APPROVAL":
        return {"approved": True, "approval_date": "2025-08-01", "expires": "2027-08-01"}
    if doc_type == "MI_CERTIFICATE":
        return {
            "certificate_number": extras.get("__source_id", ""),
            "coverage_pct":       25 if profile.get("ltv_pct", 80) > 90 else 12,
            "monthly_premium":    round(loan_amt * 0.0078 / 12),
            "carrier":            "MGIC" if profile.get("mi_provider") == "mgic" else "Radian",
            "effective_date":     "2026-05-15",
        }
    if doc_type == "CLOSING_DISCLOSURE":
        return {
            "loan_amount":      loan_amt,
            "interest_rate":    profile.get("interest_rate", 6.5),
            "closing_date":     "2026-05-15",
            "cash_to_close":    round(purchase * 0.10) if purchase else 0,
            "total_settlement_charges": 9200,
            "escrow_initial":   3500,
        }
    if doc_type == "WIRE_INSTRUCTIONS":
        return {
            "wire_amount":      round(purchase * 0.85) if purchase else loan_amt,
            "receiving_bank":   "Title Co Trust Account",
            "wire_date":        "2026-05-15",
        }
    if doc_type == "SETTLEMENT_STATEMENT":
        return {
            "total_settlement": round(purchase * 0.10) if purchase else 0,
            "lender_credits":   1200,
            "seller_credits":   3000,
        }
    if doc_type == "TRID_LE":
        return {"loan_amount": loan_amt, "apr": profile.get("interest_rate", 6.5) + 0.18,
                "issued_date": "2026-01-04"}
    if doc_type == "HMDA_LAR":
        return {
            "loan_purpose":         "1" if profile.get("loan_purpose") == "purchase" else "31",
            "occupancy":            "1",
            "loan_amount":          loan_amt,
            "race_code":            "5", "ethnicity_code": "2",
            "applicant_sex":        "1",
        }
    if doc_type == "ECOA_NOTICE":
        return {"sent_date": "2026-01-04", "delivery_method": "email"}
    if doc_type == "LO_NOTE":
        return {"author": "loan_officer", "topic": extras.get("topic", "general"),
                "body": extras.get("body", "Per LO conversation, borrower confirmed details.")}
    if doc_type == "POA":
        return {"principal": profile["primary_name"],
                "agent": "Counsel of record",
                "effective_date": "2026-04-01",
                "purpose": "real_estate_transactions"}
    if doc_type == "TRUST_AGREEMENT":
        return {"trust_name": f"{profile['primary_last']} Family Trust",
                "trustees": [profile["primary_name"]],
                "established": "2020-01-15"}
    if doc_type == "PROPERTY_SETTLEMENT":
        return {"settlement_date": "2024-09-15",
                "property_awarded_to": profile["primary_name"],
                "court": "Travis County District Court"}
    if doc_type == "CONDITIONS_RESPONSE":
        return {"condition_id": extras.get("condition_id", "COND-001"),
                "condition_type": extras.get("condition_type", "additional_bank_stmt"),
                "resolved": True,
                "response_date": "2026-04-01"}
    if doc_type == "CORRECTED_DOC":
        return {"original_doc_id": extras.get("original_doc_id", ""),
                "correction_reason": extras.get("reason", "appraisal_value_revised"),
                "corrected_value":   extras.get("corrected_value")}
    if doc_type == "AI_CHAT_TRANSCRIPT":
        return {
            "explanation_type": extras.get("explanation_type", "large_deposit"),
            "creditor":         extras.get("creditor"),
            "reason":           extras.get("reason"),
            "resolved":         True,
        }
    if doc_type == "CREDIT_EXPLANATION":
        return {
            "explanation_type": "late_payment",
            "creditor":         "Chase Visa",
            "reason":           "divorce_proceedings",
            "resolved":         True,
            "occurred":         "2024-12",
        }
    if doc_type == "VA_COE":
        return {"entitlement_amount": 144000, "service_branch": "Army",
                "discharge_status": "honorable", "issue_date": "2026-01-08"}
    return {"placeholder": True, "doc_type": doc_type}


# ===========================================================================
# Channel timing — when each channel's docs arrive in the lifecycle.
# Days are loan-relative (day 0 = origination).
# ===========================================================================


def _category_for(doc_type: str) -> str:
    if doc_type in {"URLA_1003", "PURCHASE_AGREEMENT", "RATE_LOCK",
                    "MORTGAGE_PAYOFF", "PAYMENT_HISTORY_24MO",
                    "ESCROW_ANALYSIS", "TRID_LE", "HMDA_LAR",
                    "ECOA_NOTICE", "CLOSING_DISCLOSURE",
                    "WIRE_INSTRUCTIONS", "SETTLEMENT_STATEMENT"}:
        return "loan_terms"
    if doc_type.startswith("APPRAISAL") or doc_type in {
        "TITLE_COMMITMENT", "TITLE_INSURANCE", "HOI_BINDER",
        "HOI_BINDER_HO6", "FLOOD_CERT", "PROPERTY_TAX_BILL",
        "AVM_REPORT", "HOA_CERT", "HOA_BUDGET", "HOA_INSURANCE_MASTER",
        "HOA_RESERVE_STUDY", "CONDO_QUESTIONNAIRE",
        "CONDO_PERS_APPROVAL",
    }:
        return "property"
    if doc_type in {"CREDIT_REPORT", "CREDIT_EXPLANATION"}:
        return "credit"
    if doc_type.startswith("BANK_STATEMENT") or doc_type in {
        "RETIREMENT_ACCOUNT", "GIFT_LETTER", "GIFT_FUNDS_TRAIL",
        "GIFT_DONOR_BANK_STATEMENT",
    }:
        return "asset"
    if doc_type in {"DRIVERS_LICENSE", "SSN_VALIDATION", "OFAC_CHECK",
                    "VISA_H1B", "EAD_CARD", "PASSPORT", "I94"}:
        return "identity"
    if doc_type in {"AUS_DU_FINDINGS", "AUS_LP_FINDINGS",
                    "MI_CERTIFICATE", "VA_COE"}:
        return "vendor"
    if doc_type in {"VOE_TWN", "VOE_EQUIFAX"}:
        return "employment"
    if doc_type in {"DIVORCE_DECREE", "ALIMONY_ORDER",
                    "ALIMONY_RECEIPT_HISTORY", "POA",
                    "TRUST_AGREEMENT", "PROPERTY_SETTLEMENT"}:
        return "legal"
    if doc_type in {"CONDITIONS_RESPONSE", "CORRECTED_DOC", "LO_NOTE"}:
        return "process"
    return "income"


# ===========================================================================
# Per-loan timeline builder — generates events from loan profile flags.
# Each event: ``(day, hour, channel, doc_type, role, extras)``.
# ===========================================================================


def _channel_used(profile: dict, channel: str) -> bool:
    """Predicate: should this loan emit docs on this channel?"""
    if channel == "loan_origination":
        return True
    if channel == "edms_pull":
        return True
    if channel == "los_encompass":
        return True
    if channel == "los_bytepro":
        return True   # all loans push a snapshot to BytePro
    if channel == "los_openclose":
        return True   # disclosures land here
    if channel == "mismo_feed":
        return True   # MISMO 3.4 export per loan
    if channel.startswith("employer_"):
        kind = channel.removeprefix("employer_")
        if profile.get("primary_payroll") == kind:
            return True
        if profile.get("co_payroll") == kind:
            return True
        return False
    if channel == "vendor_equifax":
        return profile.get("credit_bureau") == "equifax" or profile.get("co_credit_bureau") == "equifax"
    if channel == "vendor_experian":
        return profile.get("credit_bureau") == "experian"
    if channel == "vendor_transunion":
        return profile.get("co_credit_bureau") == "transunion"
    if channel == "vendor_lexisnexis":
        return True
    if channel == "vendor_finicity":
        return profile.get("bank_verify") == "finicity"
    if channel == "vendor_plaid":
        return profile.get("bank_verify") == "plaid"
    if channel == "vendor_corelogic":
        return True
    if channel == "vendor_mi_mgic":
        return profile.get("needs_mi") and profile.get("mi_provider") == "mgic"
    if channel == "vendor_mi_radian":
        return profile.get("needs_mi") and profile.get("mi_provider") == "radian"
    if channel.startswith("appraisal_"):
        return channel == f"appraisal_{profile.get('appraisal_amc', 'mercury')}"
    if channel.startswith("title_"):
        return channel == f"title_{profile.get('title_company', 'first_american')}"
    if channel == "insurance_statefarm":
        return profile.get("insurance_carrier") == "statefarm"
    if channel == "insurance_allstate":
        return profile.get("insurance_carrier") == "allstate"
    if channel == "insurance_flood_nfip":
        return profile.get("is_condo") or profile.get("needs_flood")
    if channel == "insurance_wind_hail":
        return profile.get("needs_wind_hail", False)
    if channel == "insurance_condo_ho6":
        return profile.get("insurance_carrier") == "condo_ho6"
    if channel == "insurance_manual_drop":
        return profile.get("insurance_carrier") == "manual_drop"
    if channel == "irs_ives":
        return True
    if channel == "irs_manual":
        return profile["scenario"] in ("self_employed", "investment_rental",
                                        "post_divorce_alimony")
    if channel == "ssa_gov":
        return profile["scenario"] == "retired_fixed_income"
    if channel == "va_gov":
        return False     # add a future VA-loan scenario
    if channel == "closing_agent":
        return not profile.get("is_refi", False)
    if channel == "servicer_current":
        return profile.get("is_refi", False)
    if channel == "borrower_portal":
        return True
    if channel == "email_inbox":
        return not profile.get("is_refi", False)
    if channel == "ai_chat":
        return profile["scenario"] in ("first_time_gift", "post_divorce_alimony")
    if channel == "hoa_management":
        return profile.get("is_condo", False)
    if channel == "condo_project":
        return profile.get("is_condo", False)
    if channel == "conditions_response":
        return profile["scenario"] in (
            "self_employed", "first_time_gift", "retired_fixed_income",
            "post_divorce_alimony",
        )
    if channel == "corrections":
        return profile["scenario"] in (
            "retired_fixed_income", "post_divorce_alimony",
        )
    if channel == "compliance":
        return True
    if channel == "loan_officer_notes":
        return True
    if channel == "shared_drive":
        return False    # global drops, not per-loan (appended later)
    if channel == "attorney_legal":
        return profile.get("needs_attorney_legal", False)
    return False


def _build_timeline(profile: dict, los_id: str) -> list:
    """Return list of ``(day, hour, channel, doc_type, role, extras)``
    events for one loan, day 0 = origination."""
    e = []
    primary_payroll  = f"employer_{profile['primary_payroll']}" if profile.get("primary_payroll") else None
    co_payroll       = f"employer_{profile['co_payroll']}" if profile.get("co_payroll") else None
    primary_credit   = f"vendor_{profile['credit_bureau']}"
    co_credit        = f"vendor_{profile['co_credit_bureau']}" if profile.get("co_credit_bureau") else None
    bank_verify      = f"vendor_{profile['bank_verify']}" if profile.get("bank_verify") else None
    appraisal_chan   = f"appraisal_{profile['appraisal_amc']}"
    title_chan       = f"title_{profile['title_company']}"
    insurance_chan   = f"insurance_{profile['insurance_carrier']}"

    # === Day 0 — origination event written separately ===
    # === Day 1-3 — credit pull batch + URLA + LE ===
    e.append((1,  9, "los_encompass", ["URLA_1003"], "primary", {}))
    e.append((1, 10, primary_credit,   "CREDIT_REPORT", "primary", {}))
    if co_credit:
        e.append((2, 10, co_credit, "CREDIT_REPORT", "co_borrower", {}))
    e.append((1, 11, "compliance", "TRID_LE", "primary", {}))
    e.append((2, 13, "compliance", "ECOA_NOTICE", "primary", {}))
    # MISMO export to GSE
    e.append((1, 12, "mismo_feed", "URLA_MISMO_3.4", "primary", {}))
    # BytePro snapshot
    e.append((2, 14, "los_bytepro", "LOAN_SNAPSHOT", "primary", {}))
    # OpenClose disclosure
    e.append((2, 15, "los_openclose", "INITIAL_DISCLOSURE_PKG", "primary", {}))

    # === Day 2-5 — payroll W2 + paystub ===
    if primary_payroll:
        e.append((2,  9, primary_payroll, "W2_CURRENT",      "primary", {}))
        e.append((3,  9, primary_payroll, "W2_PRIOR",        "primary", {}))
        e.append((3, 10, primary_payroll, "PAYSTUB_CURRENT", "primary", {}))
    if co_payroll:
        e.append((4,  9, co_payroll, "W2_CURRENT",      "co_borrower", {}))
        e.append((4, 10, co_payroll, "PAYSTUB_CURRENT", "co_borrower", {}))

    # === Day 3-5 — borrower self-uploads ===
    e.append((4, 14, "borrower_portal", "DRIVERS_LICENSE", "primary", {"format": "jpg"}))
    if profile.get("co_name"):
        e.append((5, 14, "borrower_portal", "DRIVERS_LICENSE", "co_borrower", {"format": "jpg"}))
    if profile.get("is_visa_holder"):
        e.append((5, 15, "borrower_portal", "VISA_H1B", "primary", {"format": "jpg"}))
        e.append((5, 16, "borrower_portal", "EAD_CARD", "primary", {"format": "jpg"}))
        e.append((6, 14, "borrower_portal", "PASSPORT", "primary", {"format": "jpg"}))
        e.append((6, 15, "borrower_portal", "I94",      "primary", {"format": "jpg"}))

    # === Day 5-8 — vendor returns ===
    e.append((5, 11, "vendor_lexisnexis", "SSN_VALIDATION", "primary", {}))
    e.append((5, 11, "vendor_lexisnexis", "OFAC_CHECK",     "primary", {}))
    if profile.get("co_name"):
        e.append((6, 11, "vendor_lexisnexis", "SSN_VALIDATION", "co_borrower", {}))
    e.append((6, 14, "vendor_corelogic",  "AVM_REPORT",  "primary", {}))
    e.append((6, 15, "vendor_corelogic",  "FLOOD_CERT",  "primary", {}))
    e.append((7, 10, "vendor_equifax",   "VOE_TWN",      "primary", {}))
    if profile.get("co_name") and co_payroll:
        e.append((7, 11, "vendor_equifax", "VOE_TWN", "co_borrower", {}))
    e.append((8,  9, "irs_ives", "IRS_TRANSCRIPT", "primary", {}))

    # === Day 7-12 — property docs ===
    e.append((10, 10, appraisal_chan,
              "APPRAISAL_URAR_1073" if profile.get("is_condo") else "APPRAISAL_URAR",
              "primary", {}))
    e.append((10, 11, title_chan, "TITLE_COMMITMENT", "primary", {}))
    e.append((10, 14, insurance_chan, "HOI_BINDER_HO6" if profile.get("is_condo") else "HOI_BINDER",
              "primary", {}))
    if profile.get("needs_wind_hail"):
        e.append((11, 14, "insurance_wind_hail", "WIND_HAIL_INSURANCE", "primary", {}))
    if profile.get("is_condo"):
        e.append((11, 15, "insurance_flood_nfip", "FLOOD_CERT", "primary", {}))

    # === Day 10-15 — bank verify + tax bills + manual docs ===
    if bank_verify:
        e.append((10, 9, bank_verify, "BANK_STATEMENT_M1", "primary", {}))
        e.append((11, 9, bank_verify, "BANK_STATEMENT_M2", "primary", {}))
        if profile.get("is_investment"):
            e.append((11, 10, bank_verify, "BANK_STATEMENT_M3", "primary", {}))
    e.append((12, 14, "edms_pull", "PROPERTY_TAX_BILL", "primary", {}))

    if profile["scenario"] == "self_employed":
        e.append((5, 10, "email_inbox",  "TAX_RETURN_1040_CURRENT", "primary",
                  {"sender": profile["primary_email"], "subject": "2025 Form 1040"}))
        e.append((5, 11, "email_inbox",  "SCHEDULE_C", "primary",
                  {"sender": profile["primary_email"], "subject": "Schedule C 2025"}))
        e.append((6, 10, "email_inbox",  "K1_SCHEDULE", "primary",
                  {"sender": profile["primary_email"], "subject": "K-1"}))
        e.append((15, 9, "email_inbox",  "1099_NEC",   "primary",
                  {"sender": "ar@consultco.com",  "subject": "1099-NEC ConsultCo",
                   "payer_name": "ConsultCo", "amount": 67000, "doc_id_suffix": "consultco"}))
        e.append((15, 11, "email_inbox", "1099_NEC",   "primary",
                  {"sender": "billing@designhub.com", "subject": "1099-NEC DesignHub",
                   "payer_name": "DesignHub", "amount": 33500, "doc_id_suffix": "designhub"}))
        e.append((9, 10, "irs_manual",  "TAX_RETURN_1040_CURRENT", "primary", {}))
        e.append((9, 11, "irs_manual",  "SCHEDULE_C", "primary", {}))
        # Gap-1: 2-year-average needs the prior year's 1040.
        e.append((9, 12, "irs_manual",  "TAX_RETURN_1040_PRIOR", "primary", {}))

    if profile["scenario"] == "investment_rental":
        e.append((6, 10, "irs_manual",  "SCHEDULE_E", "primary", {}))
        e.append((6, 11, "email_inbox", "RENTAL_LEASE", "primary",
                  {"sender": profile["primary_email"], "subject": "Rental lease"}))
        e.append((7, 14, "email_inbox", "RENTAL_LEASE", "primary",
                  {"sender": profile["primary_email"], "subject": "Rental lease unit 2",
                   "doc_id_suffix": "u2"}))

    if profile["scenario"] == "first_time_gift":
        e.append((12, 10, "email_inbox", "GIFT_LETTER", "primary",
                  {"sender": profile["primary_email"], "subject": "Gift letter from parents"}))
        e.append((12, 11, "edms_pull",   "GIFT_FUNDS_TRAIL", "primary", {}))
        # Gap-12: third step of the gift-verification chain — the
        # donor's own bank statement showing the withdrawal that
        # became the borrower's deposit.
        e.append((12, 14, "email_inbox", "GIFT_DONOR_BANK_STATEMENT", "primary",
                  {"sender": "robert.chen@email.com",
                   "subject": "Donor bank stmt for gift"}))
        e.append((13, 19, "ai_chat",     "AI_CHAT_TRANSCRIPT", "primary",
                  {"explanation_type": "large_deposit",
                   "reason": "gift from parents documented"}))

    if profile["scenario"] == "retired_fixed_income":
        e.append((5, 10, "ssa_gov",        "SSA_AWARD_LETTER", "primary", {}))
        # PENSION_LETTER moved from employer_manual (PDF-only, needs
        # AI Vision) to edms_pull (JSON) so monthly_benefit reaches
        # the income block deterministically — pension counts toward
        # qualifying_monthly without depending on the Anthropic key.
        e.append((5, 11, "edms_pull",      "PENSION_LETTER",   "primary", {}))

    if profile["scenario"] == "post_divorce_alimony":
        e.append((7, 10, "email_inbox",   "DIVORCE_DECREE", "primary",
                  {"sender": profile["primary_email"], "subject": "Divorce decree"}))
        e.append((7, 11, "email_inbox",   "ALIMONY_ORDER", "primary",
                  {"sender": profile["primary_email"], "subject": "Alimony order"}))
        e.append((10, 10, "edms_pull",    "ALIMONY_RECEIPT_HISTORY", "primary", {}))
        e.append((10, 19, "ai_chat",      "CREDIT_EXPLANATION", "primary", {}))
        e.append((9,  10, "irs_manual",   "TAX_RETURN_1040_CURRENT", "primary", {}))
        e.append((11, 10, "attorney_legal","DIVORCE_DECREE", "primary", {}))
        e.append((11, 11, "attorney_legal","PROPERTY_SETTLEMENT", "primary", {}))

    if profile["scenario"] == "refinance":
        e.append((5, 10, "servicer_current", "MORTGAGE_PAYOFF",     "primary", {}))
        e.append((5, 11, "servicer_current", "PAYMENT_HISTORY_24MO","primary", {}))
        e.append((6, 11, "servicer_current", "ESCROW_ANALYSIS",     "primary", {}))

    if not profile.get("is_refi"):
        e.append((20, 11, "email_inbox", "PURCHASE_AGREEMENT", "primary",
                  {"sender": profile["primary_email"],
                   "subject": "Purchase agreement signed"}))

    # === Day 15-20 — underwriting ===
    e.append((18, 11, "los_encompass", ["AUS_DU_FINDINGS"], "primary", {}))
    if profile["scenario"] in ("self_employed", "first_time_gift",
                                "retired_fixed_income", "post_divorce_alimony"):
        e.append((19, 14, "conditions_response", "CONDITIONS_RESPONSE", "primary",
                  {"condition_type": "additional_documentation"}))
    if profile["scenario"] == "retired_fixed_income":
        e.append((22, 14, "corrections", "CORRECTED_DOC", "primary",
                  {"reason": "appraisal_value_revised",
                   "corrected_value": profile.get("appraised", 0) + 8000}))
    if profile["scenario"] == "post_divorce_alimony":
        e.append((22, 14, "corrections", "CORRECTED_DOC", "primary",
                  {"reason": "credit_report_post_dispute"}))

    # === Day 18-25 — LO notes (sprinkled across timeline) ===
    e.append((4, 17, "loan_officer_notes", "LO_NOTE", "primary",
              {"topic": "intake", "body": f"Intake call for {profile['primary_name']}"}))
    e.append((14, 16, "loan_officer_notes", "LO_NOTE", "primary",
              {"topic": "underwriting",
               "body": "All initial conditions cleared; awaiting appraisal sign-off."}))
    e.append((25, 16, "loan_officer_notes", "LO_NOTE", "primary",
              {"topic": "pre_close",
               "body": "Rate locked, MI cleared, ready for closing." if profile.get("needs_mi")
                        else "Rate locked, ready for closing."}))

    # === Day 22-30 — pre-closing ===
    e.append((24, 13, "los_encompass", ["RATE_LOCK"], "primary", {}))
    if profile.get("needs_mi"):
        mi_chan = f"vendor_mi_{profile['mi_provider']}"
        e.append((25, 14, mi_chan, "MI_CERTIFICATE", "primary", {}))
    e.append((28, 11, title_chan, "TITLE_INSURANCE", "primary", {}))
    e.append((30, 12, "compliance", "HMDA_LAR", "primary", {}))

    # === Day 32-40 — closing (purchases only) ===
    if not profile.get("is_refi"):
        e.append((35, 10, "closing_agent", "CLOSING_DISCLOSURE",  "primary", {}))
        e.append((36, 11, "closing_agent", "WIRE_INSTRUCTIONS",   "primary", {}))
        e.append((38, 14, "closing_agent", "SETTLEMENT_STATEMENT","primary", {}))

    # === Condo extras ===
    if profile.get("is_condo"):
        e.append((11, 10, "hoa_management", "HOA_CERT",            "primary", {}))
        e.append((11, 11, "hoa_management", "HOA_BUDGET",          "primary", {}))
        e.append((12, 10, "hoa_management", "HOA_INSURANCE_MASTER","primary", {}))
        e.append((12, 11, "hoa_management", "HOA_RESERVE_STUDY",   "primary", {}))
        e.append((13, 10, "condo_project",  "CONDO_QUESTIONNAIRE", "primary", {}))
        e.append((14, 10, "condo_project",  "CONDO_PERS_APPROVAL", "primary", {}))

    return e


# ===========================================================================
# Shared-drive scans — global, not per-loan. 4 scans across the window
# with different artifacts (rotated / landscape / two-doc / faded).
# ===========================================================================


SHARED_DRIVE_DROPS: list = [
    (4,  16, "Loan officer's notes — meeting with borrower, recommend full doc package"),
    (14, 11, "Scanned divorce decree fragment — found in physical mail"),
    (27, 15, "Random property tax bill — borrower brought to office"),
    (44, 10, "Wet signature — purchase agreement copy mailed by seller's agent"),
]


# ===========================================================================
# Time + path helpers
# ===========================================================================


def _received_at(start_date: date, day: int, hour: int) -> str:
    ts = datetime.combine(
        start_date + timedelta(days=day - 1) if day > 0 else start_date,
        datetime.min.time().replace(hour=hour, minute=15),
        tzinfo=timezone.utc,
    )
    return ts.isoformat().replace("+00:00", "Z")


def _date_str(start_date: date, day: int) -> str:
    return (start_date + timedelta(days=day - 1) if day > 0 else start_date).isoformat()


def _channel_dir(out: Path, day: int, channel: str, start_date: date) -> Path:
    if channel == "loan_origination":
        folder = (out / _date_str(start_date, day) / "loan_origination")
    else:
        folder = (out / _date_str(start_date, day) / "post_application" / channel)
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def _doc_id(channel: str, los_id: str, doc_type: str, day: int, hour: int,
            suffix: str = "") -> str:
    prefix_map = {
        "loan_origination":         "LO",
        "edms_pull":                "EDMS",
        "los_encompass":            "ENC",
        "los_bytepro":              "BP",
        "los_openclose":            "OC",
        "mismo_feed":               "MISMO",
        "vendor_equifax":           "EFX",
        "vendor_experian":          "EXP",
        "vendor_transunion":        "TU",
        "vendor_lexisnexis":        "LN",
        "vendor_finicity":          "FIN",
        "vendor_plaid":             "PLAID",
        "vendor_corelogic":         "CL",
        "vendor_mi_mgic":           "MGIC",
        "vendor_mi_radian":         "RAD",
        "appraisal_mercury":        "MERC",
        "appraisal_corelogic_amc":  "CLA",
        "appraisal_manual":         "APR",
        "title_first_american":     "FA",
        "title_chicago":            "CT",
        "title_stewart":            "STW",
        "title_manual_drop":        "TMAN",
        "insurance_statefarm":      "SF",
        "insurance_allstate":       "ALLST",
        "insurance_flood_nfip":     "NFIP",
        "insurance_wind_hail":      "WH",
        "insurance_condo_ho6":      "HO6",
        "insurance_manual_drop":    "INSMAN",
        "irs_ives":                 "IVES",
        "irs_manual":               "IRSM",
        "ssa_gov":                  "SSA",
        "va_gov":                   "VA",
        "closing_agent":            "CLOSE",
        "servicer_current":         "SERV",
        "borrower_portal":          "PORTAL",
        "email_inbox":              "EMAIL",
        "ai_chat":                  "CHAT",
        "hoa_management":           "HOA",
        "condo_project":            "CONDO",
        "conditions_response":      "COND",
        "corrections":              "CORR",
        "compliance":               "TRID",
        "loan_officer_notes":       "LON",
        "shared_drive":             "SCAN",
        "attorney_legal":           "LEGAL",
    }
    employer_prefixes = {
        "employer_adp":     "ADP",
        "employer_paychex": "PAYCHEX",
        "employer_gusto":   "GUSTO",
        "employer_workday": "WD",
        "employer_manual":  "EMANUAL",
    }
    prefix = prefix_map.get(channel) or employer_prefixes.get(channel) or channel.upper()[:6]
    base = f"{prefix}-2026-{los_id}-{doc_type}-D{day:02d}-{hour:02d}"
    return f"{base}-{suffix}" if suffix else base


# ===========================================================================
# Format-specific writers
# ===========================================================================


def _write_json(folder: Path, doc: dict, fname: str) -> int:
    with (folder / fname).open("w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2, default=str)
    return 1


def _write_pdf(folder: Path, fname: str, pdf_bytes: bytes) -> int:
    (folder / fname).write_bytes(pdf_bytes)
    return 1


def _write_xml(folder: Path, fname: str, root: ET.Element) -> int:
    tree = ET.ElementTree(root)
    tree.write(folder / fname, encoding="utf-8", xml_declaration=True)
    return 1


def _write_csv(folder: Path, fname: str, rows: list, headers: list) -> int:
    with (folder / fname).open("w", encoding="utf-8", newline="") as f:
        writer = csv_mod.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    return 1


# ===========================================================================
# Channel dispatch — one entry point per format
# ===========================================================================


_seq_counter = {"n": 1000}


def _next_seq() -> int:
    _seq_counter["n"] += 1
    return _seq_counter["n"]


def _build_envelope(
    channel: str, los_id: str, doc_type: str, role: str, day: int, hour: int,
    fields: dict, profile: dict, start_date: date, extras: dict, doc_id: str,
) -> dict:
    seq = _next_seq()
    dt = _date_str(start_date, day).replace("-", "")
    return {
        "document_id":        doc_id,
        "document_type":      doc_type,
        "category":           _category_for(doc_type),
        "los_id":             los_id,
        "borrower_role":      role,
        "source_system":      _SOURCE_SYSTEM_NAME.get(channel, channel.upper()),
        "source_channel":     channel,
        "source_document_id": _source_id(channel, seq, dt=dt,
                                         ssn4=profile.get("primary_ssn4", "0000")),
        "received_at":        _received_at(start_date, day, hour),
        "extracted_fields":   fields,
    }


def _write_event_json(
    out, channel, los_id, doc_type, role, day, hour, extras, profile, start_date,
):
    fields = _extracted_fields(doc_type, profile, los_id, role, day, extras)
    folder = _channel_dir(out, day, channel, start_date)
    doc_id = _doc_id(channel, los_id, doc_type, day, hour,
                     extras.get("doc_id_suffix", ""))
    doc = _build_envelope(channel, los_id, doc_type, role, day, hour,
                          fields, profile, start_date, extras, doc_id)
    return _write_json(folder, doc, f"{doc_id}.json")


def _write_event_json_array(
    out, channel, los_id, doc_types, role, day, hour, extras, profile, start_date,
):
    """Encompass batch — multiple docs in one JSON array."""
    folder = _channel_dir(out, day, channel, start_date)
    folder_date = _date_str(start_date, day)
    docs = []
    for i, dt in enumerate(doc_types):
        doc_id = _doc_id(channel, los_id, dt, day, hour, str(i))
        fields = _extracted_fields(dt, profile, los_id, role, day, extras)
        docs.append(
            _build_envelope(channel, los_id, dt, role, day, hour,
                            fields, profile, start_date, extras, doc_id)
        )
    fname = f"{los_id}_batch_{folder_date}_h{hour:02d}.json"
    written = _write_json(folder, docs, fname)
    # Per-doc sibling PDFs for format-renderable types (CREDIT_REPORT etc.)
    for d in docs:
        fmt = pdf_formats.format_for(d["document_type"], los_id, role,
                                     fields=d["extracted_fields"])
        if fmt is None:
            continue
        pdf_bytes = pdf_formats.make_pdf(
            d["document_type"], d["extracted_fields"], los_id, role,
        )
        (folder / f"{d['document_id']}.pdf").write_bytes(pdf_bytes)
        written += 1
    return written


def _write_event_csv(
    out, channel, los_id, doc_type, role, day, hour, extras, profile, start_date,
):
    """BytePro CSV with a single snapshot row."""
    folder = _channel_dir(out, day, channel, start_date)
    doc_id = _doc_id(channel, los_id, doc_type, day, hour)
    fname = f"{doc_id}.csv"
    headers = [
        "document_id", "document_type", "los_id", "borrower_name",
        "loan_amount", "interest_rate", "loan_purpose", "ltv_pct",
        "credit_mid", "subject_address", "received_at", "source_document_id",
    ]
    seq = _next_seq()
    row = [
        doc_id, doc_type, los_id, profile["primary_name"],
        profile.get("loan_amount", 0), profile.get("interest_rate", 0),
        profile.get("loan_purpose", "purchase"), profile.get("ltv_pct", 80),
        profile["credit_mid"], profile["subject_address"],
        _received_at(start_date, day, hour),
        _source_id(channel, seq),
    ]
    return _write_csv(folder, fname, [row], headers)


def _write_event_xml(
    out, channel, los_id, doc_type, role, day, hour, extras, profile, start_date,
):
    """Minimal valid MISMO 3.4 envelope wrapping the loan + borrower."""
    folder = _channel_dir(out, day, channel, start_date)
    doc_id = _doc_id(channel, los_id, doc_type, day, hour)
    seq = _next_seq()
    msg = ET.Element("MESSAGE", {
        "xmlns":             "http://www.mismo.org/residential/2009/schemas",
        "MISMOReferenceModelIdentifier": "3.4.0",
        "MessageDateTime":   _received_at(start_date, day, hour),
    })
    deal_sets = ET.SubElement(msg, "DEAL_SETS")
    deal_set  = ET.SubElement(deal_sets, "DEAL_SET")
    deals     = ET.SubElement(deal_set, "DEALS")
    deal      = ET.SubElement(deals, "DEAL")
    parties   = ET.SubElement(deal, "PARTIES")
    borrower  = ET.SubElement(parties, "PARTY")
    individual = ET.SubElement(borrower, "INDIVIDUAL")
    name      = ET.SubElement(individual, "NAME")
    ET.SubElement(name, "FirstName").text = profile["primary_first"]
    ET.SubElement(name, "LastName").text  = profile["primary_last"]
    loan_set  = ET.SubElement(deal, "LOANS")
    loan      = ET.SubElement(loan_set, "LOAN")
    loan_detail = ET.SubElement(loan, "LOAN_DETAIL")
    ET.SubElement(loan_detail, "LoanPurposeType").text = (
        "Purchase" if profile.get("loan_purpose", "purchase") == "purchase"
        else "Refinance"
    )
    terms = ET.SubElement(loan, "TERMS_OF_LOAN")
    ET.SubElement(terms, "BaseLoanAmount").text = str(profile.get("loan_amount", 0))
    ET.SubElement(terms, "NoteRatePercent").text = str(profile.get("interest_rate", 6.5))
    src_id = ET.SubElement(loan, "LOAN_IDENTIFIERS")
    ident  = ET.SubElement(src_id, "LOAN_IDENTIFIER")
    ET.SubElement(ident, "LoanIdentifier").text = los_id
    ET.SubElement(ident, "LoanIdentifierType").text = "LenderLoan"
    fname = f"{doc_id}.xml"
    return _write_xml(folder, fname, msg)


def _write_event_pdf_meta(
    out, channel, los_id, doc_type, role, day, hour, extras, profile, start_date,
):
    """PDF + meta pair — meta carries source-system info, PDF is the raw
    binary evidence."""
    folder = _channel_dir(out, day, channel, start_date)
    fields = _extracted_fields(doc_type, profile, los_id, role, day, extras)
    doc_id = _doc_id(channel, los_id, doc_type, day, hour)
    seq = _next_seq()
    pdf_bytes = pdf_formats.make_pdf(doc_type, fields, los_id, role)
    (folder / f"{doc_id}.pdf").write_bytes(pdf_bytes)
    meta = {
        "document_id":         doc_id,
        "document_type":       doc_type,
        "category":            _category_for(doc_type),
        "los_id":              los_id,
        "borrower_role":       role,
        "source_system":       _SOURCE_SYSTEM_NAME.get(channel, channel.upper()),
        "source_channel":      channel,
        "source_document_id":  _source_id(channel, seq),
        "received_at":         _received_at(start_date, day, hour),
        "attachment_filename": f"{doc_type.lower()}.pdf",
        "extracted_fields":    fields,
    }
    _write_json(folder, meta, f"{doc_id}_meta.json")
    return 2


def _write_event_pdf_only(
    out, channel, los_id, doc_type, role, day, hour, extras, profile, start_date,
):
    """PDF only — no metadata. Forces the AI-Vision classification path
    to fire downstream (employer_manual / appraisal_manual /
    title_manual_drop / insurance_manual_drop / irs_manual /
    shared_drive)."""
    folder = _channel_dir(out, day, channel, start_date)
    fields = _extracted_fields(doc_type, profile, los_id, role, day, extras)
    doc_id = _doc_id(channel, los_id, doc_type, day, hour)
    pdf_bytes = pdf_formats.make_pdf(doc_type, fields, los_id, role)
    (folder / f"{doc_id}.pdf").write_bytes(pdf_bytes)
    return 1


def _write_event_borrower_portal(
    out, channel, los_id, doc_type, role, day, hour, extras, profile, start_date,
):
    folder = _channel_dir(out, day, channel, start_date)
    fields = _extracted_fields(doc_type, profile, los_id, role, day, extras)
    doc_id = _doc_id(channel, los_id, doc_type, day, hour)
    seq = _next_seq()
    declared_ext = extras.get("format", "pdf")
    pdf_bytes = pdf_formats.make_pdf(doc_type, fields, los_id, role)
    (folder / f"{doc_id}.pdf").write_bytes(pdf_bytes)
    uploaded_by = (
        profile.get("co_email") if role == "co_borrower" else profile["primary_email"]
    )
    meta = {
        "document_id":         doc_id,
        "document_type":       doc_type,
        "category":            _category_for(doc_type),
        "los_id":              los_id,
        "borrower_role":       role,
        "source_system":       "BORROWER_PORTAL",
        "source_channel":      channel,
        "source_document_id":  _source_id(channel, seq),
        "received_at":         _received_at(start_date, day, hour),
        "uploaded_by":         uploaded_by,
        "original_filename":   f"{doc_type.lower()}.{declared_ext}",
        "extracted_fields":    fields,
    }
    _write_json(folder, meta, f"{doc_id}_meta.json")
    return 2


def _write_event_email_inbox(
    out, channel, los_id, doc_type, role, day, hour, extras, profile, start_date,
):
    folder = _channel_dir(out, day, channel, start_date)
    fields = _extracted_fields(doc_type, profile, los_id, role, day, extras)
    doc_id = _doc_id(channel, los_id, doc_type, day, hour,
                     extras.get("doc_id_suffix", ""))
    seq = _next_seq()
    pdf_bytes = pdf_formats.make_pdf(doc_type, fields, los_id, role)
    (folder / f"{doc_id}.pdf").write_bytes(pdf_bytes)
    meta = {
        "document_id":         doc_id,
        "document_type":       doc_type,
        "category":            _category_for(doc_type),
        "los_id":               los_id,
        "borrower_role":       role,
        "source_system":       "EMAIL_INBOX",
        "source_channel":      channel,
        "source_document_id":  _source_id(channel, seq),
        "sender":              extras.get("sender", "borrower@email.com"),
        "subject":             extras.get("subject", f"{doc_type} attached"),
        "received_at":         _received_at(start_date, day, hour),
        "attachment_filename": f"{doc_type.lower()}.pdf",
        "extracted_fields":    fields,
    }
    _write_json(folder, meta, f"{doc_id}_meta.json")
    return 2


_WRITER_BY_FORMAT = {
    "json":             _write_event_json,
    "json_array":       _write_event_json_array,
    "csv":              _write_event_csv,
    "xml":              _write_event_xml,
    "pdf_meta":         _write_event_pdf_meta,
    "pdf_only":         _write_event_pdf_only,
    "borrower_portal":  _write_event_borrower_portal,
    "email_inbox":      _write_event_email_inbox,
}


# ===========================================================================
# Origination event + loans_config writers
# ===========================================================================


def _write_origination(
    out: Path, los_id: str, profile: dict, start_date: date,
) -> int:
    folder = _channel_dir(out, 0, "loan_origination", start_date)
    seq = _next_seq()
    co_block = None
    if profile.get("co_name"):
        co_block = {
            "first_name":   profile.get("co_first", ""),
            "last_name":    profile.get("co_last", ""),
            "dob":          profile.get("co_dob", ""),
            "ssn_last4":    profile.get("co_ssn4", ""),
            "ssn_hash":     profile.get("co_ssn_hash", ""),
            "email":        profile.get("co_email", ""),
            "stated_income":profile.get("co_income", 0),
            "stated_employer": profile.get("co_employer", ""),
        }
    event = {
        "event_type":   "loan_application_submitted",
        "los_id":       los_id,
        "received_at":  _received_at(start_date, 0, 9),
        "source_system":"ENCOMPASS",
        "source_document_id": _source_id("loan_origination", seq),
        "legacy_ids": {
            "encompass_loan_number":   f"ENC-2026-{los_id.split('-')[-1]}",
            "encompass_borrower_id":   f"ENC-BR-{seq:05d}",
        },
        "loan_terms": {
            "loan_purpose":      profile.get("loan_purpose", "purchase"),
            "loan_amount":       profile.get("loan_amount", 0),
            "interest_rate":     profile.get("interest_rate", 6.5),
            "loan_term_months":  360,
            "occupancy":         profile.get("occupancy", "primary_residence"),
            "property_type":     "condo" if profile.get("is_condo") else "SFR",
        },
        "borrower": {
            "first_name":      profile["primary_first"],
            "last_name":       profile["primary_last"],
            "dob":             profile["primary_dob"],
            "ssn_last4":       profile["primary_ssn4"],
            "ssn_hash":        profile.get("primary_ssn_hash", ""),
            "email":           profile["primary_email"],
            "phone":           profile["primary_phone"],
            "current_address": profile["primary_address"],
            "stated_income":   profile.get("stated_income", profile.get("income", 0)),
            "stated_employer": profile.get("employer", ""),
            "years_at_employer": profile.get("years_employer", 5),
            "stated_assets":   profile.get("stated_assets", 100000),
        },
        "co_borrower":   co_block,
        "property": {
            "address":          profile["subject_address"],
            "city":             profile["city"],
            "state":            profile["state"],
            "zip":              profile["zip"],
            "county":           profile["county"],
            "type":             "condo" if profile.get("is_condo") else "SFR",
            "purchase_price":   profile["purchase_price"],
            "estimated_value":  profile.get("appraised", profile["purchase_price"]),
        },
    }
    return _write_json(folder, event, f"{los_id}_application.json")


def _write_loans_config(out: Path, profiles: dict) -> None:
    """Top-level bootstrap config the backtest reads to POST /loans for
    each profile in turn."""
    cfg: list = []
    for los_id, p in profiles.items():
        co = None
        if p.get("co_name"):
            co = {
                "first_name":  p.get("co_first", ""),
                "last_name":   p.get("co_last", ""),
                "dob":         p.get("co_dob", ""),
                "ssn_last4":   p.get("co_ssn4", ""),
                "ssn_hash":    p.get("co_ssn_hash", ""),
                "email":       p.get("co_email", ""),
                "income":      p.get("co_income", 0),
                "employer":    p.get("co_employer", ""),
                "credit_mid":  p.get("co_credit_mid", 0),
            }
        cfg.append({
            "los_id":       los_id,
            "scenario":     p["scenario"],
            "loan_purpose": p.get("loan_purpose", "purchase"),
            "borrower": {
                "first_name":  p["primary_first"],
                "last_name":   p["primary_last"],
                "dob":         p["primary_dob"],
                "ssn_last4":   p["primary_ssn4"],
                "ssn_hash":    p.get("primary_ssn_hash", ""),
                "email":       p["primary_email"],
                "phone":       p["primary_phone"],
                "income":      p.get("income", 0),
                "employer":    p.get("employer", ""),
                "credit_mid":  p["credit_mid"],
                "ltv_pct":     p.get("ltv_pct", 80),
            },
            "co_borrower": co,
            "property": {
                "address":         p["subject_address"],
                "city":            p["city"],
                "state":           p["state"],
                "zip":             p["zip"],
                "type":            "condo" if p.get("is_condo") else "SFR",
                "purchase_price":  p["purchase_price"],
                "appraised":       p.get("appraised", p["purchase_price"]),
                "occupancy":       p.get("occupancy", "primary_residence"),
            },
            "loan_terms": {
                "loan_amount":   p.get("loan_amount", 0),
                "interest_rate": p.get("interest_rate", 6.5),
            },
            "needs_mi":   p.get("needs_mi", False),
            "is_refi":    p.get("is_refi", False),
            "is_condo":   p.get("is_condo", False),
        })
    with (out / "loans_config.json").open("w", encoding="utf-8") as f:
        json.dump({"loans": cfg, "generated_at": datetime.now(timezone.utc).isoformat()},
                  f, indent=2, default=str)


# ===========================================================================
# Orchestration
# ===========================================================================


def generate(
    out_dir: Path, start_date: date, num_days: int, num_loans: int, clean: bool,
) -> dict:
    if clean and out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    by_channel: dict = {}
    by_loan: dict    = {}
    by_day: dict     = {}
    by_format: dict  = {}
    files_total = 0

    selected_loans = list(LOAN_PROFILES.items())[:num_loans]

    def _bump(channel: str, n: int, los_id: str, day: int, fmt: str):
        nonlocal files_total
        files_total += n
        by_channel[channel] = by_channel.get(channel, 0) + n
        by_loan[los_id]     = by_loan.get(los_id, 0) + n
        by_day[day]         = by_day.get(day, 0) + n
        by_format[fmt]      = by_format.get(fmt, 0) + n

    for los_id, profile in selected_loans:
        # Day 0 — origination event
        n = _write_origination(out_dir, los_id, profile, start_date)
        _bump("loan_origination", n, los_id, 0, "json")

        # Day 1+ — timeline events
        timeline = _build_timeline(profile, los_id)
        for day, hour, channel, doc_type, role, extras in timeline:
            if day > num_days:
                continue
            if not _channel_used(profile, channel):
                continue
            extras = dict(extras) if extras else {}
            extras["__channel"] = channel
            extras["__seq"]     = _seq_counter["n"]
            fmt = CHANNEL_FORMAT.get(channel)
            if fmt is None:
                continue
            writer = _WRITER_BY_FORMAT[fmt]
            if fmt == "json_array":
                # doc_type is a list for batch channels
                doc_types = doc_type if isinstance(doc_type, list) else [doc_type]
                n = writer(out_dir, channel, los_id, doc_types, role, day, hour,
                           extras, profile, start_date)
            else:
                n = writer(out_dir, channel, los_id, doc_type, role, day, hour,
                           extras, profile, start_date)
            _bump(channel, n, los_id, day, fmt)

    # Global shared-drive scans (4)
    from scripts.pdf_formats import make_shared_drive_scan
    for variant_idx, (day, hour, scan_text) in enumerate(SHARED_DRIVE_DROPS):
        if day > num_days:
            continue
        folder = _channel_dir(out_dir, day, "shared_drive", start_date)
        ts = (start_date + timedelta(days=day - 1)).strftime("%Y%m%d") + f"-{hour:02d}{15:02d}"
        (folder / f"scan_{ts}.pdf").write_bytes(
            make_shared_drive_scan(scan_text, variant_idx)
        )
        _bump("shared_drive", 1, "GLOBAL", day, "pdf_only")

    _write_loans_config(out_dir, dict(selected_loans))

    return {
        "files_total": files_total,
        "by_channel":  by_channel,
        "by_loan":     by_loan,
        "by_day":      by_day,
        "by_format":   by_format,
        "out_dir":     str(out_dir),
        "loans":       len(selected_loans),
        "days":        num_days,
    }


def s3_sync(local_dir: Path, s3_target: str, dry_run: bool = False) -> int:
    """``aws s3 sync ... --sse AES256 --delete`` so the bucket-default
    KMS encryption doesn't lock the connector out, and stale objects
    from prior runs are removed."""
    cmd = ["aws", "s3", "sync", str(local_dir), s3_target,
           "--sse", "AES256", "--delete"]
    if dry_run:
        cmd.append("--dryrun")
    print(f"\n+ {' '.join(cmd)}")
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.stdout:
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
    ap.add_argument("--loans", type=int, default=DEFAULT_LOANS)
    ap.add_argument("--clean", action="store_true",
                    help="rm -rf the output dir before writing")
    ap.add_argument("--upload", action="store_true",
                    help=f"after generating, run aws s3 sync to {DEFAULT_S3_TARGET}")
    ap.add_argument("--s3-target", default=DEFAULT_S3_TARGET)
    ap.add_argument("--dry-run-s3", action="store_true")
    args = ap.parse_args()

    start = datetime.fromisoformat(args.start).date()
    out   = Path(args.out)
    summary = generate(out, start, args.days, args.loans, clean=args.clean)

    print(f"\nWrote {summary['files_total']} files to {summary['out_dir']}")
    print(f"  Loans: {summary['loans']}    Days: {summary['days']}    "
          f"Start date: {start}")
    print(f"  By format:")
    for fmt, n in sorted(summary["by_format"].items(), key=lambda x: -x[1]):
        print(f"    {fmt:18s} {n}")
    print(f"  By channel:")
    for ch, n in sorted(summary["by_channel"].items(), key=lambda x: -x[1]):
        print(f"    {ch:30s} {n}")
    print(f"  By loan:")
    for los_id, n in sorted(summary["by_loan"].items()):
        prof = LOAN_PROFILES.get(los_id, {"scenario": "global", "primary_name": "(scans)"})
        print(f"    {los_id:10s} ({prof['scenario']:25s} | {prof.get('primary_name','?'):18s}): {n}")
    days = sorted(summary["by_day"].items())
    print(f"  Day spread: {days[0][0]} -> {days[-1][0]} "
          f"(min/day={min(n for _, n in days)}, "
          f"max/day={max(n for _, n in days)})")

    if args.upload:
        rc = s3_sync(out, args.s3_target, dry_run=args.dry_run_s3)
        sys.exit(rc)


if __name__ == "__main__":
    main()
