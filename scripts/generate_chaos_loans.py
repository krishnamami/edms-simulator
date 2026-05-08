#!/usr/bin/env python3
"""Generate 5 messy real-world loan scenarios that break EDMS in realistic ways.

Each scenario targets a different failure mode that production systems hit:

SCENARIO 1: "The Self-Employed Borrower from Hell"
  - Schedule C with negative income (net loss)
  - K-1 with guaranteed payments but negative ordinary income
  - 1099s from 4 different payers (one with $0 amount)
  - Declining income trend (2025 < 2024 < 2023)
  - IRS transcript AGI doesn't match 1040 (amended return)
  - Bank statements show large unexplained deposits

SCENARIO 2: "The Co-Borrower Nightmare"
  - Primary has NO income (homemaker)
  - All income from co-borrower
  - Co-borrower has job gap (offer letter start date is future)
  - Gift letter from non-family member (employer)
  - Credit report with 30-day late from 18 months ago
  - Two different SSN last4 on different docs (typo)

SCENARIO 3: "The Property Disaster"
  - Appraisal comes in BELOW purchase price (short appraisal)
  - AVM is 25% below appraisal (outside tolerance — contradicts)
  - Flood zone change (cert says X, insurance says AE)
  - HOA with pending litigation
  - Title commitment with 6 exceptions
  - Pest inspection finds active termites
  - Rural property (well + septic required)

SCENARIO 4: "The Data Quality Catastrophe"
  - Fields with None/null values where numbers expected
  - Strings where numbers should be ("one hundred thousand")
  - Negative values (negative bank balance, negative tax)
  - Dates in wrong format (MM/DD/YYYY vs YYYY-MM-DD)
  - Unicode characters in names (José, O'Brien, hyphenated)
  - Extremely long employer name (overflow test)
  - Empty PDF (0 bytes)
  - Duplicate document IDs
  - Same doc uploaded 3 times with different values each time

SCENARIO 5: "The Late-Arriving Everything"
  - Rate lock expired (lock_expiry in the past)
  - Appraisal older than 120 days
  - W2 is for wrong tax year (2023 instead of 2025)
  - Paystub from 90 days ago (stale)
  - Credit report pulled 180 days ago
  - VOE shows "Terminated" status
  - AUS findings: "Refer with Caution"
  - Second appraisal contradicts first by 15%

Each scenario generates 15-25 documents + a manifest.
"""
import os
import json
import random
import string
import base64
from datetime import datetime, timedelta
from reportlab.lib.pagesizes import letter
from reportlab.lib.colors import HexColor, black, white
from reportlab.pdfgen import canvas

W, H = letter
BASE_OUT = os.path.join(os.path.dirname(__file__), "chaos_loan_files")


def _header(c, title, subtitle=""):
    c.setFillColor(HexColor("#8B0000"))
    c.rect(0, H - 80, W, 80, fill=1, stroke=0)
    c.setFillColor(white)
    c.setFont("Helvetica-Bold", 16)
    c.drawString(40, H - 45, title)
    if subtitle:
        c.setFont("Helvetica", 10)
        c.drawString(40, H - 62, subtitle)
    c.setFont("Helvetica", 8)
    c.drawRightString(W - 40, H - 30, "CHAOS TEST")
    c.setFillColor(black)


def _fields(c, x, y, pairs):
    c.setFont("Helvetica", 9)
    for label, value in pairs:
        c.setFont("Helvetica-Bold", 9)
        c.drawString(x, y, f"{label}:")
        c.setFont("Helvetica", 9)
        c.drawString(x + 180, y, str(value))
        y -= 16
    return y


def _make_pdf(out_dir, filename, title, subtitle, fields_list):
    path = os.path.join(out_dir, filename)
    c = canvas.Canvas(path, pagesize=letter)
    _header(c, title, subtitle)
    y = H - 110
    for section_name, fields in fields_list:
        c.setFont("Helvetica-Bold", 11)
        c.setFillColor(HexColor("#8B0000"))
        c.drawString(40, y, section_name)
        c.line(40, y - 3, W - 40, y - 3)
        c.setFillColor(black)
        y -= 20
        y = _fields(c, 40, y, fields)
        y -= 10
    c.save()
    return filename


def _empty_pdf(out_dir, filename):
    """Create an empty/corrupt PDF."""
    path = os.path.join(out_dir, filename)
    with open(path, "wb") as f:
        f.write(b"")  # truly empty
    return filename


def _garbage_pdf(out_dir, filename):
    """Create a PDF with garbage bytes."""
    path = os.path.join(out_dir, filename)
    with open(path, "wb") as f:
        f.write(b"%PDF-1.4\n" + os.urandom(500) + b"\n%%EOF")
    return filename


# =====================================================================
# SCENARIO 1: Self-Employed Borrower from Hell
# =====================================================================
def gen_scenario_1():
    name = "scenario_1_self_employed"
    out = os.path.join(BASE_OUT, name)
    os.makedirs(out, exist_ok=True)
    docs = []

    borrower = {"first": "Maria", "last": "Gonzalez-Rivera", "ssn_last4": "1234"}
    los_id = "LOS-CHAOS-SE01"

    # W2 with LOW wages (side job, not main income)
    docs.append({"filename": _make_pdf(out, "W2_2025.pdf",
        "W-2 Wage and Tax Statement — 2025", "Part-time consulting",
        [("Employee", [
            ("Employee Name", f"{borrower['first']} {borrower['last']}"),
            ("SSN", f"XXX-XX-{borrower['ssn_last4']}"),
            ("Employer", "QuickBooks Freelance Payroll"),
            ("Employer EIN", "00-0000000"),
        ]), ("Wages", [
            ("Box 1 — Wages", "$8,500.00"),
            ("Tax Year", "2025"),
        ])]),
        "doc_type": "W2_CURRENT", "category": "income",
        "fields": {"box1_wages": 8500, "tax_year": "2025",
                   "employer_name": "QuickBooks Freelance Payroll"}
    })

    # Schedule C with NET LOSS
    docs.append({"filename": _make_pdf(out, "SCHEDULE_C_2025.pdf",
        "Schedule C — Profit or Loss from Business", "Tax Year 2025",
        [("Business", [
            ("Business Name", "MG Web Design LLC"),
            ("Principal Business", "Web Development"),
            ("EIN", "45-6789012"),
        ]), ("Income/Loss", [
            ("Gross Receipts", "$145,000.00"),
            ("Total Expenses", "$162,000.00"),
            ("Net Profit (Loss)", "($17,000.00)"),
            ("Depreciation", "$28,000.00"),
            ("Amortization", "$5,000.00"),
            ("Tax Year", "2025"),
        ])]),
        "doc_type": "SCHEDULE_C", "category": "income",
        "fields": {"gross_receipts": 145000, "total_expenses": 162000,
                   "net_profit": -17000, "depreciation": 28000,
                   "amortization": 5000, "business_name": "MG Web Design LLC",
                   "tax_year": "2025"}
    })

    # K-1 with negative ordinary, positive guaranteed
    docs.append({"filename": _make_pdf(out, "K1_2025.pdf",
        "Schedule K-1 (Form 1065)", "Tax Year 2025",
        [("Partnership", [
            ("Partnership", "Southwest Ventures LP"),
            ("EIN", "77-1111111"),
            ("Box 1 — Ordinary income", "($5,200.00)"),
            ("Box 4 — Guaranteed payments", "$36,000.00"),
            ("Box 2 — Net rental", "$0.00"),
            ("Tax Year", "2025"),
        ])]),
        "doc_type": "K1_SCHEDULE", "category": "income",
        "fields": {"ordinary_income": -5200, "guaranteed_payments": 36000,
                   "rental_income": 0, "partnership_name": "Southwest Ventures LP",
                   "tax_year": "2025"}
    })

    # 1099s from 4 payers — one with $0
    for i, (payer, amount) in enumerate([
        ("Acme Consulting", 42000),
        ("Beta Corp", 18500),
        ("Gamma Industries", 7200),
        ("Delta LLC", 0),  # zero amount — should handle gracefully
    ]):
        docs.append({"filename": _make_pdf(out, f"1099_NEC_{i+1}.pdf",
            f"1099-NEC — {payer}", "Tax Year 2025",
            [("", [
                ("Payer", payer),
                ("Nonemployee Compensation", f"${amount:,.2f}"),
                ("Tax Year", "2025"),
            ])]),
            "doc_type": "FORM_1099_NEC", "category": "income",
            "fields": {"nonemployee_compensation": amount, "payer_name": payer,
                       "tax_year": "2025"}
        })

    # IRS transcript — AGI DOESN'T MATCH 1040 (amended return)
    docs.append({"filename": _make_pdf(out, "IRS_TRANSCRIPT.pdf",
        "IRS Tax Return Transcript", "4506-C — AMENDED",
        [("Income", [
            ("Wages", "$8,500.00"),
            ("Schedule C net", "($17,000.00)"),
            ("1099-NEC total", "$67,700.00"),
            ("K-1 ordinary", "($5,200.00)"),
            ("K-1 guaranteed", "$36,000.00"),
            ("AGI (as amended)", "$82,350.00"),
            ("AGI (original filing)", "$91,500.00"),
            ("AMENDED RETURN", "YES"),
            ("Tax Year", "2025"),
        ])]),
        "doc_type": "IRS_TRANSCRIPT", "category": "income",
        "fields": {"agi": 82350, "wages_salaries": 8500,
                   "self_employment_income": -17000,
                   "schedule_c_net": -17000,
                   "tax_year": "2025", "amended": True}
    })

    # 1040 — original (doesn't match transcript)
    docs.append({"filename": _make_pdf(out, "FORM_1040.pdf",
        "Form 1040 — Tax Year 2025", "ORIGINAL FILING",
        [("Income", [
            ("Line 1 — Wages", "$8,500.00"),
            ("Schedule C income", "$12,000.00"),
            ("AGI", "$91,500.00"),
            ("Tax Year", "2025"),
        ])]),
        "doc_type": "FORM_1040", "category": "income",
        "fields": {"agi": 91500, "wages_line1": 8500,
                   "schedule_c_income": 12000, "tax_year": "2025"}
    })

    # Bank statements with LARGE unexplained deposits
    for m, (balance, deposit_note) in enumerate([
        (92000, "Includes $45,000 wire from unknown source"),
        (47000, "Normal activity"),
        (48500, "Includes $30,000 cash deposit"),
    ], 1):
        docs.append({"filename": _make_pdf(out, f"BANK_STMT_M{m}.pdf",
            f"Bank Statement — Month {m}", "Chase Bank",
            [("Account", [
                ("Ending Balance", f"${balance:,.2f}"),
                ("Large Deposits", deposit_note),
                ("Average Balance", f"${balance * 0.85:,.2f}"),
            ])]),
            "doc_type": f"BANK_STATEMENT_M{m}", "category": "asset",
            "fields": {"ending_balance": balance,
                       "avg_balance": round(balance * 0.85),
                       "large_deposits_noted": True}
        })

    # Declining income: prior year W2 is HIGHER
    docs.append({"filename": _make_pdf(out, "W2_2024.pdf",
        "W-2 — 2024", "Prior Year (HIGHER than current)",
        [("", [
            ("Box 1 — Wages", "$52,000.00"),
            ("Employer", "OldJob Corp"),
            ("Tax Year", "2024"),
        ])]),
        "doc_type": "W2_PRIOR", "category": "income",
        "fields": {"box1_wages": 52000, "tax_year": "2024",
                   "employer_name": "OldJob Corp"}
    })

    # Credit report
    docs.append({"filename": _make_pdf(out, "CREDIT_REPORT.pdf",
        "Tri-Merge Credit Report", "",
        [("Scores", [
            ("Mid Score", "698"),
            ("Tradelines", "15"),
            ("Monthly Payments", "$2,350.00"),
            ("Collections", "1 (medical, $450, disputed)"),
        ])]),
        "doc_type": "CREDIT_REPORT", "category": "credit",
        "fields": {"mid_score": 698, "tradeline_count": 15,
                   "total_monthly_payments": 2350, "derogatory_count": 1,
                   "collections_count": 1}
    })

    # Standard property docs
    docs.append({"filename": _make_pdf(out, "APPRAISAL.pdf",
        "Appraisal — URAR", "",
        [("", [("Appraised Value", "$385,000"), ("Property Type", "SFR")])]),
        "doc_type": "APPRAISAL_URAR", "category": "property",
        "fields": {"appraised_value": 385000, "property_type": "SFR"}
    })

    docs.append({"filename": _make_pdf(out, "PURCHASE_AGT.pdf",
        "Purchase Agreement", "",
        [("", [("Purchase Price", "$379,000"), ("Closing Date", "2026-08-15")])]),
        "doc_type": "PURCHASE_AGREEMENT", "category": "loan_terms",
        "fields": {"purchase_price": 379000, "closing_date": "2026-08-15"}
    })

    # DL and identity
    docs.append({"filename": _make_pdf(out, "DL.pdf",
        "Driver's License", "State of Texas",
        [("", [("Name", "Maria Gonzalez-Rivera"), ("DL#", "TX-98765432"),
               ("Expiry", "2027-11-30")])]),
        "doc_type": "DRIVERS_LICENSE", "category": "identity",
        "fields": {"dl_number": "TX-98765432", "state": "TX",
                   "expiry_date": "2027-11-30", "name_match": True}
    })

    _write_manifest(out, docs, los_id, borrower, name)
    return name, len(docs)


# =====================================================================
# SCENARIO 2: Co-Borrower Nightmare
# =====================================================================
def gen_scenario_2():
    name = "scenario_2_coborrower"
    out = os.path.join(BASE_OUT, name)
    os.makedirs(out, exist_ok=True)
    docs = []

    primary = {"first": "Thomas", "last": "O'Brien-Smith", "ssn_last4": "5678"}
    co = {"first": "Sarah", "last": "O'Brien-Smith", "ssn_last4": "9012"}
    los_id = "LOS-CHAOS-CO02"

    # Primary has NO W2 — homemaker
    # No income docs for primary at all

    # Co-borrower W2
    docs.append({"filename": _make_pdf(out, "CO_W2_2025.pdf",
        "W-2 — 2025", "Co-Borrower Income",
        [("", [
            ("Employee", f"{co['first']} {co['last']}"),
            ("Box 1 — Wages", "$142,000.00"),
            ("Employer", "Memorial Hermann Health System — Department of Radiology and Interventional Procedures"),
            ("Tax Year", "2025"),
        ])]),
        "doc_type": "W2_CURRENT", "category": "income", "role": "co_borrower",
        "fields": {"box1_wages": 142000, "tax_year": "2025",
                   "employer_name": "Memorial Hermann Health System — Department of Radiology and Interventional Procedures"}
    })

    # Co-borrower paystub
    docs.append({"filename": _make_pdf(out, "CO_PAYSTUB.pdf",
        "Earnings Statement", "Co-Borrower",
        [("", [
            ("Name", f"{co['first']} {co['last']}"),
            ("YTD Gross", "$71,000.00"),
            ("Pay Period End", "2026-06-15"),
            ("Pay Frequency", "Semi-Monthly"),
        ])]),
        "doc_type": "PAYSTUB_CURRENT", "category": "income", "role": "co_borrower",
        "fields": {"ytd_gross": 71000, "pay_period_end": "2026-06-15",
                   "pay_frequency": "semi_monthly"}
    })

    # Co-borrower has a FUTURE start date offer letter (job change mid-loan)
    docs.append({"filename": _make_pdf(out, "CO_OFFER_LETTER.pdf",
        "Offer Letter — NEW POSITION", "Starts AFTER closing",
        [("", [
            ("Employer", "Texas Children's Hospital"),
            ("Position", "Lead Radiologist"),
            ("Start Date", "2026-09-01"),
            ("Base Salary", "$165,000.00"),
            ("Current Employer", "Memorial Hermann (resigning)"),
        ])]),
        "doc_type": "OFFER_LETTER", "category": "employment", "role": "co_borrower",
        "fields": {"employer_name": "Texas Children's Hospital",
                   "position_title": "Lead Radiologist",
                   "start_date": "2026-09-01", "base_salary": 165000,
                   "employment_type": "full_time"}
    })

    # Gift letter from EMPLOYER (non-family — red flag)
    docs.append({"filename": _make_pdf(out, "GIFT_LETTER.pdf",
        "Gift Letter", "FROM EMPLOYER — NON-FAMILY DONOR",
        [("", [
            ("Donor Name", "Memorial Hermann Health System"),
            ("Donor Relationship", "Employer"),
            ("Gift Amount", "$25,000.00"),
            ("Repayment Required", "No"),
            ("Source of Funds", "Employee Assistance Program"),
        ])]),
        "doc_type": "GIFT_LETTER", "category": "asset",
        "fields": {"gift_amount": 25000, "donor_name": "Memorial Hermann Health System",
                   "donor_relationship": "employer", "repayment_required": False}
    })

    # Credit report with 30-day late
    docs.append({"filename": _make_pdf(out, "CREDIT_REPORT.pdf",
        "Tri-Merge Credit Report", "",
        [("Scores", [
            ("Experian", "721"),
            ("TransUnion", "718"),
            ("Equifax", "725"),
            ("Mid Score", "721"),
        ]), ("Derogatory", [
            ("30-Day Late", "Chase Visa — December 2024"),
            ("Late Payment Amount", "$85.00"),
            ("Current Status", "Current — Paid as Agreed"),
            ("Derogatory Count", "1"),
        ])]),
        "doc_type": "CREDIT_REPORT", "category": "credit",
        "fields": {"mid_score": 721, "tradeline_count": 10,
                   "total_monthly_payments": 1200, "derogatory_count": 1}
    })

    # Credit explanation for the late
    docs.append({"filename": _make_pdf(out, "CREDIT_EXPLAIN.pdf",
        "Letter of Explanation", "30-Day Late Payment",
        [("", [
            ("Creditor", "Chase Visa"),
            ("Explanation", "Hospitalization — auto-pay failed during medical leave"),
            ("Resolved", "Yes — paid immediately upon discharge"),
        ])]),
        "doc_type": "CREDIT_EXPLANATION", "category": "credit",
        "fields": {"explanation_type": "late_payment", "creditor": "Chase Visa",
                   "reason": "medical_emergency", "resolved": True}
    })

    # SSN MISMATCH — different last4 on DL vs SSN validation (typo)
    docs.append({"filename": _make_pdf(out, "DL.pdf",
        "Driver's License", "",
        [("", [
            ("Name", f"{primary['first']} {primary['last']}"),
            ("SSN shown", "XXX-XX-5678"),
        ])]),
        "doc_type": "DRIVERS_LICENSE", "category": "identity",
        "fields": {"dl_number": "TX-11111111", "name_match": True}
    })

    docs.append({"filename": _make_pdf(out, "SSN_VALID.pdf",
        "SSN Validation", "MISMATCH — see below",
        [("", [
            ("Name", f"{primary['first']} {primary['last']}"),
            ("SSN", "XXX-XX-5687"),
            ("SSN Valid", "Yes"),
            ("NOTE", "Last 4 digits differ from application (5678 vs 5687)"),
        ])]),
        "doc_type": "SSN_VALIDATION", "category": "identity",
        "fields": {"ssn_valid": True, "name_match": True,
                   "ssn_last4_on_doc": "5687"}
    })

    docs.append({"filename": _make_pdf(out, "OFAC.pdf",
        "OFAC Check", "", [("", [("OFAC Clear", "Yes")])]),
        "doc_type": "OFAC_CHECK", "category": "identity",
        "fields": {"ofac_clear": True}
    })

    # Bank statements (primary — low balance, co — main funds)
    docs.append({"filename": _make_pdf(out, "BANK_M1_PRIMARY.pdf",
        "Bank Statement — Primary", "Very low balance",
        [("", [("Ending Balance", "$1,200.00"), ("Avg Balance", "$950.00")])]),
        "doc_type": "BANK_STATEMENT_M1", "category": "asset",
        "fields": {"ending_balance": 1200, "avg_balance": 950}
    })

    docs.append({"filename": _make_pdf(out, "BANK_M1_CO.pdf",
        "Bank Statement — Co-Borrower", "",
        [("", [("Ending Balance", "$67,000.00"), ("Avg Balance", "$62,000.00")])]),
        "doc_type": "BANK_STATEMENT_M1", "category": "asset", "role": "co_borrower",
        "fields": {"ending_balance": 67000, "avg_balance": 62000}
    })

    # Property docs
    docs.append({"filename": _make_pdf(out, "APPRAISAL.pdf",
        "Appraisal", "",
        [("", [("Appraised Value", "$520,000"), ("Property Type", "Condo")])]),
        "doc_type": "APPRAISAL_URAR", "category": "property",
        "fields": {"appraised_value": 520000, "property_type": "Condo"}
    })

    docs.append({"filename": _make_pdf(out, "PURCHASE_AGT.pdf",
        "Purchase Agreement", "",
        [("", [("Purchase Price", "$515,000")])]),
        "doc_type": "PURCHASE_AGREEMENT", "category": "loan_terms",
        "fields": {"purchase_price": 515000}
    })

    docs.append({"filename": _make_pdf(out, "URLA.pdf",
        "URLA 1003", "",
        [("", [("Loan Amount", "$412,000"), ("Rate", "6.75%"),
               ("Term", "360"), ("Occupancy", "Primary")])]),
        "doc_type": "URLA_1003", "category": "loan_terms",
        "fields": {"loan_amount": 412000, "interest_rate": 6.75,
                   "loan_term_months": 360, "occupancy": "primary_residence"}
    })

    _write_manifest(out, docs, los_id, primary, name, co_borrower=co)
    return name, len(docs)


# =====================================================================
# SCENARIO 3: Property Disaster
# =====================================================================
def gen_scenario_3():
    name = "scenario_3_property_disaster"
    out = os.path.join(BASE_OUT, name)
    os.makedirs(out, exist_ok=True)
    docs = []
    borrower = {"first": "David", "last": "Chen", "ssn_last4": "3456"}
    los_id = "LOS-CHAOS-PD03"

    # Standard income (clean — the chaos is in property)
    docs.append({"filename": _make_pdf(out, "W2_2025.pdf",
        "W-2 — 2025", "",
        [("", [("Box 1", "$95,000.00"), ("Employer", "State Farm Insurance"),
               ("Tax Year", "2025")])]),
        "doc_type": "W2_CURRENT", "category": "income",
        "fields": {"box1_wages": 95000, "employer_name": "State Farm Insurance",
                   "tax_year": "2025"}
    })

    docs.append({"filename": _make_pdf(out, "CREDIT.pdf",
        "Credit Report", "",
        [("", [("Mid Score", "765"), ("Monthly Payments", "$1,100")])]),
        "doc_type": "CREDIT_REPORT", "category": "credit",
        "fields": {"mid_score": 765, "total_monthly_payments": 1100}
    })

    # APPRAISAL BELOW PURCHASE PRICE — short appraisal
    docs.append({"filename": _make_pdf(out, "APPRAISAL.pdf",
        "Appraisal — URAR", "*** VALUE BELOW CONTRACT PRICE ***",
        [("Valuation", [
            ("Appraised Value", "$340,000.00"),
            ("Property Type", "SFR"),
            ("Condition", "C4 — Fair"),
            ("Year Built", "1972"),
            ("GLA", "1,800 sq ft"),
            ("NOTE", "Deferred maintenance — roof replacement needed"),
        ])]),
        "doc_type": "APPRAISAL_URAR", "category": "property",
        "fields": {"appraised_value": 340000, "property_type": "SFR",
                   "condition": "C4", "year_built": 1972, "gla_sqft": 1800}
    })

    docs.append({"filename": _make_pdf(out, "PURCHASE_AGT.pdf",
        "Purchase Agreement", "Contract price ABOVE appraisal",
        [("", [
            ("Purchase Price", "$365,000.00"),
            ("Closing Date", "2026-08-01"),
        ])]),
        "doc_type": "PURCHASE_AGREEMENT", "category": "loan_terms",
        "fields": {"purchase_price": 365000, "closing_date": "2026-08-01"}
    })

    # AVM 25% BELOW appraisal
    docs.append({"filename": _make_pdf(out, "AVM.pdf",
        "AVM Report", "*** SIGNIFICANT DEVIATION ***",
        [("", [
            ("AVM Value", "$255,000.00"),
            ("Confidence", "0.62"),
            ("Note", "High uncertainty — limited comps in area"),
        ])]),
        "doc_type": "AVM_REPORT", "category": "property",
        "fields": {"avm_value": 255000, "confidence_score": 0.62}
    })

    # Flood zone MISMATCH
    docs.append({"filename": _make_pdf(out, "FLOOD_CERT.pdf",
        "Flood Determination", "Zone X — Minimal Risk",
        [("", [("Flood Zone", "X"), ("Requires Insurance", "No")])]),
        "doc_type": "FLOOD_CERT", "category": "property",
        "fields": {"flood_zone": "X", "requires_insurance": False}
    })

    docs.append({"filename": _make_pdf(out, "FLOOD_INSURANCE.pdf",
        "Flood Insurance Policy", "*** ZONE AE — HIGH RISK ***",
        [("", [
            ("Flood Zone on Policy", "AE"),
            ("Annual Premium", "$2,800.00"),
            ("NOTE", "Zone changed after FEMA remap — cert is outdated"),
        ])]),
        "doc_type": "FLOOD_CERT", "category": "property",
        "fields": {"flood_zone": "AE", "requires_insurance": True,
                   "annual_premium": 2800}
    })

    # HOA with LITIGATION
    docs.append({"filename": _make_pdf(out, "HOA.pdf",
        "HOA Certification", "*** LITIGATION PENDING ***",
        [("", [
            ("Monthly Dues", "$450.00"),
            ("Special Assessments", "$3,500.00 (roof repair levy)"),
            ("Reserve Balance", "$12,000.00 (below recommended)"),
            ("Litigation Pending", "YES — construction defect suit"),
            ("Litigation Amount", "$2.1M"),
        ])]),
        "doc_type": "HOA_CERT", "category": "property",
        "fields": {"monthly_dues": 450, "special_assessments": 3500,
                   "reserve_balance": 12000, "litigation_pending": True}
    })

    # Title with 6 exceptions
    docs.append({"filename": _make_pdf(out, "TITLE.pdf",
        "Title Commitment", "6 Schedule B Exceptions",
        [("", [
            ("Exceptions", "6"),
            ("Exception 1", "Standard printed exceptions"),
            ("Exception 2", "Utility easement — 15 ft rear"),
            ("Exception 3", "Property tax lien — $3,200 past due"),
            ("Exception 4", "Mechanics lien — $12,500 (contractor dispute)"),
            ("Exception 5", "Judgment lien — $8,000 (resolved, release pending)"),
            ("Exception 6", "Encroachment — neighbor fence 2 ft over line"),
        ])]),
        "doc_type": "TITLE_COMMITMENT", "category": "property",
        "fields": {"exceptions_count": 6, "effective_date": "2026-06-15",
                   "has_liens": True}
    })

    # ACTIVE TERMITES
    docs.append({"filename": _make_pdf(out, "PEST.pdf",
        "WDO Inspection", "*** ACTIVE INFESTATION FOUND ***",
        [("", [
            ("Findings", "Active subterranean termite infestation"),
            ("Location", "Foundation — south wall, garage header"),
            ("Treatment Required", "YES — full treatment estimated $2,500"),
            ("Structural Damage", "Moderate — sistering of joists recommended"),
        ])]),
        "doc_type": "WDO_REPORT", "category": "property",
        "fields": {"findings": "active_infestation", "treatment_required": True,
                   "structural_damage": True}
    })

    # Rural — well and septic
    docs.append({"filename": _make_pdf(out, "WELL_SEPTIC.pdf",
        "Well & Septic Inspection", "Rural Property",
        [("Well", [
            ("Flow Rate", "3.2 GPM"),
            ("Water Quality", "Elevated iron — treatment recommended"),
        ]), ("Septic", [
            ("System Type", "Conventional"),
            ("Condition", "Fair — drain field showing stress"),
            ("Estimated Remaining Life", "5-8 years"),
            ("Replacement Cost Estimate", "$15,000-$22,000"),
        ])]),
        "doc_type": "WELL_SEPTIC_INSPECTION", "category": "property",
        "fields": {"well_flow_rate_gpm": 3.2, "septic_condition": "fair",
                   "rural_property": True, "water_quality_issues": True}
    })

    # Identity + loan terms
    docs.append({"filename": _make_pdf(out, "DL.pdf", "Driver's License", "",
        [("", [("Name", "David Chen")])]),
        "doc_type": "DRIVERS_LICENSE", "category": "identity",
        "fields": {"name_match": True}
    })

    docs.append({"filename": _make_pdf(out, "URLA.pdf", "URLA 1003", "",
        [("", [("Loan Amount", "$292,000"), ("Rate", "7.125%"), ("Term", "360")])]),
        "doc_type": "URLA_1003", "category": "loan_terms",
        "fields": {"loan_amount": 292000, "interest_rate": 7.125,
                   "loan_term_months": 360}
    })

    _write_manifest(out, docs, los_id, borrower, name)
    return name, len(docs)


# =====================================================================
# SCENARIO 4: Data Quality Catastrophe
# =====================================================================
def gen_scenario_4():
    name = "scenario_4_data_quality"
    out = os.path.join(BASE_OUT, name)
    os.makedirs(out, exist_ok=True)
    docs = []
    borrower = {"first": "José", "last": "García-López", "ssn_last4": "7890"}
    los_id = "LOS-CHAOS-DQ04"

    # W2 with None values and string wages
    docs.append({"filename": _make_pdf(out, "W2_MESSY.pdf",
        "W-2 — 2025", "Malformed fields",
        [("", [("Box 1", "one hundred ten thousand"), ("Employer", ""),
               ("Tax Year", "2025")])]),
        "doc_type": "W2_CURRENT", "category": "income",
        "fields": {"box1_wages": "one hundred ten thousand",
                   "tax_year": "2025", "employer_name": None,
                   "employer_ein": None}
    })

    # Bank statement with NEGATIVE balance
    docs.append({"filename": _make_pdf(out, "BANK_NEGATIVE.pdf",
        "Bank Statement", "OVERDRAWN",
        [("", [("Ending Balance", "-$2,340.00"),
               ("Average Balance", "-$890.00")])]),
        "doc_type": "BANK_STATEMENT_M1", "category": "asset",
        "fields": {"ending_balance": -2340, "avg_balance": -890}
    })

    # Date format chaos
    docs.append({"filename": _make_pdf(out, "PAYSTUB_DATES.pdf",
        "Paystub", "Mixed date formats",
        [("", [
            ("YTD Gross", "$55,000"),
            ("Pay Period End", "06/15/2026"),
            ("Hire Date", "March 3rd, 2019"),
        ])]),
        "doc_type": "PAYSTUB_CURRENT", "category": "income",
        "fields": {"ytd_gross": 55000,
                   "pay_period_end": "06/15/2026",
                   "hire_date": "March 3rd, 2019"}
    })

    # Empty PDF (0 bytes)
    docs.append({"filename": _empty_pdf(out, "EMPTY.pdf"),
        "doc_type": "APPRAISAL_URAR", "category": "property",
        "fields": {}
    })

    # Garbage PDF
    docs.append({"filename": _garbage_pdf(out, "GARBAGE.pdf"),
        "doc_type": "CREDIT_REPORT", "category": "credit",
        "fields": {"mid_score": None, "tradeline_count": "unknown"}
    })

    # Duplicate document IDs — same doc uploaded 3 times with different values
    for i, wages in enumerate([95000, 98000, 102000]):
        docs.append({"filename": _make_pdf(out, f"W2_DUPLICATE_{i}.pdf",
            f"W-2 — Version {i+1}", f"DUPLICATE — wages changed to ${wages:,}",
            [("", [("Box 1", f"${wages:,.2f}"), ("Tax Year", "2025")])]),
            "doc_type": "W2_CURRENT", "category": "income",
            "fields": {"box1_wages": wages, "tax_year": "2025",
                       "employer_name": "Stable Corp"},
            "force_doc_id": f"DOC-{los_id}-W2-DUPE"  # SAME doc_id
        })

    # Unicode in names
    docs.append({"filename": _make_pdf(out, "DL_UNICODE.pdf",
        "Driver's License", "Unicode characters",
        [("", [
            ("Name", "José García-López"),
            ("Address", "123 Señor Ave, San José, CA"),
        ])]),
        "doc_type": "DRIVERS_LICENSE", "category": "identity",
        "fields": {"dl_number": "CA-ÜN1C0DE", "name_match": True}
    })

    # Extremely long employer name
    long_name = "The Very Long Named Corporation of America and International Partners " * 3
    docs.append({"filename": _make_pdf(out, "VOE_LONG.pdf",
        "VOE", "Extremely long employer name",
        [("", [
            ("Employer", long_name[:200]),
            ("Status", "Active"),
            ("Income", "$110,000"),
        ])]),
        "doc_type": "VOE_TWN", "category": "employment",
        "fields": {"employer_name": long_name, "employment_status": "Active",
                   "income_amount": 110000}
    })

    # Property + loan terms (minimal, to let the chaos above be the focus)
    docs.append({"filename": _make_pdf(out, "PURCHASE.pdf",
        "Purchase Agreement", "",
        [("", [("Purchase Price", "$425,000")])]),
        "doc_type": "PURCHASE_AGREEMENT", "category": "loan_terms",
        "fields": {"purchase_price": 425000}
    })

    docs.append({"filename": _make_pdf(out, "URLA.pdf",
        "URLA 1003", "",
        [("", [("Loan Amount", "$340,000"), ("Rate", "6.5%"), ("Term", "360")])]),
        "doc_type": "URLA_1003", "category": "loan_terms",
        "fields": {"loan_amount": 340000, "interest_rate": 6.5,
                   "loan_term_months": 360}
    })

    _write_manifest(out, docs, los_id, borrower, name)
    return name, len(docs)


# =====================================================================
# SCENARIO 5: Late-Arriving Everything
# =====================================================================
def gen_scenario_5():
    name = "scenario_5_stale_expired"
    out = os.path.join(BASE_OUT, name)
    os.makedirs(out, exist_ok=True)
    docs = []
    borrower = {"first": "Amanda", "last": "Wilson", "ssn_last4": "2345"}
    los_id = "LOS-CHAOS-LE05"

    # W2 for WRONG YEAR
    docs.append({"filename": _make_pdf(out, "W2_WRONG_YEAR.pdf",
        "W-2 — 2023", "*** WRONG TAX YEAR — should be 2025 ***",
        [("", [("Box 1", "$88,000.00"), ("Tax Year", "2023"),
               ("Employer", "Previous Corp")])]),
        "doc_type": "W2_CURRENT", "category": "income",
        "fields": {"box1_wages": 88000, "tax_year": "2023",
                   "employer_name": "Previous Corp"}
    })

    # Stale paystub (90 days old)
    docs.append({"filename": _make_pdf(out, "PAYSTUB_STALE.pdf",
        "Earnings Statement", "*** 90 DAYS OLD ***",
        [("", [
            ("YTD Gross", "$22,000.00"),
            ("Pay Period End", "2026-03-15"),
            ("Note", "Paystub is 90+ days old — may need refreshed"),
        ])]),
        "doc_type": "PAYSTUB_CURRENT", "category": "income",
        "fields": {"ytd_gross": 22000, "pay_period_end": "2026-03-15"}
    })

    # Expired rate lock
    docs.append({"filename": _make_pdf(out, "RATE_LOCK_EXPIRED.pdf",
        "Rate Lock", "*** EXPIRED ***",
        [("", [
            ("Locked Rate", "5.875%"),
            ("Lock Date", "2026-01-15"),
            ("Lock Expiry", "2026-03-01"),
            ("STATUS", "EXPIRED — lock period ended"),
        ])]),
        "doc_type": "RATE_LOCK", "category": "loan_terms",
        "fields": {"locked_rate": 5.875, "lock_expiry": "2026-03-01",
                   "lock_days": 45, "status": "expired"}
    })

    # Old credit report (180 days)
    docs.append({"filename": _make_pdf(out, "CREDIT_OLD.pdf",
        "Credit Report", "*** PULLED 180 DAYS AGO ***",
        [("", [
            ("Mid Score", "735"),
            ("Report Date", "2025-12-01"),
            ("Note", "Report is 180+ days old — requires refresh per AUS"),
        ])]),
        "doc_type": "CREDIT_REPORT", "category": "credit",
        "fields": {"mid_score": 735, "total_monthly_payments": 1500,
                   "report_date": "2025-12-01"}
    })

    # VOE shows TERMINATED
    docs.append({"filename": _make_pdf(out, "VOE_TERMINATED.pdf",
        "Verification of Employment", "*** TERMINATED ***",
        [("", [
            ("Employee", "Amanda Wilson"),
            ("Employer", "TechStartup Inc"),
            ("Employment Status", "TERMINATED"),
            ("Termination Date", "2026-04-30"),
            ("Reason", "Company layoff — position eliminated"),
        ])]),
        "doc_type": "VOE_TWN", "category": "employment",
        "fields": {"employer_name": "TechStartup Inc",
                   "employment_status": "Terminated",
                   "termination_date": "2026-04-30",
                   "income_amount": 88000}
    })

    # AUS: Refer with Caution
    docs.append({"filename": _make_pdf(out, "AUS_REFER.pdf",
        "Desktop Underwriter", "*** REFER WITH CAUTION ***",
        [("", [
            ("Recommendation", "Refer with Caution"),
            ("Risk Class", "Caution"),
            ("Conditions", "Employment gap; stale credit; DTI exceeds guidelines"),
        ])]),
        "doc_type": "AUS_DU_FINDINGS", "category": "vendor",
        "fields": {"recommendation": "Refer with Caution",
                   "risk_class": "Caution"}
    })

    # TWO appraisals — second contradicts first by 15%
    docs.append({"filename": _make_pdf(out, "APPRAISAL_1.pdf",
        "Appraisal — Original", "First opinion of value",
        [("", [("Appraised Value", "$410,000"), ("Effective Date", "2026-03-01")])]),
        "doc_type": "APPRAISAL_URAR", "category": "property",
        "fields": {"appraised_value": 410000, "effective_date": "2026-03-01"}
    })

    docs.append({"filename": _make_pdf(out, "APPRAISAL_2.pdf",
        "Appraisal — SECOND OPINION", "*** 15% LOWER ***",
        [("", [
            ("Appraised Value", "$348,500.00"),
            ("Effective Date", "2026-06-01"),
            ("Note", "Second appraisal ordered after review — 15% below original"),
        ])]),
        "doc_type": "APPRAISAL_URAR", "category": "property",
        "fields": {"appraised_value": 348500, "effective_date": "2026-06-01"}
    })

    # Old appraisal (>120 days)
    docs.append({"filename": _make_pdf(out, "APPRAISAL_OLD.pdf",
        "Appraisal Update", "*** ORIGINAL > 120 DAYS OLD ***",
        [("", [
            ("Original Date", "2026-01-10"),
            ("Update Date", "2026-06-15"),
            ("Days Since Original", "156"),
            ("Updated Value", "$405,000"),
        ])]),
        "doc_type": "APPRAISAL_UPDATE", "category": "property",
        "fields": {"original_value": 410000, "updated_value": 405000,
                   "update_date": "2026-06-15", "days_since_original": 156}
    })

    docs.append({"filename": _make_pdf(out, "PURCHASE.pdf",
        "Purchase Agreement", "",
        [("", [("Purchase Price", "$400,000")])]),
        "doc_type": "PURCHASE_AGREEMENT", "category": "loan_terms",
        "fields": {"purchase_price": 400000}
    })

    # Minimal identity + loan terms
    docs.append({"filename": _make_pdf(out, "DL.pdf", "Driver's License", "",
        [("", [("Name", "Amanda Wilson")])]),
        "doc_type": "DRIVERS_LICENSE", "category": "identity",
        "fields": {"name_match": True}
    })

    docs.append({"filename": _make_pdf(out, "URLA.pdf", "URLA 1003", "",
        [("", [("Loan Amount", "$320,000"), ("Rate", "6.875%"), ("Term", "360")])]),
        "doc_type": "URLA_1003", "category": "loan_terms",
        "fields": {"loan_amount": 320000, "interest_rate": 6.875,
                   "loan_term_months": 360}
    })

    docs.append({"filename": _make_pdf(out, "BANK_M1.pdf",
        "Bank Statement", "",
        [("", [("Ending Balance", "$35,000"), ("Avg Balance", "$32,000")])]),
        "doc_type": "BANK_STATEMENT_M1", "category": "asset",
        "fields": {"ending_balance": 35000, "avg_balance": 32000}
    })

    _write_manifest(out, docs, los_id, borrower, name)
    return name, len(docs)


# =====================================================================
# Manifest + Runner
# =====================================================================
def _write_manifest(out_dir, docs, los_id, borrower, scenario_name,
                    co_borrower=None):
    manifest = {
        "scenario": scenario_name,
        "los_id": los_id,
        "borrower": borrower,
        "co_borrower": co_borrower,
        "loan": {},
        "documents": [],
    }
    for d in docs:
        entry = {
            "filename": d["filename"],
            "doc_type": d["doc_type"],
            "category": d["category"],
            "role": d.get("role", "primary"),
            "fields": d.get("fields", {}),
        }
        if "force_doc_id" in d:
            entry["force_doc_id"] = d["force_doc_id"]
        manifest["documents"].append(entry)

    path = os.path.join(out_dir, "manifest.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)


def main():
    print("=" * 60)
    print("CHAOS LOAN FILE GENERATOR")
    print("Generating 5 messy real-world loan scenarios")
    print("=" * 60)

    os.makedirs(BASE_OUT, exist_ok=True)

    scenarios = [
        gen_scenario_1,
        gen_scenario_2,
        gen_scenario_3,
        gen_scenario_4,
        gen_scenario_5,
    ]

    total_docs = 0
    for gen_fn in scenarios:
        name, count = gen_fn()
        total_docs += count
        print(f"\n  [{name}] {count} documents generated")

    print(f"\n{'='*60}")
    print(f"TOTAL: {total_docs} documents across 5 scenarios")
    print(f"Output: {BASE_OUT}/")
    print(f"\nScenarios:")
    print(f"  1. Self-employed: negative income, amended returns, unexplained deposits")
    print(f"  2. Co-borrower:   primary no income, employer gift, SSN mismatch")
    print(f"  3. Property:      short appraisal, AVM deviation, termites, litigation")
    print(f"  4. Data quality:  nulls, strings as numbers, empty PDF, duplicates, unicode")
    print(f"  5. Stale/expired: wrong year W2, expired lock, terminated VOE, dual appraisals")


if __name__ == "__main__":
    main()
