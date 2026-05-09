"""Format-variant PDF generators for the realworld simulator.

Real institutions emit wildly different PDF layouts for the same doc
type — an ADP W-2 looks nothing like a Paychex W-2 even though both
carry the same nine boxes. Our deterministic pymupdf extractors parse
text patterns, so format variation is what proves they generalise, and
it's what forces the AI-Vision fallback (``extract_with_claude``) into
action when patterns don't match.

This module defines one renderer per (doc_type, format) plus a
dispatcher ``make_pdf(doc_type, fields, los_id, role)`` that picks the
right format for a given loan + borrower role. Field values rendered
into the PDF come from the same ``fields`` dict the meta.json record
uses, so the **ground-truth invariant** holds: what the PDF says, the
meta.json says — and an extraction-accuracy test can compare directly.

Multi-page docs intentionally place key fields on different pages
across formats (e.g. BOA bank statement puts ``ending_balance`` on the
last transaction page, Chase puts it on the cover-summary page) so the
AI-Vision fallback's ``AI_EXTRACTION_MAX_PAGES=3`` window is exercised
both ways.

Shared-drive scans simulate scanner artifacts: a slight rotation, a
landscape page, two docs jammed onto one scan, faded photocopy text.
"""
from __future__ import annotations

import io
import math
import random
from datetime import date, timedelta
from typing import Any

from reportlab.lib import colors
from reportlab.lib.pagesizes import landscape, letter
from reportlab.pdfgen import canvas


# ===========================================================================
# Per-loan format mapping
# ===========================================================================
#
# W-2 + paystub formats are pinned per loan so re-runs stay deterministic
# (LOAN-101 always gets ADP, LOAN-103 always Gusto/ADP for primary/co etc.)
# This way an extraction-accuracy regression can be tracked against a known
# layout, not a roll of the dice.

W2_FORMAT_BY_LOAN: dict = {
    "LOAN-101": ("ADP",     "ADP"),
    "LOAN-102": ("Paychex", "Paychex"),
    "LOAN-103": ("Gusto",   "ADP"),
    "LOAN-104": ("ADP",     "ADP"),
    "LOAN-105": ("Paychex", "Paychex"),
    "LOAN-106": ("Gusto",   "Gusto"),
    "LOAN-107": ("ADP",     "Paychex"),
    "LOAN-108": ("Gusto",   "Gusto"),
    "LOAN-109": ("Paychex", "Paychex"),
    "LOAN-110": ("ADP",     "Gusto"),
}

PAYSTUB_FORMAT_BY_LOAN: dict = {
    "LOAN-101": ("ADP",     "ADP"),
    "LOAN-102": ("Paychex", "Paychex"),
    "LOAN-103": ("Workday", "ADP"),
    "LOAN-104": ("ADP",     "ADP"),
    "LOAN-105": ("Paychex", "Paychex"),
    "LOAN-106": ("Workday", "Workday"),
    "LOAN-107": ("ADP",     "Paychex"),
    "LOAN-108": ("Workday", "Workday"),
    "LOAN-109": ("Paychex", "Paychex"),
    "LOAN-110": ("ADP",     "Workday"),
}


def _bank_format(los_id: str, fields: dict) -> str:
    """Bank-statement format keyed off the institution name (when known),
    falling back to a stable round-robin by loan index so every bank stmt
    still picks one of the three defined formats."""
    bank = (fields.get("bank_name") or "").lower()
    if "chase" in bank:
        return "Chase"
    if "wells" in bank:
        return "Wells"
    if "bank of america" in bank or "boa" in bank:
        return "BOA"
    idx = int(los_id.rsplit("-", 1)[-1])
    return ("Chase", "Wells", "BOA")[idx % 3]


def _rotated_format(los_id: str, choices: tuple) -> str:
    """Stable rotation: format chosen by ``int(los_id.suffix) % len(choices)``."""
    idx = int(los_id.rsplit("-", 1)[-1])
    return choices[idx % len(choices)]


CREDIT_FORMATS    = ("Equifax",        "Experian")
APPRAISAL_FORMATS = ("URAR",           "Narrative")
TITLE_FORMATS     = ("FirstAmerican",  "Chicago")


# ===========================================================================
# Canvas + rendering helpers
# ===========================================================================


PAGE_W, PAGE_H = letter   # 612 × 792


def _new_canvas(pagesize=letter) -> tuple[io.BytesIO, canvas.Canvas]:
    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=pagesize)
    return buf, c


def _save(c: canvas.Canvas, buf: io.BytesIO) -> bytes:
    c.save()
    return buf.getvalue()


def _money(v: Any) -> str:
    try:
        return f"${float(v):,.2f}"
    except (TypeError, ValueError):
        return str(v) if v is not None else ""


def _short_money(v: Any) -> str:
    """``$62,000`` (no cents) — used in narrative-style appraisals
    where decimals would clutter the prose."""
    try:
        return f"${float(v):,.0f}"
    except (TypeError, ValueError):
        return str(v) if v is not None else ""


def _band(c, x: float, y: float, w: float, h: float, color):
    c.setFillColor(color)
    c.rect(x, y, w, h, fill=1, stroke=0)
    c.setFillColor(colors.black)


def _ssn_masked(fields: dict) -> str:
    last4 = fields.get("ssn_last4") or fields.get("ssn") or "0000"
    return f"***-**-{last4}"


def _wrap(text: str, width: int) -> list[str]:
    """Break a string into <=``width``-char lines on word boundaries."""
    out, line = [], ""
    for word in str(text).split():
        if len(line) + len(word) + 1 <= width:
            line = (line + " " + word).strip()
        else:
            if line:
                out.append(line)
            line = word
    if line:
        out.append(line)
    return out


# ===========================================================================
# W-2 — three formats
# ===========================================================================


def gen_w2_adp(fields: dict) -> bytes:
    """ADP-style: red header strip, employer top-left, six labelled
    boxes in a 2-column grid. Helvetica throughout."""
    buf, c = _new_canvas()
    _band(c, 0, 740, PAGE_W, 32, colors.HexColor("#D71921"))
    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 14)
    c.drawString(36, 750, "ADP — Form W-2 Wage and Tax Statement")

    c.setFillColor(colors.black)
    c.setFont("Helvetica", 9)
    c.drawString(36, 720, "Employer:")
    c.setFont("Helvetica-Bold", 11)
    c.drawString(100, 720, str(fields.get("employer_name", "")))
    c.setFont("Helvetica", 9)
    c.drawString(36, 706, f"EIN: {fields.get('employer_ein', '')}")
    c.drawString(36, 692, f"Tax Year: {fields.get('tax_year', '')}")

    c.drawString(360, 720, f"Employee: {fields.get('employee_name', '')}")
    c.drawString(360, 706, f"SSN: {_ssn_masked(fields)}")

    ss_wages = fields.get("box3_ss_wages") or fields.get("box1_wages") or 0
    boxes = [
        ("1. Wages, tips, other compensation", fields.get("box1_wages", 0)),
        ("2. Federal income tax withheld",     fields.get("box2_fed_tax", 0)),
        ("3. Social security wages",            ss_wages),
        ("4. Social security tax withheld",     round(float(ss_wages) * 0.062)),
        ("5. Medicare wages and tips",          ss_wages),
        ("6. Medicare tax withheld",            round(float(ss_wages) * 0.0145)),
    ]
    base_y = 640
    for i, (label, val) in enumerate(boxes):
        col, row = i % 2, i // 2
        x = 36 + col * 270
        y = base_y - row * 70
        c.rect(x, y - 50, 250, 60, stroke=1, fill=0)
        c.setFont("Helvetica", 8)
        c.drawString(x + 6, y, label)
        c.setFont("Helvetica-Bold", 12)
        c.drawString(x + 6, y - 30, _money(val))

    c.setFont("Helvetica-Oblique", 7)
    c.drawString(36, 80, "Generated by ADP Inc. payroll services.")
    c.showPage()
    return _save(c, buf)


def gen_w2_paychex(fields: dict) -> bytes:
    """Paychex-style: horizontal bands across the whole page, employer at
    bottom, Times-Roman, no boxes — labels left-aligned, values
    right-aligned in same row."""
    buf, c = _new_canvas()
    c.setFont("Times-Bold", 16)
    c.drawString(36, 740, "PAYCHEX — W-2 Wage & Tax Statement")
    c.setFont("Times-Roman", 10)
    c.drawString(36, 720, f"Tax Year {fields.get('tax_year', '')}")
    c.drawString(420, 720, f"Employee SSN {_ssn_masked(fields)}")

    bands = [
        ("Wages, tips, other compensation",   fields.get("box1_wages", 0)),
        ("Federal income tax withheld",        fields.get("box2_fed_tax", 0)),
        ("Social security wages",              fields.get("box3_ss_wages",
                                                          fields.get("box1_wages", 0))),
        ("Social security tax withheld",       round(float(fields.get("box3_ss_wages",
                                                          fields.get("box1_wages", 0)) or 0) * 0.062)),
        ("Medicare wages and tips",            fields.get("box3_ss_wages",
                                                          fields.get("box1_wages", 0))),
        ("Medicare tax withheld",              round(float(fields.get("box3_ss_wages",
                                                          fields.get("box1_wages", 0)) or 0) * 0.0145)),
        ("State wages",                        fields.get("box1_wages", 0)),
        ("State income tax",                   round(float(fields.get("box1_wages", 0) or 0) * 0.04)),
    ]
    y = 680
    for i, (label, val) in enumerate(bands):
        if i % 2 == 0:
            _band(c, 36, y - 4, 540, 18, colors.HexColor("#EEF2F8"))
        c.setFont("Times-Roman", 11)
        c.drawString(48, y, label)
        c.setFont("Times-Bold", 11)
        c.drawRightString(560, y, _money(val))
        y -= 28

    # Employee + employer at bottom
    c.setFont("Times-Bold", 10)
    c.drawString(36, 200, "Employee")
    c.setFont("Times-Roman", 10)
    c.drawString(36, 184, str(fields.get("employee_name", "")))
    c.drawString(36, 168, f"SSN {_ssn_masked(fields)}")

    c.setFont("Times-Bold", 10)
    c.drawString(330, 200, "Employer")
    c.setFont("Times-Roman", 10)
    c.drawString(330, 184, str(fields.get("employer_name", "")))
    c.drawString(330, 168, f"EIN {fields.get('employer_ein', '')}")
    c.setFont("Times-Italic", 7)
    c.drawString(36, 60, "Issued by Paychex, Inc. — Rochester, NY")
    c.showPage()
    return _save(c, buf)


def gen_w2_gusto(fields: dict) -> bytes:
    """Gusto-style: modern, employer centered at top in a thin rule, boxes
    in single-column rows. Helvetica-Bold for labels, light gray rules."""
    buf, c = _new_canvas()
    c.setFont("Helvetica-Bold", 18)
    c.drawCentredString(PAGE_W / 2, 750, "Gusto Payroll · W-2")
    c.setFont("Helvetica", 11)
    c.drawCentredString(PAGE_W / 2, 730, str(fields.get("employer_name", "")))
    c.setFont("Helvetica-Oblique", 9)
    c.drawCentredString(PAGE_W / 2, 716,
                        f"EIN {fields.get('employer_ein', '')}  ·  Tax Year {fields.get('tax_year', '')}")

    # Light grey rule under header
    c.setStrokeColor(colors.HexColor("#CCCCCC"))
    c.line(72, 706, PAGE_W - 72, 706)
    c.setStrokeColor(colors.black)

    # Employee identity
    c.setFont("Helvetica", 10)
    c.drawString(72, 686, "Employee")
    c.setFont("Helvetica-Bold", 12)
    c.drawString(72, 670, str(fields.get("employee_name", "")))
    c.setFont("Helvetica", 9)
    c.drawString(72, 654, f"SSN ending {fields.get('ssn_last4', '')}")

    rows = [
        ("Box 1 — Wages, tips, other compensation", fields.get("box1_wages", 0)),
        ("Box 2 — Federal income tax withheld",     fields.get("box2_fed_tax", 0)),
        ("Box 3 — Social security wages",           fields.get("box3_ss_wages",
                                                                fields.get("box1_wages", 0))),
        ("Box 5 — Medicare wages and tips",         fields.get("box3_ss_wages",
                                                                fields.get("box1_wages", 0))),
    ]
    y = 600
    for label, val in rows:
        c.setFont("Helvetica", 10)
        c.setFillColor(colors.HexColor("#666666"))
        c.drawString(72, y, label)
        c.setFillColor(colors.black)
        c.setFont("Helvetica-Bold", 14)
        c.drawString(72, y - 18, _money(val))
        c.setStrokeColor(colors.HexColor("#EEEEEE"))
        c.line(72, y - 32, PAGE_W - 72, y - 32)
        y -= 56
    c.setStrokeColor(colors.black)

    c.setFont("Helvetica-Oblique", 7)
    c.drawCentredString(PAGE_W / 2, 60, "Generated by Gusto · gusto.com")
    c.showPage()
    return _save(c, buf)


_W2_GENS = {"ADP": gen_w2_adp, "Paychex": gen_w2_paychex, "Gusto": gen_w2_gusto}


# ===========================================================================
# Paystub — three formats
# ===========================================================================


def gen_paystub_adp(fields: dict) -> bytes:
    """Earnings + deductions tables side-by-side. Red ADP header."""
    buf, c = _new_canvas()
    _band(c, 0, 740, PAGE_W, 32, colors.HexColor("#D71921"))
    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 14)
    c.drawString(36, 750, "ADP — Earnings Statement")
    c.setFillColor(colors.black)
    c.setFont("Helvetica", 9)
    c.drawString(36, 720, f"Employer: {fields.get('employer_name', '')}")
    c.drawString(36, 706, f"Employee: {fields.get('employee_name', '')}")
    c.drawString(36, 692,
                 f"Pay period ending: {fields.get('pay_period_end', '')}")

    gross = float(fields.get("gross_pay", 0) or 0)
    ytd   = float(fields.get("ytd_gross", 0) or 0)
    net   = float(fields.get("net_pay", 0) or 0)
    ded   = gross - net

    # Earnings table left
    c.setFont("Helvetica-Bold", 11)
    c.drawString(36, 660, "Earnings")
    c.setFont("Helvetica", 9)
    c.line(36, 656, 280, 656)
    c.drawString(36, 640, "Regular")
    c.drawRightString(270, 640, _money(gross))
    c.drawString(36, 624, "YTD Gross")
    c.drawRightString(270, 624, _money(ytd))

    # Deductions right
    c.setFont("Helvetica-Bold", 11)
    c.drawString(330, 660, "Deductions")
    c.setFont("Helvetica", 9)
    c.line(330, 656, 576, 656)
    c.drawString(330, 640, "Federal tax")
    c.drawRightString(566, 640, _money(round(ded * 0.55)))
    c.drawString(330, 624, "FICA + Medicare")
    c.drawRightString(566, 624, _money(round(ded * 0.30)))
    c.drawString(330, 608, "State tax")
    c.drawRightString(566, 608, _money(round(ded * 0.15)))

    c.setFont("Helvetica-Bold", 12)
    c.drawString(36, 560, f"Net Pay  {_money(net)}")
    c.showPage()
    return _save(c, buf)


def gen_paystub_paychex(fields: dict) -> bytes:
    """Earnings stacked top, deductions bottom, YTD column right."""
    buf, c = _new_canvas()
    c.setFont("Times-Bold", 16)
    c.drawString(36, 740, "PAYCHEX Earnings Statement")
    c.setFont("Times-Roman", 10)
    c.drawString(36, 720, str(fields.get("employer_name", "")))
    c.drawString(420, 720, f"Period ending {fields.get('pay_period_end', '')}")

    gross = float(fields.get("gross_pay", 0) or 0)
    ytd   = float(fields.get("ytd_gross", 0) or 0)
    net   = float(fields.get("net_pay", 0) or 0)

    # Earnings band
    _band(c, 36, 686, 540, 18, colors.HexColor("#1F4E79"))
    c.setFillColor(colors.white)
    c.setFont("Times-Bold", 11)
    c.drawString(48, 692, "EARNINGS")
    c.drawString(420, 692, "YTD")
    c.setFillColor(colors.black)

    c.setFont("Times-Roman", 11)
    c.drawString(48, 666, "Regular")
    c.drawRightString(380, 666, _money(gross))
    c.drawRightString(560, 666, _money(ytd))

    # Deductions band
    _band(c, 36, 600, 540, 18, colors.HexColor("#1F4E79"))
    c.setFillColor(colors.white)
    c.drawString(48, 606, "DEDUCTIONS")
    c.drawString(420, 606, "YTD")
    c.setFillColor(colors.black)

    ded = gross - net
    rows = [
        ("Federal Income Tax", round(ded * 0.55), round(ded * 0.55 * 12)),
        ("Social Security",    round(ded * 0.18), round(ded * 0.18 * 12)),
        ("Medicare",           round(ded * 0.06), round(ded * 0.06 * 12)),
        ("State Tax",          round(ded * 0.15), round(ded * 0.15 * 12)),
    ]
    y = 580
    for label, cur, ytd_v in rows:
        c.drawString(48, y, label)
        c.drawRightString(380, y, _money(cur))
        c.drawRightString(560, y, _money(ytd_v))
        y -= 18

    c.setFont("Times-Bold", 14)
    c.drawString(36, 480, f"NET PAY  {_money(net)}")
    c.setFont("Times-Italic", 8)
    c.drawString(36, 60, "Paychex Inc. — Rochester, NY")
    c.showPage()
    return _save(c, buf)


def gen_paystub_workday(fields: dict) -> bytes:
    """Workday-style: minimal single-column, Helvetica thin, lots of
    whitespace, no colored bands."""
    buf, c = _new_canvas()
    c.setFont("Helvetica", 24)
    c.drawString(72, 740, "Pay Statement")
    c.setFont("Helvetica", 10)
    c.setFillColor(colors.HexColor("#666666"))
    c.drawString(72, 720, "Workday Payroll")
    c.setFillColor(colors.black)
    c.line(72, 712, PAGE_W - 72, 712)

    gross = float(fields.get("gross_pay", 0) or 0)
    ytd   = float(fields.get("ytd_gross", 0) or 0)
    net   = float(fields.get("net_pay", 0) or 0)

    pairs = [
        ("Employer",          fields.get("employer_name", "")),
        ("Employee",          fields.get("employee_name", "")),
        ("Period ending",     fields.get("pay_period_end", "")),
        ("Gross pay",         _money(gross)),
        ("YTD gross",         _money(ytd)),
        ("Net pay",           _money(net)),
    ]
    y = 680
    for label, val in pairs:
        c.setFont("Helvetica", 9)
        c.setFillColor(colors.HexColor("#999999"))
        c.drawString(72, y, label.upper())
        c.setFillColor(colors.black)
        c.setFont("Helvetica", 12)
        c.drawString(72, y - 16, str(val))
        y -= 44

    c.showPage()
    return _save(c, buf)


_PAYSTUB_GENS = {
    "ADP":     gen_paystub_adp,
    "Paychex": gen_paystub_paychex,
    "Workday": gen_paystub_workday,
}


# ===========================================================================
# Bank statement — three formats, multi-page
# ===========================================================================


def _fake_txns(seed: int, count: int, avg: float) -> list[tuple]:
    """Reproducible transaction list: (date, description, amount, kind)."""
    r = random.Random(seed)
    today = date.today()
    txns: list[tuple] = []
    for i in range(count):
        d = today - timedelta(days=i)
        if r.random() < 0.4:
            amt = round(avg / 10 * (0.5 + r.random()), 2)
            txns.append((d.isoformat(), "Payroll deposit", amt, "credit"))
        elif r.random() < 0.6:
            amt = -round(avg / 30 * (0.3 + r.random()), 2)
            desc = r.choice([
                "POS PURCHASE H-E-B", "ACH ELEC PAYMENT TXU",
                "APPLE.COM/BILL", "AMZN MKTP US",
                "NETFLIX.COM", "SHELL OIL 10025",
            ])
            txns.append((d.isoformat(), desc, amt, "debit"))
        else:
            amt = -round(avg / 50 * (0.5 + r.random()), 2)
            txns.append((d.isoformat(), "ATM WITHDRAWAL", amt, "debit"))
    return txns


def gen_bank_chase(fields: dict) -> bytes:
    """Chase: blue header, summary on page 1, transactions on pages 2-3.
    ``ending_balance`` lands on page 1."""
    buf, c = _new_canvas()
    end_bal = float(fields.get("ending_balance", 0) or 0)
    holder  = fields.get("account_holder", "")
    avg_dep = float(fields.get("avg_monthly_deposits", 0) or 0)

    # Page 1 — summary
    _band(c, 0, 740, PAGE_W, 36, colors.HexColor("#117ACA"))
    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 18)
    c.drawString(36, 750, "Chase Personal Checking")
    c.setFillColor(colors.black)
    c.setFont("Helvetica", 10)
    c.drawString(36, 720, f"Account holder: {holder}")
    c.drawString(36, 706, f"Statement period: {date.today().isoformat()}")

    c.setFont("Helvetica-Bold", 12)
    c.drawString(36, 670, "Account summary")
    c.setFont("Helvetica", 11)
    c.drawString(36, 650, "Beginning balance")
    c.drawRightString(560, 650, _money(end_bal - avg_dep + 1500))
    c.drawString(36, 632, "Total deposits")
    c.drawRightString(560, 632, _money(avg_dep))
    c.drawString(36, 614, "Total withdrawals")
    c.drawRightString(560, 614, _money(-1500))
    c.setFont("Helvetica-Bold", 12)
    c.drawString(36, 590, "Ending balance")
    c.drawRightString(560, 590, _money(end_bal))
    c.showPage()

    # Pages 2-3 — transactions
    txns = _fake_txns(seed=hash(holder) & 0xFFFF, count=24, avg=avg_dep or 5000)
    pages = [txns[:12], txns[12:]]
    for page_txns in pages:
        _band(c, 0, 740, PAGE_W, 24, colors.HexColor("#117ACA"))
        c.setFillColor(colors.white)
        c.setFont("Helvetica-Bold", 12)
        c.drawString(36, 746, "Transaction details")
        c.setFillColor(colors.black)

        c.setFont("Helvetica-Bold", 10)
        c.drawString(36, 712, "Date")
        c.drawString(140, 712, "Description")
        c.drawRightString(560, 712, "Amount")
        c.line(36, 706, 560, 706)

        c.setFont("Helvetica", 10)
        y = 690
        for d_, desc, amt, _kind in page_txns:
            c.drawString(36, y, d_)
            c.drawString(140, y, desc[:40])
            c.drawRightString(560, y, _money(amt))
            y -= 16
        c.showPage()
    return _save(c, buf)


def gen_bank_wells(fields: dict) -> bytes:
    """Wells Fargo: red accents, summary on page 1, transactions on
    page 2. ``ending_balance`` on page 1."""
    buf, c = _new_canvas()
    end_bal = float(fields.get("ending_balance", 0) or 0)
    holder  = fields.get("account_holder", "")
    avg_dep = float(fields.get("avg_monthly_deposits", 0) or 0)

    # Page 1 — summary
    _band(c, 0, 730, PAGE_W, 50, colors.HexColor("#D71E28"))
    c.setFillColor(colors.HexColor("#FFCD41"))
    c.setFont("Helvetica-Bold", 22)
    c.drawString(36, 752, "WELLS FARGO")
    c.setFillColor(colors.white)
    c.setFont("Helvetica", 10)
    c.drawString(36, 736, "Everyday Checking Statement")

    c.setFillColor(colors.black)
    c.setFont("Helvetica", 10)
    c.drawString(36, 700, f"Customer: {holder}")
    c.drawString(36, 686, f"Period ending: {date.today().isoformat()}")

    c.setFont("Helvetica-Bold", 12)
    c.drawString(36, 650, "Summary of activity")
    c.setFont("Helvetica", 11)
    c.drawString(36, 630, "Ending balance")
    c.setFont("Helvetica-Bold", 14)
    c.drawRightString(560, 630, _money(end_bal))
    c.setFont("Helvetica", 11)
    c.drawString(36, 612, "Average ledger balance")
    c.drawRightString(560, 612, _money(end_bal * 0.94))
    c.drawString(36, 594, "Deposits")
    c.drawRightString(560, 594, _money(avg_dep))
    c.drawString(36, 576, "Withdrawals")
    c.drawRightString(560, 576, _money(-(avg_dep - 1500)))
    c.showPage()

    # Page 2 — transactions
    txns = _fake_txns(seed=hash(holder) & 0xFFFF, count=18, avg=avg_dep or 5000)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(36, 750, "Transaction history")
    c.setFont("Helvetica-Bold", 10)
    c.drawString(36, 720, "Date")
    c.drawString(120, 720, "Description")
    c.drawRightString(560, 720, "Amount ($)")
    c.line(36, 714, 560, 714)
    c.setFont("Helvetica", 10)
    y = 700
    for d_, desc, amt, _kind in txns:
        c.drawString(36, y, d_)
        c.drawString(120, y, desc[:42])
        c.drawRightString(560, y, _money(amt))
        y -= 15
    c.showPage()
    return _save(c, buf)


def gen_bank_boa(fields: dict) -> bytes:
    """BOA: column-based with a running-balance column. Summary at top
    of page 1 (no ending balance), transactions span pages 2-3, and the
    final ``ending_balance`` lands on page 3 (last running-balance row).
    Stresses the multi-page extraction window."""
    buf, c = _new_canvas()
    end_bal = float(fields.get("ending_balance", 0) or 0)
    holder  = fields.get("account_holder", "")
    avg_dep = float(fields.get("avg_monthly_deposits", 0) or 0)

    # Page 1 — masthead + transactions header
    _band(c, 0, 738, PAGE_W, 30, colors.HexColor("#012169"))
    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 16)
    c.drawString(36, 748, "Bank of America   ·   Account Statement")
    c.setFillColor(colors.black)
    c.setFont("Helvetica", 10)
    c.drawString(36, 718, f"Account: {holder}")
    c.drawString(36, 704, f"Statement period: {date.today().isoformat()}")
    c.drawString(36, 690, f"Average daily balance: {_money(end_bal * 0.96)}")
    c.drawString(36, 676,
                 f"Total deposits this period: {_money(avg_dep)}")

    c.setFont("Helvetica-Bold", 11)
    c.drawString(36, 640, "Date")
    c.drawString(110, 640, "Description")
    c.drawRightString(420, 640, "Amount")
    c.drawRightString(560, 640, "Balance")
    c.line(36, 634, 560, 634)
    c.showPage()

    # Pages 2 & 3 — running-balance ledger; ending balance lands on p3.
    txns = _fake_txns(seed=hash(holder) & 0xFFFF, count=30,
                      avg=avg_dep or 5000)
    running = end_bal - sum(t[2] for t in txns)
    pages = [txns[:15], txns[15:]]
    for pi, page_txns in enumerate(pages):
        c.setFont("Helvetica-Bold", 11)
        c.drawString(36, 750, "Date")
        c.drawString(110, 750, "Description")
        c.drawRightString(420, 750, "Amount")
        c.drawRightString(560, 750, "Balance")
        c.line(36, 744, 560, 744)
        c.setFont("Helvetica", 10)
        y = 728
        for d_, desc, amt, _ in page_txns:
            running += amt
            c.drawString(36, y, d_)
            c.drawString(110, y, desc[:38])
            c.drawRightString(420, y, _money(amt))
            c.drawRightString(560, y, _money(running))
            y -= 16
        # Last page → stamp the period-ending balance prominently.
        if pi == len(pages) - 1:
            c.setFont("Helvetica-Bold", 14)
            c.drawString(36, y - 12, f"Ending balance  {_money(end_bal)}")
        c.showPage()
    return _save(c, buf)


_BANK_GENS = {"Chase": gen_bank_chase, "Wells": gen_bank_wells, "BOA": gen_bank_boa}


# ===========================================================================
# Title — two formats, multi-page
# ===========================================================================


def gen_title_first_american(fields: dict, doc_type: str = "TITLE_COMMITMENT") -> bytes:
    """Formal legal layout — Schedule A and B on separate pages. Times-Roman
    everywhere, italic disclaimers."""
    buf, c = _new_canvas()
    is_commit = doc_type == "TITLE_COMMITMENT"
    title_lbl = "Commitment for Title Insurance" if is_commit else "Title Insurance Policy"

    # Cover page
    c.setFont("Times-Bold", 22)
    c.drawCentredString(PAGE_W / 2, 700, "First American Title")
    c.setFont("Times-Italic", 12)
    c.drawCentredString(PAGE_W / 2, 680, "Title Insurance Company")
    c.line(72, 670, PAGE_W - 72, 670)

    c.setFont("Times-Bold", 16)
    c.drawCentredString(PAGE_W / 2, 620, title_lbl)
    c.setFont("Times-Roman", 11)
    if is_commit:
        c.drawString(72, 580, f"Commitment No.: {fields.get('title_commitment_id', '')}")
        c.drawString(72, 560, f"Lender: {fields.get('lender_name', 'EDMS Mortgage')}")
    else:
        c.drawString(72, 580, f"Policy No.: {fields.get('policy_number', '')}")
        c.drawString(72, 560, f"Coverage Amount: {_money(fields.get('coverage_amount', 0))}")
    c.drawString(72, 540, f"Date: {date.today().isoformat()}")
    c.drawString(72, 520, "Underwriter: First American Title Insurance Co.")
    c.showPage()

    # Schedule A
    c.setFont("Times-Bold", 14)
    c.drawString(72, 740, "Schedule A")
    c.setFont("Times-Roman", 11)
    c.line(72, 734, PAGE_W - 72, 734)
    c.drawString(72, 710, "1. Effective Date: " + date.today().isoformat())
    c.drawString(72, 690, "2. Estate insured: Fee Simple")
    c.drawString(72, 670, "3. Title to the estate is vested in the Insured.")
    c.drawString(72, 650, "4. Land described in the Commitment is set forth in Schedule A:")
    c.setFont("Times-Italic", 11)
    c.drawString(96, 632, "Lot 12, Block 7, of Hill Country Subdivision")
    c.drawString(96, 614, "Travis County, Texas")
    c.showPage()

    # Schedule B
    c.setFont("Times-Bold", 14)
    c.drawString(72, 740, "Schedule B — Exceptions and Requirements")
    c.setFont("Times-Roman", 11)
    c.line(72, 734, PAGE_W - 72, 734)
    items = [
        "Easements as shown on recorded plat.",
        "Restrictive covenants of record.",
        "Real-estate taxes for the current year, not yet due.",
        "All matters disclosed by survey delivered prior to closing.",
        "Mineral rights as reserved in prior conveyances.",
    ]
    y = 710
    for i, item in enumerate(items, start=1):
        c.drawString(72, y, f"{i}. {item}")
        y -= 22
    c.showPage()
    return _save(c, buf)


def gen_title_chicago(fields: dict, doc_type: str = "TITLE_COMMITMENT") -> bytes:
    """Combined Schedule A+B under headings, modern sans, no separate
    cover page."""
    buf, c = _new_canvas()
    is_commit = doc_type == "TITLE_COMMITMENT"

    _band(c, 0, 745, PAGE_W, 24, colors.HexColor("#7B0F1A"))
    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 14)
    c.drawString(36, 750, "CHICAGO TITLE")
    c.setFillColor(colors.black)
    c.setFont("Helvetica-Bold", 16)
    c.drawString(36, 720,
                 "Title Commitment" if is_commit else "Owner's Policy of Title Insurance")
    c.setFont("Helvetica", 10)
    if is_commit:
        c.drawString(36, 700,
                     f"File no.  {fields.get('title_commitment_id', '')}")
        c.drawString(36, 686,
                     f"Lender    {fields.get('lender_name', 'EDMS Mortgage')}")
    else:
        c.drawString(36, 700,
                     f"Policy no.       {fields.get('policy_number', '')}")
        c.drawString(36, 686,
                     f"Coverage amount  {_money(fields.get('coverage_amount', 0))}")
    c.drawString(36, 672, f"Issued        {date.today().isoformat()}")
    c.line(36, 666, PAGE_W - 36, 666)

    c.setFont("Helvetica-Bold", 11)
    c.drawString(36, 640, "Schedule A — Property")
    c.setFont("Helvetica", 10)
    c.drawString(36, 620, "Estate / Interest:    Fee simple")
    c.drawString(36, 606, "Vesting:              Buyer of record (per closing)")
    c.drawString(36, 592, "Property description:")
    c.drawString(72, 578, "Lot 12, Block 7, Hill Country Subdivision, Travis Co., TX")

    c.setFont("Helvetica-Bold", 11)
    c.drawString(36, 540, "Schedule B — Exceptions")
    c.setFont("Helvetica", 10)
    items = [
        "Restrictive covenants of record",
        "Easements per recorded plat",
        "Mineral reservations",
        "Current-year property taxes not yet due",
    ]
    y = 522
    for item in items:
        c.drawString(36, y, "• " + item)
        y -= 16
    c.showPage()

    # Page 2 — terms
    c.setFont("Helvetica-Bold", 12)
    c.drawString(36, 750, "Schedule B — Requirements")
    c.setFont("Helvetica", 10)
    reqs = [
        "Survey by licensed Texas surveyor.",
        "Lender's closing protection letter.",
        "Wire instructions verified by phone.",
        "Hazard insurance binder naming lender as mortgagee.",
    ]
    y = 720
    for r in reqs:
        c.drawString(36, y, "— " + r)
        y -= 16

    c.setFont("Helvetica-Oblique", 9)
    c.drawString(36, 80, "Chicago Title Insurance Company · A Fidelity National Financial brand")
    c.showPage()
    return _save(c, buf)


_TITLE_GENS = {
    "FirstAmerican": gen_title_first_american,
    "Chicago":       gen_title_chicago,
}


# ===========================================================================
# Credit report — two formats, multi-page
# ===========================================================================


def _fake_tradelines(seed: int, count: int) -> list[dict]:
    r = random.Random(seed)
    creditors = [
        "CHASE CARD SERVICES", "CITI VISA", "CAPITAL ONE BANK",
        "DISCOVER FIN SVC", "WELLS FARGO HE LOC", "TOYOTA MOTOR CRED",
        "BANK OF AMER MTG", "AMERICAN EXPRESS", "BARCLAYS CARD",
        "USAA SAVINGS BK", "STUDENT LOAN SERV",
    ]
    out = []
    for i in range(count):
        bal = r.randint(0, 18000)
        lim = max(bal + r.randint(100, 8000), 500)
        out.append({
            "creditor":      r.choice(creditors),
            "type":          r.choice(["Revolving", "Installment"]),
            "opened":        f"{2014 + r.randint(0, 8):04d}-{r.randint(1,12):02d}",
            "balance":       bal,
            "limit":         lim,
            "monthly_pmt":   r.randint(25, 600),
            "status":        r.choice(["Current", "Current", "Current", "30 days"]),
        })
    return out


def gen_credit_equifax(fields: dict) -> bytes:
    """Equifax tri-merge: scores in a row at top of page 1, tradelines
    span pages 2-3, inquiries + public records on page 4."""
    buf, c = _new_canvas()
    eq, ex, tu = (fields.get("equifax_score"),
                  fields.get("experian_score"),
                  fields.get("transunion_score"))

    # Page 1 — header + scores
    _band(c, 0, 740, PAGE_W, 30, colors.HexColor("#A41F35"))
    c.setFillColor(colors.white)
    c.setFont("Helvetica-Bold", 16)
    c.drawString(36, 750, "Equifax Tri-Merge Credit Report")
    c.setFillColor(colors.black)
    c.setFont("Helvetica", 10)
    c.drawString(36, 720, "Subject:  (lender confidential)")
    c.drawString(36, 706, f"Pulled:   {date.today().isoformat()}")
    c.drawString(36, 692, f"Mid score: {fields.get('mid_score', 0)}")

    # Three score boxes side-by-side
    boxes = [("Equifax", eq), ("Experian", ex), ("TransUnion", tu)]
    box_w = 160
    for i, (lbl, val) in enumerate(boxes):
        x = 36 + i * (box_w + 16)
        c.rect(x, 600, box_w, 70, stroke=1, fill=0)
        c.setFont("Helvetica", 10)
        c.drawString(x + 8, 654, lbl)
        c.setFont("Helvetica-Bold", 28)
        c.drawString(x + 8, 615, str(val))

    c.setFont("Helvetica-Bold", 11)
    c.drawString(36, 560, "Summary")
    c.setFont("Helvetica", 10)
    c.drawString(36, 542, f"Tradelines: {fields.get('tradeline_count', 0)}")
    c.drawString(36, 528,
                 f"Total monthly obligations: {_money(fields.get('total_monthly_obligations', 0))}")
    c.drawString(36, 514,
                 f"Hard inquiries (12 mo): {fields.get('hard_inquiries_12mo', 0)}")
    c.drawString(36, 500,
                 f"Credit band: {fields.get('credit_band', '')}")
    c.showPage()

    # Pages 2-3 — tradelines
    tl_count = int(fields.get("tradeline_count") or 14)
    tls = _fake_tradelines(seed=tl_count, count=tl_count)
    for chunk_start in range(0, len(tls), 8):
        chunk = tls[chunk_start:chunk_start + 8]
        c.setFont("Helvetica-Bold", 12)
        c.drawString(36, 750, "Tradelines")
        c.setFont("Helvetica-Bold", 9)
        c.drawString(36, 728, "Creditor")
        c.drawString(220, 728, "Type")
        c.drawString(290, 728, "Opened")
        c.drawString(350, 728, "Balance")
        c.drawString(420, 728, "Limit")
        c.drawString(480, 728, "Pmt")
        c.drawString(530, 728, "Status")
        c.line(36, 722, 575, 722)
        c.setFont("Helvetica", 9)
        y = 706
        for tl in chunk:
            c.drawString(36, y, tl["creditor"][:28])
            c.drawString(220, y, tl["type"])
            c.drawString(290, y, tl["opened"])
            c.drawRightString(410, y, _money(tl["balance"]))
            c.drawRightString(470, y, _money(tl["limit"]))
            c.drawRightString(525, y, _money(tl["monthly_pmt"]))
            c.drawString(530, y, tl["status"])
            y -= 28
        c.showPage()

    # Page 4 — inquiries
    c.setFont("Helvetica-Bold", 12)
    c.drawString(36, 750, "Hard Inquiries (last 12 months)")
    c.setFont("Helvetica", 10)
    inquiry_lines = [
        f"{(date.today() - timedelta(days=30 * i)).isoformat()}   "
        f"{r}"
        for i, r in enumerate(["EDMS Mortgage Origination", "Auto Lender Pull"][
            : int(fields.get("hard_inquiries_12mo") or 1) or 1
        ])
    ]
    y = 722
    for ln in inquiry_lines:
        c.drawString(48, y, ln)
        y -= 16
    c.setFont("Helvetica-Bold", 12)
    c.drawString(36, y - 20, "Public Records")
    c.setFont("Helvetica", 10)
    c.drawString(48, y - 38, "None reported.")
    c.showPage()
    return _save(c, buf)


def gen_credit_experian(fields: dict) -> bytes:
    """Experian: scores in summary boxes on the right side, tradelines
    page 2-3, inquiries page 4. Different score placement vs Equifax."""
    buf, c = _new_canvas()

    c.setFont("Helvetica-Bold", 22)
    c.setFillColor(colors.HexColor("#26478D"))
    c.drawString(36, 740, "experian.")
    c.setFillColor(colors.black)
    c.setFont("Helvetica", 11)
    c.drawString(36, 720, "Consumer Credit File Disclosure")
    c.line(36, 714, PAGE_W - 36, 714)

    c.setFont("Helvetica-Bold", 11)
    c.drawString(36, 690, "File summary")
    c.setFont("Helvetica", 10)
    c.drawString(36, 672, f"Credit band:                {fields.get('credit_band', '')}")
    c.drawString(36, 658, f"Tradelines reporting:       {fields.get('tradeline_count', 0)}")
    c.drawString(36, 644, f"Monthly obligations:        {_money(fields.get('total_monthly_obligations', 0))}")
    c.drawString(36, 630, f"Hard inquiries (12 mo):     {fields.get('hard_inquiries_12mo', 0)}")

    # Score boxes — right side, stacked
    box_x, box_y = 410, 600
    boxes = [
        ("Experian",   fields.get("experian_score")),
        ("Equifax",    fields.get("equifax_score")),
        ("TransUnion", fields.get("transunion_score")),
        ("Mid",        fields.get("mid_score")),
    ]
    for lbl, val in boxes:
        c.rect(box_x, box_y, 160, 50, stroke=1, fill=0)
        c.setFont("Helvetica", 9)
        c.drawString(box_x + 8, box_y + 32, lbl)
        c.setFont("Helvetica-Bold", 18)
        c.drawRightString(box_x + 152, box_y + 12, str(val))
        box_y -= 60
    c.showPage()

    # Pages 2-3 — tradelines
    tl_count = int(fields.get("tradeline_count") or 14)
    tls = _fake_tradelines(seed=tl_count + 7, count=tl_count)
    for chunk_start in range(0, len(tls), 8):
        chunk = tls[chunk_start:chunk_start + 8]
        c.setFont("Helvetica-Bold", 11)
        c.drawString(36, 750, "Open accounts")
        c.setFont("Helvetica", 9)
        for i, tl in enumerate(chunk):
            y = 710 - i * 64
            c.setFont("Helvetica-Bold", 10)
            c.drawString(36, y, tl["creditor"])
            c.setFont("Helvetica", 9)
            c.drawString(36, y - 14, f"{tl['type']}  ·  Opened {tl['opened']}")
            c.drawString(36, y - 28, f"Balance {_money(tl['balance'])}   "
                                     f"Limit {_money(tl['limit'])}   "
                                     f"Pmt {_money(tl['monthly_pmt'])}")
            c.drawString(36, y - 42, f"Status: {tl['status']}")
            c.line(36, y - 50, 575, y - 50)
        c.showPage()

    # Page 4 — inquiries + public records
    c.setFont("Helvetica-Bold", 12)
    c.drawString(36, 750, "Inquiries")
    c.setFont("Helvetica", 10)
    n_inq = int(fields.get("hard_inquiries_12mo") or 1) or 1
    y = 722
    for i in range(n_inq):
        c.drawString(48, y,
                     f"{(date.today() - timedelta(days=30 * i)).isoformat()}   "
                     "EDMS Mortgage  (mortgage)")
        y -= 16
    c.setFont("Helvetica-Bold", 12)
    c.drawString(36, y - 20, "Public records")
    c.setFont("Helvetica", 10)
    c.drawString(48, y - 38, "None.")
    c.showPage()
    return _save(c, buf)


_CREDIT_GENS = {"Equifax": gen_credit_equifax, "Experian": gen_credit_experian}


# ===========================================================================
# Appraisal — two formats, multi-page
# ===========================================================================


def gen_appraisal_urar(fields: dict) -> bytes:
    """URAR-style box form. ``appraised_value`` on page 1 (subject)."""
    buf, c = _new_canvas()
    appraised = fields.get("appraised_value", 0)

    # Page 1 — Subject
    c.setFont("Helvetica-Bold", 14)
    c.drawString(36, 750, "Uniform Residential Appraisal Report")
    c.setFont("Helvetica", 9)
    c.drawString(36, 736, "Form 1004 / Fannie Mae")
    c.line(36, 730, PAGE_W - 36, 730)

    rect_specs = [
        ("Property type",     fields.get("property_type", "SFR"),       36, 700, 260, 30),
        ("Condition rating",  fields.get("condition_rating", "C3"),     300, 700, 240, 30),
        ("Appraisal form",    fields.get("appraisal_form", "URAR"),     36, 660, 260, 30),
        ("Appraised value",   _money(appraised),                         300, 660, 240, 30),
    ]
    for label, val, x, y, w, h in rect_specs:
        c.rect(x, y, w, h, stroke=1, fill=0)
        c.setFont("Helvetica", 8)
        c.drawString(x + 4, y + h - 10, label)
        c.setFont("Helvetica-Bold", 12)
        c.drawString(x + 4, y + 6, str(val))
    c.showPage()

    # Page 2 — Comparables
    c.setFont("Helvetica-Bold", 12)
    c.drawString(36, 750, "Sales Comparison Approach")
    c.setFont("Helvetica-Bold", 9)
    c.drawString(36, 720, "Comparable")
    c.drawString(160, 720, "Sale price")
    c.drawString(280, 720, "GLA (sq ft)")
    c.drawString(400, 720, "Adjustments")
    c.line(36, 714, 560, 714)
    c.setFont("Helvetica", 9)
    base = float(appraised or 0)
    rows = [
        ("Comp 1", base * 0.97, 2150, "+ $2,500 condition"),
        ("Comp 2", base * 1.03, 2200, "− $5,000 location"),
        ("Comp 3", base * 0.99, 2125, "+ $1,000 lot size"),
    ]
    y = 700
    for name, price, gla, adj in rows:
        c.drawString(36, y, name)
        c.drawString(160, y, _money(price))
        c.drawString(280, y, str(gla))
        c.drawString(400, y, adj)
        y -= 18
    c.showPage()

    # Page 3 — Photos
    c.setFont("Helvetica-Bold", 12)
    c.drawString(36, 750, "Photo Addendum (placeholder)")
    c.setFont("Helvetica-Oblique", 9)
    c.drawString(36, 730,
                 "Actual photos omitted in synthetic generation.")
    c.rect(72, 500, 220, 180, stroke=1, fill=0)
    c.rect(312, 500, 220, 180, stroke=1, fill=0)
    c.setFont("Helvetica", 9)
    c.drawString(120, 480, "Front elevation")
    c.drawString(360, 480, "Rear elevation")
    c.showPage()

    # Page 4 — Market conditions
    c.setFont("Helvetica-Bold", 12)
    c.drawString(36, 750, "Market Conditions Addendum (Form 1004MC)")
    c.setFont("Helvetica", 10)
    paras = [
        "Inventory of competing properties: declining over the prior 12-month period.",
        "Median sale price: increasing 3-5% year over year.",
        "Median days-on-market: 28 (stable).",
        "Sale-to-list ratio: 99.2%.",
        "Appraiser concludes the subject is in a stable / slightly appreciating market.",
    ]
    y = 720
    for p in paras:
        for ln in _wrap(p, 90):
            c.drawString(36, y, ln)
            y -= 14
        y -= 6
    c.showPage()
    return _save(c, buf)


def gen_appraisal_narrative(fields: dict) -> bytes:
    """Narrative-style: prose paragraphs with values inline. Buries
    ``appraised_value`` in page 2 (the comparables paragraph) — different
    page than URAR's page 1."""
    buf, c = _new_canvas()
    appraised = fields.get("appraised_value", 0)

    # Page 1 — Letter / scope of work (no appraised_value yet)
    c.setFont("Times-Bold", 14)
    c.drawString(72, 740, "Narrative Appraisal Report")
    c.setFont("Times-Roman", 10)
    c.drawString(72, 724, f"Prepared {date.today().isoformat()}")
    c.line(72, 718, PAGE_W - 72, 718)

    intro = (
        "Pursuant to your request, the appraiser has inspected the property "
        "and completed an analysis of recent sales activity in the immediate "
        "neighbourhood. The subject is identified as a "
        f"{fields.get('property_type', 'single-family residence')} of "
        f"condition rating {fields.get('condition_rating', 'C3')}. The scope "
        "of work covers the subject's physical condition, neighbourhood "
        "characteristics, market conditions, and a sales-comparison analysis "
        "of comparable transactions within the past 180 days."
    )
    c.setFont("Times-Roman", 11)
    y = 690
    for ln in _wrap(intro, 92):
        c.drawString(72, y, ln)
        y -= 14
    c.showPage()

    # Page 2 — Comparables (appraised_value lives HERE)
    c.setFont("Times-Bold", 13)
    c.drawString(72, 740, "Sales Comparison Analysis")
    c.line(72, 734, PAGE_W - 72, 734)
    c.setFont("Times-Roman", 11)
    para = (
        "Three comparable sales were considered. After standard adjustments "
        "for condition, GLA, and location, the indicated value of the subject "
        f"property is concluded to be {_short_money(appraised)}. This value "
        "is supported by the listing-price-to-sale-price ratio observed in "
        "the immediate market."
    )
    y = 700
    for ln in _wrap(para, 92):
        c.drawString(72, y, ln)
        y -= 14
    c.setFont("Times-Bold", 11)
    c.drawString(72, y - 12,
                 f"Concluded value:  {_money(appraised)}")
    c.showPage()

    # Page 3 — Reconciliation
    c.setFont("Times-Bold", 13)
    c.drawString(72, 740, "Reconciliation & Final Opinion of Value")
    c.line(72, 734, PAGE_W - 72, 734)
    c.setFont("Times-Roman", 11)
    para = (
        "Greatest weight is given to the sales-comparison approach. The "
        "cost approach was developed but accorded supporting weight only. "
        "The income approach was not developed as the subject is owner-"
        "occupied and the market is dominated by owner-occupants."
    )
    y = 700
    for ln in _wrap(para, 92):
        c.drawString(72, y, ln)
        y -= 14
    c.showPage()

    # Page 4 — Market conditions
    c.setFont("Times-Bold", 13)
    c.drawString(72, 740, "Market Conditions")
    c.line(72, 734, PAGE_W - 72, 734)
    c.setFont("Times-Roman", 11)
    text = (
        "Median sale prices in the neighbourhood have increased 3-5% over "
        "the prior twelve months. Inventory of competing properties has "
        "declined modestly. Days on market median: 28. Sale-to-list ratio: "
        "99.2%. The appraiser concludes the subject is in a stable to "
        "slightly appreciating market."
    )
    y = 700
    for ln in _wrap(text, 92):
        c.drawString(72, y, ln)
        y -= 14
    c.showPage()
    return _save(c, buf)


_APPRAISAL_GENS = {
    "URAR":      gen_appraisal_urar,
    "Narrative": gen_appraisal_narrative,
}


# ===========================================================================
# Shared-drive scan variants — visual artifacts of real-world scanning
# ===========================================================================


def _draw_paragraph(c, text: str, x: float, y: float,
                    font: str = "Helvetica", size: int = 11,
                    leading: int = 14, width: int = 90) -> None:
    c.setFont(font, size)
    cy = y
    for ln in _wrap(text, width):
        c.drawString(x, cy, ln)
        cy -= leading


def gen_scan_rotated(text: str, angle: float = 1.5) -> bytes:
    """Whole page rotated by ``angle`` degrees — simulates a sheet
    misfed into a flatbed scanner."""
    buf, c = _new_canvas()
    c.saveState()
    c.translate(PAGE_W / 2, PAGE_H / 2)
    c.rotate(angle)
    c.translate(-PAGE_W / 2, -PAGE_H / 2)

    c.setFont("Helvetica-Bold", 14)
    c.drawString(72, 720, "SCANNED — TO BE CLASSIFIED")
    _draw_paragraph(c, text, 72, 700)
    c.setFont("Helvetica-Oblique", 9)
    c.drawString(72, 100, "(no metadata; classify via AI Vision)")
    c.restoreState()
    c.showPage()
    return _save(c, buf)


def gen_scan_landscape(text: str) -> bytes:
    """Landscape orientation — page rotated 90°. AI Vision must handle
    the orientation difference."""
    buf, c = _new_canvas(pagesize=landscape(letter))
    c.setFont("Helvetica-Bold", 14)
    c.drawString(72, 540, "SCANNED — landscape orientation")
    _draw_paragraph(c, text, 72, 510, width=120)
    c.setFont("Helvetica-Oblique", 9)
    c.drawString(72, 80, "Original page captured sideways; rotate before review.")
    c.showPage()
    return _save(c, buf)


def gen_scan_double_doc(text: str) -> bytes:
    """Two distinct docs jammed onto one scanned page (a common real-
    world issue when batch-feeding documents)."""
    buf, c = _new_canvas()
    # Top half — looks like a property tax receipt
    c.setFont("Helvetica-Bold", 12)
    c.drawString(72, 740, "Travis County Tax Assessor — Receipt #11045")
    c.setFont("Helvetica", 10)
    c.drawString(72, 720, "Property: 1234 Hill Country Rd, Austin TX")
    c.drawString(72, 706, "Tax year: 2025  ·  Amount paid: $7,940.00")
    c.drawString(72, 692, "Method: ACH  ·  Date: 2026-01-15")
    c.line(36, 660, PAGE_W - 36, 660)

    # Bottom half — looks like a personal note
    c.setFont("Helvetica-Bold", 12)
    c.drawString(72, 620, "Note from borrower")
    _draw_paragraph(c, text, 72, 600, width=80)
    c.setFont("Helvetica-Oblique", 9)
    c.drawString(72, 80, "(two physical documents on one scan — split before processing)")
    c.showPage()
    return _save(c, buf)


def gen_scan_faded(text: str) -> bytes:
    """Simulated old photocopy — light gray fill across the page."""
    buf, c = _new_canvas()
    c.setFillColor(colors.HexColor("#888888"))
    c.setFont("Helvetica-Bold", 14)
    c.drawString(72, 720, "SCANNED — FADED PHOTOCOPY")
    c.setFont("Helvetica", 11)
    cy = 700
    for ln in _wrap(text, 92):
        c.drawString(72, cy, ln)
        cy -= 14
    c.setFont("Helvetica", 10)
    c.drawString(72, 660, "Generated values:  borrower DL no. TX-099-1244,")
    c.drawString(72, 646, "                   ssn ending 4421,")
    c.drawString(72, 632, "                   dob 1979-08-22.")
    c.setFont("Helvetica-Oblique", 9)
    c.drawString(72, 100, "(simulated old-photocopy artifact: low contrast)")
    c.showPage()
    return _save(c, buf)


_SCAN_VARIANTS = (gen_scan_rotated, gen_scan_landscape,
                  gen_scan_double_doc, gen_scan_faded)


def make_shared_drive_scan(text: str, variant_idx: int = 0) -> bytes:
    return _SCAN_VARIANTS[variant_idx % len(_SCAN_VARIANTS)](text)


# ===========================================================================
# Generic fallback — for doc types without a custom format
# ===========================================================================


def gen_generic(title: str, lines: list) -> bytes:
    buf, c = _new_canvas()
    c.setFont("Helvetica-Bold", 14)
    c.drawString(72, 720, str(title)[:90])
    c.setFont("Helvetica", 10)
    y = 690
    for ln in lines:
        c.drawString(72, y, str(ln)[:100])
        y -= 14
        if y < 80:
            break
    c.showPage()
    return _save(c, buf)


# ===========================================================================
# Top-level dispatcher
# ===========================================================================


def make_pdf(
    doc_type: str, fields: dict, los_id: str, role: str = "primary",
) -> bytes:
    """Produce a format-aware PDF for ``doc_type`` if a renderer exists;
    else return the generic title+kv fallback. Field values rendered into
    the PDF are taken straight from ``fields`` so the meta.json record
    and the PDF stay in lockstep — what one says, the other says."""
    if doc_type in ("W2_CURRENT", "W2_PRIOR"):
        primary_fmt, co_fmt = W2_FORMAT_BY_LOAN.get(los_id, ("ADP", "ADP"))
        fmt = co_fmt if role == "co_borrower" else primary_fmt
        return _W2_GENS[fmt](fields)

    if doc_type == "PAYSTUB_CURRENT":
        primary_fmt, co_fmt = PAYSTUB_FORMAT_BY_LOAN.get(los_id, ("ADP", "ADP"))
        fmt = co_fmt if role == "co_borrower" else primary_fmt
        return _PAYSTUB_GENS[fmt](fields)

    if doc_type.startswith("BANK_STATEMENT") or doc_type == "GIFT_FUNDS_TRAIL":
        fmt = _bank_format(los_id, fields)
        return _BANK_GENS[fmt](fields)

    if doc_type == "CREDIT_REPORT":
        fmt = _rotated_format(los_id, CREDIT_FORMATS)
        return _CREDIT_GENS[fmt](fields)

    if doc_type in ("APPRAISAL_URAR", "APPRAISAL_URAR_1073"):
        fmt = _rotated_format(los_id, APPRAISAL_FORMATS)
        return _APPRAISAL_GENS[fmt](fields)

    if doc_type in ("TITLE_COMMITMENT", "TITLE_INSURANCE"):
        fmt = _rotated_format(los_id, TITLE_FORMATS)
        return _TITLE_GENS[fmt](fields, doc_type)

    title = f"{doc_type} — {los_id}"
    lines = [f"{k}: {v}" for k, v in fields.items()][:12]
    return gen_generic(title, lines)


def format_for(
    doc_type: str, los_id: str, role: str = "primary",
    fields: dict | None = None,
) -> str | None:
    """Surface the format name we'll use, for use by the verification
    script + diagnostics. Mirrors the dispatch logic of ``make_pdf``
    (which is the source of truth) so a verification report can show
    the exact label that was rendered. Returns ``None`` when the doc
    type has no custom format renderer."""
    if doc_type in ("W2_CURRENT", "W2_PRIOR"):
        primary_fmt, co_fmt = W2_FORMAT_BY_LOAN.get(los_id, ("ADP", "ADP"))
        return co_fmt if role == "co_borrower" else primary_fmt
    if doc_type == "PAYSTUB_CURRENT":
        primary_fmt, co_fmt = PAYSTUB_FORMAT_BY_LOAN.get(los_id, ("ADP", "ADP"))
        return co_fmt if role == "co_borrower" else primary_fmt
    if doc_type.startswith("BANK_STATEMENT") or doc_type == "GIFT_FUNDS_TRAIL":
        # When we have the fields dict, defer to the same bank-name
        # heuristic ``make_pdf`` uses; without fields, fall back to a
        # stable round-robin so the label is at least deterministic.
        if fields is not None:
            return _bank_format(los_id, fields)
        idx = int(los_id.rsplit("-", 1)[-1])
        return ("Chase", "Wells", "BOA")[idx % 3]
    if doc_type == "CREDIT_REPORT":
        return _rotated_format(los_id, CREDIT_FORMATS)
    if doc_type in ("APPRAISAL_URAR", "APPRAISAL_URAR_1073"):
        return _rotated_format(los_id, APPRAISAL_FORMATS)
    if doc_type in ("TITLE_COMMITMENT", "TITLE_INSURANCE"):
        return _rotated_format(los_id, TITLE_FORMATS)
    return None
