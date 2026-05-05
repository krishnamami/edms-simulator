"""Pay stub PDF generator (reportlab).

Renders a single-period pay stub with earnings table, YTD column, and
deductions breakdown. Returns (pdf_bytes, metadata).
"""
from __future__ import annotations

import io
from datetime import date
from typing import Optional

from reportlab.lib.colors import HexColor
from reportlab.lib.pagesizes import LETTER
from reportlab.pdfgen import canvas


def _money(v: float) -> str:
    return f"${v:,.2f}"


def generate_paystub(
    *,
    employer_name: str,
    employee_name: str,
    employee_ssn_last4: str,
    pay_period_start: date,
    pay_period_end: date,
    pay_date: date,
    gross_pay: float,
    ytd_gross: float,
    federal_tax: Optional[float] = None,
    state_tax: Optional[float] = None,
    social_security: Optional[float] = None,
    medicare: Optional[float] = None,
    health_insurance: Optional[float] = None,
    retirement_401k: Optional[float] = None,
) -> tuple[bytes, dict]:
    if federal_tax is None:
        federal_tax = round(gross_pay * 0.12, 2)
    if state_tax is None:
        state_tax = round(gross_pay * 0.05, 2)
    if social_security is None:
        social_security = round(gross_pay * 0.062, 2)
    if medicare is None:
        medicare = round(gross_pay * 0.0145, 2)
    if health_insurance is None:
        health_insurance = 150.00
    if retirement_401k is None:
        retirement_401k = round(gross_pay * 0.06, 2)

    deductions_total = sum([
        federal_tax, state_tax, social_security, medicare,
        health_insurance, retirement_401k,
    ])
    net_pay = round(gross_pay - deductions_total, 2)

    metadata = {
        "document_type": "PAYSTUB",
        "employer_name": employer_name,
        "employee_name": employee_name,
        "employee_ssn_masked": f"***-**-{employee_ssn_last4[-4:]}",
        "pay_period_start": pay_period_start.isoformat(),
        "pay_period_end": pay_period_end.isoformat(),
        "pay_date": pay_date.isoformat(),
        "gross_pay": float(gross_pay),
        "ytd_gross": float(ytd_gross),
        "federal_tax": float(federal_tax),
        "state_tax": float(state_tax),
        "social_security": float(social_security),
        "medicare": float(medicare),
        "health_insurance": float(health_insurance),
        "retirement_401k": float(retirement_401k),
        "deductions_total": float(round(deductions_total, 2)),
        "net_pay": float(net_pay),
    }

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=LETTER)
    w, h = LETTER

    # Header
    c.setFont("Helvetica-Bold", 18)
    c.drawString(40, h - 50, employer_name)
    c.setFont("Helvetica-Bold", 12)
    c.drawString(40, h - 70, "EARNINGS STATEMENT")

    # Right-side summary block
    c.setFont("Helvetica", 10)
    c.drawRightString(w - 40, h - 50, f"Pay Period: {pay_period_start.isoformat()} – {pay_period_end.isoformat()}")
    c.drawRightString(w - 40, h - 64, f"Pay Date: {pay_date.isoformat()}")

    # Employee info
    c.setStrokeColor(HexColor("#444444"))
    c.line(40, h - 90, w - 40, h - 90)
    c.setFont("Helvetica-Bold", 10)
    c.drawString(40, h - 110, "Employee:")
    c.setFont("Helvetica", 10)
    c.drawString(110, h - 110, employee_name)
    c.drawString(40, h - 124, f"SSN: ***-**-{employee_ssn_last4[-4:]}")

    # Earnings table
    table_y = h - 170
    c.setFont("Helvetica-Bold", 10)
    c.drawString(40, table_y, "EARNINGS")
    c.line(40, table_y - 4, w - 40, table_y - 4)
    c.setFont("Helvetica-Bold", 9)
    c.drawString(40,  table_y - 22, "Description")
    c.drawRightString(360, table_y - 22, "Current")
    c.drawRightString(500, table_y - 22, "YTD")
    c.setFont("Helvetica", 10)
    c.drawString(40,  table_y - 40, "Regular Pay")
    c.drawRightString(360, table_y - 40, _money(gross_pay))
    c.drawRightString(500, table_y - 40, _money(ytd_gross))
    c.setFont("Helvetica-Bold", 10)
    c.drawString(40, table_y - 60, "Gross Pay")
    c.drawRightString(360, table_y - 60, _money(gross_pay))
    c.drawRightString(500, table_y - 60, _money(ytd_gross))

    # Deductions table
    ded_y = table_y - 110
    c.setFont("Helvetica-Bold", 10)
    c.drawString(40, ded_y, "DEDUCTIONS")
    c.line(40, ded_y - 4, w - 40, ded_y - 4)
    c.setFont("Helvetica", 10)
    rows = [
        ("Federal Income Tax", federal_tax),
        ("State Income Tax",   state_tax),
        ("Social Security",    social_security),
        ("Medicare",           medicare),
        ("Health Insurance",   health_insurance),
        ("401(k) Retirement",  retirement_401k),
    ]
    for i, (label, val) in enumerate(rows):
        c.drawString(40, ded_y - 22 - i * 14, label)
        c.drawRightString(360, ded_y - 22 - i * 14, _money(val))

    # Totals
    total_y = ded_y - 22 - len(rows) * 14 - 10
    c.setFont("Helvetica-Bold", 10)
    c.drawString(40, total_y, "Total Deductions")
    c.drawRightString(360, total_y, _money(deductions_total))
    c.drawString(40, total_y - 24, "Net Pay")
    c.drawRightString(360, total_y - 24, _money(net_pay))

    # Footer
    c.setFont("Helvetica-Oblique", 8)
    c.drawCentredString(
        w / 2,
        40,
        "This is a simulated pay stub generated by EDMS Simulator for testing.",
    )

    c.showPage()
    c.save()
    return buf.getvalue(), metadata
