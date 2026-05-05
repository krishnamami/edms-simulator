"""W-2 PDF generator (reportlab).

Produces a recognizable IRS Form W-2 layout — not a pixel-perfect replica,
but with all the box numbers, labels, and values that a text extractor will
look for. The accompanying metadata dict is the ground truth: any extractor
should round-trip these numbers.
"""
from __future__ import annotations

import io
from typing import Optional

from reportlab.lib.colors import HexColor
from reportlab.lib.pagesizes import LETTER
from reportlab.pdfgen import canvas


def _format_ein(ein: str) -> str:
    digits = "".join(c for c in ein if c.isdigit())
    if len(digits) >= 9:
        return f"{digits[:2]}-{digits[2:9]}"
    return ein


def _mask_ssn(ssn_last4: str) -> str:
    last4 = "".join(c for c in (ssn_last4 or "") if c.isdigit())[-4:]
    return f"***-**-{last4 or 'XXXX'}"


def _money(v: float) -> str:
    return f"{v:,.2f}"


def generate_w2(
    *,
    employee_name: str,
    employee_ssn_last4: str,
    employee_address: str,
    employer_name: str,
    employer_ein: str,
    employer_address: str,
    tax_year: int,
    box1_wages: float,
    box2_fed_tax: Optional[float] = None,
    box3_ss_wages: Optional[float] = None,
    box4_ss_tax: Optional[float] = None,
    box5_medicare_wages: Optional[float] = None,
    box6_medicare_tax: Optional[float] = None,
    box12a_code_d_401k: Optional[float] = None,
) -> tuple[bytes, dict]:
    """Generate a W-2 PDF. Returns (pdf_bytes, metadata).

    Defaults derive standard withholding rates from box1_wages so callers
    can pass just wages and get a coherent doc.
    """
    if box3_ss_wages is None:
        box3_ss_wages = box1_wages
    if box5_medicare_wages is None:
        box5_medicare_wages = box1_wages
    if box2_fed_tax is None:
        box2_fed_tax = round(box1_wages * 0.12, 2)
    if box4_ss_tax is None:
        box4_ss_tax = round(box3_ss_wages * 0.062, 2)
    if box6_medicare_tax is None:
        box6_medicare_tax = round(box5_medicare_wages * 0.0145, 2)
    if box12a_code_d_401k is None:
        box12a_code_d_401k = 0.0

    metadata = {
        "document_type": "W2",
        "tax_year": tax_year,
        "employee_name": employee_name,
        "employee_ssn_masked": _mask_ssn(employee_ssn_last4),
        "employer_name": employer_name,
        "employer_ein": _format_ein(employer_ein),
        "box1_wages": float(box1_wages),
        "box2_fed_tax": float(box2_fed_tax),
        "box3_ss_wages": float(box3_ss_wages),
        "box4_ss_tax": float(box4_ss_tax),
        "box5_medicare_wages": float(box5_medicare_wages),
        "box6_medicare_tax": float(box6_medicare_tax),
        "box12a_code_d_401k": float(box12a_code_d_401k),
    }

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=LETTER)
    width, height = LETTER

    # Watermark — tax year, very light grey, behind everything
    c.saveState()
    c.setFillColor(HexColor("#EAEAEA"))
    c.setFont("Helvetica-Bold", 140)
    c.drawCentredString(width / 2, height / 2 - 40, str(tax_year))
    c.restoreState()

    # Header
    c.setFont("Helvetica-Bold", 16)
    c.drawString(40, height - 50, "Form W-2 Wage and Tax Statement")
    c.setFont("Helvetica", 10)
    c.drawString(40, height - 66, f"Tax Year {tax_year}")
    c.drawRightString(width - 40, height - 50, "OMB No. 1545-0008")

    # Employer panel (top-left)
    y = height - 110
    c.setFont("Helvetica-Bold", 9)
    c.drawString(40, y, "b  Employer Identification Number (EIN)")
    c.setFont("Helvetica", 11)
    c.drawString(40, y - 14, _format_ein(employer_ein))

    c.setFont("Helvetica-Bold", 9)
    c.drawString(40, y - 36, "c  Employer's name, address, and ZIP code")
    c.setFont("Helvetica", 10)
    for i, line in enumerate([employer_name] + employer_address.split("\n")):
        c.drawString(40, y - 50 - i * 12, line)

    # Employee panel (mid-left)
    ey = y - 130
    c.setFont("Helvetica-Bold", 9)
    c.drawString(40, ey, "a  Employee's social security number")
    c.setFont("Helvetica", 11)
    c.drawString(40, ey - 14, _mask_ssn(employee_ssn_last4))

    c.setFont("Helvetica-Bold", 9)
    c.drawString(40, ey - 36, "e  Employee's first name, last name, address")
    c.setFont("Helvetica", 10)
    for i, line in enumerate([employee_name] + employee_address.split("\n")):
        c.drawString(40, ey - 50 - i * 12, line)

    # Box panel (right side)
    box_x = width - 260
    by = height - 110

    def _box(label: str, value: str, ypos: float):
        c.setFont("Helvetica-Bold", 9)
        c.drawString(box_x, ypos, label)
        c.setFont("Helvetica", 11)
        c.drawString(box_x, ypos - 14, value)
        c.setStrokeColor(HexColor("#444444"))
        c.rect(box_x - 4, ypos - 18, 240, 26, stroke=1, fill=0)

    _box("1  Wages, tips, other compensation", _money(box1_wages), by)
    _box("2  Federal income tax withheld",     _money(box2_fed_tax), by - 36)
    _box("3  Social security wages",           _money(box3_ss_wages), by - 72)
    _box("4  Social security tax withheld",    _money(box4_ss_tax), by - 108)
    _box("5  Medicare wages and tips",         _money(box5_medicare_wages), by - 144)
    _box("6  Medicare tax withheld",           _money(box6_medicare_tax), by - 180)
    _box("12a  Code D (401(k) contributions)", f"D  {_money(box12a_code_d_401k)}", by - 216)

    # Footer
    c.setFont("Helvetica-Oblique", 8)
    c.drawCentredString(
        width / 2,
        40,
        "This is a simulated W-2 generated by EDMS Simulator for testing.",
    )

    c.showPage()
    c.save()
    return buf.getvalue(), metadata
