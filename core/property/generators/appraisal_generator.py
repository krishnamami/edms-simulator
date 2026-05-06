"""URAR (Uniform Residential Appraisal Report) PDF generator.

Not a pixel-perfect Form 1004 replica — but with all the labels and dollar
values an extractor would look for: opinion of value, condition rating,
effective date, three comparable sales.
"""
from __future__ import annotations

import io
from typing import Optional

from reportlab.lib.colors import HexColor
from reportlab.lib.pagesizes import LETTER
from reportlab.pdfgen import canvas


def _money(v: float) -> str:
    return f"${v:,.0f}"


def generate_appraisal(
    *,
    property_address: str,
    legal_description: str = "",
    property_type: str = "Single Family",
    year_built: int = 2005,
    sqft: int = 2400,
    appraised_value: float = 575_000,
    effective_date: str = "2025-01-15",
    condition_rating: str = "C3",
    appraiser_name: str = "Jane Doe, MAI",
    appraiser_license: str = "AB-12345",
    comparables: Optional[list[dict]] = None,
) -> tuple[bytes, dict]:
    """Generate a URAR appraisal PDF. Returns (pdf_bytes, metadata)."""
    if comparables is None:
        comparables = [
            {"address": "120 Oak Ave",   "sale_price": appraised_value - 10_000, "sqft": sqft - 50},
            {"address": "230 Maple St",  "sale_price": appraised_value + 5_000,  "sqft": sqft + 75},
            {"address": "340 Cedar Ln",  "sale_price": appraised_value - 5_000,  "sqft": sqft - 25},
        ]

    metadata = {
        "document_type":      "APPRAISAL_URAR",
        "appraised_value":    float(appraised_value),
        "condition_rating":   condition_rating,
        "effective_date":     effective_date,
        "property_address":   property_address,
        "year_built":         year_built,
        "sqft":               sqft,
        "appraiser_name":     appraiser_name,
        "appraiser_license":  appraiser_license,
        "comparables_count":  len(comparables),
    }

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=LETTER)
    width, height = LETTER

    # Watermark
    c.saveState()
    c.setFillColor(HexColor("#EAEAEA"))
    c.setFont("Helvetica-Bold", 80)
    c.drawCentredString(width / 2, height / 2 - 40, "URAR")
    c.restoreState()

    # Title
    c.setFont("Helvetica-Bold", 16)
    c.drawString(40, height - 50, "Uniform Residential Appraisal Report")
    c.setFont("Helvetica", 9)
    c.drawString(40, height - 64, "Form 1004 (Fannie Mae) / 70 (Freddie Mac)")
    c.drawRightString(width - 40, height - 50, "File No. 0001")

    # Subject section
    y = height - 100
    c.setFont("Helvetica-Bold", 11)
    c.drawString(40, y, "SUBJECT")
    c.setFont("Helvetica-Bold", 9)
    c.drawString(40, y - 18, "Property Address")
    c.setFont("Helvetica", 10)
    for i, line in enumerate(property_address.split("\n")):
        c.drawString(40, y - 32 - i * 12, line)

    c.setFont("Helvetica-Bold", 9)
    c.drawString(300, y - 18, "Legal Description")
    c.setFont("Helvetica", 10)
    c.drawString(300, y - 32, legal_description or "Lot 12, Block 4")

    c.setFont("Helvetica-Bold", 9)
    c.drawString(40, y - 70, "Property Type")
    c.setFont("Helvetica", 10)
    c.drawString(40, y - 84, property_type)

    c.setFont("Helvetica-Bold", 9)
    c.drawString(180, y - 70, "Year Built")
    c.setFont("Helvetica", 10)
    c.drawString(180, y - 84, str(year_built))

    c.setFont("Helvetica-Bold", 9)
    c.drawString(280, y - 70, "Gross Living Area (SqFt)")
    c.setFont("Helvetica", 10)
    c.drawString(280, y - 84, f"{sqft:,}")

    c.setFont("Helvetica-Bold", 9)
    c.drawString(440, y - 70, "Condition")
    c.setFont("Helvetica", 11)
    c.drawString(440, y - 84, condition_rating)

    # Comparables
    cy = y - 130
    c.setFont("Helvetica-Bold", 11)
    c.drawString(40, cy, "COMPARABLE SALES")
    c.setFont("Helvetica-Bold", 9)
    c.drawString(40, cy - 18, "Comp")
    c.drawString(80, cy - 18, "Address")
    c.drawString(320, cy - 18, "Sale Price")
    c.drawString(420, cy - 18, "SqFt")
    c.setFont("Helvetica", 10)
    for i, comp in enumerate(comparables):
        row_y = cy - 34 - i * 14
        c.drawString(40, row_y, f"#{i+1}")
        c.drawString(80, row_y, comp.get("address", ""))
        c.drawString(320, row_y, _money(float(comp.get("sale_price", 0))))
        c.drawString(420, row_y, f"{int(comp.get('sqft', 0)):,}")

    # Approaches summary
    ay = cy - 90 - len(comparables) * 14
    c.setFont("Helvetica-Bold", 11)
    c.drawString(40, ay, "RECONCILIATION OF VALUE")
    c.setFont("Helvetica", 10)
    c.drawString(40, ay - 16, "Sales Comparison Approach: indicated")
    c.drawString(40, ay - 30, "Cost Approach: supportive")
    c.drawString(40, ay - 44, "Income Approach: not applicable")

    # The big number — Opinion of Value
    oy = ay - 90
    c.setStrokeColor(HexColor("#222222"))
    c.rect(40, oy - 30, width - 80, 50, stroke=1, fill=0)
    c.setFont("Helvetica-Bold", 11)
    c.drawString(56, oy + 6, "Opinion of Value")
    c.setFont("Helvetica-Bold", 14)
    c.drawString(56, oy - 14, _money(float(appraised_value)))
    c.setFont("Helvetica-Bold", 9)
    c.drawString(360, oy + 6, "Appraised Value")
    c.setFont("Helvetica", 11)
    c.drawString(360, oy - 14, _money(float(appraised_value)))
    c.setFont("Helvetica-Bold", 9)
    c.drawRightString(width - 56, oy + 6, "Effective Date")
    c.setFont("Helvetica", 11)
    c.drawRightString(width - 56, oy - 14, effective_date)

    # Appraiser block
    sy = oy - 70
    c.setFont("Helvetica-Bold", 9)
    c.drawString(40, sy, "Appraiser")
    c.setFont("Helvetica", 10)
    c.drawString(40, sy - 14, appraiser_name)
    c.drawString(40, sy - 28, f"License: {appraiser_license}")
    c.line(40, sy - 44, 280, sy - 44)
    c.setFont("Helvetica-Oblique", 8)
    c.drawString(40, sy - 56, "Signature")

    # Footer
    c.setFont("Helvetica-Oblique", 8)
    c.drawCentredString(
        width / 2, 30,
        "Simulated URAR appraisal generated by EDMS Simulator for testing.",
    )

    c.showPage()
    c.save()
    return buf.getvalue(), metadata
