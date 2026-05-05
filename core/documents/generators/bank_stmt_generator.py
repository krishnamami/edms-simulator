"""3-month bank statement PDF generator.

Generates a deterministic transaction stream (seeded RNG) so that tests can
round-trip extracted values back to the metadata. Returns (pdf_bytes, metadata).
"""
from __future__ import annotations

import io
import random
from datetime import date, timedelta
from typing import Optional

from reportlab.lib.colors import HexColor
from reportlab.lib.pagesizes import LETTER
from reportlab.pdfgen import canvas

# Pool of plausible transactions; debits are negative, credits positive
_TXN_POOL = [
    ("Direct Deposit - Payroll",     +1850.00),
    ("Grocery Outlet",               -84.32),
    ("AMZN Marketplace",             -42.99),
    ("Shell Gas Station",            -52.10),
    ("Trader Joe's",                 -67.45),
    ("Spotify Subscription",         -10.99),
    ("Netflix",                      -15.99),
    ("Comcast Internet",             -89.95),
    ("PG&E Utilities",               -142.30),
    ("ATM Withdrawal",               -200.00),
    ("Starbucks",                    -8.45),
    ("Restaurant Charge",            -56.20),
    ("Apple iCloud",                 -2.99),
    ("Pharmacy",                     -38.15),
    ("Costco Wholesale",             -184.66),
    ("Uber Ride",                    -22.75),
    ("Mortgage Payment",             -1980.00),
    ("Car Insurance",                -147.00),
    ("Gym Membership",               -49.99),
    ("Online Transfer In",           +500.00),
    ("Cellular Bill",                -85.00),
    ("Bookstore",                    -35.40),
    ("Movie Theater",                -32.00),
    ("Department Store",             -119.40),
    ("Refund - Amazon",              +28.50),
]


def _mask_account(acct: str) -> str:
    digits = "".join(c for c in acct if c.isdigit())
    return f"****{digits[-4:]}" if len(digits) >= 4 else f"****{acct}"


def _money(v: float) -> str:
    sign = "-" if v < 0 else ""
    return f"{sign}${abs(v):,.2f}"


def _generate_month(start: date, opening: float, txns_per_month: int, rng: random.Random):
    end = (start.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    days_in_month = (end - start).days + 1
    transactions = []
    balance = opening
    chosen_days = sorted(rng.sample(range(1, days_in_month + 1), min(txns_per_month, days_in_month)))
    for day in chosen_days:
        desc, base_amt = rng.choice(_TXN_POOL)
        # Jitter the amount by ±15% so months don't look identical
        amount = round(base_amt * (1 + rng.uniform(-0.15, 0.15)), 2)
        balance = round(balance + amount, 2)
        transactions.append({
            "date": (start + timedelta(days=day - 1)).isoformat(),
            "description": desc,
            "amount": amount,
            "balance_after": balance,
        })
    return {
        "year_month": start.strftime("%Y-%m"),
        "opening_balance": opening,
        "closing_balance": balance,
        "transactions": transactions,
    }


def generate_bank_statement(
    *,
    bank_name: str,
    account_holder: str,
    account_number: str,
    statement_end_date: date,
    starting_balance: float = 5_000.00,
    txns_per_month: int = 25,
    seed: Optional[int] = None,
) -> tuple[bytes, dict]:
    rng = random.Random(seed if seed is not None else 17)

    # Build last 3 months ending in statement_end_date's month
    months = []
    cur_start = statement_end_date.replace(day=1)
    starts: list[date] = []
    for _ in range(3):
        starts.append(cur_start)
        prev_month_end = cur_start - timedelta(days=1)
        cur_start = prev_month_end.replace(day=1)
    starts.reverse()

    opening = starting_balance
    for s in starts:
        m = _generate_month(s, opening, txns_per_month, rng)
        months.append(m)
        opening = m["closing_balance"]

    metadata = {
        "document_type": "BANK_STATEMENT",
        "bank_name": bank_name,
        "account_holder": account_holder,
        "account_number_masked": _mask_account(account_number),
        "statement_end_date": statement_end_date.isoformat(),
        "starting_balance": float(starting_balance),
        "ending_balance": float(months[-1]["closing_balance"]),
        "months": months,
    }

    buf = io.BytesIO()
    c = canvas.Canvas(buf, pagesize=LETTER)
    w, h = LETTER

    for page_idx, m in enumerate(months):
        # Header
        c.setFont("Helvetica-Bold", 18)
        c.drawString(40, h - 50, bank_name)
        c.setFont("Helvetica-Bold", 12)
        c.drawString(40, h - 70, "Account Summary")
        c.setFont("Helvetica", 10)
        c.drawRightString(w - 40, h - 50, f"Statement: {m['year_month']}")
        c.drawRightString(w - 40, h - 64, f"Account: {_mask_account(account_number)}")
        c.drawRightString(w - 40, h - 78, f"Holder: {account_holder}")

        # Summary box
        c.setStrokeColor(HexColor("#444444"))
        c.rect(40, h - 130, w - 80, 36, stroke=1, fill=0)
        c.setFont("Helvetica", 10)
        c.drawString(50, h - 110, f"Opening Balance: {_money(m['opening_balance'])}")
        c.drawString(280, h - 110, f"Closing Balance: {_money(m['closing_balance'])}")
        c.drawString(50, h - 124, f"Transactions: {len(m['transactions'])}")

        # Transactions table
        ty = h - 160
        c.setFont("Helvetica-Bold", 10)
        c.drawString(40, ty, "Date")
        c.drawString(120, ty, "Description")
        c.drawRightString(440, ty, "Amount")
        c.drawRightString(540, ty, "Balance")
        c.line(40, ty - 4, w - 40, ty - 4)

        c.setFont("Helvetica", 9)
        for i, t in enumerate(m["transactions"]):
            y = ty - 18 - i * 14
            if y < 60:
                break
            c.drawString(40, y, t["date"])
            c.drawString(120, y, t["description"][:40])
            c.drawRightString(440, y, _money(t["amount"]))
            c.drawRightString(540, y, _money(t["balance_after"]))

        # Footer
        c.setFont("Helvetica-Oblique", 8)
        c.drawCentredString(
            w / 2, 40,
            "Simulated bank statement generated by EDMS Simulator for testing.",
        )

        if page_idx < len(months) - 1:
            c.showPage()

    c.showPage()
    c.save()
    return buf.getvalue(), metadata
