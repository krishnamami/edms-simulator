"""Synthetic CreditProfile generator.

In production this would be replaced by a real bureau-pull adapter; for the
simulator we generate plausible profiles deterministically by credit band.
"""
import random
from datetime import date, timedelta


class CreditAssembler:
    def generate_synthetic(
        self, applicant_id: str, loan_data: dict
    ) -> dict:
        band = loan_data.get("credit_band", "near-prime")
        ranges = {
            "prime": (740, 820),
            "near-prime": (680, 739),
            "subprime": (620, 679),
            "deep-subprime": (580, 619),
        }
        lo, hi = ranges.get(band, (680, 739))
        base = random.randint(lo, hi)
        scores = sorted(
            [max(300, min(850, base + random.randint(-8, 8))) for _ in range(3)]
        )
        mid = scores[1]
        obs = self._generate_obligations(loan_data)
        report_date = date.today()
        expiry = report_date + timedelta(days=120)
        return {
            "applicant_id": applicant_id,
            "experian_score": scores[2],
            "equifax_score": scores[0],
            "transunion_score": scores[1],
            "mid_score": mid,
            "credit_band": band,
            "open_tradelines": random.randint(4, 15),
            "revolving_utilization": round(random.uniform(0.10, 0.45), 2),
            "monthly_obligations": obs,
            "total_monthly_obligations": round(
                sum(o["monthly_payment"] for o in obs), 2
            ),
            "derogatory_marks": (
                0 if band == "prime" else random.randint(0, 2)
            ),
            "active_bankruptcy": False,
            "foreclosure_last_36mo": False,
            "late_30day": (
                0 if band == "prime" else random.randint(0, 1)
            ),
            "late_60day": 0,
            "late_90day": 0,
            "hard_inquiries_12mo": random.randint(1, 4),
            "report_date": report_date.isoformat(),
            "is_current": True,
            "expiry_date": expiry.isoformat(),
            "pull_type": "hard",
        }

    def _generate_obligations(self, loan_data: dict) -> list:
        obs: list[dict] = []
        if random.random() > 0.4:
            obs.append(
                {
                    "type": "car",
                    "creditor": "Auto Finance",
                    "monthly_payment": random.randint(250, 600),
                }
            )
        if random.random() > 0.5:
            obs.append(
                {
                    "type": "student",
                    "creditor": "Student Loans",
                    "monthly_payment": random.randint(150, 450),
                }
            )
        obs.append(
            {
                "type": "credit_card",
                "creditor": "Chase",
                "monthly_payment": random.randint(50, 300),
            }
        )
        return obs
