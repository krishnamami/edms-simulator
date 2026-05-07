"""CreditProfile assembler.

Prefers a real ``CREDIT_REPORT`` document when one is in ``document_index``
for the applicant; falls back to a synthetic profile so downstream income +
context assembly never blocks on a missing bureau pull.
"""
import random
from datetime import date, timedelta
from typing import Optional


class CreditAssembler:
    def assemble(
        self,
        applicant_id: str,
        loan_data: dict,
        docs: Optional[list] = None,
    ) -> dict:
        """Build a CreditProfile from the highest-confidence CREDIT_REPORT
        in ``docs`` if present, otherwise return a synthetic profile.

        Callers without document context (legacy, demo-warmup) can omit
        ``docs`` and get the synthetic path.
        """
        credit_doc = self._pick_credit_doc(docs)
        if credit_doc:
            return self._from_document(applicant_id, credit_doc, loan_data)
        return self.generate_synthetic(applicant_id, loan_data)

    @staticmethod
    def _pick_credit_doc(docs: Optional[list]) -> Optional[dict]:
        if not docs:
            return None
        candidates = [
            d for d in docs
            if d.get("document_type") == "CREDIT_REPORT"
            and d.get("mid_score") is not None
        ]
        if not candidates:
            return None
        # Highest confidence wins; tiebreak on most recent report_date.
        candidates.sort(
            key=lambda d: (
                float(d.get("confidence_score") or 0.0),
                d.get("report_date") or "",
            ),
            reverse=True,
        )
        return candidates[0]

    @staticmethod
    def _normalize_obligations(raw: list) -> list:
        out: list[dict] = []
        for o in raw or []:
            payment = o.get("monthly_payment")
            if payment is None:
                payment = o.get("payment", 0)
            out.append({
                "type":            o.get("type"),
                "creditor":        o.get("creditor"),
                "monthly_payment": payment,
            })
        return out

    def _from_document(
        self, applicant_id: str, doc: dict, loan_data: dict
    ) -> dict:
        obs = self._normalize_obligations(
            doc.get("monthly_obligations_detail") or []
        )
        # The doc may carry a precomputed total at "monthly_obligations"
        # (number) OR a list of obligations there. Tolerate both.
        raw_total = doc.get("monthly_obligations")
        if isinstance(raw_total, list):
            obs = obs or self._normalize_obligations(raw_total)
            total = round(sum((o.get("monthly_payment") or 0) for o in obs), 2)
        elif raw_total is not None:
            total = float(raw_total)
        else:
            total = round(sum((o.get("monthly_payment") or 0) for o in obs), 2)

        report_date_str = doc.get("report_date") or date.today().isoformat()
        try:
            report_date = date.fromisoformat(str(report_date_str))
        except ValueError:
            report_date = date.today()
        expiry = report_date + timedelta(days=120)

        band = (
            doc.get("credit_band")
            or loan_data.get("credit_band", "near-prime")
        )
        return {
            "applicant_id":              applicant_id,
            "experian_score":            doc.get("experian_score"),
            "equifax_score":             doc.get("equifax_score"),
            "transunion_score":          doc.get("transunion_score"),
            "mid_score":                 doc.get("mid_score"),
            "credit_band":               band,
            "open_tradelines":           doc.get("open_tradelines", 0),
            "revolving_utilization":     doc.get("revolving_utilization"),
            "monthly_obligations":       obs,
            "total_monthly_obligations": round(total, 2),
            "derogatory_marks":          doc.get("derogatory_marks", 0),
            "active_bankruptcy":         doc.get("active_bankruptcy", False),
            "foreclosure_last_36mo":     doc.get("foreclosure_last_36mo", False),
            "late_30day":                doc.get("late_30day", 0),
            "late_60day":                doc.get("late_60day", 0),
            "late_90day":                doc.get("late_90day", 0),
            "hard_inquiries_12mo":       doc.get("hard_inquiries_12mo", 0),
            "report_date":               report_date.isoformat(),
            "is_current":                True,
            "expiry_date":               expiry.isoformat(),
            "pull_type":                 doc.get("pull_type", "hard"),
        }

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
