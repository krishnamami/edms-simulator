"""CreditProfile assembler.

Prefers a real ``CREDIT_REPORT`` document when one is in ``document_index``
for the applicant; falls back to a synthetic profile so downstream income +
context assembly never blocks on a missing bureau pull.

Read pattern: every credit field is pulled from ``doc["extracted_fields"]``
(the Postgres jsonb column). Only the columnar metadata —
``document_type`` — is read from the top level of the row dict.
"""
import json
import logging
import random
from datetime import date, timedelta
from typing import Optional

logger = logging.getLogger(__name__)


class CreditAssembler:
    def __init__(self, postgres_store=None):
        # Optional so existing callers that don't have the store
        # (smoke_aggregation, some tests) can still construct one.
        self.postgres_store = postgres_store

    async def assemble(
        self,
        applicant_id: str,
        loan_data: dict,
        postgres_store=None,
        docs: Optional[list] = None,
    ) -> dict:
        """Build a CreditProfile from a real ``CREDIT_REPORT`` indexed for
        ``applicant_id`` if one is present, otherwise return a synthetic
        profile.

        Doc lookup order:
          1. ``postgres_store`` arg's ``get_documents_for_applicant``
             (preferred — gives us the canonical pg-row shape with
             ``extracted_fields`` nested as jsonb)
          2. ``self.postgres_store`` if set on the instance
          3. ``docs`` arg as a last-resort fallback for callers without a
             store (smoke tests). These docs must already carry
             ``extracted_fields`` nested.
        """
        pg = postgres_store or self.postgres_store
        if pg is not None:
            try:
                docs = await pg.get_documents_for_applicant(applicant_id)
            except Exception as exc:
                logger.warning(
                    "credit_doc_fetch_failed",
                    extra={"applicant_id": applicant_id, "error": str(exc)},
                )
                docs = docs or []

        credit_doc = next(
            (
                d for d in (docs or [])
                if d.get("document_type") == "CREDIT_REPORT"
            ),
            None,
        )
        if not credit_doc:
            return self.generate_synthetic(applicant_id, loan_data)

        fields = self._read_extracted_fields(credit_doc)
        mid_score = fields.get("mid_score")

        logger.info(
            "credit_doc_fields_read",
            extra={
                "mid_score_found": mid_score,
                "source": "extracted_fields",
            },
        )

        if mid_score is None:
            # Doc exists but has no usable score — fall back rather than
            # ship a profile with mid_score=None that downstream readers
            # would crash on.
            return self.generate_synthetic(applicant_id, loan_data)

        return self._from_extracted_fields(applicant_id, fields, loan_data)

    @staticmethod
    def _read_extracted_fields(doc: dict) -> dict:
        """Return the ``extracted_fields`` jsonb dict, decoding if asyncpg
        handed it back as a JSON string. Never reads top-level — the
        top-level on a pg row only carries columnar metadata
        (document_type, applicant_id, ...), not the extracted data."""
        fields = doc.get("extracted_fields") or {}
        if isinstance(fields, str):
            try:
                fields = json.loads(fields)
            except Exception:
                fields = {}
        if not isinstance(fields, dict):
            fields = {}
        return fields

    @staticmethod
    def _normalize_obligations(raw) -> list:
        if isinstance(raw, dict):
            raw = [raw]
        out: list[dict] = []
        for o in raw or []:
            if not isinstance(o, dict):
                continue
            payment = o.get("monthly_payment")
            if payment is None:
                payment = o.get("payment", 0)
            out.append({
                "type":            o.get("type"),
                "creditor":        o.get("creditor"),
                "monthly_payment": payment,
            })
        return out

    def _from_extracted_fields(
        self, applicant_id: str, fields: dict, loan_data: dict
    ) -> dict:
        # Obligations may live under either key in the jsonb.
        detail = fields.get("monthly_obligations_detail")
        raw_total = fields.get("monthly_obligations")

        if isinstance(detail, list):
            obs = self._normalize_obligations(detail)
        elif isinstance(raw_total, list):
            obs = self._normalize_obligations(raw_total)
        else:
            obs = []

        if isinstance(raw_total, (int, float)):
            total = float(raw_total)
        else:
            total = round(sum((o.get("monthly_payment") or 0) for o in obs), 2)

        report_date_str = fields.get("report_date") or date.today().isoformat()
        try:
            report_date = date.fromisoformat(str(report_date_str))
        except ValueError:
            report_date = date.today()
        expiry = report_date + timedelta(days=120)

        band = (
            fields.get("credit_band")
            or loan_data.get("credit_band", "near-prime")
        )
        return {
            "applicant_id":              applicant_id,
            "experian_score":            fields.get("experian_score"),
            "equifax_score":             fields.get("equifax_score"),
            "transunion_score":          fields.get("transunion_score"),
            "mid_score":                 int(fields["mid_score"]),
            "credit_band":               band,
            "open_tradelines":           fields.get("open_tradelines", 0),
            "revolving_utilization":     fields.get("revolving_utilization"),
            "monthly_obligations":       obs,
            "total_monthly_obligations": round(total, 2),
            "derogatory_marks":          fields.get("derogatory_marks", 0),
            "active_bankruptcy":         fields.get("active_bankruptcy", False),
            "foreclosure_last_36mo":     fields.get("foreclosure_last_36mo", False),
            "late_30day":                fields.get("late_30day", 0),
            "late_60day":                fields.get("late_60day", 0),
            "late_90day":                fields.get("late_90day", 0),
            "hard_inquiries_12mo":       fields.get("hard_inquiries_12mo", 0),
            "report_date":               report_date.isoformat(),
            "is_current":                True,
            "expiry_date":               expiry.isoformat(),
            "pull_type":                 fields.get("pull_type", "hard"),
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
