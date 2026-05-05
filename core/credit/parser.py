"""Tradeline parser — normalizes raw bureau pulls into our CreditProfile shape.

In production this layer ingests Experian/Equifax/TransUnion XML/JSON pulls.
For the simulator, we accept already-normalized dicts and pass them through
with light validation.
"""
from typing import Optional


class CreditPullParseError(ValueError):
    pass


class CreditParser:
    REQUIRED_TOP_LEVEL = {"applicant_id", "mid_score", "credit_band"}
    BANDS = {"prime", "near-prime", "subprime", "deep-subprime"}

    def parse(self, raw: dict) -> dict:
        missing = self.REQUIRED_TOP_LEVEL - set(raw.keys())
        if missing:
            raise CreditPullParseError(f"Missing keys: {missing}")
        if raw["credit_band"] not in self.BANDS:
            raise CreditPullParseError(
                f"Invalid credit_band: {raw['credit_band']}"
            )
        normalized = dict(raw)
        normalized["monthly_obligations"] = [
            self._normalize_obligation(o)
            for o in raw.get("monthly_obligations", [])
        ]
        normalized["total_monthly_obligations"] = round(
            sum(o["monthly_payment"] for o in normalized["monthly_obligations"]),
            2,
        )
        return normalized

    @staticmethod
    def _normalize_obligation(obligation: dict) -> dict:
        return {
            "type": obligation.get("type", "other"),
            "creditor": obligation.get("creditor", "Unknown"),
            "monthly_payment": float(obligation.get("monthly_payment", 0)),
            "balance": obligation.get("balance"),
            "months_remaining": obligation.get("months_remaining"),
            "omitted": obligation.get("omitted", False),
            "omission_reason": obligation.get("omission_reason"),
        }

    @staticmethod
    def derive_band(mid_score: int) -> str:
        if mid_score >= 740:
            return "prime"
        if mid_score >= 680:
            return "near-prime"
        if mid_score >= 620:
            return "subprime"
        return "deep-subprime"

    @staticmethod
    def expiry_for_band(band: str) -> Optional[str]:
        # Conservative defaults; production overrides via product policy.
        return None
