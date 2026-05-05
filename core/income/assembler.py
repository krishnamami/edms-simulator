"""IncomeAssembler — orchestrates per-borrower income calculations and obligations."""
import hashlib
from datetime import datetime
from typing import Optional

from core.income.rules import (
    calculate_asset_depletion,
    calculate_military,
    calculate_rental,
    calculate_retirement_ssa,
    calculate_self_employed,
    calculate_w2_salaried,
)
from core.income.sources import IncomeProfile, MonthlyDebtObligation


class IncomeAssembler:
    def assemble(
        self,
        primary_docs: list,
        co_borrower_docs: Optional[list],
        primary_credit: Optional[dict],
        co_borrower_credit: Optional[dict],
        application_id: str,
        applicant_id: str,
        co_applicant_id: Optional[str] = None,
    ) -> IncomeProfile:
        primary = self._assemble_borrower(
            primary_docs, applicant_id, "primary"
        )
        co = (
            self._assemble_borrower(
                co_borrower_docs, co_applicant_id, "co_borrower"
            )
            if co_applicant_id and co_borrower_docs
            else None
        )

        obligations = self._extract_obligations(primary_credit)
        co_obligations = (
            self._extract_obligations(co_borrower_credit)
            if co_borrower_credit
            else []
        )
        all_obs = obligations + co_obligations
        total_obs = round(
            sum(o.monthly_payment for o in all_obs if not o.omitted), 2
        )

        primary_mid = (primary_credit or {}).get("mid_score", 620)
        co_mid = (
            (co_borrower_credit or {}).get("mid_score", 999)
            if co_borrower_credit
            else 999
        )
        q_score = min(primary_mid, co_mid)
        combined = round(
            primary["qualifying_monthly"]
            + (co["qualifying_monthly"] if co else 0),
            2,
        )

        all_doc_ids = [
            d.get("document_id", "")
            for d in (primary_docs + (co_borrower_docs or []))
        ]
        lineage_hash = hashlib.sha256(
            ",".join(sorted(all_doc_ids)).encode()
        ).hexdigest()[:16]

        warnings = primary["warnings"] + (co["warnings"] if co else [])
        needs_review = any(
            s.get("confidence", 1) < 0.80 and not s.get("excluded")
            for s in primary["sources"]
        )

        return IncomeProfile(
            applicant_id=applicant_id,
            application_id=application_id,
            assembled_at=datetime.utcnow().isoformat(),
            primary_borrower=primary,
            co_borrower=co,
            combined_qualifying_monthly=combined,
            qualifying_score_used=q_score,
            monthly_debt_obligations=all_obs,
            total_monthly_obligations=total_obs,
            dti_inputs_ready=True,
            assembly_warnings=warnings,
            requires_human_review=needs_review,
            lineage_hash=lineage_hash,
        )

    def _assemble_borrower(
        self, docs: list, borrower_id: str, role: str
    ) -> dict:
        if not docs:
            return {
                "borrower_id": borrower_id,
                "role": role,
                "qualifying_monthly": 0,
                "overall_confidence": 0,
                "sources": [],
                "warnings": ["No documents found"],
            }
        w2_docs = [d for d in docs if "W2" in d.get("document_type", "")]
        paystubs = [
            d for d in docs if "PAYSTUB" in d.get("document_type", "")
        ]
        tax_returns = [
            d for d in docs if "TAX_RETURN" in d.get("document_type", "")
        ]
        ssa_letters = [
            d for d in docs if d.get("document_type") == "SSA_AWARD_LETTER"
        ]
        assets = [
            d
            for d in docs
            if d.get("document_type")
            in [
                "BANK_STATEMENT_M1",
                "BANK_STATEMENT_M2",
                "BANK_STATEMENT_M3",
                "ASSET_STATEMENT",
            ]
        ]
        les_docs = [d for d in docs if d.get("document_type") == "LES"]
        schedule_e = next(
            (d for d in docs if d.get("document_type") == "SCHEDULE_E"), None
        )
        leases = [
            d for d in docs if d.get("document_type") == "LEASE_AGREEMENT"
        ]

        sources: list[dict] = []
        warnings: list[str] = []

        if w2_docs:
            sources.append(
                calculate_w2_salaried(
                    w2_docs, paystubs, borrower_id
                ).model_dump()
            )
        if tax_returns and any(
            d.get("has_schedule_c") for d in tax_returns
        ):
            sources.append(
                calculate_self_employed(tax_returns, borrower_id).model_dump()
            )
        if schedule_e:
            sources.append(
                calculate_rental(
                    schedule_e, leases, borrower_id
                ).model_dump()
            )
        if ssa_letters:
            sources.append(
                calculate_retirement_ssa(
                    ssa_letters[0], borrower_id
                ).model_dump()
            )
        if assets and not w2_docs:
            sources.append(
                calculate_asset_depletion(
                    assets, 65, borrower_id
                ).model_dump()
            )
        if les_docs:
            sources.append(
                calculate_military(les_docs[0], borrower_id).model_dump()
            )

        active_sources = [s for s in sources if not s.get("excluded")]
        qualifying_monthly = round(
            sum(s["qualifying_monthly"] for s in active_sources), 2
        )
        overall_confidence = min(
            (s["confidence"] for s in active_sources), default=0.0
        )

        return {
            "borrower_id": borrower_id,
            "role": role,
            "qualifying_monthly": qualifying_monthly,
            "overall_confidence": overall_confidence,
            "sources": sources,
            "warnings": warnings,
        }

    def _extract_obligations(
        self, credit_profile: Optional[dict]
    ) -> list[MonthlyDebtObligation]:
        if not credit_profile:
            return []
        return [
            MonthlyDebtObligation(
                obligation_type=item.get("type", "other"),
                creditor_name=item.get("creditor"),
                monthly_payment=float(item.get("monthly_payment", 0)),
                outstanding_balance=item.get("balance"),
                months_remaining=item.get("months_remaining"),
                omitted=item.get("omitted", False),
                omission_reason=item.get("omission_reason"),
            )
            for item in credit_profile.get("monthly_obligations", [])
        ]
