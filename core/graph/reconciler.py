"""DocumentReconciler — writes typed graph edges between documents.

Compares each new document against existing documents for the same applicant
and emits relationships (confirms / corroborates / contradicts). Numeric
divergence uses the same NUMERIC_CONFLICT_THRESHOLD as ConfidenceResolver,
so within-event and across-document conflict rules stay aligned.

Per-pair thresholds in :data:`FIELD_CONFLICT_THRESHOLDS` override the
default for fields that need tighter (wages vs IRS) or looser (appraisal
vs tax assessment) tolerance.
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import date
from typing import Optional

from core.graph.models import DocumentRelationship, RelationshipType
from core.ingestion.confidence import NUMERIC_CONFLICT_THRESHOLD

logger = logging.getLogger(__name__)


def _deterministic_relationship_id(
    applicant_id: str,
    source_doc_id: str,
    target_doc_id: str,
    field_label: str,
    rel_type: str,
) -> str:
    """Stable id so re-running reconciliation upserts a row instead of
    piling N copies of the same logical edge into document_relationships
    every time _persist_and_reconcile_documents runs."""
    key = "|".join([
        applicant_id, source_doc_id, target_doc_id, field_label, rel_type,
    ])
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:32]


# Which field pairs to compare for each document type combination.
# Format: (type_a, type_b) -> [(field_in_a, field_in_b, weight)]
COMPARISON_MAP: dict[tuple, list[tuple]] = {

    # ── INCOME CROSS-CHECKS ──────────────────────────────────────────────

    # W2 vs IRS Transcript — most critical edge. Transcript is IRS ground
    # truth; any difference > 5% is a fraud flag. Two field tuples on
    # ``wages`` because the IRS extractor in this repo emits
    # ``wages_salaries`` while older / external extractors emit
    # ``wages_tips_compensation`` — both should agree, both fire when
    # both are present.
    ("W2_CURRENT", "IRS_TRANSCRIPT"): [
        ("box1_wages",    "wages_tips_compensation", 1.0),
        ("box1_wages",    "wages_salaries",          1.0),
        ("employer_name", "employer_name",           0.7),
        ("tax_year",      "tax_year",                0.5),
    ],
    ("W2_PRIOR", "IRS_TRANSCRIPT"): [
        ("box1_wages", "wages_tips_compensation", 0.9),
        ("tax_year",   "tax_year",                0.5),
    ],

    # W2 vs Pay Stub — confirms employment continuity.
    ("W2_CURRENT", "PAYSTUB_CURRENT"): [
        ("employer_name", "employer_name",  0.8),
        ("box1_wages",    "annualized_ytd", 0.9),
    ],
    ("W2_PRIOR", "PAYSTUB_PRIOR"): [
        ("employer_name", "employer_name",  0.7),
        ("box1_wages",    "annualized_ytd", 0.7),
    ],

    # W2 current vs prior year — income history consistency.
    ("W2_CURRENT", "W2_PRIOR"): [
        ("employer_name", "employer_name", 0.8),
        ("box1_wages",    "box1_wages",    0.6),
    ],

    # W2 vs Tax Return — wages reconcile to 1040. ``wages_line1`` is the
    # 1040 extractor's name for line 1 wages; ``wages_salaries`` is the
    # legacy alias.
    ("W2_CURRENT", "TAX_RETURN_1040_CURRENT"): [
        ("box1_wages", "wages_salaries", 0.9),
        ("box1_wages", "wages_line1",    0.9),
        ("tax_year",   "tax_year",       0.5),
    ],
    ("W2_PRIOR", "TAX_RETURN_1040_PRIOR"): [
        ("box1_wages", "wages_salaries", 0.8),
    ],

    # W2 vs Bank Statement — net deposits should reflect wages.
    ("W2_CURRENT", "BANK_STATEMENT_M1"): [
        ("box1_wages", "annual_payroll_deposits", 0.6),
    ],

    # W2 vs 1099 — usually mutually exclusive but happens in the same year.
    ("W2_CURRENT", "1099_NEC"): [
        ("box1_wages", "amount", 1.0),
    ],

    # Pay Stub current vs prior month.
    ("PAYSTUB_CURRENT", "PAYSTUB_PRIOR"): [
        ("employer_name", "employer_name", 0.9),
        ("gross_pay",     "gross_pay",     0.5),
    ],

    # IRS Transcript vs Tax Return — should be identical.
    ("IRS_TRANSCRIPT", "TAX_RETURN_1040_CURRENT"): [
        ("wages_tips_compensation", "wages_salaries", 1.0),
        ("agi",                     "agi",            1.0),
        ("tax_year",                "tax_year",       0.5),
    ],

    # Schedule C / E flow into 1040. Field-name pairs cover both the
    # legacy field names (business_income / rental_income) and the
    # current extractors' names (schedule_c_income / schedule_e_income,
    # net_profit / net_rental_income).
    ("SCHEDULE_C", "TAX_RETURN_1040_CURRENT"): [
        ("net_profit", "business_income",    0.9),
        ("net_profit", "schedule_c_income",  1.0),
        ("tax_year",   "tax_year",           0.5),
    ],
    ("SCHEDULE_E", "TAX_RETURN_1040_CURRENT"): [
        ("net_income",         "rental_income",       0.9),
        ("net_rental_income",  "schedule_e_income",   1.0),
        ("tax_year",           "tax_year",            0.5),
    ],

    # 1099 vs Tax Return — both legacy and current extractor field names.
    ("1099_NEC", "TAX_RETURN_1040_CURRENT"): [
        ("amount_1099",                "self_employment_income", 0.8),
        ("nonemployee_compensation",   "other_income",           0.7),
        ("tax_year",                   "tax_year",               0.5),
    ],

    # K-1 vs Tax Return — partnership / S-corp pass-through income.
    ("K1_PARTNERSHIP", "TAX_RETURN_1040_CURRENT"): [
        ("ordinary_income", "ordinary_income",   0.9),
        ("ordinary_income", "schedule_e_income", 0.6),  # falls into Sch E line
        ("tax_year",        "tax_year",          0.5),
    ],

    # Cross-borrower W2 (joint application). Different borrowers, same
    # loan year → tax_year should match. Don't compare wages or employer.
    ("W2_CURRENT", "W2_CURRENT"): [
        ("tax_year", "tax_year", 0.5),
    ],

    # ── EMPLOYMENT CROSS-CHECKS ──────────────────────────────────────────

    # VOE vs W2 — verifies still employed at the same employer.
    ("VOE_TWN", "W2_CURRENT"): [
        ("employer_name",     "employer_name", 0.9),
        ("base_pay_annual",   "box1_wages",    0.8),
        ("employment_status", "tax_year",      0.4),
    ],
    ("VOE_EQUIFAX", "W2_CURRENT"): [
        ("employer_name", "employer_name", 0.9),
        ("annual_salary", "box1_wages",    0.8),
    ],
    ("VOE_TWN", "PAYSTUB_CURRENT"): [
        ("employer_name",   "employer_name",  0.9),
        ("base_pay_annual", "annualized_ytd", 0.7),
    ],

    # ── PROPERTY CROSS-CHECKS ────────────────────────────────────────────

    # Appraisal vs Purchase Agreement — value-gap detection.
    ("APPRAISAL_URAR", "PURCHASE_AGREEMENT"): [
        ("appraised_value", "purchase_price", 1.0),
    ],

    # Appraisal vs AVM — automated validation. Two field tuples on the
    # AVM side because vendors disagree on the name (``estimated_value``
    # vs ``avm_value``).
    ("APPRAISAL_URAR", "AVM_REPORT"): [
        ("appraised_value", "estimated_value", 0.8),
        ("appraised_value", "avm_value",       0.8),
    ],

    # Appraisal vs market-conditions addendum (1004MC). Wide tolerance
    # — median sale price for the neighborhood is rarely close to the
    # subject's appraisal but should be in the same order of magnitude.
    ("APPRAISAL_URAR", "FORM_1004MC"): [
        ("appraised_value", "median_sale_price", 0.5),
    ],

    # Appraisal vs property tax — assessed is typically 60-85% of market.
    ("APPRAISAL_URAR", "PROPERTY_TAX_BILL"): [
        ("appraised_value", "assessed_value", 0.4),
    ],
    ("APPRAISAL_URAR", "PROPERTY_TAX_TRANSCRIPT"): [
        ("appraised_value", "assessed_value", 0.4),
    ],

    # Appraisal update vs original.
    ("APPRAISAL_UPDATE", "APPRAISAL_URAR"): [
        ("updated_value",    "appraised_value",  0.9),
        ("property_address", "property_address", 0.8),
    ],

    # HOI Binder vs Declarations.
    ("HOI_BINDER", "HOI_DECLARATIONS"): [
        ("annual_premium",    "annual_premium",    0.9),
        ("dwelling_coverage", "dwelling_coverage", 0.8),
        ("carrier_name",      "carrier_name",      0.8),
    ],

    # Flood cert vs flood insurance — zone must match.
    ("FLOOD_CERT", "FLOOD_INSURANCE_BINDER"): [
        ("flood_zone", "flood_zone", 1.0),
    ],

    # Property tax bill vs transcript — same source, high confidence.
    ("PROPERTY_TAX_BILL", "PROPERTY_TAX_TRANSCRIPT"): [
        ("annual_tax",     "annual_tax",     1.0),
        ("assessed_value", "assessed_value", 0.9),
        ("tax_year",       "tax_year",       0.5),
    ],

    # ── CREDIT CROSS-CHECKS ──────────────────────────────────────────────

    # Credit report vs bank statement — undisclosed debt.
    ("CREDIT_REPORT", "BANK_STATEMENT_M1"): [
        ("total_monthly_obligations", "avg_monthly_debits", 0.5),
    ],

    # Credit supplement vs original report.
    ("CREDIT_SUPPLEMENT", "CREDIT_REPORT"): [
        ("mid_score",   "mid_score",   0.9),
        ("credit_band", "credit_band", 0.8),
    ],

    # Credit report vs divorce decree — court obligations.
    ("CREDIT_REPORT", "DIVORCE_DECREE"): [
        ("total_monthly_obligations", "total_court_obligations", 0.7),
    ],

    # Credit ↔ income SSN cross-check.
    ("W2_CURRENT", "CREDIT_REPORT"): [
        ("ssn_last4", "ssn_last4", 0.6),
    ],
    ("PAYSTUB_CURRENT", "CREDIT_REPORT"): [
        ("ssn_last4", "ssn_last4", 0.6),
    ],

    # ── ASSET CROSS-CHECKS ───────────────────────────────────────────────

    # Bank statement month-to-month consistency.
    ("BANK_STATEMENT_M1", "BANK_STATEMENT_M2"): [
        ("avg_monthly_deposits", "avg_monthly_deposits", 0.7),
        ("account_type",         "account_type",         0.8),
    ],
    ("BANK_STATEMENT_M2", "BANK_STATEMENT_M3"): [
        ("avg_monthly_deposits", "avg_monthly_deposits", 0.7),
    ],

    # Bank deposits vs W2 wages.
    ("BANK_STATEMENT_M1", "W2_CURRENT"): [
        ("avg_monthly_deposits", "box1_wages", 0.5),
    ],

    # Gift letter vs bank — gift should be visible as a large deposit.
    # Two tuples: legacy compares to a ``large_deposits`` field; current
    # extractor surfaces ``ending_balance`` instead so the gift should
    # be a meaningful fraction of the closing balance.
    ("GIFT_LETTER", "BANK_STATEMENT_M1"): [
        ("gift_amount", "large_deposits", 0.8),
        ("gift_amount", "ending_balance", 0.5),
    ],

    # Self-pair: two retirement statements from different periods don't
    # have a meaningful comparison via this map. Skip silently.
    ("ASSET_STATEMENT_RETIREMENT", "ASSET_STATEMENT_RETIREMENT"): [],

    # ── VENDOR-RETURN CROSS-CHECKS ──────────────────────────────────────

    # AUS vs income — DU/LP saw the same wages.
    ("AUS_DU_FINDINGS", "W2_CURRENT"): [
        ("qualifying_income", "box1_wages", 0.7),
    ],

    # Fraud report vs identity.
    ("FRAUD_REPORT", "IDENTITY_DL"): [
        ("kyc_pass", "full_name", 0.6),
    ],

    # ── LOAN TERMS / EMPLOYMENT CROSS-CHECKS ────────────────────────────

    # URLA stated income vs documented W2. URLA stores monthly stated;
    # ``monthly_income_stated_annual`` is the logical field that
    # annualises before comparison (handled in
    # ``_extract_compare_value``). A wide gap is the classic
    # stated-income fraud signal.
    ("URLA_1003", "W2_CURRENT"): [
        ("monthly_income_stated_annual", "box1_wages", 1.0),
    ],

    # URLA loan amount vs purchase price — sanity check on stated LTV.
    ("URLA_1003", "PURCHASE_AGREEMENT"): [
        ("loan_amount", "purchase_price", 0.7),
    ],

    # Rate lock vs URLA — locked rate must match what the application
    # promised the borrower.
    ("RATE_LOCK", "URLA_1003"): [
        ("locked_rate", "interest_rate", 1.0),
    ],

    # Offer letter vs W2 — new-job income source. Tolerance is wider
    # because a job change usually changes comp.
    ("OFFER_LETTER", "W2_CURRENT"): [
        ("base_salary", "box1_wages", 0.8),
    ],

    # Offer letter vs paystub — stub annualised vs offered base salary.
    ("OFFER_LETTER", "PAYSTUB_CURRENT"): [
        ("base_salary", "annualized_ytd", 0.7),
    ],

    # Offer letter vs Work Number VOE — both should report the same
    # base compensation for a new hire.
    ("OFFER_LETTER", "VOE_TWN"): [
        ("base_salary", "base_pay_annual", 0.8),
    ],

    # ── SAME-TYPE PAIRS — explicit empty (no meaningful comparison) ─────
    ("CREDIT_REPORT",     "CREDIT_REPORT"):     [],
    ("APPRAISAL_URAR",    "APPRAISAL_URAR"):    [],
    ("BANK_STATEMENT_M1", "BANK_STATEMENT_M1"): [],
}


# Per-(type_a, type_b, field) overrides of the default
# NUMERIC_CONFLICT_THRESHOLD (0.10). Tighter tolerance for ground-truth
# pairs (wages-vs-IRS), looser for inherently-divergent pairs
# (appraisal-vs-tax assessment).
FIELD_CONFLICT_THRESHOLDS: dict[tuple, float] = {
    # Wage fields — tight tolerance.
    ("W2_CURRENT",   "IRS_TRANSCRIPT",          "box1_wages"): 0.05,
    ("W2_CURRENT",   "PAYSTUB_CURRENT",         "box1_wages"): 0.10,
    ("W2_CURRENT",   "TAX_RETURN_1040_CURRENT", "box1_wages"): 0.05,

    # Property values — looser tolerance.
    ("APPRAISAL_URAR", "PURCHASE_AGREEMENT",      "appraised_value"): 0.05,
    ("APPRAISAL_URAR", "AVM_REPORT",              "appraised_value"): 0.15,
    ("APPRAISAL_URAR", "PROPERTY_TAX_BILL",       "appraised_value"): 0.40,
    ("APPRAISAL_URAR", "PROPERTY_TAX_TRANSCRIPT", "appraised_value"): 0.40,
    # 1004MC's median sale price is the *neighborhood* median — keep a
    # wide gate so a healthy comp doesn't trip a contradicts.
    ("APPRAISAL_URAR", "FORM_1004MC",             "appraised_value"): 0.20,

    # Tax figures — very tight.
    ("IRS_TRANSCRIPT", "TAX_RETURN_1040_CURRENT", "agi"): 0.02,

    # Insurance premiums — moderate.
    ("HOI_BINDER", "HOI_DECLARATIONS", "annual_premium"): 0.05,

    # ── Tier-2 additions ────────────────────────────────────────────────

    # Stated vs documented income (URLA section 1c vs W2 box1) — fraud
    # signal at >10%.
    ("URLA_1003", "W2_CURRENT", "monthly_income_stated_annual"): 0.10,

    # Offer-letter base salary vs W2 box1 — wider, since a new job
    # often comes with comp changes.
    ("OFFER_LETTER", "W2_CURRENT", "base_salary"): 0.15,

    # Rate lock locked_rate vs URLA stated interest_rate — must match
    # exactly in basis points; even 0.05 is generous.
    ("RATE_LOCK", "URLA_1003", "locked_rate"): 0.05,
}


class DocumentReconciler:
    def __init__(self, postgres_store):
        self.postgres_store = postgres_store

    async def reconcile(
        self,
        applicant_id: str,
        new_doc: dict,
        also_compare_with: Optional[list] = None,
    ) -> list[DocumentRelationship]:
        """Compare ``new_doc`` against every other current doc for
        ``applicant_id`` plus any extra docs in ``also_compare_with``
        (typically the co-applicant's docs, so cross-borrower edges get
        emitted within a joint application)."""
        existing = await self.postgres_store.get_documents_for_applicant(applicant_id)
        candidates = list(existing) + list(also_compare_with or [])
        # De-dupe by document_id; new_doc itself never matches.
        seen: set = set()
        unique: list[dict] = []
        for d in candidates:
            doc_id = d.get("document_id")
            if not doc_id or doc_id == new_doc.get("document_id") or doc_id in seen:
                continue
            seen.add(doc_id)
            unique.append(d)

        relationships: list[DocumentRelationship] = []
        for existing_doc in unique:
            relationships.extend(self._compare_pair(applicant_id, new_doc, existing_doc))
        for rel in relationships:
            await self.postgres_store.save_relationship(rel.model_dump())
            logger.info(
                "relationship_written",
                extra={
                    "type": rel.relationship_type.value,
                    "applicant_id": applicant_id,
                    "field": rel.field_name,
                },
            )
        return relationships

    @staticmethod
    def _normalise_value(val) -> Optional[float]:
        """Normalise monetary / numeric field values for comparison.
        Handles ``"$92,400.00"``, ``"92400"``, ``92400``, ``"92,400"``,
        ``"92000-95000"`` → midpoint."""
        if val is None:
            return None
        if isinstance(val, bool):
            # bool is a subclass of int — exclude explicitly.
            return None
        if isinstance(val, (int, float)):
            return float(val)
        s = str(val).strip()
        if not s:
            return None
        s = s.replace("$", "").replace(",", "").replace(" ", "")
        # Range "92000-95000" → midpoint.
        if "-" in s and not s.startswith("-"):
            parts = s.split("-")
            if len(parts) == 2:
                try:
                    return (float(parts[0]) + float(parts[1])) / 2
                except ValueError:
                    pass
        try:
            return float(s)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _annualize_ytd(
        ytd_gross, pay_period_end: Optional[str] = None
    ) -> Optional[float]:
        """Annualise a YTD gross figure based on how far through the year
        we are. If no date, assume YTD covers ~one-third of the year."""
        normalized = DocumentReconciler._normalise_value(ytd_gross)
        if normalized is None:
            return None
        if pay_period_end:
            try:
                end_date = date.fromisoformat(str(pay_period_end))
                day_of_year = end_date.timetuple().tm_yday
                fraction = day_of_year / 365.0
                if fraction > 0.05:
                    return round(normalized / fraction, 2)
            except Exception:
                pass
        # Default: assume YTD is ~4 months in.
        return round(normalized * 3.0, 2)

    def _compare_pair(
        self, applicant_id: str, doc_a: dict, doc_b: dict
    ) -> list[DocumentRelationship]:
        type_a = doc_a.get("document_type", "")
        type_b = doc_b.get("document_type", "")
        fields_a = doc_a.get("extracted_fields") or {}
        fields_b = doc_b.get("extracted_fields") or {}

        if isinstance(fields_a, str):
            try:
                fields_a = json.loads(fields_a)
            except Exception:
                fields_a = {}
        if isinstance(fields_b, str):
            try:
                fields_b = json.loads(fields_b)
            except Exception:
                fields_b = {}

        pairs = self._get_pairs(type_a, type_b)
        results: list[DocumentRelationship] = []
        for field_a, field_b, weight in pairs:
            val_a = self._extract_compare_value(fields_a, field_a)
            val_b = self._extract_compare_value(fields_b, field_b)
            if val_a is None or val_b is None:
                continue
            rel = self._make_relationship(
                applicant_id=applicant_id,
                source_doc_id=doc_a["document_id"],
                target_doc_id=doc_b["document_id"],
                type_a=type_a,
                type_b=type_b,
                field_a=field_a,
                field_b=field_b,
                val_a=val_a,
                val_b=val_b,
                weight=weight,
            )
            if rel:
                results.append(rel)
        return results

    @classmethod
    def _extract_compare_value(cls, fields: dict, field_name: str):
        """Resolve a logical comparison field to its raw value.

        ``annualized_ytd`` is dual-shape: callers may supply it directly
        (already annualised) OR provide ``ytd_gross`` + optional
        ``pay_period_end`` and let us annualise. If both are present the
        explicit value wins.

        ``monthly_income_stated_annual`` is a logical field for URLA-vs-W2
        comparisons: URLA stores monthly stated income, W2 stores annual
        wages — multiply the URLA value by 12 before comparison.
        """
        if field_name == "annualized_ytd":
            direct = fields.get("annualized_ytd")
            if direct is not None:
                return cls._normalise_value(direct)
            return cls._annualize_ytd(
                fields.get("ytd_gross"), fields.get("pay_period_end"),
            )
        if field_name == "monthly_income_stated_annual":
            monthly = cls._normalise_value(fields.get("monthly_income_stated"))
            return monthly * 12 if monthly is not None else None
        return fields.get(field_name)

    def _make_relationship(
        self,
        applicant_id: str,
        source_doc_id: str,
        target_doc_id: str,
        type_a: str,
        type_b: str,
        field_a: str,
        field_b: str,
        val_a,
        val_b,
        weight: float = 1.0,
    ) -> Optional[DocumentRelationship]:
        field_label = f"{field_a}↔{field_b}"

        # Resolve per-pair conflict threshold; fall back to the default.
        threshold = FIELD_CONFLICT_THRESHOLDS.get(
            (type_a, type_b, field_a),
            FIELD_CONFLICT_THRESHOLDS.get(
                (type_b, type_a, field_b),
                NUMERIC_CONFLICT_THRESHOLD,
            ),
        )

        a = self._normalise_value(val_a)
        b = self._normalise_value(val_b)
        if a is not None and b is not None:
            if max(abs(a), abs(b)) == 0:
                return None
            delta = abs(a - b) / max(abs(a), abs(b))
            confirms_band = min(0.05, threshold)
            if delta <= confirms_band:
                rel_type = RelationshipType.CONFIRMS
                conf = 0.95 * weight
                note = f"delta {delta*100:.1f}% ≤ {confirms_band*100:.1f}% — confirms"
            elif delta <= threshold:
                rel_type = RelationshipType.CORROBORATES
                conf = 0.75 * weight
                note = (
                    f"delta {delta*100:.1f}% ≤ "
                    f"{threshold*100:.0f}% — corroborates"
                )
            else:
                rel_type = RelationshipType.CONTRADICTS
                conf = 0.90 * weight
                note = (
                    f"delta {delta*100:.1f}% > "
                    f"{threshold*100:.0f}% — CONFLICT"
                )
            return DocumentRelationship(
                relationship_id=_deterministic_relationship_id(
                    applicant_id, source_doc_id, target_doc_id,
                    field_label, rel_type.value,
                ),
                applicant_id=applicant_id,
                source_doc_id=source_doc_id,
                target_doc_id=target_doc_id,
                relationship_type=rel_type,
                field_name=field_label,
                source_value=val_a,
                target_value=val_b,
                delta_pct=round(delta * 100, 2),
                confidence=round(conf, 3),
                reasoning=f"{field_label}: {a:,.0f} vs {b:,.0f} — {note}",
            )

        # String path — rapidfuzz similarity.
        try:
            from rapidfuzz import fuzz
        except ImportError:
            return None

        score = fuzz.ratio(str(val_a).lower(), str(val_b).lower()) / 100
        if score > 0.90:
            rel_type = RelationshipType.CONFIRMS
            conf = score * weight
        elif score > 0.70:
            rel_type = RelationshipType.CORROBORATES
            conf = score * 0.80 * weight
        else:
            rel_type = RelationshipType.CONTRADICTS
            conf = (1 - score) * weight
        return DocumentRelationship(
            relationship_id=_deterministic_relationship_id(
                applicant_id, source_doc_id, target_doc_id,
                field_label, rel_type.value,
            ),
            applicant_id=applicant_id,
            source_doc_id=source_doc_id,
            target_doc_id=target_doc_id,
            relationship_type=rel_type,
            field_name=field_label,
            source_value=val_a,
            target_value=val_b,
            delta_pct=None,
            confidence=round(conf, 3),
            reasoning=f"{field_label}: fuzzy match {score:.2f}",
        )

    def _get_pairs(self, type_a: str, type_b: str) -> list[tuple]:
        result = COMPARISON_MAP.get((type_a, type_b))
        if result is not None:
            return result
        result = COMPARISON_MAP.get((type_b, type_a))
        if result is not None:
            return [(b, a, w) for a, b, w in result]
        return []
