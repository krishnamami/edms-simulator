"""Builds + persists ``entity_states`` rows at the end of
``AggregationService._run_assembly``.

One row per entity in the lending tree:

    LOAN APPLICATION
      ├── LOAN TERMS              entity_id = LOAN-{application_id}
      ├── PRIMARY BORROWER        entity_id = applicant_id
      ├── CO-BORROWER (opt)       entity_id = co_applicant_id
      └── PROPERTY (opt)          entity_id = property_id  | PROP-{application_id}

The state JSONB stores a structured summary (income / employment /
credit / asset / identity for borrowers; valuation / title /
insurance / tax / inspections for property; urla / purchase_agreement
/ rate_lock / aus_findings for loan_terms). Counts (document_count /
graph_edge_count / conflict_count / completeness_pct) are computed
inline from PG.

Failure isolation: every public ``upsert_*`` returns ``True`` on
success and ``False`` on failure (logged but never raised). The
caller's contract is "best-effort, never break upload latency".
"""
from __future__ import annotations

import json
import logging
import random
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Doc-type families used by the income / identity enrichment derivations.
# Keep these synced with the canonical names in
# ``core/income/assembler.py`` + the indexer's extractor dispatch.
# ---------------------------------------------------------------------------

_W2_TYPES            = ("W2_CURRENT", "W2_PRIOR")
_PAYSTUB_TYPES       = ("PAYSTUB_CURRENT", "PAYSTUB_PRIOR")
_SELF_EMPLOYED_TYPES = ("1099_NEC", "FORM_1099_NEC", "FORM_1099_MISC",
                        "SCHEDULE_C", "SCHEDULE_E")
_RETIRED_TYPES       = ("SSA_AWARD_LETTER", "PENSION_LETTER")
_OFAC_TYPES          = ("OFAC_CHECK", "OFAC_REPORT")
_DL_TYPES            = ("DRIVERS_LICENSE", "IDENTITY_DL")

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Doc-type catalogs — one per entity. completeness_pct = filled / total.
# ---------------------------------------------------------------------------


# 15 borrower-side required slots (mirrors api/routes._REQUIRED_DOCS).
_BORROWER_SLOTS: list[dict] = [
    {"doc_type": "W2_CURRENT",        "alternates": ["W2_PRIOR"]},
    {"doc_type": "PAYSTUB_CURRENT",   "alternates": ["PAYSTUB_PRIOR"]},
    {"doc_type": "CREDIT_REPORT",     "alternates": []},
    {"doc_type": "BANK_STATEMENT_M1", "alternates": []},
    {"doc_type": "IDENTITY_DL",       "alternates": ["DRIVERS_LICENSE"]},
    {"doc_type": "SSN_VALIDATION",    "alternates": ["IDENTITY_SSN_CARD"]},
    {"doc_type": "OFAC_REPORT",       "alternates": ["OFAC_CHECK"]},
    {"doc_type": "APPRAISAL_URAR",    "alternates": []},
    {"doc_type": "TITLE_COMMITMENT",  "alternates": []},
    {"doc_type": "HOI_BINDER",        "alternates": ["HOI_DECLARATIONS"]},
    {"doc_type": "FLOOD_CERT",        "alternates": []},
    {"doc_type": "PROPERTY_TAX_BILL", "alternates": []},
    {"doc_type": "URLA_1003",         "alternates": []},
    {"doc_type": "PURCHASE_AGREEMENT", "alternates": []},
    {"doc_type": "AUS_DU_FINDINGS",   "alternates": ["AUS_LP_FINDINGS"]},
]


_VALUATION_TYPES   = ["APPRAISAL_URAR", "APPRAISAL_UPDATE", "AVM_REPORT", "FORM_1004MC"]
_TITLE_TYPES       = ["TITLE_COMMITMENT", "TITLE_INSURANCE", "SURVEY"]
_INSURANCE_TYPES   = ["HOI_BINDER", "HOI_DECLARATIONS", "FLOOD_CERT", "WIND_HAIL_INSURANCE"]
_TAX_TYPES         = ["PROPERTY_TAX_BILL", "PROPERTY_TAX_TRANSCRIPT"]
_INSPECTION_TYPES  = ["WDO_REPORT", "PEST_WDO_INSPECTION", "WELL_SEPTIC_INSPECTION", "HOA_CERT", "HOA_CERTIFICATION"]
_PROPERTY_TYPES    = (_VALUATION_TYPES + _TITLE_TYPES + _INSURANCE_TYPES
                      + _TAX_TYPES + _INSPECTION_TYPES)

_LOAN_TYPES = ["URLA_1003", "PURCHASE_AGREEMENT", "RATE_LOCK",
               "AUS_DU_FINDINGS", "AUS_LP_FINDINGS"]

_EMPLOYMENT_TYPES = ["VOE_TWN", "VOE_EQUIFAX", "VOE", "OFFER_LETTER"]


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _slot_received(slot: dict, have: set[str]) -> bool:
    if slot["doc_type"] in have:
        return True
    return any(alt in have for alt in (slot.get("alternates") or []))


def _ensure_dict(value: Any) -> dict:
    """asyncpg returns JSONB as either dict or JSON-encoded string
    depending on driver/version. Normalize."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value) or {}
        except Exception:
            return {}
    return {}


def _doc_fields(doc: dict) -> dict:
    """Return ``extracted_fields`` whether stored as dict or string."""
    raw = doc.get("extracted_fields")
    return _ensure_dict(raw)


def _completeness(doc_types: set[str], slots: list[dict]) -> float:
    if not slots:
        return 0.0
    filled = sum(1 for s in slots if _slot_received(s, doc_types))
    return round(filled / len(slots) * 100, 1)


def _sub_completeness(present_buckets: list[bool]) -> float:
    """For property + loan_terms, completeness is the fraction of
    sub-entity buckets (valuation, title, …) that have at least one
    populated doc."""
    if not present_buckets:
        return 0.0
    return round(sum(1 for p in present_buckets if p) / len(present_buckets) * 100, 1)


def _latest(docs: list[dict]) -> Optional[dict]:
    if not docs:
        return None
    return docs[0]


# ---------------------------------------------------------------------------
# Borrower / co-borrower
# ---------------------------------------------------------------------------


async def build_borrower_state(
    pg, redis, applicant_id: str, application_id: str, tenant_id: str,
) -> dict:
    """Compose the borrower (or co-borrower) state dict."""
    income   = await pg.get_income_profile(applicant_id, tenant_id=tenant_id)
    credit   = await pg.get_credit_profile(applicant_id, tenant_id=tenant_id)
    assets   = await redis.get_asset_summary(applicant_id, tenant_id=tenant_id) or {}
    identity = await redis.get_identity_summary(applicant_id, tenant_id=tenant_id) or {}

    employment_docs = await pg.get_documents_by_types(
        applicant_id, _EMPLOYMENT_TYPES, tenant_id=tenant_id,
    )
    employment: dict = {}
    if employment_docs:
        latest = employment_docs[0]
        f = _doc_fields(latest)
        employment = {
            "doc_type":           latest.get("document_type"),
            "employer_name":      f.get("employer_name"),
            "employment_status":  f.get("employment_status"),
            "income_amount":      f.get("income_amount") or f.get("base_pay_annual"),
            "verified":           bool(f.get("employment_verified")) or None,
        }

    docs = await pg.get_documents_for_applicant(applicant_id, tenant_id=tenant_id)
    doc_types = sorted({d.get("document_type") for d in docs if d.get("document_type")})

    # Income block carries both the assembled-profile summary AND the
    # 9 enrichment fields (verified_/stated_, employment_type,
    # multiple_income_sources, income_confidence_score, stability,
    # trending, gap_in_employment) that the income_assessment persona
    # in Decision OS reads.
    income_block = _income_summary(income)
    income_block.update(_derive_income_enrichment(
        docs, income, applicant_id, employment.get("income_amount"),
    ))

    # Identity block carries the existing Redis summary plus the 5
    # FraudProfile fields the fraud persona reads.
    identity_block = dict(identity or {})
    identity_block.update(_derive_identity_enrichment(docs, applicant_id))

    return {
        "applicant_id":   applicant_id,
        "application_id": application_id,
        "income":         income_block,
        "employment":     employment,
        "credit":         _credit_summary(credit, applicant_id=applicant_id),
        "assets":         assets,
        "identity":       identity_block,
        "doc_types":      doc_types,
    }


def _income_summary(income: Optional[dict]) -> dict:
    if not income:
        return {}
    primary = income.get("primary_borrower") or {}
    return {
        "qualifying_monthly":          income.get("qualifying_monthly")
                                        or primary.get("qualifying_monthly"),
        "combined_qualifying_monthly": income.get("combined_qualifying_monthly"),
        "overall_confidence":          primary.get("overall_confidence"),
        "source_types": sorted({
            (s or {}).get("source_type")
            for s in (primary.get("sources") or [])
            if (s or {}).get("source_type")
        }),
    }


def _classify_employment_type(doc_types: set[str]) -> str:
    """Group 2 — employment_type buckets the borrower into one of four
    income channels by the doc types they've filed. Distinct from
    ``employment.employment_status`` which tracks active/terminated."""
    if any(t in doc_types for t in _W2_TYPES) \
            or any(t in doc_types for t in _PAYSTUB_TYPES):
        return "salaried"
    if any(t in doc_types for t in _SELF_EMPLOYED_TYPES):
        return "self_employed"
    if any(t in doc_types for t in _RETIRED_TYPES):
        return "retired"
    return "other"


def _derive_income_enrichment(
    docs: list,
    income: Optional[dict],
    applicant_id: str,
    employment_income_amount: Optional[float],
) -> dict:
    """Groups 2 + 3 — surface the 9 income / employment fields the
    Decision OS income_assessment persona reads. All derived from
    ``document_index`` rows we already have (W2_CURRENT, W2_PRIOR,
    URLA_1003, etc.); no extra PG hits."""
    docs_by_type: dict[str, list[dict]] = {}
    type_set: set[str] = set()
    for d in docs:
        dt = d.get("document_type")
        if dt:
            docs_by_type.setdefault(dt, []).append(d)
            type_set.add(dt)

    # ── Group 2 ──────────────────────────────────────────────────────
    w2_current = _latest(docs_by_type.get("W2_CURRENT") or [])
    verified_annual: Optional[float] = None
    if w2_current:
        ef = _doc_fields(w2_current)
        verified_annual = ef.get("box1_wages") or ef.get("wages_salaries")
    if verified_annual is None and employment_income_amount is not None:
        verified_annual = employment_income_amount

    urla            = _latest(docs_by_type.get("URLA_1003") or [])
    stated_annual:   Optional[float] = None
    stated_employer: Optional[str]   = None
    if urla:
        ef = _doc_fields(urla)
        monthly = ef.get("monthly_income_stated")
        if monthly is not None:
            try:
                stated_annual = float(monthly) * 12.0
            except (TypeError, ValueError):
                stated_annual = None
        stated_employer = (ef.get("employer_name")
                           or ef.get("stated_employer")
                           or ef.get("borrower_employer"))

    # multiple_income_sources — different income CHANNELS, not just
    # different doc types within one channel (W2_CURRENT + W2_PRIOR is
    # still a single salaried channel).
    channels: set[str] = set()
    if any(t in type_set for t in _W2_TYPES):            channels.add("w2")
    if any(t in type_set for t in _PAYSTUB_TYPES):       channels.add("paystub")
    if any(t in type_set for t in _SELF_EMPLOYED_TYPES): channels.add("self_employed")
    if any(t in type_set for t in _RETIRED_TYPES):       channels.add("retired")
    multiple_sources = len(channels) > 1

    confidence: float = 0.85
    if income:
        primary = income.get("primary_borrower") or {}
        oc = primary.get("overall_confidence")
        if oc is not None:
            try:
                confidence = float(oc)
            except (TypeError, ValueError):
                pass

    # ── Group 3: income stability + trending ─────────────────────────
    w2_prior = _latest(docs_by_type.get("W2_PRIOR") or [])
    stability = "insufficient_history"
    trending  = "unknown"
    if w2_current and w2_prior:
        cur_f = _doc_fields(w2_current)
        pri_f = _doc_fields(w2_prior)
        cur_emp = (cur_f.get("employer_name") or "").strip().lower()
        pri_emp = (pri_f.get("employer_name") or "").strip().lower()
        stability = ("stable" if cur_emp and pri_emp and cur_emp == pri_emp
                     else "new_employment")

        cur_wages = cur_f.get("box1_wages") or cur_f.get("wages_salaries")
        pri_wages = pri_f.get("box1_wages") or pri_f.get("wages_salaries")
        try:
            if cur_wages is not None and pri_wages is not None and float(pri_wages):
                delta = (float(cur_wages) - float(pri_wages)) / float(pri_wages)
                if   delta >  0.03: trending = "increasing"
                elif delta < -0.03: trending = "decreasing"
                else:               trending = "flat"
        except (TypeError, ValueError, ZeroDivisionError):
            pass

    return {
        "verified_income_annual":  verified_annual,
        "stated_income_annual":    stated_annual,
        "stated_employer":         stated_employer,
        "employment_type":         _classify_employment_type(type_set),
        "multiple_income_sources": multiple_sources,
        "income_confidence_score": round(confidence, 2),
        "income_stability":        stability,
        "income_trending":         trending,
        # gap_in_employment would need start/end dates we don't capture
        # in the synthetic corpus; default False so the persona has the
        # key but knows the signal isn't computed.
        "gap_in_employment":       False,
    }


def _derive_identity_enrichment(docs: list, applicant_id: str) -> dict:
    """Group 4 — FraudProfile fields. Decision OS's fraud persona reads
    these to gate ALLOW / ESCALATE. Synthetic data → derived from doc
    presence + a seeded ±0.03 wiggle on ``fraud_score`` so re-running
    the same applicant doesn't churn the value."""
    types    = {d.get("document_type") for d in docs if d.get("document_type")}
    has_ofac = any(t in types for t in _OFAC_TYPES)
    has_ssn  = "SSN_VALIDATION" in types
    has_dl   = any(t in types for t in _DL_TYPES)
    id_count = sum([has_ofac, has_ssn, has_dl])

    if   id_count == 3: base = 0.05
    elif id_count >= 1: base = 0.30
    else:               base = 0.80
    rng         = random.Random(applicant_id or "")
    fraud_score = round(max(0.0, min(1.0, base + rng.uniform(-0.03, 0.03))), 3)

    if   has_ssn and has_dl: match_conf = 0.98
    elif has_ssn or has_dl:  match_conf = 0.70
    else:                    match_conf = 0.0

    return {
        "fraud_score":                 fraud_score,
        "identity_match_confidence":   match_conf,
        "document_authenticity_score": 0.95,
        "watchlist_match":             False,
        "synthetic_identity_flag":     False,
    }


def _derive_credit_assessment_fields(
    applicant_id: str, mid_score: int, credit_band: str,
) -> dict:
    """Derive the 7 fields the credit_assessment persona in Decision OS
    needs to gate ALLOW / ESCALATE / BLOCK. Our CREDIT_REPORT docs are
    synthetic and don't carry these explicitly, so we derive from the
    underwriting buckets that ``mid_score`` already places the
    applicant in. Seeded on ``applicant_id`` so a backfill that
    re-composes the same applicant surfaces the same utilization /
    tradeline counts — Decision OS snapshots stay stable across
    re-runs."""
    rng = random.Random(applicant_id or "")
    if mid_score < 620:
        derog        = 4
        tradelines   = rng.randint(1, 3)
        utilization  = round(rng.uniform(0.60, 0.95), 2)
        no_derog_24mo = False
    elif mid_score < 680:
        derog        = 2
        tradelines   = rng.randint(3, 8)
        utilization  = round(rng.uniform(0.40, 0.70), 2)
        no_derog_24mo = rng.random() < 0.70
    elif mid_score < 740:
        derog        = 1
        tradelines   = rng.randint(6, 15)
        utilization  = round(rng.uniform(0.20, 0.50), 2)
        no_derog_24mo = True
    else:
        derog        = 0
        tradelines   = rng.randint(6, 15)
        utilization  = round(rng.uniform(0.05, 0.30), 2)
        no_derog_24mo = True
    return {
        "active_bankruptcy":            False,
        "foreclosure_last_36_months":   False,
        "thin_file":                    mid_score < 620,
        "no_derogatory_last_24_months": no_derog_24mo,
        "derogatory_marks":             derog,
        "open_tradelines":              tradelines,
        "credit_utilization":           utilization,
    }


def _credit_summary(credit: Optional[dict], applicant_id: str = "") -> dict:
    if not credit:
        return {}
    obligations = credit.get("monthly_obligations") or credit.get("total_monthly_obligations")
    mid_score   = credit.get("mid_score")
    credit_band = credit.get("credit_band") or "near-prime"
    out = {
        "mid_score":          mid_score,
        "credit_band":        credit_band,
        "monthly_obligations": obligations,
        "experian_score":     credit.get("experian_score"),
        "equifax_score":      credit.get("equifax_score"),
        "transunion_score":   credit.get("transunion_score"),
    }
    if mid_score is not None:
        try:
            out.update(_derive_credit_assessment_fields(
                applicant_id, int(mid_score), credit_band,
            ))
        except (TypeError, ValueError):
            pass
    return out


# ---------------------------------------------------------------------------
# Property
# ---------------------------------------------------------------------------


async def build_property_state(
    pg, redis, application_id: str, tenant_id: str,
) -> tuple[Optional[str], dict]:
    """Returns ``(property_id, state)`` — or ``(None, {})`` when no
    property docs are present. The id falls back to ``PROP-{app_id}``
    if the ``properties`` row hasn't been provisioned yet."""
    docs = await pg.get_documents_for_application_by_types(
        application_id, _PROPERTY_TYPES, tenant_id=tenant_id,
    )
    if not docs:
        return None, {}

    by_type: dict[str, list[dict]] = {}
    for d in docs:
        by_type.setdefault(d.get("document_type"), []).append(d)

    property_id = None
    try:
        prop_row = await pg.get_property_by_application(
            application_id, tenant_id=tenant_id,
        )
        if prop_row:
            property_id = prop_row.get("property_id")
    except Exception:
        property_id = None
    if not property_id:
        property_id = f"PROP-{application_id}"

    valuation: dict   = {}
    appraisal_doc = _latest(by_type.get("APPRAISAL_URAR") or by_type.get("APPRAISAL_UPDATE") or [])
    if appraisal_doc:
        f = _doc_fields(appraisal_doc)
        valuation.update({
            "appraised_value":   f.get("appraised_value") or f.get("updated_value"),
            "condition_rating":  f.get("condition_rating"),
            "appraisal_date":    f.get("appraisal_date"),
            "doc_type":          appraisal_doc.get("document_type"),
        })
    avm_doc = _latest(by_type.get("AVM_REPORT") or [])
    if avm_doc:
        valuation["avm_value"] = _doc_fields(avm_doc).get("avm_value")
    mc_doc = _latest(by_type.get("FORM_1004MC") or [])
    if mc_doc:
        valuation["median_sale_price"] = _doc_fields(mc_doc).get("median_sale_price")

    title: dict = {}
    for t in _TITLE_TYPES:
        rows = by_type.get(t) or []
        title[f"{t.lower()}_received"] = bool(rows)
        if rows:
            f = _doc_fields(rows[0])
            if t == "TITLE_INSURANCE":
                title["coverage_amount"] = f.get("coverage_amount")
            elif t == "TITLE_COMMITMENT":
                title["title_commitment_id"] = f.get("title_commitment_id")
            elif t == "SURVEY":
                title["survey_date"] = f.get("survey_date")

    insurance: dict = {}
    hoi = _latest(by_type.get("HOI_BINDER") or by_type.get("HOI_DECLARATIONS") or [])
    if hoi:
        f = _doc_fields(hoi)
        insurance.update({
            "hoi_premium":      f.get("annual_premium"),
            "hoi_carrier":      f.get("carrier") or f.get("carrier_name"),
            "dwelling_coverage": f.get("dwelling_coverage"),
        })
    flood = _latest(by_type.get("FLOOD_CERT") or [])
    if flood:
        f = _doc_fields(flood)
        insurance.update({
            "flood_zone":              f.get("flood_zone"),
            "flood_insurance_required": f.get("flood_insurance_required"),
        })
    wind = _latest(by_type.get("WIND_HAIL_INSURANCE") or [])
    if wind:
        f = _doc_fields(wind)
        insurance["wind_hail_premium"] = f.get("annual_premium")

    tax: dict = {}
    tax_doc = _latest(by_type.get("PROPERTY_TAX_BILL") or by_type.get("PROPERTY_TAX_TRANSCRIPT") or [])
    if tax_doc:
        f = _doc_fields(tax_doc)
        tax.update({
            "annual_tax":     f.get("annual_tax"),
            "assessed_value": f.get("assessed_value"),
            "tax_year":       f.get("tax_year"),
        })

    inspections: dict = {}
    pest = _latest(by_type.get("WDO_REPORT") or by_type.get("PEST_WDO_INSPECTION") or [])
    if pest:
        f = _doc_fields(pest)
        inspections["pest"] = {
            "wdo_present":     f.get("wdo_present"),
            "inspection_date": f.get("inspection_date"),
        }
    well = _latest(by_type.get("WELL_SEPTIC_INSPECTION") or [])
    if well:
        f = _doc_fields(well)
        inspections["well_septic"] = {
            "well_pass":   f.get("well_pass"),
            "septic_pass": f.get("septic_pass"),
        }
    hoa = _latest(by_type.get("HOA_CERT") or by_type.get("HOA_CERTIFICATION") or [])
    if hoa:
        f = _doc_fields(hoa)
        inspections["hoa"] = {
            "monthly_dues":      f.get("monthly_dues"),
            "delinquency":       f.get("delinquency"),
            "litigation_pending": f.get("litigation_pending"),
        }

    state = {
        "property_id":   property_id,
        "application_id": application_id,
        "valuation":     valuation,
        "title":         title,
        "insurance":     insurance,
        "tax":           tax,
        "inspections":   inspections,
        "doc_types":     sorted(by_type.keys()),
    }
    return property_id, state


def _property_completeness(state: dict) -> float:
    return _sub_completeness([
        bool(state.get("valuation")),
        bool(state.get("title") and any(v for k, v in state["title"].items() if k.endswith("_received") and v)),
        bool(state.get("insurance")),
        bool(state.get("tax")),
        bool(state.get("inspections")),
    ])


# ---------------------------------------------------------------------------
# Loan terms
# ---------------------------------------------------------------------------


async def build_loan_terms_state(
    pg, application_id: str, tenant_id: str,
) -> dict:
    docs = await pg.get_documents_for_application_by_types(
        application_id, _LOAN_TYPES, tenant_id=tenant_id,
    )
    if not docs:
        return {}

    by_type: dict[str, list[dict]] = {}
    for d in docs:
        by_type.setdefault(d.get("document_type"), []).append(d)

    urla_doc = _latest(by_type.get("URLA_1003") or [])
    urla = {}
    if urla_doc:
        f = _doc_fields(urla_doc)
        urla = {
            "loan_purpose":      f.get("loan_purpose"),
            "loan_amount":       f.get("loan_amount"),
            "interest_rate":     f.get("interest_rate"),
            "loan_term_months":  f.get("loan_term_months"),
            "property_type":     f.get("property_type"),
            "occupancy":         f.get("occupancy"),
            "monthly_income_stated": f.get("monthly_income_stated"),
        }

    pa_doc = _latest(by_type.get("PURCHASE_AGREEMENT") or [])
    purchase_agreement = {}
    if pa_doc:
        f = _doc_fields(pa_doc)
        purchase_agreement = {
            "purchase_price":      f.get("purchase_price"),
            "earnest_money":       f.get("earnest_money"),
            "closing_date":        f.get("closing_date"),
            "seller_concessions":  f.get("seller_concessions"),
        }

    rl_doc = _latest(by_type.get("RATE_LOCK") or [])
    rate_lock = {}
    if rl_doc:
        f = _doc_fields(rl_doc)
        rate_lock = {
            "locked_rate":   f.get("locked_rate"),
            "lock_expiry":   f.get("lock_expiry"),
            "lock_days":     f.get("lock_days"),
            "points":        f.get("points"),
            "loan_program":  f.get("loan_program"),
        }

    aus_doc = _latest(by_type.get("AUS_DU_FINDINGS") or by_type.get("AUS_LP_FINDINGS") or [])
    aus_findings = {}
    if aus_doc:
        f = _doc_fields(aus_doc)
        aus_findings = {
            "doc_type":       aus_doc.get("document_type"),
            "approved":       f.get("approved"),
            "recommendation": f.get("recommendation"),
            "risk_class":     f.get("risk_class"),
            "case_id":        f.get("case_id") or f.get("aus_case_id"),
            "qualifying_income": f.get("qualifying_income"),
        }

    return {
        "application_id":     application_id,
        "urla":               urla,
        "purchase_agreement": purchase_agreement,
        "rate_lock":          rate_lock,
        "aus_findings":       aus_findings,
        "doc_types":          sorted(by_type.keys()),
    }


def _loan_terms_completeness(state: dict) -> float:
    return _sub_completeness([
        bool(state.get("urla")),
        bool(state.get("purchase_agreement")),
        bool(state.get("rate_lock")),
        bool(state.get("aus_findings")),
    ])


# ---------------------------------------------------------------------------
# Top-level orchestrator — called from AggregationService._run_assembly
# ---------------------------------------------------------------------------


async def upsert_all_entities(
    pg, redis, application_id: str, applicant_id: str,
    co_applicant_id: Optional[str], tenant_id: str = "default",
) -> dict:
    """Build + upsert every relevant entity_state row for the
    application. Returns a result dict ``{"borrower": bool,
    "co_borrower": bool, "property": bool, "loan_terms": bool}``
    indicating which upserts succeeded. Per-entity failures are
    logged + bucketed False; the function never raises."""
    results = {"borrower": False, "co_borrower": False,
               "property": False, "loan_terms": False}

    # ── Borrower ───────────────────────────────────────────────────
    try:
        state = await build_borrower_state(
            pg, redis, applicant_id, application_id, tenant_id,
        )
        doc_types_set = set(state.get("doc_types") or [])
        doc_count = len(state.get("doc_types") or [])
        # Real doc_count from PG (state.doc_types is unique types — we
        # want raw doc count for the column).
        try:
            all_docs = await pg.get_documents_for_applicant(applicant_id, tenant_id=tenant_id)
            doc_count = len(all_docs)
        except Exception:
            pass
        try:
            edge_count = await pg.count_edges_for_entity(applicant_id, tenant_id=tenant_id)
        except Exception:
            edge_count = 0
        try:
            conflict_count = await pg.count_conflicts_for_entity(applicant_id, tenant_id=tenant_id)
        except Exception:
            conflict_count = 0

        completeness = _completeness(doc_types_set, _BORROWER_SLOTS)
        await pg.upsert_entity_state(
            entity_id=applicant_id, entity_type="borrower",
            application_id=application_id,
            state=state, document_count=doc_count,
            graph_edge_count=edge_count, conflict_count=conflict_count,
            completeness_pct=completeness, tenant_id=tenant_id,
        )
        await redis.set_entity_state(
            applicant_id, json.dumps(state, default=str), tenant_id=tenant_id,
        )
        results["borrower"] = True
    except Exception as exc:
        logger.warning("entity_state_borrower_failed",
                       extra={"applicant_id": applicant_id, "error": str(exc)[:200]})

    # ── Co-borrower ────────────────────────────────────────────────
    if co_applicant_id:
        try:
            co_docs = await pg.get_documents_for_applicant(co_applicant_id, tenant_id=tenant_id)
            if co_docs:
                state = await build_borrower_state(
                    pg, redis, co_applicant_id, application_id, tenant_id,
                )
                doc_types_set = set(state.get("doc_types") or [])
                doc_count = len(co_docs)
                try:
                    edge_count = await pg.count_edges_for_entity(co_applicant_id, tenant_id=tenant_id)
                except Exception:
                    edge_count = 0
                try:
                    conflict_count = await pg.count_conflicts_for_entity(co_applicant_id, tenant_id=tenant_id)
                except Exception:
                    conflict_count = 0
                completeness = _completeness(doc_types_set, _BORROWER_SLOTS)
                await pg.upsert_entity_state(
                    entity_id=co_applicant_id, entity_type="co_borrower",
                    application_id=application_id,
                    state=state, document_count=doc_count,
                    graph_edge_count=edge_count, conflict_count=conflict_count,
                    completeness_pct=completeness, tenant_id=tenant_id,
                )
                await redis.set_entity_state(
                    co_applicant_id, json.dumps(state, default=str), tenant_id=tenant_id,
                )
                results["co_borrower"] = True
        except Exception as exc:
            logger.warning("entity_state_co_borrower_failed",
                           extra={"co_applicant_id": co_applicant_id, "error": str(exc)[:200]})

    # ── Property ───────────────────────────────────────────────────
    try:
        property_id, state = await build_property_state(
            pg, redis, application_id, tenant_id,
        )
        if property_id and state:
            doc_count = len(state.get("doc_types") or [])
            try:
                # Property edge counts roll up per applicant; track it
                # against the primary applicant_id so the dashboard
                # surfaces graph activity that involved property docs.
                edge_count = await pg.count_edges_for_entity(applicant_id, tenant_id=tenant_id)
                conflict_count = await pg.count_conflicts_for_entity(applicant_id, tenant_id=tenant_id)
            except Exception:
                edge_count = conflict_count = 0
            completeness = _property_completeness(state)
            await pg.upsert_entity_state(
                entity_id=property_id, entity_type="property",
                application_id=application_id,
                state=state, document_count=doc_count,
                graph_edge_count=edge_count, conflict_count=conflict_count,
                completeness_pct=completeness, tenant_id=tenant_id,
            )
            await redis.set_entity_state(
                property_id, json.dumps(state, default=str), tenant_id=tenant_id,
            )
            results["property"] = True
    except Exception as exc:
        logger.warning("entity_state_property_failed",
                       extra={"application_id": application_id, "error": str(exc)[:200]})

    # ── Loan terms ─────────────────────────────────────────────────
    try:
        state = await build_loan_terms_state(pg, application_id, tenant_id)
        if state:
            entity_id = f"LOAN-{application_id}"
            doc_count = len(state.get("doc_types") or [])
            completeness = _loan_terms_completeness(state)
            await pg.upsert_entity_state(
                entity_id=entity_id, entity_type="loan_terms",
                application_id=application_id,
                state=state, document_count=doc_count,
                graph_edge_count=0, conflict_count=0,
                completeness_pct=completeness, tenant_id=tenant_id,
            )
            await redis.set_entity_state(
                entity_id, json.dumps(state, default=str), tenant_id=tenant_id,
            )
            results["loan_terms"] = True
    except Exception as exc:
        logger.warning("entity_state_loan_terms_failed",
                       extra={"application_id": application_id, "error": str(exc)[:200]})

    return results
