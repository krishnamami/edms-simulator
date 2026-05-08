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
from typing import Any, Optional

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
    return {
        "applicant_id":   applicant_id,
        "application_id": application_id,
        "income":         _income_summary(income),
        "employment":     employment,
        "credit":         _credit_summary(credit),
        "assets":         assets,
        "identity":       identity,
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


def _credit_summary(credit: Optional[dict]) -> dict:
    if not credit:
        return {}
    obligations = credit.get("monthly_obligations") or credit.get("total_monthly_obligations")
    return {
        "mid_score":          credit.get("mid_score"),
        "credit_band":        credit.get("credit_band"),
        "monthly_obligations": obligations,
        "experian_score":     credit.get("experian_score"),
        "equifax_score":      credit.get("equifax_score"),
        "transunion_score":   credit.get("transunion_score"),
    }


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
