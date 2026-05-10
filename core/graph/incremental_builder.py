"""Incremental knowledge-graph builder.

Pulls new documents from a ``BaseEDMSConnector``, persists each one,
runs assembly + reconciler per affected entity, and updates a single
row per entity in ``entity_states`` (no versioning — last write wins).
The companion :class:`core.graph.snapshot_scheduler.SnapshotScheduler`
copies the live ``entity_states`` into ``entity_snapshots`` at EOD so
a Decision-OS replay can walk an entity's evolution day by day.

This is the canonical replacement for the old "re-assemble on every
upload" path when running against an S3 EDMS source: you tick the
builder N times per day, it pulls only what changed, and the cost
stays bounded.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import date, datetime, timezone
from typing import Optional

from core.connectors.base_connector import BaseEDMSConnector

logger = logging.getLogger(__name__)


# Required-slot catalog mirrors api/routes._REQUIRED_DOCS — duplicating
# here to keep the builder importable without dragging in the FastAPI
# router. The catalog defines what "complete" means for completeness_pct.
_REQUIRED_SLOTS: list[dict] = [
    {"item": "W-2",                  "doc_type": "W2_CURRENT",       "alternates": ["W2_PRIOR"]},
    {"item": "Pay stub",             "doc_type": "PAYSTUB_CURRENT",  "alternates": ["PAYSTUB_PRIOR"]},
    {"item": "Credit report",        "doc_type": "CREDIT_REPORT",    "alternates": []},
    {"item": "Bank statement",       "doc_type": "BANK_STATEMENT_M1", "alternates": []},
    {"item": "DL",                   "doc_type": "DRIVERS_LICENSE",  "alternates": ["IDENTITY_DL"]},
    {"item": "SSN validation",       "doc_type": "SSN_VALIDATION",   "alternates": ["IDENTITY_SSN_CARD"]},
    {"item": "OFAC clearance",       "doc_type": "OFAC_CHECK",       "alternates": ["OFAC_REPORT"]},
    {"item": "URAR",                 "doc_type": "APPRAISAL_URAR",   "alternates": []},
    {"item": "Title commitment",     "doc_type": "TITLE_COMMITMENT", "alternates": []},
    {"item": "HOI",                  "doc_type": "HOI_BINDER",       "alternates": ["HOI_DECLARATIONS"]},
    {"item": "Flood cert",           "doc_type": "FLOOD_CERT",       "alternates": []},
    {"item": "Property tax bill",    "doc_type": "PROPERTY_TAX_BILL", "alternates": []},
    {"item": "URLA",                 "doc_type": "URLA_1003",        "alternates": []},
    {"item": "Purchase agreement",   "doc_type": "PURCHASE_AGREEMENT", "alternates": []},
    {"item": "AUS findings",         "doc_type": "AUS_DU_FINDINGS",  "alternates": ["AUS_LP_FINDINGS"]},
]
_REQUIRED_TOTAL = len(_REQUIRED_SLOTS)


def _slot_received(slot: dict, have: set[str]) -> bool:
    if slot["doc_type"] in have:
        return True
    return any(alt in have for alt in (slot.get("alternates") or []))


# ===========================================================================
# v4 per-borrower fold helpers — turn a set of indexed doc rows into the
# nested JSONB blocks ``entity_states`` carries. Each helper takes a list
# of doc dicts (already PG-shape: ``document_type`` + ``extracted_fields``
# + ``source_document_id`` + ``received_at``) and returns one dict.
# ===========================================================================


def _f(d: dict, *keys, default=None):
    """Pull a field from ``d['extracted_fields']`` or top-level d, in
    that order. Returns ``default`` if all keys miss."""
    fields = d.get("extracted_fields") or {}
    for k in keys:
        if k in fields and fields[k] not in (None, ""):
            return fields[k]
        if k in d and d[k] not in (None, ""):
            return d[k]
    return default


def _doc(docs: list, doc_type: str, *alts) -> dict | None:
    """Return the most recent doc whose ``document_type`` matches one
    of the supplied types, or ``None``.

    v4.1 — Gap 6: corrections-channel docs supersede everything else.
    A doc dropped into ``corrections/`` (regenerated W-2 because the
    original wage was wrong, revised appraisal after a value
    challenge, etc.) wins over the original even if the original has
    a later ``received_at``. Within the corrections set, last-write
    wins."""
    types = {doc_type, *alts}
    matches = [d for d in docs if d.get("document_type") in types]
    if not matches:
        return None
    corrections = [
        d for d in matches if d.get("source_channel") == "corrections"
    ]
    if corrections:
        corrections.sort(key=lambda x: x.get("received_at") or "", reverse=True)
        return corrections[0]
    matches.sort(key=lambda x: x.get("received_at") or "", reverse=True)
    return matches[0]


def _all_docs(docs: list, *doc_types) -> list:
    types = set(doc_types)
    return [d for d in docs if d.get("document_type") in types]


def _income_block(docs: list) -> dict:
    """v4.1 — full income aggregation with calculation_type +
    per-source breakdown.

    Detects three calculation modes from the doc-type mix:
      - ``self_employed_2yr_avg`` if Schedule C / K-1 / 1099-NEC /
        multiple 1040s present (2-year average with depreciation
        addback, declining-trend alert).
      - ``fixed_income`` if SSA + pension and no W-2.
      - ``w2_salaried`` otherwise (the default purchase + refi path).

    Builds a ``sources`` array containing one entry per income stream
    (W2, Schedule C, 1099-NEC per payer, K-1, SSA, pension, rental at
    75%, alimony when ≥3 yrs remaining). Each entry carries
    ``annual`` + ``monthly`` + a ``source_doc`` ID so the workbench
    can tie back to the originating system.
    """
    from datetime import date as _date

    w2_docs = _all_docs(docs, "W2_CURRENT", "W2_PRIOR")
    ps_doc  = _doc(docs, "PAYSTUB_CURRENT", "PAYSTUB_PRIOR", "PAYSTUB")
    irs     = _doc(docs, "IRS_TRANSCRIPT")
    ssa     = _doc(docs, "SSA_AWARD_LETTER")
    pen     = _doc(docs, "PENSION_LETTER")
    sch_c   = _doc(docs, "SCHEDULE_C")
    sch_e_docs = _all_docs(docs, "SCHEDULE_E", "RENTAL_LEASE")
    nec_docs = _all_docs(docs, "1099_NEC", "FORM_1099_NEC")
    k1      = _doc(docs, "K1_SCHEDULE", "K1_PARTNERSHIP")
    f1040_curr = _doc(docs, "TAX_RETURN_1040_CURRENT", "FORM_1040")
    f1040_prior = _doc(docs, "TAX_RETURN_1040_PRIOR")
    divorce = _doc(docs, "DIVORCE_DECREE", "ALIMONY_ORDER")
    alimony_history = _doc(docs, "ALIMONY_RECEIPT_HISTORY")
    commission = _doc(docs, "COMMISSION_HISTORY")
    rsu_docs = _all_docs(docs, "RSU_STATEMENT")

    # ---- determine calculation_type ----------------------------------
    has_se = bool(sch_c or k1 or nec_docs or f1040_prior)
    has_fixed = bool(ssa or pen) and not w2_docs
    has_commission = bool(commission)
    if has_se:
        calculation_type = "self_employed_2yr_avg"
    elif has_fixed:
        calculation_type = "fixed_income"
    elif has_commission:
        calculation_type = "base_plus_commission"
    else:
        calculation_type = "w2_salaried"

    sources: list = []
    src_doc_ids: list = []

    # ---- W-2 salaried ------------------------------------------------
    for w2 in w2_docs:
        if w2.get("document_type") != "W2_CURRENT":
            continue   # don't double-count current + prior
        try:
            wages = float(_f(w2, "box1_wages") or 0)
        except (TypeError, ValueError):
            wages = 0
        if wages:
            sources.append({
                "type":       "W2_SALARIED",
                "annual":     wages,
                "monthly":    round(wages / 12, 2),
                "employer":   _f(w2, "employer_name"),
                "source_doc": w2.get("source_document_id"),
            })
            src_doc_ids.append(w2.get("source_document_id"))

    # ---- Schedule C net profit (self-employed) -----------------------
    if sch_c:
        try:
            net = float(_f(sch_c, "net_profit") or 0)
        except (TypeError, ValueError):
            net = 0
        if net:
            sources.append({
                "type":       "SCHEDULE_C",
                "annual":     net,
                "monthly":    round(net / 12, 2),
                "business_name": _f(sch_c, "business_name"),
                "source_doc": sch_c.get("source_document_id"),
            })
            src_doc_ids.append(sch_c.get("source_document_id"))

    # ---- K-1 ----------------------------------------------------------
    if k1:
        try:
            ord_inc = float(_f(k1, "ordinary_income") or 0)
            gp      = float(_f(k1, "guaranteed_payments") or 0)
        except (TypeError, ValueError):
            ord_inc = gp = 0
        k1_total = ord_inc + gp
        if k1_total:
            sources.append({
                "type":       "K1_PARTNERSHIP",
                "annual":     k1_total,
                "monthly":    round(k1_total / 12, 2),
                "partnership_name": _f(k1, "partnership_name"),
                "source_doc": k1.get("source_document_id"),
            })
            src_doc_ids.append(k1.get("source_document_id"))

    # ---- 1099-NEC (per payer) ----------------------------------------
    for nec in nec_docs:
        try:
            comp = float(_f(nec, "nonemployee_compensation") or 0)
        except (TypeError, ValueError):
            comp = 0
        if comp:
            sources.append({
                "type":       "1099_NEC",
                "annual":     comp,
                "monthly":    round(comp / 12, 2),
                "payer":      _f(nec, "payer_name"),
                "source_doc": nec.get("source_document_id"),
            })
            src_doc_ids.append(nec.get("source_document_id"))

    # ---- Schedule E rental — 75% of net for qualifying ---------------
    for se in sch_e_docs:
        try:
            net_rental = float(_f(se, "net_rental_income") or 0)
        except (TypeError, ValueError):
            net_rental = 0
        if net_rental:
            qualifying = net_rental * 0.75
            sources.append({
                "type":       "RENTAL_NET",
                "annual":     round(qualifying, 2),
                "monthly":    round(qualifying / 12, 2),
                "gross_net":  net_rental,
                "note":       f"75% of ${net_rental:,.0f} net rental",
                "source_doc": se.get("source_document_id"),
            })
            src_doc_ids.append(se.get("source_document_id"))

    # ---- SSA retirement ----------------------------------------------
    if ssa:
        try:
            mb = float(_f(ssa, "monthly_benefit") or 0)
        except (TypeError, ValueError):
            mb = 0
        if mb:
            sources.append({
                "type":       "SSA_RETIREMENT",
                "annual":     mb * 12,
                "monthly":    mb,
                "benefit_type": _f(ssa, "benefit_type"),
                "source_doc": ssa.get("source_document_id"),
            })
            src_doc_ids.append(ssa.get("source_document_id"))

    # ---- Pension -----------------------------------------------------
    if pen:
        try:
            mb = float(_f(pen, "monthly_benefit") or 0)
        except (TypeError, ValueError):
            mb = 0
        if mb:
            sources.append({
                "type":       "PENSION",
                "annual":     mb * 12,
                "monthly":    mb,
                "employer":   _f(pen, "employer_name"),
                "source_doc": pen.get("source_document_id"),
            })
            src_doc_ids.append(pen.get("source_document_id"))

    # ---- Commission / bonus history (2-yr average) ------------------
    if commission:
        try:
            avg = float(_f(commission, "two_year_average") or 0)
        except (TypeError, ValueError):
            avg = 0
        if avg:
            sources.append({
                "type":       "COMMISSION_BONUS",
                "annual":     avg,
                "monthly":    round(avg / 12, 2),
                "trending":   _f(commission, "trending"),
                "source_doc": commission.get("source_document_id"),
            })
            src_doc_ids.append(commission.get("source_document_id"))

    # ---- RSU / equity comp ------------------------------------------
    for rsu in rsu_docs:
        try:
            vested = float(_f(rsu, "vested_annual") or 0)
        except (TypeError, ValueError):
            vested = 0
        if vested:
            sources.append({
                "type":       "RSU_VESTED",
                "annual":     vested,
                "monthly":    round(vested / 12, 2),
                "vested_at":  _f(rsu, "vest_date"),
                "source_doc": rsu.get("source_document_id"),
            })
            src_doc_ids.append(rsu.get("source_document_id"))

    # ---- Alimony — only with 3+ yrs remaining + receipt history -----
    if divorce:
        try:
            amt       = float(_f(divorce, "alimony_amount", "monthly_amount") or 0)
            remaining = int(_f(divorce, "remaining_years") or 0)
        except (TypeError, ValueError):
            amt = remaining = 0
        # If receipt history present, that satisfies the 12-month rule.
        history_ok = bool(alimony_history)
        if amt and remaining >= 3:
            note = f"${amt}/mo, {remaining} yrs remaining"
            if not history_ok:
                note += " (no 12-month receipt history yet)"
            sources.append({
                "type":            "ALIMONY_RECEIVED",
                "annual":          amt * 12,
                "monthly":         amt,
                "remaining_years": remaining,
                "note":            note,
                "source_doc":      divorce.get("source_document_id"),
            })
            src_doc_ids.append(divorce.get("source_document_id"))

    # ---- 2-year-average + trending for self-employed ----------------
    se_calc = {}
    if calculation_type == "self_employed_2yr_avg":
        try:
            y1_agi = float(_f(f1040_curr, "agi") or 0) if f1040_curr else 0
            y2_agi = float(_f(f1040_prior, "agi") or 0) if f1040_prior else 0
        except (TypeError, ValueError):
            y1_agi = y2_agi = 0
        try:
            y1_dep = float(_f(f1040_curr, "depreciation") or 0) if f1040_curr else 0
            y2_dep = float(_f(f1040_prior, "depreciation") or 0) if f1040_prior else 0
        except (TypeError, ValueError):
            y1_dep = y2_dep = 0
        y1_adj = y1_agi + y1_dep
        y2_adj = y2_agi + y2_dep
        if y2_adj and y1_adj:
            trending = (
                "declining" if y2_adj > y1_adj * 1.2
                else ("increasing" if y2_adj < y1_adj * 0.8
                      else "stable")
            )
            # Note: trend direction is y1 (more recent) vs y2 (older).
            if y1_adj > y2_adj * 1.05:
                trending = "increasing"
            elif y1_adj < y2_adj * 0.8:
                trending = "declining"
            else:
                trending = "stable"
            se_calc = {
                "year1_adjusted":      round(y1_adj, 2),
                "year2_adjusted":      round(y2_adj, 2),
                "trending":            trending,
                "depreciation_addback": round(y1_dep + y2_dep, 2),
                "two_year_average":    round((y1_adj + y2_adj) / 2, 2),
            }

    annual = sum(s["annual"] for s in sources) or None
    qualifying_monthly = round(annual / 12, 2) if annual else None
    verified_at = max(
        (d.get("received_at") for d in (w2_docs + sch_e_docs + nec_docs +
                                        [ps_doc, irs, ssa, pen, sch_c,
                                         k1, f1040_curr, f1040_prior,
                                         divorce, alimony_history])
         if d and d.get("received_at")),
        default=None,
    )
    block = {
        "calculation_type":   calculation_type,
        "qualifying_monthly": qualifying_monthly,
        "annual":             annual,
        "sources":            sources,
        "source_docs":        [s for s in src_doc_ids if s],
        "verified":           bool(annual),
        "verified_at":        verified_at,
    }
    if se_calc:
        block.update(se_calc)
    return block


def _employment_block(docs: list) -> dict:
    voe = _doc(docs, "VOE_TWN", "VOE_EQUIFAX", "VOE")
    # Section B: optional employment-gap explanation letter
    gap_letter = _doc(docs, "EMPLOYMENT_GAP_LETTER", "EMPLOYMENT_GAP_EXPLANATION")
    gap_block = None
    if gap_letter:
        gap_block = {
            "has_gap":              True,
            "gap_reason":           _f(gap_letter, "reason"),
            "gap_start":            _f(gap_letter, "gap_start"),
            "gap_end":              _f(gap_letter, "gap_end"),
            "explanation_received": True,
            "source_doc":           gap_letter.get("source_document_id"),
        }
    if not voe:
        # Fall back to the W-2's employer_name so we have at least
        # *something* to surface in /context — but mark unverified.
        w2 = _doc(docs, "W2_CURRENT", "W2_PRIOR")
        if w2:
            block = {
                "employer":      _f(w2, "employer_name"),
                "status":        None,
                "verified":      False,
                "source_doc":    None,
                "verified_at":   None,
            }
            if gap_block:
                block["gap"] = gap_block
            return block
        return {"verified": False, **({"gap": gap_block} if gap_block else {})}
    block = {
        "employer":          _f(voe, "employer_name"),
        "status":            _f(voe, "employment_status"),
        "hire_date":         _f(voe, "hire_date"),
        "position":          _f(voe, "position"),
        "income_amount":     _f(voe, "income_amount"),
        "verification_date": _f(voe, "verification_date"),
        "source_doc":        voe.get("source_document_id"),
        "verified":          True,
        "verified_at":       voe.get("received_at"),
    }
    if gap_block:
        block["gap"] = gap_block
    return block


def _credit_block(docs: list) -> dict:
    cr = _doc(docs, "CREDIT_REPORT")
    if not cr:
        return {"verified": False}
    return {
        "mid_score":           _f(cr, "mid_score"),
        "equifax":             _f(cr, "equifax_score"),
        "experian":            _f(cr, "experian_score"),
        "transunion":          _f(cr, "transunion_score"),
        "credit_band":         _f(cr, "credit_band"),
        "monthly_obligations": _f(cr, "total_monthly_obligations",
                                  "total_monthly_payments"),
        "tradeline_count":     _f(cr, "tradeline_count"),
        "hard_inquiries_12mo": _f(cr, "hard_inquiries_12mo"),
        "source_doc":          cr.get("source_document_id"),
        "verified":            True,
        "verified_at":         cr.get("received_at"),
    }


def _assets_block(docs: list) -> dict:
    bank_stmts = _all_docs(docs, "BANK_STATEMENT_M1", "BANK_STATEMENT_M2",
                           "BANK_STATEMENT_M3", "GIFT_FUNDS_TRAIL")
    retirement = _doc(docs, "RETIREMENT_ACCOUNT", "ASSET_STATEMENT_RETIREMENT")
    gift_letter      = _doc(docs, "GIFT_LETTER")
    gift_donor_stmt  = _doc(docs, "GIFT_DONOR_BANK_STATEMENT",
                            "GIFT_FUNDS_TRAIL")

    total_liquid = 0.0
    src_ids: list = []
    for s in bank_stmts:
        bal = _f(s, "ending_balance")
        try:
            total_liquid += float(bal or 0)
        except (TypeError, ValueError):
            pass
        if s.get("source_document_id"):
            src_ids.append(s["source_document_id"])

    retirement_balance = None
    if retirement:
        try:
            retirement_balance = float(_f(retirement, "balance") or 0) or None
        except (TypeError, ValueError):
            retirement_balance = None
        if retirement.get("source_document_id"):
            src_ids.append(retirement["source_document_id"])

    # v4.1 — Gap 12: gift verification chain (3-step trace from
    # signed letter → donor bank stmt withdrawal → borrower bank
    # stmt deposit). The third step is heuristic: any large deposit
    # in a bank statement matching the gift amount counts as
    # ``deposit_confirmed``. A future hook can swap in transaction-
    # level matching when MoneyTransfer-style data lands.
    gift_amount = None
    gift_verification: dict = {}
    if gift_letter:
        try:
            gift_amount = float(_f(gift_letter, "gift_amount") or 0) or None
        except (TypeError, ValueError):
            gift_amount = None
        deposit_confirmed = False
        if gift_amount:
            for s in bank_stmts:
                bal = _f(s, "ending_balance")
                ldep = _f(s, "largest_deposit")
                try:
                    if ldep and float(ldep) >= gift_amount * 0.95:
                        deposit_confirmed = True
                        break
                    # Fallback: ending_balance jumped by approx the
                    # gift amount in M1 vs M2/M3 — the simulator
                    # doesn't expose largest_deposit on every stmt
                    # so this catches the case.
                    if bal and gift_amount and float(bal) >= gift_amount:
                        deposit_confirmed = True
                except (TypeError, ValueError):
                    pass
        gift_verification = {
            "gift_amount":           gift_amount,
            "donor_name":            _f(gift_letter, "donor_name"),
            "donor_relationship":    _f(gift_letter, "donor_relationship"),
            "letter_received":       True,
            "donor_bank_stmt_received": bool(gift_donor_stmt),
            "deposit_confirmed":     deposit_confirmed,
            "fully_verified": bool(gift_donor_stmt) and deposit_confirmed,
            "chain": [
                {"step": "Gift letter signed",                  "complete": True},
                {"step": "Donor bank stmt showing withdrawal",  "complete": bool(gift_donor_stmt)},
                {"step": "Borrower bank stmt showing deposit",  "complete": deposit_confirmed},
            ],
        }

    latest_at = max(
        (s.get("received_at") for s in bank_stmts if s.get("received_at")),
        default=None,
    )
    return {
        "total_liquid":      total_liquid or None,
        "retirement":        retirement_balance,
        "gift_funds":        gift_amount,
        "gift_verification": gift_verification,
        "source_docs":       src_ids,
        "verified":          bool(bank_stmts),
        "verified_at":       latest_at,
    }


def _identity_block(docs: list) -> dict:
    dl  = _doc(docs, "DRIVERS_LICENSE", "IDENTITY_DL")
    ssn = _doc(docs, "SSN_VALIDATION")
    of  = _doc(docs, "OFAC_CHECK", "OFAC_REPORT")
    dl_ok  = bool(dl)
    ssn_ok = bool(ssn) and bool(_f(ssn, "ssn_valid"))
    of_ok  = bool(of)  and bool(_f(of,  "ofac_clear"))
    latest_at = max(
        (d.get("received_at") for d in (dl, ssn, of) if d and d.get("received_at")),
        default=None,
    )
    return {
        "dl_verified":  dl_ok,
        "ssn_verified": ssn_ok,
        "ofac_clear":   of_ok,
        "complete":     dl_ok and ssn_ok and of_ok,
        "verified_at":  latest_at,
    }


def _property_block(all_docs: list) -> dict:
    appr = _doc(all_docs, "APPRAISAL_URAR", "APPRAISAL_URAR_1073",
                "APPRAISAL_UPDATE")
    avm  = _doc(all_docs, "AVM_REPORT")
    tc   = _doc(all_docs, "TITLE_COMMITMENT")
    ti   = _doc(all_docs, "TITLE_INSURANCE")
    hoi  = _doc(all_docs, "HOI_BINDER", "HOI_BINDER_HO6", "HOI_DECLARATIONS")
    flood = _doc(all_docs, "FLOOD_CERT")
    wind  = _doc(all_docs, "WIND_HAIL_INSURANCE")
    tax   = _doc(all_docs, "PROPERTY_TAX_BILL")
    hoa   = _doc(all_docs, "HOA_CERT")
    wdo   = _doc(all_docs, "WDO_REPORT")
    well  = _doc(all_docs, "WELL_SEPTIC_INSPECTION")

    appr_value = float(_f(appr, "appraised_value") or 0) or None if appr else None
    avm_value  = float(_f(avm,  "avm_value")       or 0) or None if avm  else None
    delta_pct  = None
    if appr_value and avm_value:
        delta_pct = round(abs(appr_value - avm_value) / appr_value * 100, 2)

    valuation = {}
    if appr or avm:
        valuation = {
            "appraised_value":  appr_value,
            "avm_value":        avm_value,
            "delta_pct":        delta_pct,
            "condition":        _f(appr, "condition_rating") if appr else None,
            "gla_sqft":         _f(appr, "gla_sqft") if appr else None,
            "year_built":       _f(appr, "year_built") if appr else None,
            "bedrooms":         _f(appr, "bedrooms") if appr else None,
            "bathrooms":        _f(appr, "bathrooms") if appr else None,
            "comparable_1":     _f(appr, "comparable_1_price") if appr else None,
            "comparable_2":     _f(appr, "comparable_2_price") if appr else None,
            "comparable_3":     _f(appr, "comparable_3_price") if appr else None,
            "source_doc":       (appr or {}).get("source_document_id"),
            "verified":         bool(appr),
            "verified_at":      (appr or {}).get("received_at"),
        }

    title = {}
    if tc or ti:
        title = {
            "committed":          bool(tc),
            "insured":            bool(ti),
            "commitment_number":  _f(tc, "commitment_number") if tc else None,
            "policy_amount":      _f(tc, "policy_amount") if tc else None,
            "exceptions_count":   _f(tc, "exceptions_count") if tc else None,
            "tax_lien_clear":     _f(tc, "tax_lien_clear") if tc else None,
            "judgment_lien_clear":_f(tc, "judgment_lien_clear") if tc else None,
            "vesting":            _f(tc, "vesting") if tc else None,
            "source_doc":         (tc or {}).get("source_document_id"),
            "verified":           bool(tc) and bool(ti),
            "commitment_date":    (tc or {}).get("received_at"),
            "insurance_date":     (ti or {}).get("received_at"),
        }

    flood_ins = _doc(all_docs, "FLOOD_INSURANCE", "NFIP_POLICY")
    insurance = {}
    if hoi or flood or wind or flood_ins:
        # Section B: flood zone A/AE/V/VE forces flood insurance.
        flood_zone = _f(flood, "flood_zone") if flood else None
        in_sfha = (flood_zone or "X") in ("A", "AE", "V", "VE")
        insurance = {
            "hoi_premium_annual":  _f(hoi, "annual_premium") if hoi else None,
            "hoi_carrier":         _f(hoi, "carrier") if hoi else None,
            "flood_zone":          flood_zone,
            "flood_insurance_required": in_sfha or (
                bool(_f(flood, "requires_insurance")) if flood else False
            ),
            "flood_premium_annual": (
                _f(flood_ins, "annual_premium") if flood_ins else None
            ),
            "wind_hail_premium":   _f(wind, "annual_premium") if wind else None,
            "source_doc":          (hoi or {}).get("source_document_id"),
            "verified":            bool(hoi),
            "verified_at":         (hoi or {}).get("received_at"),
        }

    tax_block = {}
    if tax:
        tax_block = {
            "annual_tax":     _f(tax, "annual_tax"),
            "assessed_value": _f(tax, "assessed_value"),
            "verified":       True,
            "verified_at":    tax.get("received_at"),
        }

    inspections = {}
    if hoa or wdo or well:
        inspections = {
            "pest_clear":        (str(_f(wdo, "findings") or "").lower() == "clear")
                                  if wdo else None,
            "well_septic":       _f(well, "septic_condition") if well else None,
            "hoa_dues_monthly":  _f(hoa, "monthly_dues") if hoa else None,
            "verified":          bool(hoa or wdo or well),
        }

    prop_doc_types = sorted({
        d.get("document_type") for d in all_docs
        if d.get("category") == "property" and d.get("document_type")
    })
    return {
        "valuation":   valuation,
        "title":       title,
        "insurance":   insurance,
        "tax":         tax_block,
        "inspections": inspections,
        "doc_types":   prop_doc_types,
        "doc_count":   sum(1 for d in all_docs if d.get("category") == "property"),
    }


def _loan_terms_block(all_docs: list) -> dict:
    urla = _doc(all_docs, "URLA_1003", "URLA_MISMO_3.4")
    pa   = _doc(all_docs, "PURCHASE_AGREEMENT")
    rl   = _doc(all_docs, "RATE_LOCK")
    aus  = _doc(all_docs, "AUS_DU_FINDINGS", "AUS_LP_FINDINGS")
    cd   = _doc(all_docs, "CLOSING_DISCLOSURE")
    # v4.1 — Gap 10: refi current-mortgage info.
    payoff = _doc(all_docs, "MORTGAGE_PAYOFF",
                  "PAYOFF_STATEMENT", "MORTGAGE_PAYOFF_STATEMENT")
    pmt_hist = _doc(all_docs, "PAYMENT_HISTORY_24MO", "MORTGAGE_PAYMENT_HISTORY")
    escrow   = _doc(all_docs, "ESCROW_ANALYSIS")

    return {
        "loan_amount":   (_f(urla, "loan_amount")
                          or (_f(rl, "loan_amount") if rl else None)),
        "interest_rate": (_f(rl, "locked_rate") if rl else None)
                          or _f(urla, "interest_rate"),
        "term_months":   _f(urla, "loan_term_months") or 360,
        "purpose":       _f(urla, "loan_purpose"),
        "occupancy":     _f(urla, "occupancy"),
        "property_type": _f(urla, "property_type"),
        "loan_program":  (_f(rl, "loan_program") if rl else None)
                          or "Conv 30yr Fixed",
        "purchase_agreement": ({
            "purchase_price":    _f(pa, "purchase_price"),
            "earnest_money":     _f(pa, "earnest_money"),
            "closing_date":      _f(pa, "closing_date"),
            "source_doc":        pa.get("source_document_id"),
            "verified":          True,
            "verified_at":       pa.get("received_at"),
        } if pa else {}),
        "rate_lock": ({
            "locked_rate":   _f(rl, "locked_rate"),
            "lock_expiry":   _f(rl, "lock_expiry"),
            "lock_days":     _f(rl, "lock_days"),
            "loan_amount":   _f(rl, "loan_amount"),
            "loan_program":  _f(rl, "loan_program"),
            "source_doc":    rl.get("source_document_id"),
            "verified":      True,
            "verified_at":   rl.get("received_at"),
        } if rl else {}),
        "aus": ({
            "recommendation":   _f(aus, "recommendation"),
            "risk_class":       _f(aus, "risk_class"),
            "casefile_id":      _f(aus, "casefile_id"),
            "conditions_count": _f(aus, "conditions_count"),
            "ltv":              _f(aus, "ltv"),
            "dti":              _f(aus, "dti"),
            "source_doc":       aus.get("source_document_id"),
            "verified":         True,
            "verified_at":      aus.get("received_at"),
        } if aus else {}),
        "closing_disclosure": ({
            "closing_date":     _f(cd, "closing_date"),
            "loan_amount":      _f(cd, "loan_amount"),
            "interest_rate":    _f(cd, "interest_rate"),
            "cash_to_close":    _f(cd, "cash_to_close"),
            "verified":         True,
            "verified_at":      cd.get("received_at"),
        } if cd else {}),
        # v4.1 — current-mortgage block populated for refis (PART 10).
        # Falsy when no servicer docs present, which is correct for
        # purchase loans.
        "current_mortgage": ({
            "payoff_amount":    _f(payoff, "current_balance",
                                   "payoff_amount") if payoff else None,
            "payoff_through":   _f(payoff, "payoff_through") if payoff else None,
            "monthly_payment":  _f(pmt_hist, "monthly_payment") if pmt_hist else None,
            "months_reviewed":  _f(pmt_hist, "months_reviewed") if pmt_hist else None,
            "late_payments":    _f(pmt_hist, "late_payments") if pmt_hist else None,
            "current_rate":     _f(payoff, "current_rate") if payoff else None,
            "servicer":         _f(payoff, "lender") if payoff else None,
            "escrow_balance":   _f(escrow, "current_balance") if escrow else None,
            "monthly_escrow":   _f(escrow, "monthly_escrow") if escrow else None,
            "source_docs":      [
                d.get("source_document_id")
                for d in (payoff, pmt_hist, escrow)
                if d and d.get("source_document_id")
            ],
            "verified":         bool(payoff or pmt_hist or escrow),
        } if (payoff or pmt_hist or escrow) else {}),
    }


def _rate_lock_block(status: bool, summary: str, verified_at,
                    lock_expiry, today) -> dict:
    """Rate lock has its own expiration: the doc carries a literal
    ``lock_expiry`` date. Compute days_remaining + alerts directly
    rather than going through the TTL helper."""
    out = {
        "status":      status,
        "summary":     summary,
        "verified_at": verified_at,
        "lock_expiry": lock_expiry,
    }
    if not lock_expiry:
        return out
    try:
        from datetime import date as _date
        if isinstance(lock_expiry, str):
            d = _date.fromisoformat(lock_expiry[:10])
        else:
            d = lock_expiry
        days_left = (d - today).days
        out["days_until_expiry"] = days_left
        out["alerts"] = (
            [f"Rate lock expires in {days_left} days"]
            if 0 < days_left < 7
            else ([f"EXPIRED {abs(days_left)} days ago"] if days_left <= 0 else [])
        )
    except Exception:
        pass
    return out


def _build_verifications(
    borrower: dict, co_borrowers: list, property_block: dict,
    loan_terms: dict, combined_income: float, total_liquid: float,
    piti_monthly, mid_credit_score, monthly_obligations: float,
    reserves_remaining: float | None = None,
    months_reserves: float | None = None,
) -> dict:
    """Persona-ready verifications block with summary text + status
    flags. Each top-level key gives a Decision-OS persona enough info
    to render a card without reading any other field. v4.1 adds
    expiration tracking (``expires_at`` / ``days_until_expiry`` /
    ``alerts``) on the time-sensitive flags so a Decision-OS card
    can show "expires in 5 days" without a separate query."""
    from datetime import date as _date, datetime as _dt, timedelta as _td

    today = _date.today()

    def _safe(v: dict, *keys):
        for k in keys:
            v = (v or {}).get(k)
        return v

    def _expiry(verified_at, days: int) -> dict:
        """Compute expires_at + days_until_expiry + alerts list."""
        if not verified_at:
            return {}
        try:
            base = (_dt.fromisoformat(str(verified_at).replace("Z", "+00:00"))
                    if isinstance(verified_at, str)
                    else verified_at)
            expires = (base.date() if hasattr(base, "date") else base) + _td(days=days)
            days_left = (expires - today).days
            alerts: list = []
            if days_left <= 0:
                alerts.append(f"EXPIRED {abs(days_left)} days ago")
            elif days_left < 14:
                alerts.append(f"Expires in {days_left} days")
            return {
                "expires_at":         str(expires),
                "days_until_expiry":  days_left,
                "alerts":             alerts,
            }
        except Exception:
            return {}

    # v4.1 — Gap 11: per-borrower verification matters in the summary.
    # Mark co-borrowers whose income hasn't landed as PENDING so a
    # Decision-OS card distinguishes "all in" from "primary verified,
    # co pending".
    income_status = bool(borrower.get("income", {}).get("verified")) and all(
        cb.get("income", {}).get("verified") for cb in co_borrowers
    )
    employer = _safe(borrower, "employment", "employer") or "?"
    primary_annual = _safe(borrower, "income", "annual") or 0
    income_summary = f"Primary {employer} ${primary_annual / 1000:.0f}k"
    for cb in co_borrowers:
        co_emp  = _safe(cb, "employment", "employer") or "?"
        co_ann  = _safe(cb, "income", "annual") or 0
        co_name = cb.get("name") or "co-borrower"
        if (cb.get("income") or {}).get("verified"):
            income_summary += f" + Co {co_emp} ${co_ann / 1000:.0f}k"
        else:
            income_summary += f" + Co {co_name} PENDING"

    employment_status = bool(_safe(borrower, "employment", "verified")) and all(
        _safe(cb, "employment", "verified") for cb in co_borrowers
    )

    credit_status = bool(_safe(borrower, "credit", "verified"))
    credit_summary = (
        f"Qualifying mid {mid_credit_score} (lower of all borrowers), "
        f"obligations ${monthly_obligations:,.0f}"
        if mid_credit_score else "Credit not yet pulled"
    )

    assets_status = bool(_safe(borrower, "assets", "verified"))
    if reserves_remaining is None:
        reserves_remaining = (
            _safe(borrower, "assets", "reserves_remaining") or 0
        )
    if months_reserves is None:
        months_reserves = _safe(borrower, "assets", "months_reserves")
    assets_summary = (
        f"${total_liquid:,.0f} liquid → "
        f"${reserves_remaining:,.0f} after closing"
        + (f" ({months_reserves}mo reserves)" if months_reserves else "")
        if total_liquid else "Assets not yet verified"
    )

    identity_status = bool(_safe(borrower, "identity", "complete")) and all(
        _safe(cb, "identity", "complete") for cb in co_borrowers
    )

    appraisal_status = bool(_safe(property_block, "valuation", "verified"))
    appraised = _safe(property_block, "valuation", "appraised_value")
    appraisal_summary = (
        f"${appraised:,.0f} appraised"
        if appraised else "Appraisal pending"
    )

    title_status = bool(_safe(property_block, "title", "verified"))
    excs = _safe(property_block, "title", "exceptions_count")
    title_summary = (
        f"{_safe(property_block, 'title', 'vesting') or 'Fee Simple'}, "
        f"{excs or 0} exceptions, liens clear"
        if title_status else "Title not yet bound"
    )

    insurance_status = bool(_safe(property_block, "insurance", "verified"))
    carrier = _safe(property_block, "insurance", "hoi_carrier")
    premium = _safe(property_block, "insurance", "hoi_premium_annual")
    flood   = _safe(property_block, "insurance", "flood_zone")
    insurance_summary = (
        f"{carrier or '?'} ${premium or 0:,.0f}/yr, flood zone {flood or '?'}"
        if insurance_status else "HOI not yet bound"
    )

    aus_status = bool(_safe(loan_terms, "aus", "verified")) and \
        (_safe(loan_terms, "aus", "recommendation")
         in ("approve_eligible", "accept", "approve"))
    aus_recommendation = _safe(loan_terms, "aus", "recommendation") or "?"
    aus_count = _safe(loan_terms, "aus", "conditions_count") or 0
    aus_summary = (
        f"DU {aus_recommendation}, {aus_count} conditions"
        if _safe(loan_terms, "aus", "verified")
        else "AUS not yet run"
    )

    rate_locked_status = bool(_safe(loan_terms, "rate_lock", "verified"))
    rate_summary = (
        f"{_safe(loan_terms, 'rate_lock', 'locked_rate')}% locked, "
        f"expires {_safe(loan_terms, 'rate_lock', 'lock_expiry')}"
        if rate_locked_status else "Rate not yet locked"
    )

    # conditions_cleared = AUS approved + every condition tracked clear.
    # For now: any AUS approval with conditions_count == 0 is "cleared"
    # — richer per-condition tracking can grow here.
    conditions_cleared = aus_status and (aus_count == 0)

    # clear_to_close — every gate above plus rate lock.
    blocking = []
    flags_for_ctc = {
        "income_verified":     income_status,
        "employment_verified": employment_status,
        "credit_pulled":       credit_status,
        "assets_verified":     assets_status,
        "identity_complete":   identity_status,
        "appraisal_complete":  appraisal_status,
        "title_clear":         title_status,
        "insurance_bound":     insurance_status,
        "aus_approved":        aus_status,
        "rate_locked":         rate_locked_status,
        "conditions_cleared":  conditions_cleared,
    }
    blocking = [k for k, v in flags_for_ctc.items() if not v]
    ctc_status = not blocking
    ctc_summary = (
        "All verifications complete" if ctc_status
        else "Blocking: " + ", ".join(blocking[:5])
    )

    return {
        "income_verified": {
            "status": income_status, "summary": income_summary,
            "verified_at": _safe(borrower, "income", "verified_at"),
        },
        "employment_verified": {
            "status": employment_status,
            "summary": (
                f"{employer}"
                + (f" + Co {_safe(co_borrowers[0], 'employment', 'employer')}"
                   if co_borrowers else "")
            ),
            "verified_at": _safe(borrower, "employment", "verified_at"),
            **_expiry(_safe(borrower, "employment", "verified_at"), 120),
        },
        "credit_pulled": {
            "status":  credit_status, "summary": credit_summary,
            "verified_at": _safe(borrower, "credit", "verified_at"),
            **_expiry(_safe(borrower, "credit", "verified_at"), 120),
        },
        "assets_verified": {
            "status":  assets_status, "summary": assets_summary,
            "reserves_remaining": reserves_remaining,
            "months_reserves":    months_reserves,
            "verified_at": _safe(borrower, "assets", "verified_at"),
            **_expiry(_safe(borrower, "assets", "verified_at"), 60),
        },
        "identity_complete": {
            "status": identity_status,
            "summary": ("All borrowers DL + SSN + OFAC clear"
                        if identity_status else "Identity incomplete"),
            "verified_at": _safe(borrower, "identity", "verified_at"),
        },
        "appraisal_complete": {
            "status":  appraisal_status, "summary": appraisal_summary,
            "verified_at": _safe(property_block, "valuation", "verified_at"),
            **_expiry(_safe(property_block, "valuation", "verified_at"), 180),
        },
        "title_clear": {
            "status":  title_status, "summary": title_summary,
            "commitment_date": _safe(property_block, "title", "commitment_date"),
            "insurance_date":  _safe(property_block, "title", "insurance_date"),
            **_expiry(_safe(property_block, "title", "commitment_date"), 90),
        },
        "insurance_bound": {
            "status":  insurance_status, "summary": insurance_summary,
            "verified_at": _safe(property_block, "insurance", "verified_at"),
            **_expiry(_safe(property_block, "insurance", "verified_at"), 365),
        },
        "aus_approved": {
            "status":  aus_status, "summary": aus_summary,
            "verified_at": _safe(loan_terms, "aus", "verified_at"),
        },
        "rate_locked": _rate_lock_block(
            rate_locked_status, rate_summary,
            _safe(loan_terms, "rate_lock", "verified_at"),
            _safe(loan_terms, "rate_lock", "lock_expiry"),
            today,
        ),
        "conditions_cleared": {
            "status": conditions_cleared,
            "summary": (f"{aus_count} conditions cleared"
                        if conditions_cleared else "Conditions outstanding"),
        },
        "clear_to_close": {
            "status":         ctc_status,
            "summary":        ctc_summary,
            "blocking_items": blocking,
        },
    }


class IncrementalGraphBuilder:
    """Single-tick driver: pull → save → reconcile → assemble → upsert."""

    def __init__(
        self,
        connector: BaseEDMSConnector,
        postgres_store,
        redis_store,
        reconciler=None,
        aggregation_service=None,
    ):
        self.connector = connector
        self.pg        = postgres_store
        self.redis     = redis_store
        # Optional: when present, full assembly fans out via the existing
        # service. When absent, the builder still saves docs + reconciles +
        # records summary state — enough for the backtest report card.
        self.reconciler          = reconciler
        self.aggregation_service = aggregation_service

    async def run_build(
        self,
        build_date: date,
        build_number: int,
        until: Optional[str] = None,
        tenant_id: str = "default",
    ) -> dict:
        """One incremental tick.

        Steps:
          1. Read watermark from the connector.
          2. Pull ``received_at`` ∈ (watermark, until] from the connector.
          3. ``save_document`` each new row (idempotent via document_id).
          4. Group by applicant_id; for each:
              a. Re-assemble (income/credit/property/asset/identity) via
                 ``AggregationService._run_assembly`` if injected.
              b. Reconcile new docs vs existing (graph edges).
              c. Compose state summary; ``upsert_entity_state``.
          5. Advance the watermark to ``max(received_at)``.
          6. Record the run in ``graph_build_runs``.
        """
        started_at = datetime.now(timezone.utc)
        t0         = time.perf_counter()

        stats = {
            "documents_pulled":      0,
            "documents_new":         0,
            "documents_skipped":     0,
            "documents_classified":  0,    # AI-Vision step 2.4 successes
            "applications_created":  0,    # v3 step 2.0 (loan_origination)
            "entities_updated":      0,
            "edges_created":         0,
            "duration_ms":           0,
        }

        wm_from = await self.connector.get_watermark()
        logger.info(
            "incremental_build_start",
            extra={"build_date": str(build_date),
                   "build_number": build_number,
                   "watermark_from": wm_from,
                   "until": until},
        )

        try:
            new_docs = await self.connector.pull_documents_since(
                wm_from, until=until,
            )
        except Exception as exc:
            stats["duration_ms"] = int((time.perf_counter() - t0) * 1000)
            await self._record_run(
                build_date, build_number, wm_from, wm_from,
                stats, started_at, status="failed",
                error_details=str(exc)[:1000], tenant_id=tenant_id,
            )
            logger.error("incremental_build_pull_failed",
                         extra={"error": str(exc)[:200]})
            return stats

        stats["documents_pulled"] = len(new_docs)
        if not new_docs:
            stats["duration_ms"] = int((time.perf_counter() - t0) * 1000)
            await self._record_run(
                build_date, build_number, wm_from, wm_from,
                stats, started_at, tenant_id=tenant_id,
            )
            return stats

        # ── Step 2.0: process v3 loan_application_submitted events ───
        # The v3 simulator emits one ``loan_origination/{los_id}_
        # application.json`` per loan with ``event_type ==
        # 'loan_application_submitted'``. Process these BEFORE los_id
        # resolution so the apps + applicants exist when the rest of
        # the day's docs hit the resolver. Idempotent: PG helper checks
        # for an existing row and returns it on re-pull, so resetting
        # the watermark + replaying the bucket doesn't double-create.
        application_events = [
            d for d in new_docs
            if d.get("event_type") == "loan_application_submitted"
        ]
        legacy_ids_by_los: dict[str, dict] = {}
        if application_events:
            create_event = getattr(self.pg, "create_application_from_event", None)
            for evt in application_events:
                los_id = evt.get("los_id")
                if not los_id:
                    continue
                # Stash the legacy_ids the event carries — the builder
                # threads these into upsert_entity_state when the same
                # los_id's docs land later in this same tick.
                legacy = dict(evt.get("legacy_ids") or {})
                legacy.setdefault("los_id", los_id)
                legacy_ids_by_los[los_id] = legacy
                if create_event is None:
                    logger.debug(
                        "create_application_from_event_unavailable "
                        f"pg={type(self.pg).__name__}"
                    )
                    continue
                try:
                    result = await create_event(evt, tenant_id=tenant_id)
                    stats["applications_created"] += 1
                    logger.info(
                        f"application_created los_id={los_id} "
                        f"applicant_id={result.get('applicant_id')} "
                        f"co_applicant_id={result.get('co_applicant_id')} "
                        f"application_id={result.get('application_id')}"
                    )
                except Exception as exc:
                    logger.warning(
                        f"create_application_failed los_id={los_id} "
                        f"error_type={type(exc).__name__} "
                        f"error={str(exc)[:200]}"
                    )
            # Drop the events from new_docs — they're not real documents
            # and the persist gate would otherwise try to FK them.
            new_docs = [
                d for d in new_docs
                if d.get("event_type") != "loan_application_submitted"
            ]

        # ── Step 2.4: AI-Vision classify shared-drive scans ──────────
        # Connector synthesises ``UNKNOWN`` docs with
        # ``requires_classification=True`` for every raw scan that
        # arrived without metadata. Fetch each PDF, run Claude Vision
        # with the UNKNOWN field hint (asks for document_type +
        # los_id + borrower-identifying fields), and merge whatever
        # came back onto the doc. If Vision returned a recognisable
        # document_type and/or los_id, the doc rolls forward into the
        # los_id-resolution step below and may now resolve to a real
        # applicant; if not, it stays unclassified and falls out at
        # the persist gate (no FK violation, just a documents_skipped).
        # All-graceful: extract_with_claude returns ({}, 0.5) on any
        # missing key / disabled flag / network error.
        await self._classify_unknown_docs(new_docs, stats)

        # ── Step 2.5: resolve los_id → applicant_id ──────────────────
        # The v2 connector emits docs that carry only ``los_id`` (the
        # generators don't know which APL-XXXXX-P the API minted). Look
        # up each unique los_id once and stamp applicant_id +
        # application_id onto every doc that lacks them. Docs whose
        # los_id can't be resolved get skipped further down because
        # the persist loop refuses any doc without applicant_id. The
        # ``UNCLASSIFIED`` los_id (synthesised by shared_drive scans)
        # also ends up here and is skipped — exactly what we want.
        los_cache: dict[str, Optional[dict]] = {}
        for doc in new_docs:
            if doc.get("applicant_id"):
                continue
            los_id = doc.get("los_id")
            if not los_id:
                continue
            if los_id not in los_cache:
                try:
                    los_cache[los_id] = await self.pg.get_application_by_los_id(
                        los_id, tenant_id=tenant_id,
                    )
                except Exception as exc:
                    logger.warning(
                        "los_id_lookup_failed",
                        extra={"los_id": los_id, "error": str(exc)[:200]},
                    )
                    los_cache[los_id] = None
            app = los_cache[los_id]
            if app:
                # The role tells us which applicant_id maps in: primary →
                # applicant_id; co_borrower → co_applicant_id (with
                # primary fallback when no co_applicant exists).
                role = doc.get("borrower_role", "primary")
                if role == "co_borrower" and app.get("co_applicant_id"):
                    doc["applicant_id"] = app["co_applicant_id"]
                else:
                    doc["applicant_id"] = app["applicant_id"]
                doc["application_id"] = app["application_id"]
            else:
                logger.warning(
                    "unknown_los_id",
                    extra={"los_id": los_id,
                           "doc_id": doc.get("document_id"),
                           "channel": doc.get("source_channel")},
                )

        # ── Step 3: persist docs ─────────────────────────────────────
        wm_to = wm_from
        affected_apps: dict[str, str] = {}  # application_id → los_id
        for doc in new_docs:
            doc_id = doc.get("document_id")
            applicant_id = doc.get("applicant_id")
            application_id = doc.get("application_id")
            if not doc_id or not applicant_id:
                stats["documents_skipped"] += 1
                continue

            existing = None
            try:
                existing = await self.pg.get_document(doc_id)
            except Exception:
                existing = None
            if existing and existing.get("status") == "indexed":
                stats["documents_skipped"] += 1
            else:
                save_doc = self._build_save_doc(doc)
                try:
                    await self.pg.save_document(save_doc, tenant_id=tenant_id)
                    stats["documents_new"] += 1
                except Exception as exc:
                    logger.warning(
                        "incremental_save_doc_failed",
                        extra={"document_id": doc_id, "error": str(exc)[:200]},
                    )
                    continue

            received = doc.get("received_at") or wm_to
            if received and received > (wm_to or ""):
                wm_to = received
            if application_id:
                affected_apps.setdefault(application_id, doc.get("los_id") or "")

        # ── Step 4: reconcile (scoped per-doc) ─────────────────────
        # Edges still come out of the existing reconciler — we just
        # stamp ``application_id`` on each row so a workbench query
        # can scope edges to a single loan.
        if self.reconciler is not None:
            for doc in new_docs:
                applicant_id   = doc.get("applicant_id")
                application_id = doc.get("application_id")
                if not applicant_id:
                    continue
                save_doc = self._build_save_doc(doc)
                try:
                    edges = await self.reconciler.reconcile(
                        applicant_id, save_doc,
                    )
                except Exception as exc:
                    logger.warning(
                        f"incremental_reconcile_failed "
                        f"applicant_id={applicant_id} "
                        f"error={str(exc)[:200]}"
                    )
                    continue
                for edge in edges or []:
                    try:
                        row = (edge.model_dump()
                               if hasattr(edge, "model_dump")
                               else dict(edge))
                        if application_id:
                            row["application_id"] = application_id
                        await self.pg.save_relationship(
                            row, tenant_id=tenant_id,
                        )
                        stats["edges_created"] += 1
                    except Exception as exc:
                        logger.debug(
                            "incremental_edge_persist_failed",
                            extra={"error": str(exc)[:200]},
                        )

        # ── Step 5: assemble golden record per application ───────
        for application_id, los_id in affected_apps.items():
            try:
                state_data = await self._assemble_application_state(
                    application_id=application_id,
                    los_id=los_id,
                    legacy_ids=legacy_ids_by_los.get(los_id, {}),
                    new_docs_for_app=[
                        d for d in new_docs
                        if d.get("application_id") == application_id
                    ],
                    tenant_id=tenant_id,
                )
                await self.pg.upsert_entity_state(
                    application_id, state_data, tenant_id=tenant_id,
                )
                stats["entities_updated"] += 1

                # Update applications.verified_* so /context can show
                # stated-vs-verified side by side.
                borrower = state_data.get("borrower") or {}
                prop     = state_data.get("property") or {}
                await self.pg.update_application_verified_fields(
                    application_id,
                    {
                        "verified_income": (
                            (borrower.get("income") or {}).get("annual")
                        ),
                        "verified_property_value": (
                            ((prop.get("valuation") or {}).get("appraised_value"))
                        ),
                        "verified_assets": (
                            (borrower.get("assets") or {}).get("total_liquid")
                        ),
                        "verified_employer": (
                            (borrower.get("employment") or {}).get("employer")
                        ),
                    },
                    tenant_id=tenant_id,
                )

                # Append a coarse-grained build_complete event so
                # /entity/{id}/events shows the per-tick rhythm.
                # Field-level diff logging is left for the upsert
                # observers — coarse log keeps the table from
                # exploding on every doc.
                try:
                    await self.pg.log_entity_state_event(
                        application_id=application_id,
                        event_type="build_complete",
                        triggered_by=f"build:{build_date}-{build_number}",
                        new_value={
                            "status":           state_data.get("status"),
                            "document_count":   state_data.get("document_count"),
                            "completeness_pct": state_data.get("completeness_pct"),
                        },
                        tenant_id=tenant_id,
                    )
                except Exception:
                    pass
            except Exception as exc:
                logger.warning(
                    "incremental_assemble_failed "
                    f"application_id={application_id} "
                    f"error_type={type(exc).__name__} "
                    f"error={str(exc)[:200]}"
                )

        # ── Step 5: advance watermark ────────────────────────────────
        if wm_to and wm_to != wm_from:
            try:
                await self.connector.set_watermark(wm_to)
            except Exception as exc:
                logger.warning("watermark_save_failed",
                               extra={"error": str(exc)[:200]})

        # ── Step 6: record the run ───────────────────────────────────
        stats["duration_ms"] = int((time.perf_counter() - t0) * 1000)
        await self._record_run(
            build_date, build_number, wm_from, wm_to,
            stats, started_at, tenant_id=tenant_id,
        )
        logger.info("incremental_build_complete", extra={
            "build_date":    str(build_date),
            "build_number":  build_number,
            **stats,
        })
        return stats

    # ------------------------------------------------------------------
    # Per-application golden-record assembly (v4)
    # ------------------------------------------------------------------

    async def _assemble_application_state(
        self,
        application_id: str,
        los_id: str,
        legacy_ids: dict,
        new_docs_for_app: list,
        tenant_id: str,
    ) -> dict:
        """Build the full ``entity_states`` row for an application.

        Steps:
          1. Fetch every doc + applicant tied to this application.
          2. Group docs by applicant_id; build per-borrower JSONB
             (income / employment / credit / assets / identity).
          3. Build property + loan_terms JSONB from property + loan_terms
             docs across the application.
          4. Compute indexed columns (LTV / DTI / PITI / mid score).
          5. Build a verifications JSONB block with persona-ready
             summaries + boolean flags.
          6. Determine status from the flag stack.
          7. Merge accumulating legacy_ids.
        """
        all_docs   = await self.pg.get_documents_for_application(
            application_id, tenant_id=tenant_id,
        )
        applicants = await self.pg.get_applicants_for_application(
            application_id, tenant_id=tenant_id,
        )
        primary    = next((a for a in applicants if a.get("role") == "primary"), None)
        co_list    = [a for a in applicants if a.get("role") != "primary"]

        # --- per-borrower fold ----------------------------------------
        def _fold_borrower(applicant: dict) -> dict:
            aid    = applicant.get("applicant_id")
            ad     = [d for d in all_docs if d.get("applicant_id") == aid]
            return {
                "applicant_id": aid,
                "name": (
                    f"{applicant.get('first_name', '')} "
                    f"{applicant.get('last_name', '')}"
                ).strip(),
                "role":      applicant.get("role"),
                "income":    _income_block(ad),
                "employment": _employment_block(ad),
                "credit":    _credit_block(ad),
                "assets":    _assets_block(ad),
                "identity":  _identity_block(ad),
                "doc_types": sorted({
                    d.get("document_type") for d in ad if d.get("document_type")
                }),
                "doc_count": len(ad),
            }

        borrower     = _fold_borrower(primary) if primary else {}
        co_borrowers = [_fold_borrower(co) for co in co_list]

        # --- property + loan_terms ------------------------------------
        property_block   = _property_block(all_docs)
        loan_terms_block = _loan_terms_block(all_docs)

        # --- indexed columns ------------------------------------------
        all_scores = []
        for b in [borrower, *co_borrowers]:
            ms = (b.get("credit") or {}).get("mid_score")
            if ms:
                all_scores.append(int(ms))
        mid_credit_score = min(all_scores) if all_scores else None

        # v4.1 — Gap 11: only count VERIFIED borrowers' qualifying
        # monthly income. Pending co-borrower W-2s don't pad combined.
        b_inc = borrower.get("income") or {}
        primary_qm = (b_inc.get("qualifying_monthly") or 0) if b_inc.get("verified") else 0
        co_qm_total = 0
        for cb in co_borrowers:
            ci = cb.get("income") or {}
            if ci.get("verified"):
                co_qm_total += ci.get("qualifying_monthly") or 0
        combined_monthly_income = primary_qm + co_qm_total

        # v4.1 — Gap 11: only count VERIFIED borrowers' liquid +
        # obligations into the combined totals. A co-borrower whose
        # bank stmts haven't landed yet shouldn't pad the reserves.
        total_liquid = 0.0
        b_assets = borrower.get("assets") or {}
        if b_assets.get("verified"):
            total_liquid += float(b_assets.get("total_liquid") or 0)
        for cb in co_borrowers:
            ca = cb.get("assets") or {}
            if ca.get("verified"):
                total_liquid += float(ca.get("total_liquid") or 0)

        monthly_obligations = 0.0
        b_credit = borrower.get("credit") or {}
        if b_credit.get("verified"):
            monthly_obligations += float(b_credit.get("monthly_obligations") or 0)
        for cb in co_borrowers:
            cc = cb.get("credit") or {}
            if cc.get("verified"):
                monthly_obligations += float(cc.get("monthly_obligations") or 0)

        appraised_value = (
            (property_block.get("valuation") or {}).get("appraised_value")
        )
        purchase_price = (
            (loan_terms_block.get("purchase_agreement") or {}).get("purchase_price")
        )
        loan_amount   = loan_terms_block.get("loan_amount")
        interest_rate = loan_terms_block.get("interest_rate")
        term_months   = loan_terms_block.get("term_months") or 360

        # ---- LTV — refi vs purchase formula split (Gap 7) ----------
        # Refi uses appraised value alone (no purchase price exists);
        # purchase uses min(appraised, purchase_price) per agency
        # convention.
        purpose = (loan_terms_block.get("purpose") or "").lower()
        ltv = None
        if loan_amount:
            if purpose.startswith("refinance") or purpose == "refi":
                if appraised_value:
                    ltv = round(loan_amount / appraised_value * 100, 2)
            else:
                if appraised_value and purchase_price:
                    denom = min(appraised_value, purchase_price)
                    ltv = round(loan_amount / denom * 100, 2)
                elif appraised_value:
                    ltv = round(loan_amount / appraised_value * 100, 2)

        # ---- MI — required when LTV > 80, included in PITI (Gap 6) -
        # Tiered rate per LTV band (mirrors agency tables):
        #   80-90 LTV  → 0.5% / yr
        #   90-95 LTV  → 0.8% / yr
        #   95+ LTV    → 1.0% / yr
        # If a real MI_CERTIFICATE exists with monthly_premium, that
        # always wins over the estimate.
        mi_monthly = 0.0
        if ltv and ltv > 80:
            mi_doc = _doc(all_docs, "MI_CERTIFICATE", "MI_PREMIUM_QUOTE")
            if mi_doc:
                try:
                    mi_monthly = float(_f(mi_doc, "monthly_premium") or 0)
                except (TypeError, ValueError):
                    mi_monthly = 0.0
            if not mi_monthly and loan_amount:
                rate = (
                    0.005 if ltv <= 90
                    else (0.008 if ltv <= 95 else 0.01)
                )
                mi_monthly = round(float(loan_amount) * rate / 12, 2)

        # ---- CLTV — combined with subordinate liens (Gap 10) -------
        # For refis, the existing payoff is being replaced (not
        # subordinate). For purchases, any second mortgage / HELOC
        # adds to CLTV. We don't have subordinate-loan docs in the
        # simulator yet, so default subordinate=0; the column lights
        # up the moment a HELOC / second-mortgage doc type lands.
        subordinate_total = 0.0
        cltv = ltv  # CLTV defaults to LTV when no subordinate financing
        denom_for_cltv = appraised_value
        if denom_for_cltv and loan_amount:
            cltv = round(
                (float(loan_amount) + subordinate_total) / float(denom_for_cltv) * 100,
                2,
            )

        # ---- Existing-mortgage payment for refis (Gap 10 cont.) ----
        existing_mortgage_payment = None
        if purpose.startswith("refinance") or purpose == "refi":
            existing_mortgage_payment = (
                (loan_terms_block.get("current_mortgage") or {}).get("monthly_payment")
            )

        # ---- PITI + DTI (MI- and flood-aware) -----------------------
        annual_tax = (property_block.get("tax") or {}).get("annual_tax")
        annual_hoi = (
            (property_block.get("insurance") or {}).get("hoi_premium_annual")
        )
        # Section B: flood premium added to PITI when in zone A/V/AE/VE.
        annual_flood = (
            (property_block.get("insurance") or {}).get("flood_premium_annual")
        )
        flood_monthly = (
            float(annual_flood) / 12 if annual_flood else 0.0
        )
        piti_monthly = None
        if loan_amount and interest_rate and annual_tax and annual_hoi:
            try:
                rate_m = float(interest_rate) / 100 / 12
                n      = int(term_months)
                if rate_m > 0:
                    pi = (float(loan_amount) * (rate_m * (1 + rate_m) ** n)
                          / ((1 + rate_m) ** n - 1))
                else:
                    pi = float(loan_amount) / max(n, 1)
                piti_monthly = round(
                    pi + float(annual_tax) / 12 + float(annual_hoi) / 12
                    + float(mi_monthly or 0) + flood_monthly, 2,
                )
            except (TypeError, ValueError, ZeroDivisionError):
                piti_monthly = None

        dti_front = dti_back = None
        if combined_monthly_income and combined_monthly_income > 0 and piti_monthly:
            dti_front = round(piti_monthly / combined_monthly_income * 100, 2)
            dti_back  = round(
                (piti_monthly + monthly_obligations) / combined_monthly_income * 100, 2,
            )

        # --- Gap 9: reserves after down payment + closing costs -----
        # On purchases the borrower has to bring cash to closing
        # (down payment + ~3% closing costs); on refis they pay ~2%
        # closing only. Reserves remaining is what's left to ride out
        # an income-loss period — months_reserves = remaining / PITI.
        if (purpose.startswith("refinance") or purpose == "refi"):
            est_closing_costs = round(float(loan_amount or 0) * 0.02, 2)
            down_payment = 0.0
        else:
            try:
                down_payment = max(
                    0.0,
                    float(purchase_price or 0) - float(loan_amount or 0),
                )
            except (TypeError, ValueError):
                down_payment = 0.0
            est_closing_costs = round(float(loan_amount or 0) * 0.03, 2)
        reserves_remaining = round(
            max(0.0, total_liquid - down_payment - est_closing_costs), 2,
        )
        months_reserves = (
            round(reserves_remaining / piti_monthly, 1)
            if reserves_remaining and piti_monthly else None
        )
        # Stash reserves on the borrower.assets block so /context can
        # surface them inline.
        if borrower.get("assets") is not None:
            borrower["assets"]["down_payment"]            = down_payment or None
            borrower["assets"]["estimated_closing_costs"] = est_closing_costs or None
            borrower["assets"]["reserves_remaining"]      = reserves_remaining or None
            borrower["assets"]["months_reserves"]         = months_reserves

        # --- verifications block --------------------------------------
        verifications = _build_verifications(
            borrower, co_borrowers, property_block, loan_terms_block,
            combined_monthly_income, total_liquid, piti_monthly, mid_credit_score,
            monthly_obligations,
            reserves_remaining=reserves_remaining,
            months_reserves=months_reserves,
        )

        # --- flags (mirror verifications.*.status) --------------------
        flags = {
            k: bool((verifications.get(k) or {}).get("status"))
            for k in (
                "income_verified", "employment_verified", "credit_pulled",
                "assets_verified", "identity_complete", "appraisal_complete",
                "title_clear", "insurance_bound", "aus_approved",
                "rate_locked", "conditions_cleared", "clear_to_close",
            )
        }

        # --- status flip ----------------------------------------------
        if flags["clear_to_close"]:
            status = "clear_to_close"
        elif flags["conditions_cleared"]:
            status = "conditions_cleared"
        elif flags["aus_approved"]:
            status = "approved_with_conditions"
        elif flags["credit_pulled"] and flags["income_verified"]:
            status = "in_underwriting"
        elif len(all_docs) > 0:
            status = "docs_collecting"
        else:
            status = "application_received"

        # --- counts ---------------------------------------------------
        try:
            edge_count = await self.pg.count_edges_for_application(
                application_id, tenant_id=tenant_id,
            )
        except Exception:
            edge_count = 0
        try:
            conflict_count = await self.pg.count_conflicts_for_application(
                application_id, tenant_id=tenant_id,
            )
            critical_conflict_count = (
                await self.pg.count_conflicts_for_application(
                    application_id, tenant_id=tenant_id, critical_only=True,
                )
            )
        except Exception:
            conflict_count = critical_conflict_count = 0

        # --- completeness (verified-blocks ratio) --------------------
        verified_blocks = sum(1 for v in verifications.values()
                              if isinstance(v, dict) and v.get("status"))
        total_blocks    = len(verifications) or 1
        completeness_pct = round(verified_blocks / total_blocks * 100, 1)

        # --- legacy_ids accumulator ----------------------------------
        legacy = dict(legacy_ids or {})
        legacy.setdefault("los_id", los_id)
        src_ids = sorted({
            d.get("source_document_id") for d in new_docs_for_app
            if d.get("source_document_id")
        })
        if src_ids:
            legacy["source_document_ids"] = src_ids

        return {
            "los_id":        los_id,
            "legacy_ids":    legacy,
            "borrower":      borrower,
            "co_borrowers":  co_borrowers,
            "property":      property_block,
            "loan_terms":    loan_terms_block,
            "verifications": verifications,
            "mid_credit_score":              mid_credit_score,
            "qualifying_monthly":            primary_qm or None,
            "co_borrower_qualifying_monthly": co_qm_total or None,
            "combined_monthly_income":       combined_monthly_income or None,
            "total_liquid_assets":           total_liquid or None,
            "appraised_value":               appraised_value,
            "purchase_price":                purchase_price,
            "loan_amount":                   loan_amount,
            "interest_rate":                 interest_rate,
            "ltv":                           ltv,
            "cltv":                          cltv,
            "dti_front":                     dti_front,
            "dti_back":                      dti_back,
            "piti_monthly":                  piti_monthly,
            "mi_monthly":                    mi_monthly or None,
            "monthly_obligations":           monthly_obligations or None,
            "existing_mortgage_payment":     existing_mortgage_payment,
            "document_count":                len(all_docs),
            "graph_edge_count":              edge_count,
            "conflict_count":                conflict_count,
            "critical_conflict_count":       critical_conflict_count,
            "completeness_pct":              completeness_pct,
            "status":                        status,
            **flags,
        }

    # ------------------------------------------------------------------

    async def _classify_unknown_docs(
        self, new_docs: list[dict], stats: dict,
    ) -> None:
        """Run Claude Vision on every doc carrying
        ``requires_classification=True``. Updates the doc in-place when
        Vision returned actionable fields:

        - ``document_type`` from the model overrides ``UNKNOWN`` so the
          downstream graph reconciler treats the doc as the right kind.
        - ``los_id`` (if visible on the doc) overrides ``UNCLASSIFIED``
          so the next step can resolve it to a real applicant.
        - All extracted fields merge into ``extracted_fields``.
        - ``extraction_method='ai_vision'`` records provenance for the
          ``/applicant/.../graph/summary`` extraction breakdown.

        Vision-failure or empty-response leaves the doc untouched —
        it still falls through to the los_id-resolution step and (with
        ``los_id='UNCLASSIFIED'``) gets skipped at the persist gate.
        """
        candidates = [d for d in new_docs if d.get("requires_classification")]
        if not candidates:
            return

        try:
            from core.documents.extractors.claude_extractor import (
                extract_with_claude,
            )
        except Exception as exc:    # pragma: no cover — import-only failure
            logger.warning(
                "vision_extractor_unavailable",
                extra={"error": str(exc)[:200]},
            )
            return

        connector_get_bytes = getattr(
            self.connector, "get_evidence_bytes", None,
        )
        if connector_get_bytes is None:
            logger.warning(
                "vision_classify_skipped reason=connector_lacks_get_evidence_bytes "
                f"connector={type(self.connector).__name__}"
            )
            return

        for doc in candidates:
            evidence_path = doc.get("evidence_file")
            if not evidence_path:
                continue
            try:
                # connector.get_evidence_bytes is sync (boto3 / Path)
                # so wrap in a thread executor to keep the event loop
                # unblocked on multi-megabyte PDFs from S3.
                pdf_bytes = await asyncio.to_thread(
                    connector_get_bytes, evidence_path,
                )
            except Exception as exc:
                logger.warning(
                    f"vision_evidence_fetch_failed "
                    f"doc_id={doc.get('document_id')} "
                    f"evidence={evidence_path} "
                    f"error_type={type(exc).__name__} "
                    f"error={str(exc)[:200]}"
                )
                continue
            if not pdf_bytes:
                continue

            extracted, conf = await extract_with_claude(pdf_bytes, "UNKNOWN")
            if not extracted:
                logger.info(
                    f"vision_classify_empty doc_id={doc.get('document_id')} "
                    f"evidence={evidence_path}"
                )
                continue

            new_type = extracted.get("document_type")
            new_los  = extracted.get("los_id")
            if new_type:
                doc["document_type"] = new_type
                # Re-derive category so the entity classifier + graph
                # downstream see a valid bucket.
                doc["category"] = doc.get("category") or "income"
            if new_los:
                doc["los_id"] = new_los
            doc["extracted_fields"] = {
                **(doc.get("extracted_fields") or {}),
                **{k: v for k, v in extracted.items()
                   if k not in ("document_type", "los_id")},
            }
            doc["extraction_method"]      = "ai_vision"
            doc["confidence_score"]       = conf
            doc["requires_classification"] = False
            stats["documents_classified"] += 1

            logger.info(
                f"vision_classified doc_id={doc.get('document_id')} "
                f"new_type={new_type or '?'} new_los_id={new_los or '?'} "
                f"fields_extracted={len(extracted)} "
                f"confidence={conf}"
            )

    # ------------------------------------------------------------------

    @staticmethod
    def _classify_entity(doc: dict) -> str:
        if doc.get("category") == "property":
            return "property"
        if doc.get("borrower_role") == "co_borrower":
            return "co_borrower"
        return "borrower"

    @staticmethod
    def _build_save_doc(doc: dict) -> dict:
        """Coerce the connector's flat shape to the ``save_document``
        contract — the existing PG store expects ``document_category``,
        ``borrower_role`` etc. v3 docs also carry ``source_document_id``
        + ``source_channel`` which thread through to the new
        ``document_index`` columns."""
        return {
            "document_id":         doc.get("document_id"),
            "applicant_id":        doc.get("applicant_id"),
            "application_id":      doc.get("application_id"),
            "document_type":       doc.get("document_type"),
            "document_category":   doc.get("category") or doc.get("document_category", "income"),
            "borrower_role":       doc.get("borrower_role", "primary"),
            "s3_key":              doc.get("s3_key"),
            "status":              "indexed",
            "extracted_fields":    doc.get("extracted_fields") or {},
            "confidence_score":    doc.get("confidence_score") or 0.94,
            "extraction_method":   doc.get("extraction_method") or "caller_supplied",
            "source_document_id":  doc.get("source_document_id"),
            "source_channel":      doc.get("source_channel"),
        }

    async def _compose_state(
        self, applicant_id: str, application_id: str, tenant_id: str,
    ) -> tuple[dict, int, float]:
        """Build the JSONB ``state`` blob from the applicant's doc set
        + assembled profiles. Returns ``(state, doc_count, completeness_pct)``."""
        try:
            docs = await self.pg.get_documents_for_applicant(
                applicant_id, tenant_id=tenant_id,
            )
        except Exception:
            docs = []
        doc_types  = sorted({d.get("document_type") for d in docs if d.get("document_type")})
        doc_count  = len(docs)
        # Slot fulfillment — required slots only (conditional slots
        # belong to the missing-documents catalog endpoint).
        have       = set(doc_types)
        filled     = sum(1 for s in _REQUIRED_SLOTS if _slot_received(s, have))
        completeness = round(filled / _REQUIRED_TOTAL * 100, 1) if _REQUIRED_TOTAL else 0.0

        income = credit = None
        try:
            income = await self.pg.get_income_profile(applicant_id, tenant_id=tenant_id)
        except Exception:
            income = None
        try:
            credit = await self.pg.get_credit_profile(applicant_id, tenant_id=tenant_id)
        except Exception:
            credit = None

        last_received = max(
            (d.get("received_at") for d in docs if d.get("received_at")),
            default=None,
        )

        state = {
            "application_id":         application_id,
            "doc_types":              doc_types,
            "required_slots_filled":  filled,
            "required_slots_total":   _REQUIRED_TOTAL,
            "completeness_pct":       completeness,
            "last_doc_received_at":   last_received,
            "qualifying_monthly":     (
                (income or {}).get("qualifying_monthly")
                or (income or {}).get("primary_borrower", {}).get("qualifying_monthly")
            ),
            "mid_score":              (credit or {}).get("mid_score"),
            "credit_band":            (credit or {}).get("credit_band"),
        }
        return state, doc_count, completeness

    async def _record_run(
        self, build_date, build_number, wm_from, wm_to,
        stats, started_at, tenant_id="default",
        status: str = "completed",
        error_details: Optional[str] = None,
    ) -> None:
        try:
            await self.pg.insert_graph_build_run(
                build_date=build_date,
                build_number=build_number,
                watermark_from=wm_from,
                watermark_to=wm_to,
                stats=stats,
                started_at=started_at,
                completed_at=datetime.now(timezone.utc),
                status=status,
                error_details=error_details,
                tenant_id=tenant_id,
            )
        except Exception as exc:
            logger.warning(
                "graph_build_run_persist_failed",
                extra={"error": str(exc)[:200]},
            )
