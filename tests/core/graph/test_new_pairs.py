"""Tests for the Tier-2 COMPARISON_MAP additions.

Covers the cross-doc pairs for IRS↔W2 (5% threshold), URLA↔W2 stated-vs-
documented income with the ``monthly_income_stated_annual`` logical
field, AVM↔appraisal (15% threshold), purchase↔appraisal (5%), gift↔
bank deposit corroboration, plus a size assertion that the registry
keeps growing as new doc types arrive.
"""
import pytest

from core.graph.reconciler import (
    COMPARISON_MAP, DocumentReconciler,
)
from core.graph.models import RelationshipType


def _doc(doc_id, doc_type, applicant_id="APL-00001-P", **fields):
    return {
        "document_id":      doc_id,
        "applicant_id":     applicant_id,
        "document_type":    doc_type,
        "extracted_fields": fields,
    }


# ---------------------------------------------------------------------------
# IRS ↔ W2 — wages_salaries field tuple under the 5% threshold
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_irs_vs_w2_confirms_within_5pct(postgres_store):
    """IRS=125000, W2=124000 → ~0.8% delta → confirms."""
    irs = _doc("D-IRS", "IRS_TRANSCRIPT", wages_salaries=125000, tax_year=2024)
    w2  = _doc("D-W2",  "W2_CURRENT",     box1_wages=124000, tax_year=2024)
    await postgres_store.save_document(irs)
    await postgres_store.save_document(w2)

    rels = await DocumentReconciler(postgres_store).reconcile("APL-00001-P", irs)
    wage_rels = [
        r for r in rels
        if r.field_name and "wages_salaries" in r.field_name
        and "box1_wages" in r.field_name
    ]
    assert wage_rels, (
        f"expected a wages_salaries↔box1_wages relationship, got "
        f"{[(r.field_name, r.relationship_type) for r in rels]}"
    )
    assert wage_rels[0].relationship_type == RelationshipType.CONFIRMS


@pytest.mark.asyncio
async def test_irs_vs_w2_contradicts_outside_5pct(postgres_store):
    """IRS=125000, W2=100000 → 20% delta → contradicts (>5% threshold)."""
    irs = _doc("D-IRS", "IRS_TRANSCRIPT", wages_salaries=125000, tax_year=2024)
    w2  = _doc("D-W2",  "W2_CURRENT",     box1_wages=100000, tax_year=2024)
    await postgres_store.save_document(irs)
    await postgres_store.save_document(w2)

    rels = await DocumentReconciler(postgres_store).reconcile("APL-00001-P", irs)
    wage_rels = [
        r for r in rels
        if r.field_name and "wages_salaries" in r.field_name
        and "box1_wages" in r.field_name
    ]
    assert wage_rels
    assert wage_rels[0].relationship_type == RelationshipType.CONTRADICTS
    assert wage_rels[0].delta_pct >= 5.0


# ---------------------------------------------------------------------------
# URLA ↔ W2 — stated-vs-documented with ``monthly_income_stated_annual``
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_urla_vs_w2_confirms_when_stated_close(postgres_store):
    """URLA stated=$10000/mo (annualised $120k) vs W2 box1=$118k →
    ~1.7% delta → confirms (under the 10% threshold)."""
    urla = _doc("D-URLA", "URLA_1003",   monthly_income_stated=10000)
    w2   = _doc("D-W2",   "W2_CURRENT",  box1_wages=118000, tax_year=2024)
    await postgres_store.save_document(urla)
    await postgres_store.save_document(w2)

    rels = await DocumentReconciler(postgres_store).reconcile("APL-00001-P", urla)
    stated_rels = [
        r for r in rels
        if r.field_name and "monthly_income_stated_annual" in r.field_name
    ]
    assert stated_rels, (
        f"expected stated-vs-documented edge, got "
        f"{[(r.field_name, r.relationship_type) for r in rels]}"
    )
    assert stated_rels[0].relationship_type == RelationshipType.CONFIRMS


@pytest.mark.asyncio
async def test_urla_vs_w2_contradicts_stated_inflation(postgres_store):
    """Classic stated-income fraud: URLA=$20000/mo (annualised $240k) vs
    W2 box1=$125k → 48% delta → contradicts."""
    urla = _doc("D-URLA", "URLA_1003",   monthly_income_stated=20000)
    w2   = _doc("D-W2",   "W2_CURRENT",  box1_wages=125000, tax_year=2024)
    await postgres_store.save_document(urla)
    await postgres_store.save_document(w2)

    rels = await DocumentReconciler(postgres_store).reconcile("APL-00001-P", urla)
    stated_rels = [
        r for r in rels
        if r.field_name and "monthly_income_stated_annual" in r.field_name
    ]
    assert stated_rels
    assert stated_rels[0].relationship_type == RelationshipType.CONTRADICTS
    assert stated_rels[0].delta_pct >= 10.0


# ---------------------------------------------------------------------------
# AVM ↔ Appraisal — 15% threshold
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_avm_vs_appraisal_confirms_within_15pct(postgres_store):
    """AVM=455000 vs appraisal=460000 → ~1.1% delta → confirms."""
    appraisal = _doc(
        "D-APR", "APPRAISAL_URAR", appraised_value=460000,
    )
    avm = _doc("D-AVM", "AVM_REPORT", avm_value=455000)
    await postgres_store.save_document(appraisal)
    await postgres_store.save_document(avm)

    rels = await DocumentReconciler(postgres_store).reconcile("APL-00001-P", appraisal)
    valuation_rels = [
        r for r in rels
        if r.field_name and "avm_value" in r.field_name
    ]
    assert valuation_rels, (
        f"expected appraised_value↔avm_value edge, got "
        f"{[(r.field_name, r.relationship_type) for r in rels]}"
    )
    assert valuation_rels[0].relationship_type == RelationshipType.CONFIRMS


@pytest.mark.asyncio
async def test_avm_vs_appraisal_contradicts_outside_15pct(postgres_store):
    """AVM=380000 vs appraisal=460000 → ~17% delta → contradicts (>15%)."""
    appraisal = _doc(
        "D-APR", "APPRAISAL_URAR", appraised_value=460000,
    )
    avm = _doc("D-AVM", "AVM_REPORT", avm_value=380000)
    await postgres_store.save_document(appraisal)
    await postgres_store.save_document(avm)

    rels = await DocumentReconciler(postgres_store).reconcile("APL-00001-P", appraisal)
    valuation_rels = [
        r for r in rels
        if r.field_name and "avm_value" in r.field_name
    ]
    assert valuation_rels
    assert valuation_rels[0].relationship_type == RelationshipType.CONTRADICTS
    assert valuation_rels[0].delta_pct > 15.0


# ---------------------------------------------------------------------------
# Purchase agreement ↔ Appraisal
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_purchase_vs_appraisal_confirms(postgres_store):
    """Purchase=450000 vs appraisal=460000 → ~2.2% delta → confirms
    (5% threshold)."""
    purchase  = _doc("D-PUR", "PURCHASE_AGREEMENT", purchase_price=450000)
    appraisal = _doc("D-APR", "APPRAISAL_URAR",     appraised_value=460000)
    await postgres_store.save_document(purchase)
    await postgres_store.save_document(appraisal)

    rels = await DocumentReconciler(postgres_store).reconcile("APL-00001-P", appraisal)
    price_rels = [
        r for r in rels
        if r.field_name
        and "appraised_value" in r.field_name
        and "purchase_price" in r.field_name
    ]
    assert price_rels, (
        f"expected appraised_value↔purchase_price edge, got "
        f"{[(r.field_name, r.relationship_type) for r in rels]}"
    )
    assert price_rels[0].relationship_type == RelationshipType.CONFIRMS


# ---------------------------------------------------------------------------
# Gift letter ↔ Bank statement deposit corroboration
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_gift_vs_bank_corroborates_when_visible(postgres_store):
    """Gift=$20k visible as a $20k closing-balance bump on the bank
    statement → corroborates / confirms via the new
    ``gift_amount`` ↔ ``ending_balance`` field tuple. ``ending_balance``
    will rarely be exactly the gift amount, so we only assert that the
    edge fires (any non-CONTRADICTS relationship type)."""
    gift = _doc("D-GIFT", "GIFT_LETTER",      gift_amount=20000)
    bank = _doc("D-BANK", "BANK_STATEMENT_M1", ending_balance=20000)
    await postgres_store.save_document(gift)
    await postgres_store.save_document(bank)

    rels = await DocumentReconciler(postgres_store).reconcile("APL-00001-P", gift)
    gift_rels = [
        r for r in rels
        if r.field_name
        and "gift_amount" in r.field_name
        and "ending_balance" in r.field_name
    ]
    assert gift_rels, (
        f"expected gift_amount↔ending_balance edge, got "
        f"{[(r.field_name, r.relationship_type) for r in rels]}"
    )
    # When the deposit lines up exactly (synthetic test scenario) the
    # edge is CONFIRMS; in real loans it's usually CORROBORATES (small
    # delta from intervening transactions). Anything but CONTRADICTS
    # is acceptable.
    assert gift_rels[0].relationship_type != RelationshipType.CONTRADICTS


# ---------------------------------------------------------------------------
# Registry growth — locks in the Tier-2 expansion
# ---------------------------------------------------------------------------

def test_comparison_map_has_at_least_43_pairs():
    """COMPARISON_MAP started with 25 pairs at commit a6370f4. Tier-2
    adds 18 more pairs (some are extensions of existing entries with new
    field tuples, but several are entirely new keys). Lock the lower
    bound so the count never silently regresses."""
    assert len(COMPARISON_MAP) >= 43, (
        f"COMPARISON_MAP has {len(COMPARISON_MAP)} pairs — Tier-2 "
        "expansion expected at least 43"
    )
