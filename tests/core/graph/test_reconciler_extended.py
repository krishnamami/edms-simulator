"""Extended reconciler coverage — wage/IRS tight thresholds, property
value-gap, YTD annualisation, currency normalisation, COMPARISON_MAP
coverage."""
import pytest

from core.graph.models import RelationshipType
from core.graph.reconciler import (
    COMPARISON_MAP,
    DocumentReconciler,
    FIELD_CONFLICT_THRESHOLDS,
)


def _doc(doc_id, doc_type, applicant_id="APL-00001-P", **fields):
    return {
        "document_id":      doc_id,
        "applicant_id":     applicant_id,
        "document_type":    doc_type,
        "extracted_fields": fields,
    }


# ── Wage cross-checks ───────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_w2_vs_irs_confirms_exact_match(postgres_store):
    w2  = _doc("D-W2", "W2_CURRENT", box1_wages=92400)
    irs = _doc("D-IRS", "IRS_TRANSCRIPT", wages_tips_compensation=92400)
    await postgres_store.save_document(w2)
    await postgres_store.save_document(irs)

    rels = await DocumentReconciler(postgres_store).reconcile("APL-00001-P", w2)
    wage = next(r for r in rels if "box1_wages" in (r.field_name or ""))
    assert wage.relationship_type == RelationshipType.CONFIRMS
    assert wage.delta_pct == pytest.approx(0.0, abs=0.1)


@pytest.mark.asyncio
async def test_w2_vs_irs_contradicts_15pct_delta(postgres_store):
    """W2 92400 vs IRS 78000 → 15.6% delta. With the tight 5% override
    in FIELD_CONFLICT_THRESHOLDS, this must contradict (not corroborate)."""
    w2  = _doc("D-W2", "W2_CURRENT", box1_wages=92400)
    irs = _doc("D-IRS", "IRS_TRANSCRIPT", wages_tips_compensation=78000)
    await postgres_store.save_document(w2)
    await postgres_store.save_document(irs)

    rels = await DocumentReconciler(postgres_store).reconcile("APL-00001-P", w2)
    wage = next(r for r in rels if "box1_wages" in (r.field_name or ""))
    assert wage.relationship_type == RelationshipType.CONTRADICTS
    assert wage.delta_pct > 5


# ── Pay stub annualisation ──────────────────────────────────────────────

@pytest.mark.asyncio
async def test_w2_vs_paystub_uses_annualised_ytd(postgres_store):
    """Pay stub ships ytd_gross + period_end. The reconciler annualises
    on the fly so we can compare against W2 box1."""
    w2 = _doc("D-W2", "W2_CURRENT", box1_wages=92400)
    paystub = _doc(
        "D-PS", "PAYSTUB_CURRENT",
        ytd_gross=30800, pay_period_end="2026-04-30",
    )
    await postgres_store.save_document(w2)
    await postgres_store.save_document(paystub)

    rels = await DocumentReconciler(postgres_store).reconcile("APL-00001-P", w2)
    wages = [
        r for r in rels
        if "box1_wages" in (r.field_name or "")
        and "annualized_ytd" in (r.field_name or "")
    ]
    assert wages, "expected a wages↔annualized_ytd relationship"
    # 30800 / (120/365) ≈ 93,683 → ~1.4% off W2 92400
    assert wages[0].relationship_type == RelationshipType.CONFIRMS


def test_annualize_ytd_april():
    """Day-of-year on Apr 30 ≈ 120, fraction ≈ 0.329, annualised ≈ 93,623."""
    val = DocumentReconciler._annualize_ytd(30800, "2026-04-30")
    assert val is not None
    assert 92000 < val < 95000


def test_annualize_ytd_no_date_assumes_third():
    """Without a period-end, default to 3× (≈4 months in)."""
    val = DocumentReconciler._annualize_ytd(30000, None)
    assert val == pytest.approx(90000, abs=1)


# ── Currency normalisation ──────────────────────────────────────────────

def test_normalise_value_currency_string():
    assert DocumentReconciler._normalise_value("$92,400.00") == 92400.0


def test_normalise_value_with_commas():
    assert DocumentReconciler._normalise_value("92,400") == 92400.0


def test_normalise_value_passthrough_numeric():
    assert DocumentReconciler._normalise_value(92400) == 92400.0


def test_normalise_value_range_returns_midpoint():
    assert DocumentReconciler._normalise_value("90000-100000") == 95000.0


def test_normalise_value_returns_none_for_garbage():
    assert DocumentReconciler._normalise_value("not a number") is None
    assert DocumentReconciler._normalise_value(None) is None
    assert DocumentReconciler._normalise_value(True) is None  # bool != number


# ── Property value-gap detection ────────────────────────────────────────

@pytest.mark.asyncio
async def test_appraisal_vs_purchase_confirms_when_close(postgres_store):
    appraisal = _doc("D-APP", "APPRAISAL_URAR", appraised_value=485000)
    purchase  = _doc("D-PA",  "PURCHASE_AGREEMENT", purchase_price=480000)
    await postgres_store.save_document(appraisal)
    await postgres_store.save_document(purchase)

    rels = await DocumentReconciler(postgres_store).reconcile(
        "APL-00001-P", appraisal,
    )
    val = [
        r for r in rels
        if "appraised_value" in (r.field_name or "")
        and "purchase_price" in (r.field_name or "")
    ]
    assert val
    assert val[0].relationship_type == RelationshipType.CONFIRMS


@pytest.mark.asyncio
async def test_appraisal_vs_purchase_contradicts_value_gap(postgres_store):
    """Appraisal $450k vs purchase $485k → 7.2% delta > 5% tight threshold
    for this pair → CONTRADICTS (the loan would be over-market)."""
    appraisal = _doc("D-APP", "APPRAISAL_URAR", appraised_value=450000)
    purchase  = _doc("D-PA",  "PURCHASE_AGREEMENT", purchase_price=485000)
    await postgres_store.save_document(appraisal)
    await postgres_store.save_document(purchase)

    rels = await DocumentReconciler(postgres_store).reconcile(
        "APL-00001-P", appraisal,
    )
    val = next(
        r for r in rels
        if "appraised_value" in (r.field_name or "")
        and "purchase_price" in (r.field_name or "")
    )
    assert val.relationship_type == RelationshipType.CONTRADICTS


# ── Tax-figure tight threshold ──────────────────────────────────────────

@pytest.mark.asyncio
async def test_irs_vs_tax_return_tight_agi_threshold(postgres_store):
    """IRS agi 95000 vs tax return agi 97000 → 2.06% delta. Default
    NUMERIC_CONFLICT_THRESHOLD (10%) would corroborate, but the override
    for (IRS_TRANSCRIPT, TAX_RETURN_1040_CURRENT, agi) is 2% → contradicts."""
    irs = _doc("D-IRS", "IRS_TRANSCRIPT", agi=95000)
    ret = _doc(
        "D-RET", "TAX_RETURN_1040_CURRENT",
        agi=97000,
    )
    await postgres_store.save_document(irs)
    await postgres_store.save_document(ret)

    rels = await DocumentReconciler(postgres_store).reconcile(
        "APL-00001-P", irs,
    )
    agi = next(r for r in rels if "agi" in (r.field_name or ""))
    assert agi.relationship_type == RelationshipType.CONTRADICTS


# ── COMPARISON_MAP coverage ─────────────────────────────────────────────

def test_comparison_map_pair_count():
    """Build target: 25+ document pairs covering every entity layer."""
    assert len(COMPARISON_MAP) >= 25


def test_comparison_map_only_same_type_can_be_empty():
    """Every non-same-type entry must have at least one comparable field
    tuple — empty lists are reserved for explicit (X, X) skip pairs."""
    for (a, b), pairs in COMPARISON_MAP.items():
        if a == b:
            continue
        assert pairs, f"COMPARISON_MAP[({a}, {b})] is empty"


def test_field_thresholds_are_floats_in_range():
    for key, threshold in FIELD_CONFLICT_THRESHOLDS.items():
        assert 0.0 < threshold < 1.0, f"{key} threshold out of range"
