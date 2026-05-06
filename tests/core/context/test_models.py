"""Sanity checks for the Phase C pydantic models."""
from core.context.models import (
    ApplicationContext,
    BorrowerSnapshot,
    PropertySnapshot,
    ReadinessFlags,
)


def _primary():
    return BorrowerSnapshot(
        applicant_id="APL-1",
        full_name="Maya Patel",
        role="primary",
        qualifying_monthly=8000,
        income_confidence=0.95,
        income_verified=True,
        mid_score=720,
        credit_band="prime",
        monthly_obligations=400,
    )


def test_context_minimal_construction():
    ctx = ApplicationContext(
        application_id="APP-1",
        los_id="LOS-1",
        primary=_primary(),
        combined_qualifying_monthly=8000,
        qualifying_score_used=720,
        total_monthly_obligations=400,
        readiness=ReadinessFlags(),
    )
    assert ctx.primary.applicant_id == "APL-1"
    assert ctx.co_borrower is None
    assert ctx.property is None
    assert ctx.front_end_dti is None
    assert ctx.ltv is None


def test_property_snapshot_defaults():
    snap = PropertySnapshot(
        property_id="PROP-1",
        address="123 Main, SF, CA",
        property_type="single_family",
    )
    assert snap.flood_insurance_required is False
    assert snap.hoa_monthly == 0
    assert snap.piti_total is None


def test_readiness_flags_defaults():
    flags = ReadinessFlags()
    assert flags.aus_ready is False
    assert flags.missing_items == []
