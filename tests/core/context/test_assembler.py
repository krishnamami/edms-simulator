"""ContextAssembler integration tests.

Uses the FakePostgresStore + a fresh fakeredis-backed RedisStore to drive
the assembler end-to-end without touching real infrastructure.
"""
import pytest

from core.context.assembler import ContextAssembler


def _income_profile(
    *,
    applicant_id="APL-PRI",
    application_id="APP-1",
    primary_qualifying=7700,
    primary_confidence=0.95,
    co_qualifying=None,
    co_confidence=0.92,
    requires_review=False,
):
    primary = {
        "borrower_id": applicant_id,
        "role": "primary",
        "qualifying_monthly": primary_qualifying,
        "overall_confidence": primary_confidence,
        "sources": [{"source_type": "W2_SALARIED",
                     "qualifying_monthly": primary_qualifying}],
    }
    co = None
    if co_qualifying is not None:
        co = {
            "borrower_id": "APL-CO",
            "role": "co_borrower",
            "qualifying_monthly": co_qualifying,
            "overall_confidence": co_confidence,
            "sources": [{"source_type": "W2_SALARIED",
                         "qualifying_monthly": co_qualifying}],
        }
    combined = primary_qualifying + (co_qualifying or 0)
    return {
        "applicant_id": applicant_id,
        "application_id": application_id,
        "assembled_at": "2026-05-06T00:00:00",
        "primary_borrower": primary,
        "co_borrower": co,
        "combined_qualifying_monthly": combined,
        "qualifying_score_used": 720,
        "monthly_debt_obligations": [],
        "total_monthly_obligations": 0.0,
        "dti_inputs_ready": True,
        "requires_human_review": requires_review,
        "lineage_hash": "abc",
    }


def _credit_profile(applicant_id, *, mid=720, band="prime",
                    obligations=400, derogatory=0):
    return {
        "applicant_id": applicant_id,
        "mid_score": mid,
        "credit_band": band,
        "total_monthly_obligations": obligations,
        "derogatory_marks": derogatory,
        "monthly_obligations": [
            {"type": "auto", "monthly_payment": obligations,
             "creditor": "Example"}
        ] if obligations else [],
    }


def _property_profile(
    *,
    property_id="PROP-1",
    application_id="APP-1",
    appraised_value=400_000,
    annual_taxes=6_000,
    hoi_monthly=150,
    flood_zone="X",
    condition="C3",
    piti_total=2_500.0,
):
    piti_components = {
        "principal_interest": piti_total - 500 - 150,
        "taxes_monthly": 500,
        "insurance_monthly": 150,
        "hoa_monthly": 0,
        "flood_monthly": 0,
        "total_piti": piti_total,
    } if piti_total else None
    return {
        "property_id": property_id,
        "application_id": application_id,
        "appraised_value": appraised_value,
        "appraisal_confidence": 0.97,
        "annual_taxes": annual_taxes,
        "monthly_taxes": annual_taxes / 12 if annual_taxes else None,
        "hoi_monthly": hoi_monthly,
        "flood_zone": flood_zone,
        "flood_insurance_required": flood_zone not in ("X", "B", "C"),
        "hoa_monthly": 0,
        "condition_rating": condition,
        "piti_components": piti_components,
        "lineage_hash": "abc",
        "assembled_at": "2026-05-06T00:00:00",
    }


async def _seed_application(
    pg,
    *,
    application_id="APP-1",
    applicant_id="APL-PRI",
    co_applicant_id=None,
    los_id="LOS-1",
    loan_amount=None,
    property_id=None,
):
    await pg.save_golden_record({
        "applicant_id": applicant_id,
        "full_name": "Maya Patel",
        "first_name": "Maya",
        "last_name": "Patel",
        "dob": "1990-01-01",
        "ssn_hash": "h-pri",
        "ssn_last4": "1234",
        "status": "active",
        "identity_xrefs": [],
        "application_ids": [application_id],
    })
    if co_applicant_id:
        await pg.save_golden_record({
            "applicant_id": co_applicant_id,
            "full_name": "James Okafor",
            "first_name": "James",
            "last_name": "Okafor",
            "dob": "1988-04-12",
            "ssn_hash": "h-co",
            "ssn_last4": "5678",
            "status": "active",
            "identity_xrefs": [],
            "application_ids": [application_id],
        })
    await pg.save_application({
        "application_id": application_id,
        "applicant_id": applicant_id,
        "co_applicant_id": co_applicant_id,
        "los_id": los_id,
        "status": "active",
    })
    if loan_amount is not None:
        await pg.update_application_loan_data(
            application_id, {"loan_amount": loan_amount}
        )
    if property_id is not None:
        await pg.update_application_property(application_id, property_id)


@pytest.mark.asyncio
async def test_assemble_primary_only(postgres_store, redis_store):
    await _seed_application(postgres_store)
    await postgres_store.save_income_profile(_income_profile())
    await postgres_store.save_credit_profile(_credit_profile("APL-PRI"))

    ctx = await ContextAssembler(postgres_store, redis_store).assemble("APP-1")

    assert ctx.co_borrower is None
    assert ctx.property is None
    assert ctx.front_end_dti is None
    assert ctx.readiness.appraisal_complete is False
    assert "appraisal" in ctx.readiness.missing_items


@pytest.mark.asyncio
async def test_assemble_with_co_borrower(postgres_store, redis_store):
    await _seed_application(
        postgres_store, co_applicant_id="APL-CO"
    )
    await postgres_store.save_income_profile(_income_profile(
        primary_qualifying=7700, co_qualifying=4600,
    ))
    await postgres_store.save_credit_profile(_credit_profile("APL-PRI", mid=720))
    await postgres_store.save_credit_profile(_credit_profile("APL-CO", mid=680))

    ctx = await ContextAssembler(postgres_store, redis_store).assemble("APP-1")

    assert ctx.co_borrower is not None
    assert ctx.combined_qualifying_monthly == 12300
    assert ctx.qualifying_score_used == 680  # min of 720, 680


@pytest.mark.asyncio
async def test_dti_calculated_when_piti_available(
    postgres_store, redis_store
):
    await postgres_store.save_property({
        "property_id":   "PROP-1",
        "application_id": "APP-1",
        "address_line1":  "123 Main St",
        "city":           "SF",
        "state":          "CA",
        "zip_code":       "94105",
        "property_type":  "single_family",
        "units":          1,
    })
    await _seed_application(
        postgres_store, loan_amount=320_000, property_id="PROP-1"
    )
    await postgres_store.save_income_profile(_income_profile(
        primary_qualifying=10_000, primary_confidence=0.95,
    ))
    await postgres_store.save_credit_profile(_credit_profile(
        "APL-PRI", obligations=500
    ))
    await postgres_store.save_property_profile(_property_profile(
        piti_total=2500
    ))

    ctx = await ContextAssembler(postgres_store, redis_store).assemble("APP-1")

    assert ctx.front_end_dti == 25.0
    # back-end DTI = (2500 + 500) / 10000 * 100 = 30.0
    assert ctx.back_end_dti == 30.0


@pytest.mark.asyncio
async def test_ltv_calculated_when_appraisal_available(
    postgres_store, redis_store
):
    await postgres_store.save_property({
        "property_id":   "PROP-1",
        "application_id": "APP-1",
        "address_line1":  "123 Main St",
        "city":           "SF",
        "state":          "CA",
        "zip_code":       "94105",
        "property_type":  "single_family",
        "units":          1,
    })
    await _seed_application(
        postgres_store, loan_amount=320_000, property_id="PROP-1"
    )
    await postgres_store.save_income_profile(_income_profile())
    await postgres_store.save_credit_profile(_credit_profile("APL-PRI"))
    await postgres_store.save_property_profile(_property_profile(
        appraised_value=400_000
    ))

    ctx = await ContextAssembler(postgres_store, redis_store).assemble("APP-1")
    assert ctx.ltv == 80.0
    assert ctx.property.ltv == 80.0


@pytest.mark.asyncio
async def test_requires_review_when_conflicts(
    postgres_store, redis_store
):
    await _seed_application(postgres_store)
    await postgres_store.save_income_profile(_income_profile())
    await postgres_store.save_credit_profile(_credit_profile("APL-PRI"))
    # Synthesize a graph conflict via the relationship store hook.
    postgres_store.relationships.append({
        "applicant_id": "APL-PRI",
        "relationship_type": "contradicts",
    })

    ctx = await ContextAssembler(postgres_store, redis_store).assemble("APP-1")
    assert ctx.graph_summary.get("conflict_count", 0) > 0
    assert ctx.requires_review is True


@pytest.mark.asyncio
async def test_readiness_aus_ready_all_green(
    postgres_store, redis_store
):
    await postgres_store.save_property({
        "property_id":   "PROP-1",
        "application_id": "APP-1",
        "address_line1":  "123 Main St",
        "city":           "SF",
        "state":          "CA",
        "zip_code":       "94105",
        "property_type":  "single_family",
        "units":          1,
    })
    await _seed_application(
        postgres_store, loan_amount=320_000, property_id="PROP-1"
    )
    await postgres_store.save_income_profile(_income_profile(
        primary_qualifying=10_000, primary_confidence=0.95,
    ))
    await postgres_store.save_credit_profile(_credit_profile("APL-PRI"))
    await postgres_store.save_property_profile(_property_profile(
        piti_total=2500
    ))
    # Phase D: aus_ready is now gated on a real AUS approval, so seed one.
    await postgres_store.save_document({
        "document_id":      "DOC-AUS-1",
        "applicant_id":     "APL-PRI",
        "application_id":   "APP-1",
        "document_type":    "AUS_DU_FINDINGS",
        "document_category": "vendor",
        "borrower_role":    "primary",
        "extracted_fields": {"recommendation": "Approve/Eligible"},
    })

    ctx = await ContextAssembler(postgres_store, redis_store).assemble("APP-1")

    assert ctx.readiness.income_verified is True
    assert ctx.readiness.credit_pulled is True
    assert ctx.readiness.appraisal_complete is True
    assert ctx.readiness.insurance_bound is True
    assert ctx.readiness.aus_ready is True


@pytest.mark.asyncio
async def test_context_cached_in_redis(postgres_store, redis_store):
    await _seed_application(postgres_store)
    await postgres_store.save_income_profile(_income_profile())
    await postgres_store.save_credit_profile(_credit_profile("APL-PRI"))

    assembler = ContextAssembler(postgres_store, redis_store)
    await assembler.assemble("APP-1")

    cached = redis_store.get_application_context("APP-1")
    assert cached is not None
    assert cached["application_id"] == "APP-1"


@pytest.mark.asyncio
async def test_context_invalidated_after_income_update(
    postgres_store, redis_store
):
    await _seed_application(postgres_store)
    await postgres_store.save_income_profile(_income_profile())
    await postgres_store.save_credit_profile(_credit_profile("APL-PRI"))

    assembler = ContextAssembler(postgres_store, redis_store)
    await assembler.assemble("APP-1")
    assert redis_store.get_application_context("APP-1") is not None

    redis_store.invalidate_context("APP-1")
    assert redis_store.get_application_context("APP-1") is None
