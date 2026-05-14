"""Unit tests for the 7 credit-assessment fields added to
``borrower.credit`` in ``core/aggregation/entity_state_builder.py``.

Decision OS's credit_assessment persona reads these fields to gate
ALLOW / ESCALATE / BLOCK. The fields are derived from the underwriting
bucket ``mid_score`` already places the applicant in; ``applicant_id``
seeds the randomness so re-running a backfill is stable.
"""
from __future__ import annotations

from core.aggregation.entity_state_builder import (
    _credit_summary,
    _derive_credit_assessment_fields,
)


_REQUIRED_KEYS = {
    "active_bankruptcy",
    "foreclosure_last_36_months",
    "thin_file",
    "no_derogatory_last_24_months",
    "derogatory_marks",
    "open_tradelines",
    "credit_utilization",
}


def test_credit_summary_returns_empty_when_no_credit():
    assert _credit_summary(None, applicant_id="APL-00001-P") == {}


def test_credit_summary_includes_all_seven_assessment_fields():
    credit = {
        "mid_score":         720,
        "credit_band":       "near-prime",
        "experian_score":    725,
        "equifax_score":     720,
        "transunion_score":  715,
        "monthly_obligations": 600.0,
    }
    out = _credit_summary(credit, applicant_id="APL-00001-P")
    # Existing fields preserved.
    assert out["mid_score"] == 720
    assert out["credit_band"] == "near-prime"
    # New fields all present.
    assert _REQUIRED_KEYS.issubset(out.keys())


def test_prime_bucket_yields_zero_derog_low_utilization():
    out = _derive_credit_assessment_fields("APL-00001-P", 760, "prime")
    assert out["derogatory_marks"] == 0
    assert out["no_derogatory_last_24_months"] is True
    assert out["thin_file"] is False
    assert 6 <= out["open_tradelines"] <= 15
    assert 0.05 <= out["credit_utilization"] <= 0.30


def test_near_prime_bucket_yields_one_derog_medium_utilization():
    out = _derive_credit_assessment_fields("APL-00002-P", 700, "near-prime")
    assert out["derogatory_marks"] == 1
    assert out["no_derogatory_last_24_months"] is True
    assert 6 <= out["open_tradelines"] <= 15
    assert 0.20 <= out["credit_utilization"] <= 0.50


def test_subprime_bucket_yields_two_derog_high_utilization():
    out = _derive_credit_assessment_fields("APL-00003-P", 640, "subprime")
    assert out["derogatory_marks"] == 2
    # 70% true; can be either — just assert it's a bool.
    assert isinstance(out["no_derogatory_last_24_months"], bool)
    assert 3 <= out["open_tradelines"] <= 8
    assert 0.40 <= out["credit_utilization"] <= 0.70


def test_deep_subprime_bucket_yields_four_derog_thin_file_true():
    # mid_score < 620 AND credit_band == 'subprime' → thin_file True.
    out = _derive_credit_assessment_fields("APL-00004-P", 580, "subprime")
    assert out["derogatory_marks"] == 4
    assert out["no_derogatory_last_24_months"] is False
    assert out["thin_file"] is True
    assert 1 <= out["open_tradelines"] <= 3
    assert 0.60 <= out["credit_utilization"] <= 0.95


def test_default_synthetic_fields_are_false():
    """active_bankruptcy + foreclosure_last_36_months default to False
    for synthetic data — they would live in a real public_records pull
    that this simulator does not generate."""
    for score, band in [(760, "prime"), (700, "near-prime"),
                        (640, "subprime"), (580, "subprime")]:
        out = _derive_credit_assessment_fields("APL-X-P", score, band)
        assert out["active_bankruptcy"] is False
        assert out["foreclosure_last_36_months"] is False


def test_derivation_is_deterministic_by_applicant_id():
    """Re-running the backfill on the same applicant must yield the
    same fields — otherwise downstream Decision OS snapshots churn on
    every rebuild and break diff-based audits."""
    a = _derive_credit_assessment_fields("APL-42-P", 700, "near-prime")
    b = _derive_credit_assessment_fields("APL-42-P", 700, "near-prime")
    assert a == b


def test_derivation_varies_across_applicant_ids():
    """Two different applicants in the same bucket should not collapse
    to identical random values — verifies the seed actually drives
    distinct streams."""
    samples = {
        _derive_credit_assessment_fields(f"APL-{i:05d}-P", 700, "near-prime")[
            "credit_utilization"
        ]
        for i in range(20)
    }
    # 20 applicants → expect at least a handful of distinct values.
    assert len(samples) > 1


def test_thin_file_only_true_for_deep_subprime_band():
    """Spec: ``thin_file`` is true only when mid_score < 620 AND
    credit_band == 'subprime'. Everything else is False."""
    assert _derive_credit_assessment_fields("a", 580, "subprime")["thin_file"] is True
    assert _derive_credit_assessment_fields("a", 580, "prime")["thin_file"] is False
    assert _derive_credit_assessment_fields("a", 700, "subprime")["thin_file"] is False
    assert _derive_credit_assessment_fields("a", 800, "prime")["thin_file"] is False


def test_credit_summary_skips_derivation_when_mid_score_missing():
    """A credit profile that came back without a mid_score (rare — only
    the synthetic fallback path) should still produce a summary; just
    without the 7 derived fields."""
    credit = {"mid_score": None, "credit_band": "near-prime"}
    out = _credit_summary(credit, applicant_id="APL-X-P")
    assert "credit_band" in out
    assert not _REQUIRED_KEYS.intersection(out.keys())
