"""Tests for ConfidenceResolver and SOURCE_CONFIDENCE_RANKING."""
from core.ingestion.confidence import (
    SOURCE_CONFIDENCE_RANKING,
    ConfidenceResolver,
    FieldValue,
)
from core.ingestion.events import ChannelType


def _v(value, confidence, source, channel):
    return FieldValue(
        value=value,
        confidence=confidence,
        source=source,
        source_channel=channel,
    )


def test_irs_beats_w2_beats_chat_for_same_field():
    chat = _v(92000, SOURCE_CONFIDENCE_RANKING["CHAT"], "CHAT", ChannelType.CHAT)
    w2 = _v(92400, SOURCE_CONFIDENCE_RANKING["W2_PDF"], "W2_PDF", ChannelType.PDF_UPLOAD)
    irs = _v(
        92400,
        SOURCE_CONFIDENCE_RANKING["IRS_TRANSCRIPT"],
        "IRS_TRANSCRIPT",
        ChannelType.XML,
    )

    res = ConfidenceResolver().resolve("annual_income", [chat, w2, irs])
    assert res.chosen.source == "IRS_TRANSCRIPT"
    # Without IRS, W2 should still beat CHAT.
    res2 = ConfidenceResolver().resolve("annual_income", [chat, w2])
    assert res2.chosen.source == "W2_PDF"


def test_no_conflict_when_values_within_10_percent():
    chat = _v(92000, 0.80, "CHAT", ChannelType.CHAT)
    w2 = _v(92400, 0.95, "W2_PDF", ChannelType.PDF_UPLOAD)

    res = ConfidenceResolver().resolve("annual_income", [chat, w2])
    assert res.chosen.source == "W2_PDF"
    assert res.has_conflict is False


def test_conflict_flagged_when_values_diverge_more_than_10_percent():
    chat = _v(75000, 0.80, "CHAT", ChannelType.CHAT)
    w2 = _v(92400, 0.95, "W2_PDF", ChannelType.PDF_UPLOAD)

    res = ConfidenceResolver().resolve("annual_income", [chat, w2])
    assert res.chosen.source == "W2_PDF"
    assert res.has_conflict is True
    assert "75000" in res.conflict_reason
    assert "92400" in res.conflict_reason


def test_single_value_never_conflicts():
    only = _v(92400, 0.95, "W2_PDF", ChannelType.PDF_UPLOAD)
    res = ConfidenceResolver().resolve("annual_income", [only])
    assert res.has_conflict is False
    assert res.chosen.source == "W2_PDF"


def test_non_numeric_values_skip_conflict_check():
    chat = _v("Accenture LLC", 0.80, "CHAT", ChannelType.CHAT)
    form = _v("Accenture", 0.85, "WEB_FORM", ChannelType.FORM)

    res = ConfidenceResolver().resolve("employer", [chat, form])
    assert res.chosen.source == "WEB_FORM"
    # String mismatch isn't a numeric conflict — we don't flag it here.
    assert res.has_conflict is False


def test_source_ranking_ordering_is_correct():
    # Sanity-check the published ranking from the spec.
    ranks = SOURCE_CONFIDENCE_RANKING
    assert ranks["IRS_TRANSCRIPT"] > ranks["PAYROLL_API"] > ranks["W2_PDF"]
    assert ranks["W2_PDF"] > ranks["PAYSTUB_PDF"] > ranks["BANK_STMT_PDF"]
    assert ranks["API_JSON"] > ranks["WEB_FORM"] > ranks["CHAT"] > ranks["EMAIL_BODY"]
    assert ranks["EMAIL_BODY"] > ranks["VERBAL_STATED"]
