"""Tests for the Encompass field ID → internal field translator."""
import pytest

from core.ingestion.encompass_fields import EncompassFieldMapper


@pytest.fixture
def mapper():
    return EncompassFieldMapper()


def test_w2_field_translation(mapper):
    encompass_fields = {
        "W2.X2": "92400", "4868.X1": "Accenture LLC", "W2.X1": "2023",
    }
    result = mapper.translate(encompass_fields, "W2_CURRENT")
    assert result["box1_wages"] == 92400
    assert result["employer_name"] == "Accenture LLC"
    assert result["tax_year"] == 2023


def test_credit_field_translation(mapper):
    encompass_fields = {
        "742": "732", "743": "721", "744": "723", "745": "723",
    }
    result = mapper.translate(encompass_fields, "CREDIT_REPORT")
    assert result["credit_score_experian"] == 732
    assert result["mid_score"] == 723


def test_appraisal_field_translation(mapper):
    encompass_fields = {"1004.X1": "485000", "1004.X2": "2026-05-06"}
    result = mapper.translate(encompass_fields, "APPRAISAL_URAR")
    assert result["appraised_value"] == 485000
    assert result["appraisal_date"] == "2026-05-06"


def test_irrelevant_fields_filtered(mapper):
    """DTI fields (45/46) are not relevant to a W2 doc — they should be
    filtered out of the W2_CURRENT translation but the wage field stays."""
    encompass_fields = {"W2.X2": "92400", "45": "32.1", "46": "39.8"}
    result = mapper.translate(encompass_fields, "W2_CURRENT")
    assert "front_end_dti" not in result
    assert "back_end_dti" not in result
    assert result["box1_wages"] == 92400


def test_numeric_coercion_of_currency_strings(mapper):
    assert mapper._coerce("box1_wages", "92,400") == 92400
    assert mapper._coerce("interest_rate", "7.0") == 7.0
    assert mapper._coerce("flood_insurance_required", "Yes") is True
    assert mapper._coerce("flood_insurance_required", "no") is False


def test_doc_type_detection_from_credit_fields(mapper):
    fields = {"742": "732", "745": "723"}
    assert mapper.detect_doc_type(fields) == "CREDIT_REPORT"


def test_doc_type_detection_from_w2_fields(mapper):
    fields = {"W2.X1": "2023", "W2.X2": "92400"}
    assert mapper.detect_doc_type(fields) == "W2_CURRENT"


def test_doc_type_detection_from_appraisal_fields(mapper):
    fields = {"1004.X1": "485000"}
    assert mapper.detect_doc_type(fields) == "APPRAISAL_URAR"


def test_doc_type_detection_falls_back_to_unknown(mapper):
    fields = {"some_random_id": "value"}
    assert mapper.detect_doc_type(fields) == "UNKNOWN"


def test_doc_type_explicit_label_wins(mapper):
    """When the caller passes an explicit Encompass document type, that
    takes precedence over field-content detection."""
    fields = {"742": "732"}  # would otherwise detect CREDIT_REPORT
    assert mapper.detect_doc_type(fields, "W-2") == "W2_CURRENT"


def test_encompass_confidence_higher_than_pdf(mapper):
    """Encompass-supplied structured fields are higher confidence than
    PDF extraction. AUS findings even higher (vendor data is canonical)."""
    assert mapper.get_confidence("W2_CURRENT") == 0.97
    assert mapper.get_confidence("FLOOD_CERT") == 0.99
    assert mapper.get_confidence("AUS_DU_FINDINGS") == 0.99


def test_translate_skips_empty_values(mapper):
    """Empty strings shouldn't pollute extracted_fields with falsy noise."""
    result = mapper.translate({"W2.X2": "", "W2.X1": "2023"}, "W2_CURRENT")
    assert "box1_wages" not in result
    assert result["tax_year"] == 2023


def test_translate_with_no_doc_type_keeps_all_known_fields(mapper):
    """Without a doc_type filter, every recognised Encompass field is
    translated — useful when the LOS doesn't tell us what the doc is."""
    result = mapper.translate(
        {"W2.X2": "92400", "742": "732", "1004.X1": "485000"},
    )
    assert result["box1_wages"] == 92400
    assert result["credit_score_experian"] == 732
    assert result["appraised_value"] == 485000
