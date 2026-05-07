"""Encompass field ID → internal field name mapping.

Encompass stores loan data as field-id → value pairs. The IDs are
proprietary to ICE and differ from MISMO. Webhooks ship payloads like
``{"URLA.X1": "92400", "4868.X1": "Accenture LLC"}`` — this module
translates those into the internal field names the assemblers expect.

Usage::

    from core.ingestion.encompass_fields import EncompassFieldMapper
    mapper = EncompassFieldMapper()
    internal = mapper.translate(
        {"URLA.X1": "92400", "4868.X1": "Accenture"},
        doc_type="W2_CURRENT",
    )
    # → {"box1_wages": 92400, "employer_name": "Accenture"}
"""
from __future__ import annotations

from typing import Any, Optional


# ── Encompass field ID → internal field name ────────────────────────────
ENCOMPASS_FIELD_MAP: dict[str, str] = {
    # ── W2 / income ─────────────────────────────────────────────────────
    "URLA.X1":          "box1_wages",
    "URLA.X2":          "box2_federal_tax",
    "URLA.X3":          "employer_name",
    "URLA.X4":          "employer_address",
    "URLA.X5":          "employer_city",
    "URLA.X6":          "employer_state",
    "URLA.X7":          "employer_zip",
    "W2.X1":            "tax_year",
    "W2.X2":            "box1_wages",
    "W2.X3":            "box2_federal_tax",
    "W2.X4":            "box3_ss_wages",
    "W2.X5":            "box4_ss_tax",
    "W2.X6":            "box5_medicare_wages",
    "W2.X7":            "box6_medicare_tax",
    "4868.X1":          "employer_name",
    "4868.X2":          "employer_ein",
    "4868.X3":          "employer_address",
    "4868.X21":         "employee_ssn_last4",

    # ── 1003 / URLA borrower income ─────────────────────────────────────
    "1084.X1":          "base_income_monthly",
    "1084.X2":          "overtime_monthly",
    "1084.X3":          "bonus_monthly",
    "1084.X4":          "commission_monthly",
    "1084.X5":          "other_income_monthly",
    "1084.X6":          "total_income_monthly",
    "FE0115":           "base_pay_annual",
    "FE0116":           "total_income_annual",
    "1084.X21":         "co_borrower_base_income_monthly",
    "1084.X22":         "co_borrower_overtime_monthly",
    "1084.X23":         "co_borrower_bonus_monthly",
    "1084.X25":         "co_borrower_total_income_monthly",

    # ── Employment ──────────────────────────────────────────────────────
    "CX.EMP.NAME":      "employer_name",
    "CX.EMP.PHONE":     "employer_phone",
    "CX.EMP.START":     "employment_start_date",
    "CX.EMP.YEARS":     "years_employed",
    "38":               "years_employed",
    "1069":             "employer_name",
    "1070":             "employer_phone",
    "1714":             "employment_start_date",
    "CX.SELFEMPL":      "is_self_employed",

    # ── Borrower identity ───────────────────────────────────────────────
    "4000":             "first_name",
    "4001":             "last_name",
    "4002":             "ssn",
    "4003":             "dob",
    "65":               "marital_status",
    "CX.CITI":          "citizenship_status",
    "4004":             "co_first_name",
    "4005":             "co_last_name",
    "4006":             "co_ssn",
    "4007":             "co_dob",

    # ── Loan terms ──────────────────────────────────────────────────────
    "1":                "loan_amount",
    "2":                "interest_rate",
    "4":                "loan_term_months",
    "19":               "loan_purpose",
    "1041":             "loan_type",
    "1172":             "occupancy_type",
    "3":                "purchase_price",
    "136":              "ltv",
    "140":              "cltv",
    "1335":             "lock_date",
    "762":              "lock_expiry_date",

    # ── Property ────────────────────────────────────────────────────────
    "11":               "property_address",
    "12":               "property_city",
    "13":               "property_state",
    "15":               "property_zip",
    "16":               "property_type",
    "1004.X1":          "appraised_value",
    "1004.X2":          "appraisal_date",
    "1004.X3":          "appraiser_name",
    "1004.X4":          "appraisal_company",
    "356":              "appraised_value",
    "357":              "appraisal_date",
    "1485":             "year_built",
    "1486":             "gross_living_area",
    "1715":             "number_of_units",

    # ── Title ───────────────────────────────────────────────────────────
    "675":              "title_company_name",
    "676":              "title_company_phone",
    "674":              "title_commitment_date",
    "CX.TITLE.VEST":    "vesting_type",

    # ── Insurance ───────────────────────────────────────────────────────
    "232":              "hoi_annual_premium",
    "1715.HOI":         "hoi_carrier_name",
    "CX.HOI.POLICY":    "hoi_policy_number",
    "CX.FLOOD.ZONE":    "flood_zone",
    "CX.FLOOD.DET":     "flood_determination_date",
    "CX.FLOOD.CERT":    "flood_cert_number",
    "CX.FLOOD.INS":     "flood_insurance_required",

    # ── Tax ─────────────────────────────────────────────────────────────
    "1323":             "annual_property_taxes",
    "CX.TAX.ASSESSED":  "tax_assessed_value",
    "CX.TAX.YEAR":      "tax_year",

    # ── Credit ──────────────────────────────────────────────────────────
    "NEWHUD.X1":        "credit_pull_date",
    "742":              "credit_score_experian",
    "743":              "credit_score_equifax",
    "744":              "credit_score_transunion",
    "745":              "mid_score",
    "1475":             "credit_score_used",
    "CX.CREDIT.BAND":   "credit_band",

    # ── AUS ─────────────────────────────────────────────────────────────
    "CASASRN":          "du_case_number",
    "LPKEY":            "lp_key_number",
    "1544":             "du_recommendation",
    "1543":             "lp_recommendation",
    "CX.DU.REC":        "du_recommendation",
    "CX.LP.REC":        "lp_recommendation",
    "CX.AUS.DATE":      "aus_run_date",

    # ── PITI ────────────────────────────────────────────────────────────
    "1717":             "principal_interest",
    "1718":             "monthly_taxes",
    "1719":             "monthly_insurance",
    "1720":             "monthly_hoa",
    "1722":             "total_piti",

    # ── DTI ─────────────────────────────────────────────────────────────
    "45":               "front_end_dti",
    "46":               "back_end_dti",
    "1169":             "qualifying_rate",

    # ── Fees / closing ──────────────────────────────────────────────────
    "GFE.X5":           "origination_fee",
    "GFE.X11":          "appraisal_fee",
    "GFE.X13":          "title_fee",
    "1068":             "total_closing_costs",
}


# ── Document type → which field IDs are relevant ────────────────────────
# When Encompass sends a doc-type webhook, the payload typically carries
# the entire loan field set. We extract only what's relevant to that
# document type so e.g. DTI fields don't end up indexed under a W2.
DOC_TYPE_FIELD_IDS: dict[str, list[str]] = {
    "W2_CURRENT": [
        "W2.X1", "W2.X2", "W2.X3", "W2.X4", "W2.X5", "W2.X6", "W2.X7",
        "4868.X1", "4868.X2", "4868.X3", "4868.X21",
        "URLA.X1", "URLA.X3",
    ],
    "CREDIT_REPORT": [
        "742", "743", "744", "745", "1475", "NEWHUD.X1", "CX.CREDIT.BAND",
    ],
    "APPRAISAL_URAR": [
        "1004.X1", "1004.X2", "1004.X3", "1004.X4", "356", "357",
        "1485", "1486", "1715",
        "11", "12", "13", "15",
    ],
    "HOI_BINDER": [
        "232", "1715.HOI", "CX.HOI.POLICY",
    ],
    "FLOOD_CERT": [
        "CX.FLOOD.ZONE", "CX.FLOOD.DET", "CX.FLOOD.CERT",
        "CX.FLOOD.INS",
    ],
    "PROPERTY_TAX_BILL": [
        "1323", "CX.TAX.ASSESSED", "CX.TAX.YEAR",
    ],
    "AUS_DU_FINDINGS": [
        "CASASRN", "1544", "CX.DU.REC", "CX.AUS.DATE", "45", "46",
    ],
    "AUS_LP_FINDINGS": [
        "LPKEY", "1543", "CX.LP.REC", "CX.AUS.DATE", "45", "46",
    ],
    "URLA_1003": [
        "1", "2", "3", "4", "19", "1041", "1172",
        "4000", "4001", "4003", "4004", "4005", "4007",
        "11", "12", "13", "15", "16",
        "1084.X1", "1084.X6", "1084.X21", "1084.X25",
        "45", "46", "136", "140",
    ],
}


# When Encompass provides the field directly, confidence is higher than
# our PDF extraction baseline (0.94 W2, 0.93 paystub) because the values
# are structured rather than OCR'd.
ENCOMPASS_FIELD_CONFIDENCE: dict[str, float] = {
    "W2_CURRENT":      0.97,
    "CREDIT_REPORT":   0.95,
    "APPRAISAL_URAR":  0.93,
    "HOI_BINDER":      0.93,
    "FLOOD_CERT":      0.99,
    "AUS_DU_FINDINGS": 0.99,
    "AUS_LP_FINDINGS": 0.99,
    "URLA_1003":       0.97,
}


_NUMERIC_FIELDS = {
    "box1_wages", "box2_federal_tax", "box3_ss_wages",
    "box4_ss_tax", "box5_medicare_wages", "box6_medicare_tax",
    "base_income_monthly", "total_income_monthly",
    "loan_amount", "interest_rate", "purchase_price",
    "appraised_value", "annual_property_taxes",
    "hoi_annual_premium", "front_end_dti", "back_end_dti",
    "ltv", "cltv", "total_piti", "principal_interest",
    "monthly_taxes", "monthly_insurance", "monthly_hoa",
    "credit_score_experian", "credit_score_equifax",
    "credit_score_transunion", "mid_score",
    "total_closing_costs", "gross_living_area",
    "year_built", "tax_year", "loan_term_months",
    "tax_assessed_value", "credit_score_used",
}
_BOOL_FIELDS = {
    "is_self_employed", "flood_insurance_required",
}


class EncompassFieldMapper:
    """Translate Encompass field IDs to internal field names + extract
    only the fields relevant to a given document type."""

    def translate(
        self, encompass_fields: dict, doc_type: Optional[str] = None
    ) -> dict:
        """Convert ``{field_id: value}`` from Encompass to
        ``{internal_field_name: value}``. If ``doc_type`` is provided,
        unrelated fields are filtered out."""
        relevant: Optional[set] = None
        if doc_type and doc_type in DOC_TYPE_FIELD_IDS:
            relevant = set(DOC_TYPE_FIELD_IDS[doc_type])

        result: dict = {}
        for field_id, value in (encompass_fields or {}).items():
            if relevant is not None and field_id not in relevant:
                continue
            internal = ENCOMPASS_FIELD_MAP.get(field_id)
            if internal and value is not None and value != "":
                result[internal] = self._coerce(internal, value)
        return result

    def get_confidence(self, doc_type: str) -> float:
        """Return the confidence to attach to fields extracted from an
        Encompass payload of this document type."""
        return ENCOMPASS_FIELD_CONFIDENCE.get(doc_type, 0.90)

    @staticmethod
    def _coerce(field_name: str, value: Any) -> Any:
        """Coerce string values from Encompass into appropriate Python
        types so the assembler doesn't have to."""
        if field_name in _NUMERIC_FIELDS:
            try:
                s = str(value).replace(",", "").replace("$", "").strip()
                if "." in s:
                    return float(s)
                return int(s)
            except (ValueError, TypeError):
                return value
        if field_name in _BOOL_FIELDS:
            return str(value).strip().lower() in ("true", "yes", "1", "y")
        return value

    def detect_doc_type(
        self,
        encompass_fields: dict,
        encompass_doc_type: Optional[str] = None,
    ) -> str:
        """Resolve the internal document_type. Tries the explicit
        Encompass label first, falls back to a field-content signature
        when the label is missing or unrecognised."""
        from core.ingestion.mismo import MISMOMapper

        if encompass_doc_type:
            internal = MISMOMapper.to_internal_type(
                encompass_doc_type, "encompass"
            )
            if internal:
                return internal

        keys = set((encompass_fields or {}).keys())
        if keys & {"W2.X1", "W2.X2", "4868.X1"}:
            return "W2_CURRENT"
        if keys & {"742", "743", "744", "745"}:
            return "CREDIT_REPORT"
        if keys & {"1004.X1", "356"}:
            return "APPRAISAL_URAR"
        if keys & {"CASASRN", "1544", "CX.DU.REC"}:
            return "AUS_DU_FINDINGS"
        if keys & {"LPKEY", "1543", "CX.LP.REC"}:
            return "AUS_LP_FINDINGS"
        if keys & {"CX.FLOOD.ZONE", "CX.FLOOD.CERT"}:
            return "FLOOD_CERT"
        if keys & {"232", "1715.HOI"}:
            return "HOI_BINDER"
        if keys & {"1323", "CX.TAX.ASSESSED"}:
            return "PROPERTY_TAX_BILL"
        return "UNKNOWN"


# ── BytePro Cloud field IDs (alternative LOS) ───────────────────────────
BYTEPROCLOUD_FIELD_MAP: dict[str, str] = {
    "BorrowerFirstName":     "first_name",
    "BorrowerLastName":      "last_name",
    "LoanAmount":            "loan_amount",
    "InterestRate":          "interest_rate",
    "AppraiedValue":         "appraised_value",  # BytePro typo as-shipped
    "AppraisedValue":        "appraised_value",
    "PurchasePrice":         "purchase_price",
    "PropertyAddress":       "property_address",
    "CreditScore":           "mid_score",
    "TotalIncome":           "total_income_monthly",
    "BaseIncome":            "base_income_monthly",
    "FrontRatio":            "front_end_dti",
    "BackRatio":             "back_end_dti",
    "W2Wages":               "box1_wages",
    "EmployerName":          "employer_name",
    "AUSRecommendation":     "recommendation",
    "FloodZone":             "flood_zone",
    "AnnualTaxes":           "annual_property_taxes",
    "HOIPremium":            "hoi_annual_premium",
}


# ── OpenClose field IDs ─────────────────────────────────────────────────
OPENCLOSE_FIELD_MAP: dict[str, str] = {
    "borrower.firstName":       "first_name",
    "borrower.lastName":        "last_name",
    "loan.amount":              "loan_amount",
    "loan.rate":                "interest_rate",
    "property.appraisedValue":  "appraised_value",
    "property.address":         "property_address",
    "credit.midScore":          "mid_score",
    "income.base":              "base_income_monthly",
    "income.total":             "total_income_monthly",
    "dti.front":                "front_end_dti",
    "dti.back":                 "back_end_dti",
    "aus.recommendation":       "recommendation",
}


def get_los_mapper(source_system: str) -> dict:
    """Return the field map for a given LOS. Used by LOSConnector
    subclasses that want raw-field translation without the W2/credit/etc.
    doc-type filtering :class:`EncompassFieldMapper` provides."""
    maps = {
        "encompass":    ENCOMPASS_FIELD_MAP,
        "byteprocloud": BYTEPROCLOUD_FIELD_MAP,
        "openclose":    OPENCLOSE_FIELD_MAP,
    }
    return maps.get((source_system or "").lower(), {})
