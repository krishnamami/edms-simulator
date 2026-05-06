"""MISMO 3.4 type registry + field mapper.

Translation layer between real LOS systems (Encompass, BytePro, OpenClose,
MeridianLink, ...) and the simulator's internal model. Every LOS speaks
some flavour of MISMO 3.4; this module is the single place that maps:

  external doc-type code  ↔  internal document_type
  MISMO field names       ↔  internal extracted_fields keys
  internal type           ↔  document_category

The dictionaries below are the source of truth. New LOS connectors
should reuse them via :class:`MISMOMapper` rather than redefine.
"""
from __future__ import annotations

from typing import Optional


# ── MISMO 3.4 → internal document type mapping ──────────────────────────
MISMO_TO_INTERNAL: dict[str, str] = {
    # Income documents
    "TaxReturn":                    "TAX_RETURN_1040_CURRENT",
    "TaxReturnTranscript":          "IRS_TRANSCRIPT",
    "W2":                           "W2_CURRENT",
    "W2PriorYear":                  "W2_PRIOR",
    "PayStub":                      "PAYSTUB_CURRENT",
    "PayStubPriorPeriod":           "PAYSTUB_PRIOR",
    "EmploymentVerification":       "EMPLOYMENT_VERIFICATION",
    "SocialSecurityAwardLetter":    "SSA_AWARD_LETTER",
    "PensionAwardLetter":           "PENSION_AWARD_LETTER",
    "LeaseAgreement":               "LEASE_AGREEMENT",
    "MilitaryLeaveEarnings":        "LES",
    "Form1099":                     "1099_NEC",
    "ScheduleC":                    "SCHEDULE_C",
    "ScheduleE":                    "SCHEDULE_E",
    "ScheduleF":                    "SCHEDULE_F",
    "K1PartnershipIncome":          "K1_PARTNERSHIP",
    "OfferLetter":                  "OFFER_LETTER",

    # Asset documents
    "BankStatement":                "BANK_STATEMENT_M1",
    "RetirementAccountStatement":   "ASSET_STATEMENT_RETIREMENT",
    "BrokerageAccountStatement":    "ASSET_STATEMENT_BROKERAGE",
    "GiftLetter":                   "GIFT_LETTER",

    # Credit documents
    "CreditReport":                 "CREDIT_REPORT",
    "CreditSupplement":             "CREDIT_SUPPLEMENT",
    "CreditExplanationLetter":      "CREDIT_EXPLANATION_LETTER",

    # Identity documents
    "DriversLicense":               "IDENTITY_DL",
    "Passport":                     "IDENTITY_PASSPORT",
    "SocialSecurityCard":           "IDENTITY_SSN_CARD",
    "GreenCard":                    "IDENTITY_GREEN_CARD",
    "Visa":                         "IDENTITY_VISA",

    # Application / loan documents
    "UniformResidentialLoanApplication": "URLA_1003",
    "UnderwritingTransmittalSummary":    "FORM_1008",
    "RateLockAgreement":                 "RATE_LOCK",
    "PurchaseContract":                  "PURCHASE_AGREEMENT",
    "EarnestMoneyReceipt":               "EARNEST_MONEY_RECEIPT",

    # Property documents
    "UniformResidentialAppraisalReport": "APPRAISAL_URAR",
    "AppraisalUpdateAndOrCompletion":    "APPRAISAL_UPDATE",
    "DeskReview":                        "APPRAISAL_DESK",
    "FieldReview":                       "APPRAISAL_FIELD",
    "TitleCommitment":                   "TITLE_COMMITMENT",
    "TitleInsurancePolicy":              "TITLE_INSURANCE",
    "HazardInsuranceBinder":             "HOI_BINDER",
    "HazardInsuranceDeclarationsPage":   "HOI_DECLARATIONS",
    "FloodInsuranceBinder":              "FLOOD_INSURANCE_BINDER",
    "FloodCertification":                "FLOOD_CERT",
    "PropertyTaxBill":                   "PROPERTY_TAX_BILL",
    "TaxTranscript":                     "PROPERTY_TAX_TRANSCRIPT",
    "Survey":                            "SURVEY",
    "PestInspection":                    "PEST_INSPECTION",
    "HOACertification":                  "HOA_CERT",
    "CondominiumProjectQuestionnaire":   "CONDO_QUESTIONNAIRE",

    # AUS / vendor returns
    "AutomatedUnderwritingSystemData":   "AUS_DU_FINDINGS",
    "FreddieMacLoanProspectorFindings":  "AUS_LP_FINDINGS",
    "FraudReport":                       "FRAUD_REPORT",
    "SSAVerificationReport":             "SSN_VALIDATION",
    "OFACReport":                        "OFAC_REPORT",
}


# Internal → MISMO reverse map. Used when emitting MISMO-shaped responses
# back to a counterparty (e.g. a downstream Decision OS that wants the
# canonical MISMO name).
INTERNAL_TO_MISMO: dict[str, str] = {v: k for k, v in MISMO_TO_INTERNAL.items()}


# ── Encompass-specific document type codes ──────────────────────────────
# Encompass doesn't use MISMO names directly — it has its own labels.
ENCOMPASS_TO_INTERNAL: dict[str, str] = {
    "Tax Return":                   "TAX_RETURN_1040_CURRENT",
    "W-2":                          "W2_CURRENT",
    "Paystub":                      "PAYSTUB_CURRENT",
    "Pay Stub":                     "PAYSTUB_CURRENT",
    "Bank Statement":               "BANK_STATEMENT_M1",
    "Credit Report":                "CREDIT_REPORT",
    "Appraisal":                    "APPRAISAL_URAR",
    "1003":                         "URLA_1003",
    "Purchase Contract":            "PURCHASE_AGREEMENT",
    "Title Commitment":             "TITLE_COMMITMENT",
    "Homeowners Insurance":         "HOI_BINDER",
    "Flood Certificate":            "FLOOD_CERT",
    "1099":                         "1099_NEC",
    "Social Security Award Letter": "SSA_AWARD_LETTER",
    "Drivers License":              "IDENTITY_DL",
    "Employment Verification":      "EMPLOYMENT_VERIFICATION",
    "Gift Letter":                  "GIFT_LETTER",
    "4506-C":                       "IRS_TRANSCRIPT",
    "DU Findings":                  "AUS_DU_FINDINGS",
    "LP Findings":                  "AUS_LP_FINDINGS",
}


# ── MISMO field names → internal extracted_fields keys ──────────────────
MISMO_FIELD_MAP: dict[str, dict[str, str]] = {
    "W2_CURRENT": {
        "WagesAmount":                   "box1_wages",
        "FederalIncomeTaxWithheldAmount": "box2_federal_tax",
        "SocialSecurityWagesAmount":     "box3_ss_wages",
        "MedicareWagesAmount":           "box5_medicare_wages",
        "EmployerName":                  "employer_name",
        "EmployerIdentificationNumber":  "ein",
        "TaxYear":                       "tax_year",
        "EmployeeSSNLastFourDigits":     "ssn_last4",
    },
    "PAYSTUB_CURRENT": {
        "GrossEarningsYearToDateAmount": "ytd_gross",
        "PayPeriodEndDate":              "pay_period_end",
        "GrossPay":                      "gross_pay",
        "EmployerName":                  "employer_name",
        "EmployeeName":                  "employee_name",
    },
    "APPRAISAL_URAR": {
        "AppraisedValue":                  "appraised_value",
        "PropertyAppraisalEffectiveDate":  "appraisal_date",
        "PropertyConditionRatingType":     "condition_rating",
        "SubjectPropertyAddress":          "property_address",
    },
    "CREDIT_REPORT": {
        "CreditScoreValue":             "mid_score",
        "CreditRepositorySourceType":   "bureau",
        "TotalMonthlyPaymentAmount":    "total_monthly_obligations",
    },
    "FLOOD_CERT": {
        "FloodZoneIdentifier":              "flood_zone",
        "SpecialFloodHazardAreaIndicator":  "sfha",
        "FloodInsuranceRequiredIndicator":  "flood_insurance_required",
    },
}


# Document-category prefix table — keep the iteration order stable so
# more-specific prefixes (FORM_1008) win over broader ones if they ever
# overlap. Order doesn't matter for the present prefixes.
_CATEGORY_MAP: dict[str, list[str]] = {
    "income":     [
        "W2_", "PAYSTUB_", "TAX_RETURN", "IRS_TRANSCRIPT",
        "SSA_", "PENSION_", "LES", "1099_", "SCHEDULE_",
        "K1_", "OFFER_LETTER", "EMPLOYMENT_",
    ],
    "credit":     ["CREDIT_", "OFAC_", "SSN_"],
    "asset":      ["BANK_STATEMENT", "ASSET_STATEMENT", "GIFT_LETTER"],
    "property":   [
        "APPRAISAL_", "TITLE_", "HOI_", "FLOOD_",
        "PROPERTY_TAX", "SURVEY", "PEST_", "HOA_", "CONDO_",
    ],
    "loan":       ["URLA_", "FORM_1008", "RATE_LOCK", "PURCHASE_", "EARNEST_"],
    "compliance": ["FRAUD_", "AUS_", "HMDA_"],
    "identity":   ["IDENTITY_"],
}


# Content-signal table for fallback type detection. Same approach as
# pdf_adapter._DETECTION_PHRASES — kept here so any caller that has the
# raw text can use it (LOS connectors fall back to this when neither the
# Encompass nor MISMO label is recognised).
_CONTENT_SIGNALS: list[tuple[str, list[str]]] = [
    ("W2_CURRENT",        ["wage and tax statement", "form w-2", "w2"]),
    ("PAYSTUB_CURRENT",   ["ytd gross", "year to date", "pay period", "pay stub"]),
    ("BANK_STATEMENT_M1", ["account summary", "statement period", "beginning balance"]),
    ("CREDIT_REPORT",     ["experian", "equifax", "transunion", "credit report"]),
    ("TAX_RETURN_1040_CURRENT", ["form 1040", "individual income tax"]),
    ("APPRAISAL_URAR",    ["uniform residential appraisal", "opinion of value"]),
    ("TITLE_COMMITMENT",  ["title commitment", "alta commitment", "schedule b"]),
    ("HOI_BINDER",        ["homeowners insurance", "hazard insurance binder"]),
    ("FLOOD_CERT",        ["flood zone", "special flood hazard", "firm panel"]),
    ("PROPERTY_TAX_BILL", ["property tax", "annual tax", "assessed value"]),
    ("IRS_TRANSCRIPT",    ["tax transcript", "4506", "wage and income"]),
    ("SSA_AWARD_LETTER",  ["social security administration", "benefit amount"]),
    ("IDENTITY_DL",       ["driver", "license", "department of motor"]),
    ("AUS_DU_FINDINGS",   ["desktop underwriter", "du findings", "fannie mae"]),
    ("AUS_LP_FINDINGS",   ["loan prospector", "lpa findings", "freddie mac"]),
]


class MISMOMapper:
    """Static facade over the MISMO/Encompass/field/content tables."""

    @staticmethod
    def to_internal_type(
        external_type: str, source_system: str = "mismo_34"
    ) -> Optional[str]:
        """Convert a real LOS document type to the internal canonical type.

        Returns ``None`` if the label is unknown — caller can fall back to
        :func:`detect_type_from_content`.
        """
        if source_system == "encompass":
            return ENCOMPASS_TO_INTERNAL.get(external_type)
        return MISMO_TO_INTERNAL.get(external_type)

    @staticmethod
    def to_mismo_type(internal_type: str) -> Optional[str]:
        """Convert an internal type to MISMO 3.4. ``None`` if not in map."""
        return INTERNAL_TO_MISMO.get(internal_type)

    @staticmethod
    def map_fields(internal_type: str, mismo_fields: dict) -> dict:
        """Translate MISMO field names to internal extracted_fields keys.

        Unmapped fields pass through with their original key — that way
        rare extras don't disappear silently.
        """
        mapping = MISMO_FIELD_MAP.get(internal_type, {})
        return {mapping.get(k, k): v for k, v in (mismo_fields or {}).items()}

    @staticmethod
    def detect_type_from_content(text: str) -> Optional[str]:
        """Best-effort doc-type detection from raw text content."""
        text_lower = (text or "").lower()
        for internal_type, signals in _CONTENT_SIGNALS:
            if any(signal in text_lower for signal in signals):
                return internal_type
        return None

    @staticmethod
    def get_document_category(internal_type: str) -> str:
        """Map an internal doc-type to ``document_category`` for ``document_index``.

        Falls back to ``loan`` if no prefix matches.
        """
        for category, prefixes in _CATEGORY_MAP.items():
            if any(internal_type.startswith(p) for p in prefixes):
                return category
        return "loan"
