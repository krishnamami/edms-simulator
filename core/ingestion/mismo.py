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

    # ── Income additions (full mortgage lifecycle) ──────────────────────
    "TaxReturnPriorYear":                "TAX_RETURN_1040_PRIOR",
    "RetirementAwardLetter":             "PENSION_LETTER",
    "LeaveAndEarningsStatement":         "MILITARY_LES",

    # ── Employment / VOE additions ──────────────────────────────────────
    "EmploymentVerificationTWN":         "VOE_TWN",
    "EquifaxWorkforceVerification":      "VOE_EQUIFAX",

    # ── Asset additions ─────────────────────────────────────────────────
    "InvestmentAccountStatement":        "ASSET_BROKERAGE",

    # ── Liability additions ─────────────────────────────────────────────
    "StudentLoanStatement":              "STUDENT_LOAN_STATEMENT",
    "DivorceDecree":                     "DIVORCE_DECREE",
    "ChildSupportOrder":                 "CHILD_SUPPORT_ORDER",

    # ── Identity additions ──────────────────────────────────────────────
    "ITINLetter":                        "IDENTITY_ITIN",

    # ── Property additions ──────────────────────────────────────────────
    "MarketConditionsAddendum":          "FORM_1004MC",
    "AutomatedValuationModel":           "AVM_REPORT",
    "WindHailInsurance":                 "WIND_HAIL_INSURANCE",
    "WDOReport":                         "PEST_WDO_INSPECTION",
    "WellSepticInspection":              "WELL_SEPTIC_INSPECTION",

    # ── Loan application additions ──────────────────────────────────────
    "LoanEstimate":                      "LOAN_ESTIMATE",
    "ClosingDisclosure":                 "CLOSING_DISCLOSURE",

    # ── Vendor return additions ─────────────────────────────────────────
    "BankruptcySearch":                  "BANKRUPTCY_SEARCH",
    "JudgmentLienSearch":                "JUDGMENT_LIEN_SEARCH",
    "UndisclosedDebtMonitoring":         "UNDISCLOSED_DEBT",
    "HOIVerification":                   "HOI_VERIFICATION",
}


# ── MISMO aliases — many-to-one external→internal forward routes ────────
# Each key here resolves to an internal type that already has a canonical
# entry in MISMO_TO_INTERNAL. The alias dict gives ``to_internal_type`` a
# second lookup table so common synonyms / typos / vendor variants still
# resolve, without polluting the strict 1:1 reverse map.
MISMO_ALIASES: dict[str, str] = {
    "RentalAgreement":            "LEASE_AGREEMENT",
    "WorkNumberReport":           "VOE_TWN",
    "DownPaymentGiftLetter":      "GIFT_LETTER",
    "DivorceDegree":              "DIVORCE_DECREE",
    "PermanentResidentCard":      "IDENTITY_GREEN_CARD",
    "IdentityVerificationReport": "FRAUD_REPORT",
    "PropertyTaxTranscript":      "PROPERTY_TAX_TRANSCRIPT",
}


# ── Internal doc-type aliases — caller-supplied → canonical ─────────────
# Callers (and Decision OS) often send the "common name" for a doc type
# rather than the canonical form the simulator stores in document_index.
# E.g. a UI might send ``DRIVERS_LICENSE`` while the canonical type is
# ``IDENTITY_DL``. ``canonicalize_doc_type`` resolves these so:
#   - the missing-documents catalog and identity / asset aggregators see a
#     single doc_type per slot regardless of which name was uploaded
#   - downstream readers (graph, slices, context) don't have to dedup
DOC_TYPE_ALIASES: dict[str, str] = {
    # Income — the canonical types use TAX_RETURN_1040_* / 1099_NEC etc.
    "FORM_1040":                "TAX_RETURN_1040_CURRENT",
    "FORM_1040_CURRENT":        "TAX_RETURN_1040_CURRENT",
    "FORM_1040_PRIOR":          "TAX_RETURN_1040_PRIOR",
    "FORM_1099_NEC":            "1099_NEC",
    "K1_SCHEDULE":              "K1_PARTNERSHIP",
    "RENTAL_LEASE":             "LEASE_AGREEMENT",
    "MILITARY_LES":             "LES",
    # Credit
    "CREDIT_EXPLANATION":       "CREDIT_EXPLANATION_LETTER",
    # Asset — UI-friendly names → canonical "ASSET_STATEMENT_*" forms
    "RETIREMENT_ACCOUNT":       "ASSET_STATEMENT_RETIREMENT",
    "BROKERAGE_ACCOUNT":        "ASSET_STATEMENT_BROKERAGE",
    "ASSET_BROKERAGE":          "ASSET_STATEMENT_BROKERAGE",
    "ASSET_RETIREMENT":         "ASSET_STATEMENT_RETIREMENT",
    # Identity
    "DRIVERS_LICENSE":          "IDENTITY_DL",
    "ID_DRIVERS_LICENSE":       "IDENTITY_DL",
    "PASSPORT":                 "IDENTITY_PASSPORT",
    "SSN_CARD":                 "IDENTITY_SSN_CARD",
    "SSA_VALIDATION":           "SSN_VALIDATION",
    "OFAC_CHECK":               "OFAC_REPORT",
    # Property
    "WDO_REPORT":               "PEST_WDO_INSPECTION",
    "TERMITE_REPORT":           "PEST_WDO_INSPECTION",
    "WIND_HAIL":                "WIND_HAIL_INSURANCE",
}


def canonicalize_doc_type(doc_type: Optional[str]) -> Optional[str]:
    """Resolve a caller-supplied doc_type to its canonical internal form.

    No-op when ``doc_type`` is None / empty / already canonical. Used by
    the persistence layer so two callers passing ``DRIVERS_LICENSE`` and
    ``IDENTITY_DL`` end up rowed against the same identity slot.
    """
    if not doc_type:
        return doc_type
    return DOC_TYPE_ALIASES.get(doc_type, doc_type)


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

    # ── Encompass-specific additions ────────────────────────────────────
    "Schedule C":                   "SCHEDULE_C",
    "Schedule E":                   "SCHEDULE_E",
    "K-1":                          "K1_PARTNERSHIP",
    "Pension Award Letter":         "PENSION_AWARD_LETTER",
    "Lease Agreement":              "LEASE_AGREEMENT",
    "Military LES":                 "MILITARY_LES",
    "401k Statement":               "ASSET_STATEMENT_RETIREMENT",
    "IRA Statement":                "ASSET_STATEMENT_RETIREMENT",
    "Brokerage Statement":          "ASSET_STATEMENT_BROKERAGE",
    "Divorce Decree":               "DIVORCE_DECREE",
    "Child Support Order":          "CHILD_SUPPORT_ORDER",
    "Green Card":                   "IDENTITY_GREEN_CARD",
    "Appraisal Update":             "APPRAISAL_UPDATE",
    "1004MC":                       "FORM_1004MC",
    "AVM":                          "AVM_REPORT",
    "Title Insurance":              "TITLE_INSURANCE",
    "HOI Declarations":             "HOI_DECLARATIONS",
    "Flood Insurance":              "FLOOD_INSURANCE_BINDER",
    "Property Tax Bill":            "PROPERTY_TAX_BILL",
    "Pest Inspection":              "PEST_WDO_INSPECTION",
    "HOA Cert":                     "HOA_CERT",
    "Condo Questionnaire":          "CONDO_QUESTIONNAIRE",
    "Purchase Agreement":           "PURCHASE_AGREEMENT",
    "Rate Lock":                    "RATE_LOCK",
    "Fraud Report":                 "FRAUD_REPORT",
    "Undisclosed Debt":             "UNDISCLOSED_DEBT",
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
        # Build: comprehensive indexing
        "MILITARY_", "VOE_",
        "STUDENT_LOAN_", "DIVORCE_", "CHILD_SUPPORT_", "LEASE_",
    ],
    # OFAC_ and SSN_ moved to "vendor" — they're vendor-side checks, not
    # credit. Pure credit-bureau docs only here.
    "credit":     ["CREDIT_"],
    "asset":      ["BANK_STATEMENT", "ASSET_STATEMENT", "ASSET_RETIREMENT",
                   "ASSET_BROKERAGE", "GIFT_LETTER"],
    "property":   [
        "APPRAISAL_", "TITLE_", "HOI_", "FLOOD_",
        "PROPERTY_TAX", "SURVEY", "PEST_", "HOA_", "CONDO_",
        # Build: comprehensive indexing
        "FORM_1004MC", "AVM_", "WELL_SEPTIC_", "HOA_CERT",
        "WIND_HAIL_", "PROPERTY_TAX_TRANSCRIPT",
        "EARNEST_",
    ],
    # Renamed from "loan" → "loan_terms" so the category column matches the
    # missing-documents catalog vocabulary. No callers depend on the old
    # name (verified via grep at the time of the rename).
    "loan_terms": ["URLA_", "FORM_1008", "RATE_LOCK", "PURCHASE_", "EARNEST_"],
    # Renamed from "compliance" → "vendor". The synthetic AUS path in
    # api/routes.py and the missing-documents catalog already used
    # "vendor" — this aligns _CATEGORY_MAP with that convention.
    "vendor":     [
        "FRAUD_", "AUS_", "HMDA_",
        # Build: comprehensive indexing
        "BANKRUPTCY_", "JUDGMENT_", "UNDISCLOSED_", "HOI_VERIF",
        "LOAN_ESTIMATE", "CLOSING_DISCLOSURE",
        # Vendor-side identity / employment verifications
        "OFAC_", "SSN_VALIDATION",
    ],
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

        Looks in MISMO_TO_INTERNAL first, then MISMO_ALIASES (synonyms /
        typos / vendor variants that share a canonical internal type).
        Returns ``None`` if the label is unknown — caller can fall back to
        :func:`detect_type_from_content`.
        """
        if source_system == "encompass":
            return ENCOMPASS_TO_INTERNAL.get(external_type)
        return (
            MISMO_TO_INTERNAL.get(external_type)
            or MISMO_ALIASES.get(external_type)
        )

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
    def detect_type_from_filename(
        filename: str, category: Optional[str] = None
    ) -> Optional[str]:
        """Heuristic detection from a file's name + category.

        Used by the incremental indexer when scanning S3: the path layout
        is ``loans/{los_id}/{category}/{filename}`` so the category is
        already known. We anchor on the filename's tokens and disambiguate
        with the category when needed (e.g. ``statement.pdf`` could be
        income or asset; the parent path tells us).
        """
        if not filename:
            return None
        name = filename.lower()

        rules: list[tuple[str, list[str]]] = [
            ("W2_CURRENT",            ["w2_current", "w-2_current", "w2.current", "w2-current", "w2_2024", "w2_2025"]),
            ("W2_PRIOR",              ["w2_prior", "w-2_prior", "w2.prior", "w2_2023", "w2_2022"]),
            ("W2_CURRENT",            ["w2", "w-2"]),
            ("PAYSTUB_CURRENT",       ["paystub_current", "pay_stub_current", "paystub.current"]),
            ("PAYSTUB_CURRENT",       ["paystub", "pay_stub", "pay-stub"]),
            ("CREDIT_REPORT",         ["credit_report", "credit-report", "credit.report", "tri_merge", "tri-merge"]),
            ("BANK_STATEMENT_M1",     ["bank_statement", "bank-statement", "checking_statement", "savings_statement"]),
            ("TAX_RETURN_1040_CURRENT", ["1040", "tax_return"]),
            ("APPRAISAL_URAR",        ["appraisal_urar", "urar", "uniform_residential_appraisal", "appraisal"]),
            ("HOI_BINDER",            ["hoi_binder", "hoi-binder", "homeowner", "hazard_insurance", "hazard-insurance"]),
            ("FLOOD_CERT",            ["flood_cert", "flood-cert", "flood_determination", "fema_flood"]),
            ("PROPERTY_TAX_BILL",     ["property_tax", "tax_bill", "tax-bill", "county_tax"]),
            ("TITLE_COMMITMENT",      ["title_commitment", "title-commitment", "title_binder"]),
            ("HOA_CERT",              ["hoa_cert", "hoa-cert", "hoa_certification"]),
            ("CONDO_QUESTIONNAIRE",   ["condo_questionnaire", "condo-questionnaire"]),
            ("IDENTITY_DL",           ["drivers_license", "driver_license", "drivers-license"]),
            ("SSA_AWARD_LETTER",      ["ssa_award", "social_security_award", "ssa-award"]),
            ("LES",                   ["les", "leave_earnings"]),
            ("AUS_DU_FINDINGS",       ["du_findings", "du-findings", "desktop_underwriter"]),
            ("AUS_LP_FINDINGS",       ["lp_findings", "lp-findings", "loan_prospector", "lpa_findings"]),
        ]
        for internal_type, signals in rules:
            if any(s in name for s in signals):
                return internal_type

        # Category-only fallbacks when the filename doesn't match any rule
        if category:
            cat = category.lower()
            if cat == "income":
                return "PAYSTUB_CURRENT"
            if cat == "asset":
                return "BANK_STATEMENT_M1"
            if cat == "credit":
                return "CREDIT_REPORT"
            if cat == "property":
                return "APPRAISAL_URAR"
            if cat == "identity":
                return "IDENTITY_DL"
        return None

    @staticmethod
    def get_document_category(internal_type: str) -> str:
        """Map an internal doc-type to ``document_category`` for ``document_index``.

        Falls back to ``loan_terms`` if no prefix matches.
        """
        for category, prefixes in _CATEGORY_MAP.items():
            if any(internal_type.startswith(p) for p in prefixes):
                return category
        return "loan_terms"
