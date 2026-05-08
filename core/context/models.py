"""Pydantic models for the application-context layer.

ApplicationContext is the unified read shape Decision OS consumes via a
single ``GET /application/{id}/context`` call. It folds together the
borrower (income + credit + identity), property (collateral + PITI),
and application-level (loan terms + DTI/LTV + readiness) layers.
"""
from typing import Optional

from pydantic import BaseModel


class BorrowerSnapshot(BaseModel):
    applicant_id:           str
    full_name:              str
    role:                   str  # primary | co_borrower
    qualifying_monthly:     float
    income_sources:         list[dict] = []
    income_confidence:      float = 0.0
    income_verified:        bool = False
    income_requires_review: bool = False
    mid_score:              int = 620
    credit_band:            str = "subprime"
    monthly_obligations:    float = 0.0
    derogatory_marks:       int = 0
    employment_verified:    bool = False
    assets_total:           Optional[float] = None
    identity_verified:      bool = False
    assembled_at:           str = ""


class PropertySnapshot(BaseModel):
    property_id:              str
    address:                  str
    property_type:            str
    appraised_value:          Optional[float] = None
    appraisal_confidence:     Optional[float] = None
    annual_taxes:             Optional[float] = None
    hoi_monthly:              Optional[float] = None
    flood_zone:               Optional[str] = None
    flood_insurance_required: bool = False
    hoa_monthly:              float = 0
    condition_rating:         Optional[str] = None
    piti_total:               Optional[float] = None
    piti_components:          Optional[dict] = None
    ltv:                      Optional[float] = None
    assembled_at:             str = ""


class ReadinessFlags(BaseModel):
    # Borrower layer
    income_verified:      bool = False
    credit_pulled:        bool = False
    identity_verified:    bool = False
    employment_verified:  bool = False
    assets_verified:      bool = False
    # Tier-2: full identity tri-check (DL + SSN + OFAC) — stricter than
    # ``identity_verified`` (which fires on any one identity doc).
    identity_complete:    bool = False
    tax_docs_received:    bool = False
    # Property layer
    appraisal_complete:   bool = False
    title_clear:          bool = False
    title_received:       bool = False  # TITLE_COMMITMENT present
    insurance_bound:      bool = False
    flood_cert_received:  bool = False
    # Application layer
    dti_calculable:       bool = False
    ltv_calculable:       bool = False
    aus_ready:            bool = False
    # Tier-2: loan-terms layer
    loan_application_complete:  bool = False  # URLA_1003 present
    purchase_agreement_received: bool = False
    rate_locked:               bool = False  # RATE_LOCK + lock_expiry > today
    # Cross-doc fraud signal — flips False if any contradicts edge with
    # delta > the per-pair threshold exists in the graph.
    no_critical_conflicts: bool = True
    # What's still missing
    missing_items:        list[str] = []


class IncomeSlice(BaseModel):
    application_id:              str
    primary_qualifying_monthly:  float
    primary_income_sources:      list[dict] = []
    primary_income_confidence:   float = 0.0
    primary_income_verified:     bool = False
    co_borrower_qualifying:      Optional[float] = None
    combined_qualifying_monthly: float
    dti_calculable:              bool = False
    front_end_dti:               Optional[float] = None
    back_end_dti:                Optional[float] = None
    income_requires_review:      bool = False
    assembled_at:                str = ""


class CreditSlice(BaseModel):
    application_id:        str
    primary_mid_score:     int
    primary_credit_band:   str
    primary_obligations:   float
    co_borrower_mid_score: Optional[int] = None
    qualifying_score_used: int
    total_obligations:     float
    derogatory_flags:      bool = False
    assembled_at:          str = ""


class PropertySlice(BaseModel):
    application_id:     str
    appraised_value:    Optional[float] = None
    ltv:                Optional[float] = None
    piti_total:         Optional[float] = None
    piti_breakdown:     Optional[dict] = None
    flood_zone:         Optional[str] = None
    condition_rating:   Optional[str] = None
    appraisal_complete: bool = False
    requires_review:    bool = False
    assembled_at:       str = ""


class ComplianceSlice(BaseModel):
    application_id:     str
    readiness:          ReadinessFlags
    missing_items:      list[str] = []
    aus_recommendation: Optional[str] = None
    hmda_fields:        dict = {}
    requires_review:    bool = False
    assembled_at:       str = ""


class FraudSlice(BaseModel):
    application_id:      str
    fraud_score:         Optional[float] = None
    fraud_band:          Optional[str] = None
    ssn_valid:           Optional[bool] = None
    ofac_clear:          Optional[bool] = None
    employment_verified: Optional[bool] = None
    requires_review:     bool = False
    assembled_at:        str = ""


class BorrowerAggregation(BaseModel):
    """Tier-2 nested view of a single borrower's whole-file picture.

    Folds together the income / credit / asset / identity caches written
    through by the assembly pipeline, plus the running document_count.
    Coexists with ``ApplicationContext.primary`` and ``.co_borrower``
    (BorrowerSnapshot) — those stay for backwards compatibility; this
    new aggregation is the cleaner Decision-OS-facing shape.
    """
    applicant_id:        str
    income:              dict = {}
    credit:              dict = {}
    assets:              dict = {}
    identity:            dict = {}
    document_count:      int = 0
    qualifying_monthly:  float = 0.0


class ApplicationContext(BaseModel):
    application_id:              str
    los_id:                      str
    loan_amount:                 Optional[float] = None
    loan_type:                   Optional[str] = None
    loan_purpose:                Optional[str] = None
    primary:                     BorrowerSnapshot
    co_borrower:                 Optional[BorrowerSnapshot] = None
    property:                    Optional[PropertySnapshot] = None
    combined_qualifying_monthly: float
    qualifying_score_used:       int
    total_monthly_obligations:   float
    front_end_dti:               Optional[float] = None
    back_end_dti:                Optional[float] = None
    ltv:                         Optional[float] = None
    vendor_checks:               dict = {}
    readiness:                   ReadinessFlags
    graph_summary:               dict = {}
    # Tier-2 additions — coexist with the legacy primary / co_borrower
    # snapshots above. ``borrower`` is the nested aggregation,
    # ``loan_terms`` is the URLA / rate-lock / purchase view,
    # ``conflicts`` surfaces the top contradicts edges so a Decision OS
    # consumer can read fraud signals without a separate API call.
    borrower:                    Optional[BorrowerAggregation] = None
    co_borrower_aggregation:     Optional[BorrowerAggregation] = None
    loan_terms:                  dict = {}
    conflicts:                   dict = {"count": 0, "critical": []}
    assembled_at:                str = ""
    requires_review:             bool = False
