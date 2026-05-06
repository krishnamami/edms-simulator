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
    # Property layer
    appraisal_complete:   bool = False
    title_clear:          bool = False
    insurance_bound:      bool = False
    flood_cert_received:  bool = False
    # Application layer
    dti_calculable:       bool = False
    ltv_calculable:       bool = False
    aus_ready:            bool = False
    # What's still missing
    missing_items:        list[str] = []


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
    assembled_at:                str = ""
    requires_review:             bool = False
