"""Property layer pydantic models + source-confidence ranking.

The property side of a mortgage — collateral, taxes, insurance, flood —
arrives via several document types. This module enumerates them and
defines the shared profile + PITI shape the assembler emits.
"""
from enum import Enum
from typing import Optional

from pydantic import BaseModel


class PropertyDocType(str, Enum):
    APPRAISAL_URAR          = "APPRAISAL_URAR"
    APPRAISAL_UPDATE        = "APPRAISAL_UPDATE"
    APPRAISAL_DESK          = "APPRAISAL_DESK"
    APPRAISAL_FIELD         = "APPRAISAL_FIELD"
    AVM_REPORT              = "AVM_REPORT"
    TITLE_COMMITMENT        = "TITLE_COMMITMENT"
    TITLE_INSURANCE         = "TITLE_INSURANCE"
    HOI_BINDER              = "HOI_BINDER"
    HOI_DECLARATIONS        = "HOI_DECLARATIONS"
    FLOOD_CERT              = "FLOOD_CERT"
    PROPERTY_TAX_BILL       = "PROPERTY_TAX_BILL"
    PROPERTY_TAX_TRANSCRIPT = "PROPERTY_TAX_TRANSCRIPT"
    SURVEY                  = "SURVEY"
    PEST_INSPECTION         = "PEST_INSPECTION"
    HOA_CERT                = "HOA_CERT"
    CONDO_QUESTIONNAIRE     = "CONDO_QUESTIONNAIRE"
    PURCHASE_AGREEMENT      = "PURCHASE_AGREEMENT"


PROPERTY_CONFIDENCE: dict[str, float] = {
    "APPRAISAL_URAR":         0.97,
    "APPRAISAL_UPDATE":       0.93,
    "APPRAISAL_DESK":         0.88,
    "APPRAISAL_FIELD":        0.85,
    "AVM_REPORT":             0.75,
    "TITLE_COMMITMENT":       0.99,
    "HOI_BINDER":             0.95,
    "FLOOD_CERT":             0.99,
    "PROPERTY_TAX_BILL":      0.97,
    "PROPERTY_TAX_TRANSCRIPT": 0.99,
}


class PITIComponents(BaseModel):
    principal_interest: float
    taxes_monthly:      float
    insurance_monthly:  float
    hoa_monthly:        float = 0
    flood_monthly:      float = 0
    total_piti:         float


class PropertyProfile(BaseModel):
    property_id:              str
    application_id:           str
    appraised_value:          Optional[float] = None
    appraisal_date:           Optional[str] = None
    appraisal_type:           Optional[str] = None
    appraisal_confidence:     Optional[float] = None
    estimated_value:          Optional[float] = None
    tax_assessed_value:       Optional[float] = None
    annual_taxes:             Optional[float] = None
    monthly_taxes:            Optional[float] = None
    hoi_annual:               Optional[float] = None
    hoi_monthly:              Optional[float] = None
    flood_zone:               Optional[str] = None
    flood_insurance_required: bool = False
    flood_insurance_monthly:  Optional[float] = None
    hoa_monthly:              float = 0
    condition_rating:         Optional[str] = None
    piti_components:          Optional[PITIComponents] = None
    assembly_warnings:        list[str] = []
    requires_review:          bool = False
    lineage_hash:             str = ""
    assembled_at:             str = ""
