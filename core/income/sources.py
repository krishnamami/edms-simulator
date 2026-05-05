"""Income source / obligation / profile pydantic models."""
from enum import Enum
from typing import Optional

from pydantic import BaseModel


class IncomeSourceType(str, Enum):
    W2_SALARIED = "W2_SALARIED"
    W2_HOURLY = "W2_HOURLY"
    SELF_EMPLOYED = "SELF_EMPLOYED"
    CONTRACTOR_1099 = "CONTRACTOR_1099"
    RENTAL = "RENTAL"
    RETIREMENT_SSA = "RETIREMENT_SSA"
    RETIREMENT_PENSION = "RETIREMENT_PENSION"
    ASSET_DEPLETION = "ASSET_DEPLETION"
    MILITARY = "MILITARY"
    PART_TIME = "PART_TIME"
    BUSINESS_OWNER = "BUSINESS_OWNER"


class IncomeSource(BaseModel):
    source_type: IncomeSourceType
    borrower_id: str
    borrower_role: str
    employer_or_source_name: Optional[str] = None
    gross_monthly: float
    qualifying_monthly: float
    confidence: float
    continuance_months: int
    documents_used: list[str] = []
    calculation_method: str
    excluded: bool = False
    exclusion_reason: Optional[str] = None
    warnings: list[str] = []


class MonthlyDebtObligation(BaseModel):
    obligation_type: str
    creditor_name: Optional[str] = None
    monthly_payment: float
    outstanding_balance: Optional[float] = None
    months_remaining: Optional[int] = None
    omitted: bool = False
    omission_reason: Optional[str] = None


class IncomeProfile(BaseModel):
    applicant_id: str
    application_id: str
    assembled_at: str
    primary_borrower: dict
    co_borrower: Optional[dict] = None
    combined_qualifying_monthly: float
    qualifying_score_used: int
    monthly_debt_obligations: list[MonthlyDebtObligation] = []
    total_monthly_obligations: float
    dti_inputs_ready: bool
    assembly_warnings: list[str] = []
    requires_human_review: bool = False
    lineage_hash: str = ""
