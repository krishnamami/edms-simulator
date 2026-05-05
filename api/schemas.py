"""Request/response schemas for the EDMS Simulator API."""
from typing import Optional

from pydantic import BaseModel, Field


class BorrowerSchema(BaseModel):
    first_name: str
    last_name: str
    dob: str
    ssn_hash: Optional[str] = None
    ssn_last4: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None


class LoanSchema(BaseModel):
    loan_amount: Optional[float] = None
    purpose: Optional[str] = None
    credit_band: Optional[str] = "near-prime"


class DocumentSchema(BaseModel):
    document_id: str
    document_type: str
    document_category: str = "income"
    borrower_role: str = "primary"
    box1_wages: Optional[float] = None
    employer_name: Optional[str] = None
    monthly_benefit: Optional[float] = None
    is_non_taxable: Optional[bool] = None
    base_pay_monthly: Optional[float] = None
    bah_monthly: Optional[float] = None
    bas_monthly: Optional[float] = None
    special_pay_monthly: Optional[float] = None
    net_income_after_addbacks: Optional[float] = None
    has_schedule_c: Optional[bool] = None
    gross_rent_annual: Optional[float] = None
    expenses_annual: Optional[float] = None
    account_type: Optional[str] = None
    balance: Optional[float] = None


class CreateLoanRequest(BaseModel):
    los_id: str = Field(..., description="Loan Origination System identifier")
    borrower: BorrowerSchema
    co_borrower: Optional[BorrowerSchema] = None
    loan: LoanSchema = LoanSchema()
    documents: list[DocumentSchema] = []


class CreateLoanResponse(BaseModel):
    application_id: str
    applicant_id: str
    co_applicant_id: Optional[str] = None
    status: str
    match_method: str
    is_new_record: bool


class ApplicantIdResponse(BaseModel):
    applicant_id: str
    application_id: str
    co_applicant_id: Optional[str] = None
    cached: bool


class IncomeProfileResponse(BaseModel):
    applicant_id: str
    profile: dict
    cached: bool


class CreditProfileResponse(BaseModel):
    applicant_id: str
    profile: dict
    cached: bool


class DocumentUploadRequest(BaseModel):
    applicant_id: str
    application_id: str
    all_documents: list[DocumentSchema] = []


class HealthResponse(BaseModel):
    status: str = "ok"
    version: str = "0.1.0"


class ReadyResponse(BaseModel):
    status: str
    postgres: bool
    redis: bool
