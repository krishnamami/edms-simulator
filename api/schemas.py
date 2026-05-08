"""Request/response schemas for the EDMS Simulator API."""
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class BorrowerSchema(BaseModel):
    # Allow extra fields (e.g. address) so payloads aren't silently
    # truncated at the API boundary.
    model_config = ConfigDict(extra="allow")

    first_name: str
    last_name: str
    dob: str
    ssn_hash: Optional[str] = None
    ssn_last4: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None


class LoanSchema(BaseModel):
    model_config = ConfigDict(extra="allow")

    loan_amount: Optional[float] = None
    purpose: Optional[str] = None
    credit_band: Optional[str] = "near-prime"


class DocumentSchema(BaseModel):
    """A document carrier on /loans/document and inside /loans payloads.

    Two ways to ship extracted content:
      1. Nest the data under ``extracted_fields`` (recommended for new
         callers — clean, schema-agnostic, supports any doc type).
      2. Spread the fields at the top level (backward-compatible — the
         demo and existing income/asset adapters do this).

    The Pydantic config sets ``extra="allow"`` so non-income document
    types — CREDIT_REPORT, APPRAISAL_URAR, etc. — don't get silently
    truncated when callers spread credit/appraisal/property fields at
    the top level. Without this, mid_score / experian_score /
    appraised_value etc. were dropped at the API boundary before the
    persistence layer ever saw them, and downstream assemblers fell
    back to synthetic data.
    """
    model_config = ConfigDict(extra="allow")

    document_id: str
    document_type: str
    document_category: str = "income"
    borrower_role: str = "primary"
    extracted_fields: dict = {}
    # Income / asset / property fields kept as ``Optional[Any]`` so the
    # API boundary doesn't 422 on unparseable values like
    # ``box1_wages="one hundred ten thousand"``. The document still
    # lands in document_index with the raw value; the assemblers
    # downstream do best-effort coercion (``float(d.get(k) or 0)``)
    # and skip what they can't parse. The chaos test surfaced this —
    # rejecting at the API boundary loses the document entirely
    # (no row in document_index, no graph node, no completeness
    # credit), which is strictly worse than accepting the document
    # and letting downstream skip the bad field.
    box1_wages: Optional[Any] = None
    employer_name: Optional[Any] = None
    monthly_benefit: Optional[Any] = None
    is_non_taxable: Optional[Any] = None
    base_pay_monthly: Optional[Any] = None
    bah_monthly: Optional[Any] = None
    bas_monthly: Optional[Any] = None
    special_pay_monthly: Optional[Any] = None
    net_income_after_addbacks: Optional[Any] = None
    has_schedule_c: Optional[Any] = None
    gross_rent_annual: Optional[Any] = None
    expenses_annual: Optional[Any] = None
    account_type: Optional[Any] = None
    balance: Optional[Any] = None
    amount: Optional[Any] = None
    payer_name: Optional[Any] = None
    tax_year: Optional[Any] = None


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
    source: str = "cache"
    data: dict = {}


class CreditProfileResponse(BaseModel):
    applicant_id: str
    profile: dict
    cached: bool
    source: str = "cache"
    data: dict = {}


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
