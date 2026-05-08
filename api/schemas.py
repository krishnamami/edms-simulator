"""Request/response schemas for the EDMS Simulator API."""
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class BorrowerSchema(BaseModel):
    """Identity fields for a borrower or co-borrower on a `/loans` payload.
    Extra fields (address, etc.) flow through unchanged because of
    ``extra='allow'`` — they reach the persistence layer without API-side
    truncation."""
    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "example": {
                "first_name": "Alex",
                "last_name":  "Martinez",
                "dob":        "1985-06-20",
                "ssn_hash":   "<sha256-of-full-ssn>",
                "ssn_last4":  "4567",
                "email":      "alex.martinez@example.com",
                "phone":      "+1-512-555-0142",
            },
        },
    )

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
    """Body of `POST /loans` — creates an application + (re-)resolves
    primary and optional co-borrower applicants. Documents may be
    bundled at submission time but most callers stream them in later
    via `/documents/upload` or the `/ingest/*` channels."""
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "los_id": "LOS-12345",
                "borrower": {
                    "first_name": "Alex", "last_name": "Martinez",
                    "dob": "1985-06-20",
                    "ssn_hash": "<sha256-of-full-ssn>",
                    "ssn_last4": "4567",
                    "email": "alex.martinez@example.com",
                },
                "co_borrower": {
                    "first_name": "Pat", "last_name": "Martinez",
                    "dob": "1987-09-10",
                    "ssn_hash": "<sha256-of-full-ssn>",
                    "ssn_last4": "8901",
                },
                "loan": {
                    "loan_amount": 360000,
                    "interest_rate": 6.25,
                    "loan_term_months": 360,
                    "purpose": "purchase",
                },
                "documents": [],
            },
        },
    )

    los_id: str = Field(..., description="Loan Origination System identifier (unique per loan)")
    borrower: BorrowerSchema
    co_borrower: Optional[BorrowerSchema] = None
    loan: LoanSchema = LoanSchema()
    documents: list[DocumentSchema] = []


class CreateLoanResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "application_id":  "APP-LOS-12345",
                "applicant_id":    "APL-00316-P",
                "co_applicant_id": "APL-00317-C",
                "status":          "active",
                "match_method":    "deterministic_ssn",
                "is_new_record":   True,
            },
        },
    )

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
    """Assembled income profile. ``profile`` and ``data`` carry the same
    payload — the dual key is a backwards-compat shim from before the
    Decision-OS contract settled on ``data``."""
    applicant_id: str
    profile: dict
    cached: bool
    source: str = Field("cache", description="`cache` (Redis hit) or `postgres` (DB fallback)")
    data: dict = {}


class CreditProfileResponse(BaseModel):
    applicant_id: str
    profile: dict
    cached: bool
    source: str = Field("cache", description="`cache` (Redis hit) or `postgres` (DB fallback)")
    data: dict = {}


class DocumentUploadRequest(BaseModel):
    """Body of `POST /documents/upload` (and the `/loans/document`
    alias). One call carries one *or many* documents for a single
    applicant; mix doc_types freely and the indexer routes each by its
    canonical type. Co-borrower docs ride under the *primary's*
    applicant_id with ``borrower_role='co_borrower'`` so the joint
    application stays a single graph."""
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "applicant_id":   "APL-00316-P",
                "application_id": "APP-LOS-12345",
                "all_documents": [
                    {
                        "document_id":       "DOC-LOS-12345-W2_CURRENT-primary",
                        "document_type":     "W2_CURRENT",
                        "document_category": "income",
                        "borrower_role":     "primary",
                        "status":            "indexed",
                        "confidence_score":  0.94,
                        "extracted_fields": {
                            "box1_wages":    125000,
                            "tax_year":      2025,
                            "employer_name": "TechCorp Inc",
                            "ssn_last4":     "4567",
                        },
                    }
                ],
            },
        },
    )

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
