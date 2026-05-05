"""Full loan document package assembler.

Given an applicant (and optional co-borrower), loan data, and a scenario type,
this generates each document, uploads to S3 (or local_storage), optionally
indexes via PostgresStore.save_document, and returns a manifest.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Optional

from core.documents.generators.bank_stmt_generator import generate_bank_statement
from core.documents.generators.credit_report_generator import generate_credit_report
from core.documents.generators.identity_generator import generate_drivers_license
from core.documents.generators.paystub_generator import generate_paystub
from core.documents.generators.w2_generator import generate_w2


SCENARIO_DOC_SETS: dict[str, list[str]] = {
    "standard_w2": ["W2", "PAYSTUB", "BANK_STATEMENT", "CREDIT_REPORT", "DRIVERS_LICENSE"],
    "minimal":     ["W2", "DRIVERS_LICENSE"],
    "self_employed": ["BANK_STATEMENT", "CREDIT_REPORT", "DRIVERS_LICENSE"],
}


def _confidence_for(doc_type: str) -> float:
    # Generated docs are clean — confidence reflects what an extractor
    # can reliably recover. These mirror SOURCE_CONFIDENCE_RANKING.
    return {
        "W2": 0.95,
        "PAYSTUB": 0.93,
        "BANK_STATEMENT": 0.90,
        "CREDIT_REPORT": 0.92,
        "DRIVERS_LICENSE": 0.88,
    }.get(doc_type, 0.85)


def _generate_one(
    *,
    doc_type: str,
    applicant: dict,
    loan_data: dict,
    credit_profile: Optional[dict],
) -> tuple[bytes, dict, str, str]:
    """Returns (content_bytes, metadata, extension, content_type)."""
    full_name = f"{applicant['first_name']} {applicant['last_name']}"
    address = applicant.get("address", "1 Example St\nAnytown, CA 94000")
    dob_iso = applicant.get("dob", "1985-01-01")
    dob = date.fromisoformat(dob_iso)
    ssn_last4 = applicant.get("ssn_last4", "0000")

    if doc_type == "W2":
        annual = float(applicant.get("annual_income", 92000))
        pdf, meta = generate_w2(
            employee_name=full_name,
            employee_ssn_last4=ssn_last4,
            employee_address=address,
            employer_name=applicant.get("employer", "Example Employer LLC"),
            employer_ein=applicant.get("employer_ein", "12-3456789"),
            employer_address=applicant.get("employer_address", "1 Corporate Way\nSan Francisco, CA 94105"),
            tax_year=int(applicant.get("tax_year", date.today().year - 1)),
            box1_wages=annual,
        )
        return pdf, meta, "pdf", "application/pdf"

    if doc_type == "PAYSTUB":
        annual = float(applicant.get("annual_income", 92000))
        period_end = date.today().replace(day=15)
        period_start = period_end - timedelta(days=14)
        gross = round(annual / 26, 2)
        ytd = round(gross * (period_end.month * 2), 2)
        pdf, meta = generate_paystub(
            employer_name=applicant.get("employer", "Example Employer LLC"),
            employee_name=full_name,
            employee_ssn_last4=ssn_last4,
            pay_period_start=period_start,
            pay_period_end=period_end,
            pay_date=period_end + timedelta(days=3),
            gross_pay=gross,
            ytd_gross=ytd,
        )
        return pdf, meta, "pdf", "application/pdf"

    if doc_type == "BANK_STATEMENT":
        pdf, meta = generate_bank_statement(
            bank_name=applicant.get("bank_name", "Pacific First Bank"),
            account_holder=full_name,
            account_number=applicant.get("account_number", "1234567890"),
            statement_end_date=date.today().replace(day=1) - timedelta(days=1),
            starting_balance=float(applicant.get("starting_balance", 12_000.00)),
            seed=applicant.get("bank_seed", 42),
        )
        return pdf, meta, "pdf", "application/pdf"

    if doc_type == "CREDIT_REPORT":
        if credit_profile is None:
            raise ValueError("CREDIT_REPORT requires a credit_profile dict")
        pdf, meta = generate_credit_report(
            applicant_name=full_name,
            profile=credit_profile,
        )
        return pdf, meta, "pdf", "application/pdf"

    if doc_type == "DRIVERS_LICENSE":
        jpg, meta = generate_drivers_license(
            state=applicant.get("state", "CA"),
            full_name=full_name,
            dob=dob,
            address=address,
            dl_number=applicant.get("dl_number", "D1234567"),
            expiry=date.today() + timedelta(days=365 * 4),
        )
        return jpg, meta, "jpg", "image/jpeg"

    raise ValueError(f"Unknown doc_type: {doc_type}")


async def generate_package(
    *,
    application_id: str,
    primary: dict,
    co_borrower: Optional[dict] = None,
    loan_data: Optional[dict] = None,
    credit_profile: Optional[dict] = None,
    co_credit_profile: Optional[dict] = None,
    scenario_type: str = "standard_w2",
    s3_client: Any = None,
    postgres_store: Any = None,
) -> list[dict]:
    """Generate every document in the scenario, persist, return manifest.

    s3_client and postgres_store are optional so the function can be exercised
    in tests without those side effects (manifest is still produced).
    """
    loan_data = loan_data or {}
    doc_types = SCENARIO_DOC_SETS.get(scenario_type, SCENARIO_DOC_SETS["standard_w2"])
    manifest: list[dict] = []

    plans: list[tuple[dict, str, Optional[dict]]] = [(primary, "primary", credit_profile)]
    if co_borrower:
        plans.append((co_borrower, "co_borrower", co_credit_profile))

    for applicant, role, credit in plans:
        applicant_id = applicant.get("applicant_id", f"APL-UNK-{role}")
        for doc_type in doc_types:
            if doc_type == "CREDIT_REPORT" and credit is None:
                continue
            content, metadata, ext, mime = _generate_one(
                doc_type=doc_type,
                applicant=applicant,
                loan_data=loan_data,
                credit_profile=credit,
            )
            doc_id = f"DOC-{application_id}-{role}-{doc_type}"

            s3_key = None
            if s3_client is not None:
                s3_key = s3_client.upload_document(
                    application_id=application_id,
                    category=("identity" if doc_type == "DRIVERS_LICENSE" else "income"),
                    document_id=doc_id,
                    content=content,
                    extension=ext,
                    content_type=mime,
                )

            if postgres_store is not None:
                await postgres_store.save_document({
                    "document_id": doc_id,
                    "applicant_id": applicant_id,
                    "application_id": application_id,
                    "document_type": doc_type,
                    "document_category": "identity" if doc_type == "DRIVERS_LICENSE" else "income",
                    "borrower_role": role,
                    "s3_key": s3_key,
                    "status": "received",
                    "is_current": True,
                    "extracted_fields": metadata,
                    "confidence_score": _confidence_for(doc_type),
                })

            manifest.append({
                "document_id": doc_id,
                "document_type": doc_type,
                "borrower_role": role,
                "s3_key": s3_key,
                "metadata": metadata,
                "size_bytes": len(content),
                "confidence": _confidence_for(doc_type),
            })

    return manifest
