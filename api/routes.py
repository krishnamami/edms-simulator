"""EDMS Simulator routes.

Auth: X-API-Key validated against the edms/api/keys secret.
Cache pattern: Redis -> Postgres.
"""
import os
from typing import Optional

import structlog
from fastapi import APIRouter, Depends, Header, HTTPException, Request

from api.schemas import (
    ApplicantIdResponse,
    CreateLoanRequest,
    CreateLoanResponse,
    CreditProfileResponse,
    DocumentUploadRequest,
    IncomeProfileResponse,
)
from core.aggregation.events import (
    ApplicationSubmittedEvent,
    DocumentUploadedEvent,
    EventType,
)

logger = structlog.get_logger()
router = APIRouter()


def verify_api_key(x_api_key: Optional[str] = Header(default=None)):
    expected = os.getenv("API_KEY")
    if not expected:
        from core.storage.secrets import get_secrets
        keys = get_secrets().get_secret("edms/api/keys")
        expected = keys.get("decision_os_api_key")
    if not x_api_key or x_api_key != expected:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
    return x_api_key


@router.post(
    "/loans",
    response_model=CreateLoanResponse,
    dependencies=[Depends(verify_api_key)],
)
async def create_loan(request: Request, body: CreateLoanRequest):
    service = request.app.state.aggregation_service
    payload = body.model_dump()
    event = ApplicationSubmittedEvent(
        event_type=EventType.APPLICATION_SUBMITTED, payload=payload
    )
    result = await service.handle(event)
    return CreateLoanResponse(**result)


@router.get(
    "/loan/{los_id}/applicant-id",
    response_model=ApplicantIdResponse,
    dependencies=[Depends(verify_api_key)],
)
async def get_applicant_id(request: Request, los_id: str):
    redis_store = request.app.state.redis_store
    postgres_store = request.app.state.postgres_store

    cached = redis_store.get_app_lookup(los_id)
    if cached:
        return ApplicantIdResponse(
            applicant_id=cached["applicant_id"],
            application_id=cached["application_id"],
            co_applicant_id=cached.get("co_applicant_id"),
            cached=True,
        )

    app = await postgres_store.get_application_by_los_id(los_id)
    if not app:
        raise HTTPException(status_code=404, detail="Application not found")

    redis_store.set_app_lookup(
        los_id,
        {
            "application_id": app["application_id"],
            "applicant_id": app["applicant_id"],
            "co_applicant_id": app.get("co_applicant_id"),
        },
    )
    return ApplicantIdResponse(
        applicant_id=app["applicant_id"],
        application_id=app["application_id"],
        co_applicant_id=app.get("co_applicant_id"),
        cached=False,
    )


@router.get(
    "/applicant/{applicant_id}/income-profile",
    response_model=IncomeProfileResponse,
    dependencies=[Depends(verify_api_key)],
)
async def get_income_profile(request: Request, applicant_id: str):
    redis_store = request.app.state.redis_store
    postgres_store = request.app.state.postgres_store

    cached = redis_store.get_income_profile(applicant_id)
    if cached:
        return IncomeProfileResponse(
            applicant_id=applicant_id, profile=cached, cached=True
        )

    profile = await postgres_store.get_income_profile(applicant_id)
    if not profile:
        raise HTTPException(status_code=404, detail="Income profile not found")
    redis_store.set_income_profile(applicant_id, profile)
    return IncomeProfileResponse(
        applicant_id=applicant_id, profile=profile, cached=False
    )


@router.get(
    "/applicant/{applicant_id}/credit-profile",
    response_model=CreditProfileResponse,
    dependencies=[Depends(verify_api_key)],
)
async def get_credit_profile(request: Request, applicant_id: str):
    redis_store = request.app.state.redis_store
    postgres_store = request.app.state.postgres_store

    cached = redis_store.get_credit_profile(applicant_id)
    if cached:
        return CreditProfileResponse(
            applicant_id=applicant_id, profile=cached, cached=True
        )

    profile = await postgres_store.get_credit_profile(applicant_id)
    if not profile:
        raise HTTPException(status_code=404, detail="Credit profile not found")
    redis_store.set_credit_profile(applicant_id, profile)
    return CreditProfileResponse(
        applicant_id=applicant_id, profile=profile, cached=False
    )


@router.post(
    "/documents/upload",
    dependencies=[Depends(verify_api_key)],
)
async def upload_documents(
    request: Request, body: DocumentUploadRequest
):
    service = request.app.state.aggregation_service
    event = DocumentUploadedEvent(
        event_type=EventType.DOCUMENT_UPLOADED,
        payload=body.model_dump(),
    )
    result = await service.handle(event)
    return result
