"""EDMS Simulator routes.

Auth: X-API-Key validated against the edms/api/keys secret.
Cache pattern: Redis -> Postgres.
"""
import base64
import os
from typing import Any, Optional

import structlog
from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, Request, UploadFile

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
from core.graph.navigator import DocumentNavigator
from core.graph.reconciler import DocumentReconciler
from core.ingestion._claude_client import ClaudeUnavailable
from core.ingestion.adapters import (
    chat_adapter,
    csv_adapter,
    email_adapter,
    form_adapter,
    image_adapter,
    pdf_adapter,
    xml_adapter,
)
from core.ingestion.events import ChannelType
from core.ingestion.router import IngestRouter

try:
    from anthropic import APIStatusError as _AnthropicAPIStatusError  # type: ignore
except Exception:  # SDK absent in some environments
    _AnthropicAPIStatusError = None  # type: ignore[assignment]


def _claude_error_to_http(exc: Exception) -> HTTPException:
    """Map an upstream Anthropic error to a 502 with a useful detail."""
    detail = getattr(exc, "message", None) or str(exc)
    return HTTPException(status_code=502, detail=f"Anthropic upstream error: {detail}")

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
            applicant_id=applicant_id, profile=cached, cached=True,
            source="cache", data=cached,
        )

    profile = await postgres_store.get_income_profile(applicant_id)
    if not profile:
        raise HTTPException(status_code=404, detail="Income profile not found")
    redis_store.set_income_profile(applicant_id, profile)
    return IncomeProfileResponse(
        applicant_id=applicant_id, profile=profile, cached=False,
        source="postgres", data=profile,
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
            applicant_id=applicant_id, profile=cached, cached=True,
            source="cache", data=cached,
        )

    profile = await postgres_store.get_credit_profile(applicant_id)
    if not profile:
        raise HTTPException(status_code=404, detail="Credit profile not found")
    redis_store.set_credit_profile(applicant_id, profile)
    return CreditProfileResponse(
        applicant_id=applicant_id, profile=profile, cached=False,
        source="postgres", data=profile,
    )


async def _upload_documents_impl(request: Request, body: DocumentUploadRequest):
    service = request.app.state.aggregation_service
    event = DocumentUploadedEvent(
        event_type=EventType.DOCUMENT_UPLOADED,
        payload=body.model_dump(),
    )
    return await service.handle(event)


@router.post(
    "/documents/upload",
    dependencies=[Depends(verify_api_key)],
)
async def upload_documents(request: Request, body: DocumentUploadRequest):
    return await _upload_documents_impl(request, body)


@router.post(
    "/loans/document",
    dependencies=[Depends(verify_api_key)],
)
async def upload_documents_loans_alias(request: Request, body: DocumentUploadRequest):
    return await _upload_documents_impl(request, body)


# ---------------------------------------------------------------------------
# Universal ingestion (Phase C: all adapters wired)
# ---------------------------------------------------------------------------


def _next_question_for(missing: list[str]) -> Optional[str]:
    if not missing:
        return None
    field = missing[0]
    pretty = field.replace("_", " ")
    return f"Could you share your {pretty}?"


@router.post("/ingest/pdf", dependencies=[Depends(verify_api_key)])
async def ingest_pdf(
    file: UploadFile = File(...),
    applicant_id: Optional[str] = Form(None),
    borrower_role: str = Form("primary"),
):
    body = await file.read()
    event = pdf_adapter.adapt(
        body, applicant_id=applicant_id, borrower_role=borrower_role,
    )
    return event.model_dump()


@router.post("/ingest/image", dependencies=[Depends(verify_api_key)])
async def ingest_image(
    file: UploadFile = File(...),
    applicant_id: Optional[str] = Form(None),
    borrower_role: str = Form("primary"),
):
    body = await file.read()
    try:
        event = image_adapter.adapt(
            body, applicant_id=applicant_id, borrower_role=borrower_role,
        )
    except ClaudeUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        if _AnthropicAPIStatusError and isinstance(exc, _AnthropicAPIStatusError):
            raise _claude_error_to_http(exc)
        raise
    return event.model_dump()


@router.post("/ingest/email", dependencies=[Depends(verify_api_key)])
async def ingest_email(payload: dict):
    try:
        events = email_adapter.adapt(payload)
    except ClaudeUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        if _AnthropicAPIStatusError and isinstance(exc, _AnthropicAPIStatusError):
            raise _claude_error_to_http(exc)
        raise
    attachments_count = max(0, len(events) - 1)  # one body event + N attachments
    return {
        "events": [e.model_dump() for e in events],
        "documents_processed": attachments_count,
    }


@router.post("/ingest/chat", dependencies=[Depends(verify_api_key)])
async def ingest_chat(payload: dict):
    messages = payload.get("messages") or []
    applicant_id = payload.get("applicant_id")
    try:
        event = chat_adapter.adapt(messages, applicant_id=applicant_id)
    except ClaudeUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        if _AnthropicAPIStatusError and isinstance(exc, _AnthropicAPIStatusError):
            raise _claude_error_to_http(exc)
        raise
    return {
        "extracted": event.extracted_fields,
        "missing_fields": event.missing_fields,
        "documents_needed": event.documents_needed,
        "overall_confidence": event.confidence,
        "applicant_id": applicant_id,
        "next_question_suggestion": _next_question_for(event.missing_fields),
        "event": event.model_dump(),
    }


@router.post("/ingest/form", dependencies=[Depends(verify_api_key)])
async def ingest_form(payload: dict):
    event = form_adapter.adapt(payload)
    return event.model_dump()


@router.post("/ingest/csv", dependencies=[Depends(verify_api_key)])
async def ingest_csv(file: UploadFile = File(...)):
    body = await file.read()
    events, report = csv_adapter.adapt(body)
    return {
        **report,
        "applicants": [e.applicant_signals for e in events],
    }


@router.post("/ingest/xml", dependencies=[Depends(verify_api_key)])
async def ingest_xml(file: UploadFile = File(...)):
    body = await file.read()
    event = xml_adapter.adapt(body)
    return event.model_dump()


# ---------------------------------------------------------------------------
# Document knowledge graph
# ---------------------------------------------------------------------------


@router.get(
    "/applicant/{applicant_id}/graph/summary",
    dependencies=[Depends(verify_api_key)],
)
async def get_graph_summary(request: Request, applicant_id: str):
    redis = request.app.state.redis_store
    cached = redis.get_graph_summary(applicant_id)
    if cached:
        return {"source": "cache", "data": cached}
    pg = request.app.state.postgres_store
    summary = await pg.get_graph_summary(applicant_id)
    redis.set_graph_summary(applicant_id, summary)
    return {"source": "database", "data": summary}


@router.get(
    "/applicant/{applicant_id}/graph",
    dependencies=[Depends(verify_api_key)],
)
async def get_knowledge_graph(request: Request, applicant_id: str):
    pg = request.app.state.postgres_store
    navigator = DocumentNavigator(pg)
    graph = await navigator.build_graph(applicant_id)
    return graph.model_dump()


@router.get(
    "/applicant/{applicant_id}/conflicts",
    dependencies=[Depends(verify_api_key)],
)
async def get_conflicts(request: Request, applicant_id: str):
    pg = request.app.state.postgres_store
    conflicts = await pg.get_conflicts_for_applicant(applicant_id)
    return {
        "applicant_id": applicant_id,
        "conflict_count": len(conflicts),
        "conflicts": conflicts,
    }


@router.post(
    "/applicant/{applicant_id}/navigate",
    dependencies=[Depends(verify_api_key)],
)
async def navigate(request: Request, applicant_id: str, body: dict):
    question = body.get("question", "")
    if not question:
        raise HTTPException(status_code=400, detail="question field required")
    pg = request.app.state.postgres_store
    redis = request.app.state.redis_store
    navigator = DocumentNavigator(pg, redis)
    answer = await navigator.answer(applicant_id, question)
    return answer.model_dump()


@router.post(
    "/applicant/{applicant_id}/reconcile",
    dependencies=[Depends(verify_api_key)],
)
async def reconcile_applicant(request: Request, applicant_id: str):
    pg = request.app.state.postgres_store
    docs = await pg.get_documents_for_applicant(applicant_id)
    reconciler = DocumentReconciler(pg)
    total_rels = 0
    total_conflicts = 0
    for doc in docs:
        rels = await reconciler.reconcile(applicant_id, doc)
        total_rels += len(rels)
        total_conflicts += sum(
            1 for r in rels if r.relationship_type.value == "contradicts"
        )
    return {
        "applicant_id": applicant_id,
        "relationships_created": total_rels,
        "conflicts_found": total_conflicts,
    }
