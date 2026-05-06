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
    PropertyDocumentUploadedEvent,
)
from core.property.extractors import (
    extract_appraisal_pdf,
    extract_flood_pdf,
    extract_hoi_pdf,
    extract_tax_pdf,
)
from core.property.sources import PROPERTY_CONFIDENCE
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
from core.ingestion.los_connector import get_connector
from core.ingestion.mismo import (
    ENCOMPASS_TO_INTERNAL,
    MISMO_TO_INTERNAL,
)
from core.ingestion.pipeline import IngestionPipeline
from core.ingestion.router import IngestRouter
from core.storage.raw_ingestion_store import RawIngestionStore

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


def _build_pipeline(request: Request) -> IngestionPipeline:
    """Per-request pipeline; reuses the app.state singletons for s3 +
    postgres, default-constructs RawIngestionStore (stateless)."""
    return IngestionPipeline(
        postgres_store=request.app.state.postgres_store,
        redis_store=request.app.state.redis_store,
        s3_client=request.app.state.s3_client,
        raw_store=getattr(request.app.state, "raw_store", None) or RawIngestionStore(),
    )


def _claude_or_anthropic_to_http(exc: Exception) -> HTTPException:
    if isinstance(exc, ClaudeUnavailable):
        return HTTPException(status_code=503, detail=str(exc))
    if _AnthropicAPIStatusError and isinstance(exc, _AnthropicAPIStatusError):
        return _claude_error_to_http(exc)
    return HTTPException(status_code=500, detail=str(exc))


@router.post("/ingest/pdf", dependencies=[Depends(verify_api_key)])
async def ingest_pdf(
    request: Request,
    file: UploadFile = File(...),
    applicant_id: Optional[str] = Form(None),
    borrower_role: str = Form("primary"),
):
    body = await file.read()
    pipeline = _build_pipeline(request)
    result = await pipeline.ingest(
        channel=ChannelType.PDF_UPLOAD,
        payload=body,
        applicant_id=applicant_id,
        filename=file.filename,
    )
    return {
        **result["event"].model_dump(),
        "ingest_id": result["ingest_id"],
        "raw_s3_key": result["raw_s3_key"],
    }


@router.post("/ingest/image", dependencies=[Depends(verify_api_key)])
async def ingest_image(
    request: Request,
    file: UploadFile = File(...),
    applicant_id: Optional[str] = Form(None),
    borrower_role: str = Form("primary"),
):
    body = await file.read()
    pipeline = _build_pipeline(request)
    try:
        result = await pipeline.ingest(
            channel=ChannelType.IMAGE_UPLOAD,
            payload=body,
            applicant_id=applicant_id,
            filename=file.filename,
        )
    except (ClaudeUnavailable, Exception) as exc:
        if isinstance(exc, ClaudeUnavailable) or (
            _AnthropicAPIStatusError and isinstance(exc, _AnthropicAPIStatusError)
        ):
            raise _claude_or_anthropic_to_http(exc)
        raise
    return {
        **result["event"].model_dump(),
        "ingest_id": result["ingest_id"],
        "raw_s3_key": result["raw_s3_key"],
    }


@router.post("/ingest/email", dependencies=[Depends(verify_api_key)])
async def ingest_email(request: Request, payload: dict):
    pipeline = _build_pipeline(request)
    try:
        result = await pipeline.ingest(
            channel=ChannelType.EMAIL,
            payload=payload,
        )
    except ClaudeUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        if _AnthropicAPIStatusError and isinstance(exc, _AnthropicAPIStatusError):
            raise _claude_error_to_http(exc)
        raise
    events = result["event"]
    attachments_count = max(0, len(events) - 1)
    return {
        "ingest_id": result["ingest_id"],
        "raw_s3_key": result["raw_s3_key"],
        "events": [e.model_dump() for e in events],
        "documents_processed": attachments_count,
    }


@router.post("/ingest/chat", dependencies=[Depends(verify_api_key)])
async def ingest_chat(request: Request, payload: dict):
    messages = payload.get("messages") or []
    applicant_id = payload.get("applicant_id")
    pipeline = _build_pipeline(request)
    try:
        result = await pipeline.ingest(
            channel=ChannelType.CHAT,
            payload=messages,
            applicant_id=applicant_id,
        )
    except ClaudeUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        if _AnthropicAPIStatusError and isinstance(exc, _AnthropicAPIStatusError):
            raise _claude_error_to_http(exc)
        raise
    event = result["event"]
    return {
        "extracted": event.extracted_fields,
        "missing_fields": event.missing_fields,
        "documents_needed": event.documents_needed,
        "overall_confidence": event.confidence,
        "applicant_id": applicant_id,
        "next_question_suggestion": _next_question_for(event.missing_fields),
        "event": event.model_dump(),
        "ingest_id": result["ingest_id"],
        "raw_s3_key": result["raw_s3_key"],
    }


@router.post("/ingest/form", dependencies=[Depends(verify_api_key)])
async def ingest_form(request: Request, payload: dict):
    pipeline = _build_pipeline(request)
    result = await pipeline.ingest(channel=ChannelType.FORM, payload=payload)
    return {
        **result["event"].model_dump(),
        "ingest_id": result["ingest_id"],
        "raw_s3_key": result["raw_s3_key"],
    }


@router.post("/ingest/csv", dependencies=[Depends(verify_api_key)])
async def ingest_csv(request: Request, file: UploadFile = File(...)):
    body = await file.read()
    pipeline = _build_pipeline(request)
    result = await pipeline.ingest(
        channel=ChannelType.CSV_BATCH,
        payload=body,
        filename=file.filename,
    )
    events, report = result["event"]
    return {
        "ingest_id": result["ingest_id"],
        "raw_s3_key": result["raw_s3_key"],
        **report,
        "applicants": [e.applicant_signals for e in events],
    }


@router.post("/ingest/xml", dependencies=[Depends(verify_api_key)])
async def ingest_xml(request: Request, file: UploadFile = File(...)):
    body = await file.read()
    pipeline = _build_pipeline(request)
    result = await pipeline.ingest(
        channel=ChannelType.XML,
        payload=body,
        filename=file.filename,
    )
    return {
        **result["event"].model_dump(),
        "ingest_id": result["ingest_id"],
        "raw_s3_key": result["raw_s3_key"],
    }


# ---------------------------------------------------------------------------
# Phase A: raw_ingestion observability
# ---------------------------------------------------------------------------


@router.get(
    "/applicant/{applicant_id}/raw-ingestion",
    dependencies=[Depends(verify_api_key)],
)
async def list_raw_ingestion(request: Request, applicant_id: str):
    raw_store = getattr(request.app.state, "raw_store", None) or RawIngestionStore()
    rows = await raw_store.get_for_applicant(applicant_id)
    state = await raw_store.get_pipeline_state(applicant_id)
    return {
        "applicant_id":   applicant_id,
        "pipeline_state": state,
        "ingestions":     rows,
    }


@router.get(
    "/ingest/{ingest_id}/raw",
    dependencies=[Depends(verify_api_key)],
)
async def get_raw_ingestion(request: Request, ingest_id: str):
    raw_store = getattr(request.app.state, "raw_store", None) or RawIngestionStore()
    row = await raw_store.get(ingest_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"no raw ingestion {ingest_id}")
    return row


@router.post(
    "/ingest/{ingest_id}/reprocess",
    dependencies=[Depends(verify_api_key)],
)
async def reprocess_raw_ingestion(request: Request, ingest_id: str):
    pipeline = _build_pipeline(request)
    try:
        result = await pipeline.reprocess(ingest_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except ClaudeUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc))
    except Exception as exc:
        if _AnthropicAPIStatusError and isinstance(exc, _AnthropicAPIStatusError):
            raise _claude_error_to_http(exc)
        raise
    event = result["event"]
    summary = (
        event.model_dump() if hasattr(event, "model_dump")
        else {"events_count": len(event) if isinstance(event, (list, tuple)) else 0}
    )
    return {
        "ingest_id":   result["ingest_id"],
        "status":      result["status"],
        "raw_s3_key":  result["raw_s3_key"],
        "result":      summary,
    }


@router.get(
    "/pipeline/failed",
    dependencies=[Depends(verify_api_key)],
)
async def list_failed_ingestions(request: Request, limit: int = 50):
    raw_store = getattr(request.app.state, "raw_store", None) or RawIngestionStore()
    rows = await raw_store.get_failed(limit=limit)
    return {"count": len(rows), "ingestions": rows}


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


# ---------------------------------------------------------------------------
# Phase 0: MISMO compatibility — universal LOS endpoints
# ---------------------------------------------------------------------------


@router.post("/ingest/los", dependencies=[Depends(verify_api_key)])
async def ingest_los(request: Request, body: dict):
    """Universal LOS document receiver.

    Body shape::

        {
          "source_system": "encompass" | "mismo_34" | ...,
          "payload": { ... whatever the LOS sends ... }
        }

    The connector translates the payload to the internal model. If the
    LOS loan number maps to an existing application, the document is
    persisted into ``document_index`` and reconciled against the
    applicant's other docs. Otherwise the translated event is returned
    with ``status=pending_loan_creation`` so the caller can submit the
    loan first via ``POST /loans/from-los``.
    """
    source_system = body.get("source_system") or ""
    payload = body.get("payload") or {}
    if not source_system or not payload:
        raise HTTPException(
            status_code=400,
            detail="body must include source_system and payload",
        )
    connector = get_connector(source_system)
    translated = connector.translate_document(payload)

    pg = request.app.state.postgres_store
    external_loan_id = translated.get("external_loan_id")
    application = (
        await pg.get_application_by_external_loan_id(external_loan_id)
        if external_loan_id else None
    )

    if not application:
        return {
            "status": "pending_loan_creation",
            "document_type_detected": translated["document_type"],
            "applicant_id": None,
            "external_loan_id": external_loan_id,
            "translated": translated,
        }

    import uuid as _uuid
    applicant_id = application["applicant_id"]
    document_id = (
        translated.get("external_doc_id")
        or f"DOC-{source_system}-{_uuid.uuid4().hex[:12]}"
    )
    doc = {
        "document_id":      document_id,
        "applicant_id":     applicant_id,
        "application_id":   application["application_id"],
        "document_type":    translated["document_type"],
        "document_category": translated["document_category"],
        "borrower_role":    "primary",
        "s3_key":           None,
        "status":           "received",
        "is_current":       True,
        "extracted_fields": translated["extracted_fields"],
        "confidence_score": translated["confidence_score"],
    }
    try:
        await pg.save_document(doc)
        new_rels = await DocumentReconciler(pg).reconcile(applicant_id, doc)
    except Exception as exc:
        logger.warning("ingest_los_persist_failed", extra={"error": str(exc)})
        return {
            "status": "translation_only",
            "document_type_detected": translated["document_type"],
            "applicant_id": applicant_id,
            "external_loan_id": external_loan_id,
            "translated": translated,
            "error": str(exc),
        }

    return {
        "status": "persisted",
        "ingest_id": document_id,
        "document_type_detected": translated["document_type"],
        "applicant_id": applicant_id,
        "application_id": application["application_id"],
        "external_loan_id": external_loan_id,
        "relationships_created": len(new_rels),
        "translated": translated,
    }


@router.post("/loans/from-los", dependencies=[Depends(verify_api_key)])
async def create_loan_from_los(request: Request, body: dict):
    """Create a loan from a LOS-shaped payload.

    Translates via the connector, then drives the existing
    APPLICATION_SUBMITTED pipeline. Stores the LOS's loan number on the
    new application row and merges any external IDs onto the applicant.
    """
    source_system = body.get("source_system") or ""
    payload = body.get("payload") or {}
    if not source_system or not payload:
        raise HTTPException(
            status_code=400,
            detail="body must include source_system and payload",
        )
    connector = get_connector(source_system)
    translated = connector.translate_loan(payload)

    inner_payload = {
        "los_id":      translated["los_id"],
        "borrower":    translated["borrower"],
        "co_borrower": translated.get("co_borrower"),
        "loan":        {
            "loan_amount": (translated["loan"] or {}).get("loan_amount"),
            "credit_band": (translated["loan"] or {}).get("credit_band", "near-prime"),
        },
        "documents":   [],
    }
    service = request.app.state.aggregation_service
    event = ApplicationSubmittedEvent(
        event_type=EventType.APPLICATION_SUBMITTED, payload=inner_payload
    )
    result = await service.handle(event)

    pg = request.app.state.postgres_store
    external_loan_id = translated.get("los_id")
    loan_terms = translated.get("loan") or {}
    try:
        await pg.update_application_loan_fields(
            application_id=result["application_id"],
            loan_data={
                "loan_amount":      loan_terms.get("loan_amount"),
                "interest_rate":    loan_terms.get("interest_rate"),
                "loan_term_months": loan_terms.get("loan_term_months"),
                "loan_purpose":     loan_terms.get("loan_purpose"),
                "loan_type":        loan_terms.get("loan_type"),
                "external_loan_id": external_loan_id,
                "urla_fields":      translated.get("urla_fields") or {},
            },
        )
        for sys_name, ext_id in (translated.get("external_ids") or {}).items():
            await pg.add_external_id(result["applicant_id"], sys_name, ext_id)
    except Exception as exc:
        logger.warning("loans_from_los_patch_failed", extra={"error": str(exc)})

    return {
        "applicant_id":     result["applicant_id"],
        "co_applicant_id":  result.get("co_applicant_id"),
        "application_id":   result["application_id"],
        "external_loan_id": external_loan_id,
        "match_method":     result["match_method"],
        "is_new_record":    result["is_new_record"],
        "source_system":    source_system,
    }


@router.get(
    "/resolve/external/{source_system}/{external_id}",
    dependencies=[Depends(verify_api_key)],
)
async def resolve_external(
    request: Request, source_system: str, external_id: str
):
    """Reverse-lookup: given a real LOS loan number / contact id, return
    the simulator's internal ids."""
    pg = request.app.state.postgres_store
    application = await pg.get_application_by_external_loan_id(external_id)
    if application:
        return {
            "applicant_id":     application["applicant_id"],
            "co_applicant_id":  application.get("co_applicant_id"),
            "application_id":   application["application_id"],
            "los_id":           application.get("los_id"),
            "external_loan_id": application.get("external_loan_id"),
            "matched_via":      "applications.external_loan_id",
        }
    applicant = await pg.find_by_external_id(source_system, external_id)
    if applicant:
        return {
            "applicant_id":   applicant["applicant_id"],
            "external_ids":   applicant.get("external_ids", {}),
            "matched_via":    "applicants.external_ids",
        }
    raise HTTPException(
        status_code=404,
        detail=f"no record found for {source_system}/{external_id}",
    )


# ---------------------------------------------------------------------------
# Phase B: property layer
# ---------------------------------------------------------------------------


_PROPERTY_EXTRACTORS = {
    "APPRAISAL_URAR":    extract_appraisal_pdf,
    "APPRAISAL_UPDATE":  extract_appraisal_pdf,
    "APPRAISAL_DESK":    extract_appraisal_pdf,
    "APPRAISAL_FIELD":   extract_appraisal_pdf,
    "HOI_BINDER":        extract_hoi_pdf,
    "HOI_DECLARATIONS":  extract_hoi_pdf,
    "FLOOD_CERT":        extract_flood_pdf,
    "PROPERTY_TAX_BILL": extract_tax_pdf,
}

_REQUIRED_PROPERTY_DOC_TYPES = [
    "APPRAISAL_URAR",
    "TITLE_COMMITMENT",
    "HOI_BINDER",
    "FLOOD_CERT",
    "PROPERTY_TAX_BILL",
]


@router.post("/properties", dependencies=[Depends(verify_api_key)])
async def create_property(request: Request, body: dict):
    """Create a property record for an application.

    Body shape::

        {
          "application_id": "APP-LOS-001",
          "address": {
            "line1": "123 Main St", "line2": null, "city": "...", "state": "CA",
            "zip_code": "94105"
          },
          "property_type": "single_family",
          "units": 1,
          "year_built": 2005,
          "sqft": 2400
        }
    """
    application_id = body.get("application_id")
    address = body.get("address") or {}
    property_type = body.get("property_type", "single_family")
    if not application_id:
        raise HTTPException(status_code=400, detail="application_id required")
    if not address.get("line1") or not address.get("city") \
            or not address.get("state") or not address.get("zip_code"):
        raise HTTPException(
            status_code=400,
            detail="address.line1, address.city, address.state, address.zip_code required",
        )

    import uuid as _uuid
    pg = request.app.state.postgres_store
    property_id = body.get("property_id") or f"PROP-{_uuid.uuid4().hex[:12]}"
    prop = {
        "property_id":    property_id,
        "application_id": application_id,
        "address_line1":  address["line1"],
        "address_line2":  address.get("line2"),
        "city":           address["city"],
        "state":          address["state"],
        "zip_code":       address["zip_code"],
        "property_type":  property_type,
        "units":          int(body.get("units", 1)),
        "year_built":     body.get("year_built"),
        "sqft":           body.get("sqft"),
        "status":         "pending",
    }
    await pg.save_property(prop)
    try:
        await pg.update_application_property(application_id, property_id)
    except Exception as exc:
        logger.warning("update_application_property_failed", extra={"error": str(exc)})
    return {"property_id": property_id, "status": "pending"}


@router.get(
    "/property/{property_id}/profile",
    dependencies=[Depends(verify_api_key)],
)
async def get_property_profile(request: Request, property_id: str):
    redis = request.app.state.redis_store
    pg = request.app.state.postgres_store

    cached = redis.get_property_profile(property_id)
    if cached:
        return {"source": "cache", "data": cached}

    profile = await pg.get_property_profile(property_id)
    if not profile:
        raise HTTPException(status_code=404, detail="Property profile not found")
    redis.set_property_profile(property_id, profile)
    return {"source": "database", "data": profile}


@router.get(
    "/property/{property_id}/pipeline-state",
    dependencies=[Depends(verify_api_key)],
)
async def get_property_pipeline_state(request: Request, property_id: str):
    pg = request.app.state.postgres_store
    prop = await pg.get_property(property_id)
    if not prop:
        raise HTTPException(status_code=404, detail="Property not found")

    docs = await pg.get_property_docs(property_id)
    received_types = sorted({d.get("document_type") for d in docs if d.get("document_type")})
    missing = [t for t in _REQUIRED_PROPERTY_DOC_TYPES if t not in received_types]

    profile = await pg.get_property_profile(property_id)
    piti_ready = bool(profile and profile.get("piti_components"))
    ltv_ready = bool(profile and profile.get("appraised_value"))

    return {
        "property_id":        property_id,
        "documents_received": received_types,
        "documents_missing":  missing,
        "piti_ready":         piti_ready,
        "ltv_ready":          ltv_ready,
        "profile":            profile,
    }


@router.post("/ingest/property-doc", dependencies=[Depends(verify_api_key)])
async def ingest_property_doc(
    request: Request,
    file: UploadFile = File(...),
    property_id: str = Form(...),
    document_type: str = Form(...),
):
    """Upload a property PDF, extract, persist, and trigger reassembly."""
    body = await file.read()

    pg = request.app.state.postgres_store
    s3 = request.app.state.s3_client
    prop = await pg.get_property(property_id)
    if not prop:
        raise HTTPException(status_code=404, detail=f"property {property_id} not found")
    application_id = prop.get("application_id") or ""
    applicant_id = None
    if application_id:
        try:
            app = await pg.get_application_by_los_id(application_id.replace("APP-", "")) \
                if application_id.startswith("APP-") else None
        except Exception:
            app = None
        # Also try direct lookup since application_id == APP-{los_id}
        if not app:
            try:
                app = await pg.get_application_by_los_id(application_id)
            except Exception:
                app = None
        if app:
            applicant_id = app.get("applicant_id")

    extractor = _PROPERTY_EXTRACTORS.get(document_type)
    if extractor is None:
        extracted_fields, confidence = ({}, PROPERTY_CONFIDENCE.get(document_type, 0.7))
    else:
        try:
            extracted_fields, conf_from_extractor = extractor(body)
        except Exception as exc:
            logger.warning("property_extract_failed", extra={"error": str(exc)})
            extracted_fields, conf_from_extractor = ({}, 0.0)
        # Use the document-type's catalog confidence as the floor — the
        # extractor confidence is the recovery rate, not the source weight.
        confidence = max(
            conf_from_extractor,
            PROPERTY_CONFIDENCE.get(document_type, 0.7),
        )

    import uuid as _uuid
    document_id = f"PROPDOC-{_uuid.uuid4().hex[:12]}"
    s3_key = s3.upload_document(
        application_id=application_id or "no-app",
        category="property",
        document_id=document_id,
        content=body,
        extension="pdf",
        content_type="application/pdf",
    )

    if applicant_id:
        try:
            await pg.save_document({
                "document_id":      document_id,
                "applicant_id":     applicant_id,
                "application_id":   application_id,
                "document_type":    document_type,
                "document_category": "property",
                "borrower_role":    "primary",
                "s3_key":           s3_key,
                "status":           "received",
                "is_current":       True,
                "extracted_fields": extracted_fields,
                "confidence_score": confidence,
            })
        except Exception as exc:
            logger.warning("property_save_document_failed", extra={"error": str(exc)})

    service = request.app.state.aggregation_service
    new_doc_payload = {
        "document_id":      document_id,
        "document_type":    document_type,
        "document_category": "property",
        "extracted_fields": extracted_fields,
        "confidence_score": confidence,
    }
    docs = await pg.get_property_docs(property_id) or []
    if not any(d.get("document_id") == document_id for d in docs):
        docs.append(new_doc_payload)

    event = PropertyDocumentUploadedEvent(
        payload={
            "property_id":    property_id,
            "application_id": application_id,
            "property_docs":  docs,
        }
    )
    result = await service.handle(event)
    return {
        "property_id":    property_id,
        "document_id":    document_id,
        "document_type":  document_type,
        "s3_key":         s3_key,
        "confidence":     confidence,
        "extracted":      extracted_fields,
        "piti_total":     result.get("piti_total"),
        "profile":        result.get("profile"),
    }


@router.get("/mismo/doc-types", dependencies=[Depends(verify_api_key)])
async def mismo_doc_types():
    """Return the supported MISMO 3.4 + Encompass type mappings.

    Useful for an LOS integration team to discover what types we
    recognise without running test traffic.
    """
    return {
        "mismo_34": MISMO_TO_INTERNAL,
        "encompass": ENCOMPASS_TO_INTERNAL,
        "totals": {
            "mismo": len(MISMO_TO_INTERNAL),
            "encompass": len(ENCOMPASS_TO_INTERNAL),
        },
    }
