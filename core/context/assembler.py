"""ContextAssembler — folds borrower, property, and vendor layers into one
ApplicationContext + caches it in Redis.

Every layer-changing event (income re-assembly, property doc, etc.)
invalidates the ``context:{application_id}`` key. The next read at
``GET /application/{id}/context`` lazily re-runs this assembler.
"""
import logging
from datetime import datetime
from typing import Optional

from core.context.models import (
    ApplicationContext,
    BorrowerSnapshot,
    PropertySnapshot,
    ReadinessFlags,
)

logger = logging.getLogger(__name__)


class ContextAssembler:
    def __init__(self, postgres_store, redis_store):
        self.pg = postgres_store
        self.redis = redis_store

    async def assemble(self, application_id: str) -> ApplicationContext:
        """Read every layer for an application and return a fresh
        ``ApplicationContext``. Caches the result under
        ``context:{application_id}`` in Redis."""
        app = await self.pg.get_application(application_id)
        if not app:
            raise ValueError(f"No application: {application_id}")

        applicant_id    = app["applicant_id"]
        co_applicant_id = app.get("co_applicant_id")
        property_id     = app.get("property_id")

        # IncomeProfile is stored once under the primary applicant_id but
        # carries both ``primary_borrower`` and ``co_borrower`` sections.
        # Fetch once, route by role.
        primary_income = self.redis.get_income_profile(applicant_id) \
            or await self.pg.get_income_profile(applicant_id)

        primary = await self._build_borrower_snapshot(
            applicant_id, "primary", primary_income
        )
        co_borrower = None
        if co_applicant_id:
            co_borrower = await self._build_borrower_snapshot(
                co_applicant_id, "co_borrower", primary_income
            )

        property_snapshot = None
        if property_id:
            property_snapshot = await self._build_property_snapshot(
                property_id, app
            )

        combined_monthly = primary.qualifying_monthly + (
            co_borrower.qualifying_monthly if co_borrower else 0
        )
        qualifying_score = min(
            primary.mid_score,
            co_borrower.mid_score if co_borrower else 9999,
        )
        total_obligations = primary.monthly_obligations + (
            co_borrower.monthly_obligations if co_borrower else 0
        )

        front_dti: Optional[float] = None
        back_dti: Optional[float] = None
        if property_snapshot and property_snapshot.piti_total and combined_monthly > 0:
            front_dti = round(
                property_snapshot.piti_total / combined_monthly * 100, 2
            )
            back_dti = round(
                (property_snapshot.piti_total + total_obligations)
                / combined_monthly * 100,
                2,
            )

        ltv: Optional[float] = None
        loan_amount = app.get("loan_amount")
        if (
            property_snapshot
            and property_snapshot.appraised_value
            and loan_amount
            and float(loan_amount) > 0
        ):
            ltv = round(
                float(loan_amount)
                / float(property_snapshot.appraised_value)
                * 100,
                2,
            )

        vendor_checks = await self._get_vendor_checks(application_id)

        readiness = self._calculate_readiness(
            primary, co_borrower, property_snapshot,
            vendor_checks, front_dti, ltv,
        )

        try:
            graph_summary = await self.pg.get_graph_summary(applicant_id)
        except Exception as exc:
            logger.warning("graph_summary_failed", extra={"error": str(exc)})
            graph_summary = {}

        requires_review = bool(
            readiness.missing_items
            or primary.income_requires_review
            or (co_borrower and co_borrower.income_requires_review)
            or (property_snapshot
                and property_snapshot.condition_rating in ("C5", "C6"))
            or (graph_summary or {}).get("conflict_count", 0) > 0
        )

        ctx = ApplicationContext(
            application_id=application_id,
            los_id=app.get("los_id", "") or "",
            loan_amount=float(loan_amount) if loan_amount else None,
            loan_type=app.get("loan_type"),
            loan_purpose=app.get("loan_purpose"),
            primary=primary,
            co_borrower=co_borrower,
            property=property_snapshot,
            combined_qualifying_monthly=round(combined_monthly, 2),
            qualifying_score_used=qualifying_score,
            total_monthly_obligations=round(total_obligations, 2),
            front_end_dti=front_dti,
            back_end_dti=back_dti,
            ltv=ltv,
            vendor_checks=vendor_checks,
            readiness=readiness,
            graph_summary=graph_summary or {},
            assembled_at=datetime.utcnow().isoformat(),
            requires_review=requires_review,
        )

        self.redis.set_application_context(application_id, ctx.model_dump())
        logger.info(
            "context_assembled",
            extra={
                "application_id":  application_id,
                "front_dti":       front_dti,
                "back_dti":        back_dti,
                "ltv":             ltv,
                "requires_review": requires_review,
            },
        )
        return ctx

    async def _build_borrower_snapshot(
        self,
        applicant_id: str,
        role: str,
        primary_income: Optional[dict] = None,
    ) -> BorrowerSnapshot:
        # Income profile lives once under the primary applicant_id with
        # both sections nested. For the co-borrower call, the caller
        # passes the primary's profile via ``primary_income``.
        income = primary_income
        if income is None:
            income = self.redis.get_income_profile(applicant_id)
            if not income:
                income = await self.pg.get_income_profile(applicant_id)

        # Credit profile is rowed per applicant_id, so look it up directly.
        credit = self.redis.get_credit_profile(applicant_id)
        if not credit:
            credit = await self.pg.get_credit_profile(applicant_id)

        gr = await self.pg.find_by_applicant_id(applicant_id)
        full_name = (gr or {}).get("full_name") or applicant_id

        qualifying_monthly = 0.0
        income_sources: list = []
        income_confidence = 0.0
        income_review = False

        if income:
            if role == "co_borrower":
                section = income.get("co_borrower") or {}
            else:
                section = income.get("primary_borrower") or {}
            qualifying_monthly = float(section.get("qualifying_monthly") or 0)
            income_sources    = section.get("sources", []) or []
            income_confidence = float(section.get("overall_confidence") or 0)
            income_review     = bool(income.get("requires_human_review", False))
        income_verified = income_confidence >= 0.90

        mid_score = 620
        credit_band = "subprime"
        obligations = 0.0
        derogatory = 0
        if credit:
            mid_score = int(credit.get("mid_score") or 620)
            credit_band = credit.get("credit_band", "subprime")
            obligations = float(credit.get("total_monthly_obligations") or 0)
            derogatory = int(credit.get("derogatory_marks") or 0)

        return BorrowerSnapshot(
            applicant_id=applicant_id,
            full_name=full_name,
            role=role,
            qualifying_monthly=qualifying_monthly,
            income_sources=income_sources,
            income_confidence=income_confidence,
            income_verified=income_verified,
            income_requires_review=income_review,
            mid_score=mid_score,
            credit_band=credit_band,
            monthly_obligations=obligations,
            derogatory_marks=derogatory,
            employment_verified=income_verified,
            assembled_at=datetime.utcnow().isoformat(),
        )

    async def _build_property_snapshot(
        self, property_id: str, app: dict
    ) -> Optional[PropertySnapshot]:
        cached = self.redis.get_property_profile(property_id)
        if not cached:
            cached = await self.pg.get_property_profile(property_id)
        if not cached:
            return None

        prop = await self.pg.get_property(property_id)
        address = ""
        if prop:
            address = (
                f"{prop.get('address_line1', '')}, "
                f"{prop.get('city', '')}, "
                f"{prop.get('state', '')}"
            )

        piti_total = None
        piti_components = cached.get("piti_components")
        if piti_components:
            piti_total = piti_components.get("total_piti")

        ltv = None
        loan_amount = app.get("loan_amount")
        if loan_amount and cached.get("appraised_value"):
            ltv = round(
                float(loan_amount) / float(cached["appraised_value"]) * 100, 2
            )

        return PropertySnapshot(
            property_id=property_id,
            address=address,
            property_type=(prop or {}).get("property_type", "") or "",
            appraised_value=cached.get("appraised_value"),
            appraisal_confidence=cached.get("appraisal_confidence"),
            annual_taxes=cached.get("annual_taxes"),
            hoi_monthly=cached.get("hoi_monthly"),
            flood_zone=cached.get("flood_zone"),
            flood_insurance_required=bool(
                cached.get("flood_insurance_required", False)
            ),
            hoa_monthly=float(cached.get("hoa_monthly") or 0),
            condition_rating=cached.get("condition_rating"),
            piti_total=piti_total,
            piti_components=piti_components,
            ltv=ltv,
            assembled_at=datetime.utcnow().isoformat(),
        )

    async def _get_vendor_checks(self, application_id: str) -> dict:
        """Read vendor returns from ``document_index``.

        Phase D adapters land their parsed payload as a row in
        ``document_index`` keyed by document_type. We re-hydrate the most
        recent row of each type and surface a flat summary that
        Decision OS can consume directly off ``ApplicationContext``.
        """
        from core.ingestion.adapters.vendor_aus_adapter import VendorAUSAdapter
        from core.ingestion.adapters.vendor_fraud_adapter import VendorFraudAdapter

        vendor_doc_types = {
            "AUS_DU_FINDINGS",
            "AUS_LP_FINDINGS",
            "FRAUD_REPORT",
            "SSN_VALIDATION",
            "OFAC_REPORT",
            "EMPLOYMENT_VERIFICATION",
            "FLOOD_CERT",
        }

        try:
            docs = await self.pg.get_documents_for_application(application_id)
        except Exception as exc:
            logger.warning("vendor_docs_fetch_failed", extra={"error": str(exc)})
            docs = []

        # Most-recent doc of each type wins (the list is ORDER BY received_at DESC).
        vendor_docs: dict[str, dict] = {}
        for d in docs:
            dt = d.get("document_type")
            if dt in vendor_doc_types and dt not in vendor_docs:
                vendor_docs[dt] = d

        def _fields(doc_type: str):
            doc = vendor_docs.get(doc_type)
            if not doc:
                return None
            fields = doc.get("extracted_fields") or {}
            if isinstance(fields, str):
                import json as _json
                try:
                    fields = _json.loads(fields)
                except Exception:
                    fields = {}
            return fields

        aus_du = _fields("AUS_DU_FINDINGS")
        aus_lp = _fields("AUS_LP_FINDINGS")
        fraud  = _fields("FRAUD_REPORT")
        ssn    = _fields("SSN_VALIDATION")
        ofac   = _fields("OFAC_REPORT")
        voe    = _fields("EMPLOYMENT_VERIFICATION")
        flood  = _fields("FLOOD_CERT")

        aus_summary = None
        if aus_du or aus_lp:
            aus_payload = aus_du or aus_lp
            aus_summary = {
                "type":           "DU" if aus_du else "LP",
                "recommendation": (aus_payload or {}).get("recommendation"),
                "approved":       VendorAUSAdapter.is_approved(aus_payload or {}),
                "casefile_id":    (aus_payload or {}).get("casefile_id"),
            }

        return {
            "aus_findings":          aus_summary,
            "fraud_score":           (fraud or {}).get("fraud_score"),
            "fraud_band":            (fraud or {}).get("risk_band"),
            "fraud_requires_review": (
                VendorFraudAdapter.requires_review(fraud) if fraud else None
            ),
            "flood_determination":   flood,
            "employment_verified":   (voe or {}).get("employment_verified") if voe else None,
            "employer_verified":     (voe or {}).get("employer_name") if voe else None,
            "ssn_valid":             (ssn or {}).get("ssn_valid") if ssn else None,
            "ofac_clear":            (ofac or {}).get("ofac_clear") if ofac else None,
        }

    def _calculate_readiness(
        self,
        primary: BorrowerSnapshot,
        co_borrower: Optional[BorrowerSnapshot],
        property_snapshot: Optional[PropertySnapshot],
        vendor_checks: dict,
        front_dti: Optional[float],
        ltv: Optional[float],
    ) -> ReadinessFlags:
        missing: list[str] = []
        flags = ReadinessFlags()

        flags.income_verified = primary.income_verified
        flags.credit_pulled = primary.mid_score > 300
        # Vendor-driven flags override the borrower-snapshot defaults when a
        # real check has landed; absent a vendor return we fall back to the
        # snapshot value (which is False until a real source verifies it).
        ssn_valid = vendor_checks.get("ssn_valid")
        emp_verified = vendor_checks.get("employment_verified")
        flags.identity_verified = (
            bool(ssn_valid) if ssn_valid is not None
            else primary.identity_verified
        )
        flags.employment_verified = (
            bool(emp_verified) if emp_verified is not None
            else primary.employment_verified
        )

        if not flags.income_verified:
            missing.append("income_verification")
        if not flags.credit_pulled:
            missing.append("credit_report")
        if not flags.identity_verified:
            missing.append("identity_document")

        if property_snapshot:
            flags.appraisal_complete  = property_snapshot.appraised_value is not None
            flags.insurance_bound     = property_snapshot.hoi_monthly is not None
            flags.flood_cert_received = property_snapshot.flood_zone is not None
            if not flags.appraisal_complete:
                missing.append("appraisal")
            if not flags.insurance_bound:
                missing.append("hoi_binder")
            if not flags.flood_cert_received:
                missing.append("flood_certificate")
        else:
            missing.extend([
                "appraisal", "hoi_binder", "flood_certificate",
                "property_tax_bill",
            ])

        flags.dti_calculable = front_dti is not None
        flags.ltv_calculable = ltv is not None
        aus = vendor_checks.get("aus_findings") or {}
        flags.aus_ready = (
            flags.income_verified
            and flags.credit_pulled
            and flags.appraisal_complete
            and flags.insurance_bound
            and aus.get("approved", False)
        )

        if not flags.dti_calculable:
            missing.append("piti_calculation")
        if not flags.ltv_calculable:
            missing.append("appraised_value")

        # Vendor-driven gating items
        ofac_clear = vendor_checks.get("ofac_clear")
        if ofac_clear is False:
            missing.append("ofac_review_required")
        if vendor_checks.get("fraud_requires_review"):
            missing.append("fraud_review_required")

        flags.missing_items = missing
        return flags
