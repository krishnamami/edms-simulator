"""ContextAssembler — folds borrower, property, and vendor layers into one
ApplicationContext + caches it in Redis.

Every layer-changing event (income re-assembly, property doc, etc.)
invalidates the ``context:{application_id}`` key. The next read at
``GET /application/{id}/context`` lazily re-runs this assembler.
"""
import logging
from datetime import datetime
from typing import Optional

from core.tenancy import current_tenant_id
from core.context.models import (
    ApplicationContext,
    BorrowerAggregation,
    BorrowerSnapshot,
    PropertySnapshot,
    ReadinessFlags,
)
from core.context.webhook_publisher import WebhookPublisher

logger = logging.getLogger(__name__)


def _f(value) -> Optional[float]:
    """Best-effort float coercion. Returns None on None / unparseable."""
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class ContextAssembler:
    def __init__(
        self,
        postgres_store,
        redis_store,
        webhook_publisher: Optional[WebhookPublisher] = None,
    ):
        self.pg = postgres_store
        self.redis = redis_store
        self.webhook_publisher = webhook_publisher or WebhookPublisher(
            postgres_store
        )

    async def assemble(
        self,
        application_id: str,
        trigger_event: Optional[str] = None,
        trigger_doc_id: Optional[str] = None,
    ) -> ApplicationContext:
        """Read every layer for an application and return a fresh
        ``ApplicationContext``. Caches the result under
        ``context:{application_id}`` in Redis."""
        app = await self.pg.get_application(application_id, tenant_id=current_tenant_id())
        if not app:
            raise ValueError(f"No application: {application_id}")

        applicant_id    = app["applicant_id"]
        co_applicant_id = app.get("co_applicant_id")
        property_id     = app.get("property_id")

        # IncomeProfile is stored once under the primary applicant_id but
        # carries both ``primary_borrower`` and ``co_borrower`` sections.
        # Fetch once, route by role.
        primary_income = await self.redis.get_income_profile(applicant_id, tenant_id=current_tenant_id()) \
            or await self.pg.get_income_profile(applicant_id, tenant_id=current_tenant_id())

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

        # Build the loan-terms view early so LTV / PITI / DTI can use it.
        # _build_loan_terms folds the application row's loan_amount /
        # interest_rate / loan_purpose with the most-recent URLA,
        # RATE_LOCK, and PURCHASE_AGREEMENT extracted_fields.
        loan_terms = await self._build_loan_terms(applicant_id, app)

        # Effective loan amount: prefer the explicit URLA / app value,
        # then fall back to the rate-lock's loan_amount.
        loan_amount = (
            loan_terms.get("loan_amount")
            or (loan_terms.get("rate_lock") or {}).get("loan_amount")
        )
        loan_amount_f = _f(loan_amount)

        # Effective interest_rate / term — prefer rate-lock (the most
        # recent and authoritative source) over URLA-stated.
        rate_lock_d = loan_terms.get("rate_lock") or {}
        interest_rate = (
            rate_lock_d.get("locked_rate")
            or loan_terms.get("interest_rate")
            or app.get("interest_rate")
        )
        loan_term_months = (
            loan_terms.get("loan_term_months")
            or app.get("loan_term_months")
        )

        # Effective property value: appraised wins, fallback to
        # purchase_price (per LTV underwriting convention — the lender
        # uses the LOWER of the two when both are present, but if
        # appraisal is missing, purchase price is the proxy).
        appraised = (
            (property_snapshot.appraised_value if property_snapshot else None)
            or loan_terms.get("purchase_price")
        )
        appraised_f = _f(appraised)
        purchase_price_f = _f(loan_terms.get("purchase_price"))

        # LTV = loan_amount / min(appraised, purchase_price) × 100.
        # Use the lower of appraised vs purchase when both present so a
        # purchase loan never under-states LTV by appraising "high".
        ltv: Optional[float] = None
        if loan_amount_f and (appraised_f or purchase_price_f):
            denominator = appraised_f
            if purchase_price_f:
                denominator = min(filter(None, [appraised_f, purchase_price_f]))
            if denominator and denominator > 0:
                ltv = round(loan_amount_f / denominator * 100, 2)

        # PITI: use the property-assembler's value when present;
        # otherwise compute inline from loan terms + property tax/HOI/HOA.
        # The property assembler runs at /ingest/property-doc time and
        # may have had empty loan_data — recomputing here lets us
        # surface DTI even when the original assembly missed it.
        piti_total: Optional[float] = (
            property_snapshot.piti_total if property_snapshot else None
        )
        if piti_total is None:
            piti_total = self._compute_piti_inline(
                loan_amount=loan_amount_f,
                annual_rate_pct=_f(interest_rate),
                term_months=int(loan_term_months) if loan_term_months else None,
                annual_taxes=(
                    property_snapshot.annual_taxes if property_snapshot else None
                ),
                hoi_monthly=(
                    property_snapshot.hoi_monthly if property_snapshot else None
                ),
                hoa_monthly=(
                    property_snapshot.hoa_monthly if property_snapshot else 0
                ),
            )

        front_dti: Optional[float] = None
        back_dti: Optional[float] = None
        if piti_total and combined_monthly > 0:
            front_dti = round(piti_total / combined_monthly * 100, 2)
            back_dti = round(
                (piti_total + total_obligations) / combined_monthly * 100, 2,
            )

        vendor_checks = await self._get_vendor_checks(application_id)

        readiness = self._calculate_readiness(
            primary, co_borrower, property_snapshot,
            vendor_checks, front_dti, ltv,
        )

        try:
            graph_summary = await self.pg.get_graph_summary(applicant_id, tenant_id=current_tenant_id())
        except Exception as exc:
            logger.warning("graph_summary_failed", extra={"error": str(exc)})
            graph_summary = {}

        # Tier-2: pull the asset / identity entity summaries written
        # through by AggregationService._aggregate_and_cache_assets /
        # _aggregate_and_cache_identity. Use Redis as the primary cache
        # and fall through to recomputing from document_index if missing.
        primary_assets   = await self._read_asset_summary(applicant_id)
        primary_identity = await self._read_identity_summary(applicant_id)

        # ``loan_terms`` was already built above (we needed it early for
        # the LTV / DTI math). Reused here for the context payload.

        # Conflicts block — pulls the top contradicts edges from the
        # graph so a Decision OS reader doesn't need a separate
        # /applicant/{id}/conflicts call. Empty list when nothing
        # contradicts.
        conflicts = await self._build_conflicts(applicant_id)

        # Build the new nested BorrowerAggregation alongside the legacy
        # primary / co_borrower BorrowerSnapshot. Coexists for
        # backwards compatibility — readers that want the new shape use
        # ``borrower``; existing readers keep using ``primary``.
        borrower_agg = self._build_borrower_aggregation(
            applicant_id, primary, primary_income, primary_assets,
            primary_identity,
        )
        co_borrower_agg = None
        if co_applicant_id and co_borrower:
            co_assets   = await self._read_asset_summary(co_applicant_id)
            co_identity = await self._read_identity_summary(co_applicant_id)
            # Co-borrower's income lives on the SAME income profile dict
            # under the ``co_borrower`` section.
            co_borrower_agg = self._build_borrower_aggregation(
                co_applicant_id, co_borrower, primary_income, co_assets,
                co_identity, role="co_borrower",
            )

        requires_review = bool(
            readiness.missing_items
            or primary.income_requires_review
            or (co_borrower and co_borrower.income_requires_review)
            or (property_snapshot
                and property_snapshot.condition_rating in ("C5", "C6"))
            or (graph_summary or {}).get("conflict_count", 0) > 0
            or conflicts.get("count", 0) > 0
        )

        # Refresh readiness now that we have the new data sources to
        # power the Tier-2 flags (assets_verified, identity_complete,
        # title_received, tax_docs_received, etc.).
        readiness = self._calculate_readiness(
            primary, co_borrower, property_snapshot,
            vendor_checks, front_dti, ltv,
            assets=primary_assets,
            identity=primary_identity,
            loan_terms=loan_terms,
            conflicts=conflicts,
            applicant_docs=await self._fetch_doc_types(applicant_id),
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
            borrower=borrower_agg,
            co_borrower_aggregation=co_borrower_agg,
            loan_terms=loan_terms,
            conflicts=conflicts,
            assembled_at=datetime.utcnow().isoformat(),
            requires_review=requires_review,
        )

        await self.redis.set_application_context(application_id, ctx.model_dump(), tenant_id=current_tenant_id())

        # Phase E — snapshot every assembly into context_versions for audit.
        try:
            await self.pg.save_context_version({
                "application_id": application_id,
                "context_data":   ctx.model_dump(),
                "assembled_at":   ctx.assembled_at,
                "trigger_event":  trigger_event or "manual",
                "trigger_doc_id": trigger_doc_id,
            })
        except Exception as exc:
            logger.warning(
                "context_version_persist_failed", extra={"error": str(exc)}
            )

        # Fan out a context_updated event to any registered webhooks.
        try:
            await self.webhook_publisher.publish(
                event_type="context_updated",
                application_id=application_id,
                payload={
                    "application_id":  application_id,
                    "requires_review": ctx.requires_review,
                    "readiness":       ctx.readiness.model_dump(),
                    "assembled_at":    ctx.assembled_at,
                },
            )
        except Exception as exc:
            logger.warning(
                "context_webhook_publish_failed", extra={"error": str(exc)}
            )

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
            income = await self.redis.get_income_profile(applicant_id, tenant_id=current_tenant_id())
            if not income:
                income = await self.pg.get_income_profile(applicant_id, tenant_id=current_tenant_id())

        # Credit profile is rowed per applicant_id, so look it up directly.
        credit = await self.redis.get_credit_profile(applicant_id, tenant_id=current_tenant_id())
        if not credit:
            credit = await self.pg.get_credit_profile(applicant_id, tenant_id=current_tenant_id())

        gr = await self.pg.find_by_applicant_id(applicant_id, tenant_id=current_tenant_id())
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
        cached = await self.redis.get_property_profile(property_id, tenant_id=current_tenant_id())
        if not cached:
            cached = await self.pg.get_property_profile(property_id, tenant_id=current_tenant_id())
        if not cached:
            return None

        prop = await self.pg.get_property(property_id, tenant_id=current_tenant_id())
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
            docs = await self.pg.get_documents_for_application(application_id, tenant_id=current_tenant_id())
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
        # Tier-2: optional aggregations powering the new readiness flags.
        # Default to ``None`` / empty so old call sites (and tests that
        # call this helper directly) keep working.
        assets: Optional[dict] = None,
        identity: Optional[dict] = None,
        loan_terms: Optional[dict] = None,
        conflicts: Optional[dict] = None,
        applicant_docs: Optional[set] = None,
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

        # ── Tier-2 readiness flags ──────────────────────────────────────
        assets   = assets or {}
        identity = identity or {}
        loan_terms = loan_terms or {}
        conflicts  = conflicts or {}
        doc_types  = applicant_docs or set()

        # assets_verified: any liquid assets recorded AND at least 1
        # bank statement in the doc set.
        flags.assets_verified = (
            (assets.get("total_liquid_assets") or 0) > 0
            and any(t.startswith("BANK_STATEMENT_") for t in doc_types)
        )
        if not flags.assets_verified:
            missing.append("bank_statement")

        # identity_complete: full DL + SSN + OFAC tri-check (stricter
        # than identity_verified above, which fires on any single
        # identity doc).
        flags.identity_complete = bool(identity.get("identity_complete"))
        if not flags.identity_complete:
            missing.append("identity_complete")

        # title_received: TITLE_COMMITMENT or TITLE_INSURANCE in the
        # property layer.
        flags.title_received = bool(
            "TITLE_COMMITMENT" in doc_types or "TITLE_INSURANCE" in doc_types
        )
        if not flags.title_received:
            missing.append("title_commitment")

        # title_clear: TITLE_COMMITMENT received AND TITLE_INSURANCE
        # received. We can't determine which exceptions on a commitment
        # are blocking from the extracted_fields alone, but having an
        # insurance binder issued is the underwriter's signal that the
        # title is insurable — effectively clear for lending purposes.
        flags.title_clear = bool(
            "TITLE_COMMITMENT" in doc_types and "TITLE_INSURANCE" in doc_types
        )

        # tax_docs_received: W2 always; 1040 only if self-employed
        # (Schedule C / E / F or 1099 income present in the doc set).
        has_w2 = "W2_CURRENT" in doc_types or "W2_PRIOR" in doc_types
        is_self_employed = any(
            t in doc_types
            for t in ("SCHEDULE_C", "SCHEDULE_E", "SCHEDULE_F", "1099_NEC")
        )
        has_1040 = (
            "TAX_RETURN_1040_CURRENT" in doc_types
            or "TAX_RETURN_1040_PRIOR" in doc_types
        )
        flags.tax_docs_received = has_w2 and (has_1040 or not is_self_employed)
        if not flags.tax_docs_received:
            missing.append("tax_documents")

        # Loan-terms layer flags.
        flags.loan_application_complete  = "URLA_1003" in doc_types
        flags.purchase_agreement_received = "PURCHASE_AGREEMENT" in doc_types
        if not flags.loan_application_complete:
            missing.append("urla_1003")
        if not flags.purchase_agreement_received:
            missing.append("purchase_agreement")

        # rate_locked: a RATE_LOCK doc with lock_expiry > today.
        from datetime import date as _date
        rate_lock = loan_terms.get("rate_lock") or {}
        expiry = rate_lock.get("lock_expiry")
        if expiry:
            try:
                # Best-effort date parsing — accept ISO-8601 substrings.
                exp_date = _date.fromisoformat(str(expiry)[:10])
                flags.rate_locked = exp_date >= _date.today()
            except (ValueError, TypeError):
                flags.rate_locked = "RATE_LOCK" in doc_types
        else:
            flags.rate_locked = False

        # no_critical_conflicts: zero contradicts edges where the
        # delta exceeded the per-pair threshold (the only edges that
        # land in conflicts.critical).
        flags.no_critical_conflicts = len(
            (conflicts.get("critical") or [])
        ) == 0

        flags.missing_items = missing
        return flags

    # ------------------------------------------------------------------
    # Tier-2 helpers — Redis read-throughs + per-applicant aggregations.
    # Each falls back to "" / {} on cache miss; callers downstream
    # tolerate empty dicts.

    async def _read_asset_summary(self, applicant_id: str) -> dict:
        try:
            return await self.redis.get_asset_summary(applicant_id, tenant_id=current_tenant_id()) or {}
        except Exception as exc:
            logger.warning(
                "asset_summary_read_failed",
                extra={"applicant_id": applicant_id, "error": str(exc)},
            )
            return {}

    async def _read_identity_summary(self, applicant_id: str) -> dict:
        try:
            return await self.redis.get_identity_summary(applicant_id, tenant_id=current_tenant_id()) or {}
        except Exception as exc:
            logger.warning(
                "identity_summary_read_failed",
                extra={"applicant_id": applicant_id, "error": str(exc)},
            )
            return {}

    async def _fetch_doc_types(self, applicant_id: str) -> set:
        """Set of canonical document_type strings the applicant has on
        file. Used for readiness flags that gate on doc presence."""
        try:
            docs = await self.pg.get_documents_for_applicant(applicant_id, tenant_id=current_tenant_id())
        except Exception:
            return set()
        return {d.get("document_type") for d in docs if d.get("document_type")}

    @staticmethod
    def _compute_piti_inline(
        loan_amount: Optional[float],
        annual_rate_pct: Optional[float],
        term_months: Optional[int],
        annual_taxes: Optional[float],
        hoi_monthly: Optional[float],
        hoa_monthly: Optional[float] = 0,
    ) -> Optional[float]:
        """Compute monthly PITI from loan terms + property carrying costs.

        Returns ``None`` when the loan terms are insufficient (no
        principal, missing rate, or missing term). When loan terms are
        present but the property carrying-cost fields are missing,
        defaults the missing pieces to zero so the caller still gets a
        usable PI-only floor — better than ``None`` for DTI gating when
        an UR LA + rate-lock are on file but the appraisal hasn't
        landed yet.
        """
        if not loan_amount or loan_amount <= 0:
            return None
        if not annual_rate_pct or annual_rate_pct <= 0:
            return None
        if not term_months or term_months <= 0:
            return None
        # Standard amortization: M = P × r(1+r)^n / ((1+r)^n − 1)
        r = annual_rate_pct / 100 / 12
        n = term_months
        try:
            growth = (1 + r) ** n
            monthly_pi = loan_amount * (r * growth) / (growth - 1)
        except (OverflowError, ZeroDivisionError):
            return None
        monthly_tax = (annual_taxes or 0) / 12
        monthly_hoi = hoi_monthly or 0
        monthly_hoa = hoa_monthly or 0
        return round(monthly_pi + monthly_tax + monthly_hoi + monthly_hoa, 2)

    async def _build_loan_terms(self, applicant_id: str, app: dict) -> dict:
        """Merge URLA / RATE_LOCK / PURCHASE_AGREEMENT extracted_fields
        into a single loan_terms dict for the context payload. The app
        row's loan_amount / loan_purpose serve as the floor so callers
        get something sensible even before the URLA lands.
        """
        out: dict = {
            "loan_amount":   float(app["loan_amount"]) if app.get("loan_amount") else None,
            "interest_rate": app.get("interest_rate"),
            "loan_purpose":  app.get("loan_purpose"),
        }
        try:
            docs = await self.pg.get_documents_for_applicant(applicant_id, tenant_id=current_tenant_id())
        except Exception:
            return out

        # Pick the most-recent matching doc per type — get_documents_for_applicant
        # returns ordered DESC by received_at so first hit per type wins.
        seen: set = set()
        urla = rate_lock = purchase = None
        for d in docs:
            t = d.get("document_type")
            if not t or t in seen:
                continue
            seen.add(t)
            if t == "URLA_1003" and urla is None:
                urla = d.get("extracted_fields") or {}
            elif t == "RATE_LOCK" and rate_lock is None:
                rate_lock = d.get("extracted_fields") or {}
            elif t == "PURCHASE_AGREEMENT" and purchase is None:
                purchase = d.get("extracted_fields") or {}

        if urla:
            for k in (
                "loan_amount", "loan_purpose", "interest_rate",
                "loan_term_months", "monthly_income_stated",
            ):
                if urla.get(k) is not None:
                    out[k] = urla[k]
        if rate_lock:
            out["rate_lock"] = {
                "locked_rate": rate_lock.get("locked_rate"),
                "lock_expiry": rate_lock.get("lock_expiry"),
                "lock_days":   rate_lock.get("lock_days"),
                "loan_program": rate_lock.get("loan_program"),
            }
        if purchase:
            out["purchase_price"]      = purchase.get("purchase_price")
            out["earnest_money"]       = purchase.get("earnest_money")
            out["closing_date"]        = purchase.get("closing_date")
            out["seller_concessions"]  = purchase.get("seller_concessions")

        return out

    async def _build_conflicts(self, applicant_id: str) -> dict:
        """Return the top contradicts edges for an applicant in a shape
        Decision OS can render directly: ``{"count": N, "critical": [...]}``.
        Each critical entry is ``{"pair", "field", "values", "delta_pct"}``.
        """
        out = {"count": 0, "critical": []}
        try:
            rels = await self.pg.get_relationships_for_applicant(applicant_id, tenant_id=current_tenant_id())
        except Exception:
            return out

        critical: list[dict] = []
        for r in rels:
            if r.get("relationship_type") != "contradicts":
                continue
            critical.append({
                "pair": (
                    f"{r.get('source_doc_type', '')}↔"
                    f"{r.get('target_doc_type', '')}"
                ),
                "field":     r.get("field_name"),
                "values":    [r.get("source_value"), r.get("target_value")],
                "delta_pct": r.get("delta_pct"),
            })
        # Cap at 10 — context payload stays bounded; the full graph is
        # available via the dedicated /applicant/{id}/conflicts endpoint.
        out["count"]    = len(critical)
        out["critical"] = critical[:10]
        return out

    @staticmethod
    def _build_borrower_aggregation(
        applicant_id: str,
        snapshot: BorrowerSnapshot,
        primary_income: Optional[dict],
        assets: dict,
        identity: dict,
        role: str = "primary",
    ) -> BorrowerAggregation:
        """Pack the per-borrower entity caches into the new
        BorrowerAggregation shape. ``role`` controls whether we read
        ``primary_borrower`` or ``co_borrower`` out of the shared
        IncomeProfile dict."""
        income_section: dict = {}
        if primary_income:
            section_key = (
                "co_borrower" if role == "co_borrower" else "primary_borrower"
            )
            income_section = primary_income.get(section_key) or {}

        # Document count: assets + identity + the income/credit docs
        # listed under the borrower snapshot's income_sources. Best-
        # effort — exact counts come from /applicant/{id}/graph/summary.
        doc_count = (
            (assets.get("asset_doc_count") or 0)
            + (identity.get("identity_doc_count") or 0)
            + len(snapshot.income_sources or [])
        )

        return BorrowerAggregation(
            applicant_id=applicant_id,
            income=income_section,
            credit={
                "mid_score":           snapshot.mid_score,
                "credit_band":         snapshot.credit_band,
                "monthly_obligations": snapshot.monthly_obligations,
                "derogatory_marks":    snapshot.derogatory_marks,
            },
            assets=assets,
            identity=identity,
            document_count=doc_count,
            qualifying_monthly=snapshot.qualifying_monthly,
        )
