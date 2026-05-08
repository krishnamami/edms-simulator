"""BatchIndexer — incremental S3 → document_index → re-assembly pipeline.

Run lifecycle (per source, default ``s3``)::

  watermark.get(source)         # last_indexed_at, defaults to epoch
    ↓
  S3Scanner.scan_new(since=...) # files strictly newer than watermark
    ↓
  group_by_los(...)             # batch by LOS / applicant
    ↓
  for each group:
    pg.get_application_by_los_id(los_id)
    for each new doc:
        s3.get_raw → extract → save_document
    if income_touched: agg._run_assembly(...)
    if property_touched: redis.invalidate_property_profile(...)
    redis.invalidate_context(application_id)
    ↓
  watermark.update(source, now, stats)
  watermark.complete_run(run_id, stats)
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Optional

from core.indexing.s3_scanner import S3Document, S3Scanner
from core.indexing.watermark import WatermarkStore
from core.storage.s3_client import S3Client

logger = logging.getLogger(__name__)


_PROPERTY_DOC_TYPES = {
    "APPRAISAL_URAR", "APPRAISAL_UPDATE", "APPRAISAL_DESK", "APPRAISAL_FIELD",
    "AVM_REPORT", "TITLE_COMMITMENT", "TITLE_INSURANCE", "HOI_BINDER",
    "HOI_DECLARATIONS", "FLOOD_CERT", "PROPERTY_TAX_BILL",
    "PROPERTY_TAX_TRANSCRIPT", "SURVEY", "PEST_INSPECTION", "HOA_CERT",
    "CONDO_QUESTIONNAIRE", "PURCHASE_AGREEMENT",
}

# Cap how many distinct applicants we work on at once. The per-applicant
# assembly lock from core/aggregation/service.py already serializes
# concurrent assemblies for the *same* applicant — this semaphore caps
# parallelism across *different* applicants so a 500-applicant batch
# doesn't open 500 PG connections at once.
_MAX_CONCURRENT_APPLICANTS = 10


class BatchIndexer:
    def __init__(
        self,
        postgres_store,
        redis_store,
        aggregation_service,
        s3_client: Optional[S3Client] = None,
        scanner: Optional[S3Scanner] = None,
    ):
        self.pg = postgres_store
        self.redis = redis_store
        self.agg = aggregation_service
        self.s3 = s3_client or S3Client()
        self.watermarks = WatermarkStore(postgres_store)
        self.scanner = scanner or S3Scanner()

    async def run(
        self, source: str = "s3", dry_run: bool = False
    ) -> dict:
        start = time.time()
        now = datetime.now(tz=timezone.utc)
        since = await self.watermarks.get(source)
        run_id = await self.watermarks.create_run(source, since, now)

        logger.info(
            "batch_index_started",
            extra={
                "source":  source,
                "since":   since.isoformat(),
                "run_id":  run_id,
                "dry_run": dry_run,
            },
        )
        await self.watermarks.mark_running(source)

        stats: dict = {
            "found":                   0,
            "processed":               0,
            # Unknown-LOS skips: scanner saw S3 keys whose los_id has no
            # matching application row. Distinct from skipped_already_indexed.
            "skipped":                 0,
            # Already-indexed skips: the row was fully populated by the
            # event-driven /documents/upload or /ingest/* path, so the batch
            # indexer has nothing to add. We don't re-extract or re-assemble.
            "skipped_already_indexed": 0,
            "applicants_affected":     0,
            "errors":                  0,
            "error_details":           [],
            "run_id":                  run_id,
            "watermark_from":          since.isoformat(),
            "watermark_to":            now.isoformat(),
            "dry_run":                 dry_run,
        }

        try:
            new_docs = self.scanner.scan_new(since=since)
            stats["found"] = len(new_docs)

            if not new_docs:
                logger.info("batch_index_nothing_new", extra={"source": source})
            else:
                groups = self.scanner.group_by_los(new_docs)
                logger.info(
                    "batch_index_groups",
                    extra={"applicants": len(groups), "files": len(new_docs)},
                )

                # Process applicants in parallel, capped by a semaphore.
                # Each task returns (los_id, result_or_none, exc_or_none) so
                # the gather call itself never raises — exceptions surface
                # in the result tuple and roll up into stats below.
                sem = asyncio.Semaphore(_MAX_CONCURRENT_APPLICANTS)

                async def _process_with_sem(los_id, docs):
                    async with sem:
                        try:
                            result = await self._process_applicant(
                                los_id=los_id, new_docs=docs, dry_run=dry_run
                            )
                            return los_id, result, None
                        except Exception as exc:
                            return los_id, None, exc

                tasks = [
                    _process_with_sem(lid, d) for lid, d in groups.items()
                ]
                results = await asyncio.gather(*tasks)

                for los_id, result, exc in results:
                    if exc is not None:
                        stats["errors"] += 1
                        stats["error_details"].append({
                            "los_id": los_id, "error": str(exc)[:200],
                        })
                        logger.error(
                            "batch_index_applicant_error",
                            extra={"los_id": los_id, "error": str(exc)[:200]},
                        )
                        continue
                    if not result["applicant_known"]:
                        # Unknown LOS — count as skipped, not error.
                        stats["skipped"] += len(groups[los_id])
                    else:
                        stats["processed"] += result["processed"]
                        stats["skipped_already_indexed"] += result[
                            "skipped_already_indexed"
                        ]
                        if result["processed"] > 0:
                            stats["applicants_affected"] += 1

            if not dry_run:
                stats["duration_ms"] = int((time.time() - start) * 1000)
                await self.watermarks.update(source, now, stats)
            else:
                stats["duration_ms"] = int((time.time() - start) * 1000)

        except Exception as exc:
            stats["errors"] += 1
            stats["error_details"].append({"error": str(exc)[:200]})
            logger.error("batch_index_failed", extra={"error": str(exc)})
            stats.setdefault("duration_ms", int((time.time() - start) * 1000))

        await self.watermarks.complete_run(run_id, stats)
        logger.info("batch_index_complete", extra={**stats})
        return stats

    # ------------------------------------------------------------------

    async def _process_applicant(
        self, los_id: str, new_docs: list[S3Document], dry_run: bool
    ) -> dict:
        """Index ``new_docs`` for one LOS group.

        Returns a counts dict the caller folds into ``stats``::

            {"applicant_known": bool, "processed": int,
             "skipped_already_indexed": int}
        """
        app = await self.pg.get_application_by_los_id(los_id)
        if not app:
            logger.warning(
                "batch_index_unknown_los",
                extra={"los_id": los_id, "files": len(new_docs)},
            )
            return {
                "applicant_known":         False,
                "processed":               0,
                "skipped_already_indexed": 0,
            }

        applicant_id = app["applicant_id"]
        application_id = app["application_id"]
        logger.info(
            "batch_index_processing_applicant",
            extra={
                "los_id":       los_id,
                "applicant_id": applicant_id,
                "new_docs":     len(new_docs),
            },
        )

        if dry_run:
            for doc in new_docs:
                logger.info(
                    "dry_run_would_index",
                    extra={"key": doc.key, "type": doc.doc_type},
                )
            return {
                "applicant_known":         True,
                "processed":               len(new_docs),
                "skipped_already_indexed": 0,
            }

        processed_count = 0
        skipped_already_indexed = 0
        property_touched = False
        income_touched = False
        for s3_doc in new_docs:
            doc_id = f"DOC-{s3_doc.los_id}-{s3_doc.filename}"

            # Look up the existing row BEFORE downloading bytes. Two reasons
            # we may need it:
            #   1. Early-exit: row already fully indexed by the event-driven
            #      path (/documents/upload or /ingest/*) — skip entirely so
            #      we don't double-process and race the upload handler.
            #   2. Anti-clobber: row exists but our extractor returns empty
            #      (synthetic / minimal PDF that pymupdf can't parse) — keep
            #      the caller-supplied fields below.
            existing = await self.pg.get_document(doc_id) if hasattr(
                self.pg, "get_document"
            ) else None

            if (
                existing
                and existing.get("status") == "indexed"
                and existing.get("extracted_fields")
            ):
                logger.info(
                    "batch_index_skip_already_indexed",
                    extra={
                        "doc_id": doc_id,
                        "key":    s3_doc.key,
                        "type":   s3_doc.doc_type,
                    },
                )
                skipped_already_indexed += 1
                continue

            try:
                pdf_bytes = self.s3.get_raw(s3_doc.key)
            except Exception as exc:
                logger.error(
                    "batch_index_s3_read_failed",
                    extra={"key": s3_doc.key, "error": str(exc)[:200]},
                )
                continue

            extracted = await self._extract(
                pdf_bytes, s3_doc.doc_type, s3_doc.category,
            )

            existing_fields = (existing or {}).get("extracted_fields") or {}
            new_fields = extracted["fields"] or {}
            if not new_fields and existing_fields:
                logger.info(
                    "batch_index_skip_clobber",
                    extra={"doc_id": doc_id, "reason": "extractor_returned_empty"},
                )
                merged_fields = existing_fields
            else:
                merged_fields = {**existing_fields, **new_fields}

            doc_record = {
                "document_id":      doc_id,
                "applicant_id":     applicant_id,
                "application_id":   application_id,
                "document_type":    s3_doc.doc_type,
                "document_category": s3_doc.category,
                "borrower_role":    "primary",
                "s3_key":           s3_doc.key,
                "status":           "indexed" if merged_fields else "received",
                "is_current":       True,
                "extracted_fields": merged_fields,
                "confidence_score": extracted["confidence"],
            }
            try:
                await self.pg.save_document(doc_record)
            except Exception as exc:
                logger.error(
                    "batch_index_save_failed",
                    extra={"key": s3_doc.key, "error": str(exc)[:200]},
                )
                continue
            processed_count += 1

            cat = (s3_doc.category or "").lower()
            if cat == "property" or s3_doc.doc_type in _PROPERTY_DOC_TYPES:
                property_touched = True
            elif cat in (
                # Categories whose docs feed an entity cache that
                # _run_assembly refreshes write-through:
                #   income / employment → income profile
                #   credit               → credit profile
                #   asset                → asset:{aid} summary
                #   identity             → identity:{aid} summary
                #   loan_terms / vendor  → context invalidation (DTI etc.
                #                          can shift on rate-lock or AUS)
                "income", "credit", "asset", "identity",
                "employment", "loan_terms", "vendor",
            ):
                income_touched = True

            logger.info(
                "batch_index_doc_indexed",
                extra={
                    "doc_id":     doc_id,
                    "type":       s3_doc.doc_type,
                    "confidence": extracted["confidence"],
                },
            )

        # Re-assemble only the layers that actually changed.
        if income_touched:
            try:
                docs = await self.pg.get_documents_for_applicant(applicant_id)
                await self.agg._run_assembly(
                    applicant_id=applicant_id,
                    application_id=application_id,
                    co_applicant_id=app.get("co_applicant_id"),
                    documents=docs,
                    loan_data={},
                )
                logger.info(
                    "batch_index_income_reassembled",
                    extra={"applicant_id": applicant_id},
                )
            except Exception as exc:
                logger.error(
                    "batch_index_reassembly_failed",
                    extra={"applicant_id": applicant_id, "error": str(exc)[:200]},
                )

        if property_touched and app.get("property_id"):
            try:
                await self.redis.invalidate_property_profile(app["property_id"])
                logger.info(
                    "batch_index_property_invalidated",
                    extra={"property_id": app["property_id"]},
                )
            except Exception as exc:
                logger.error(
                    "batch_index_property_invalidate_failed",
                    extra={"error": str(exc)[:200]},
                )

        try:
            await self.redis.invalidate_context(application_id)
        except Exception as exc:
            logger.warning(
                "batch_index_invalidate_context_failed",
                extra={"application_id": application_id, "error": str(exc)[:200]},
            )
        return {
            "applicant_known":         True,
            "processed":               processed_count,
            "skipped_already_indexed": skipped_already_indexed,
        }

    # ------------------------------------------------------------------

    @staticmethod
    async def _extract(
        pdf_bytes: bytes,
        doc_type: str,
        doc_category: str = "",
    ) -> dict:
        """Pick a pymupdf extractor by doc-type, then fall through to
        Claude Vision if the deterministic extractor returned empty
        (or no extractor exists for this doc type at all). Always
        returns ``{"fields": dict, "confidence": float}`` — never
        raises. The AI fallback is gated on
        ``ENABLE_AI_EXTRACTION=true`` (default) and only fires when
        ``ANTHROPIC_API_KEY`` is set.
        """
        from core.documents.extractors.pymupdf_extractor import (
            extract_bank_statement,
            extract_credit_report,
            extract_paystub,
            extract_w2,
        )
        from core.property.extractors import (
            extract_1004mc,
            extract_appraisal_pdf,
            extract_avm_report,
            extract_flood_pdf,
            extract_hoi_pdf,
            extract_purchase_agreement,
            extract_tax_pdf,
        )
        from core.documents.extractors.income_extractors import (
            extract_1040,
            extract_1099,
            extract_irs_transcript,
            extract_k1,
            extract_schedule_c,
            extract_schedule_e,
        )
        from core.documents.extractors.asset_extractors import (
            extract_brokerage_account,
            extract_gift_letter,
            extract_retirement_account,
        )
        from core.documents.extractors.loan_extractors import (
            extract_offer_letter,
            extract_rate_lock,
            extract_urla_1003,
        )

        extractors = {
            # Income (canonical doc types from MISMOMapper). Accept the
            # alias forms too — TAX_RETURN_1040_* is canonical, but the
            # indexer may see FORM_1040 from a hand-typed S3 path.
            "W2_CURRENT":              extract_w2,
            "W2_PRIOR":                extract_w2,
            "PAYSTUB_CURRENT":         extract_paystub,
            "PAYSTUB_PRIOR":           extract_paystub,
            "IRS_TRANSCRIPT":          extract_irs_transcript,
            "TAX_RETURN_1040_CURRENT": extract_1040,
            "TAX_RETURN_1040_PRIOR":   extract_1040,
            "FORM_1040":               extract_1040,
            "SCHEDULE_C":              extract_schedule_c,
            "SCHEDULE_E":              extract_schedule_e,
            "SCHEDULE_F":              extract_schedule_e,  # same shape as E
            "1099_NEC":                extract_1099,
            "FORM_1099_NEC":           extract_1099,
            "K1_PARTNERSHIP":          extract_k1,
            "K1_SCHEDULE":             extract_k1,
            # Asset
            "BANK_STATEMENT_M1":         extract_bank_statement,
            "ASSET_STATEMENT_RETIREMENT": extract_retirement_account,
            "RETIREMENT_ACCOUNT":         extract_retirement_account,
            "ASSET_STATEMENT_BROKERAGE":  extract_brokerage_account,
            "BROKERAGE_ACCOUNT":          extract_brokerage_account,
            "GIFT_LETTER":               extract_gift_letter,
            # Credit
            "CREDIT_REPORT":           extract_credit_report,
            # Property / valuation
            "APPRAISAL_URAR":          extract_appraisal_pdf,
            "APPRAISAL_UPDATE":        extract_appraisal_pdf,
            "APPRAISAL_DESK":          extract_appraisal_pdf,
            "APPRAISAL_FIELD":         extract_appraisal_pdf,
            "AVM_REPORT":              extract_avm_report,
            "FORM_1004MC":             extract_1004mc,
            "HOI_BINDER":              extract_hoi_pdf,
            "HOI_DECLARATIONS":        extract_hoi_pdf,
            "FLOOD_CERT":              extract_flood_pdf,
            "PROPERTY_TAX_BILL":       extract_tax_pdf,
            "PURCHASE_AGREEMENT":      extract_purchase_agreement,
            # Loan terms / employment
            "URLA_1003":               extract_urla_1003,
            "RATE_LOCK":               extract_rate_lock,
            "OFFER_LETTER":            extract_offer_letter,
        }
        # Step 1 — deterministic extractor (free, fast, brittle on
        # non-synthetic PDFs).
        extractor = extractors.get(doc_type)
        det_fields: dict = {}
        det_conf: float = 0.5
        if extractor is not None:
            try:
                det_fields, det_conf_raw = extractor(pdf_bytes)
                det_conf = float(det_conf_raw or 0.5)
            except Exception as exc:
                logger.warning(
                    "batch_index_extract_failed",
                    extra={"doc_type": doc_type, "error": str(exc)[:200]},
                )

        if det_fields:
            return {"fields": det_fields, "confidence": det_conf}

        # Step 2 — Claude Vision fallback. Only fires when the
        # deterministic extractor returned empty (or no extractor
        # exists at all for this doc type). The fallback itself
        # gracefully returns ``({}, 0.5)`` when ENABLE_AI_EXTRACTION
        # is false or no API key is set, so this stays safe in CI
        # and on key-less deployments.
        try:
            from core.documents.extractors.claude_extractor import (
                extract_with_claude,
            )
            ai_fields, ai_conf = await extract_with_claude(
                pdf_bytes, doc_type, doc_category,
            )
            if ai_fields:
                return {"fields": ai_fields, "confidence": float(ai_conf)}
        except Exception as exc:
            logger.warning(
                "ai_extraction_failed",
                extra={"doc_type": doc_type, "error": str(exc)[:200]},
            )

        # Step 3 — give up gracefully. Empty fields + 0.5 confidence
        # is the documented "no signal" return; the indexer's
        # anti-clobber path keeps the caller-supplied extracted_fields.
        return {"fields": {}, "confidence": 0.5}
