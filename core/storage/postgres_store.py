"""Async Aurora-Postgres store for EDMS entities.

Uses the asyncpg pool from core.storage.db. All methods are async.
"""
import json
import logging
from datetime import date, datetime
from typing import Optional

from core.storage import db


def _to_date(value):
    if value is None or isinstance(value, date):
        return value
    return datetime.fromisoformat(str(value)).date()


def _to_ts(value):
    if value is None or isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))

logger = logging.getLogger(__name__)


def _to_jsonb(value):
    """asyncpg requires JSON columns to be supplied as JSON strings."""
    if value is None:
        return None
    return json.dumps(value, default=str)


def _row_to_dict(row) -> Optional[dict]:
    if row is None:
        return None
    import uuid as _uuid
    out = dict(row)
    for k, v in list(out.items()):
        # asyncpg returns UUID columns as uuid.UUID — coerce to str so
        # downstream Pydantic models with str fields don't reject them.
        if isinstance(v, _uuid.UUID):
            out[k] = str(v)
            continue
        if isinstance(v, str) and k in (
            "address_current",
            "identity_xrefs",
            "application_ids",
            "profile_data",
            "extracted_fields",
            "source_value",
            "target_value",
            "piti_components",
            "context_data",
            "payload",
            "events",
            "error_details",
        ):
            try:
                out[k] = json.loads(v)
            except Exception:
                pass
    return out


class PostgresStore:
    # ---------------- applicants / golden record -----------------

    async def save_golden_record(self, gr: dict) -> None:
        await db.execute(
            """
            INSERT INTO applicants (
                applicant_id, full_name, first_name, last_name, dob,
                ssn_hash, ssn_last4, email, phone, address_current,
                status, identity_xrefs, application_ids, created_at, updated_at
            ) VALUES (
                $1, $2, $3, $4, $5::date,
                $6, $7, $8, $9, $10::jsonb,
                $11, $12::jsonb, $13::jsonb, NOW(), NOW()
            )
            ON CONFLICT (applicant_id) DO UPDATE SET
                full_name       = EXCLUDED.full_name,
                first_name      = EXCLUDED.first_name,
                last_name       = EXCLUDED.last_name,
                dob             = EXCLUDED.dob,
                ssn_hash        = EXCLUDED.ssn_hash,
                ssn_last4       = EXCLUDED.ssn_last4,
                email           = EXCLUDED.email,
                phone           = EXCLUDED.phone,
                address_current = EXCLUDED.address_current,
                status          = EXCLUDED.status,
                identity_xrefs  = EXCLUDED.identity_xrefs,
                application_ids = EXCLUDED.application_ids,
                updated_at      = NOW()
            """,
            gr["applicant_id"],
            gr["full_name"],
            gr["first_name"],
            gr["last_name"],
            _to_date(gr["dob"]),
            gr["ssn_hash"],
            gr.get("ssn_last4"),
            gr.get("email"),
            gr.get("phone"),
            _to_jsonb(gr.get("address_current")),
            gr.get("status", "placeholder"),
            _to_jsonb(gr.get("identity_xrefs", [])),
            _to_jsonb(gr.get("application_ids", [])),
        )

    async def find_by_applicant_id(self, applicant_id: str) -> Optional[dict]:
        row = await db.fetchrow(
            "SELECT * FROM applicants WHERE applicant_id = $1", applicant_id
        )
        return _row_to_dict(row)

    async def find_by_ssn_hash(self, ssn_hash: str) -> Optional[dict]:
        row = await db.fetchrow(
            "SELECT * FROM applicants WHERE ssn_hash = $1", ssn_hash
        )
        return _row_to_dict(row)

    async def find_by_name_dob(self, last_name: str, dob: str) -> list:
        rows = await db.fetch(
            "SELECT * FROM applicants WHERE LOWER(last_name) = LOWER($1) AND dob = $2::date",
            last_name,
            dob,
        )
        return [_row_to_dict(r) for r in rows]

    async def update_status(self, applicant_id: str, status: str) -> None:
        await db.execute(
            "UPDATE applicants SET status = $2, updated_at = NOW() WHERE applicant_id = $1",
            applicant_id,
            status,
        )

    async def next_sequence(self) -> int:
        return int(await db.fetchval("SELECT nextval('applicant_sequence')"))

    # ---------------- xrefs -----------------

    async def save_xref(self, xref: dict) -> None:
        await db.execute(
            """
            INSERT INTO applicant_identity_xref (
                applicant_id, source_system, source_id, match_confidence, match_method
            ) VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (applicant_id, source_system, source_id) DO NOTHING
            """,
            xref["applicant_id"],
            xref["source_system"],
            xref["source_id"],
            float(xref["match_confidence"]),
            xref["match_method"],
        )

    # ---------------- applications -----------------

    async def save_application(self, app: dict) -> None:
        await db.execute(
            """
            INSERT INTO applications (
                application_id, applicant_id, co_applicant_id, los_id, status, created_at
            ) VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (application_id) DO UPDATE SET
                applicant_id    = EXCLUDED.applicant_id,
                co_applicant_id = EXCLUDED.co_applicant_id,
                status          = EXCLUDED.status
            """,
            app["application_id"],
            app["applicant_id"],
            app.get("co_applicant_id"),
            app["los_id"],
            app.get("status", "active"),
        )

    async def get_application_by_los_id(self, los_id: str) -> Optional[dict]:
        row = await db.fetchrow(
            "SELECT * FROM applications WHERE los_id = $1", los_id
        )
        return _row_to_dict(row)

    async def get_application(self, application_id: str) -> Optional[dict]:
        """Lookup an application by its primary key."""
        row = await db.fetchrow(
            "SELECT * FROM applications WHERE application_id = $1",
            application_id,
        )
        return _row_to_dict(row)

    async def get_all_applications(self, limit: int = 50) -> list:
        """Return the N most-recent applications. Used by the dashboard."""
        rows = await db.fetch(
            "SELECT * FROM applications ORDER BY created_at DESC LIMIT $1",
            limit,
        )
        return [_row_to_dict(r) for r in rows]

    async def get_raw_ingestion_for_application(
        self, application_id: str
    ) -> list:
        """Phase F: timeline reads raw_ingestion rows scoped to an
        application_id. Includes rows where applicant_id matches the
        application's primary or co-applicant, since some channels land
        the row before the application_id is known."""
        rows = await db.fetch(
            """
            SELECT r.*
            FROM raw_ingestion r
            LEFT JOIN applications a ON a.application_id = $1
            WHERE r.application_id = $1
               OR r.applicant_id    = a.applicant_id
               OR r.applicant_id    = a.co_applicant_id
            ORDER BY r.received_at ASC
            """,
            application_id,
        )
        return [_row_to_dict(r) for r in rows]

    async def get_application_by_applicant(
        self, applicant_id: str
    ) -> Optional[dict]:
        """Return the most-recent application that has ``applicant_id`` as
        primary or co-applicant. Used by the service layer to invalidate
        the right context cache after a borrower-side update."""
        row = await db.fetchrow(
            """
            SELECT * FROM applications
            WHERE applicant_id = $1 OR co_applicant_id = $1
            ORDER BY created_at DESC LIMIT 1
            """,
            applicant_id,
        )
        return _row_to_dict(row)

    async def update_application_loan_data(
        self, application_id: str, loan_data: dict
    ) -> None:
        """Phase C alias for :meth:`update_application_loan_fields`."""
        await self.update_application_loan_fields(application_id, loan_data)

    # ---------------- Phase E: webhooks + context versioning -----------------

    async def get_active_webhooks(self, event_type: str) -> list:
        """Return every active webhook subscribed to ``event_type``.

        Subscription is encoded as JSONB array on ``webhooks.events``;
        the ``@>`` operator finds rows whose array contains the string.
        """
        rows = await db.fetch(
            """
            SELECT * FROM webhooks
            WHERE is_active = TRUE
              AND events @> $1::jsonb
            ORDER BY created_at ASC
            """,
            json.dumps([event_type]),
        )
        return [_row_to_dict(r) for r in rows]

    async def list_webhooks(self) -> list:
        rows = await db.fetch(
            "SELECT * FROM webhooks ORDER BY created_at DESC"
        )
        return [_row_to_dict(r) for r in rows]

    async def get_webhook(self, webhook_id: str) -> Optional[dict]:
        row = await db.fetchrow(
            "SELECT * FROM webhooks WHERE webhook_id = $1::uuid", webhook_id
        )
        return _row_to_dict(row)

    async def save_webhook(self, webhook: dict) -> str:
        new_id = await db.fetchval(
            """
            INSERT INTO webhooks (
                name, url, secret, events, is_active
            ) VALUES (
                $1, $2, $3, $4::jsonb, COALESCE($5, TRUE)
            )
            RETURNING webhook_id
            """,
            webhook["name"],
            webhook["url"],
            webhook.get("secret"),
            json.dumps(webhook.get("events") or ["context_updated"]),
            webhook.get("is_active"),
        )
        return str(new_id)

    async def deactivate_webhook(self, webhook_id: str) -> None:
        await db.execute(
            "UPDATE webhooks SET is_active = FALSE WHERE webhook_id = $1::uuid",
            webhook_id,
        )

    async def save_webhook_delivery(self, delivery: dict) -> None:
        await db.execute(
            """
            INSERT INTO webhook_deliveries (
                webhook_id, event_type, application_id, payload,
                response_status, response_body, success
            ) VALUES (
                $1::uuid, $2, $3, $4::jsonb, $5, $6, $7
            )
            """,
            delivery.get("webhook_id"),
            delivery["event_type"],
            delivery.get("application_id"),
            _to_jsonb(delivery.get("payload")),
            delivery.get("response_status"),
            delivery.get("response_body"),
            bool(delivery.get("success", False)),
        )

    async def get_webhook_deliveries(
        self, webhook_id: str, limit: int = 50
    ) -> list:
        rows = await db.fetch(
            """
            SELECT * FROM webhook_deliveries
            WHERE webhook_id = $1::uuid
            ORDER BY delivered_at DESC LIMIT $2
            """,
            webhook_id,
            limit,
        )
        return [_row_to_dict(r) for r in rows]

    async def increment_webhook_failures(self, webhook_id) -> None:
        if not webhook_id:
            return
        await db.execute(
            """
            UPDATE webhooks
               SET failure_count = COALESCE(failure_count, 0) + 1,
                   last_triggered = NOW()
             WHERE webhook_id = $1::uuid
            """,
            str(webhook_id),
        )

    async def save_context_version(self, version: dict) -> str:
        new_id = await db.fetchval(
            """
            INSERT INTO context_versions (
                application_id, context_data, assembled_at,
                trigger_event, trigger_doc_id
            ) VALUES (
                $1, $2::jsonb, $3::timestamptz, $4, $5
            )
            RETURNING version_id
            """,
            version["application_id"],
            _to_jsonb(version["context_data"]),
            _to_ts(version["assembled_at"]),
            version.get("trigger_event"),
            version.get("trigger_doc_id"),
        )
        return str(new_id)

    async def get_context_versions(
        self, application_id: str, limit: int = 10
    ) -> list:
        rows = await db.fetch(
            """
            SELECT version_id, application_id, assembled_at,
                   trigger_event, trigger_doc_id, created_at
            FROM context_versions
            WHERE application_id = $1
            ORDER BY assembled_at DESC LIMIT $2
            """,
            application_id,
            limit,
        )
        return [_row_to_dict(r) for r in rows]

    # ---------------- incremental indexer (watermarks + runs) -----------------

    async def get_watermark(self, source: str) -> Optional[dict]:
        row = await db.fetchrow(
            "SELECT * FROM indexing_watermarks WHERE source = $1", source
        )
        return _row_to_dict(row)

    async def upsert_watermark_status(self, source: str, status: str) -> None:
        await db.execute(
            """
            INSERT INTO indexing_watermarks (source, status)
            VALUES ($1, $2)
            ON CONFLICT (source) DO UPDATE SET status = EXCLUDED.status
            """,
            source,
            status,
        )

    async def upsert_watermark_complete(
        self,
        source: str,
        last_indexed_at,
        files_processed: int,
        files_skipped: int,
        errors: int,
        run_duration_ms: Optional[int] = None,
    ) -> None:
        status = "failed" if errors and not files_processed else "complete"
        await db.execute(
            """
            INSERT INTO indexing_watermarks (
                source, last_indexed_at, last_run_at,
                files_processed, files_skipped, errors,
                status, run_duration_ms
            ) VALUES (
                $1, $2::timestamptz, NOW(), $3, $4, $5, $6, $7
            )
            ON CONFLICT (source) DO UPDATE SET
                last_indexed_at = EXCLUDED.last_indexed_at,
                last_run_at     = NOW(),
                files_processed = EXCLUDED.files_processed,
                files_skipped   = EXCLUDED.files_skipped,
                errors          = EXCLUDED.errors,
                status          = EXCLUDED.status,
                run_duration_ms = EXCLUDED.run_duration_ms
            """,
            source,
            _to_ts(last_indexed_at),
            int(files_processed),
            int(files_skipped),
            int(errors),
            status,
            run_duration_ms,
        )

    async def set_watermark_timestamp(
        self, source: str, last_indexed_at
    ) -> None:
        """Manual watermark adjustment — used by the PUT /indexing/watermark
        admin endpoint. Does not touch the run-stat columns."""
        await db.execute(
            """
            INSERT INTO indexing_watermarks (source, last_indexed_at)
            VALUES ($1, $2::timestamptz)
            ON CONFLICT (source) DO UPDATE SET
                last_indexed_at = EXCLUDED.last_indexed_at
            """,
            source,
            _to_ts(last_indexed_at),
        )

    async def create_indexing_run(
        self, source: str, watermark_from, watermark_to
    ) -> str:
        new_id = await db.fetchval(
            """
            INSERT INTO indexing_runs (source, watermark_from, watermark_to)
            VALUES ($1, $2::timestamptz, $3::timestamptz)
            RETURNING run_id
            """,
            source,
            _to_ts(watermark_from),
            _to_ts(watermark_to),
        )
        return str(new_id)

    async def complete_indexing_run(
        self, run_id: str, stats: dict
    ) -> None:
        errors = int(stats.get("errors") or 0)
        status = (
            "complete_with_errors"
            if errors and (stats.get("processed") or 0) > 0
            else ("failed" if errors else "complete")
        )
        await db.execute(
            """
            UPDATE indexing_runs SET
                completed_at        = NOW(),
                files_found         = $1,
                files_processed     = $2,
                files_skipped       = $3,
                applicants_affected = $4,
                errors              = $5,
                error_details       = $6::jsonb,
                status              = $7
             WHERE run_id = $8::uuid
            """,
            int(stats.get("found") or 0),
            int(stats.get("processed") or 0),
            int(stats.get("skipped") or 0),
            int(stats.get("applicants_affected") or 0),
            errors,
            json.dumps(stats.get("error_details") or []),
            status,
            run_id,
        )

    async def get_indexing_runs(
        self, source: Optional[str] = None, limit: int = 50
    ) -> list:
        if source:
            rows = await db.fetch(
                """
                SELECT * FROM indexing_runs
                WHERE source = $1
                ORDER BY started_at DESC LIMIT $2
                """,
                source,
                limit,
            )
        else:
            rows = await db.fetch(
                """
                SELECT * FROM indexing_runs
                ORDER BY started_at DESC LIMIT $1
                """,
                limit,
            )
        return [_row_to_dict(r) for r in rows]

    async def get_indexing_run(self, run_id: str) -> Optional[dict]:
        row = await db.fetchrow(
            "SELECT * FROM indexing_runs WHERE run_id = $1::uuid", run_id
        )
        return _row_to_dict(row)

    async def get_table_count(self, table_name: str) -> int:
        """Used by the admin /admin/table-count endpoint. Caller must
        whitelist ``table_name`` — this method does NOT validate, since
        it interpolates the identifier into the SQL string."""
        val = await db.fetchval(f"SELECT COUNT(*) FROM {table_name}")
        return int(val or 0)

    async def get_context_at(
        self, application_id: str, timestamp: str
    ) -> Optional[dict]:
        """Return the most-recent context version assembled at or
        before ``timestamp`` (ISO-8601). Used for audit replay."""
        ts = _to_ts(timestamp)
        row = await db.fetchrow(
            """
            SELECT * FROM context_versions
            WHERE application_id = $1
              AND assembled_at <= $2::timestamptz
            ORDER BY assembled_at DESC LIMIT 1
            """,
            application_id,
            ts,
        )
        if not row:
            return None
        out = _row_to_dict(row)
        data = out.get("context_data")
        if isinstance(data, str):
            out["context_data"] = json.loads(data)
        return out

    # ---------------- income profiles (versioned) -----------------

    async def save_income_profile(self, profile: dict) -> str:
        """Upsert a single current income profile per applicant. Old rows
        are removed rather than versioned via superseded_by — one row per
        applicant, not a growing history. Functionally equivalent to
        INSERT ... ON CONFLICT (applicant_id) DO UPDATE, implemented as
        DELETE + INSERT because the schema doesn't carry a unique
        constraint on applicant_id and we want to avoid a prod-side
        migration to add one."""
        applicant_id = profile["applicant_id"]
        await db.execute(
            "DELETE FROM income_profiles WHERE applicant_id = $1",
            applicant_id,
        )
        new_id = await db.fetchval(
            """
            INSERT INTO income_profiles (
                applicant_id, application_id, assembled_at, profile_data,
                lineage_hash, version
            ) VALUES ($1, $2, $3::timestamptz, $4::jsonb, $5, 1)
            RETURNING profile_id
            """,
            applicant_id,
            profile.get("application_id"),
            _to_ts(profile["assembled_at"]),
            _to_jsonb(profile),
            profile.get("lineage_hash", ""),
        )
        return str(new_id)

    async def get_income_profile(self, applicant_id: str) -> Optional[dict]:
        row = await db.fetchrow(
            """
            SELECT profile_data, lineage_hash, version, assembled_at
            FROM income_profiles
            WHERE applicant_id = $1 AND superseded_by IS NULL
            ORDER BY created_at DESC LIMIT 1
            """,
            applicant_id,
        )
        if not row:
            return None
        data = row["profile_data"]
        if isinstance(data, str):
            data = json.loads(data)
        data["_version"] = row["version"]
        return data

    # ---------------- credit profiles -----------------

    async def save_credit_profile(self, profile: dict) -> None:
        """Upsert a single current credit profile per applicant. Old rows
        (including any historical is_current=FALSE rows from the prior
        versioned implementation) are removed so the table stays at one
        row per applicant. Functionally equivalent to INSERT ... ON
        CONFLICT (applicant_id) DO UPDATE; uses DELETE + INSERT to avoid
        a prod-side schema migration to promote the partial unique index
        idx_credit_current to a full unique constraint."""
        applicant_id = profile["applicant_id"]
        await db.execute(
            "DELETE FROM credit_profiles WHERE applicant_id = $1",
            applicant_id,
        )
        await db.execute(
            """
            INSERT INTO credit_profiles (
                applicant_id, mid_score, credit_band, profile_data,
                report_date, expiry_date, is_current
            ) VALUES ($1, $2, $3, $4::jsonb, $5::date, $6::date, TRUE)
            """,
            applicant_id,
            int(profile["mid_score"]),
            profile["credit_band"],
            _to_jsonb(profile),
            _to_date(profile.get("report_date")),
            _to_date(profile.get("expiry_date")),
        )

    async def get_credit_profile(self, applicant_id: str) -> Optional[dict]:
        row = await db.fetchrow(
            """
            SELECT profile_data
            FROM credit_profiles
            WHERE applicant_id = $1 AND is_current = TRUE
            ORDER BY created_at DESC LIMIT 1
            """,
            applicant_id,
        )
        if not row:
            return None
        data = row["profile_data"]
        if isinstance(data, str):
            data = json.loads(data)
        return data

    # ---------------- documents -----------------

    async def save_document(self, doc: dict) -> None:
        # extraction_method priority on upsert:
        #   deterministic > caller_supplied > ai_vision > none
        # The CASE picks the higher-ranked of the existing row's
        # method and the incoming method. Order: if either side is
        # ``deterministic``, that wins; otherwise if either is
        # ``caller_supplied``, that wins; etc. This means a doc first
        # uploaded with caller_supplied fields then re-extracted by the
        # batch indexer to ``deterministic`` correctly upgrades; a doc
        # that AI Vision touched then later landed with caller_supplied
        # fields correctly downgrades-to-better.
        await db.execute(
            """
            INSERT INTO document_index (
                document_id, applicant_id, application_id, document_type,
                document_category, borrower_role, s3_key, status,
                expiry_date, is_current, extracted_fields, confidence_score,
                extraction_method
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                $9::date, $10, $11::jsonb, $12, $13
            )
            ON CONFLICT (document_id) DO UPDATE SET
                applicant_id      = EXCLUDED.applicant_id,
                application_id    = EXCLUDED.application_id,
                document_type     = EXCLUDED.document_type,
                document_category = EXCLUDED.document_category,
                borrower_role     = EXCLUDED.borrower_role,
                s3_key            = EXCLUDED.s3_key,
                status            = EXCLUDED.status,
                expiry_date       = EXCLUDED.expiry_date,
                is_current        = EXCLUDED.is_current,
                extracted_fields  = EXCLUDED.extracted_fields,
                confidence_score  = EXCLUDED.confidence_score,
                extraction_method = CASE
                    WHEN document_index.extraction_method = 'deterministic'
                         OR EXCLUDED.extraction_method = 'deterministic'
                        THEN 'deterministic'
                    WHEN document_index.extraction_method = 'caller_supplied'
                         OR EXCLUDED.extraction_method = 'caller_supplied'
                        THEN 'caller_supplied'
                    WHEN document_index.extraction_method = 'ai_vision'
                         OR EXCLUDED.extraction_method = 'ai_vision'
                        THEN 'ai_vision'
                    ELSE 'none'
                END
            """,
            doc["document_id"],
            doc["applicant_id"],
            doc.get("application_id"),
            doc["document_type"],
            doc["document_category"],
            doc.get("borrower_role", "primary"),
            doc.get("s3_key"),
            doc.get("status", "received"),
            _to_date(doc.get("expiry_date")),
            doc.get("is_current", True),
            _to_jsonb(doc.get("extracted_fields")),
            doc.get("confidence_score"),
            doc.get("extraction_method") or "none",
        )

    async def get_document(self, document_id: str) -> Optional[dict]:
        """Fetch a single document_index row by document_id, or None.
        Used by the batch indexer to avoid clobbering caller-supplied
        extracted_fields with an empty extractor result."""
        row = await db.fetchrow(
            "SELECT * FROM document_index WHERE document_id = $1",
            document_id,
        )
        return _row_to_dict(row)

    async def get_documents_for_applicant(self, applicant_id: str) -> list:
        rows = await db.fetch(
            """
            SELECT * FROM document_index
            WHERE applicant_id = $1 AND is_current = TRUE
            ORDER BY received_at DESC
            """,
            applicant_id,
        )
        return [_row_to_dict(r) for r in rows]

    # --- Attribute query helpers (Build: comprehensive indexing) -----

    async def get_field_value(
        self, applicant_id: str, doc_type: str, field_name: str
    ) -> Optional[dict]:
        """Highest-priority value for ``field_name`` from the most recent
        ``doc_type`` document for ``applicant_id``. Uses the GIN index on
        ``extracted_fields`` and the (applicant_id, document_type)
        composite index for the lookup."""
        row = await db.fetchrow(
            """
            SELECT extracted_fields -> $3 AS field_value,
                   confidence_score,
                   received_at,
                   document_id
            FROM document_index
            WHERE applicant_id = $1
              AND document_type = $2
              AND extracted_fields IS NOT NULL
              AND extracted_fields ? $3
            ORDER BY received_at DESC
            LIMIT 1
            """,
            applicant_id, doc_type, field_name,
        )
        if not row:
            return None
        return {
            "value":       row["field_value"],
            "confidence":  row["confidence_score"],
            "received_at": row["received_at"],
            "document_id": row["document_id"],
        }

    async def get_all_field_values(
        self, applicant_id: str, field_name: str
    ) -> list:
        """All occurrences of ``field_name`` across every doc type for
        the applicant. Used by the navigator + ``/applicant/{id}/field``
        endpoint to compare a value across sources."""
        rows = await db.fetch(
            """
            SELECT document_type,
                   document_id,
                   extracted_fields -> $2 AS field_value,
                   confidence_score,
                   extraction_method,
                   received_at
            FROM document_index
            WHERE applicant_id = $1
              AND extracted_fields IS NOT NULL
              AND extracted_fields ? $2
            ORDER BY confidence_score DESC NULLS LAST, received_at DESC
            """,
            applicant_id, field_name,
        )
        return [_row_to_dict(r) for r in rows]

    async def get_documents_by_category(
        self, applicant_id: str, category: str
    ) -> list:
        """Indexed (status='indexed') documents for an applicant scoped
        to a single ``document_category``."""
        rows = await db.fetch(
            """
            SELECT * FROM document_index
            WHERE applicant_id = $1
              AND document_category = $2
              AND status = 'indexed'
            ORDER BY received_at DESC
            """,
            applicant_id, category,
        )
        return [_row_to_dict(r) for r in rows]

    async def find_documents_with_field(
        self,
        applicant_id: str,
        field_name: str,
        field_value=None,
    ) -> list:
        """Documents for ``applicant_id`` that have ``field_name`` set,
        optionally to a specific value. Uses the GIN index when a value
        is provided so it's an indexed lookup, not a sequential scan."""
        if field_value is not None:
            rows = await db.fetch(
                """
                SELECT document_id, document_type,
                       extracted_fields -> $3 AS value,
                       confidence_score
                FROM document_index
                WHERE applicant_id = $1
                  AND extracted_fields @> $2::jsonb
                """,
                applicant_id,
                json.dumps({field_name: field_value}, default=str),
                field_name,
            )
        else:
            rows = await db.fetch(
                """
                SELECT document_id, document_type,
                       extracted_fields -> $2 AS value,
                       confidence_score
                FROM document_index
                WHERE applicant_id = $1
                  AND extracted_fields ? $2
                """,
                applicant_id, field_name,
            )
        return [_row_to_dict(r) for r in rows]

    async def get_highest_confidence_field(
        self, applicant_id: str, field_name: str
    ) -> Optional[dict]:
        """Pick the highest-confidence ``field_name`` reading across
        every indexed document for ``applicant_id``. Source ranking from
        SOURCE_CONFIDENCE_RANKING is the primary key; per-row
        ``confidence_score`` is the tiebreaker."""
        from core.ingestion.confidence import SOURCE_CONFIDENCE_RANKING

        docs = await self.get_all_field_values(applicant_id, field_name)
        if not docs:
            return None

        def sort_key(d):
            doc_type = (d.get("document_type") or "")
            type_key = SOURCE_CONFIDENCE_RANKING.get(
                doc_type.replace("_CURRENT", "_PDF").replace("_PRIOR", "_PDF"),
                0.5,
            )
            conf = float(d.get("confidence_score") or 0)
            return (type_key, conf)

        return sorted(docs, key=sort_key, reverse=True)[0]

    async def get_documents_for_application(self, application_id: str) -> list:
        rows = await db.fetch(
            """
            SELECT * FROM document_index
            WHERE application_id = $1 AND is_current = TRUE
            ORDER BY received_at DESC
            """,
            application_id,
        )
        return [_row_to_dict(r) for r in rows]

    async def get_all_applicants(self) -> list:
        """Return every applicant row. Used at app startup to hydrate
        the in-memory XRefStore so applicant_id sequence and SSN /
        source-system lookups survive across restarts."""
        rows = await db.fetch("SELECT * FROM applicants")
        return [_row_to_dict(r) for r in rows]

    # ---------------- document knowledge graph -----------------

    async def save_relationship(self, rel: dict) -> None:
        await db.execute(
            """
            INSERT INTO document_relationships (
                relationship_id, applicant_id, source_doc_id, target_doc_id,
                relationship_type, field_name, source_value, target_value,
                delta_pct, confidence, reasoning, created_by
            ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9,$10,$11,$12)
            ON CONFLICT (relationship_id) DO NOTHING
            """,
            rel["relationship_id"],
            rel["applicant_id"],
            rel["source_doc_id"],
            rel["target_doc_id"],
            rel["relationship_type"],
            rel.get("field_name"),
            json.dumps(rel.get("source_value"), default=str),
            json.dumps(rel.get("target_value"), default=str),
            rel.get("delta_pct"),
            rel["confidence"],
            rel.get("reasoning", ""),
            rel.get("created_by", "reconciler"),
        )

    async def get_relationships_for_applicant(self, applicant_id: str) -> list:
        rows = await db.fetch(
            """
            SELECT * FROM document_relationships
            WHERE applicant_id = $1
            ORDER BY created_at DESC
            """,
            applicant_id,
        )
        return [_row_to_dict(r) for r in rows]

    async def get_conflicts_for_applicant(self, applicant_id: str) -> list:
        rows = await db.fetch(
            """
            SELECT * FROM document_relationships
            WHERE applicant_id = $1 AND relationship_type = 'contradicts'
            ORDER BY confidence DESC
            """,
            applicant_id,
        )
        return [_row_to_dict(r) for r in rows]

    async def get_graph_summary(self, applicant_id: str) -> dict:
        docs = await self.get_documents_for_applicant(applicant_id)
        rels = await self.get_relationships_for_applicant(applicant_id)
        conflicts = [r for r in rels if r["relationship_type"] == "contradicts"]
        confirms  = [r for r in rels if r["relationship_type"] == "confirms"]
        # Per-applicant extraction-method breakdown so ops can see at a
        # glance how the doc fields were populated. The four buckets
        # cover every save_document path:
        #   deterministic   — pymupdf / income / asset / loan extractor
        #   caller_supplied — LOS or API caller sent structured fields
        #   ai_vision       — Claude Vision fallback
        #   none            — placeholder row with no extracted_fields
        breakdown = {
            "deterministic":   0,
            "caller_supplied": 0,
            "ai_vision":       0,
            "none":            0,
        }
        for d in docs:
            method = d.get("extraction_method") or "none"
            breakdown[method] = breakdown.get(method, 0) + 1
        return {
            "applicant_id":         applicant_id,
            "document_count":       len(docs),
            "relationship_count":   len(rels),
            "confirmation_count":   len(confirms),
            "conflict_count":       len(conflicts),
            "requires_review":      len(conflicts) > 0,
            "extraction_breakdown": breakdown,
        }

    # ---------------- external IDs / LOS integration -----------------

    async def find_by_external_id(
        self, source_system: str, external_id: str
    ) -> Optional[dict]:
        """Return the applicant whose ``external_ids`` contains the
        ``{source_system: external_id}`` pair, or ``None``."""
        rows = await db.fetch(
            "SELECT * FROM applicants WHERE external_ids @> $1::jsonb LIMIT 1",
            json.dumps({source_system: external_id}),
        )
        return _row_to_dict(rows[0]) if rows else None

    async def add_external_id(
        self, applicant_id: str, source_system: str, external_id: str
    ) -> None:
        """Merge ``{source_system: external_id}`` into the applicant's
        ``external_ids`` JSONB blob (overwrites any prior value for the
        same source_system)."""
        await db.execute(
            """
            UPDATE applicants
               SET external_ids = external_ids || $1::jsonb,
                   updated_at   = NOW()
             WHERE applicant_id = $2
            """,
            json.dumps({source_system: external_id}),
            applicant_id,
        )

    async def get_application_by_external_loan_id(
        self, external_loan_id: str
    ) -> Optional[dict]:
        """Look up an application by the LOS's loan number."""
        row = await db.fetchrow(
            "SELECT * FROM applications WHERE external_loan_id = $1",
            external_loan_id,
        )
        return _row_to_dict(row)

    # ---------------- properties (Phase B) -----------------

    async def save_property(self, prop: dict) -> str:
        """Insert or update a property row. Returns the property_id."""
        await db.execute(
            """
            INSERT INTO properties (
                property_id, application_id, address_line1, address_line2,
                city, state, zip_code, property_type, units, year_built,
                sqft, status, created_at, updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, NOW(), NOW()
            )
            ON CONFLICT (property_id) DO UPDATE SET
                application_id = EXCLUDED.application_id,
                address_line1  = EXCLUDED.address_line1,
                address_line2  = EXCLUDED.address_line2,
                city           = EXCLUDED.city,
                state          = EXCLUDED.state,
                zip_code       = EXCLUDED.zip_code,
                property_type  = EXCLUDED.property_type,
                units          = EXCLUDED.units,
                year_built     = EXCLUDED.year_built,
                sqft           = EXCLUDED.sqft,
                status         = EXCLUDED.status,
                updated_at     = NOW()
            """,
            prop["property_id"],
            prop.get("application_id"),
            prop["address_line1"],
            prop.get("address_line2"),
            prop["city"],
            prop["state"],
            prop["zip_code"],
            prop["property_type"],
            int(prop.get("units", 1)),
            prop.get("year_built"),
            prop.get("sqft"),
            prop.get("status", "pending"),
        )
        return prop["property_id"]

    async def get_property(self, property_id: str) -> Optional[dict]:
        row = await db.fetchrow(
            "SELECT * FROM properties WHERE property_id = $1", property_id
        )
        return _row_to_dict(row)

    async def get_property_by_application(
        self, application_id: str
    ) -> Optional[dict]:
        row = await db.fetchrow(
            """
            SELECT * FROM properties WHERE application_id = $1
            ORDER BY created_at DESC LIMIT 1
            """,
            application_id,
        )
        return _row_to_dict(row)

    async def save_property_profile(self, profile: dict) -> str:
        """Insert a new property profile version, marking the prior current
        version superseded."""
        property_id = profile["property_id"]
        current = await db.fetchrow(
            """
            SELECT profile_id, version
            FROM property_profiles
            WHERE property_id = $1 AND superseded_by IS NULL
            """,
            property_id,
        )
        version = (current["version"] + 1) if current else 1

        new_id = await db.fetchval(
            """
            INSERT INTO property_profiles (
                property_id, application_id, appraised_value, appraisal_date,
                appraisal_type, appraisal_confidence, estimated_value,
                tax_assessed_value, annual_taxes, monthly_taxes,
                hoi_annual, hoi_monthly, flood_zone, flood_insurance_required,
                flood_insurance_monthly, hoa_monthly, condition_rating,
                piti_components, profile_data, lineage_hash, version,
                assembled_at
            ) VALUES (
                $1, $2, $3, $4::date, $5, $6, $7, $8, $9, $10, $11, $12,
                $13, $14, $15, $16, $17, $18::jsonb, $19::jsonb, $20, $21,
                $22::timestamptz
            )
            RETURNING profile_id
            """,
            property_id,
            profile.get("application_id"),
            profile.get("appraised_value"),
            _to_date(profile.get("appraisal_date")),
            profile.get("appraisal_type"),
            profile.get("appraisal_confidence"),
            profile.get("estimated_value"),
            profile.get("tax_assessed_value"),
            profile.get("annual_taxes"),
            profile.get("monthly_taxes"),
            profile.get("hoi_annual"),
            profile.get("hoi_monthly"),
            profile.get("flood_zone"),
            bool(profile.get("flood_insurance_required", False)),
            profile.get("flood_insurance_monthly"),
            profile.get("hoa_monthly", 0),
            profile.get("condition_rating"),
            _to_jsonb(profile.get("piti_components")),
            _to_jsonb(profile),
            profile.get("lineage_hash", ""),
            version,
            _to_ts(profile.get("assembled_at")) or datetime.utcnow(),
        )
        if current:
            await db.execute(
                "UPDATE property_profiles SET superseded_by = $1 WHERE profile_id = $2",
                new_id,
                current["profile_id"],
            )
        return str(new_id)

    async def get_property_profile(self, property_id: str) -> Optional[dict]:
        row = await db.fetchrow(
            """
            SELECT profile_data, lineage_hash, version, assembled_at
            FROM property_profiles
            WHERE property_id = $1 AND superseded_by IS NULL
            ORDER BY created_at DESC LIMIT 1
            """,
            property_id,
        )
        if not row:
            return None
        data = row["profile_data"]
        if isinstance(data, str):
            data = json.loads(data)
        data["_version"] = row["version"]
        return data

    async def get_property_docs(self, property_id: str) -> list:
        """Property documents are tagged via document_index.application_id ->
        properties.application_id. Resolve via the join."""
        rows = await db.fetch(
            """
            SELECT di.* FROM document_index di
            JOIN properties p ON di.application_id = p.application_id
            WHERE p.property_id = $1
              AND di.is_current = TRUE
              AND di.document_category = 'property'
            ORDER BY di.received_at DESC
            """,
            property_id,
        )
        return [_row_to_dict(r) for r in rows]

    async def update_application_property(
        self, application_id: str, property_id: str
    ) -> None:
        await db.execute(
            """
            UPDATE applications
               SET property_id = $1
             WHERE application_id = $2
            """,
            property_id,
            application_id,
        )

    async def update_application_loan_fields(
        self, application_id: str, loan_data: dict
    ) -> None:
        """Patch loan terms / URLA fields on an existing application.
        Only non-None values overwrite — uses ``COALESCE`` to preserve
        prior values for fields the caller didn't provide."""
        urla = loan_data.get("urla_fields")
        await db.execute(
            """
            UPDATE applications SET
                loan_amount      = COALESCE($1, loan_amount),
                interest_rate    = COALESCE($2, interest_rate),
                loan_term_months = COALESCE($3, loan_term_months),
                loan_purpose     = COALESCE($4, loan_purpose),
                loan_type        = COALESCE($5, loan_type),
                occupancy        = COALESCE($6, occupancy),
                external_loan_id = COALESCE($7, external_loan_id),
                urla_fields      = COALESCE($8::jsonb, urla_fields),
                updated_at       = NOW()
             WHERE application_id = $9
            """,
            loan_data.get("loan_amount"),
            loan_data.get("interest_rate"),
            loan_data.get("loan_term_months"),
            loan_data.get("loan_purpose"),
            loan_data.get("loan_type"),
            loan_data.get("occupancy"),
            loan_data.get("external_loan_id"),
            _to_jsonb(urla) if urla is not None else None,
            application_id,
        )

    # ---------------- reports (Interface 2: operational reports) -----------------
    #
    # The methods below back the /reports/* endpoints in api/reports.py.
    # They run analytical SQL across many loans (LIMIT/OFFSET pagination,
    # date-range filters) and never warm Redis directly — caching happens
    # at the endpoint layer with a 5-minute TTL keyed on the param hash.

    async def count_pipeline_report(
        self,
        date_from,
        date_to,
        status: Optional[str] = None,
    ) -> int:
        val = await db.fetchval(
            """
            SELECT COUNT(*)
            FROM applications a
            WHERE a.created_at >= $1::timestamptz
              AND a.created_at <  $2::timestamptz
              AND ($3::text IS NULL OR a.status = $3)
            """,
            _to_ts(date_from), _to_ts(date_to), status,
        )
        return int(val or 0)

    async def get_pipeline_report(
        self,
        date_from,
        date_to,
        status: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list:
        """One row per application with the heavy lifting done in SQL —
        per-applicant doc count, conflict count, max(received_at) — joined
        against the applicant golden record, income/credit profiles, and
        the latest context snapshot for readiness flags + DTI/LTV."""
        rows = await db.fetch(
            """
            SELECT
                a.application_id, a.los_id, a.status,
                a.applicant_id, a.co_applicant_id,
                a.loan_amount, a.interest_rate, a.created_at,
                p.full_name  AS borrower_name,
                cp.full_name AS co_borrower_name,
                ip.profile_data AS income_data,
                cr.mid_score    AS mid_score,
                cr.credit_band  AS credit_band,
                (SELECT COUNT(*) FROM document_index di
                  WHERE di.is_current = TRUE
                    AND (di.application_id = a.application_id
                         OR di.applicant_id = a.applicant_id
                         OR (a.co_applicant_id IS NOT NULL
                             AND di.applicant_id = a.co_applicant_id))
                ) AS docs_received,
                COALESCE(
                  (SELECT ARRAY_AGG(DISTINCT di.document_type)
                     FROM document_index di
                    WHERE di.is_current = TRUE
                      AND di.document_type IS NOT NULL
                      AND (di.application_id = a.application_id
                           OR di.applicant_id = a.applicant_id
                           OR (a.co_applicant_id IS NOT NULL
                               AND di.applicant_id = a.co_applicant_id))
                  ), ARRAY[]::text[]
                ) AS doc_types,
                (SELECT COUNT(*) FROM document_relationships dr
                  WHERE dr.relationship_type = 'contradicts'
                    AND (dr.applicant_id = a.applicant_id
                         OR (a.co_applicant_id IS NOT NULL
                             AND dr.applicant_id = a.co_applicant_id))
                ) AS conflict_count,
                (SELECT COUNT(*) FROM document_relationships dr
                  WHERE dr.relationship_type = 'contradicts'
                    AND COALESCE(dr.delta_pct, 0) >= 20
                    AND (dr.applicant_id = a.applicant_id
                         OR (a.co_applicant_id IS NOT NULL
                             AND dr.applicant_id = a.co_applicant_id))
                ) AS critical_conflict_count,
                (SELECT MAX(di.received_at) FROM document_index di
                  WHERE di.is_current = TRUE
                    AND (di.application_id = a.application_id
                         OR di.applicant_id = a.applicant_id
                         OR (a.co_applicant_id IS NOT NULL
                             AND di.applicant_id = a.co_applicant_id))
                ) AS last_doc_received_at,
                (SELECT cv.context_data FROM context_versions cv
                  WHERE cv.application_id = a.application_id
                  ORDER BY cv.assembled_at DESC LIMIT 1
                ) AS context_data
            FROM applications a
            LEFT JOIN applicants p   ON p.applicant_id  = a.applicant_id
            LEFT JOIN applicants cp  ON cp.applicant_id = a.co_applicant_id
            LEFT JOIN income_profiles ip
                ON ip.applicant_id = a.applicant_id AND ip.superseded_by IS NULL
            LEFT JOIN credit_profiles cr
                ON cr.applicant_id = a.applicant_id AND cr.is_current = TRUE
            WHERE a.created_at >= $1::timestamptz
              AND a.created_at <  $2::timestamptz
              AND ($3::text IS NULL OR a.status = $3)
            ORDER BY a.created_at DESC
            LIMIT $4 OFFSET $5
            """,
            _to_ts(date_from), _to_ts(date_to), status, int(limit), int(offset),
        )
        return [_row_to_dict(r) for r in rows]

    async def count_conflicts_report(
        self,
        date_from,
        date_to,
        min_delta_pct: Optional[float] = None,
    ) -> int:
        val = await db.fetchval(
            """
            SELECT COUNT(*)
            FROM document_relationships dr
            WHERE dr.relationship_type = 'contradicts'
              AND dr.created_at >= $1::timestamptz
              AND dr.created_at <  $2::timestamptz
              AND ($3::float IS NULL OR COALESCE(dr.delta_pct, 0) >= $3)
            """,
            _to_ts(date_from), _to_ts(date_to), min_delta_pct,
        )
        return int(val or 0)

    async def get_conflicts_report(
        self,
        date_from,
        date_to,
        min_delta_pct: Optional[float] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list:
        """Every contradicts edge in the window, joined to source/target
        document_type and the application that owns the applicant. The
        ordering — delta_pct DESC NULLS LAST — surfaces the largest
        divergences first so triage UIs see the highest-fraud-signal
        edges at the top of page 1."""
        rows = await db.fetch(
            """
            SELECT
                dr.relationship_id,
                dr.applicant_id,
                dr.source_doc_id,
                dr.target_doc_id,
                dr.relationship_type,
                dr.field_name,
                dr.source_value,
                dr.target_value,
                dr.delta_pct,
                dr.confidence,
                dr.created_at,
                sd.document_type AS source_doc_type,
                td.document_type AS target_doc_type,
                a.application_id,
                a.los_id,
                p.full_name AS borrower_name
            FROM document_relationships dr
            JOIN document_index sd ON sd.document_id = dr.source_doc_id
            JOIN document_index td ON td.document_id = dr.target_doc_id
            LEFT JOIN applications a
                ON a.applicant_id = dr.applicant_id
                OR a.co_applicant_id = dr.applicant_id
            LEFT JOIN applicants p ON p.applicant_id = dr.applicant_id
            WHERE dr.relationship_type = 'contradicts'
              AND dr.created_at >= $1::timestamptz
              AND dr.created_at <  $2::timestamptz
              AND ($3::float IS NULL OR COALESCE(dr.delta_pct, 0) >= $3)
            ORDER BY dr.delta_pct DESC NULLS LAST, dr.created_at DESC
            LIMIT $4 OFFSET $5
            """,
            _to_ts(date_from), _to_ts(date_to), min_delta_pct,
            int(limit), int(offset),
        )
        return [_row_to_dict(r) for r in rows]

    async def get_applications_with_doc_types(
        self,
        date_from,
        date_to,
    ) -> list:
        """Every application in the window with the de-duped set of
        document_types its file currently carries (across the primary +
        co-applicant + the application itself). Powers the completeness
        report — the endpoint computes the missing slots in Python by
        diffing against the _REQUIRED_DOCS / _CONDITIONAL_DOCS catalogs.
        Done in SQL with array_agg so we never round-trip per-row."""
        rows = await db.fetch(
            """
            SELECT
                a.application_id, a.los_id, a.applicant_id, a.co_applicant_id,
                a.created_at,
                COALESCE(
                  ARRAY_AGG(DISTINCT di.document_type)
                    FILTER (WHERE di.document_type IS NOT NULL),
                  ARRAY[]::text[]
                ) AS doc_types
            FROM applications a
            LEFT JOIN document_index di
              ON di.is_current = TRUE
             AND (di.application_id = a.application_id
                  OR di.applicant_id = a.applicant_id
                  OR (a.co_applicant_id IS NOT NULL
                      AND di.applicant_id = a.co_applicant_id))
            WHERE a.created_at >= $1::timestamptz
              AND a.created_at <  $2::timestamptz
            GROUP BY a.application_id, a.los_id, a.applicant_id,
                     a.co_applicant_id, a.created_at
            ORDER BY a.created_at DESC
            """,
            _to_ts(date_from), _to_ts(date_to),
        )
        return [_row_to_dict(r) for r in rows]

    async def get_extraction_method_totals(
        self,
        date_from,
        date_to,
    ) -> dict:
        """One row of grand totals for the extraction-quality report —
        one count column per extraction_method bucket. Filtered on
        received_at so the numerator/denominator stay in the same window."""
        row = await db.fetchrow(
            """
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE extraction_method = 'deterministic')   AS deterministic,
                COUNT(*) FILTER (WHERE extraction_method = 'caller_supplied') AS caller_supplied,
                COUNT(*) FILTER (WHERE extraction_method = 'ai_vision')       AS ai_vision,
                COUNT(*) FILTER (WHERE extraction_method = 'none'
                                   OR extraction_method IS NULL)              AS none_method
            FROM document_index
            WHERE received_at >= $1::timestamptz
              AND received_at <  $2::timestamptz
            """,
            _to_ts(date_from), _to_ts(date_to),
        )
        return _row_to_dict(row) or {}

    async def get_extraction_method_by_doc_type(
        self,
        date_from,
        date_to,
    ) -> list:
        rows = await db.fetch(
            """
            SELECT
                document_type,
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE extraction_method = 'deterministic')   AS deterministic,
                COUNT(*) FILTER (WHERE extraction_method = 'caller_supplied') AS caller_supplied,
                COUNT(*) FILTER (WHERE extraction_method = 'ai_vision')       AS ai_vision,
                COUNT(*) FILTER (WHERE extraction_method = 'none'
                                   OR extraction_method IS NULL)              AS none_method
            FROM document_index
            WHERE received_at >= $1::timestamptz
              AND received_at <  $2::timestamptz
            GROUP BY document_type
            ORDER BY total DESC, document_type
            """,
            _to_ts(date_from), _to_ts(date_to),
        )
        return [_row_to_dict(r) for r in rows]

    async def get_income_verification_data(
        self,
        date_from,
        date_to,
    ) -> list:
        """Pair every URLA_1003 with every W2_CURRENT for the same
        applicant in the window, surfacing the raw stated/documented
        numbers + applicant + application fields. The endpoint computes
        the delta_pct + flag in Python so non-numeric extracted_fields
        (e.g. ``box1_wages='one hundred ten thousand'`` from the chaos
        test) silently skip instead of raising in SQL CAST."""
        rows = await db.fetch(
            """
            SELECT
                u.applicant_id,
                u.application_id,
                u.extracted_fields ->> 'monthly_income_stated' AS monthly_stated_raw,
                w.extracted_fields ->> 'box1_wages'            AS w2_wages_raw,
                u.received_at AS urla_received_at,
                w.received_at AS w2_received_at,
                p.full_name AS borrower_name,
                a.los_id   AS los_id
            FROM document_index u
            JOIN document_index w
              ON w.applicant_id = u.applicant_id
             AND w.document_type = 'W2_CURRENT'
             AND w.is_current = TRUE
             AND w.extracted_fields ? 'box1_wages'
            LEFT JOIN applicants  p ON p.applicant_id = u.applicant_id
            LEFT JOIN applications a
              ON a.applicant_id  = u.applicant_id
              OR a.co_applicant_id = u.applicant_id
            WHERE u.document_type = 'URLA_1003'
              AND u.is_current = TRUE
              AND u.extracted_fields ? 'monthly_income_stated'
              AND u.received_at >= $1::timestamptz
              AND u.received_at <  $2::timestamptz
            """,
            _to_ts(date_from), _to_ts(date_to),
        )
        return [_row_to_dict(r) for r in rows]

    # ---------------- bulk export (Interface 3) -----------------
    #
    # The stream_* methods are async generators that yield rows one at a
    # time via a server-side asyncpg cursor (see core.storage.db.stream).
    # This lets the /export/* endpoints write a multi-thousand-row JSONL
    # stream straight to the HTTP response without ever loading the full
    # result set into Python memory. The per-stream prefetch defaults to
    # 500 — small enough to stay under typical API-gateway buffers,
    # large enough to keep round-trip count manageable.
    #
    # SAFE_NUM is a regex tested against an extracted_fields string
    # before casting to numeric. Chaos-test inputs like
    # box1_wages='one hundred ten thousand' would crash a bare ::numeric
    # cast; the CASE wrapper turns those into NULL instead, matching the
    # silent-skip semantics _coerce_float uses on the report endpoints.

    _SAFE_NUM = r"^-?[0-9]+(\.[0-9]+)?$"

    async def stream_entities(
        self,
        since=None,
        prefetch: int = 500,
    ):
        """One row per applicant joined with the latest income +
        credit profiles, owning application, and a few document /
        relationship aggregates (counts, asset rollups, identity
        flags) computed via correlated subqueries against
        document_index. Filtered on ``applicants.updated_at``."""
        query = f"""
            SELECT
                a.applicant_id,
                a.full_name,
                a.first_name,
                a.last_name,
                a.status                         AS applicant_status,
                a.updated_at,
                COALESCE(app.application_id, app2.application_id) AS application_id,
                CASE
                    WHEN app.applicant_id    = a.applicant_id THEN 'primary'
                    WHEN app2.co_applicant_id = a.applicant_id THEN 'co_borrower'
                    ELSE 'unknown'
                END                              AS role,
                ip.profile_data                  AS income_data,
                cp.mid_score                     AS mid_score,
                cp.profile_data                  AS credit_data,
                (SELECT COUNT(*) FROM document_index di
                  WHERE di.applicant_id = a.applicant_id
                    AND di.is_current = TRUE
                ) AS document_count,
                (SELECT COUNT(*) FROM document_relationships dr
                  WHERE dr.applicant_id = a.applicant_id
                    AND dr.relationship_type = 'contradicts'
                ) AS conflict_count,
                (SELECT COALESCE(SUM(
                          CASE WHEN di.extracted_fields->>'ending_balance' ~ '{self._SAFE_NUM}'
                               THEN (di.extracted_fields->>'ending_balance')::numeric
                               ELSE 0 END), 0)
                   FROM document_index di
                  WHERE di.applicant_id = a.applicant_id
                    AND di.is_current = TRUE
                    AND di.document_type LIKE 'BANK_STATEMENT%'
                ) AS total_liquid,
                (SELECT COALESCE(SUM(
                          CASE WHEN di.extracted_fields->>'balance' ~ '{self._SAFE_NUM}'
                               THEN (di.extracted_fields->>'balance')::numeric
                               ELSE 0 END), 0)
                   FROM document_index di
                  WHERE di.applicant_id = a.applicant_id
                    AND di.is_current = TRUE
                    AND di.document_type IN ('RETIREMENT_401K','RETIREMENT_IRA','RETIREMENT')
                ) AS total_retirement,
                (SELECT COALESCE(SUM(
                          CASE WHEN di.extracted_fields->>'gift_amount' ~ '{self._SAFE_NUM}'
                               THEN (di.extracted_fields->>'gift_amount')::numeric
                               ELSE 0 END), 0)
                   FROM document_index di
                  WHERE di.applicant_id = a.applicant_id
                    AND di.is_current = TRUE
                    AND di.document_type = 'GIFT_LETTER'
                ) AS gift_funds,
                EXISTS (SELECT 1 FROM document_index di
                         WHERE di.applicant_id = a.applicant_id
                           AND di.is_current = TRUE
                           AND di.document_type = 'IDENTITY_DL'
                ) AS dl_verified,
                EXISTS (SELECT 1 FROM document_index di
                         WHERE di.applicant_id = a.applicant_id
                           AND di.is_current = TRUE
                           AND di.document_type IN ('SSN_VALIDATION','IDENTITY_SSN_CARD')
                ) AS ssn_verified,
                EXISTS (SELECT 1 FROM document_index di
                         WHERE di.applicant_id = a.applicant_id
                           AND di.is_current = TRUE
                           AND di.document_type IN ('OFAC_REPORT','OFAC_CHECK')
                ) AS ofac_clear
            FROM applicants a
            LEFT JOIN applications app  ON app.applicant_id    = a.applicant_id
            LEFT JOIN applications app2 ON app2.co_applicant_id = a.applicant_id
            LEFT JOIN income_profiles ip
              ON ip.applicant_id = a.applicant_id AND ip.superseded_by IS NULL
            LEFT JOIN credit_profiles cp
              ON cp.applicant_id = a.applicant_id AND cp.is_current = TRUE
            WHERE ($1::timestamptz IS NULL OR a.updated_at > $1::timestamptz)
            ORDER BY a.updated_at ASC, a.applicant_id ASC
        """
        async for row in db.stream(query, _to_ts(since), prefetch=prefetch):
            yield _row_to_dict(row)

    async def stream_documents(
        self,
        since=None,
        doc_type: Optional[str] = None,
        category: Optional[str] = None,
        prefetch: int = 500,
    ):
        """Every row in ``document_index`` ordered by received_at, with
        optional doc_type / category / since filters."""
        query = """
            SELECT
                document_id,
                applicant_id,
                application_id,
                document_type,
                document_category,
                borrower_role,
                s3_key,
                status,
                received_at,
                expiry_date,
                is_current,
                extracted_fields,
                confidence_score,
                extraction_method
            FROM document_index
            WHERE ($1::timestamptz IS NULL OR received_at > $1::timestamptz)
              AND ($2::text IS NULL OR document_type = $2)
              AND ($3::text IS NULL OR document_category = $3)
            ORDER BY received_at ASC, document_id ASC
        """
        async for row in db.stream(
            query, _to_ts(since), doc_type, category, prefetch=prefetch,
        ):
            yield _row_to_dict(row)

    async def stream_graph_edges(
        self,
        since=None,
        relationship_type: Optional[str] = None,
        prefetch: int = 500,
    ):
        query = """
            SELECT
                relationship_id,
                applicant_id,
                source_doc_id,
                target_doc_id,
                relationship_type,
                field_name,
                source_value,
                target_value,
                delta_pct,
                confidence,
                reasoning,
                created_by,
                created_at
            FROM document_relationships
            WHERE ($1::timestamptz IS NULL OR created_at > $1::timestamptz)
              AND ($2::text IS NULL OR relationship_type = $2)
            ORDER BY created_at ASC, relationship_id ASC
        """
        async for row in db.stream(
            query, _to_ts(since), relationship_type, prefetch=prefetch,
        ):
            yield _row_to_dict(row)

    async def stream_income_profiles(
        self,
        since=None,
        prefetch: int = 500,
    ):
        query = """
            SELECT
                profile_id, applicant_id, application_id,
                assembled_at, profile_data, lineage_hash, version,
                created_at
            FROM income_profiles
            WHERE superseded_by IS NULL
              AND ($1::timestamptz IS NULL OR assembled_at > $1::timestamptz)
            ORDER BY assembled_at ASC, profile_id ASC
        """
        async for row in db.stream(query, _to_ts(since), prefetch=prefetch):
            yield _row_to_dict(row)

    async def stream_credit_profiles(
        self,
        since=None,
        prefetch: int = 500,
    ):
        query = """
            SELECT
                profile_id, applicant_id, mid_score, credit_band,
                profile_data, report_date, expiry_date, created_at
            FROM credit_profiles
            WHERE is_current = TRUE
              AND ($1::timestamptz IS NULL OR created_at > $1::timestamptz)
            ORDER BY created_at ASC, profile_id ASC
        """
        async for row in db.stream(query, _to_ts(since), prefetch=prefetch):
            yield _row_to_dict(row)

    async def stream_applications_export(
        self,
        since=None,
        prefetch: int = 500,
    ):
        """Application-level summary including loan terms, joined to
        the borrower golden record + the latest context_versions
        snapshot for readiness/DTI/LTV. Filter on
        COALESCE(updated_at, created_at) so older rows that pre-date
        the updated_at column still flow through the first export."""
        query = """
            SELECT
                a.application_id, a.applicant_id, a.co_applicant_id,
                a.los_id, a.status, a.loan_amount, a.interest_rate,
                a.loan_term_months, a.loan_purpose, a.loan_type,
                a.occupancy, a.external_loan_id,
                a.created_at, a.updated_at,
                p.full_name  AS borrower_name,
                cp.full_name AS co_borrower_name,
                (SELECT COUNT(*) FROM document_index di
                   WHERE di.is_current = TRUE
                     AND (di.application_id = a.application_id
                          OR di.applicant_id = a.applicant_id
                          OR (a.co_applicant_id IS NOT NULL
                              AND di.applicant_id = a.co_applicant_id))
                ) AS document_count,
                (SELECT COUNT(*) FROM document_relationships dr
                   WHERE dr.relationship_type = 'contradicts'
                     AND (dr.applicant_id = a.applicant_id
                          OR (a.co_applicant_id IS NOT NULL
                              AND dr.applicant_id = a.co_applicant_id))
                ) AS conflict_count,
                (SELECT cv.context_data FROM context_versions cv
                   WHERE cv.application_id = a.application_id
                   ORDER BY cv.assembled_at DESC LIMIT 1
                ) AS context_data
            FROM applications a
            LEFT JOIN applicants p  ON p.applicant_id  = a.applicant_id
            LEFT JOIN applicants cp ON cp.applicant_id = a.co_applicant_id
            WHERE ($1::timestamptz IS NULL
                   OR COALESCE(a.updated_at, a.created_at) > $1::timestamptz)
            ORDER BY COALESCE(a.updated_at, a.created_at) ASC, a.application_id ASC
        """
        async for row in db.stream(query, _to_ts(since), prefetch=prefetch):
            yield _row_to_dict(row)

    # ---- export watermarks (DWH consumer state) ----

    async def get_export_watermark(
        self, consumer: str, table_name: str
    ) -> Optional[dict]:
        row = await db.fetchrow(
            """
            SELECT consumer, table_name, watermark_ts, updated_at
              FROM export_watermarks
             WHERE consumer = $1 AND table_name = $2
            """,
            consumer, table_name,
        )
        return _row_to_dict(row)

    async def upsert_export_watermark(
        self, consumer: str, table_name: str, watermark_ts,
    ) -> dict:
        await db.execute(
            """
            INSERT INTO export_watermarks (consumer, table_name, watermark_ts)
            VALUES ($1, $2, $3::timestamptz)
            ON CONFLICT (consumer, table_name) DO UPDATE SET
                watermark_ts = EXCLUDED.watermark_ts,
                updated_at   = NOW()
            """,
            consumer, table_name, _to_ts(watermark_ts),
        )
        return await self.get_export_watermark(consumer, table_name)

    async def list_export_watermarks(
        self, consumer: Optional[str] = None
    ) -> list:
        if consumer:
            rows = await db.fetch(
                """
                SELECT consumer, table_name, watermark_ts, updated_at
                  FROM export_watermarks
                 WHERE consumer = $1
                 ORDER BY table_name
                """,
                consumer,
            )
        else:
            rows = await db.fetch(
                """
                SELECT consumer, table_name, watermark_ts, updated_at
                  FROM export_watermarks
                 ORDER BY consumer, table_name
                """
            )
        return [_row_to_dict(r) for r in rows]
