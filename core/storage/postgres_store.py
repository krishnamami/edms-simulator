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
        """Insert a new income profile version, marking the prior current version superseded."""
        applicant_id = profile["applicant_id"]
        current = await db.fetchrow(
            """
            SELECT profile_id, version
            FROM income_profiles
            WHERE applicant_id = $1 AND superseded_by IS NULL
            """,
            applicant_id,
        )
        version = (current["version"] + 1) if current else 1

        new_id = await db.fetchval(
            """
            INSERT INTO income_profiles (
                applicant_id, application_id, assembled_at, profile_data,
                lineage_hash, version
            ) VALUES ($1, $2, $3::timestamptz, $4::jsonb, $5, $6)
            RETURNING profile_id
            """,
            applicant_id,
            profile.get("application_id"),
            _to_ts(profile["assembled_at"]),
            _to_jsonb(profile),
            profile.get("lineage_hash", ""),
            version,
        )
        if current:
            await db.execute(
                "UPDATE income_profiles SET superseded_by = $1 WHERE profile_id = $2",
                new_id,
                current["profile_id"],
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
        applicant_id = profile["applicant_id"]
        await db.execute(
            "UPDATE credit_profiles SET is_current = FALSE WHERE applicant_id = $1 AND is_current = TRUE",
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
        await db.execute(
            """
            INSERT INTO document_index (
                document_id, applicant_id, application_id, document_type,
                document_category, borrower_role, s3_key, status,
                expiry_date, is_current, extracted_fields, confidence_score
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                $9::date, $10, $11::jsonb, $12
            )
            ON CONFLICT (document_id) DO UPDATE SET
                document_type     = EXCLUDED.document_type,
                document_category = EXCLUDED.document_category,
                s3_key            = EXCLUDED.s3_key,
                status            = EXCLUDED.status,
                expiry_date       = EXCLUDED.expiry_date,
                is_current        = EXCLUDED.is_current,
                extracted_fields  = EXCLUDED.extracted_fields,
                confidence_score  = EXCLUDED.confidence_score
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
        )

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
        return {
            "applicant_id":       applicant_id,
            "document_count":     len(docs),
            "relationship_count": len(rels),
            "confirmation_count": len(confirms),
            "conflict_count":     len(conflicts),
            "requires_review":    len(conflicts) > 0,
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
