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
