-- EDMS Simulator production schema (Aurora Postgres 15 / local Postgres 15)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SEQUENCE IF NOT EXISTS applicant_sequence START 1;

CREATE TABLE IF NOT EXISTS applicants (
    applicant_id    VARCHAR PRIMARY KEY,
    full_name       VARCHAR NOT NULL,
    first_name      VARCHAR NOT NULL,
    last_name       VARCHAR NOT NULL,
    dob             DATE NOT NULL,
    ssn_hash        VARCHAR NOT NULL,
    ssn_last4       VARCHAR(4),
    email           VARCHAR,
    phone           VARCHAR,
    address_current JSONB,
    status          VARCHAR NOT NULL DEFAULT 'placeholder'
                    CHECK (status IN ('placeholder','resolving','active','stale','conflict','error')),
    identity_xrefs  JSONB NOT NULL DEFAULT '[]',
    application_ids JSONB NOT NULL DEFAULT '[]',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_applicant_ssn ON applicants(ssn_hash);
CREATE INDEX IF NOT EXISTS idx_applicant_name_dob ON applicants(LOWER(last_name), dob);

CREATE TABLE IF NOT EXISTS applicant_identity_xref (
    xref_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    applicant_id     VARCHAR NOT NULL REFERENCES applicants(applicant_id),
    source_system    VARCHAR NOT NULL,
    source_id        VARCHAR NOT NULL,
    match_confidence FLOAT NOT NULL,
    match_method     VARCHAR NOT NULL,
    added_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (applicant_id, source_system, source_id)
);

CREATE TABLE IF NOT EXISTS applications (
    application_id  VARCHAR PRIMARY KEY,
    applicant_id    VARCHAR NOT NULL REFERENCES applicants(applicant_id),
    co_applicant_id VARCHAR REFERENCES applicants(applicant_id),
    los_id          VARCHAR NOT NULL,
    status          VARCHAR NOT NULL DEFAULT 'active',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_app_los ON applications(los_id);

CREATE TABLE IF NOT EXISTS income_profiles (
    profile_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    applicant_id   VARCHAR NOT NULL REFERENCES applicants(applicant_id),
    application_id VARCHAR REFERENCES applications(application_id),
    assembled_at   TIMESTAMPTZ NOT NULL,
    profile_data   JSONB NOT NULL,
    lineage_hash   VARCHAR NOT NULL,
    version        INT NOT NULL DEFAULT 1,
    superseded_by  UUID REFERENCES income_profiles(profile_id),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_income_active ON income_profiles(applicant_id)
    WHERE superseded_by IS NULL;

CREATE TABLE IF NOT EXISTS credit_profiles (
    profile_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    applicant_id VARCHAR NOT NULL REFERENCES applicants(applicant_id),
    mid_score    INT NOT NULL,
    credit_band  VARCHAR NOT NULL,
    profile_data JSONB NOT NULL,
    report_date  DATE,
    expiry_date  DATE,
    is_current   BOOLEAN NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_credit_current ON credit_profiles(applicant_id)
    WHERE is_current=TRUE;

CREATE TABLE IF NOT EXISTS document_index (
    document_id       VARCHAR PRIMARY KEY,
    applicant_id      VARCHAR NOT NULL REFERENCES applicants(applicant_id),
    application_id    VARCHAR REFERENCES applications(application_id),
    document_type     VARCHAR NOT NULL,
    document_category VARCHAR NOT NULL,
    borrower_role     VARCHAR NOT NULL,
    s3_key            VARCHAR,
    status            VARCHAR NOT NULL DEFAULT 'received',
    received_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expiry_date       DATE,
    is_current        BOOLEAN NOT NULL DEFAULT TRUE,
    extracted_fields  JSONB,
    confidence_score  FLOAT
);
CREATE INDEX IF NOT EXISTS idx_doc_applicant ON document_index(applicant_id)
    WHERE is_current=TRUE;

-- Document knowledge graph
CREATE TABLE IF NOT EXISTS document_relationships (
    relationship_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    applicant_id      VARCHAR NOT NULL REFERENCES applicants(applicant_id),
    source_doc_id     VARCHAR NOT NULL REFERENCES document_index(document_id),
    target_doc_id     VARCHAR NOT NULL REFERENCES document_index(document_id),
    relationship_type VARCHAR NOT NULL
                      CHECK (relationship_type IN (
                        'confirms','contradicts','supersedes',
                        'references','corroborates'
                      )),
    field_name        VARCHAR,
    source_value      JSONB,
    target_value      JSONB,
    delta_pct         FLOAT,
    confidence        FLOAT NOT NULL,
    reasoning         TEXT,
    created_by        VARCHAR NOT NULL DEFAULT 'reconciler',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rel_applicant
    ON document_relationships(applicant_id);
CREATE INDEX IF NOT EXISTS idx_rel_type
    ON document_relationships(relationship_type, applicant_id);
CREATE INDEX IF NOT EXISTS idx_rel_source
    ON document_relationships(source_doc_id);
CREATE INDEX IF NOT EXISTS idx_rel_target
    ON document_relationships(target_doc_id);

CREATE OR REPLACE VIEW document_graph AS
SELECT
    r.applicant_id,
    r.relationship_id,
    r.relationship_type,
    r.field_name,
    r.source_value,
    r.target_value,
    r.delta_pct,
    r.confidence,
    r.reasoning,
    s.document_type     AS source_type,
    s.document_category AS source_category,
    t.document_type     AS target_type,
    t.document_category AS target_category,
    r.created_at
FROM document_relationships r
JOIN document_index s ON r.source_doc_id = s.document_id
JOIN document_index t ON r.target_doc_id = t.document_id;

-- =====================================================================
-- Phase 0: MISMO compatibility — external IDs + LOS / type registries
-- =====================================================================

-- External-system IDs on applicants. JSONB blob like
-- {"encompass": "CONTACT-12345", "mismo_party": "P-001-A"}.
ALTER TABLE applicants
  ADD COLUMN IF NOT EXISTS external_ids JSONB NOT NULL DEFAULT '{}';

CREATE INDEX IF NOT EXISTS idx_applicant_external_ids
  ON applicants USING gin(external_ids);

-- External loan identifier and the URLA / HMDA / loan-terms surface on
-- applications. All optional — older rows stay valid.
ALTER TABLE applications
  ADD COLUMN IF NOT EXISTS external_loan_id VARCHAR,
  ADD COLUMN IF NOT EXISTS loan_type        VARCHAR,
  ADD COLUMN IF NOT EXISTS loan_purpose     VARCHAR
    CHECK (loan_purpose IN (
      'purchase','refinance_rate_term','refinance_cash_out',
      'construction','home_equity','reverse','other'
    )),
  ADD COLUMN IF NOT EXISTS occupancy        VARCHAR
    CHECK (occupancy IN (
      'primary_residence','second_home','investment_property'
    )),
  ADD COLUMN IF NOT EXISTS loan_amount      NUMERIC,
  ADD COLUMN IF NOT EXISTS interest_rate    NUMERIC,
  ADD COLUMN IF NOT EXISTS loan_term_months INT DEFAULT 360,
  ADD COLUMN IF NOT EXISTS urla_fields      JSONB DEFAULT '{}',
  ADD COLUMN IF NOT EXISTS hmda_fields      JSONB DEFAULT '{}',
  ADD COLUMN IF NOT EXISTS updated_at       TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_app_external_loan
  ON applications(external_loan_id)
  WHERE external_loan_id IS NOT NULL;

-- MISMO 3.4 doc-type registry. Maps external LOS doc type codes to
-- internal canonical types (W2_CURRENT, etc.) so any new LOS can be
-- onboarded without code changes — just inserts here.
CREATE TABLE IF NOT EXISTS mismo_doc_type_registry (
    registry_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source_system     VARCHAR NOT NULL,
    external_type     VARCHAR NOT NULL,
    internal_type     VARCHAR NOT NULL,
    mismo_type        VARCHAR,
    field_mapping     JSONB DEFAULT '{}',
    confidence_weight FLOAT DEFAULT 1.0,
    is_active         BOOLEAN DEFAULT TRUE,
    created_at        TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (source_system, external_type)
);

CREATE INDEX IF NOT EXISTS idx_mismo_source
  ON mismo_doc_type_registry(source_system, is_active);

-- LOS connector registry. Useful for surfacing which systems are wired.
CREATE TABLE IF NOT EXISTS los_connectors (
    connector_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR NOT NULL UNIQUE,
    display_name    VARCHAR NOT NULL,
    base_url        VARCHAR,
    auth_type       VARCHAR DEFAULT 'api_key',
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- =====================================================================
-- Phase A: raw storage layer. Every inbound payload is persisted to
-- S3 + raw_ingestion BEFORE extraction so re-extraction is always
-- possible and the audit trail starts at the first byte received.
-- =====================================================================
CREATE TABLE IF NOT EXISTS raw_ingestion (
    ingest_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- applicant_id / application_id are intentionally NOT foreign keys.
    -- Phase A's premise is that raw payloads can arrive BEFORE the
    -- applicant or application exists in the system; the audit row should
    -- never be blocked by a missing parent. document_id IS a FK because
    -- it's only set post-extraction, when the row exists.
    applicant_id      VARCHAR,
    application_id    VARCHAR,
    source_channel    VARCHAR NOT NULL,
    raw_s3_key        VARCHAR,
    raw_payload_type  VARCHAR NOT NULL,
    raw_size_bytes    INTEGER,
    filename          VARCHAR,
    mime_type         VARCHAR,
    status            VARCHAR NOT NULL DEFAULT 'received'
                      CHECK (status IN (
                        'received',
                        'extracting',
                        'indexed',
                        'failed',
                        'reprocessing'
                      )),
    document_id       VARCHAR REFERENCES document_index(document_id),
    extracted_at      TIMESTAMPTZ,
    extraction_error  TEXT,
    received_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_applicant
    ON raw_ingestion(applicant_id);
CREATE INDEX IF NOT EXISTS idx_raw_status
    ON raw_ingestion(status);
CREATE INDEX IF NOT EXISTS idx_raw_channel
    ON raw_ingestion(source_channel, received_at DESC);
CREATE INDEX IF NOT EXISTS idx_raw_application
    ON raw_ingestion(application_id);

