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
    confidence_score  FLOAT,
    -- How the extracted_fields were populated. One of:
    --   'deterministic'  — pymupdf / income / asset / loan / property
    --                      extractor parsed structured fields from the PDF
    --   'caller_supplied' — LOS / API caller sent structured fields directly
    --                      (most production traffic — authoritative source)
    --   'ai_vision'       — Claude Vision fallback extracted from the image
    --                      (lower confidence, used when deterministic returns empty)
    --   'none'            — no fields recovered (placeholder row)
    -- Priority for upsert: deterministic > caller_supplied > ai_vision > none.
    extraction_method VARCHAR DEFAULT 'none'
);
CREATE INDEX IF NOT EXISTS idx_doc_applicant ON document_index(applicant_id)
    WHERE is_current=TRUE;
-- Backfill the new column on pre-existing tables. Idempotent — DO NOTHING
-- if the column already exists (which is the case after a fresh CREATE
-- TABLE above). Ops applies this against prod after merging.
ALTER TABLE document_index
    ADD COLUMN IF NOT EXISTS extraction_method VARCHAR DEFAULT 'none';

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
    -- Phase A premise is that raw payloads can arrive BEFORE the
    -- applicant or application exists in the system, so the audit row
    -- should never be blocked by a missing parent. document_id IS a FK
    -- because it is only set post-extraction, when the row exists.
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

-- =====================================================================
-- Phase B: property layer. Adds the collateral side of the loan —
-- properties + property_profiles. Property docs (appraisal, HOI, flood,
-- tax bill, HOA cert, ...) extract into the profile, which is what the
-- DTI / LTV calculations consume downstream.
-- =====================================================================
CREATE TABLE IF NOT EXISTS properties (
    property_id       VARCHAR PRIMARY KEY,
    application_id    VARCHAR REFERENCES applications(application_id),
    address_line1     VARCHAR NOT NULL,
    address_line2     VARCHAR,
    city              VARCHAR NOT NULL,
    state             VARCHAR(2) NOT NULL,
    zip_code          VARCHAR(10) NOT NULL,
    property_type     VARCHAR NOT NULL
                      CHECK (property_type IN (
                        'single_family','condo','townhouse',
                        'multi_family_2','multi_family_3',
                        'multi_family_4','manufactured','co_op'
                      )),
    units             INT NOT NULL DEFAULT 1,
    year_built        INT,
    sqft              INT,
    status            VARCHAR NOT NULL DEFAULT 'pending'
                      CHECK (status IN (
                        'pending',
                        'active',
                        'conflict',
                        'complete'
                      )),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_property_application
    ON properties(application_id);

CREATE TABLE IF NOT EXISTS property_profiles (
    profile_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    property_id         VARCHAR NOT NULL REFERENCES properties(property_id),
    application_id      VARCHAR REFERENCES applications(application_id),
    appraised_value     NUMERIC,
    appraisal_date      DATE,
    appraisal_type      VARCHAR,
    appraisal_confidence FLOAT,
    estimated_value     NUMERIC,
    tax_assessed_value  NUMERIC,
    annual_taxes        NUMERIC,
    monthly_taxes       NUMERIC,
    hoi_annual          NUMERIC,
    hoi_monthly         NUMERIC,
    flood_zone          VARCHAR,
    flood_insurance_required BOOLEAN DEFAULT FALSE,
    flood_insurance_monthly  NUMERIC,
    hoa_monthly         NUMERIC DEFAULT 0,
    condition_rating    VARCHAR,
    piti_components     JSONB,
    profile_data        JSONB NOT NULL,
    lineage_hash        VARCHAR,
    version             INT NOT NULL DEFAULT 1,
    superseded_by       UUID REFERENCES property_profiles(profile_id),
    assembled_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_prop_profile_property
    ON property_profiles(property_id)
    WHERE superseded_by IS NULL;

ALTER TABLE applications
    ADD COLUMN IF NOT EXISTS property_id VARCHAR
    REFERENCES properties(property_id);

-- =====================================================================
-- Phase E: webhooks + context versioning. Decision OS subscribes to
-- context_updated; every assembly snapshots the full context for audit.
-- =====================================================================
CREATE TABLE IF NOT EXISTS webhooks (
    webhook_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR NOT NULL,
    url             VARCHAR NOT NULL,
    secret          VARCHAR,
    events          JSONB NOT NULL DEFAULT '["context_updated"]',
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    last_triggered  TIMESTAMPTZ,
    failure_count   INT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS webhook_deliveries (
    delivery_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    webhook_id      UUID REFERENCES webhooks(webhook_id),
    event_type      VARCHAR NOT NULL,
    application_id  VARCHAR,
    payload         JSONB NOT NULL,
    response_status INT,
    response_body   TEXT,
    delivered_at    TIMESTAMPTZ DEFAULT NOW(),
    success         BOOLEAN
);

CREATE TABLE IF NOT EXISTS context_versions (
    version_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    application_id  VARCHAR NOT NULL,
    context_data    JSONB NOT NULL,
    assembled_at    TIMESTAMPTZ NOT NULL,
    trigger_event   VARCHAR,
    trigger_doc_id  VARCHAR,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_context_versions_app
    ON context_versions(application_id, assembled_at DESC);
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_webhook
    ON webhook_deliveries(webhook_id, delivered_at DESC);

-- =====================================================================
-- Incremental indexer: watermark per source + per-run audit history.
-- The scheduler reads watermarks.last_indexed_at, scans S3 for files
-- modified after it, processes only those, then advances the watermark.
-- =====================================================================
CREATE TABLE IF NOT EXISTS indexing_watermarks (
    watermark_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source          VARCHAR NOT NULL UNIQUE,
    last_indexed_at TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01',
    last_run_at     TIMESTAMPTZ,
    files_processed INT DEFAULT 0,
    files_skipped   INT DEFAULT 0,
    errors          INT DEFAULT 0,
    status          VARCHAR DEFAULT 'idle'
                    CHECK (status IN ('idle','running','complete','failed')),
    run_duration_ms INT
);

CREATE TABLE IF NOT EXISTS indexing_runs (
    run_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    source          VARCHAR NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    watermark_from  TIMESTAMPTZ NOT NULL,
    watermark_to    TIMESTAMPTZ NOT NULL,
    files_found     INT DEFAULT 0,
    files_processed INT DEFAULT 0,
    files_skipped   INT DEFAULT 0,
    applicants_affected INT DEFAULT 0,
    errors          INT DEFAULT 0,
    error_details   JSONB DEFAULT '[]',
    status          VARCHAR DEFAULT 'running'
);

CREATE INDEX IF NOT EXISTS idx_indexing_runs_source
    ON indexing_runs(source, started_at DESC);

INSERT INTO indexing_watermarks (source, last_indexed_at, status)
VALUES ('s3', '1970-01-01', 'idle')
ON CONFLICT (source) DO NOTHING;

-- =====================================================================
-- Comprehensive document indexing — performance + GIN on extracted_fields
-- =====================================================================

CREATE INDEX IF NOT EXISTS idx_doc_applicant_type
    ON document_index(applicant_id, document_type);

CREATE INDEX IF NOT EXISTS idx_doc_application_category
    ON document_index(application_id, document_category);

CREATE INDEX IF NOT EXISTS idx_doc_type_status
    ON document_index(document_type, status);

CREATE INDEX IF NOT EXISTS idx_doc_received
    ON document_index(received_at DESC);

CREATE INDEX IF NOT EXISTS idx_doc_confidence
    ON document_index(confidence_score DESC)
    WHERE confidence_score IS NOT NULL;

-- GIN index on extracted_fields JSONB for field-level queries.
-- Enables: WHERE extracted_fields @> '{"mid_score": 723}'
CREATE INDEX IF NOT EXISTS idx_doc_extracted_fields_gin
    ON document_index USING gin(extracted_fields)
    WHERE extracted_fields IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_doc_w2_current
    ON document_index(applicant_id, received_at DESC)
    WHERE document_type = 'W2_CURRENT';

CREATE INDEX IF NOT EXISTS idx_doc_credit_report
    ON document_index(applicant_id, received_at DESC)
    WHERE document_type = 'CREDIT_REPORT';

CREATE INDEX IF NOT EXISTS idx_doc_appraisal
    ON document_index(application_id, received_at DESC)
    WHERE document_type IN ('APPRAISAL_URAR','APPRAISAL_UPDATE',
                             'APPRAISAL_DESK','APPRAISAL_FIELD');

CREATE INDEX IF NOT EXISTS idx_doc_property_category
    ON document_index(application_id, received_at DESC)
    WHERE document_category = 'property';

CREATE INDEX IF NOT EXISTS idx_doc_income_category
    ON document_index(applicant_id, received_at DESC)
    WHERE document_category = 'income';

-- Indexes on document_relationships for graph traversal
CREATE INDEX IF NOT EXISTS idx_rel_applicant_type
    ON document_relationships(applicant_id, relationship_type);

CREATE INDEX IF NOT EXISTS idx_rel_field
    ON document_relationships(applicant_id, field_name)
    WHERE field_name IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_rel_conflicts
    ON document_relationships(applicant_id, created_at DESC)
    WHERE relationship_type = 'contradicts';

CREATE INDEX IF NOT EXISTS idx_rel_confirms
    ON document_relationships(applicant_id, confidence DESC)
    WHERE relationship_type = 'confirms';

-- =====================================================================
-- Bulk Export (Interface 3) — DWH consumer watermarks
-- One row per (consumer, table_name) tuple. Consumers like a Snowflake
-- ETL pipeline POST a watermark after a successful pull so the next
-- incremental query starts from where they left off.
-- =====================================================================
CREATE TABLE IF NOT EXISTS export_watermarks (
    watermark_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    consumer       VARCHAR NOT NULL,
    table_name     VARCHAR NOT NULL,
    watermark_ts   TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (consumer, table_name)
);
CREATE INDEX IF NOT EXISTS idx_export_watermarks_consumer
    ON export_watermarks(consumer);
CREATE INDEX IF NOT EXISTS idx_applicants_updated_at
    ON applicants(updated_at);
-- The bulk-export "since" filter on documents/relationships uses the
-- existing received_at / created_at columns. Backfill an explicit index
-- there so a multi-million-row table can answer the incremental query
-- without scanning. (received_at already has idx_doc_received DESC; this
-- is the ASC ordering used by the streaming export.)
CREATE INDEX IF NOT EXISTS idx_doc_received_asc
    ON document_index(received_at);
CREATE INDEX IF NOT EXISTS idx_rel_created_asc
    ON document_relationships(created_at);

-- =====================================================================
-- Multi-tenancy. Every domain table gains a tenant_id column with a
-- 'default' fallback so existing rows survive the migration. Auth
-- resolves the inbound X-API-Key against api_keys, attaches tenant_id
-- to the request, and every Postgres read/write filters/tags by it.
-- The 'default' tenant is the implicit pre-multi-tenant world; new
-- deployments create discrete tenants via POST /admin/tenants.
-- =====================================================================
ALTER TABLE applicants
    ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(50) NOT NULL DEFAULT 'default';
ALTER TABLE applications
    ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(50) NOT NULL DEFAULT 'default';
ALTER TABLE document_index
    ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(50) NOT NULL DEFAULT 'default';
ALTER TABLE document_relationships
    ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(50) NOT NULL DEFAULT 'default';
ALTER TABLE income_profiles
    ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(50) NOT NULL DEFAULT 'default';
ALTER TABLE credit_profiles
    ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(50) NOT NULL DEFAULT 'default';
ALTER TABLE properties
    ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(50) NOT NULL DEFAULT 'default';
ALTER TABLE property_profiles
    ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(50) NOT NULL DEFAULT 'default';
ALTER TABLE export_watermarks
    ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(50) NOT NULL DEFAULT 'default';
ALTER TABLE context_versions
    ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(50) NOT NULL DEFAULT 'default';
ALTER TABLE raw_ingestion
    ADD COLUMN IF NOT EXISTS tenant_id VARCHAR(50) NOT NULL DEFAULT 'default';

-- Composite indexes — the tenant_id filter is the leading column on
-- every analytical / cross-loan query, so it pays off as the first
-- key. Existing single-column indexes stay (point lookups by ID).
CREATE INDEX IF NOT EXISTS idx_applicants_tenant
    ON applicants(tenant_id);
CREATE INDEX IF NOT EXISTS idx_applications_tenant
    ON applications(tenant_id);
CREATE INDEX IF NOT EXISTS idx_document_index_tenant
    ON document_index(tenant_id);
CREATE INDEX IF NOT EXISTS idx_document_relationships_tenant
    ON document_relationships(tenant_id);
CREATE INDEX IF NOT EXISTS idx_income_profiles_tenant
    ON income_profiles(tenant_id);
CREATE INDEX IF NOT EXISTS idx_credit_profiles_tenant
    ON credit_profiles(tenant_id);
CREATE INDEX IF NOT EXISTS idx_properties_tenant
    ON properties(tenant_id);

CREATE TABLE IF NOT EXISTS tenants (
    tenant_id   VARCHAR(50) PRIMARY KEY,
    name        VARCHAR(200) NOT NULL,
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO tenants (tenant_id, name)
VALUES ('default', 'Default Tenant')
ON CONFLICT (tenant_id) DO NOTHING;

-- API keys → tenant binding. ``scopes`` is a comma-separated list — the
-- spec uses 'read,write,admin'. Auth caches the row in Redis for 5 min;
-- ``last_used_at`` is updated best-effort on each request (off the hot
-- path; missed updates are harmless).
CREATE TABLE IF NOT EXISTS api_keys (
    api_key       VARCHAR(64) PRIMARY KEY,
    tenant_id     VARCHAR(50) NOT NULL REFERENCES tenants(tenant_id),
    name          VARCHAR(100),
    scopes        VARCHAR(200) NOT NULL DEFAULT 'read,write',
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_api_keys_tenant ON api_keys(tenant_id);
CREATE INDEX IF NOT EXISTS idx_api_keys_active ON api_keys(is_active) WHERE is_active;

-- Seed the development key with admin scope so the local dev workflow
-- and the existing 329 tests (which all use edms_dev_key) continue to
-- work without modification on a fresh DB.
INSERT INTO api_keys (api_key, tenant_id, name, scopes)
VALUES ('edms_dev_key', 'default', 'Development Key', 'read,write,admin')
ON CONFLICT (api_key) DO NOTHING;

-- =====================================================================
-- v4 — entity_states is THE verified knowledge graph (one row per loan).
--
-- Old shape: 4 rows per loan (borrower / co-borrower / property /
-- loan_terms entities), keyed by entity_id. Mid-flight refactor flips
-- to ONE row per ``application_id`` carrying every borrower (primary
-- + co-borrowers as a JSONB array), property, loan_terms, and a
-- ``verifications`` block with persona-ready summaries. Indexed
-- columns (mid_credit_score, ltv, dti_back, etc.) make workbench
-- queries (``WHERE mid_credit_score >= 700 AND completeness_pct >=
-- 80 AND NOT clear_to_close``) sub-millisecond.
--
-- IMPORTANT: this section must stay idempotent. ``apply_schema()``
-- runs the whole file on every ECS task boot, so any destructive DDL
-- here wipes production on every rolling deploy. To wipe + rebuild
-- entity_states, run ``scripts/reset_v3_data.sql`` manually — that is
-- the supported reset path. The golden-record backfill orchestrator
-- (``POST /admin/rebuild-golden-records``) is the supported way to
-- re-derive rows from document_index + applicants + applications
-- after a wipe.
-- =====================================================================

CREATE TABLE IF NOT EXISTS entity_states (
    application_id            VARCHAR(100) PRIMARY KEY,
    tenant_id                 VARCHAR(50) NOT NULL DEFAULT 'default',
    los_id                    VARCHAR(100),
    legacy_ids                JSONB NOT NULL DEFAULT '{}'::jsonb,

    -- Verified golden record — the structured detail behind the
    -- indexed columns. JSONB so /entity/{id}/state can return rich
    -- nested data without joining 6 tables.
    borrower                  JSONB NOT NULL DEFAULT '{}'::jsonb,
    co_borrowers              JSONB DEFAULT '[]'::jsonb,
    property                  JSONB NOT NULL DEFAULT '{}'::jsonb,
    loan_terms                JSONB NOT NULL DEFAULT '{}'::jsonb,
    verifications             JSONB NOT NULL DEFAULT '{}'::jsonb,

    -- Indexed columns — duplicated from the JSONB so workbench filters
    -- (``WHERE mid_credit_score >= 700 AND ltv < 80 AND …``) hit a
    -- B-tree instead of pumping JSONB through every row.
    mid_credit_score          INT,
    qualifying_monthly        FLOAT,
    co_borrower_qualifying_monthly FLOAT,
    combined_monthly_income   FLOAT,
    total_liquid_assets       FLOAT,
    appraised_value           FLOAT,
    purchase_price            FLOAT,
    loan_amount               FLOAT,
    interest_rate             FLOAT,
    ltv                       FLOAT,
    dti_front                 FLOAT,
    dti_back                  FLOAT,
    piti_monthly              FLOAT,
    monthly_obligations       FLOAT,

    -- Counts
    document_count            INT NOT NULL DEFAULT 0,
    graph_edge_count          INT NOT NULL DEFAULT 0,
    conflict_count            INT NOT NULL DEFAULT 0,
    critical_conflict_count   INT NOT NULL DEFAULT 0,
    completeness_pct          FLOAT NOT NULL DEFAULT 0.0,

    -- Decision tracking
    status                    VARCHAR(50) NOT NULL DEFAULT 'application_received',
    last_decision_by          VARCHAR(100),
    last_decision_at          TIMESTAMPTZ,
    decision_trail            JSONB NOT NULL DEFAULT '[]'::jsonb,

    -- Verification flags — boolean-indexed for fast filtering. Each
    -- one mirrors the ``status`` field of the matching verifications
    -- block; redundant on purpose so SQL can predicate without parsing
    -- JSONB.
    income_verified           BOOLEAN NOT NULL DEFAULT FALSE,
    employment_verified       BOOLEAN NOT NULL DEFAULT FALSE,
    credit_pulled             BOOLEAN NOT NULL DEFAULT FALSE,
    assets_verified           BOOLEAN NOT NULL DEFAULT FALSE,
    identity_complete         BOOLEAN NOT NULL DEFAULT FALSE,
    appraisal_complete        BOOLEAN NOT NULL DEFAULT FALSE,
    title_clear               BOOLEAN NOT NULL DEFAULT FALSE,
    insurance_bound           BOOLEAN NOT NULL DEFAULT FALSE,
    aus_approved              BOOLEAN NOT NULL DEFAULT FALSE,
    rate_locked               BOOLEAN NOT NULL DEFAULT FALSE,
    conditions_cleared        BOOLEAN NOT NULL DEFAULT FALSE,
    clear_to_close            BOOLEAN NOT NULL DEFAULT FALSE,

    last_updated              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at                TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_es_tenant       ON entity_states(tenant_id);
CREATE INDEX IF NOT EXISTS idx_es_los          ON entity_states(los_id);
CREATE INDEX IF NOT EXISTS idx_es_status       ON entity_states(status, tenant_id);
CREATE INDEX IF NOT EXISTS idx_es_updated      ON entity_states(last_updated);
CREATE INDEX IF NOT EXISTS idx_es_score        ON entity_states(mid_credit_score);
CREATE INDEX IF NOT EXISTS idx_es_ltv          ON entity_states(ltv);
CREATE INDEX IF NOT EXISTS idx_es_dti          ON entity_states(dti_back);
CREATE INDEX IF NOT EXISTS idx_es_completeness ON entity_states(completeness_pct);
CREATE INDEX IF NOT EXISTS idx_es_clear        ON entity_states(clear_to_close);

-- One row per (snapshot_date, application_id) — EOD copy of
-- entity_states. Powers /entity/{id}/timeline + lineage replay.
CREATE TABLE IF NOT EXISTS entity_snapshots (
    id                        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_date             DATE NOT NULL,
    application_id            VARCHAR(100) NOT NULL,
    tenant_id                 VARCHAR(50) NOT NULL DEFAULT 'default',
    los_id                    VARCHAR(100),
    legacy_ids                JSONB NOT NULL DEFAULT '{}'::jsonb,
    borrower                  JSONB NOT NULL DEFAULT '{}'::jsonb,
    co_borrowers              JSONB DEFAULT '[]'::jsonb,
    property                  JSONB NOT NULL DEFAULT '{}'::jsonb,
    loan_terms                JSONB NOT NULL DEFAULT '{}'::jsonb,
    verifications             JSONB NOT NULL DEFAULT '{}'::jsonb,
    mid_credit_score          INT,
    qualifying_monthly        FLOAT,
    combined_monthly_income   FLOAT,
    ltv                       FLOAT,
    dti_front                 FLOAT,
    dti_back                  FLOAT,
    document_count            INT NOT NULL DEFAULT 0,
    completeness_pct          FLOAT NOT NULL DEFAULT 0.0,
    status                    VARCHAR(50),
    income_verified           BOOLEAN,
    credit_pulled             BOOLEAN,
    appraisal_complete        BOOLEAN,
    title_clear               BOOLEAN,
    clear_to_close            BOOLEAN,
    snapshot_taken_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (snapshot_date, application_id)
);
CREATE INDEX IF NOT EXISTS idx_snap_date  ON entity_snapshots(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_snap_app   ON entity_snapshots(application_id, snapshot_date);
CREATE INDEX IF NOT EXISTS idx_snap_tenant ON entity_snapshots(tenant_id, snapshot_date);

-- Append-only event log for state-level changes (status flips,
-- new doc-driven field updates, condition cleared, etc.). The builder
-- writes these alongside the upsert so /entity/{id}/events surfaces a
-- granular history without diff-scanning snapshots.
CREATE TABLE IF NOT EXISTS entity_state_events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    application_id  VARCHAR(100) NOT NULL,
    tenant_id       VARCHAR(50) NOT NULL DEFAULT 'default',
    event_type      VARCHAR(50) NOT NULL,
    field_path      VARCHAR(200),
    old_value       JSONB,
    new_value       JSONB,
    triggered_by    VARCHAR(200),
    document_id     VARCHAR(200),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ese_app
    ON entity_state_events(application_id, created_at);

-- One row per builder execution. The watermark trail
-- (watermark_from → watermark_to) shows where the incremental pull
-- advanced on each tick; entities_updated / edges_created / docs_pulled
-- give the ops dashboard a per-build delta.
CREATE TABLE IF NOT EXISTS graph_build_runs (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id          VARCHAR(50) NOT NULL DEFAULT 'default',
    build_date         DATE NOT NULL,
    build_number       INT NOT NULL,
    watermark_from     TIMESTAMPTZ,
    watermark_to       TIMESTAMPTZ,
    documents_pulled   INT NOT NULL DEFAULT 0,
    documents_new      INT NOT NULL DEFAULT 0,
    documents_skipped  INT NOT NULL DEFAULT 0,
    entities_updated   INT NOT NULL DEFAULT 0,
    edges_created      INT NOT NULL DEFAULT 0,
    duration_ms        INT NOT NULL DEFAULT 0,
    status             VARCHAR(20) NOT NULL DEFAULT 'completed',
    error_details      TEXT,
    started_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at       TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_graph_build_runs_date
    ON graph_build_runs(build_date, build_number);
CREATE INDEX IF NOT EXISTS idx_graph_build_runs_tenant
    ON graph_build_runs(tenant_id, build_date DESC);

-- =====================================================================
-- v3 additions — source_document_id + source_channel on document_index
-- so a downstream consumer can trace each row back to the system that
-- emitted it (ADP-W2-2025-4567, MERC-RPT-2026-1019, FA-TC-2026-78901,
-- ENC-2026-12345, …). Idempotent ALTERs — safe to re-run on every
-- container boot via core.storage.migrations.apply_schema().
-- =====================================================================
ALTER TABLE document_index
    ADD COLUMN IF NOT EXISTS source_document_id VARCHAR(200);
ALTER TABLE document_index
    ADD COLUMN IF NOT EXISTS source_channel VARCHAR(100);
CREATE INDEX IF NOT EXISTS idx_doc_source_channel
    ON document_index(source_channel)
    WHERE source_channel IS NOT NULL;

-- =====================================================================
-- v4.5 — golden_record_backfill_state. Singleton row (per tenant)
-- tracking restartable progress of POST /admin/rebuild-golden-records.
-- ``last_completed_application_id`` is the cursor: on restart the
-- orchestrator does ``WHERE application_id > $last_completed`` and
-- resumes. Errors accumulate in a JSONB array so the operator can see
-- which applications crashed without scrolling CloudWatch.
-- =====================================================================
CREATE TABLE IF NOT EXISTS golden_record_backfill_state (
    tenant_id                      VARCHAR(50)  PRIMARY KEY DEFAULT 'default',
    last_completed_application_id  VARCHAR(200),
    completed_count                INT          NOT NULL DEFAULT 0,
    total_count                    INT          NOT NULL DEFAULT 0,
    status                         VARCHAR(20)  NOT NULL DEFAULT 'not_started',
    errors                         JSONB        NOT NULL DEFAULT '[]'::jsonb,
    started_at                     TIMESTAMPTZ,
    updated_at                     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    completed_at                   TIMESTAMPTZ
);

-- =====================================================================
-- v4 additions — applications tracks stated (from URLA) vs verified
-- (from documents) values side by side, so /application/{id}/context
-- can surface "stated $125k income, verified $123k via W-2" without
-- joining 5 tables. Reconciler edges now carry application_id so a
-- workbench query can scope edges to a single loan.
-- =====================================================================
ALTER TABLE applications
    ADD COLUMN IF NOT EXISTS stated_income           FLOAT,
    ADD COLUMN IF NOT EXISTS verified_income         FLOAT,
    ADD COLUMN IF NOT EXISTS stated_property_value   FLOAT,
    ADD COLUMN IF NOT EXISTS verified_property_value FLOAT,
    ADD COLUMN IF NOT EXISTS stated_assets           FLOAT,
    ADD COLUMN IF NOT EXISTS verified_assets         FLOAT,
    ADD COLUMN IF NOT EXISTS stated_employer         VARCHAR(200),
    ADD COLUMN IF NOT EXISTS verified_employer       VARCHAR(200);

ALTER TABLE document_relationships
    ADD COLUMN IF NOT EXISTS application_id VARCHAR(100);
CREATE INDEX IF NOT EXISTS idx_dr_app
    ON document_relationships(application_id);

-- v4.1 — Gap-fixes 6, 8, 10: mortgage-insurance + combined LTV +
-- existing-mortgage-payment indexed columns. ``mi_monthly`` lifts the
-- monthly MI premium out of the JSONB so DTI / PITI workbench filters
-- can predicate on it. ``cltv`` is loan_amount + subordinate liens /
-- property_value × 100 — the metric secondary mortgages are bound by.
-- ``existing_mortgage_payment`` matters on refis where the borrower's
-- current mortgage payment is being replaced; the credit report
-- already counts it as a monthly obligation, so this column is for
-- workbench display + audit.
ALTER TABLE entity_states
    ADD COLUMN IF NOT EXISTS mi_monthly                FLOAT,
    ADD COLUMN IF NOT EXISTS cltv                      FLOAT,
    ADD COLUMN IF NOT EXISTS existing_mortgage_payment FLOAT;

-- v4.6 — Group 9 (Management) timing columns. ``days_in_current_status``
-- is "how long has status been at its current value" (carried forward
-- across re-builds when status doesn't change). ``loan_age_days`` is
-- "how long since the application was created" — pure ``NOW() -
-- applications.created_at``. Both are recomputed on every entity_states
-- upsert by the golden-record builder.
ALTER TABLE entity_states
    ADD COLUMN IF NOT EXISTS days_in_current_status INT,
    ADD COLUMN IF NOT EXISTS loan_age_days          INT;

-- =====================================================================
-- Webhook outbox — async delivery decouples upload latency from
-- subscriber availability. Every assembly fan-out writes a row here;
-- a background worker (core/webhooks/delivery_worker.py) polls
-- ``status='pending' AND next_retry_at <= NOW()`` and POSTs.
-- Backoff: 2^attempts * 30s; cap at max_attempts then status='failed'.
-- =====================================================================
CREATE TABLE IF NOT EXISTS webhook_outbox (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       VARCHAR(50) NOT NULL DEFAULT 'default',
    webhook_id      UUID NOT NULL REFERENCES webhooks(webhook_id) ON DELETE CASCADE,
    event_type      VARCHAR(50) NOT NULL,
    application_id  VARCHAR,
    payload         JSONB NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending','delivered','failed')),
    attempts        INT NOT NULL DEFAULT 0,
    max_attempts    INT NOT NULL DEFAULT 3,
    next_retry_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivered_at    TIMESTAMPTZ
);

-- Worker query — pending rows whose next_retry_at has elapsed,
-- ordered by created_at to drain FIFO. Partial index keeps it tiny:
-- delivered rows fall out of the index immediately.
CREATE INDEX IF NOT EXISTS idx_outbox_pending
    ON webhook_outbox(next_retry_at, created_at)
    WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_outbox_tenant
    ON webhook_outbox(tenant_id);
CREATE INDEX IF NOT EXISTS idx_outbox_webhook
    ON webhook_outbox(webhook_id, status, created_at DESC);

-- =====================================================================
-- Decision OS persona context views (read-only).
--
-- Decision OS evaluates 13 boundary-rule personas (Wave 1-5) against
-- entity_states JSONB. Rather than make every persona walk the same
-- JSONB paths and casts at query time, we flatten the fields each
-- persona reads into a dedicated view. Decision OS issues plain
-- ``SELECT ... FROM vw_<persona>_context WHERE application_id = $1``
-- and gets a stable typed row back — no JSONB knowledge required.
--
-- Why CREATE OR REPLACE VIEW:
--   ``core.storage.migrations.apply_schema`` runs this file on every
--   ECS task boot. ``CREATE OR REPLACE`` is idempotent on identical
--   column shapes — re-runs are free. If a future change adds or
--   removes columns, drop the affected view first (Postgres rejects
--   shape changes via REPLACE).
--
-- All views read entity_states only — no joins, no extra tables. The
-- JSONB path expressions return NULL when a key is missing, which is
-- the correct null-as-unknown signal for downstream persona logic.
-- =====================================================================

-- View 1 — credit_assessment (Wave 1, auto_execute, medium risk)
CREATE OR REPLACE VIEW vw_credit_assessment_context AS
SELECT
    application_id,
    tenant_id,
    borrower->>'applicant_id'                                       AS applicant_id,
    (borrower->'credit'->>'mid_score')::int                         AS credit_score,
    borrower->'credit'->>'credit_band'                              AS credit_band,
    (borrower->'credit'->>'equifax_score')::int                     AS equifax_score,
    (borrower->'credit'->>'experian_score')::int                    AS experian_score,
    (borrower->'credit'->>'transunion_score')::int                  AS transunion_score,
    (borrower->'credit'->>'active_bankruptcy')::boolean             AS active_bankruptcy,
    (borrower->'credit'->>'foreclosure_last_36_months')::boolean    AS foreclosure_last_36_months,
    (borrower->'credit'->>'thin_file')::boolean                     AS thin_file,
    (borrower->'credit'->>'no_derogatory_last_24_months')::boolean  AS no_derogatory_last_24_months,
    (borrower->'credit'->>'derogatory_marks')::int                  AS derogatory_marks,
    (borrower->'credit'->>'open_tradelines')::int                   AS open_tradelines,
    (borrower->'credit'->>'credit_utilization')::float              AS credit_utilization,
    (borrower->'credit'->>'monthly_obligations')::float             AS monthly_obligations,
    status,
    completeness_pct
FROM entity_states;

-- View 2 — fraud_screening (Wave 1, auto_execute, high risk)
CREATE OR REPLACE VIEW vw_fraud_screening_context AS
SELECT
    application_id,
    tenant_id,
    borrower->>'applicant_id'                                       AS applicant_id,
    (borrower->'identity'->>'fraud_score')::float                   AS fraud_score,
    (borrower->'identity'->>'identity_match_confidence')::float     AS identity_match_confidence,
    (borrower->'identity'->>'document_authenticity_score')::float   AS document_authenticity_score,
    (borrower->'identity'->>'watchlist_match')::boolean             AS watchlist_match,
    (borrower->'identity'->>'synthetic_identity_flag')::boolean     AS synthetic_identity_flag,
    status
FROM entity_states;

-- View 3 — compliance_check (Wave 1, human_approval, high risk)
CREATE OR REPLACE VIEW vw_compliance_check_context AS
SELECT
    application_id,
    tenant_id,
    (verifications->>'hmda_complete')::boolean                      AS all_hmda_fields_complete,
    (verifications->>'no_fair_lending_flags')::boolean              AS no_fair_lending_flags,
    (verifications->>'state_rules_passed')::boolean                 AS state_rules_passed,
    (verifications->>'fair_lending_violation')::boolean             AS fair_lending_violation,
    (verifications->>'missing_required_disclosures')::boolean       AS missing_required_disclosures,
    (verifications->>'regulatory_ambiguity')::boolean               AS regulatory_ambiguity,
    (verifications->>'mixed_jurisdiction')::boolean                 AS mixed_jurisdiction,
    (verifications->>'minor_data_gap')::boolean                     AS minor_data_gap,
    completeness_pct,
    status
FROM entity_states;

-- View 4 — employment_reconciliation (Wave 1, recommend, medium risk)
CREATE OR REPLACE VIEW vw_employment_reconciliation_context AS
SELECT
    application_id,
    tenant_id,
    borrower->>'applicant_id'                                       AS applicant_id,
    borrower->'employment'->>'reconciliation_status'                AS reconciliation_status,
    (borrower->'employment'->>'continuity_coverage_pct')::float     AS continuity_coverage_pct,
    (borrower->'employment'->>'max_gap_days')::int                  AS max_gap_days,
    (borrower->'employment'->>'employer_name_match_confidence')::float AS employer_name_match_confidence,
    (borrower->'employment'->>'stated_vs_verified_drift_pct')::float   AS stated_vs_verified_drift_pct,
    (borrower->'employment'->>'employer_on_watchlist')::boolean     AS employer_on_watchlist,
    borrower->'employment'->>'employer_name'                        AS employer_name,
    borrower->'employment'->>'period_start'                         AS period_start,
    borrower->'employment'->>'period_end'                           AS period_end,
    borrower->'employment'->>'employment_status'                    AS employment_status,
    (borrower->'employment'->>'income_amount')::float               AS gross_amount,
    borrower->'income'->>'stated_employer'                          AS stated_employer,
    (borrower->'income'->>'stated_income_annual')::float            AS stated_income,
    status
FROM entity_states;

-- View 5 — income_verification (Wave 2, human_approval, medium risk)
CREATE OR REPLACE VIEW vw_income_verification_context AS
SELECT
    application_id,
    tenant_id,
    borrower->>'applicant_id'                                       AS applicant_id,
    (borrower->'income'->>'income_confidence_score')::float         AS income_confidence_score,
    borrower->'income'->>'employment_type'                          AS employment_type,
    (borrower->'income'->>'payroll_verified')::boolean              AS payroll_verified,
    borrower->'employment'->>'reconciliation_status'                AS reconciliation_status,
    (borrower->'income'->>'income_discrepancy_pct')::float          AS income_discrepancy_pct,
    (borrower->'income'->>'stated_income_annual')::float            AS stated_income,
    (borrower->'income'->>'verified_income_annual')::float          AS verified_income,
    (borrower->'income'->>'multiple_income_sources')::boolean       AS multiple_income_sources,
    borrower->'income'->>'income_stability'                         AS income_stability,
    borrower->'income'->>'income_trending'                          AS income_trending,
    (borrower->'income'->>'overall_confidence')::float              AS overall_confidence,
    status
FROM entity_states;

-- View 6 — dti_calculation (Wave 2). Indexed columns are pulled
-- directly; JSONB only for income_confidence + monthly_obligations.
CREATE OR REPLACE VIEW vw_dti_calculation_context AS
SELECT
    application_id,
    tenant_id,
    dti_back                                                        AS dti,
    dti_front,
    (borrower->'credit'->>'monthly_obligations')::float             AS existing_debt_obligations,
    piti_monthly                                                    AS proposed_payment,
    qualifying_monthly,
    combined_monthly_income,
    (borrower->'income'->>'overall_confidence')::float              AS income_confidence,
    loan_amount,
    interest_rate,
    status
FROM entity_states;

-- View 7 — ltv_assessment (Wave 2). title_status is NULL today; left
-- in the projection so Decision OS doesn't have to special-case once
-- the title block grows that field.
CREATE OR REPLACE VIEW vw_ltv_assessment_context AS
SELECT
    application_id,
    tenant_id,
    ltv,
    appraised_value,
    purchase_price,
    loan_amount,
    (property->>'down_payment')::float                              AS down_payment,
    (property->>'appraisal_disputed')::boolean                      AS appraisal_disputed,
    property->'title'->>'title_status'                              AS title_status,
    (property->>'lien_dispute')::boolean                            AS lien_dispute,
    borrower->'credit'->>'credit_band'                              AS credit_band,
    status
FROM entity_states;

-- View 8 — product_eligibility (Wave 3)
CREATE OR REPLACE VIEW vw_product_eligibility_context AS
SELECT
    application_id,
    tenant_id,
    dti_back                                                        AS dti_ratio,
    ltv                                                             AS ltv_ratio,
    borrower->'credit'->>'credit_band'                              AS credit_band,
    mid_credit_score                                                AS credit_score,
    loan_terms->>'loan_type'                                        AS loan_type,
    loan_amount,
    loan_terms->'urla'->>'loan_purpose'                             AS loan_purpose,
    status
FROM entity_states;

-- View 9 — rate_pricing (Wave 3)
CREATE OR REPLACE VIEW vw_rate_pricing_context AS
SELECT
    application_id,
    tenant_id,
    mid_credit_score                                                AS credit_score,
    dti_back                                                        AS dti_ratio,
    ltv                                                             AS ltv_ratio,
    interest_rate,
    loan_terms->>'loan_type'                                        AS loan_type,
    (loan_terms->>'rate_within_normal_band')::boolean               AS rate_within_normal_band,
    (loan_terms->>'no_manual_adjustments')::boolean                 AS no_manual_adjustments_required,
    (loan_terms->>'rate_exceeds_usury')::boolean                    AS rate_exceeds_usury_limit,
    (loan_terms->>'concurrent_rate_lock_conflict')::boolean         AS concurrent_rate_lock_conflict,
    (loan_terms->>'llpa_adjustment')::float                         AS llpa_adjustment,
    loan_terms->'rate_lock'->>'loan_program'                        AS loan_program,
    status
FROM entity_states;

-- View 10 — underwriting_decision (Wave 4). Reads EVERYTHING — every
-- JSONB block + every indexed column. Persona makes a final
-- ALLOW/BLOCK/ESCALATE call by re-evaluating the file.
CREATE OR REPLACE VIEW vw_underwriting_decision_context AS
SELECT
    application_id,
    tenant_id,
    borrower,
    co_borrowers,
    property,
    loan_terms,
    verifications,
    mid_credit_score,
    ltv,
    dti_back,
    dti_front,
    piti_monthly,
    qualifying_monthly,
    combined_monthly_income,
    total_liquid_assets,
    loan_amount,
    interest_rate,
    appraised_value,
    purchase_price,
    completeness_pct,
    status
FROM entity_states;

-- View 11 — approval_routing (Wave 5). Thin slice — routing decides
-- which queue / approver the application lands in, so it just needs
-- the high-level status + completeness.
CREATE OR REPLACE VIEW vw_approval_routing_context AS
SELECT
    application_id,
    tenant_id,
    borrower->>'applicant_id'                                       AS applicant_id,
    status,
    completeness_pct
FROM entity_states;

-- View 12 — closing_readiness (Wave 5, human_approval, high risk)
CREATE OR REPLACE VIEW vw_closing_readiness_context AS
SELECT
    application_id,
    tenant_id,
    (verifications->>'conditions_cleared')::boolean                 AS all_conditions_cleared,
    (verifications->>'cd_timing_compliant')::boolean                AS cd_timing_compliant,
    (verifications->>'title_clear')::boolean                        AS title_clear,
    (verifications->>'cd_timing_violation')::boolean                AS cd_timing_violation,
    (property->>'title_defect')::boolean                            AS title_defect,
    (property->>'lien_dispute')::boolean                            AS lien_dispute,
    (property->>'insurance_gap')::boolean                           AS insurance_gap,
    (verifications->>'insurance_bound')::boolean                    AS insurance_binder,
    loan_terms->>'cd_sent_at'                                       AS closing_disclosure_sent_at,
    (loan_terms->>'days_until_rate_lock_expiry')::int               AS days_until_rate_lock_expiry,
    status
FROM entity_states;
