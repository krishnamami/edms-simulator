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
-- Incremental knowledge-graph backtest layer.
--
-- The S3 → connector → builder pipeline pulls new docs incrementally
-- via watermarks, indexes them, runs assemblers + reconciler, and
-- updates entity_states *in place* (one row per applicant / property).
-- An EOD scheduler copies the current entity_states into entity_snapshots
-- so a Decision-OS lineage view can replay how an entity evolved day
-- by day. graph_build_runs records every builder execution with the
-- watermark trail for ops + audit.
-- =====================================================================
CREATE TABLE IF NOT EXISTS entity_states (
    entity_id          VARCHAR(100) PRIMARY KEY,
    entity_type        VARCHAR(50) NOT NULL,
    application_id     VARCHAR(100) NOT NULL,
    tenant_id          VARCHAR(50) NOT NULL DEFAULT 'default',
    state              JSONB NOT NULL DEFAULT '{}',
    document_count     INT  NOT NULL DEFAULT 0,
    graph_edge_count   INT  NOT NULL DEFAULT 0,
    conflict_count     INT  NOT NULL DEFAULT 0,
    completeness_pct   FLOAT NOT NULL DEFAULT 0.0,
    last_updated       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_entity_states_app
    ON entity_states(application_id);
CREATE INDEX IF NOT EXISTS idx_entity_states_updated
    ON entity_states(last_updated);
CREATE INDEX IF NOT EXISTS idx_entity_states_tenant
    ON entity_states(tenant_id);
CREATE INDEX IF NOT EXISTS idx_entity_states_type
    ON entity_states(entity_type, tenant_id);

-- One row per (snapshot_date, entity_id) — EOD copy of entity_states.
-- Powers /entity/{id}/timeline and the lineage view.
CREATE TABLE IF NOT EXISTS entity_snapshots (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    snapshot_date      DATE NOT NULL,
    entity_id          VARCHAR(100) NOT NULL,
    entity_type        VARCHAR(50) NOT NULL,
    application_id     VARCHAR(100) NOT NULL,
    tenant_id          VARCHAR(50) NOT NULL DEFAULT 'default',
    state              JSONB NOT NULL,
    document_count     INT  NOT NULL DEFAULT 0,
    graph_edge_count   INT  NOT NULL DEFAULT 0,
    conflict_count     INT  NOT NULL DEFAULT 0,
    completeness_pct   FLOAT NOT NULL DEFAULT 0.0,
    snapshot_taken_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (snapshot_date, entity_id)
);
CREATE INDEX IF NOT EXISTS idx_snapshots_date
    ON entity_snapshots(snapshot_date);
CREATE INDEX IF NOT EXISTS idx_snapshots_entity
    ON entity_snapshots(entity_id, snapshot_date);
CREATE INDEX IF NOT EXISTS idx_snapshots_tenant
    ON entity_snapshots(tenant_id, snapshot_date);

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
-- v3 additions — legacy_ids accumulator on entity rows so a
-- multi-system reconciliation can stitch back to the originating IDs
-- (encompass_loan_number, MERC-RPT-2026-1019, FA-TC-2026-78901, …).
-- ``source_document_id`` + ``source_channel`` on document_index let a
-- downstream consumer trace each row to the system that emitted it.
-- All four ALTERs are additive + idempotent — safe to re-run on every
-- container boot via core.storage.migrations.apply_schema().
-- =====================================================================
ALTER TABLE entity_states
    ADD COLUMN IF NOT EXISTS legacy_ids JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE entity_snapshots
    ADD COLUMN IF NOT EXISTS legacy_ids JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE document_index
    ADD COLUMN IF NOT EXISTS source_document_id VARCHAR(200);
ALTER TABLE document_index
    ADD COLUMN IF NOT EXISTS source_channel VARCHAR(100);
CREATE INDEX IF NOT EXISTS idx_doc_source_channel
    ON document_index(source_channel)
    WHERE source_channel IS NOT NULL;

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

