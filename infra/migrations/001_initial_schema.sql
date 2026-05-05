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
    UNIQUE (source_system, source_id)
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
