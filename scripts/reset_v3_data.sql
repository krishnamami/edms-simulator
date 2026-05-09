-- One-time data reset for a clean v3 simulation run.
--
-- WHY this is a separate script and not in infra/schema.sql:
-- core.storage.migrations.apply_schema() runs schema.sql on EVERY
-- container boot. If we put TRUNCATE / DROP TABLE in there, every
-- ECS rolling deploy (one a session, sometimes more) would wipe
-- production data. Bad.
--
-- Run this manually when you want a clean slate before swapping the
-- connector source to s3_simulation_v3:
--
--   # local docker-compose
--   docker exec edms-simulator-postgres-1 \
--     psql -U edms -d edms -f /tmp/reset_v3_data.sql
--
--   # or pipe in
--   cat scripts/reset_v3_data.sql | \
--     docker exec -i edms-simulator-postgres-1 psql -U edms -d edms
--
--   # production (Aurora) — get a session via the bastion / SSM
--   psql -h <aurora-host> -U <admin> -d edms -f scripts/reset_v3_data.sql
--
-- The order matters because of FK constraints: child tables before
-- parents. CASCADE on the tail cleans up anything we forgot.

BEGIN;

-- Builder-managed tables
TRUNCATE TABLE entity_snapshots CASCADE;
TRUNCATE TABLE entity_states CASCADE;
TRUNCATE TABLE graph_build_runs CASCADE;
TRUNCATE TABLE indexing_watermarks CASCADE;

-- Document graph + index
TRUNCATE TABLE document_relationships CASCADE;
TRUNCATE TABLE document_index CASCADE;

-- Profiles
TRUNCATE TABLE income_profiles CASCADE;
TRUNCATE TABLE credit_profiles CASCADE;

-- Applications + applicants — wipe last because everything else FKs here
TRUNCATE TABLE applications CASCADE;
TRUNCATE TABLE applicant_identity_xref CASCADE;
TRUNCATE TABLE applicants CASCADE;

-- Reset the applicant_id sequence so APL-00001-P starts fresh
ALTER SEQUENCE applicant_sequence RESTART WITH 1;

COMMIT;

-- Sanity: row counts after reset (all should be 0)
SELECT 'entity_states'      AS t, COUNT(*) FROM entity_states
UNION ALL SELECT 'entity_snapshots',     COUNT(*) FROM entity_snapshots
UNION ALL SELECT 'graph_build_runs',     COUNT(*) FROM graph_build_runs
UNION ALL SELECT 'indexing_watermarks',  COUNT(*) FROM indexing_watermarks
UNION ALL SELECT 'document_relationships', COUNT(*) FROM document_relationships
UNION ALL SELECT 'document_index',       COUNT(*) FROM document_index
UNION ALL SELECT 'income_profiles',      COUNT(*) FROM income_profiles
UNION ALL SELECT 'credit_profiles',      COUNT(*) FROM credit_profiles
UNION ALL SELECT 'applications',         COUNT(*) FROM applications
UNION ALL SELECT 'applicants',           COUNT(*) FROM applicants;
