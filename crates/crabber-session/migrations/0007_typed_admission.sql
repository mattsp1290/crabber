-- Expand only the private codec carrier constraints; no retained payload is
-- parsed, rewritten, or made executable by migration.
ALTER TABLE run_execution_bindings DROP CONSTRAINT IF EXISTS run_execution_bindings_capsule_version_check;
ALTER TABLE run_execution_bindings ADD CONSTRAINT run_execution_bindings_capsule_version_check CHECK (capsule_version IN (2,3));
ALTER TABLE admission_executions DROP CONSTRAINT IF EXISTS admission_executions_capsule_version_check;
ALTER TABLE admission_executions ADD CONSTRAINT admission_executions_capsule_version_check CHECK (capsule_version IN (1,2));
-- Typed envelopes may contain lossless arbitrary JSON state/context. Their
-- private original evidence uses TEXT instead of PostgreSQL JSONB.
ALTER TABLE admission_executions ADD COLUMN IF NOT EXISTS data_json text;
INSERT INTO schema_version(version) VALUES (7) ON CONFLICT DO NOTHING;
