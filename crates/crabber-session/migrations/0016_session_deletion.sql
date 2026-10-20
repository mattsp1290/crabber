-- Minimal permanent erasure identity; deliberately has no session/run FK.
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS ownership_protocol smallint NOT NULL DEFAULT 0 CHECK(ownership_protocol IN (0,1));
CREATE UNIQUE INDEX IF NOT EXISTS sessions_ownership_scope ON sessions(id,ownership_protocol);
CREATE TABLE IF NOT EXISTS session_ownership_origins (
 session_id text PRIMARY KEY REFERENCES sessions(id),
 protocol_version smallint NOT NULL DEFAULT 1 CHECK(protocol_version=1),
 data_json text NOT NULL CHECK(octet_length(data_json)<=8192),
 FOREIGN KEY(session_id,protocol_version) REFERENCES sessions(id,ownership_protocol)
);
CREATE OR REPLACE FUNCTION crabber_immutable_ownership_protocol() RETURNS trigger AS $$
BEGIN
 IF NEW.ownership_protocol IS DISTINCT FROM OLD.ownership_protocol THEN
  RAISE EXCEPTION 'crabber_ownership_protocol_immutable';
 END IF;
 RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS sessions_ownership_protocol_immutable ON sessions;
CREATE TRIGGER sessions_ownership_protocol_immutable BEFORE UPDATE OF ownership_protocol ON sessions
 FOR EACH ROW EXECUTE FUNCTION crabber_immutable_ownership_protocol();
CREATE TABLE IF NOT EXISTS session_tombstones (
 session_id text PRIMARY KEY CHECK(octet_length(session_id) BETWEEN 1 AND 512),
 operation_id text NOT NULL CHECK(octet_length(operation_id) BETWEEN 1 AND 128),
 expected_revision bigint NOT NULL CHECK(expected_revision>=0 AND expected_revision<9223372036854775807),
 final_revision bigint NOT NULL CHECK(final_revision=expected_revision+1),
 semantic_digest text NOT NULL CHECK(semantic_digest ~ '^[0-9a-f]{64}$'),
 deleted_at_seconds bigint NOT NULL,
 deleted_at_nanos integer NOT NULL CHECK(deleted_at_nanos BETWEEN 0 AND 999999999),
 format_version smallint NOT NULL CHECK(format_version=1)
);
-- These checks participate in the same gate as supported Store mutations,
-- including preparation before a Session row exists.
CREATE OR REPLACE FUNCTION crabber_guard_deleted_session() RETURNS trigger AS $$
DECLARE scope text;
BEGIN
 IF TG_TABLE_NAME='sessions' THEN scope:=NEW.id; ELSE scope:=NEW.session_id; END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended(scope,0));
 IF EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=scope) THEN
  RAISE EXCEPTION 'crabber_session_deleted';
 END IF;
 RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION crabber_lock_tombstone() RETURNS trigger AS $$
BEGIN
 PERFORM pg_advisory_xact_lock(hashtextextended(NEW.session_id,0));
 RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION crabber_immutable_tombstone() RETURNS trigger AS $$
BEGIN
 RAISE EXCEPTION 'crabber_tombstone_immutable';
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS session_tombstone_gate ON session_tombstones;
CREATE TRIGGER session_tombstone_gate BEFORE INSERT ON session_tombstones
 FOR EACH ROW EXECUTE FUNCTION crabber_lock_tombstone();
DROP TRIGGER IF EXISTS session_tombstone_immutable ON session_tombstones;
CREATE TRIGGER session_tombstone_immutable BEFORE UPDATE OR DELETE ON session_tombstones
 FOR EACH ROW EXECUTE FUNCTION crabber_immutable_tombstone();
DROP TRIGGER IF EXISTS sessions_deleted_guard ON sessions;
CREATE TRIGGER sessions_deleted_guard BEFORE INSERT OR UPDATE OF id ON sessions
 FOR EACH ROW EXECUTE FUNCTION crabber_guard_deleted_session();
DROP TRIGGER IF EXISTS session_owner_contexts_deleted_guard ON session_owner_contexts;
CREATE TRIGGER session_owner_contexts_deleted_guard BEFORE INSERT OR UPDATE OF session_id ON session_owner_contexts
 FOR EACH ROW EXECUTE FUNCTION crabber_guard_deleted_session();
DROP TRIGGER IF EXISTS session_owner_current_deleted_guard ON session_owner_current;
CREATE TRIGGER session_owner_current_deleted_guard BEFORE INSERT OR UPDATE OF session_id ON session_owner_current
 FOR EACH ROW EXECUTE FUNCTION crabber_guard_deleted_session();
DROP TRIGGER IF EXISTS session_owner_audits_deleted_guard ON session_owner_audits;
CREATE TRIGGER session_owner_audits_deleted_guard BEFORE INSERT OR UPDATE OF session_id ON session_owner_audits
 FOR EACH ROW EXECUTE FUNCTION crabber_guard_deleted_session();
INSERT INTO schema_version(version) VALUES(16) ON CONFLICT DO NOTHING;

-- Both records commit with the actual admission; the independent first native
-- issuance anchors this witness fingerprint. No Session shell or fake Run.
CREATE UNIQUE INDEX IF NOT EXISTS runs_ownership_scope ON runs(id,session_id);
CREATE TABLE IF NOT EXISTS preparation_admission_witnesses (
 run_id text PRIMARY KEY,
 session_id text NOT NULL REFERENCES sessions(id),
 root_join text NOT NULL CHECK(octet_length(root_join) BETWEEN 1 AND 128),
 data_json text NOT NULL CHECK(octet_length(data_json)<=8192),
 UNIQUE(session_id,root_join),
 FOREIGN KEY(run_id,session_id) REFERENCES runs(id,session_id)
);

CREATE OR REPLACE FUNCTION crabber_immutable_preparation_admission() RETURNS trigger AS $$
BEGIN RAISE EXCEPTION 'crabber_preparation_admission_immutable'; END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS preparation_admission_immutable ON preparation_admission_witnesses;
CREATE TRIGGER preparation_admission_immutable BEFORE UPDATE ON preparation_admission_witnesses
 FOR EACH ROW EXECUTE FUNCTION crabber_immutable_preparation_admission();
