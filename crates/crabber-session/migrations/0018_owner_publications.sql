-- Positive owner publication protocol is assigned only by actual new Reserve.
ALTER TABLE session_owner_current ADD COLUMN IF NOT EXISTS source_protocol smallint NOT NULL DEFAULT 0 CHECK(source_protocol IN (0,1));
CREATE UNIQUE INDEX IF NOT EXISTS session_owner_protocol_scope ON session_owner_current(session_id,owner_id,source_protocol);
CREATE OR REPLACE FUNCTION crabber_owner_protocol_immutable() RETURNS trigger AS $$
BEGIN
 IF NEW.source_protocol IS DISTINCT FROM OLD.source_protocol THEN RAISE EXCEPTION 'crabber_owner_protocol_immutable'; END IF;
 RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS session_owner_protocol_immutable ON session_owner_current;
CREATE TRIGGER session_owner_protocol_immutable BEFORE UPDATE OF source_protocol ON session_owner_current FOR EACH ROW EXECUTE FUNCTION crabber_owner_protocol_immutable();
CREATE TABLE IF NOT EXISTS session_owner_publication_cohorts (
 session_id text NOT NULL, owner_id text NOT NULL CHECK(octet_length(owner_id)<=128),
 protocol_version smallint NOT NULL DEFAULT 1 CHECK(protocol_version=1),
 origin_id text NOT NULL CHECK(octet_length(origin_id)<=128),
 origin_digest text NOT NULL CHECK(length(origin_digest)=64),
 head_sequence bigint NOT NULL CHECK(head_sequence>0), head_digest text NOT NULL CHECK(length(head_digest)=64),
 PRIMARY KEY(session_id,owner_id),
 FOREIGN KEY(session_id,owner_id,protocol_version) REFERENCES session_owner_current(session_id,owner_id,source_protocol)
);
CREATE TABLE IF NOT EXISTS session_owner_publications (
 session_id text NOT NULL, owner_id text NOT NULL, operation_id text NOT NULL CHECK(octet_length(operation_id)<=128),
 sequence bigint NOT NULL CHECK(sequence>0), operation text NOT NULL CHECK(length(operation)<=32), state text NOT NULL CHECK(length(state)<=32), audit_digest text NOT NULL CHECK(length(audit_digest)=64),
 data_json text NOT NULL CHECK(octet_length(data_json)<=32768),
 PRIMARY KEY(session_id,operation_id), UNIQUE(session_id,owner_id,sequence),
 FOREIGN KEY(session_id,owner_id) REFERENCES session_owner_publication_cohorts(session_id,owner_id)
);
CREATE OR REPLACE FUNCTION crabber_owner_publication_guard() RETURNS trigger AS $$
BEGIN
 IF TG_OP='DELETE' AND EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) THEN RETURN OLD; END IF;
 RAISE EXCEPTION 'crabber_owner_publication_immutable';
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS session_owner_publication_immutable ON session_owner_publications;
CREATE TRIGGER session_owner_publication_immutable BEFORE UPDATE OR DELETE ON session_owner_publications FOR EACH ROW EXECUTE FUNCTION crabber_owner_publication_guard();
CREATE OR REPLACE FUNCTION crabber_owner_cohort_guard() RETURNS trigger AS $$
BEGIN
 IF TG_OP='DELETE' THEN
  IF EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) THEN RETURN OLD; END IF;
  RAISE EXCEPTION 'crabber_owner_cohort_immutable';
 END IF;
 IF TG_OP='INSERT' THEN
  IF NEW.head_sequence<>1 OR NEW.head_digest<>NEW.origin_digest THEN RAISE EXCEPTION 'crabber_owner_cohort_immutable'; END IF;
  RETURN NEW;
 END IF;
 IF (NEW.session_id,NEW.owner_id,NEW.protocol_version,NEW.origin_id,NEW.origin_digest) IS DISTINCT FROM (OLD.session_id,OLD.owner_id,OLD.protocol_version,OLD.origin_id,OLD.origin_digest)
 OR NEW.head_sequence<>OLD.head_sequence+1
 OR NOT EXISTS(SELECT 1 FROM session_owner_publications p WHERE p.session_id=NEW.session_id AND p.owner_id=NEW.owner_id AND p.sequence=NEW.head_sequence AND p.audit_digest=NEW.head_digest) THEN
  RAISE EXCEPTION 'crabber_owner_cohort_immutable';
 END IF;
 RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS session_owner_cohort_immutable ON session_owner_publication_cohorts;
CREATE TRIGGER session_owner_cohort_immutable BEFORE INSERT OR UPDATE OR DELETE ON session_owner_publication_cohorts FOR EACH ROW EXECUTE FUNCTION crabber_owner_cohort_guard();
-- Scope birth is independently retained. Existing sessions/contexts stay legacy.
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS owner_scope_protocol smallint NOT NULL DEFAULT 0 CHECK(owner_scope_protocol IN (0,1));
ALTER TABLE session_owner_contexts ADD COLUMN IF NOT EXISTS scope_protocol smallint NOT NULL DEFAULT 0 CHECK(scope_protocol IN (0,1));
CREATE OR REPLACE FUNCTION crabber_owner_scope_marker() RETURNS trigger AS $$
BEGIN
 IF (TG_TABLE_NAME='sessions' AND NEW.owner_scope_protocol IS DISTINCT FROM OLD.owner_scope_protocol) THEN RAISE EXCEPTION 'crabber_owner_scope_immutable'; END IF;
 RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- Separate functions avoid record-field lookup on the other table's row type.
CREATE OR REPLACE FUNCTION crabber_owner_context_marker() RETURNS trigger AS $$
BEGIN
 IF NEW.scope_protocol IS DISTINCT FROM OLD.scope_protocol THEN RAISE EXCEPTION 'crabber_owner_scope_immutable'; END IF;
 RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS sessions_owner_scope_immutable ON sessions;
CREATE TRIGGER sessions_owner_scope_immutable BEFORE UPDATE OF owner_scope_protocol ON sessions FOR EACH ROW EXECUTE FUNCTION crabber_owner_scope_marker();
DROP TRIGGER IF EXISTS contexts_owner_scope_immutable ON session_owner_contexts;
CREATE TRIGGER contexts_owner_scope_immutable BEFORE UPDATE OF scope_protocol ON session_owner_contexts FOR EACH ROW EXECUTE FUNCTION crabber_owner_context_marker();
CREATE TABLE IF NOT EXISTS session_owner_scope_origins (
 session_id TEXT PRIMARY KEY, data_json TEXT NOT NULL CHECK(octet_length(data_json)<=32768)
);
DROP TRIGGER IF EXISTS owner_scope_origin_immutable ON session_owner_scope_origins;
CREATE TRIGGER owner_scope_origin_immutable BEFORE UPDATE OR DELETE ON session_owner_scope_origins FOR EACH ROW EXECUTE FUNCTION crabber_owner_publication_guard();
DROP TRIGGER IF EXISTS owner_scope_origin_deleted ON session_owner_scope_origins;
CREATE OR REPLACE FUNCTION crabber_owner_scope_live() RETURNS trigger AS $$
BEGIN
 PERFORM pg_advisory_xact_lock(hashtextextended(NEW.session_id,0));
 IF EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE EXCEPTION 'crabber_session_deleted'; END IF;
 RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER owner_scope_origin_deleted BEFORE INSERT ON session_owner_scope_origins FOR EACH ROW EXECUTE FUNCTION crabber_owner_scope_live();
INSERT INTO schema_version(version) VALUES(18) ON CONFLICT(version) DO NOTHING;
