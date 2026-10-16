-- Actual new Session/Run births opt in; retained rows never gain a source era.
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS native_run_protocol smallint NOT NULL DEFAULT 0 CHECK(native_run_protocol IN(0,1));
ALTER TABLE session_owner_contexts ADD COLUMN IF NOT EXISTS native_run_protocol smallint NOT NULL DEFAULT 0 CHECK(native_run_protocol IN(0,1));
ALTER TABLE runs ADD COLUMN IF NOT EXISTS native_source_protocol smallint NOT NULL DEFAULT 0 CHECK(native_source_protocol IN(0,1));
CREATE UNIQUE INDEX IF NOT EXISTS runs_native_source_scope ON runs(session_id,id,native_source_protocol);
CREATE OR REPLACE FUNCTION crabber_native_session_identity() RETURNS trigger AS $$
BEGIN
 IF TG_OP='DELETE' THEN
  IF EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.id) THEN RETURN OLD; END IF;
  RAISE EXCEPTION 'crabber_session_identity_immutable';
 END IF;
 IF NEW.id IS DISTINCT FROM OLD.id OR NEW.native_run_protocol IS DISTINCT FROM OLD.native_run_protocol OR NEW.public_revision<OLD.public_revision THEN RAISE EXCEPTION 'crabber_session_identity_immutable'; END IF;
 RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS sessions_native_identity ON sessions;
CREATE TRIGGER sessions_native_identity BEFORE UPDATE OF id,native_run_protocol,public_revision OR DELETE ON sessions FOR EACH ROW EXECUTE FUNCTION crabber_native_session_identity();
CREATE OR REPLACE FUNCTION crabber_native_context_identity() RETURNS trigger AS $$
BEGIN
 IF TG_OP='DELETE' THEN
  IF EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) THEN RETURN OLD; END IF;
  RAISE EXCEPTION 'crabber_context_identity_immutable';
 END IF;
 IF NEW.session_id IS DISTINCT FROM OLD.session_id OR NEW.native_run_protocol IS DISTINCT FROM OLD.native_run_protocol THEN RAISE EXCEPTION 'crabber_context_identity_immutable'; END IF;
 RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS contexts_native_identity ON session_owner_contexts;
CREATE TRIGGER contexts_native_identity BEFORE UPDATE OF session_id,native_run_protocol OR DELETE ON session_owner_contexts FOR EACH ROW EXECUTE FUNCTION crabber_native_context_identity();
CREATE OR REPLACE FUNCTION crabber_native_run_identity() RETURNS trigger AS $$
BEGIN
 IF TG_OP='DELETE' THEN
  IF OLD.native_source_protocol=0 OR EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) THEN RETURN OLD; END IF;
  RAISE EXCEPTION 'crabber_native_run_identity_immutable';
 ELSIF TG_OP='UPDATE' THEN
  IF (NEW.id,NEW.session_id,NEW.native_source_protocol) IS DISTINCT FROM (OLD.id,OLD.session_id,OLD.native_source_protocol) THEN RAISE EXCEPTION 'crabber_native_run_protocol_immutable'; END IF;
 ELSE
  PERFORM pg_advisory_xact_lock(hashtextextended(NEW.session_id,0));
  IF EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE EXCEPTION 'crabber_session_deleted'; END IF;
  IF NEW.native_source_protocol=0 AND EXISTS(SELECT 1 FROM sessions WHERE id=NEW.session_id AND native_run_protocol=1) THEN RAISE EXCEPTION 'crabber_native_run_protocol_required'; END IF;
 END IF;
 RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS runs_native_identity ON runs;
CREATE TRIGGER runs_native_identity BEFORE INSERT OR UPDATE OF id,session_id,native_source_protocol OR DELETE ON runs FOR EACH ROW EXECUTE FUNCTION crabber_native_run_identity();
CREATE TABLE IF NOT EXISTS native_run_sources (
 session_id text NOT NULL CHECK(octet_length(session_id) BETWEEN 1 AND 512),run_id text NOT NULL CHECK(octet_length(run_id) BETWEEN 1 AND 128),sequence bigint NOT NULL CHECK(sequence>0),
 protocol_version smallint NOT NULL DEFAULT 1 CHECK(protocol_version=1),
 issuance_digest text NOT NULL CHECK(issuance_digest ~ '^[0-9a-f]{64}$'),
 previous_digest text CHECK(previous_digest ~ '^[0-9a-f]{64}$'),
 chain_digest text NOT NULL CHECK(chain_digest ~ '^[0-9a-f]{64}$'),
 CHECK((sequence=1)=(previous_digest IS NULL)),
 PRIMARY KEY(session_id,run_id,sequence),UNIQUE(run_id,sequence),
 UNIQUE(session_id,run_id,chain_digest),UNIQUE(session_id,run_id,sequence,chain_digest),
 FOREIGN KEY(session_id,run_id,protocol_version) REFERENCES runs(session_id,id,native_source_protocol)
);
CREATE TABLE IF NOT EXISTS native_run_source_heads (
 session_id text NOT NULL CHECK(octet_length(session_id) BETWEEN 1 AND 512),run_id text NOT NULL CHECK(octet_length(run_id) BETWEEN 1 AND 128),sequence bigint NOT NULL CHECK(sequence>0),
 protocol_version smallint NOT NULL DEFAULT 1 CHECK(protocol_version=1),
 origin_digest text NOT NULL CHECK(origin_digest ~ '^[0-9a-f]{64}$'),
 chain_digest text NOT NULL CHECK(chain_digest ~ '^[0-9a-f]{64}$'),
 PRIMARY KEY(session_id,run_id),UNIQUE(run_id),
 FOREIGN KEY(session_id,run_id,origin_digest) REFERENCES native_run_sources(session_id,run_id,chain_digest),
 FOREIGN KEY(session_id,run_id,sequence,chain_digest) REFERENCES native_run_sources(session_id,run_id,sequence,chain_digest)
);
CREATE OR REPLACE FUNCTION crabber_native_source_guard() RETURNS trigger AS $$
DECLARE prior_sequence bigint; prior_digest text;
BEGIN
 IF TG_OP='DELETE' THEN
  IF EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) THEN RETURN OLD; END IF;
  RAISE EXCEPTION 'crabber_native_source_immutable';
 END IF;
 IF TG_OP='UPDATE' THEN RAISE EXCEPTION 'crabber_native_source_immutable'; END IF;
 IF EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE EXCEPTION 'crabber_session_deleted'; END IF;
 IF NEW.sequence=1 THEN
  IF NEW.previous_digest IS NOT NULL OR EXISTS(SELECT 1 FROM native_run_source_heads WHERE run_id=NEW.run_id) OR EXISTS(SELECT 1 FROM native_run_sources WHERE run_id=NEW.run_id) THEN RAISE EXCEPTION 'crabber_native_source_origin'; END IF;
 ELSE
  SELECT sequence,chain_digest INTO prior_sequence,prior_digest FROM native_run_source_heads WHERE session_id=NEW.session_id AND run_id=NEW.run_id FOR UPDATE;
  IF NOT FOUND OR prior_sequence=9223372036854775807 OR NEW.sequence<>prior_sequence+1 OR NEW.previous_digest IS DISTINCT FROM prior_digest THEN RAISE EXCEPTION 'crabber_native_source_sequence'; END IF;
 END IF;
 RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS native_run_source_guard ON native_run_sources;
CREATE TRIGGER native_run_source_guard BEFORE INSERT OR UPDATE OR DELETE ON native_run_sources FOR EACH ROW EXECUTE FUNCTION crabber_native_source_guard();
CREATE OR REPLACE FUNCTION crabber_native_head_guard() RETURNS trigger AS $$
BEGIN
 IF TG_OP='DELETE' THEN
  IF EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) THEN RETURN OLD; END IF;
  RAISE EXCEPTION 'crabber_native_head_immutable';
 END IF;
 IF EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE EXCEPTION 'crabber_session_deleted'; END IF;
 IF TG_OP='INSERT' THEN
  IF NEW.sequence<>1 OR NEW.origin_digest<>NEW.chain_digest THEN RAISE EXCEPTION 'crabber_native_head_origin'; END IF;
 ELSE
  IF (NEW.session_id,NEW.run_id,NEW.protocol_version,NEW.origin_digest) IS DISTINCT FROM (OLD.session_id,OLD.run_id,OLD.protocol_version,OLD.origin_digest)
   OR OLD.sequence=9223372036854775807 OR NEW.sequence<>OLD.sequence+1
   OR NOT EXISTS(SELECT 1 FROM native_run_sources WHERE session_id=NEW.session_id AND run_id=NEW.run_id AND sequence=NEW.sequence AND chain_digest=NEW.chain_digest AND previous_digest=OLD.chain_digest)
  THEN RAISE EXCEPTION 'crabber_native_head_sequence'; END IF;
 END IF;
 RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS native_run_head_guard ON native_run_source_heads;
CREATE TRIGGER native_run_head_guard BEFORE INSERT OR UPDATE OR DELETE ON native_run_source_heads FOR EACH ROW EXECUTE FUNCTION crabber_native_head_guard();
CREATE OR REPLACE FUNCTION crabber_publish_native_source() RETURNS trigger AS $$
BEGIN
 IF NEW.sequence=1 THEN
  INSERT INTO native_run_source_heads(session_id,run_id,sequence,origin_digest,chain_digest) VALUES(NEW.session_id,NEW.run_id,1,NEW.chain_digest,NEW.chain_digest);
 ELSE
  UPDATE native_run_source_heads SET sequence=NEW.sequence,chain_digest=NEW.chain_digest WHERE session_id=NEW.session_id AND run_id=NEW.run_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'crabber_native_source_head_absent'; END IF;
 END IF;
 RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS native_run_source_publish ON native_run_sources;
CREATE TRIGGER native_run_source_publish AFTER INSERT ON native_run_sources FOR EACH ROW EXECUTE FUNCTION crabber_publish_native_source();
INSERT INTO schema_version(version) VALUES(19) ON CONFLICT(version) DO NOTHING;
