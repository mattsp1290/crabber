-- Retained Runs stay legacy0. Actual new Run writers supply protocol1 and the
-- exact same-Tx origin. Neither a later Driver nor this migration promotes one.
ALTER TABLE runs ADD COLUMN execution_enrollment_protocol smallint NOT NULL DEFAULT 0 CHECK(execution_enrollment_protocol IN(0,1));
CREATE UNIQUE INDEX runs_execution_enrollment_scope ON runs(session_id,id,execution_enrollment_protocol);
CREATE UNIQUE INDEX native_run_sources_issuer_identity ON native_run_sources(session_id,run_id,sequence,issuance_digest);
CREATE TABLE native_execution_origins (
 session_id text NOT NULL CHECK(octet_length(session_id) BETWEEN 1 AND 512),
 run_id text NOT NULL CHECK(octet_length(run_id) BETWEEN 1 AND 128),
 protocol_version smallint NOT NULL DEFAULT 1 CHECK(protocol_version=1),
 first_sequence bigint NOT NULL DEFAULT 1 CHECK(first_sequence=1),
 first_issuer_digest text NOT NULL CHECK(first_issuer_digest ~ '^[0-9a-f]{64}$'),
 origin_digest text NOT NULL CHECK(origin_digest ~ '^[0-9a-f]{64}$'),
 -- Reserved storage discriminator; current Rust supports ONLY raw. Future
 -- pending support requires a new producer protocol/schema, never adoption.
 birth_kind text NOT NULL DEFAULT 'raw' CHECK(birth_kind IN('raw','pending')),
 unclassified_run_id text,
 data text NOT NULL CHECK(octet_length(data) BETWEEN 1 AND 4096),
 CHECK((birth_kind='raw' AND unclassified_run_id IS NOT NULL AND unclassified_run_id=run_id)
    OR (birth_kind='pending' AND unclassified_run_id IS NULL)),
 PRIMARY KEY(session_id,run_id),UNIQUE(run_id),UNIQUE(session_id,run_id,origin_digest),
 FOREIGN KEY(session_id,run_id,protocol_version) REFERENCES runs(session_id,id,execution_enrollment_protocol) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(session_id,run_id,first_sequence,first_issuer_digest) REFERENCES native_run_sources(session_id,run_id,sequence,issuance_digest) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE native_execution_unclassified (
 session_id text NOT NULL CHECK(octet_length(session_id) BETWEEN 1 AND 512),
 run_id text NOT NULL CHECK(octet_length(run_id) BETWEEN 1 AND 128),
 origin_digest text NOT NULL CHECK(origin_digest ~ '^[0-9a-f]{64}$'),
 data text NOT NULL CHECK(octet_length(data) BETWEEN 1 AND 4096),
 PRIMARY KEY(session_id,run_id),UNIQUE(run_id),
 FOREIGN KEY(session_id,run_id,origin_digest) REFERENCES native_execution_origins(session_id,run_id,origin_digest) DEFERRABLE INITIALLY DEFERRED
);
ALTER TABLE native_execution_origins ADD FOREIGN KEY(unclassified_run_id) REFERENCES native_execution_unclassified(run_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE runs ADD COLUMN execution_enrollment_origin_id text REFERENCES native_execution_origins(run_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE runs ADD CHECK((execution_enrollment_protocol=0 AND execution_enrollment_origin_id IS NULL) OR (execution_enrollment_protocol=1 AND execution_enrollment_origin_id IS NOT NULL AND execution_enrollment_origin_id=id));
CREATE FUNCTION crabber_execution_run_identity() RETURNS trigger AS $$
BEGIN
 IF TG_OP='DELETE' THEN
  IF EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) THEN RETURN OLD; END IF;
  RAISE EXCEPTION 'crabber_execution_run_identity_immutable';
 END IF;
 IF (NEW.id,NEW.session_id,NEW.execution_enrollment_protocol,NEW.execution_enrollment_origin_id)
  IS DISTINCT FROM (OLD.id,OLD.session_id,OLD.execution_enrollment_protocol,OLD.execution_enrollment_origin_id)
 THEN RAISE EXCEPTION 'crabber_execution_run_identity_immutable'; END IF;
 RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER runs_execution_identity BEFORE UPDATE OF id,session_id,execution_enrollment_protocol,execution_enrollment_origin_id OR DELETE ON runs FOR EACH ROW EXECUTE FUNCTION crabber_execution_run_identity();
CREATE FUNCTION crabber_execution_source_guard() RETURNS trigger AS $$
BEGIN
 IF TG_OP='DELETE' THEN
  IF EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) THEN RETURN OLD; END IF;
  RAISE EXCEPTION 'crabber_execution_source_immutable';
 END IF;
 IF TG_OP='UPDATE' THEN RAISE EXCEPTION 'crabber_execution_source_immutable'; END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended(NEW.session_id,0));
 IF EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE EXCEPTION 'crabber_session_deleted'; END IF;
 RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER execution_origin_guard BEFORE INSERT OR UPDATE OR DELETE ON native_execution_origins FOR EACH ROW EXECUTE FUNCTION crabber_execution_source_guard();
CREATE TRIGGER execution_unclassified_guard BEFORE INSERT OR UPDATE OR DELETE ON native_execution_unclassified FOR EACH ROW EXECUTE FUNCTION crabber_execution_source_guard();
INSERT INTO schema_version(version) VALUES(22) ON CONFLICT(version) DO NOTHING;
