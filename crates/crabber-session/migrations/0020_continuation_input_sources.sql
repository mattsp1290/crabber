-- Full original continuation inputs anchored only by actual new births.
CREATE OR REPLACE FUNCTION crabber_continuation_input_guard() RETURNS trigger AS $$
BEGIN
 IF TG_OP='DELETE' THEN
  IF EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) THEN RETURN OLD; END IF;
  RAISE EXCEPTION 'crabber_continuation_input_immutable';
 END IF;
 IF TG_OP='UPDATE' THEN RAISE EXCEPTION 'crabber_continuation_input_immutable'; END IF;
 IF EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE EXCEPTION 'crabber_session_deleted'; END IF;
 RETURN NEW;
END;
$$ LANGUAGE plpgsql;
ALTER TABLE continuations ADD COLUMN IF NOT EXISTS input_source_protocol smallint NOT NULL DEFAULT 0 CHECK(input_source_protocol IN(0,1));
CREATE UNIQUE INDEX IF NOT EXISTS approval_input_current_scope ON continuations(session_id,run_id,id,generation,input_source_protocol);
CREATE TABLE IF NOT EXISTS approval_input_sources (
 session_id text NOT NULL CHECK(octet_length(session_id) BETWEEN 1 AND 512),
 run_id text NOT NULL CHECK(octet_length(run_id) BETWEEN 1 AND 128),
 continuation_id text NOT NULL CHECK(octet_length(continuation_id) BETWEEN 1 AND 128),
 generation bigint NOT NULL CHECK(generation>0),protocol_version INTEGER NOT NULL DEFAULT 1 CHECK(protocol_version=1),
 operation_id text NOT NULL CHECK(octet_length(operation_id) BETWEEN 1 AND 128),
 kind text NOT NULL CHECK(kind IN('suspension','answer')),
 subject text NOT NULL CHECK(octet_length(subject) BETWEEN 1 AND 128 AND (kind<>'suspension' OR subject='suspension')),
 source_digest text NOT NULL CHECK(source_digest ~ '^[0-9a-f]{64}$'),
 event_seq bigint NOT NULL CHECK(event_seq>0),
 data_json text NOT NULL CHECK(octet_length(data_json)<=4096),
 PRIMARY KEY(session_id,operation_id),
 UNIQUE(session_id,run_id,continuation_id,generation,kind,subject),
 UNIQUE(session_id,run_id,continuation_id,generation,operation_id,kind,subject,source_digest),
 FOREIGN KEY(session_id,run_id) REFERENCES runs(session_id,id)
);
CREATE TABLE IF NOT EXISTS approval_input_cohorts (
 session_id text NOT NULL CHECK(octet_length(session_id) BETWEEN 1 AND 512),
 run_id text NOT NULL CHECK(octet_length(run_id) BETWEEN 1 AND 128),
 continuation_id text NOT NULL CHECK(octet_length(continuation_id) BETWEEN 1 AND 128),
 generation bigint NOT NULL CHECK(generation>0),protocol_version INTEGER NOT NULL DEFAULT 1 CHECK(protocol_version=1),
 origin_operation text NOT NULL CHECK(octet_length(origin_operation) BETWEEN 1 AND 128),
 origin_digest text NOT NULL CHECK(origin_digest ~ '^[0-9a-f]{64}$'),
 origin_kind text NOT NULL DEFAULT 'suspension' CHECK(origin_kind='suspension'),
 origin_subject text NOT NULL DEFAULT 'suspension' CHECK(origin_subject='suspension'),
 PRIMARY KEY(session_id,run_id,continuation_id,generation),
 UNIQUE(session_id,run_id,continuation_id,generation,protocol_version),
 FOREIGN KEY(session_id,run_id,continuation_id,generation,protocol_version)
 REFERENCES continuations(session_id,run_id,id,generation,input_source_protocol),
 FOREIGN KEY(session_id,run_id,continuation_id,generation,origin_operation,origin_kind,origin_subject,origin_digest)
 REFERENCES approval_input_sources(session_id,run_id,continuation_id,generation,operation_id,kind,subject,source_digest) DEFERRABLE INITIALLY DEFERRED
);
CREATE OR REPLACE FUNCTION crabber_approval_input_identity() RETURNS trigger AS $$
BEGIN
 IF TG_OP='DELETE' THEN
  IF NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) THEN RAISE EXCEPTION 'crabber_continuation_input_identity'; END IF;
  RETURN OLD;
 END IF;
 IF TG_OP='INSERT' THEN
  IF EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE EXCEPTION 'crabber_session_deleted'; END IF;
  RETURN NEW;
 END IF;
 IF (NEW.id,NEW.session_id,NEW.run_id,NEW.generation,NEW.input_source_protocol) IS DISTINCT FROM (OLD.id,OLD.session_id,OLD.run_id,OLD.generation,OLD.input_source_protocol) THEN RAISE EXCEPTION 'crabber_continuation_input_identity'; END IF;
 RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS approval_input_identity ON continuations;
CREATE TRIGGER approval_input_identity BEFORE INSERT OR UPDATE OR DELETE ON continuations FOR EACH ROW EXECUTE FUNCTION crabber_approval_input_identity();
DROP TRIGGER IF EXISTS approval_input_sources_immutable ON approval_input_sources;
CREATE TRIGGER approval_input_sources_immutable BEFORE INSERT OR UPDATE OR DELETE ON approval_input_sources FOR EACH ROW EXECUTE FUNCTION crabber_continuation_input_guard();
DROP TRIGGER IF EXISTS approval_input_cohorts_immutable ON approval_input_cohorts;
CREATE TRIGGER approval_input_cohorts_immutable BEFORE INSERT OR UPDATE OR DELETE ON approval_input_cohorts FOR EACH ROW EXECUTE FUNCTION crabber_continuation_input_guard();
ALTER TABLE tool_continuations ADD COLUMN IF NOT EXISTS input_source_protocol smallint NOT NULL DEFAULT 0 CHECK(input_source_protocol IN(0,1));
CREATE UNIQUE INDEX IF NOT EXISTS tool_input_current_scope ON tool_continuations(session_id,run_id,id,generation,input_source_protocol);
CREATE TABLE IF NOT EXISTS tool_input_sources (
 session_id text NOT NULL CHECK(octet_length(session_id) BETWEEN 1 AND 512),
 run_id text NOT NULL CHECK(octet_length(run_id) BETWEEN 1 AND 128),
 continuation_id text NOT NULL CHECK(octet_length(continuation_id) BETWEEN 1 AND 128),
 generation bigint NOT NULL CHECK(generation>0),protocol_version INTEGER NOT NULL DEFAULT 1 CHECK(protocol_version=1),
 operation_id text NOT NULL CHECK(octet_length(operation_id) BETWEEN 1 AND 128),
 kind text NOT NULL CHECK(kind IN('suspension','answer')),
 subject text NOT NULL CHECK(octet_length(subject) BETWEEN 1 AND 128 AND (kind<>'suspension' OR subject='suspension')),
 source_digest text NOT NULL CHECK(source_digest ~ '^[0-9a-f]{64}$'),
 event_seq bigint NOT NULL CHECK(event_seq>0),
 data_json text NOT NULL CHECK(octet_length(data_json)<=4096),
 PRIMARY KEY(session_id,operation_id),
 UNIQUE(session_id,run_id,continuation_id,generation,kind,subject),
 UNIQUE(session_id,run_id,continuation_id,generation,operation_id,kind,subject,source_digest),
 FOREIGN KEY(session_id,run_id) REFERENCES runs(session_id,id)
);
CREATE TABLE IF NOT EXISTS tool_input_cohorts (
 session_id text NOT NULL CHECK(octet_length(session_id) BETWEEN 1 AND 512),
 run_id text NOT NULL CHECK(octet_length(run_id) BETWEEN 1 AND 128),
 continuation_id text NOT NULL CHECK(octet_length(continuation_id) BETWEEN 1 AND 128),
 generation bigint NOT NULL CHECK(generation>0),protocol_version INTEGER NOT NULL DEFAULT 1 CHECK(protocol_version=1),
 origin_operation text NOT NULL CHECK(octet_length(origin_operation) BETWEEN 1 AND 128),
 origin_digest text NOT NULL CHECK(origin_digest ~ '^[0-9a-f]{64}$'),
 origin_kind text NOT NULL DEFAULT 'suspension' CHECK(origin_kind='suspension'),
 origin_subject text NOT NULL DEFAULT 'suspension' CHECK(origin_subject='suspension'),
 PRIMARY KEY(session_id,run_id,continuation_id,generation),
 UNIQUE(session_id,run_id,continuation_id,generation,protocol_version),
 FOREIGN KEY(session_id,run_id,continuation_id,generation,protocol_version)
 REFERENCES tool_continuations(session_id,run_id,id,generation,input_source_protocol),
 FOREIGN KEY(session_id,run_id,continuation_id,generation,origin_operation,origin_kind,origin_subject,origin_digest)
 REFERENCES tool_input_sources(session_id,run_id,continuation_id,generation,operation_id,kind,subject,source_digest) DEFERRABLE INITIALLY DEFERRED
);
CREATE OR REPLACE FUNCTION crabber_tool_input_identity() RETURNS trigger AS $$
BEGIN
 IF TG_OP='DELETE' THEN
  IF NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) THEN RAISE EXCEPTION 'crabber_continuation_input_identity'; END IF;
  RETURN OLD;
 END IF;
 IF TG_OP='INSERT' THEN
  IF EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE EXCEPTION 'crabber_session_deleted'; END IF;
  RETURN NEW;
 END IF;
 IF (NEW.id,NEW.session_id,NEW.run_id,NEW.generation,NEW.input_source_protocol) IS DISTINCT FROM (OLD.id,OLD.session_id,OLD.run_id,OLD.generation,OLD.input_source_protocol) THEN RAISE EXCEPTION 'crabber_continuation_input_identity'; END IF;
 RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS tool_input_identity ON tool_continuations;
CREATE TRIGGER tool_input_identity BEFORE INSERT OR UPDATE OR DELETE ON tool_continuations FOR EACH ROW EXECUTE FUNCTION crabber_tool_input_identity();
DROP TRIGGER IF EXISTS tool_input_sources_immutable ON tool_input_sources;
CREATE TRIGGER tool_input_sources_immutable BEFORE INSERT OR UPDATE OR DELETE ON tool_input_sources FOR EACH ROW EXECUTE FUNCTION crabber_continuation_input_guard();
DROP TRIGGER IF EXISTS tool_input_cohorts_immutable ON tool_input_cohorts;
CREATE TRIGGER tool_input_cohorts_immutable BEFORE INSERT OR UPDATE OR DELETE ON tool_input_cohorts FOR EACH ROW EXECUTE FUNCTION crabber_continuation_input_guard();
DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conname='approval_input_source_cohort_fk' AND conrelid='approval_input_sources'::regclass) THEN
 ALTER TABLE approval_input_sources ADD CONSTRAINT approval_input_source_cohort_fk FOREIGN KEY(session_id,run_id,continuation_id,generation,protocol_version) REFERENCES approval_input_cohorts(session_id,run_id,continuation_id,generation,protocol_version) DEFERRABLE INITIALLY DEFERRED;
 END IF;
END $$;
DO $$ BEGIN
 IF NOT EXISTS(SELECT 1 FROM pg_constraint WHERE conname='tool_input_source_cohort_fk' AND conrelid='tool_input_sources'::regclass) THEN
 ALTER TABLE tool_input_sources ADD CONSTRAINT tool_input_source_cohort_fk FOREIGN KEY(session_id,run_id,continuation_id,generation,protocol_version) REFERENCES tool_input_cohorts(session_id,run_id,continuation_id,generation,protocol_version) DEFERRABLE INITIALLY DEFERRED;
 END IF;
END $$;
INSERT INTO schema_version(version) VALUES(20) ON CONFLICT DO NOTHING;
