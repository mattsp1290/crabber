-- Every actual queue control is published atomically; old identities stay legacy.
ALTER TABLE future_queue ADD COLUMN IF NOT EXISTS source_protocol INTEGER NOT NULL DEFAULT 0 CHECK(source_protocol IN(0,1));
CREATE UNIQUE INDEX IF NOT EXISTS queue_publication_current_scope ON future_queue(session_id,queue_id,source_protocol);
CREATE TABLE IF NOT EXISTS queue_publication_sources (
 session_id text NOT NULL CHECK(octet_length(session_id) BETWEEN 1 AND 512),
 queue_id text NOT NULL CHECK(octet_length(queue_id) BETWEEN 1 AND 128),
 protocol_version INTEGER NOT NULL DEFAULT 1 CHECK(protocol_version=1),
 queue_sequence bigint NOT NULL CHECK(queue_sequence>0),
 operation_id text NOT NULL CHECK(octet_length(operation_id) BETWEEN 1 AND 128),
 sequence bigint NOT NULL CHECK(sequence>0),
 kind text NOT NULL CHECK(kind IN('enqueue','claim','prepare','cancel','admit')),
 revision bigint NOT NULL CHECK(revision>0),
 previous_digest text CHECK(previous_digest IS NULL OR octet_length(previous_digest)=64),
 source_digest text NOT NULL CHECK(octet_length(source_digest)=64),
 data_json text NOT NULL CHECK(octet_length(data_json)<=4096),
 CHECK((sequence=1 AND kind='enqueue' AND previous_digest IS NULL) OR (sequence>1 AND kind<>'enqueue' AND previous_digest IS NOT NULL)),
 PRIMARY KEY(session_id,operation_id),UNIQUE(session_id,queue_id,sequence),
 UNIQUE(session_id,queue_id,sequence,source_digest)
);
CREATE TABLE IF NOT EXISTS queue_publication_cohorts (
 session_id text NOT NULL,queue_id text NOT NULL,
 protocol_version INTEGER NOT NULL DEFAULT 1 CHECK(protocol_version=1),
 origin_operation text NOT NULL,origin_digest text NOT NULL CHECK(octet_length(origin_digest)=64),
 head_sequence bigint NOT NULL CHECK(head_sequence>0),head_digest text NOT NULL CHECK(octet_length(head_digest)=64),
 PRIMARY KEY(session_id,queue_id),UNIQUE(session_id,queue_id,protocol_version),
 FOREIGN KEY(session_id,queue_id,protocol_version) REFERENCES future_queue(session_id,queue_id,source_protocol),
 FOREIGN KEY(session_id,origin_operation) REFERENCES queue_publication_sources(session_id,operation_id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(session_id,queue_id,head_sequence,head_digest) REFERENCES queue_publication_sources(session_id,queue_id,sequence,source_digest) DEFERRABLE INITIALLY DEFERRED
);
ALTER TABLE queue_publication_sources DROP CONSTRAINT IF EXISTS queue_publication_source_cohort;
ALTER TABLE queue_publication_sources ADD CONSTRAINT queue_publication_source_cohort FOREIGN KEY(session_id,queue_id,protocol_version) REFERENCES queue_publication_cohorts(session_id,queue_id,protocol_version) DEFERRABLE INITIALLY DEFERRED;
CREATE OR REPLACE FUNCTION crabber_queue_identity() RETURNS trigger AS $$
BEGIN
 IF TG_OP='DELETE' THEN
  IF NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) THEN RAISE EXCEPTION 'crabber_queue_identity'; END IF; RETURN OLD;
 END IF;
 IF TG_OP='INSERT' THEN
  IF EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE EXCEPTION 'crabber_session_deleted'; END IF;
  IF EXISTS(SELECT 1 FROM future_queue WHERE session_id=NEW.session_id AND (queue_id=NEW.queue_id OR sequence=NEW.sequence OR admission_key=NEW.admission_key)) THEN RAISE EXCEPTION 'crabber_queue_identity'; END IF; RETURN NEW;
 END IF;
 IF (NEW.session_id,NEW.queue_id,NEW.sequence,NEW.admission_key,NEW.source_protocol) IS DISTINCT FROM (OLD.session_id,OLD.queue_id,OLD.sequence,OLD.admission_key,OLD.source_protocol) THEN RAISE EXCEPTION 'crabber_queue_identity'; END IF; RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS queue_publication_identity ON future_queue;
CREATE TRIGGER queue_publication_identity BEFORE INSERT OR UPDATE OR DELETE ON future_queue FOR EACH ROW EXECUTE FUNCTION crabber_queue_identity();
CREATE OR REPLACE FUNCTION crabber_queue_source() RETURNS trigger AS $$
BEGIN
 IF TG_OP='DELETE' AND EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) THEN RETURN OLD; END IF;
 IF TG_OP<>'INSERT' THEN RAISE EXCEPTION 'crabber_queue_source_immutable'; END IF;
 IF EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE EXCEPTION 'crabber_session_deleted'; END IF;
 IF EXISTS(SELECT 1 FROM queue_publication_cohorts c WHERE c.session_id=NEW.session_id AND c.queue_id=NEW.queue_id AND (NEW.sequence<>c.head_sequence+1 OR NEW.previous_digest IS DISTINCT FROM c.head_digest)) OR (NOT EXISTS(SELECT 1 FROM queue_publication_cohorts c WHERE c.session_id=NEW.session_id AND c.queue_id=NEW.queue_id) AND NEW.sequence<>1) THEN RAISE EXCEPTION 'crabber_queue_source_chain'; END IF; RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS queue_publication_source_guard ON queue_publication_sources;
CREATE TRIGGER queue_publication_source_guard BEFORE INSERT OR UPDATE OR DELETE ON queue_publication_sources FOR EACH ROW EXECUTE FUNCTION crabber_queue_source();
CREATE OR REPLACE FUNCTION crabber_queue_cohort() RETURNS trigger AS $$
BEGIN
 IF TG_OP='DELETE' THEN
  IF NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) THEN RAISE EXCEPTION 'crabber_queue_cohort_immutable'; END IF; RETURN OLD;
 END IF;
 IF TG_OP='INSERT' THEN
  IF EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE EXCEPTION 'crabber_session_deleted'; END IF;
  IF NEW.head_sequence<>1 OR NEW.head_digest<>NEW.origin_digest OR NOT EXISTS(SELECT 1 FROM queue_publication_sources s WHERE s.session_id=NEW.session_id AND s.queue_id=NEW.queue_id AND s.sequence=1 AND s.operation_id=NEW.origin_operation AND s.source_digest=NEW.origin_digest) THEN RAISE EXCEPTION 'crabber_queue_cohort_immutable'; END IF; RETURN NEW;
 END IF;
 IF (NEW.session_id,NEW.queue_id,NEW.protocol_version,NEW.origin_operation,NEW.origin_digest) IS DISTINCT FROM (OLD.session_id,OLD.queue_id,OLD.protocol_version,OLD.origin_operation,OLD.origin_digest) OR NEW.head_sequence<>OLD.head_sequence+1 OR NOT EXISTS(SELECT 1 FROM queue_publication_sources s WHERE s.session_id=NEW.session_id AND s.queue_id=NEW.queue_id AND s.sequence=NEW.head_sequence AND s.source_digest=NEW.head_digest AND s.previous_digest=OLD.head_digest) THEN RAISE EXCEPTION 'crabber_queue_cohort_immutable'; END IF; RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS queue_publication_cohort_guard ON queue_publication_cohorts;
CREATE TRIGGER queue_publication_cohort_guard BEFORE INSERT OR UPDATE OR DELETE ON queue_publication_cohorts FOR EACH ROW EXECUTE FUNCTION crabber_queue_cohort();
INSERT INTO schema_version(version) VALUES(21) ON CONFLICT DO NOTHING;
