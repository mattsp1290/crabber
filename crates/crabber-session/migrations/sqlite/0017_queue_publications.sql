-- Every actual queue control is published atomically; old identities stay legacy.
ALTER TABLE future_queue ADD COLUMN source_protocol INTEGER NOT NULL DEFAULT 0 CHECK(source_protocol IN(0,1));
CREATE UNIQUE INDEX IF NOT EXISTS queue_publication_current_scope ON future_queue(session_id,queue_id,source_protocol);
CREATE TABLE IF NOT EXISTS queue_publication_sources (
 session_id text NOT NULL CHECK(length(CAST(session_id AS BLOB)) BETWEEN 1 AND 512),
 queue_id text NOT NULL CHECK(length(CAST(queue_id AS BLOB)) BETWEEN 1 AND 128),
 protocol_version INTEGER NOT NULL DEFAULT 1 CHECK(protocol_version=1),
 queue_sequence INTEGER NOT NULL CHECK(queue_sequence>0),
 operation_id text NOT NULL CHECK(length(CAST(operation_id AS BLOB)) BETWEEN 1 AND 128),
 sequence INTEGER NOT NULL CHECK(sequence>0),
 kind text NOT NULL CHECK(kind IN('enqueue','claim','prepare','cancel','admit')),
 revision INTEGER NOT NULL CHECK(revision>0),
 previous_digest text CHECK(previous_digest IS NULL OR length(CAST(previous_digest AS BLOB))=64),
 source_digest text NOT NULL CHECK(length(CAST(source_digest AS BLOB))=64),
 data_json text NOT NULL CHECK(length(CAST(data_json AS BLOB))<=4096),
 CHECK((sequence=1 AND kind='enqueue' AND previous_digest IS NULL) OR (sequence>1 AND kind<>'enqueue' AND previous_digest IS NOT NULL)),
 PRIMARY KEY(session_id,operation_id),UNIQUE(session_id,queue_id,sequence),
 UNIQUE(session_id,queue_id,sequence,source_digest),
 FOREIGN KEY(session_id,queue_id,protocol_version) REFERENCES queue_publication_cohorts(session_id,queue_id,protocol_version) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE IF NOT EXISTS queue_publication_cohorts (
 session_id text NOT NULL,queue_id text NOT NULL,
 protocol_version INTEGER NOT NULL DEFAULT 1 CHECK(protocol_version=1),
 origin_operation text NOT NULL,origin_digest text NOT NULL CHECK(length(CAST(origin_digest AS BLOB))=64),
 head_sequence INTEGER NOT NULL CHECK(head_sequence>0),head_digest text NOT NULL CHECK(length(CAST(head_digest AS BLOB))=64),
 PRIMARY KEY(session_id,queue_id),UNIQUE(session_id,queue_id,protocol_version),
 FOREIGN KEY(session_id,queue_id,protocol_version) REFERENCES future_queue(session_id,queue_id,source_protocol),
 FOREIGN KEY(session_id,origin_operation) REFERENCES queue_publication_sources(session_id,operation_id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(session_id,queue_id,head_sequence,head_digest) REFERENCES queue_publication_sources(session_id,queue_id,sequence,source_digest) DEFERRABLE INITIALLY DEFERRED
);
CREATE TRIGGER queue_identity_update BEFORE UPDATE ON future_queue
WHEN (NEW.session_id,NEW.queue_id,NEW.sequence,NEW.admission_key,NEW.source_protocol) IS NOT (OLD.session_id,OLD.queue_id,OLD.sequence,OLD.admission_key,OLD.source_protocol)
BEGIN SELECT RAISE(ABORT,'crabber_queue_identity'); END;
CREATE TRIGGER queue_identity_delete BEFORE DELETE ON future_queue
WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id)
BEGIN SELECT RAISE(ABORT,'crabber_queue_identity'); END;
CREATE TRIGGER queue_identity_replace BEFORE INSERT ON future_queue
WHEN EXISTS(SELECT 1 FROM future_queue WHERE session_id=NEW.session_id AND (queue_id=NEW.queue_id OR sequence=NEW.sequence OR admission_key=NEW.admission_key))
BEGIN SELECT RAISE(ABORT,'crabber_queue_identity'); END;
CREATE TRIGGER queue_identity_deleted BEFORE INSERT ON future_queue
WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id)
BEGIN SELECT RAISE(ABORT,'crabber_session_deleted'); END;
CREATE TRIGGER queue_source_update BEFORE UPDATE ON queue_publication_sources
WHEN 1
BEGIN SELECT RAISE(ABORT,'crabber_queue_source_immutable'); END;
CREATE TRIGGER queue_source_delete BEFORE DELETE ON queue_publication_sources
WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id)
BEGIN SELECT RAISE(ABORT,'crabber_queue_source_immutable'); END;
CREATE TRIGGER queue_source_chain BEFORE INSERT ON queue_publication_sources
WHEN EXISTS(SELECT 1 FROM queue_publication_cohorts c WHERE c.session_id=NEW.session_id AND c.queue_id=NEW.queue_id AND (NEW.sequence<>c.head_sequence+1 OR NEW.previous_digest IS NOT c.head_digest)) OR (NOT EXISTS(SELECT 1 FROM queue_publication_cohorts c WHERE c.session_id=NEW.session_id AND c.queue_id=NEW.queue_id) AND NEW.sequence<>1)
BEGIN SELECT RAISE(ABORT,'crabber_queue_source_chain'); END;
CREATE TRIGGER queue_source_replace BEFORE INSERT ON queue_publication_sources
WHEN EXISTS(SELECT 1 FROM queue_publication_sources WHERE session_id=NEW.session_id AND (operation_id=NEW.operation_id OR (queue_id=NEW.queue_id AND sequence=NEW.sequence)))
BEGIN SELECT RAISE(ABORT,'crabber_queue_source_immutable'); END;
CREATE TRIGGER queue_source_deleted BEFORE INSERT ON queue_publication_sources
WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id)
BEGIN SELECT RAISE(ABORT,'crabber_session_deleted'); END;
CREATE TRIGGER queue_cohort_update BEFORE UPDATE ON queue_publication_cohorts
WHEN (NEW.session_id,NEW.queue_id,NEW.protocol_version,NEW.origin_operation,NEW.origin_digest) IS NOT (OLD.session_id,OLD.queue_id,OLD.protocol_version,OLD.origin_operation,OLD.origin_digest) OR NEW.head_sequence<>OLD.head_sequence+1 OR NOT EXISTS(SELECT 1 FROM queue_publication_sources s WHERE s.session_id=NEW.session_id AND s.queue_id=NEW.queue_id AND s.sequence=NEW.head_sequence AND s.source_digest=NEW.head_digest AND s.previous_digest=OLD.head_digest)
BEGIN SELECT RAISE(ABORT,'crabber_queue_cohort_immutable'); END;
CREATE TRIGGER queue_cohort_birth BEFORE INSERT ON queue_publication_cohorts
WHEN NEW.head_sequence<>1 OR NEW.head_digest<>NEW.origin_digest OR NOT EXISTS(SELECT 1 FROM queue_publication_sources s WHERE s.session_id=NEW.session_id AND s.queue_id=NEW.queue_id AND s.sequence=1 AND s.operation_id=NEW.origin_operation AND s.source_digest=NEW.origin_digest)
BEGIN SELECT RAISE(ABORT,'crabber_queue_cohort_immutable'); END;
CREATE TRIGGER queue_cohort_delete BEFORE DELETE ON queue_publication_cohorts
WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id)
BEGIN SELECT RAISE(ABORT,'crabber_queue_cohort_immutable'); END;
CREATE TRIGGER queue_cohort_replace BEFORE INSERT ON queue_publication_cohorts
WHEN EXISTS(SELECT 1 FROM queue_publication_cohorts WHERE session_id=NEW.session_id AND queue_id=NEW.queue_id)
BEGIN SELECT RAISE(ABORT,'crabber_queue_cohort_immutable'); END;
CREATE TRIGGER queue_cohort_deleted BEFORE INSERT ON queue_publication_cohorts
WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id)
BEGIN SELECT RAISE(ABORT,'crabber_session_deleted'); END;
