-- Full original continuation inputs anchored only by actual new births.
ALTER TABLE continuations ADD COLUMN input_source_protocol INTEGER NOT NULL DEFAULT 0 CHECK(input_source_protocol IN(0,1));
CREATE UNIQUE INDEX IF NOT EXISTS approval_input_current_scope ON continuations(session_id,run_id,id,generation,input_source_protocol);
CREATE TABLE IF NOT EXISTS approval_input_sources (
 session_id text NOT NULL CHECK(length(CAST(session_id AS BLOB)) BETWEEN 1 AND 512),
 run_id text NOT NULL CHECK(length(CAST(run_id AS BLOB)) BETWEEN 1 AND 128),
 continuation_id text NOT NULL CHECK(length(CAST(continuation_id AS BLOB)) BETWEEN 1 AND 128),
 generation INTEGER NOT NULL CHECK(generation>0),protocol_version INTEGER NOT NULL DEFAULT 1 CHECK(protocol_version=1),
 operation_id text NOT NULL CHECK(length(CAST(operation_id AS BLOB)) BETWEEN 1 AND 128),
 kind text NOT NULL CHECK(kind IN('suspension','answer')),
 subject text NOT NULL CHECK(length(CAST(subject AS BLOB)) BETWEEN 1 AND 128 AND (kind<>'suspension' OR subject='suspension')),
 source_digest text NOT NULL CHECK(length(CAST(source_digest AS BLOB))=64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
 event_seq INTEGER NOT NULL CHECK(event_seq>0),
 data_json text NOT NULL CHECK(length(CAST(data_json AS BLOB))<=4096),
 PRIMARY KEY(session_id,operation_id),
 UNIQUE(session_id,run_id,continuation_id,generation,kind,subject),
 UNIQUE(session_id,run_id,continuation_id,generation,operation_id,kind,subject,source_digest),
 FOREIGN KEY(session_id,run_id) REFERENCES runs(session_id,id),
 FOREIGN KEY(session_id,run_id,continuation_id,generation,protocol_version)
 REFERENCES approval_input_cohorts(session_id,run_id,continuation_id,generation,protocol_version) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE IF NOT EXISTS approval_input_cohorts (
 session_id text NOT NULL CHECK(length(CAST(session_id AS BLOB)) BETWEEN 1 AND 512),
 run_id text NOT NULL CHECK(length(CAST(run_id AS BLOB)) BETWEEN 1 AND 128),
 continuation_id text NOT NULL CHECK(length(CAST(continuation_id AS BLOB)) BETWEEN 1 AND 128),
 generation INTEGER NOT NULL CHECK(generation>0),protocol_version INTEGER NOT NULL DEFAULT 1 CHECK(protocol_version=1),
 origin_operation text NOT NULL CHECK(length(CAST(origin_operation AS BLOB)) BETWEEN 1 AND 128),
 origin_digest text NOT NULL CHECK(length(CAST(origin_digest AS BLOB))=64 AND origin_digest NOT GLOB '*[^0-9a-f]*'),
 origin_kind text NOT NULL DEFAULT 'suspension' CHECK(origin_kind='suspension'),
 origin_subject text NOT NULL DEFAULT 'suspension' CHECK(origin_subject='suspension'),
 PRIMARY KEY(session_id,run_id,continuation_id,generation),
 UNIQUE(session_id,run_id,continuation_id,generation,protocol_version),
 FOREIGN KEY(session_id,run_id,continuation_id,generation,protocol_version)
 REFERENCES continuations(session_id,run_id,id,generation,input_source_protocol),
 FOREIGN KEY(session_id,run_id,continuation_id,generation,origin_operation,origin_kind,origin_subject,origin_digest)
 REFERENCES approval_input_sources(session_id,run_id,continuation_id,generation,operation_id,kind,subject,source_digest) DEFERRABLE INITIALLY DEFERRED
);
CREATE TRIGGER approval_input_identity BEFORE UPDATE OF id,session_id,run_id,generation,input_source_protocol ON continuations
WHEN NEW.id<>OLD.id OR NEW.session_id<>OLD.session_id OR NEW.run_id<>OLD.run_id OR NEW.generation<>OLD.generation OR NEW.input_source_protocol<>OLD.input_source_protocol
BEGIN SELECT RAISE(ABORT,'crabber_continuation_input_identity'); END;
CREATE TRIGGER approval_input_no_replace BEFORE INSERT ON continuations
WHEN EXISTS(SELECT 1 FROM continuations WHERE id=NEW.id OR (run_id=NEW.run_id AND generation=NEW.generation))
 OR EXISTS(SELECT 1 FROM approval_input_cohorts WHERE continuation_id=NEW.id OR (run_id=NEW.run_id AND generation=NEW.generation))
BEGIN SELECT RAISE(ABORT,'crabber_continuation_input_replace'); END;
CREATE TRIGGER approval_input_deleted BEFORE INSERT ON continuations
WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id)
BEGIN SELECT RAISE(ABORT,'crabber_session_deleted'); END;
CREATE TRIGGER approval_input_delete BEFORE DELETE ON continuations
WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id)
BEGIN SELECT RAISE(ABORT,'crabber_continuation_input_identity'); END;
CREATE TRIGGER approval_input_sources_immutable BEFORE UPDATE ON approval_input_sources
BEGIN SELECT RAISE(ABORT,'crabber_continuation_input_immutable'); END;
CREATE TRIGGER approval_input_sources_delete BEFORE DELETE ON approval_input_sources
WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id)
BEGIN SELECT RAISE(ABORT,'crabber_continuation_input_immutable'); END;
CREATE TRIGGER approval_input_sources_deleted BEFORE INSERT ON approval_input_sources
WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id)
BEGIN SELECT RAISE(ABORT,'crabber_session_deleted'); END;
CREATE TRIGGER approval_input_cohorts_immutable BEFORE UPDATE ON approval_input_cohorts
BEGIN SELECT RAISE(ABORT,'crabber_continuation_input_immutable'); END;
CREATE TRIGGER approval_input_cohorts_delete BEFORE DELETE ON approval_input_cohorts
WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id)
BEGIN SELECT RAISE(ABORT,'crabber_continuation_input_immutable'); END;
CREATE TRIGGER approval_input_cohorts_deleted BEFORE INSERT ON approval_input_cohorts
WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id)
BEGIN SELECT RAISE(ABORT,'crabber_session_deleted'); END;
CREATE TRIGGER approval_input_sources_no_replace BEFORE INSERT ON approval_input_sources
WHEN EXISTS(SELECT 1 FROM approval_input_sources WHERE (session_id=NEW.session_id AND operation_id=NEW.operation_id)
 OR (session_id=NEW.session_id AND run_id=NEW.run_id AND continuation_id=NEW.continuation_id AND generation=NEW.generation AND kind=NEW.kind AND subject=NEW.subject))
BEGIN SELECT RAISE(ABORT,'crabber_continuation_input_replace'); END;
CREATE TRIGGER approval_input_cohorts_no_replace BEFORE INSERT ON approval_input_cohorts
WHEN EXISTS(SELECT 1 FROM approval_input_cohorts WHERE session_id=NEW.session_id AND run_id=NEW.run_id AND continuation_id=NEW.continuation_id AND generation=NEW.generation)
BEGIN SELECT RAISE(ABORT,'crabber_continuation_input_replace'); END;
ALTER TABLE tool_continuations ADD COLUMN input_source_protocol INTEGER NOT NULL DEFAULT 0 CHECK(input_source_protocol IN(0,1));
CREATE UNIQUE INDEX IF NOT EXISTS tool_input_current_scope ON tool_continuations(session_id,run_id,id,generation,input_source_protocol);
CREATE TABLE IF NOT EXISTS tool_input_sources (
 session_id text NOT NULL CHECK(length(CAST(session_id AS BLOB)) BETWEEN 1 AND 512),
 run_id text NOT NULL CHECK(length(CAST(run_id AS BLOB)) BETWEEN 1 AND 128),
 continuation_id text NOT NULL CHECK(length(CAST(continuation_id AS BLOB)) BETWEEN 1 AND 128),
 generation INTEGER NOT NULL CHECK(generation>0),protocol_version INTEGER NOT NULL DEFAULT 1 CHECK(protocol_version=1),
 operation_id text NOT NULL CHECK(length(CAST(operation_id AS BLOB)) BETWEEN 1 AND 128),
 kind text NOT NULL CHECK(kind IN('suspension','answer')),
 subject text NOT NULL CHECK(length(CAST(subject AS BLOB)) BETWEEN 1 AND 128 AND (kind<>'suspension' OR subject='suspension')),
 source_digest text NOT NULL CHECK(length(CAST(source_digest AS BLOB))=64 AND source_digest NOT GLOB '*[^0-9a-f]*'),
 event_seq INTEGER NOT NULL CHECK(event_seq>0),
 data_json text NOT NULL CHECK(length(CAST(data_json AS BLOB))<=4096),
 PRIMARY KEY(session_id,operation_id),
 UNIQUE(session_id,run_id,continuation_id,generation,kind,subject),
 UNIQUE(session_id,run_id,continuation_id,generation,operation_id,kind,subject,source_digest),
 FOREIGN KEY(session_id,run_id) REFERENCES runs(session_id,id),
 FOREIGN KEY(session_id,run_id,continuation_id,generation,protocol_version)
 REFERENCES tool_input_cohorts(session_id,run_id,continuation_id,generation,protocol_version) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE IF NOT EXISTS tool_input_cohorts (
 session_id text NOT NULL CHECK(length(CAST(session_id AS BLOB)) BETWEEN 1 AND 512),
 run_id text NOT NULL CHECK(length(CAST(run_id AS BLOB)) BETWEEN 1 AND 128),
 continuation_id text NOT NULL CHECK(length(CAST(continuation_id AS BLOB)) BETWEEN 1 AND 128),
 generation INTEGER NOT NULL CHECK(generation>0),protocol_version INTEGER NOT NULL DEFAULT 1 CHECK(protocol_version=1),
 origin_operation text NOT NULL CHECK(length(CAST(origin_operation AS BLOB)) BETWEEN 1 AND 128),
 origin_digest text NOT NULL CHECK(length(CAST(origin_digest AS BLOB))=64 AND origin_digest NOT GLOB '*[^0-9a-f]*'),
 origin_kind text NOT NULL DEFAULT 'suspension' CHECK(origin_kind='suspension'),
 origin_subject text NOT NULL DEFAULT 'suspension' CHECK(origin_subject='suspension'),
 PRIMARY KEY(session_id,run_id,continuation_id,generation),
 UNIQUE(session_id,run_id,continuation_id,generation,protocol_version),
 FOREIGN KEY(session_id,run_id,continuation_id,generation,protocol_version)
 REFERENCES tool_continuations(session_id,run_id,id,generation,input_source_protocol),
 FOREIGN KEY(session_id,run_id,continuation_id,generation,origin_operation,origin_kind,origin_subject,origin_digest)
 REFERENCES tool_input_sources(session_id,run_id,continuation_id,generation,operation_id,kind,subject,source_digest) DEFERRABLE INITIALLY DEFERRED
);
CREATE TRIGGER tool_input_identity BEFORE UPDATE OF id,session_id,run_id,generation,input_source_protocol ON tool_continuations
WHEN NEW.id<>OLD.id OR NEW.session_id<>OLD.session_id OR NEW.run_id<>OLD.run_id OR NEW.generation<>OLD.generation OR NEW.input_source_protocol<>OLD.input_source_protocol
BEGIN SELECT RAISE(ABORT,'crabber_continuation_input_identity'); END;
CREATE TRIGGER tool_input_no_replace BEFORE INSERT ON tool_continuations
WHEN EXISTS(SELECT 1 FROM tool_continuations WHERE id=NEW.id OR (run_id=NEW.run_id AND generation=NEW.generation))
 OR EXISTS(SELECT 1 FROM tool_input_cohorts WHERE continuation_id=NEW.id OR (run_id=NEW.run_id AND generation=NEW.generation))
BEGIN SELECT RAISE(ABORT,'crabber_continuation_input_replace'); END;
CREATE TRIGGER tool_input_deleted BEFORE INSERT ON tool_continuations
WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id)
BEGIN SELECT RAISE(ABORT,'crabber_session_deleted'); END;
CREATE TRIGGER tool_input_delete BEFORE DELETE ON tool_continuations
WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id)
BEGIN SELECT RAISE(ABORT,'crabber_continuation_input_identity'); END;
CREATE TRIGGER tool_input_sources_immutable BEFORE UPDATE ON tool_input_sources
BEGIN SELECT RAISE(ABORT,'crabber_continuation_input_immutable'); END;
CREATE TRIGGER tool_input_sources_delete BEFORE DELETE ON tool_input_sources
WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id)
BEGIN SELECT RAISE(ABORT,'crabber_continuation_input_immutable'); END;
CREATE TRIGGER tool_input_sources_deleted BEFORE INSERT ON tool_input_sources
WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id)
BEGIN SELECT RAISE(ABORT,'crabber_session_deleted'); END;
CREATE TRIGGER tool_input_cohorts_immutable BEFORE UPDATE ON tool_input_cohorts
BEGIN SELECT RAISE(ABORT,'crabber_continuation_input_immutable'); END;
CREATE TRIGGER tool_input_cohorts_delete BEFORE DELETE ON tool_input_cohorts
WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id)
BEGIN SELECT RAISE(ABORT,'crabber_continuation_input_immutable'); END;
CREATE TRIGGER tool_input_cohorts_deleted BEFORE INSERT ON tool_input_cohorts
WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id)
BEGIN SELECT RAISE(ABORT,'crabber_session_deleted'); END;
CREATE TRIGGER tool_input_sources_no_replace BEFORE INSERT ON tool_input_sources
WHEN EXISTS(SELECT 1 FROM tool_input_sources WHERE (session_id=NEW.session_id AND operation_id=NEW.operation_id)
 OR (session_id=NEW.session_id AND run_id=NEW.run_id AND continuation_id=NEW.continuation_id AND generation=NEW.generation AND kind=NEW.kind AND subject=NEW.subject))
BEGIN SELECT RAISE(ABORT,'crabber_continuation_input_replace'); END;
CREATE TRIGGER tool_input_cohorts_no_replace BEFORE INSERT ON tool_input_cohorts
WHEN EXISTS(SELECT 1 FROM tool_input_cohorts WHERE session_id=NEW.session_id AND run_id=NEW.run_id AND continuation_id=NEW.continuation_id AND generation=NEW.generation)
BEGIN SELECT RAISE(ABORT,'crabber_continuation_input_replace'); END;
