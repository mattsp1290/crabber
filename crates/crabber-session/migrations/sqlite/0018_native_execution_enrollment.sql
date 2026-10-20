-- The cyclic deferred raw Run/origin/poison references require the actual new
-- raw admission transaction to publish all three; migration leaves legacy rows0.
ALTER TABLE runs ADD COLUMN execution_enrollment_protocol INTEGER NOT NULL DEFAULT 0 CHECK(execution_enrollment_protocol IN(0,1));
CREATE UNIQUE INDEX runs_execution_enrollment_scope ON runs(session_id,id,execution_enrollment_protocol);
CREATE UNIQUE INDEX native_run_sources_issuer_identity ON native_run_sources(session_id,run_id,sequence,issuance_digest);
CREATE TABLE native_execution_origins (
 session_id TEXT NOT NULL CHECK(length(CAST(session_id AS BLOB)) BETWEEN 1 AND 512),
 run_id TEXT NOT NULL CHECK(length(CAST(run_id AS BLOB)) BETWEEN 1 AND 128),
 protocol_version INTEGER NOT NULL DEFAULT 1 CHECK(protocol_version=1),
 first_sequence INTEGER NOT NULL DEFAULT 1 CHECK(first_sequence=1),
 first_issuer_digest TEXT NOT NULL CHECK(length(first_issuer_digest)=64 AND first_issuer_digest NOT GLOB '*[^0-9a-f]*'),
 origin_digest TEXT NOT NULL CHECK(length(origin_digest)=64 AND origin_digest NOT GLOB '*[^0-9a-f]*'),
 -- Reserved storage discriminator; current Rust supports ONLY raw. Future
 -- pending support requires a new producer protocol/schema, never adoption.
 birth_kind TEXT NOT NULL DEFAULT 'raw' CHECK(birth_kind IN('raw','pending')),
 unclassified_run_id TEXT,
 data TEXT NOT NULL CHECK(length(CAST(data AS BLOB)) BETWEEN 1 AND 4096),
 CHECK((birth_kind='raw' AND unclassified_run_id IS NOT NULL AND unclassified_run_id=run_id)
    OR (birth_kind='pending' AND unclassified_run_id IS NULL)),
 PRIMARY KEY(session_id,run_id),UNIQUE(run_id),UNIQUE(session_id,run_id,origin_digest),
 FOREIGN KEY(session_id,run_id,protocol_version) REFERENCES runs(session_id,id,execution_enrollment_protocol) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(session_id,run_id,first_sequence,first_issuer_digest) REFERENCES native_run_sources(session_id,run_id,sequence,issuance_digest) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(unclassified_run_id) REFERENCES native_execution_unclassified(run_id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE native_execution_unclassified (
 session_id TEXT NOT NULL CHECK(length(CAST(session_id AS BLOB)) BETWEEN 1 AND 512),
 run_id TEXT NOT NULL CHECK(length(CAST(run_id AS BLOB)) BETWEEN 1 AND 128),
 origin_digest TEXT NOT NULL CHECK(length(origin_digest)=64 AND origin_digest NOT GLOB '*[^0-9a-f]*'),
 data TEXT NOT NULL CHECK(length(CAST(data AS BLOB)) BETWEEN 1 AND 4096),
 PRIMARY KEY(session_id,run_id),UNIQUE(run_id),
 FOREIGN KEY(session_id,run_id,origin_digest) REFERENCES native_execution_origins(session_id,run_id,origin_digest) DEFERRABLE INITIALLY DEFERRED
);
ALTER TABLE runs ADD COLUMN execution_enrollment_origin_id TEXT REFERENCES native_execution_origins(run_id) DEFERRABLE INITIALLY DEFERRED
 CHECK((execution_enrollment_protocol=0 AND execution_enrollment_origin_id IS NULL) OR (execution_enrollment_protocol=1 AND execution_enrollment_origin_id IS NOT NULL AND execution_enrollment_origin_id=id));
CREATE TRIGGER runs_execution_identity BEFORE UPDATE ON runs
 WHEN NEW.id IS NOT OLD.id OR NEW.session_id IS NOT OLD.session_id OR NEW.execution_enrollment_protocol<>OLD.execution_enrollment_protocol OR NEW.execution_enrollment_origin_id IS NOT OLD.execution_enrollment_origin_id
 BEGIN SELECT RAISE(ABORT,'crabber_execution_run_identity_immutable'); END;
CREATE TRIGGER runs_execution_delete BEFORE DELETE ON runs WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id)
 BEGIN SELECT RAISE(ABORT,'crabber_execution_run_identity_immutable'); END;
CREATE TRIGGER runs_execution_insert BEFORE INSERT ON runs BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE(ABORT,'crabber_session_deleted') END;
 -- Cover every unique key even when recursive_triggers is disabled. This also
 -- preserves legacy0 identity against DELETE+INSERT/REPLACE promotion.
 SELECT CASE WHEN EXISTS(SELECT 1 FROM runs WHERE id=NEW.id OR seq=NEW.seq OR (session_id=NEW.session_id AND status IN('pending','running','paused') AND NEW.status IN('pending','running','paused')))
 THEN RAISE(ABORT,'crabber_execution_run_identity_immutable') END;
END;
CREATE TRIGGER execution_origin_insert BEFORE INSERT ON native_execution_origins BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE(ABORT,'crabber_session_deleted') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM native_execution_origins WHERE run_id=NEW.run_id OR (session_id=NEW.session_id AND run_id=NEW.run_id AND origin_digest=NEW.origin_digest)) THEN RAISE(ABORT,'crabber_execution_source_immutable') END;
END;
CREATE TRIGGER execution_origin_update BEFORE UPDATE ON native_execution_origins BEGIN SELECT RAISE(ABORT,'crabber_execution_source_immutable'); END;
CREATE TRIGGER execution_origin_delete BEFORE DELETE ON native_execution_origins WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_execution_source_immutable'); END;
CREATE TRIGGER execution_unclassified_insert BEFORE INSERT ON native_execution_unclassified BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE(ABORT,'crabber_session_deleted') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM native_execution_unclassified WHERE run_id=NEW.run_id) THEN RAISE(ABORT,'crabber_execution_source_immutable') END;
END;
CREATE TRIGGER execution_unclassified_update BEFORE UPDATE ON native_execution_unclassified BEGIN SELECT RAISE(ABORT,'crabber_execution_source_immutable'); END;
CREATE TRIGGER execution_unclassified_delete BEFORE DELETE ON native_execution_unclassified WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_execution_source_immutable'); END;
