-- Fresh births alone enter this era; migration never promotes retained rows.
ALTER TABLE sessions ADD COLUMN native_run_protocol INTEGER NOT NULL DEFAULT 0 CHECK(native_run_protocol IN(0,1));
ALTER TABLE session_owner_contexts ADD COLUMN native_run_protocol INTEGER NOT NULL DEFAULT 0 CHECK(native_run_protocol IN(0,1));
ALTER TABLE runs ADD COLUMN native_source_protocol INTEGER NOT NULL DEFAULT 0 CHECK(native_source_protocol IN(0,1));
CREATE UNIQUE INDEX runs_native_source_scope ON runs(session_id,id,native_source_protocol);
CREATE TRIGGER sessions_native_identity BEFORE UPDATE ON sessions WHEN NEW.id IS NOT OLD.id OR NEW.native_run_protocol<>OLD.native_run_protocol OR NEW.public_revision<OLD.public_revision BEGIN SELECT RAISE(ABORT,'crabber_session_identity_immutable'); END;
CREATE TRIGGER sessions_native_insert BEFORE INSERT ON sessions WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.id) AND EXISTS(SELECT 1 FROM sessions WHERE id=NEW.id AND (native_run_protocol<>NEW.native_run_protocol OR public_revision>NEW.public_revision)) BEGIN SELECT RAISE(ABORT,'crabber_session_identity_immutable'); END;
CREATE TRIGGER sessions_native_delete BEFORE DELETE ON sessions WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.id) BEGIN SELECT RAISE(ABORT,'crabber_session_identity_immutable'); END;
CREATE TRIGGER contexts_native_identity BEFORE UPDATE ON session_owner_contexts WHEN NEW.session_id IS NOT OLD.session_id OR NEW.native_run_protocol<>OLD.native_run_protocol BEGIN SELECT RAISE(ABORT,'crabber_context_identity_immutable'); END;
CREATE TRIGGER contexts_native_insert BEFORE INSERT ON session_owner_contexts WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) AND EXISTS(SELECT 1 FROM session_owner_contexts WHERE session_id=NEW.session_id AND native_run_protocol<>NEW.native_run_protocol) BEGIN SELECT RAISE(ABORT,'crabber_context_identity_immutable'); END;
CREATE TRIGGER contexts_native_delete BEFORE DELETE ON session_owner_contexts WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_context_identity_immutable'); END;
CREATE TRIGGER runs_native_identity BEFORE UPDATE ON runs WHEN NEW.id IS NOT OLD.id OR NEW.session_id IS NOT OLD.session_id OR NEW.native_source_protocol<>OLD.native_source_protocol BEGIN SELECT RAISE(ABORT,'crabber_native_run_protocol_immutable'); END;
CREATE TRIGGER runs_native_delete BEFORE DELETE ON runs WHEN OLD.native_source_protocol=1 AND NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_native_run_identity_immutable'); END;
CREATE TRIGGER runs_native_insert BEFORE INSERT ON runs BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE(ABORT,'crabber_session_deleted') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM runs WHERE native_source_protocol=1 AND (id=NEW.id OR seq=NEW.seq OR (session_id=NEW.session_id AND status IN ('pending','running','paused') AND NEW.status IN ('pending','running','paused')))) THEN RAISE(ABORT,'crabber_native_run_identity_immutable') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM runs WHERE id=NEW.id AND (session_id<>NEW.session_id OR native_source_protocol<>NEW.native_source_protocol)) THEN RAISE(ABORT,'crabber_native_run_protocol_immutable') END;
 SELECT CASE WHEN NEW.native_source_protocol=0 AND EXISTS(SELECT 1 FROM sessions WHERE id=NEW.session_id AND native_run_protocol=1) THEN RAISE(ABORT,'crabber_native_run_protocol_required') END;
END;
CREATE TABLE native_run_sources (
 session_id TEXT NOT NULL CHECK(length(CAST(session_id AS BLOB)) BETWEEN 1 AND 512),run_id TEXT NOT NULL CHECK(length(CAST(run_id AS BLOB)) BETWEEN 1 AND 128),
 sequence INTEGER NOT NULL CHECK(typeof(sequence)='integer' AND sequence>0),protocol_version INTEGER NOT NULL DEFAULT 1 CHECK(protocol_version=1),
 issuance_digest TEXT NOT NULL CHECK(length(issuance_digest)=64 AND issuance_digest NOT GLOB '*[^0-9a-f]*'),
 previous_digest TEXT CHECK(length(previous_digest)=64 AND previous_digest NOT GLOB '*[^0-9a-f]*'),
 chain_digest TEXT NOT NULL CHECK(length(chain_digest)=64 AND chain_digest NOT GLOB '*[^0-9a-f]*'),
 CHECK((sequence=1)=(previous_digest IS NULL)),
 PRIMARY KEY(session_id,run_id,sequence),UNIQUE(run_id,sequence),UNIQUE(session_id,run_id,chain_digest),UNIQUE(session_id,run_id,sequence,chain_digest),
 FOREIGN KEY(session_id,run_id,protocol_version) REFERENCES runs(session_id,id,native_source_protocol)
);
CREATE TABLE native_run_source_heads (
 session_id TEXT NOT NULL,run_id TEXT NOT NULL,sequence INTEGER NOT NULL CHECK(typeof(sequence)='integer' AND sequence>0),
 protocol_version INTEGER NOT NULL DEFAULT 1 CHECK(protocol_version=1),
 origin_digest TEXT NOT NULL CHECK(length(origin_digest)=64 AND origin_digest NOT GLOB '*[^0-9a-f]*'),
 chain_digest TEXT NOT NULL CHECK(length(chain_digest)=64 AND chain_digest NOT GLOB '*[^0-9a-f]*'),
 PRIMARY KEY(session_id,run_id),UNIQUE(run_id),
 FOREIGN KEY(session_id,run_id,origin_digest) REFERENCES native_run_sources(session_id,run_id,chain_digest),
 FOREIGN KEY(session_id,run_id,sequence,chain_digest) REFERENCES native_run_sources(session_id,run_id,sequence,chain_digest)
);
CREATE TRIGGER native_run_source_insert BEFORE INSERT ON native_run_sources BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE(ABORT,'crabber_session_deleted') END;
 -- Cover every unique conflict even when recursive_triggers is disabled.
 SELECT CASE WHEN EXISTS(SELECT 1 FROM native_run_sources WHERE (run_id=NEW.run_id AND sequence=NEW.sequence) OR (session_id=NEW.session_id AND run_id=NEW.run_id AND chain_digest=NEW.chain_digest)) THEN RAISE(ABORT,'crabber_native_source_immutable') END;
 SELECT CASE WHEN NEW.sequence=1 AND (NEW.previous_digest IS NOT NULL OR EXISTS(SELECT 1 FROM native_run_source_heads WHERE run_id=NEW.run_id) OR EXISTS(SELECT 1 FROM native_run_sources WHERE run_id=NEW.run_id)) THEN RAISE(ABORT,'crabber_native_source_origin') END;
 SELECT CASE WHEN NEW.sequence<>1 AND NOT EXISTS(SELECT 1 FROM native_run_source_heads WHERE session_id=NEW.session_id AND run_id=NEW.run_id AND sequence<9223372036854775807 AND NEW.sequence=sequence+1 AND NEW.previous_digest=chain_digest) THEN RAISE(ABORT,'crabber_native_source_sequence') END;
END;
CREATE TRIGGER native_run_source_update BEFORE UPDATE ON native_run_sources BEGIN SELECT RAISE(ABORT,'crabber_native_source_immutable'); END;
CREATE TRIGGER native_run_source_delete BEFORE DELETE ON native_run_sources WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_native_source_immutable'); END;
CREATE TRIGGER native_run_head_insert BEFORE INSERT ON native_run_source_heads BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE(ABORT,'crabber_session_deleted') END;
 SELECT CASE WHEN NEW.sequence<>1 OR NEW.origin_digest<>NEW.chain_digest OR EXISTS(SELECT 1 FROM native_run_source_heads WHERE run_id=NEW.run_id) THEN RAISE(ABORT,'crabber_native_head_origin') END;
END;
CREATE TRIGGER native_run_head_update BEFORE UPDATE ON native_run_source_heads WHEN NEW.session_id IS NOT OLD.session_id OR NEW.run_id<>OLD.run_id OR NEW.protocol_version<>OLD.protocol_version OR NEW.origin_digest<>OLD.origin_digest OR OLD.sequence=9223372036854775807 OR NEW.sequence<>OLD.sequence+1 OR NOT EXISTS(SELECT 1 FROM native_run_sources WHERE session_id=NEW.session_id AND run_id=NEW.run_id AND sequence=NEW.sequence AND chain_digest=NEW.chain_digest AND previous_digest=OLD.chain_digest) BEGIN SELECT RAISE(ABORT,'crabber_native_head_sequence'); END;
CREATE TRIGGER native_run_head_delete BEFORE DELETE ON native_run_source_heads WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_native_head_immutable'); END;
CREATE TRIGGER native_run_source_publish AFTER INSERT ON native_run_sources BEGIN
 INSERT INTO native_run_source_heads(session_id,run_id,sequence,origin_digest,chain_digest) SELECT NEW.session_id,NEW.run_id,1,NEW.chain_digest,NEW.chain_digest WHERE NEW.sequence=1;
 UPDATE native_run_source_heads SET sequence=NEW.sequence,chain_digest=NEW.chain_digest WHERE session_id=NEW.session_id AND run_id=NEW.run_id AND NEW.sequence>1;
END;
