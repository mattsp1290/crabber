ALTER TABLE session_owner_current ADD COLUMN source_protocol INTEGER NOT NULL DEFAULT 0 CHECK(source_protocol IN (0,1));
CREATE UNIQUE INDEX session_owner_protocol_scope ON session_owner_current(session_id,owner_id,source_protocol);
CREATE TRIGGER session_owner_protocol_immutable BEFORE UPDATE OF source_protocol ON session_owner_current WHEN NEW.source_protocol<>OLD.source_protocol BEGIN SELECT RAISE(ABORT,'crabber_owner_protocol_immutable'); END;
CREATE TRIGGER session_owner_protocol_insert_immutable BEFORE INSERT ON session_owner_current WHEN EXISTS(SELECT 1 FROM session_owner_current WHERE session_id=NEW.session_id AND owner_id=NEW.owner_id AND source_protocol<>NEW.source_protocol) BEGIN SELECT RAISE(ABORT,'crabber_owner_protocol_immutable'); END;
CREATE TABLE session_owner_publication_cohorts (
 session_id TEXT NOT NULL, owner_id TEXT NOT NULL CHECK(length(CAST(owner_id AS BLOB))<=128),
 protocol_version INTEGER NOT NULL DEFAULT 1 CHECK(protocol_version=1),
 origin_id TEXT NOT NULL CHECK(length(CAST(origin_id AS BLOB))<=128), origin_digest TEXT NOT NULL CHECK(length(origin_digest)=64),
 head_sequence INTEGER NOT NULL CHECK(head_sequence>0), head_digest TEXT NOT NULL CHECK(length(head_digest)=64),
 PRIMARY KEY(session_id,owner_id),
 FOREIGN KEY(session_id,owner_id,protocol_version) REFERENCES session_owner_current(session_id,owner_id,source_protocol)
);
CREATE TABLE session_owner_publications (
 session_id TEXT NOT NULL, owner_id TEXT NOT NULL, operation_id TEXT NOT NULL CHECK(length(CAST(operation_id AS BLOB))<=128),
 sequence INTEGER NOT NULL CHECK(sequence>0), operation text NOT NULL CHECK(length(operation)<=32), state text NOT NULL CHECK(length(state)<=32), audit_digest TEXT NOT NULL CHECK(length(audit_digest)=64),
 data_json TEXT NOT NULL CHECK(length(CAST(data_json AS BLOB))<=32768),
 PRIMARY KEY(session_id,operation_id), UNIQUE(session_id,owner_id,sequence),
 FOREIGN KEY(session_id,owner_id) REFERENCES session_owner_publication_cohorts(session_id,owner_id)
);
CREATE TRIGGER session_owner_publication_no_replace BEFORE INSERT ON session_owner_publications WHEN EXISTS(SELECT 1 FROM session_owner_publications WHERE session_id=NEW.session_id AND (operation_id=NEW.operation_id OR (owner_id=NEW.owner_id AND sequence=NEW.sequence))) BEGIN SELECT RAISE(ABORT,'crabber_owner_publication_immutable'); END;
CREATE TRIGGER session_owner_publication_no_update BEFORE UPDATE ON session_owner_publications BEGIN SELECT RAISE(ABORT,'crabber_owner_publication_immutable'); END;
CREATE TRIGGER session_owner_publication_no_delete BEFORE DELETE ON session_owner_publications WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_owner_publication_immutable'); END;
CREATE TRIGGER session_owner_cohort_insert BEFORE INSERT ON session_owner_publication_cohorts WHEN NEW.head_sequence<>1 OR NEW.head_digest<>NEW.origin_digest OR EXISTS(SELECT 1 FROM session_owner_publication_cohorts WHERE session_id=NEW.session_id AND owner_id=NEW.owner_id) BEGIN SELECT RAISE(ABORT,'crabber_owner_cohort_immutable'); END;
CREATE TRIGGER session_owner_cohort_no_delete BEFORE DELETE ON session_owner_publication_cohorts WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_owner_cohort_immutable'); END;
CREATE TRIGGER session_owner_cohort_monotonic BEFORE UPDATE ON session_owner_publication_cohorts WHEN NEW.session_id<>OLD.session_id OR NEW.owner_id<>OLD.owner_id OR NEW.protocol_version<>OLD.protocol_version OR NEW.origin_id<>OLD.origin_id OR NEW.origin_digest<>OLD.origin_digest OR NEW.head_sequence<>OLD.head_sequence+1 OR NOT EXISTS(SELECT 1 FROM session_owner_publications p WHERE p.session_id=NEW.session_id AND p.owner_id=NEW.owner_id AND p.sequence=NEW.head_sequence AND p.audit_digest=NEW.head_digest) BEGIN SELECT RAISE(ABORT,'crabber_owner_cohort_immutable'); END;
-- Actual new scope creation alone opts in; migration never upgrades old scopes.
ALTER TABLE sessions ADD COLUMN owner_scope_protocol INTEGER NOT NULL DEFAULT 0 CHECK(owner_scope_protocol IN (0,1));
ALTER TABLE session_owner_contexts ADD COLUMN scope_protocol INTEGER NOT NULL DEFAULT 0 CHECK(scope_protocol IN (0,1));
CREATE TRIGGER sessions_owner_scope_immutable BEFORE UPDATE OF owner_scope_protocol ON sessions WHEN NEW.owner_scope_protocol<>OLD.owner_scope_protocol BEGIN SELECT RAISE(ABORT,'crabber_owner_scope_immutable'); END;
CREATE TRIGGER sessions_owner_scope_insert BEFORE INSERT ON sessions WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.id) AND EXISTS(SELECT 1 FROM sessions WHERE id=NEW.id AND owner_scope_protocol<>NEW.owner_scope_protocol) BEGIN SELECT RAISE(ABORT,'crabber_owner_scope_immutable'); END;
CREATE TRIGGER contexts_owner_scope_immutable BEFORE UPDATE OF scope_protocol ON session_owner_contexts WHEN NEW.scope_protocol<>OLD.scope_protocol BEGIN SELECT RAISE(ABORT,'crabber_owner_scope_immutable'); END;
CREATE TRIGGER contexts_owner_scope_insert BEFORE INSERT ON session_owner_contexts WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) AND EXISTS(SELECT 1 FROM session_owner_contexts WHERE session_id=NEW.session_id AND scope_protocol<>NEW.scope_protocol) BEGIN SELECT RAISE(ABORT,'crabber_owner_scope_immutable'); END;
CREATE TABLE session_owner_scope_origins(session_id TEXT PRIMARY KEY,data_json TEXT NOT NULL CHECK(length(CAST(data_json AS BLOB))<=32768));
CREATE TRIGGER owner_scope_origin_insert BEFORE INSERT ON session_owner_scope_origins WHEN EXISTS(SELECT 1 FROM session_owner_scope_origins WHERE session_id=NEW.session_id) OR EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) BEGIN SELECT RAISE(ABORT,'crabber_owner_scope_immutable'); END;
CREATE TRIGGER owner_scope_origin_immutable BEFORE UPDATE ON session_owner_scope_origins BEGIN SELECT RAISE(ABORT,'crabber_owner_scope_immutable'); END;
CREATE TRIGGER owner_scope_origin_delete BEFORE DELETE ON session_owner_scope_origins WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_owner_scope_immutable'); END;
