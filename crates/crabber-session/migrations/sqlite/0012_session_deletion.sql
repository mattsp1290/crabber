-- No session/run FK: a deleted ID remains permanently unavailable.
CREATE TABLE session_tombstones (
 session_id TEXT PRIMARY KEY NOT NULL CHECK(length(CAST(session_id AS BLOB)) BETWEEN 1 AND 512),
 operation_id TEXT NOT NULL CHECK(length(CAST(operation_id AS BLOB)) BETWEEN 1 AND 128),
 expected_revision INTEGER NOT NULL CHECK(expected_revision>=0 AND expected_revision<9223372036854775807),
 final_revision INTEGER NOT NULL CHECK(final_revision=expected_revision+1),
 semantic_digest TEXT NOT NULL CHECK(length(semantic_digest)=64 AND semantic_digest NOT GLOB '*[^0-9a-f]*'),
 deleted_at_seconds INTEGER NOT NULL,
 deleted_at_nanos INTEGER NOT NULL CHECK(deleted_at_nanos BETWEEN 0 AND 999999999),
 format_version INTEGER NOT NULL CHECK(format_version=1)
);
CREATE TRIGGER session_tombstone_no_replace BEFORE INSERT ON session_tombstones
 WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id)
 BEGIN SELECT RAISE(ABORT,'crabber_tombstone_immutable'); END;
CREATE TRIGGER session_tombstone_no_update BEFORE UPDATE ON session_tombstones
 BEGIN SELECT RAISE(ABORT,'crabber_tombstone_immutable'); END;
CREATE TRIGGER session_tombstone_no_delete BEFORE DELETE ON session_tombstones
 BEGIN SELECT RAISE(ABORT,'crabber_tombstone_immutable'); END;
CREATE TRIGGER sessions_deleted_guard_0 BEFORE INSERT ON sessions
 WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.id)
 BEGIN SELECT RAISE(ABORT,'crabber_session_deleted'); END;
CREATE TRIGGER sessions_deleted_guard_1 BEFORE UPDATE OF id ON sessions
 WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.id)
 BEGIN SELECT RAISE(ABORT,'crabber_session_deleted'); END;
CREATE TRIGGER session_owner_contexts_deleted_guard_0 BEFORE INSERT ON session_owner_contexts
 WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id)
 BEGIN SELECT RAISE(ABORT,'crabber_session_deleted'); END;
CREATE TRIGGER session_owner_contexts_deleted_guard_1 BEFORE UPDATE OF session_id ON session_owner_contexts
 WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id)
 BEGIN SELECT RAISE(ABORT,'crabber_session_deleted'); END;
CREATE TRIGGER session_owner_current_deleted_guard_0 BEFORE INSERT ON session_owner_current
 WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id)
 BEGIN SELECT RAISE(ABORT,'crabber_session_deleted'); END;
CREATE TRIGGER session_owner_current_deleted_guard_1 BEFORE UPDATE OF session_id ON session_owner_current
 WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id)
 BEGIN SELECT RAISE(ABORT,'crabber_session_deleted'); END;
CREATE TRIGGER session_owner_audits_deleted_guard_0 BEFORE INSERT ON session_owner_audits
 WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id)
 BEGIN SELECT RAISE(ABORT,'crabber_session_deleted'); END;
CREATE TRIGGER session_owner_audits_deleted_guard_1 BEFORE UPDATE OF session_id ON session_owner_audits
 WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id)
 BEGIN SELECT RAISE(ABORT,'crabber_session_deleted'); END;
