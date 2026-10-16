-- Fresh databases only. Imported provenance has no source foreign keys.
DROP TABLE operation_receipts;
CREATE TABLE operation_receipts (
 session_id text NOT NULL REFERENCES sessions(id), operation_id text NOT NULL,
 kind text NOT NULL, semantic_digest text NOT NULL CHECK(length(semantic_digest)=64),
 event_seq integer, data_json text NOT NULL,
 PRIMARY KEY(session_id,operation_id), FOREIGN KEY(session_id,event_seq) REFERENCES events(session_id,seq),
 CHECK((kind IN ('session_management','future_queue','session_fork') AND event_seq IS NULL) OR (kind NOT IN ('session_management','future_queue','session_fork') AND event_seq IS NOT NULL))
);
CREATE TABLE tool_call_identities (
 id text PRIMARY KEY,
 kind text NOT NULL CHECK(kind IN ('native','imported'))
);
-- This schema is constructed only for fresh databases, before any records exist.
DROP TABLE tool_calls;
CREATE TABLE tool_calls (
  seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE REFERENCES tool_call_identities(id),
  run_id TEXT NOT NULL REFERENCES runs(id), status TEXT NOT NULL, data TEXT NOT NULL,
  snapshot_parts INTEGER NOT NULL, snapshot_text INTEGER NOT NULL,
  snapshot_bytes INTEGER NOT NULL
);
CREATE INDEX tool_calls_run_status ON tool_calls(run_id, status);
CREATE UNIQUE INDEX calls_run_identity ON tool_calls(run_id,id);
CREATE TRIGGER tool_calls_accounting_insert BEFORE INSERT ON tool_calls BEGIN
  SELECT RAISE(ABORT, 'snapshot accounting mismatch')
    WHERE length(CAST(NEW.data AS BLOB)) <> NEW.snapshot_bytes;
END;
CREATE TRIGGER tool_calls_accounting_update BEFORE UPDATE ON tool_calls BEGIN
  SELECT RAISE(ABORT, 'snapshot accounting mismatch')
    WHERE length(CAST(NEW.data AS BLOB)) <> NEW.snapshot_bytes;
END;
CREATE TRIGGER tool_calls_revision_update BEFORE UPDATE ON tool_calls BEGIN
  UPDATE sessions SET snapshot_revision = snapshot_revision + 1
    WHERE id = (SELECT session_id FROM runs WHERE id = OLD.run_id);
END;
CREATE TRIGGER tool_calls_revision_delete BEFORE DELETE ON tool_calls BEGIN
  UPDATE sessions SET snapshot_revision = snapshot_revision + 1
    WHERE id = (SELECT session_id FROM runs WHERE id = OLD.run_id);
END;
CREATE TABLE imported_tool_calls (
 id text PRIMARY KEY REFERENCES tool_call_identities(id), session_id text NOT NULL REFERENCES sessions(id),
 ordinal bigint NOT NULL CHECK(ordinal>0), data_json text NOT NULL,
 snapshot_parts bigint NOT NULL CHECK(snapshot_parts=0),
 snapshot_text bigint NOT NULL CHECK(snapshot_text>=0),
 snapshot_bytes bigint NOT NULL CHECK(snapshot_bytes>0 AND snapshot_bytes<=8388608),
 UNIQUE(session_id,ordinal)
);
CREATE TABLE imported_message_provenance (
 message_id text PRIMARY KEY REFERENCES messages(id),
 session_id text NOT NULL REFERENCES sessions(id),
 ordinal bigint NOT NULL CHECK(ordinal>0), data_json text NOT NULL,
 snapshot_bytes bigint NOT NULL CHECK(snapshot_bytes>0 AND snapshot_bytes<=8388608),
 UNIQUE(session_id,ordinal)
);
CREATE TABLE session_fork_audit (
 session_id text PRIMARY KEY REFERENCES sessions(id), operation_id text NOT NULL,
 data_json text NOT NULL,
 FOREIGN KEY(session_id,operation_id) REFERENCES operation_receipts(session_id,operation_id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TRIGGER native_call_identity_insert BEFORE INSERT ON tool_calls BEGIN
 SELECT RAISE(ABORT,'tool call identity already reserved') WHERE EXISTS(SELECT 1 FROM tool_call_identities WHERE id=NEW.id AND kind<>'native');
 INSERT INTO tool_call_identities(id,kind) SELECT NEW.id,'native' WHERE NOT EXISTS(SELECT 1 FROM tool_call_identities WHERE id=NEW.id);
END;

CREATE TRIGGER native_call_identity_update BEFORE UPDATE OF id ON tool_calls WHEN NEW.id <> OLD.id BEGIN
 SELECT RAISE(ABORT,'tool call identity already reserved') WHERE EXISTS(SELECT 1 FROM tool_call_identities WHERE id=NEW.id AND kind<>'native');
 INSERT INTO tool_call_identities(id,kind) SELECT NEW.id,'native' WHERE NOT EXISTS(SELECT 1 FROM tool_call_identities WHERE id=NEW.id);
END;
CREATE TRIGGER native_call_identity_delete AFTER DELETE ON tool_calls BEGIN
 DELETE FROM tool_call_identities WHERE id=OLD.id AND kind='native';
END;
CREATE TRIGGER native_call_identity_release AFTER UPDATE OF id ON tool_calls WHEN NEW.id<>OLD.id BEGIN
 DELETE FROM tool_call_identities WHERE id=OLD.id AND kind='native';
END;

CREATE TRIGGER imported_call_identity_insert BEFORE INSERT ON imported_tool_calls BEGIN
 SELECT RAISE(ABORT,'tool call identity already reserved') WHERE EXISTS(SELECT 1 FROM tool_call_identities WHERE id=NEW.id AND kind<>'imported');
 INSERT INTO tool_call_identities(id,kind) SELECT NEW.id,'imported' WHERE NOT EXISTS(SELECT 1 FROM tool_call_identities WHERE id=NEW.id);
END;

CREATE TRIGGER imported_call_identity_update BEFORE UPDATE OF id ON imported_tool_calls WHEN NEW.id <> OLD.id BEGIN
 SELECT RAISE(ABORT,'tool call identity already reserved') WHERE EXISTS(SELECT 1 FROM tool_call_identities WHERE id=NEW.id AND kind<>'imported');
 INSERT INTO tool_call_identities(id,kind) SELECT NEW.id,'imported' WHERE NOT EXISTS(SELECT 1 FROM tool_call_identities WHERE id=NEW.id);
END;
CREATE TRIGGER imported_call_identity_delete AFTER DELETE ON imported_tool_calls BEGIN
 DELETE FROM tool_call_identities WHERE id=OLD.id AND kind='imported';
END;
CREATE TRIGGER imported_call_identity_release AFTER UPDATE OF id ON imported_tool_calls WHEN NEW.id<>OLD.id BEGIN
 DELETE FROM tool_call_identities WHERE id=OLD.id AND kind='imported';
END;
