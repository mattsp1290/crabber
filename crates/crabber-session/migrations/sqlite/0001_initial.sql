CREATE TABLE schema_version (version INTEGER PRIMARY KEY);
CREATE TABLE sessions (
  id TEXT NOT NULL PRIMARY KEY, data TEXT NOT NULL,
  snapshot_revision INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE runs (
  seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE,
  session_id TEXT NOT NULL REFERENCES sessions(id), status TEXT NOT NULL,
  claim_token TEXT NOT NULL, lease_until INTEGER NOT NULL, data TEXT NOT NULL
);
CREATE UNIQUE INDEX runs_one_active_per_session ON runs(session_id)
  WHERE status IN ('pending', 'running', 'paused');
CREATE INDEX runs_session_order ON runs(session_id, seq DESC);
CREATE TABLE messages (
  seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE,
  session_id TEXT NOT NULL REFERENCES sessions(id), run_id TEXT REFERENCES runs(id),
  data TEXT NOT NULL, snapshot_parts INTEGER NOT NULL,
  snapshot_text INTEGER NOT NULL, snapshot_bytes INTEGER NOT NULL
);
CREATE INDEX messages_session_order ON messages(session_id, seq);
CREATE TABLE parts (
  id TEXT NOT NULL PRIMARY KEY, message_id TEXT NOT NULL REFERENCES messages(id),
  ordinal INTEGER NOT NULL, data TEXT NOT NULL, UNIQUE(message_id, ordinal)
);
CREATE TABLE tool_calls (
  seq INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT NOT NULL UNIQUE,
  run_id TEXT NOT NULL REFERENCES runs(id), status TEXT NOT NULL, data TEXT NOT NULL,
  snapshot_parts INTEGER NOT NULL, snapshot_text INTEGER NOT NULL,
  snapshot_bytes INTEGER NOT NULL
);
CREATE INDEX tool_calls_run_status ON tool_calls(run_id, status);
CREATE TABLE epochs (
  id TEXT NOT NULL PRIMARY KEY, session_id TEXT NOT NULL REFERENCES sessions(id),
  run_id TEXT NOT NULL REFERENCES runs(id), data TEXT NOT NULL
);
CREATE TABLE events (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL REFERENCES sessions(id),
  run_id TEXT NOT NULL REFERENCES runs(id), data TEXT NOT NULL
);
CREATE INDEX events_session_order ON events(session_id, seq);
CREATE TABLE inbox (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id TEXT NOT NULL REFERENCES sessions(id), kind TEXT NOT NULL,
  consumed_by_run TEXT REFERENCES runs(id), data TEXT NOT NULL
);
CREATE INDEX inbox_unconsumed ON inbox(session_id, kind, seq)
  WHERE consumed_by_run IS NULL;
CREATE TABLE extension_state (
  session_id TEXT NOT NULL REFERENCES sessions(id), extension_id TEXT NOT NULL,
  key TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY(session_id, extension_id, key)
);
CREATE TABLE admission_receipts (
  session_id TEXT NOT NULL REFERENCES sessions(id), admission_key TEXT NOT NULL,
  run_id TEXT NOT NULL UNIQUE REFERENCES runs(id),
  user_message_id TEXT NOT NULL UNIQUE REFERENCES messages(id), data TEXT NOT NULL,
  PRIMARY KEY(session_id, admission_key)
);
CREATE TABLE snapshot_auth (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1), secret TEXT NOT NULL
);
CREATE TABLE abandonment_commits (
  run_id TEXT NOT NULL PRIMARY KEY REFERENCES runs(id),
  event_seq INTEGER NOT NULL UNIQUE REFERENCES events(seq), data TEXT NOT NULL
);
CREATE TABLE admission_executions (
  run_id TEXT NOT NULL PRIMARY KEY REFERENCES runs(id), session_id TEXT NOT NULL,
  admission_key TEXT NOT NULL,
  capsule_version INTEGER NOT NULL CHECK (capsule_version = 1),
  start_state TEXT NOT NULL CHECK (start_state IN ('Unstarted','Started')),
  data TEXT NOT NULL, UNIQUE(session_id, admission_key),
  FOREIGN KEY (session_id, admission_key)
    REFERENCES admission_receipts(session_id, admission_key)
);

CREATE TRIGGER messages_accounting_insert BEFORE INSERT ON messages BEGIN
  SELECT RAISE(ABORT, 'snapshot accounting mismatch')
    WHERE length(CAST(NEW.data AS BLOB)) <> NEW.snapshot_bytes;
END;
CREATE TRIGGER messages_accounting_update BEFORE UPDATE ON messages BEGIN
  SELECT RAISE(ABORT, 'snapshot accounting mismatch')
    WHERE length(CAST(NEW.data AS BLOB)) <> NEW.snapshot_bytes;
END;
CREATE TRIGGER tool_calls_accounting_insert BEFORE INSERT ON tool_calls BEGIN
  SELECT RAISE(ABORT, 'snapshot accounting mismatch')
    WHERE length(CAST(NEW.data AS BLOB)) <> NEW.snapshot_bytes;
END;
CREATE TRIGGER tool_calls_accounting_update BEFORE UPDATE ON tool_calls BEGIN
  SELECT RAISE(ABORT, 'snapshot accounting mismatch')
    WHERE length(CAST(NEW.data AS BLOB)) <> NEW.snapshot_bytes;
END;
CREATE TRIGGER messages_revision_update BEFORE UPDATE ON messages BEGIN
  UPDATE sessions SET snapshot_revision = snapshot_revision + 1
    WHERE id = OLD.session_id;
END;
CREATE TRIGGER messages_revision_delete BEFORE DELETE ON messages BEGIN
  UPDATE sessions SET snapshot_revision = snapshot_revision + 1
    WHERE id = OLD.session_id;
END;
CREATE TRIGGER tool_calls_revision_update BEFORE UPDATE ON tool_calls BEGIN
  UPDATE sessions SET snapshot_revision = snapshot_revision + 1
    WHERE id = (SELECT session_id FROM runs WHERE id = OLD.run_id);
END;
CREATE TRIGGER tool_calls_revision_delete BEFORE DELETE ON tool_calls BEGIN
  UPDATE sessions SET snapshot_revision = snapshot_revision + 1
    WHERE id = (SELECT session_id FROM runs WHERE id = OLD.run_id);
END;
