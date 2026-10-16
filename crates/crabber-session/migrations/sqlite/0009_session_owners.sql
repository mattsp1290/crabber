-- Native evidence and independently fenced pre-session ownership.
-- Existing native Begin records never gain a fabricated witness.
CREATE TABLE IF NOT EXISTS native_parent_witnesses (
  event_seq INTEGER PRIMARY KEY REFERENCES events(seq),
  session_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  kind TEXT NOT NULL CHECK(kind IN ('attempt','phase')),
  operation_id TEXT NOT NULL,
  data_json TEXT NOT NULL,
  UNIQUE(session_id,operation_id),
  FOREIGN KEY(session_id,run_id) REFERENCES runs(session_id,id)
);
CREATE TABLE IF NOT EXISTS native_run_issuances (
  session_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  sequence INTEGER NOT NULL CHECK(sequence>0),
  data_json TEXT NOT NULL,
  PRIMARY KEY(session_id,run_id,sequence),
  FOREIGN KEY(session_id,run_id) REFERENCES runs(session_id,id)
);


-- No sessions/runs foreign key: preparation precedes their creation.
CREATE TABLE IF NOT EXISTS session_owner_contexts (
 session_id TEXT PRIMARY KEY,
 data_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS session_owner_current (
 session_id TEXT NOT NULL,
 owner_id TEXT NOT NULL,
 data_json TEXT NOT NULL,
 PRIMARY KEY(session_id,owner_id)
);
CREATE TABLE IF NOT EXISTS session_owner_audits (
 ordinal INTEGER PRIMARY KEY,
 session_id TEXT NOT NULL,
 operation_id TEXT NOT NULL,
 owner_id TEXT NOT NULL,
 sequence BIGINT NOT NULL CHECK(sequence>0),
 operation TEXT NOT NULL,
 state TEXT NOT NULL,
 data_json TEXT NOT NULL,
 UNIQUE(session_id,operation_id),
 UNIQUE(session_id,owner_id,sequence)
);
CREATE INDEX IF NOT EXISTS session_owner_latest ON session_owner_audits(session_id,owner_id,sequence DESC);
CREATE INDEX IF NOT EXISTS session_owner_origins ON session_owner_audits(session_id,operation,ordinal);
