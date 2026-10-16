-- DRAFT: registered only after durable run controls8. Owner tables will join
-- this version. Existing native Begin records never gain a fabricated witness.
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
