-- DRAFT: registered only after durable run controls12. Owner tables will join
-- this version. Existing native Begin records never gain a fabricated witness.
CREATE TABLE IF NOT EXISTS native_parent_witnesses (
  event_seq bigint PRIMARY KEY REFERENCES events(seq),
  session_id text NOT NULL,
  run_id text NOT NULL,
  kind text NOT NULL CHECK(kind IN ('attempt','phase')),
  operation_id text NOT NULL,
  data_json text NOT NULL,
  UNIQUE(session_id,operation_id),
  FOREIGN KEY(session_id,run_id) REFERENCES runs(session_id,id)
);
CREATE TABLE IF NOT EXISTS native_run_issuances (
  session_id text NOT NULL,
  run_id text NOT NULL,
  sequence bigint NOT NULL CHECK(sequence>0),
  data_json text NOT NULL,
  PRIMARY KEY(session_id,run_id,sequence),
  FOREIGN KEY(session_id,run_id) REFERENCES runs(session_id,id)
);
INSERT INTO schema_version(version) VALUES(13) ON CONFLICT DO NOTHING;
