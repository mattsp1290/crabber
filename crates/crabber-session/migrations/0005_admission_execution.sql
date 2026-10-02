CREATE TABLE IF NOT EXISTS admission_executions (
  run_id text PRIMARY KEY REFERENCES runs(id),
  session_id text NOT NULL,
  admission_key text NOT NULL,
  capsule_version integer NOT NULL CHECK (capsule_version = 1),
  start_state text NOT NULL CHECK (start_state IN ('Unstarted','Started')),
  data jsonb NOT NULL,
  UNIQUE (session_id, admission_key),
  FOREIGN KEY (session_id, admission_key) REFERENCES admission_receipts(session_id, admission_key)
);
INSERT INTO schema_version(version) VALUES (5) ON CONFLICT DO NOTHING;
