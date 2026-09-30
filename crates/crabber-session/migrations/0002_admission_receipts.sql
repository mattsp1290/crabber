CREATE TABLE IF NOT EXISTS admission_receipts (
  session_id text NOT NULL REFERENCES sessions(id),
  admission_key text NOT NULL,
  run_id text NOT NULL UNIQUE REFERENCES runs(id),
  user_message_id text NOT NULL UNIQUE REFERENCES messages(id),
  data jsonb NOT NULL,
  PRIMARY KEY (session_id, admission_key)
);
INSERT INTO schema_version(version) VALUES (2) ON CONFLICT DO NOTHING;
