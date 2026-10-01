CREATE TABLE IF NOT EXISTS abandonment_commits (
  run_id text PRIMARY KEY REFERENCES runs(id),
  event_seq bigint NOT NULL UNIQUE REFERENCES events(seq),
  data jsonb NOT NULL
);
INSERT INTO schema_version(version) VALUES (4) ON CONFLICT DO NOTHING;
