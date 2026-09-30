CREATE TABLE IF NOT EXISTS schema_version (version integer PRIMARY KEY);
CREATE TABLE IF NOT EXISTS sessions (id text PRIMARY KEY, data jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS runs (
  id text PRIMARY KEY, session_id text NOT NULL REFERENCES sessions(id),
  status text NOT NULL, claim_token text NOT NULL, lease_until bigint NOT NULL,
  data jsonb NOT NULL, seq bigint GENERATED ALWAYS AS IDENTITY
);
CREATE UNIQUE INDEX IF NOT EXISTS runs_one_active_per_session ON runs(session_id)
  WHERE status IN ('pending', 'running', 'paused');
CREATE INDEX IF NOT EXISTS runs_session_order ON runs(session_id, seq DESC);
CREATE TABLE IF NOT EXISTS messages (
  id text PRIMARY KEY, session_id text NOT NULL REFERENCES sessions(id),
  run_id text REFERENCES runs(id), data jsonb NOT NULL,
  seq bigint GENERATED ALWAYS AS IDENTITY
);
CREATE INDEX IF NOT EXISTS messages_session_order ON messages(session_id, seq);
CREATE TABLE IF NOT EXISTS parts (
  id text PRIMARY KEY, message_id text NOT NULL REFERENCES messages(id),
  ordinal integer NOT NULL, data jsonb NOT NULL,
  UNIQUE(message_id, ordinal)
);
CREATE TABLE IF NOT EXISTS tool_calls (
  id text PRIMARY KEY, run_id text NOT NULL REFERENCES runs(id),
  status text NOT NULL, data jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS tool_calls_run_status ON tool_calls(run_id, status);
CREATE TABLE IF NOT EXISTS epochs (
  id text PRIMARY KEY, session_id text NOT NULL REFERENCES sessions(id),
  run_id text NOT NULL REFERENCES runs(id), data jsonb NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
  seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  session_id text NOT NULL REFERENCES sessions(id), run_id text NOT NULL REFERENCES runs(id),
  data jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS events_session_order ON events(session_id, seq);
CREATE TABLE IF NOT EXISTS inbox (
  seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  session_id text NOT NULL REFERENCES sessions(id), kind text NOT NULL,
  consumed_by_run text REFERENCES runs(id), data jsonb NOT NULL
);
CREATE INDEX IF NOT EXISTS inbox_unconsumed ON inbox(session_id, kind, seq) WHERE consumed_by_run IS NULL;
CREATE TABLE IF NOT EXISTS extension_state (
  session_id text NOT NULL REFERENCES sessions(id), extension_id text NOT NULL,
  key text NOT NULL, value text NOT NULL,
  PRIMARY KEY(session_id, extension_id, key)
);
INSERT INTO schema_version(version) VALUES (1) ON CONFLICT DO NOTHING;
