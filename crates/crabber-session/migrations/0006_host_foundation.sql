-- Additive host foundation. Retained execution rows are never rewritten or
-- inspected; the runtime separately rejects unsupported capsule/checkpoint formats.
ALTER TABLE sessions ADD COLUMN IF NOT EXISTS public_revision bigint NOT NULL DEFAULT 0 CHECK (public_revision >= 0);
CREATE UNIQUE INDEX IF NOT EXISTS runs_session_identity ON runs(session_id,id);
CREATE UNIQUE INDEX IF NOT EXISTS calls_run_identity ON tool_calls(run_id,id);
CREATE UNIQUE INDEX IF NOT EXISTS events_session_identity ON events(session_id,seq);

-- JSON TEXT is deliberate: arbitrary public/private JSON may contain escaped
-- NUL or number forms that cannot be retained losslessly as PostgreSQL JSONB.
CREATE TABLE IF NOT EXISTS session_public_state (
  session_id text PRIMARY KEY REFERENCES sessions(id),
  format_version integer NOT NULL CHECK (format_version = 1),
  state_json text NOT NULL,
  activities_json text NOT NULL
);
CREATE TABLE IF NOT EXISTS run_execution_bindings (
  run_id text PRIMARY KEY REFERENCES runs(id),
  capsule_version integer NOT NULL CHECK (capsule_version = 2),
  data_json text NOT NULL
);
CREATE TABLE IF NOT EXISTS execution_attempts (
  id text PRIMARY KEY,
  run_id text NOT NULL REFERENCES runs(id),
  generation bigint NOT NULL CHECK (generation > 0),
  owner text NOT NULL,
  claim_token text NOT NULL,
  run_claim_token text NOT NULL,
  lease_until bigint NOT NULL,
  state text NOT NULL CHECK (state IN ('Unstarted','Started','Suspended','Completed','Failed','Interrupted','OutcomeUnknown')),
  cleanup_state text NOT NULL CHECK (cleanup_state IN ('Pending','Complete','Incomplete')),
  outcome_json text,
  UNIQUE(run_id,generation),
  UNIQUE(run_id,id)
);
CREATE UNIQUE INDEX IF NOT EXISTS attempts_one_open_owner ON execution_attempts(run_id)
  WHERE cleanup_state <> 'Complete' OR state IN ('Unstarted','Started','OutcomeUnknown');
CREATE TABLE IF NOT EXISTS tool_phases (
  id text PRIMARY KEY,
  run_id text NOT NULL REFERENCES runs(id),
  call_id text NOT NULL,
  attempt_id text NOT NULL,
  generation bigint NOT NULL CHECK (generation > 0),
  claim_token text NOT NULL,
  state text NOT NULL CHECK (state IN ('Claimed','Started','Suspended','Completed','Failed','Uncertain')),
  quiescence_state text NOT NULL CHECK (quiescence_state IN ('Pending','Complete','Incomplete')),
  outcome_json text,
  UNIQUE(run_id,call_id,generation),
  FOREIGN KEY(run_id,call_id) REFERENCES tool_calls(run_id,id),
  FOREIGN KEY(run_id,attempt_id) REFERENCES execution_attempts(run_id,id)
);
CREATE UNIQUE INDEX IF NOT EXISTS phases_one_open_owner ON tool_phases(run_id,call_id)
  WHERE quiescence_state <> 'Complete' OR state IN ('Claimed','Started','Uncertain');
CREATE TABLE IF NOT EXISTS operation_receipts (
  session_id text NOT NULL REFERENCES sessions(id),
  operation_id text NOT NULL,
  kind text NOT NULL,
  semantic_digest text NOT NULL CHECK (length(semantic_digest) = 64),
  event_seq bigint NOT NULL,
  data_json text NOT NULL,
  PRIMARY KEY(session_id,operation_id),
  FOREIGN KEY(session_id,event_seq) REFERENCES events(session_id,seq)
);
CREATE TABLE IF NOT EXISTS session_invocations (
  id text PRIMARY KEY,
  session_id text NOT NULL REFERENCES sessions(id),
  parent_run_id text NOT NULL,
  parent_call_id text,
  generation bigint NOT NULL CHECK (generation > 0),
  owner text NOT NULL,
  claim_token text NOT NULL,
  lease_until bigint NOT NULL,
  state text NOT NULL CHECK (state IN ('Unstarted','Running','Completed','Failed','Interrupted','Uncertain')),
  cleanup_state text NOT NULL CHECK (cleanup_state IN ('Pending','Complete','Incomplete')),
  data_json text NOT NULL,
  FOREIGN KEY(session_id,parent_run_id) REFERENCES runs(session_id,id),
  FOREIGN KEY(parent_run_id,parent_call_id) REFERENCES tool_calls(run_id,id)
);
CREATE INDEX IF NOT EXISTS invocations_session_owner ON session_invocations(session_id,state,cleanup_state);
INSERT INTO schema_version(version) VALUES (6) ON CONFLICT DO NOTHING;
