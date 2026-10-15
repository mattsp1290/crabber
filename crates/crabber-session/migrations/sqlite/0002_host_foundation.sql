-- Applied only on fresh databases, after 0001. Existing versions fail closed.
ALTER TABLE sessions ADD COLUMN public_revision INTEGER NOT NULL DEFAULT 0 CHECK (public_revision >= 0);
CREATE UNIQUE INDEX runs_session_identity ON runs(session_id,id);
CREATE UNIQUE INDEX calls_run_identity ON tool_calls(run_id,id);
CREATE UNIQUE INDEX events_session_identity ON events(session_id,seq);

-- JSON TEXT is deliberate: arbitrary public/private JSON may contain escaped
-- NUL or number forms that cannot be retained losslessly as PostgreSQL JSONB.
CREATE TABLE session_public_state (
  session_id TEXT PRIMARY KEY REFERENCES sessions(id),
  format_version INTEGER NOT NULL CHECK (format_version = 1),
  state_json TEXT NOT NULL,
  activities_json TEXT NOT NULL
);
CREATE TABLE run_execution_bindings (
  run_id TEXT PRIMARY KEY REFERENCES runs(id),
  capsule_version INTEGER NOT NULL CHECK (capsule_version = 2),
  data_json TEXT NOT NULL
);
CREATE TABLE execution_attempts (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(id),
  generation INTEGER NOT NULL CHECK (generation > 0),
  owner TEXT NOT NULL,
  claim_token TEXT NOT NULL,
  run_claim_token TEXT NOT NULL,
  lease_until INTEGER NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('Unstarted','Started','Suspended','Completed','Failed','Interrupted','OutcomeUnknown')),
  cleanup_state TEXT NOT NULL CHECK (cleanup_state IN ('Pending','Complete','Incomplete')),
  outcome_json TEXT,
  UNIQUE(run_id,generation),
  UNIQUE(run_id,id)
);
CREATE UNIQUE INDEX attempts_one_open_owner ON execution_attempts(run_id)
  WHERE cleanup_state <> 'Complete' OR state IN ('Unstarted','Started','OutcomeUnknown');
CREATE TABLE tool_phases (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(id),
  call_id TEXT NOT NULL,
  attempt_id TEXT NOT NULL,
  generation INTEGER NOT NULL CHECK (generation > 0),
  claim_token TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('Claimed','Started','Suspended','Completed','Failed','Uncertain')),
  quiescence_state TEXT NOT NULL CHECK (quiescence_state IN ('Pending','Complete','Incomplete')),
  outcome_json TEXT,
  UNIQUE(run_id,call_id,generation),
  FOREIGN KEY(run_id,call_id) REFERENCES tool_calls(run_id,id),
  FOREIGN KEY(run_id,attempt_id) REFERENCES execution_attempts(run_id,id)
);
CREATE UNIQUE INDEX phases_one_open_owner ON tool_phases(run_id,call_id)
  WHERE quiescence_state <> 'Complete' OR state IN ('Claimed','Started','Uncertain');
CREATE TABLE operation_receipts (
  session_id TEXT NOT NULL REFERENCES sessions(id),
  operation_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  semantic_digest TEXT NOT NULL CHECK (length(semantic_digest) = 64),
  event_seq INTEGER NOT NULL,
  data_json TEXT NOT NULL,
  PRIMARY KEY(session_id,operation_id),
  FOREIGN KEY(session_id,event_seq) REFERENCES events(session_id,seq)
);
CREATE TABLE session_invocations (
  id TEXT PRIMARY KEY,
  session_id TEXT NOT NULL REFERENCES sessions(id),
  parent_run_id TEXT NOT NULL,
  parent_call_id TEXT,
  generation INTEGER NOT NULL CHECK (generation > 0),
  owner TEXT NOT NULL,
  claim_token TEXT NOT NULL,
  lease_until INTEGER NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('Unstarted','Running','Completed','Failed','Interrupted','Uncertain')),
  cleanup_state TEXT NOT NULL CHECK (cleanup_state IN ('Pending','Complete','Incomplete')),
  data_json TEXT NOT NULL,
  FOREIGN KEY(session_id,parent_run_id) REFERENCES runs(session_id,id),
  FOREIGN KEY(parent_run_id,parent_call_id) REFERENCES tool_calls(run_id,id)
);
CREATE INDEX invocations_session_owner ON session_invocations(session_id,state,cleanup_state);

