-- Explicit native state machines. This file never rewrites older execution rows.
CREATE UNIQUE INDEX IF NOT EXISTS phases_run_identity ON tool_phases(run_id,id);
CREATE TABLE IF NOT EXISTS tool_continuations (
  id text PRIMARY KEY NOT NULL,
  session_id text NOT NULL,
  run_id text NOT NULL,
  generation bigint NOT NULL CHECK(generation>0),
  state text NOT NULL CHECK(state IN ('waiting','claimed','released')),
  format_version integer NOT NULL CHECK(format_version=1),
  data_json text NOT NULL,
  UNIQUE(session_id,id), UNIQUE(run_id,id), UNIQUE(run_id,generation),
  FOREIGN KEY(session_id,run_id) REFERENCES runs(session_id,id)
);
CREATE TABLE IF NOT EXISTS tool_continuation_barriers (
  session_id text PRIMARY KEY NOT NULL,
  run_id text NOT NULL,
  continuation_id text NOT NULL UNIQUE,
  FOREIGN KEY(session_id,run_id) REFERENCES runs(session_id,id),
  FOREIGN KEY(session_id,continuation_id) REFERENCES tool_continuations(session_id,id),
  FOREIGN KEY(run_id,continuation_id) REFERENCES tool_continuations(run_id,id)
);
CREATE TABLE IF NOT EXISTS tool_continuation_interactions (
  id text PRIMARY KEY NOT NULL,
  continuation_id text NOT NULL,
  run_id text NOT NULL,
  call_id text NOT NULL,
  phase_id text NOT NULL,
  FOREIGN KEY(run_id,continuation_id) REFERENCES tool_continuations(run_id,id),
  FOREIGN KEY(run_id,call_id) REFERENCES tool_calls(run_id,id),
  FOREIGN KEY(run_id,phase_id) REFERENCES tool_phases(run_id,id)
);
CREATE INDEX IF NOT EXISTS tool_interactions_continuation ON tool_continuation_interactions(continuation_id);
CREATE TABLE IF NOT EXISTS tool_continuation_phases (
  continuation_id text NOT NULL,
  run_id text NOT NULL,
  phase_id text PRIMARY KEY NOT NULL,
  FOREIGN KEY(run_id,continuation_id) REFERENCES tool_continuations(run_id,id),
  FOREIGN KEY(run_id,phase_id) REFERENCES tool_phases(run_id,id)
);
INSERT INTO schema_version(version) VALUES(11) ON CONFLICT DO NOTHING;
