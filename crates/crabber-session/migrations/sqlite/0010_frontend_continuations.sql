-- Applied only while constructing a fresh SQLite v10 database.
CREATE TABLE IF NOT EXISTS frontend_tool_origins (
  call_id text PRIMARY KEY NOT NULL,
  session_id text NOT NULL,
  run_id text NOT NULL,
  format_version integer NOT NULL CHECK(format_version=1),
  data_json text NOT NULL,
  UNIQUE(run_id,call_id),
  FOREIGN KEY(session_id,run_id) REFERENCES runs(session_id,id),
  FOREIGN KEY(run_id,call_id) REFERENCES tool_calls(run_id,id)
);
CREATE TABLE IF NOT EXISTS tool_continuation_frontend_calls (
  continuation_id text NOT NULL,
  run_id text NOT NULL,
  call_id text NOT NULL,
  phase_id text NOT NULL,
  PRIMARY KEY(continuation_id,call_id),
  UNIQUE(continuation_id,phase_id),
  FOREIGN KEY(run_id,continuation_id) REFERENCES tool_continuations(run_id,id),
  FOREIGN KEY(run_id,call_id) REFERENCES frontend_tool_origins(run_id,call_id)
);
