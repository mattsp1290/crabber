-- Applied only during fresh initialization, after 0001/0002. Keep the carriers
-- independently versioned and preserve their rows without decoding payloads.
ALTER TABLE run_execution_bindings RENAME TO run_execution_bindings_v2;
CREATE TABLE run_execution_bindings (
  run_id TEXT PRIMARY KEY REFERENCES runs(id),
  capsule_version INTEGER NOT NULL CHECK (capsule_version IN (2,3)),
  data_json TEXT NOT NULL
);
INSERT INTO run_execution_bindings SELECT * FROM run_execution_bindings_v2;
DROP TABLE run_execution_bindings_v2;
ALTER TABLE admission_executions RENAME TO admission_executions_v1;
CREATE TABLE admission_executions (
  run_id TEXT NOT NULL PRIMARY KEY REFERENCES runs(id), session_id TEXT NOT NULL,
  admission_key TEXT NOT NULL,
  capsule_version INTEGER NOT NULL CHECK (capsule_version IN (1,2)),
  start_state TEXT NOT NULL CHECK (start_state IN ('Unstarted','Started')),
  data TEXT NOT NULL, UNIQUE(session_id, admission_key),
  FOREIGN KEY (session_id, admission_key) REFERENCES admission_receipts(session_id, admission_key)
);
INSERT INTO admission_executions SELECT * FROM admission_executions_v1;
DROP TABLE admission_executions_v1;
