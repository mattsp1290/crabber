-- This migration runs only while constructing a fresh v5 database. Older
-- populated files are rejected by the strict lifecycle, never upgraded/copied.
DROP TABLE operation_receipts;
CREATE TABLE operation_receipts (
  session_id text NOT NULL REFERENCES sessions(id),
  operation_id text NOT NULL,
  kind text NOT NULL,
  semantic_digest text NOT NULL CHECK(length(semantic_digest)=64),
  event_seq integer,
  data_json text NOT NULL,
  PRIMARY KEY(session_id,operation_id),
  FOREIGN KEY(session_id,event_seq) REFERENCES events(session_id,seq),
  CHECK((kind='session_management' AND event_seq IS NULL) OR (kind<>'session_management' AND event_seq IS NOT NULL))
);
CREATE TABLE session_management (
  session_id text PRIMARY KEY REFERENCES sessions(id),
  workspace_id text NOT NULL,
  archived integer NOT NULL CHECK(archived IN (0,1)),
  format_version integer NOT NULL CHECK(format_version=1),
  data_json text NOT NULL
);
CREATE INDEX session_management_workspace ON session_management(workspace_id,session_id);
CREATE TABLE session_management_audit (
  session_id text NOT NULL,
  operation_id text NOT NULL,
  data_json text NOT NULL,
  PRIMARY KEY(session_id,operation_id),
  FOREIGN KEY(session_id,operation_id) REFERENCES operation_receipts(session_id,operation_id) DEFERRABLE INITIALLY DEFERRED
);
