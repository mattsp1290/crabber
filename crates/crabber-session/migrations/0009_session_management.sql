-- Host controls have session-scoped audit records, without synthetic runs.
ALTER TABLE operation_receipts ALTER COLUMN event_seq DROP NOT NULL;
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='operation_receipts_audit_kind' AND conrelid='operation_receipts'::regclass) THEN
    ALTER TABLE operation_receipts ADD CONSTRAINT operation_receipts_audit_kind CHECK ((kind='session_management' AND event_seq IS NULL) OR (kind<>'session_management' AND event_seq IS NOT NULL));
  END IF;
END $$;
CREATE TABLE IF NOT EXISTS session_management (
  session_id text PRIMARY KEY REFERENCES sessions(id),
  workspace_id text NOT NULL,
  archived boolean NOT NULL,
  format_version integer NOT NULL CHECK(format_version=1),
  data_json text NOT NULL
);
CREATE INDEX IF NOT EXISTS session_management_workspace ON session_management(workspace_id,session_id);
CREATE TABLE IF NOT EXISTS session_management_audit (
  session_id text NOT NULL,
  operation_id text NOT NULL,
  data_json text NOT NULL,
  PRIMARY KEY(session_id,operation_id),
  FOREIGN KEY(session_id,operation_id) REFERENCES operation_receipts(session_id,operation_id) DEFERRABLE INITIALLY DEFERRED
);
INSERT INTO schema_version(version) VALUES(9) ON CONFLICT DO NOTHING;
