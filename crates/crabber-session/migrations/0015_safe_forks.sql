-- Imported calls are presentation history, never native executable records.
ALTER TABLE operation_receipts DROP CONSTRAINT IF EXISTS operation_receipts_audit_kind;
ALTER TABLE operation_receipts ADD CONSTRAINT operation_receipts_audit_kind CHECK ((kind IN ('session_management','future_queue','session_fork') AND event_seq IS NULL) OR (kind NOT IN ('session_management','future_queue','session_fork') AND event_seq IS NOT NULL));
CREATE TABLE imported_tool_calls (
 id text PRIMARY KEY, session_id text NOT NULL REFERENCES sessions(id),
 ordinal bigint NOT NULL CHECK(ordinal>0), data_json text NOT NULL,
 snapshot_parts bigint NOT NULL CHECK(snapshot_parts=0),
 snapshot_text bigint NOT NULL CHECK(snapshot_text>=0),
 snapshot_bytes bigint NOT NULL CHECK(snapshot_bytes>0 AND snapshot_bytes<=8388608),
 UNIQUE(session_id,ordinal)
);
CREATE TABLE imported_message_provenance (
 message_id text PRIMARY KEY REFERENCES messages(id),
 session_id text NOT NULL REFERENCES sessions(id),
 ordinal bigint NOT NULL CHECK(ordinal>0), data_json text NOT NULL,
 snapshot_bytes bigint NOT NULL CHECK(snapshot_bytes>0 AND snapshot_bytes<=8388608),
 UNIQUE(session_id,ordinal)
);
CREATE TABLE session_fork_audit (
 session_id text PRIMARY KEY REFERENCES sessions(id), operation_id text NOT NULL,
 data_json text NOT NULL,
 FOREIGN KEY(session_id,operation_id) REFERENCES operation_receipts(session_id,operation_id) DEFERRABLE INITIALLY DEFERRED
);
INSERT INTO schema_version(version) VALUES(15) ON CONFLICT DO NOTHING;
