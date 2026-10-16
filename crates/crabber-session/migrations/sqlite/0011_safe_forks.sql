-- Fresh databases only. Imported provenance has no source foreign keys.
DROP TABLE operation_receipts;
CREATE TABLE operation_receipts (
 session_id text NOT NULL REFERENCES sessions(id), operation_id text NOT NULL,
 kind text NOT NULL, semantic_digest text NOT NULL CHECK(length(semantic_digest)=64),
 event_seq integer, data_json text NOT NULL,
 PRIMARY KEY(session_id,operation_id), FOREIGN KEY(session_id,event_seq) REFERENCES events(session_id,seq),
 CHECK((kind IN ('session_management','future_queue','session_fork') AND event_seq IS NULL) OR (kind NOT IN ('session_management','future_queue','session_fork') AND event_seq IS NOT NULL))
);
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
