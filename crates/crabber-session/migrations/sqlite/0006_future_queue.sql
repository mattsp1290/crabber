-- Fresh databases only; strict connect refuses older populated schemas.
DROP TABLE operation_receipts;
CREATE TABLE operation_receipts (
 session_id text NOT NULL REFERENCES sessions(id), operation_id text NOT NULL,
 kind text NOT NULL, semantic_digest text NOT NULL CHECK(length(semantic_digest)=64),
 event_seq integer, data_json text NOT NULL,
 PRIMARY KEY(session_id,operation_id), FOREIGN KEY(session_id,event_seq) REFERENCES events(session_id,seq),
 CHECK((kind IN ('session_management','future_queue') AND event_seq IS NULL) OR (kind NOT IN ('session_management','future_queue') AND event_seq IS NOT NULL))
);
CREATE TABLE future_queue (
 session_id text NOT NULL REFERENCES sessions(id), queue_id text NOT NULL,
 sequence integer NOT NULL CHECK(sequence>0), state text NOT NULL CHECK(state IN ('Queued','Claimed','Admitted','Cancelled')),
 prepared integer NOT NULL CHECK(prepared IN (0,1)), summary_json text NOT NULL, data_json text NOT NULL,
 admission_key text NOT NULL,
 PRIMARY KEY(session_id,queue_id), UNIQUE(session_id,sequence), UNIQUE(session_id,admission_key)
);
CREATE INDEX future_queue_head ON future_queue(session_id,sequence) WHERE state IN ('Queued','Claimed');
CREATE TABLE future_queue_audit (
 session_id text NOT NULL, operation_id text NOT NULL, queue_id text NOT NULL, revision bigint NOT NULL CHECK(revision>0), data_json text NOT NULL,
 PRIMARY KEY(session_id,operation_id), UNIQUE(session_id,revision),
 FOREIGN KEY(session_id,operation_id) REFERENCES operation_receipts(session_id,operation_id) DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX future_queue_audit_latest ON future_queue_audit(session_id,queue_id,revision DESC);
