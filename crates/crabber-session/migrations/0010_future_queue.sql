-- Session controls are audited without inventing an executing runtime run.
ALTER TABLE operation_receipts DROP CONSTRAINT IF EXISTS operation_receipts_audit_kind;
ALTER TABLE operation_receipts ADD CONSTRAINT operation_receipts_audit_kind CHECK ((kind IN ('session_management','future_queue') AND event_seq IS NULL) OR (kind NOT IN ('session_management','future_queue') AND event_seq IS NOT NULL));
CREATE TABLE IF NOT EXISTS future_queue (
 session_id text NOT NULL REFERENCES sessions(id),
 queue_id text NOT NULL,
 sequence bigint NOT NULL CHECK(sequence>0),
 state text NOT NULL CHECK(state IN ('Queued','Claimed','Admitted','Cancelled')),
 prepared boolean NOT NULL,
 summary_json text NOT NULL,
 data_json text NOT NULL,
 admission_key text NOT NULL,
 PRIMARY KEY(session_id,queue_id), UNIQUE(session_id,sequence), UNIQUE(session_id,admission_key)
);
CREATE INDEX IF NOT EXISTS future_queue_head ON future_queue(session_id,sequence) WHERE state IN ('Queued','Claimed');
CREATE TABLE IF NOT EXISTS future_queue_audit (
 session_id text NOT NULL, operation_id text NOT NULL, queue_id text NOT NULL, revision bigint NOT NULL CHECK(revision>0), data_json text NOT NULL,
 PRIMARY KEY(session_id,operation_id), UNIQUE(session_id,revision),
 FOREIGN KEY(session_id,operation_id) REFERENCES operation_receipts(session_id,operation_id) DEFERRABLE INITIALLY DEFERRED
);
INSERT INTO schema_version(version) VALUES(10) ON CONFLICT DO NOTHING;

CREATE INDEX IF NOT EXISTS future_queue_audit_latest ON future_queue_audit(session_id,queue_id,revision DESC);
