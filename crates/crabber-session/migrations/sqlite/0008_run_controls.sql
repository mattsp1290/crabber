-- Durable exact-run controls; all progress is corroborated by immutable audits.
CREATE TABLE IF NOT EXISTS run_controls (
 session_id TEXT NOT NULL, run_id TEXT NOT NULL, operation_id TEXT NOT NULL,
 sequence BIGINT NOT NULL CHECK(sequence BETWEEN 1 AND 64),
 request_json TEXT NOT NULL, summary_json TEXT NOT NULL,
 PRIMARY KEY(session_id,operation_id), UNIQUE(run_id,sequence),
 FOREIGN KEY(session_id,run_id) REFERENCES runs(session_id,id),
 FOREIGN KEY(session_id,operation_id) REFERENCES operation_receipts(session_id,operation_id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE IF NOT EXISTS run_control_progress (
 session_id TEXT NOT NULL, operation_id TEXT NOT NULL, event_seq BIGINT NOT NULL,
 summary_json TEXT NOT NULL,
 PRIMARY KEY(session_id,operation_id,event_seq),
 FOREIGN KEY(session_id,event_seq) REFERENCES events(session_id,seq),
 FOREIGN KEY(session_id,operation_id) REFERENCES run_controls(session_id,operation_id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS run_suspension_proofs (
 run_id TEXT PRIMARY KEY REFERENCES runs(id), data_json TEXT NOT NULL
);
