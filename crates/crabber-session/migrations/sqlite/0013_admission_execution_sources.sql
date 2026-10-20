ALTER TABLE admission_executions ADD COLUMN source_protocol INTEGER NOT NULL DEFAULT 0 CHECK(source_protocol IN (0,1));
CREATE UNIQUE INDEX admission_execution_source_scope ON admission_executions(session_id,run_id,source_protocol);
CREATE TRIGGER admission_execution_protocol_immutable BEFORE UPDATE OF source_protocol ON admission_executions
 WHEN NEW.source_protocol<>OLD.source_protocol
 BEGIN SELECT RAISE(ABORT,'crabber_execution_protocol_immutable'); END;
CREATE TRIGGER admission_execution_protocol_insert_immutable BEFORE INSERT ON admission_executions
 WHEN EXISTS(SELECT 1 FROM admission_executions WHERE run_id=NEW.run_id AND source_protocol<>NEW.source_protocol)
 BEGIN SELECT RAISE(ABORT,'crabber_execution_protocol_immutable'); END;
CREATE TABLE admission_execution_sources (
 session_id TEXT NOT NULL,
 run_id TEXT NOT NULL,
 kind TEXT NOT NULL CHECK(kind IN ('init','begin')),
 protocol_version INTEGER NOT NULL DEFAULT 1 CHECK(protocol_version=1),
 data_json TEXT NOT NULL CHECK(length(CAST(data_json AS BLOB))<=8192),
 PRIMARY KEY(run_id,kind),
 FOREIGN KEY(session_id,run_id) REFERENCES runs(session_id,id),
 FOREIGN KEY(session_id,run_id,protocol_version) REFERENCES admission_executions(session_id,run_id,source_protocol)
);
CREATE TRIGGER admission_execution_source_no_replace BEFORE INSERT ON admission_execution_sources
 WHEN EXISTS(SELECT 1 FROM admission_execution_sources WHERE run_id=NEW.run_id AND kind=NEW.kind)
 BEGIN SELECT RAISE(ABORT,'crabber_execution_source_immutable'); END;
CREATE TRIGGER admission_execution_source_no_update BEFORE UPDATE ON admission_execution_sources
 BEGIN SELECT RAISE(ABORT,'crabber_execution_source_immutable'); END;
CREATE TRIGGER admission_execution_source_no_delete BEFORE DELETE ON admission_execution_sources
 WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id)
 BEGIN SELECT RAISE(ABORT,'crabber_execution_source_immutable'); END;
