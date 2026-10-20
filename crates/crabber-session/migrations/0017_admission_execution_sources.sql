-- A cohort is assigned only with the actual new execution record. Legacy rows
-- cannot acquire immutable no-effect evidence retroactively.
ALTER TABLE admission_executions ADD COLUMN IF NOT EXISTS source_protocol smallint NOT NULL DEFAULT 0 CHECK(source_protocol IN (0,1));
CREATE UNIQUE INDEX IF NOT EXISTS admission_execution_source_scope ON admission_executions(session_id,run_id,source_protocol);
CREATE OR REPLACE FUNCTION crabber_immutable_execution_protocol() RETURNS trigger AS $$
BEGIN
 IF NEW.source_protocol IS DISTINCT FROM OLD.source_protocol THEN
  RAISE EXCEPTION 'crabber_execution_protocol_immutable';
 END IF;
 RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER admission_execution_protocol_immutable BEFORE UPDATE OF source_protocol ON admission_executions
 FOR EACH ROW EXECUTE FUNCTION crabber_immutable_execution_protocol();
CREATE TABLE admission_execution_sources (
 session_id text NOT NULL,
 run_id text NOT NULL,
 kind text NOT NULL CHECK(kind IN ('init','begin')),
 protocol_version smallint NOT NULL DEFAULT 1 CHECK(protocol_version=1),
 data_json text NOT NULL CHECK(octet_length(data_json)<=8192),
 PRIMARY KEY(run_id,kind),
 FOREIGN KEY(session_id,run_id) REFERENCES runs(session_id,id),
 FOREIGN KEY(session_id,run_id,protocol_version) REFERENCES admission_executions(session_id,run_id,source_protocol)
);
CREATE OR REPLACE FUNCTION crabber_protect_execution_source() RETURNS trigger AS $$
BEGIN
 IF TG_OP='DELETE' AND EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) THEN
  RETURN OLD;
 END IF;
 RAISE EXCEPTION 'crabber_execution_source_immutable';
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER admission_execution_source_immutable BEFORE UPDATE OR DELETE ON admission_execution_sources
 FOR EACH ROW EXECUTE FUNCTION crabber_protect_execution_source();
INSERT INTO schema_version(version) VALUES(17);
