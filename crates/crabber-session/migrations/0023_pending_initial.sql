-- Genuine first Pending producer. No legacy backfill, Session or Run FK before birth.
CREATE UNIQUE INDEX native_execution_pending_scope ON native_execution_origins(session_id,run_id,birth_kind);
CREATE TABLE pending_initial_cohorts (
 session_id text NOT NULL CHECK(octet_length(session_id) BETWEEN 1 AND 512),
 source_key text NOT NULL CHECK(source_key ~ '^[0-9a-f]{64}$'),
 epoch_id text NOT NULL CHECK(octet_length(epoch_id) BETWEEN 1 AND 128),
 epoch_digest text NOT NULL CHECK(epoch_digest ~ '^[0-9a-f]{64}$'),
 PRIMARY KEY(session_id,source_key), UNIQUE(epoch_id), UNIQUE(session_id,source_key,epoch_id,epoch_digest)
);
CREATE TABLE pending_initial_epochs (
 session_id text NOT NULL CHECK(octet_length(session_id) BETWEEN 1 AND 512),
 source_key text NOT NULL CHECK(octet_length(source_key) BETWEEN 1 AND 64),
 epoch_id text NOT NULL CHECK(octet_length(epoch_id) BETWEEN 1 AND 128),
 epoch_digest text NOT NULL CHECK(octet_length(epoch_digest) BETWEEN 1 AND 64),
 kind text NOT NULL CHECK(octet_length(kind) BETWEEN 1 AND 32),
 complete_call text NOT NULL CHECK(octet_length(complete_call) BETWEEN 1 AND 128),
 required_complete text,
 required_close text,
 data_json text NOT NULL CHECK(octet_length(data_json) BETWEEN 1 AND 8192),
 CHECK((kind='Opened' AND required_complete IS NOT NULL AND required_complete=complete_call AND required_close IS NULL)
 OR (kind='SealedBeforeOpen' AND required_complete IS NULL AND required_close IS NOT NULL)),
 PRIMARY KEY(session_id,epoch_id), UNIQUE(epoch_id), UNIQUE(session_id,source_key,epoch_id,epoch_digest)
);
CREATE TABLE pending_initial_source_slots (
 session_id text NOT NULL CHECK(octet_length(session_id) BETWEEN 1 AND 512),
 source_key text NOT NULL CHECK(octet_length(source_key) BETWEEN 1 AND 64),
 epoch_id text NOT NULL CHECK(octet_length(epoch_id) BETWEEN 1 AND 128),
 epoch_digest text NOT NULL CHECK(octet_length(epoch_digest) BETWEEN 1 AND 64),
 data_json text NOT NULL CHECK(octet_length(data_json) BETWEEN 1 AND 8192),
 PRIMARY KEY(session_id,source_key), UNIQUE(epoch_id), UNIQUE(session_id,source_key,epoch_id,epoch_digest)
);
CREATE TABLE pending_initial_registered_completes (
 session_id text NOT NULL CHECK(octet_length(session_id) BETWEEN 1 AND 512),
 source_key text NOT NULL CHECK(octet_length(source_key) BETWEEN 1 AND 64),
 epoch_id text NOT NULL CHECK(octet_length(epoch_id) BETWEEN 1 AND 128),
 epoch_digest text NOT NULL CHECK(octet_length(epoch_digest) BETWEEN 1 AND 64),
 call_id text NOT NULL CHECK(octet_length(call_id) BETWEEN 1 AND 128),
 data_json text NOT NULL CHECK(octet_length(data_json) BETWEEN 1 AND 8192),
 PRIMARY KEY(session_id,epoch_id), UNIQUE(call_id), UNIQUE(session_id,epoch_id,call_id)
);
CREATE TABLE pending_initial_assignments (
 session_id text NOT NULL CHECK(octet_length(session_id) BETWEEN 1 AND 512),
 source_key text NOT NULL CHECK(octet_length(source_key) BETWEEN 1 AND 64),
 epoch_id text NOT NULL CHECK(octet_length(epoch_id) BETWEEN 1 AND 128),
 epoch_digest text NOT NULL CHECK(octet_length(epoch_digest) BETWEEN 1 AND 64),
 call_id text NOT NULL CHECK(octet_length(call_id) BETWEEN 1 AND 128),
 run_id text NOT NULL CHECK(octet_length(run_id) BETWEEN 1 AND 128),
 data_json text NOT NULL CHECK(octet_length(data_json) BETWEEN 1 AND 8192),
 birth_kind TEXT NOT NULL DEFAULT 'pending' CHECK(birth_kind='pending'),
 PRIMARY KEY(session_id,epoch_id), UNIQUE(run_id), UNIQUE(session_id,epoch_id,run_id)
);
CREATE TABLE pending_initial_closes (
 session_id text NOT NULL CHECK(octet_length(session_id) BETWEEN 1 AND 512),
 source_key text NOT NULL CHECK(octet_length(source_key) BETWEEN 1 AND 64),
 epoch_id text NOT NULL CHECK(octet_length(epoch_id) BETWEEN 1 AND 128),
 epoch_digest text NOT NULL CHECK(octet_length(epoch_digest) BETWEEN 1 AND 64),
 seal_id text NOT NULL CHECK(octet_length(seal_id) BETWEEN 1 AND 128),
 data_json text NOT NULL CHECK(octet_length(data_json) BETWEEN 1 AND 8192),
 PRIMARY KEY(session_id,epoch_id), UNIQUE(seal_id), UNIQUE(session_id,epoch_id,seal_id)
);
CREATE TABLE pending_initial_publications (
 session_id text NOT NULL CHECK(octet_length(session_id) BETWEEN 1 AND 512),
 source_key text NOT NULL CHECK(octet_length(source_key) BETWEEN 1 AND 64),
 epoch_id text NOT NULL CHECK(octet_length(epoch_id) BETWEEN 1 AND 128),
 epoch_digest text NOT NULL CHECK(octet_length(epoch_digest) BETWEEN 1 AND 64),
 sequence BIGINT NOT NULL CHECK(sequence BETWEEN 1 AND 3),
 kind text NOT NULL CHECK(octet_length(kind) BETWEEN 1 AND 32),
 publication_digest text NOT NULL CHECK(octet_length(publication_digest) BETWEEN 1 AND 64),
 data_json text NOT NULL CHECK(octet_length(data_json) BETWEEN 1 AND 8192),
 PRIMARY KEY(session_id,epoch_id,sequence), UNIQUE(session_id,epoch_id,sequence,publication_digest)
);
CREATE TABLE pending_initial_heads (
 session_id text NOT NULL CHECK(octet_length(session_id) BETWEEN 1 AND 512),
 source_key text NOT NULL CHECK(octet_length(source_key) BETWEEN 1 AND 64),
 epoch_id text NOT NULL CHECK(octet_length(epoch_id) BETWEEN 1 AND 128),
 epoch_digest text NOT NULL CHECK(octet_length(epoch_digest) BETWEEN 1 AND 64),
 sequence BIGINT NOT NULL CHECK(sequence BETWEEN 1 AND 3),
 publication_digest text NOT NULL CHECK(octet_length(publication_digest) BETWEEN 1 AND 64),
 data_json text NOT NULL CHECK(octet_length(data_json) BETWEEN 1 AND 8192),
 PRIMARY KEY(session_id,epoch_id)
);
ALTER TABLE pending_initial_epochs ADD FOREIGN KEY(session_id,source_key,epoch_id,epoch_digest) REFERENCES pending_initial_source_slots(session_id,source_key,epoch_id,epoch_digest) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE pending_initial_epochs ADD FOREIGN KEY(session_id,epoch_id,required_complete) REFERENCES pending_initial_registered_completes(session_id,epoch_id,call_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE pending_initial_epochs ADD FOREIGN KEY(session_id,epoch_id,required_close) REFERENCES pending_initial_closes(session_id,epoch_id,seal_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE pending_initial_epochs ADD FOREIGN KEY(session_id,epoch_id) REFERENCES pending_initial_heads(session_id,epoch_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE pending_initial_source_slots ADD FOREIGN KEY(session_id,source_key,epoch_id,epoch_digest) REFERENCES pending_initial_epochs(session_id,source_key,epoch_id,epoch_digest) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE pending_initial_registered_completes ADD FOREIGN KEY(session_id,source_key,epoch_id,epoch_digest) REFERENCES pending_initial_epochs(session_id,source_key,epoch_id,epoch_digest) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE pending_initial_assignments ADD FOREIGN KEY(session_id,epoch_id,call_id) REFERENCES pending_initial_registered_completes(session_id,epoch_id,call_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE pending_initial_assignments ADD FOREIGN KEY(session_id,source_key,epoch_id,epoch_digest) REFERENCES pending_initial_epochs(session_id,source_key,epoch_id,epoch_digest) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE pending_initial_assignments ADD FOREIGN KEY(session_id,run_id,birth_kind) REFERENCES native_execution_origins(session_id,run_id,birth_kind) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE pending_initial_closes ADD FOREIGN KEY(session_id,source_key,epoch_id,epoch_digest) REFERENCES pending_initial_epochs(session_id,source_key,epoch_id,epoch_digest) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE pending_initial_publications ADD FOREIGN KEY(session_id,source_key,epoch_id,epoch_digest) REFERENCES pending_initial_epochs(session_id,source_key,epoch_id,epoch_digest) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE pending_initial_heads ADD FOREIGN KEY(session_id,source_key,epoch_id,epoch_digest) REFERENCES pending_initial_epochs(session_id,source_key,epoch_id,epoch_digest) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE pending_initial_heads ADD FOREIGN KEY(session_id,epoch_id,sequence,publication_digest) REFERENCES pending_initial_publications(session_id,epoch_id,sequence,publication_digest) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE pending_initial_cohorts ADD FOREIGN KEY(session_id,source_key,epoch_id,epoch_digest) REFERENCES pending_initial_source_slots(session_id,source_key,epoch_id,epoch_digest) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE pending_initial_source_slots ADD FOREIGN KEY(session_id,source_key,epoch_id,epoch_digest) REFERENCES pending_initial_cohorts(session_id,source_key,epoch_id,epoch_digest) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE native_execution_origins ADD COLUMN pending_epoch text
 REFERENCES pending_initial_epochs(epoch_id) DEFERRABLE INITIALLY DEFERRED
 CHECK((birth_kind='raw' AND pending_epoch IS NULL) OR (birth_kind='pending' AND pending_epoch IS NOT NULL));
ALTER TABLE native_execution_origins ADD FOREIGN KEY(session_id,pending_epoch,run_id) REFERENCES pending_initial_assignments(session_id,epoch_id,run_id) DEFERRABLE INITIALLY DEFERRED;
CREATE FUNCTION crabber_pending_initial_guard() RETURNS trigger AS $$
BEGIN
 IF TG_OP='DELETE' THEN
  IF EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) THEN RETURN OLD; END IF;
  RAISE EXCEPTION 'crabber_pending_initial_immutable';
 END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended(NEW.session_id,0));
 IF EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE EXCEPTION 'crabber_session_deleted'; END IF;
 IF EXISTS(SELECT 1 FROM pending_initial_closes WHERE session_id=NEW.session_id AND epoch_id=NEW.epoch_id) THEN RAISE EXCEPTION 'crabber_pending_initial_closed'; END IF;
 IF TG_OP='UPDATE' THEN
  IF TG_TABLE_NAME<>'pending_initial_heads' THEN RAISE EXCEPTION 'crabber_pending_initial_immutable'; END IF;
  IF (NEW.session_id,NEW.source_key,NEW.epoch_id,NEW.epoch_digest) IS DISTINCT FROM
   (OLD.session_id,OLD.source_key,OLD.epoch_id,OLD.epoch_digest) OR NEW.sequence<>OLD.sequence+1
  THEN RAISE EXCEPTION 'crabber_pending_initial_immutable'; END IF;
 END IF;
 IF TG_TABLE_NAME='pending_initial_heads' THEN
  IF (TG_OP='INSERT' AND NEW.sequence<>1) OR NEW.sequence IS DISTINCT FROM (SELECT MAX(sequence) FROM pending_initial_publications WHERE session_id=NEW.session_id AND epoch_id=NEW.epoch_id)
  THEN RAISE EXCEPTION 'crabber_pending_initial_head'; END IF;
 END IF;
 RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER pending_initial_cohorts_guard BEFORE INSERT OR UPDATE OR DELETE ON pending_initial_cohorts FOR EACH ROW EXECUTE FUNCTION crabber_pending_initial_guard();
CREATE TRIGGER pending_initial_epochs_guard BEFORE INSERT OR UPDATE OR DELETE ON pending_initial_epochs FOR EACH ROW EXECUTE FUNCTION crabber_pending_initial_guard();
CREATE TRIGGER pending_initial_source_slots_guard BEFORE INSERT OR UPDATE OR DELETE ON pending_initial_source_slots FOR EACH ROW EXECUTE FUNCTION crabber_pending_initial_guard();
CREATE TRIGGER pending_initial_registered_completes_guard BEFORE INSERT OR UPDATE OR DELETE ON pending_initial_registered_completes FOR EACH ROW EXECUTE FUNCTION crabber_pending_initial_guard();
CREATE TRIGGER pending_initial_assignments_guard BEFORE INSERT OR UPDATE OR DELETE ON pending_initial_assignments FOR EACH ROW EXECUTE FUNCTION crabber_pending_initial_guard();
CREATE TRIGGER pending_initial_closes_guard BEFORE INSERT OR UPDATE OR DELETE ON pending_initial_closes FOR EACH ROW EXECUTE FUNCTION crabber_pending_initial_guard();
CREATE TRIGGER pending_initial_publications_guard BEFORE INSERT OR UPDATE OR DELETE ON pending_initial_publications FOR EACH ROW EXECUTE FUNCTION crabber_pending_initial_guard();
CREATE TRIGGER pending_initial_heads_guard BEFORE INSERT OR UPDATE OR DELETE ON pending_initial_heads FOR EACH ROW EXECUTE FUNCTION crabber_pending_initial_guard();
INSERT INTO schema_version(version) VALUES(23) ON CONFLICT(version) DO NOTHING;
