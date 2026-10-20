-- Closed original Initial consumer; no legacy adoption or Session/Run FK before birth.
CREATE TABLE initial_consumer_origins (
 session_id TEXT NOT NULL CHECK(octet_length(session_id) BETWEEN 1 AND 512),
 source_key TEXT NOT NULL CHECK(octet_length(source_key)=64),
 epoch_id TEXT NOT NULL CHECK(octet_length(epoch_id) BETWEEN 1 AND 128),
 record_digest TEXT NOT NULL CHECK(octet_length(record_digest)=64),
 origin_kind TEXT NOT NULL CHECK(origin_kind IN ('opened','no_opened_member')),
 data_json TEXT NOT NULL CHECK(octet_length(data_json)>0),
 PRIMARY KEY(session_id,source_key),
 UNIQUE(epoch_id),UNIQUE(session_id,source_key,epoch_id),UNIQUE(session_id,source_key,epoch_id,record_digest)
);
CREATE TABLE initial_consumer_cohorts (
 session_id TEXT NOT NULL CHECK(octet_length(session_id) BETWEEN 1 AND 512),
 source_key TEXT NOT NULL CHECK(octet_length(source_key)=64),
 epoch_id TEXT NOT NULL CHECK(octet_length(epoch_id) BETWEEN 1 AND 128),
 record_digest TEXT NOT NULL CHECK(octet_length(record_digest)=64),
 origin_kind TEXT NOT NULL CHECK(origin_kind IN ('opened','no_opened_member')),
 data_json TEXT NOT NULL CHECK(octet_length(data_json)>0),
 PRIMARY KEY(session_id,source_key),
 UNIQUE(epoch_id),UNIQUE(session_id,source_key,epoch_id,record_digest)
);
CREATE TABLE initial_consumer_assignments (
 session_id TEXT NOT NULL CHECK(octet_length(session_id) BETWEEN 1 AND 512),
 source_key TEXT NOT NULL CHECK(octet_length(source_key)=64),
 epoch_id TEXT NOT NULL CHECK(octet_length(epoch_id) BETWEEN 1 AND 128),
 record_digest TEXT NOT NULL CHECK(octet_length(record_digest)=64),
 run_id TEXT NOT NULL UNIQUE,
 data_json TEXT NOT NULL CHECK(octet_length(data_json)>0),
 PRIMARY KEY(session_id,epoch_id)
 
);
CREATE TABLE initial_consumer_calls (
 session_id TEXT NOT NULL CHECK(octet_length(session_id) BETWEEN 1 AND 512),
 source_key TEXT NOT NULL CHECK(octet_length(source_key)=64),
 epoch_id TEXT NOT NULL CHECK(octet_length(epoch_id) BETWEEN 1 AND 128),
 record_digest TEXT NOT NULL CHECK(octet_length(record_digest)=64),
 call_id TEXT NOT NULL UNIQUE,kind TEXT NOT NULL,input_digest TEXT NOT NULL,
 data_json TEXT NOT NULL CHECK(octet_length(data_json)>0),
 PRIMARY KEY(session_id,epoch_id,call_id),
 UNIQUE(session_id,epoch_id,call_id,record_digest)
);
CREATE TABLE initial_consumer_results (
 session_id TEXT NOT NULL CHECK(octet_length(session_id) BETWEEN 1 AND 512),
 source_key TEXT NOT NULL CHECK(octet_length(source_key)=64),
 epoch_id TEXT NOT NULL CHECK(octet_length(epoch_id) BETWEEN 1 AND 128),
 record_digest TEXT NOT NULL CHECK(octet_length(record_digest)=64),
 call_id TEXT NOT NULL,registration_digest TEXT NOT NULL,
 data_json TEXT NOT NULL CHECK(octet_length(data_json)>0),
 PRIMARY KEY(session_id,epoch_id,call_id),
 UNIQUE(session_id,epoch_id,call_id,record_digest)
);
CREATE TABLE initial_consumer_roster (
 session_id TEXT NOT NULL CHECK(octet_length(session_id) BETWEEN 1 AND 512),
 source_key TEXT NOT NULL CHECK(octet_length(source_key)=64),
 epoch_id TEXT NOT NULL CHECK(octet_length(epoch_id) BETWEEN 1 AND 128),
 record_digest TEXT NOT NULL CHECK(octet_length(record_digest)=64),
 call_id TEXT NOT NULL,registration_digest TEXT NOT NULL,result_digest TEXT NOT NULL,
 data_json TEXT NOT NULL CHECK(octet_length(data_json)>0),
 PRIMARY KEY(session_id,epoch_id,call_id)
 
);
CREATE TABLE initial_consumer_reverse (
 session_id TEXT NOT NULL CHECK(octet_length(session_id) BETWEEN 1 AND 512),
 source_key TEXT NOT NULL CHECK(octet_length(source_key)=64),
 epoch_id TEXT NOT NULL CHECK(octet_length(epoch_id) BETWEEN 1 AND 128),
 record_digest TEXT NOT NULL CHECK(octet_length(record_digest)=64),
 call_id TEXT NOT NULL,kind TEXT NOT NULL,member_key TEXT NOT NULL,result_digest TEXT NOT NULL,
 data_json TEXT NOT NULL CHECK(octet_length(data_json)>0),
 PRIMARY KEY(kind,member_key)
 
);
CREATE TABLE initial_consumer_publications (
 session_id TEXT NOT NULL CHECK(octet_length(session_id) BETWEEN 1 AND 512),
 source_key TEXT NOT NULL CHECK(octet_length(source_key)=64),
 epoch_id TEXT NOT NULL CHECK(octet_length(epoch_id) BETWEEN 1 AND 128),
 record_digest TEXT NOT NULL CHECK(octet_length(record_digest)=64),
 sequence BIGINT NOT NULL CHECK(sequence>0),kind TEXT NOT NULL,original_digest TEXT NOT NULL,previous_digest TEXT,
 data_json TEXT NOT NULL CHECK(octet_length(data_json)>0),
 PRIMARY KEY(session_id,epoch_id,sequence),
 UNIQUE(session_id,epoch_id,sequence,record_digest)
);
CREATE TABLE initial_consumer_heads (
 session_id TEXT NOT NULL CHECK(octet_length(session_id) BETWEEN 1 AND 512),
 source_key TEXT NOT NULL CHECK(octet_length(source_key)=64),
 epoch_id TEXT NOT NULL CHECK(octet_length(epoch_id) BETWEEN 1 AND 128),
 record_digest TEXT NOT NULL CHECK(octet_length(record_digest)=64),
 sequence BIGINT NOT NULL CHECK(sequence>0),origin_digest TEXT NOT NULL,
 data_json TEXT NOT NULL CHECK(octet_length(data_json)>0),
 PRIMARY KEY(session_id,epoch_id)
 
);
CREATE TABLE initial_consumer_closes (
 session_id TEXT NOT NULL CHECK(octet_length(session_id) BETWEEN 1 AND 512),
 source_key TEXT NOT NULL CHECK(octet_length(source_key)=64),
 epoch_id TEXT NOT NULL CHECK(octet_length(epoch_id) BETWEEN 1 AND 128),
 record_digest TEXT NOT NULL CHECK(octet_length(record_digest)=64),
 anchor_digest TEXT NOT NULL,birth_close_digest TEXT NOT NULL,
 data_json TEXT NOT NULL CHECK(octet_length(data_json)>0),
 PRIMARY KEY(session_id,epoch_id)
 
);
ALTER TABLE initial_consumer_origins ADD FOREIGN KEY(session_id,source_key,epoch_id,record_digest) REFERENCES initial_consumer_cohorts(session_id,source_key,epoch_id,record_digest) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE initial_consumer_origins ADD FOREIGN KEY(session_id,epoch_id) REFERENCES initial_consumer_heads(session_id,epoch_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE initial_consumer_cohorts ADD FOREIGN KEY(session_id,source_key,epoch_id,record_digest) REFERENCES initial_consumer_origins(session_id,source_key,epoch_id,record_digest) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE initial_consumer_assignments ADD FOREIGN KEY(session_id,source_key,epoch_id) REFERENCES initial_consumer_origins(session_id,source_key,epoch_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE initial_consumer_calls ADD FOREIGN KEY(session_id,source_key,epoch_id) REFERENCES initial_consumer_origins(session_id,source_key,epoch_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE initial_consumer_results ADD FOREIGN KEY(session_id,epoch_id,call_id,registration_digest) REFERENCES initial_consumer_calls(session_id,epoch_id,call_id,record_digest) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE initial_consumer_results ADD FOREIGN KEY(session_id,epoch_id,call_id) REFERENCES initial_consumer_roster(session_id,epoch_id,call_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE initial_consumer_results ADD FOREIGN KEY(session_id,source_key,epoch_id) REFERENCES initial_consumer_origins(session_id,source_key,epoch_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE initial_consumer_roster ADD FOREIGN KEY(session_id,epoch_id,call_id,registration_digest) REFERENCES initial_consumer_calls(session_id,epoch_id,call_id,record_digest) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE initial_consumer_roster ADD FOREIGN KEY(session_id,epoch_id,call_id,result_digest) REFERENCES initial_consumer_results(session_id,epoch_id,call_id,record_digest) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE initial_consumer_roster ADD FOREIGN KEY(session_id,source_key,epoch_id) REFERENCES initial_consumer_origins(session_id,source_key,epoch_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE initial_consumer_reverse ADD FOREIGN KEY(session_id,epoch_id,call_id,result_digest) REFERENCES initial_consumer_results(session_id,epoch_id,call_id,record_digest) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE initial_consumer_reverse ADD FOREIGN KEY(session_id,source_key,epoch_id) REFERENCES initial_consumer_origins(session_id,source_key,epoch_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE initial_consumer_publications ADD FOREIGN KEY(session_id,source_key,epoch_id) REFERENCES initial_consumer_origins(session_id,source_key,epoch_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE initial_consumer_heads ADD FOREIGN KEY(session_id,epoch_id,sequence,record_digest) REFERENCES initial_consumer_publications(session_id,epoch_id,sequence,record_digest) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE initial_consumer_heads ADD FOREIGN KEY(session_id,source_key,epoch_id) REFERENCES initial_consumer_origins(session_id,source_key,epoch_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE initial_consumer_closes ADD FOREIGN KEY(session_id,source_key,epoch_id) REFERENCES initial_consumer_origins(session_id,source_key,epoch_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE initial_consumer_origins ADD FOREIGN KEY(epoch_id) REFERENCES pending_initial_source_slots(epoch_id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE initial_consumer_assignments ADD FOREIGN KEY(session_id,epoch_id,run_id) REFERENCES pending_initial_assignments(session_id,epoch_id,run_id) DEFERRABLE INITIALLY DEFERRED;
CREATE FUNCTION crabber_initial_consumer_guard() RETURNS trigger AS $$
BEGIN
 IF TG_OP='DELETE' THEN
  IF EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) THEN RETURN OLD; END IF;
  RAISE EXCEPTION 'crabber_initial_consumer_immutable';
 END IF;
 IF TG_OP='UPDATE' THEN
  IF TG_TABLE_NAME='initial_consumer_heads' THEN
   IF NEW.session_id=OLD.session_id AND NEW.source_key=OLD.source_key AND NEW.epoch_id=OLD.epoch_id AND NEW.origin_digest=OLD.origin_digest AND NEW.sequence=OLD.sequence+1 AND NEW.record_digest<>OLD.record_digest AND NOT EXISTS(SELECT 1 FROM initial_consumer_closes WHERE session_id=OLD.session_id AND epoch_id=OLD.epoch_id) AND NOT EXISTS(SELECT 1 FROM initial_consumer_origins WHERE epoch_id=OLD.epoch_id AND origin_kind='no_opened_member') THEN RETURN NEW; END IF;
  END IF;
  RAISE EXCEPTION 'crabber_initial_consumer_immutable';
 END IF;
 IF EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE EXCEPTION 'crabber_session_deleted'; END IF;
 IF EXISTS(SELECT 1 FROM initial_consumer_origins WHERE epoch_id=NEW.epoch_id AND origin_kind='no_opened_member') THEN
  IF TG_TABLE_NAME='initial_consumer_cohorts' THEN RETURN NEW; END IF;
  IF TG_TABLE_NAME IN ('initial_consumer_publications','initial_consumer_heads') THEN
   IF NEW.sequence=1 THEN RETURN NEW; END IF;
  END IF;
  RAISE EXCEPTION 'crabber_initial_consumer_closed';
 END IF;
 IF TG_TABLE_NAME<>'initial_consumer_cohorts' AND EXISTS(SELECT 1 FROM initial_consumer_closes WHERE session_id=NEW.session_id AND epoch_id=NEW.epoch_id) THEN RAISE EXCEPTION 'crabber_initial_consumer_closed'; END IF;
 RETURN NEW;
END $$ LANGUAGE plpgsql;
CREATE TRIGGER initial_consumer_origins_guard BEFORE INSERT OR UPDATE OR DELETE ON initial_consumer_origins FOR EACH ROW EXECUTE FUNCTION crabber_initial_consumer_guard();
CREATE TRIGGER initial_consumer_cohorts_guard BEFORE INSERT OR UPDATE OR DELETE ON initial_consumer_cohorts FOR EACH ROW EXECUTE FUNCTION crabber_initial_consumer_guard();
CREATE TRIGGER initial_consumer_assignments_guard BEFORE INSERT OR UPDATE OR DELETE ON initial_consumer_assignments FOR EACH ROW EXECUTE FUNCTION crabber_initial_consumer_guard();
CREATE TRIGGER initial_consumer_calls_guard BEFORE INSERT OR UPDATE OR DELETE ON initial_consumer_calls FOR EACH ROW EXECUTE FUNCTION crabber_initial_consumer_guard();
CREATE TRIGGER initial_consumer_results_guard BEFORE INSERT OR UPDATE OR DELETE ON initial_consumer_results FOR EACH ROW EXECUTE FUNCTION crabber_initial_consumer_guard();
CREATE TRIGGER initial_consumer_roster_guard BEFORE INSERT OR UPDATE OR DELETE ON initial_consumer_roster FOR EACH ROW EXECUTE FUNCTION crabber_initial_consumer_guard();
CREATE TRIGGER initial_consumer_reverse_guard BEFORE INSERT OR UPDATE OR DELETE ON initial_consumer_reverse FOR EACH ROW EXECUTE FUNCTION crabber_initial_consumer_guard();
CREATE TRIGGER initial_consumer_publications_guard BEFORE INSERT OR UPDATE OR DELETE ON initial_consumer_publications FOR EACH ROW EXECUTE FUNCTION crabber_initial_consumer_guard();
CREATE TRIGGER initial_consumer_heads_guard BEFORE INSERT OR UPDATE OR DELETE ON initial_consumer_heads FOR EACH ROW EXECUTE FUNCTION crabber_initial_consumer_guard();
CREATE TRIGGER initial_consumer_closes_guard BEFORE INSERT OR UPDATE OR DELETE ON initial_consumer_closes FOR EACH ROW EXECUTE FUNCTION crabber_initial_consumer_guard();
INSERT INTO schema_version(version) VALUES(24) ON CONFLICT(version) DO NOTHING;
