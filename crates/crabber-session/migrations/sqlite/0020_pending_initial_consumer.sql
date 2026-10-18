-- Closed original Initial consumer; no legacy adoption or Session/Run FK before birth.
CREATE TABLE initial_consumer_origins (
 session_id TEXT NOT NULL CHECK(length(CAST(session_id AS BLOB)) BETWEEN 1 AND 512),
 source_key TEXT NOT NULL CHECK(length(CAST(source_key AS BLOB))=64),
 epoch_id TEXT NOT NULL CHECK(length(CAST(epoch_id AS BLOB)) BETWEEN 1 AND 128),
 record_digest TEXT NOT NULL CHECK(length(CAST(record_digest AS BLOB))=64),
 origin_kind TEXT NOT NULL CHECK(origin_kind IN ('opened','no_opened_member')),
 data_json TEXT NOT NULL CHECK(length(CAST(data_json AS BLOB))>0),
 PRIMARY KEY(session_id,source_key),
 UNIQUE(epoch_id),
 UNIQUE(session_id,source_key,epoch_id),
 UNIQUE(session_id,source_key,epoch_id,record_digest),
 FOREIGN KEY(session_id,source_key,epoch_id,record_digest) REFERENCES initial_consumer_cohorts(session_id,source_key,epoch_id,record_digest) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(session_id,epoch_id) REFERENCES initial_consumer_heads(session_id,epoch_id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(epoch_id) REFERENCES pending_initial_source_slots(epoch_id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE initial_consumer_cohorts (
 session_id TEXT NOT NULL CHECK(length(CAST(session_id AS BLOB)) BETWEEN 1 AND 512),
 source_key TEXT NOT NULL CHECK(length(CAST(source_key AS BLOB))=64),
 epoch_id TEXT NOT NULL CHECK(length(CAST(epoch_id AS BLOB)) BETWEEN 1 AND 128),
 record_digest TEXT NOT NULL CHECK(length(CAST(record_digest AS BLOB))=64),
 origin_kind TEXT NOT NULL CHECK(origin_kind IN ('opened','no_opened_member')),
 data_json TEXT NOT NULL CHECK(length(CAST(data_json AS BLOB))>0),
 PRIMARY KEY(session_id,source_key),
 UNIQUE(epoch_id),
 UNIQUE(session_id,source_key,epoch_id,record_digest),
 FOREIGN KEY(session_id,source_key,epoch_id,record_digest) REFERENCES initial_consumer_origins(session_id,source_key,epoch_id,record_digest) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE initial_consumer_assignments (
 session_id TEXT NOT NULL CHECK(length(CAST(session_id AS BLOB)) BETWEEN 1 AND 512),
 source_key TEXT NOT NULL CHECK(length(CAST(source_key AS BLOB))=64),
 epoch_id TEXT NOT NULL CHECK(length(CAST(epoch_id AS BLOB)) BETWEEN 1 AND 128),
 record_digest TEXT NOT NULL CHECK(length(CAST(record_digest AS BLOB))=64),
 run_id TEXT NOT NULL UNIQUE,
 data_json TEXT NOT NULL CHECK(length(CAST(data_json AS BLOB))>0),
 PRIMARY KEY(session_id,epoch_id),
 FOREIGN KEY(session_id,source_key,epoch_id) REFERENCES initial_consumer_origins(session_id,source_key,epoch_id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(session_id,epoch_id,run_id) REFERENCES pending_initial_assignments(session_id,epoch_id,run_id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE initial_consumer_calls (
 session_id TEXT NOT NULL CHECK(length(CAST(session_id AS BLOB)) BETWEEN 1 AND 512),
 source_key TEXT NOT NULL CHECK(length(CAST(source_key AS BLOB))=64),
 epoch_id TEXT NOT NULL CHECK(length(CAST(epoch_id AS BLOB)) BETWEEN 1 AND 128),
 record_digest TEXT NOT NULL CHECK(length(CAST(record_digest AS BLOB))=64),
 call_id TEXT NOT NULL UNIQUE,kind TEXT NOT NULL,input_digest TEXT NOT NULL,
 data_json TEXT NOT NULL CHECK(length(CAST(data_json AS BLOB))>0),
 PRIMARY KEY(session_id,epoch_id,call_id),
 UNIQUE(session_id,epoch_id,call_id,record_digest),
 FOREIGN KEY(session_id,source_key,epoch_id) REFERENCES initial_consumer_origins(session_id,source_key,epoch_id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE initial_consumer_results (
 session_id TEXT NOT NULL CHECK(length(CAST(session_id AS BLOB)) BETWEEN 1 AND 512),
 source_key TEXT NOT NULL CHECK(length(CAST(source_key AS BLOB))=64),
 epoch_id TEXT NOT NULL CHECK(length(CAST(epoch_id AS BLOB)) BETWEEN 1 AND 128),
 record_digest TEXT NOT NULL CHECK(length(CAST(record_digest AS BLOB))=64),
 call_id TEXT NOT NULL,registration_digest TEXT NOT NULL,
 data_json TEXT NOT NULL CHECK(length(CAST(data_json AS BLOB))>0),
 PRIMARY KEY(session_id,epoch_id,call_id),
 UNIQUE(session_id,epoch_id,call_id,record_digest),
 FOREIGN KEY(session_id,epoch_id,call_id,registration_digest) REFERENCES initial_consumer_calls(session_id,epoch_id,call_id,record_digest) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(session_id,epoch_id,call_id) REFERENCES initial_consumer_roster(session_id,epoch_id,call_id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(session_id,source_key,epoch_id) REFERENCES initial_consumer_origins(session_id,source_key,epoch_id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE initial_consumer_roster (
 session_id TEXT NOT NULL CHECK(length(CAST(session_id AS BLOB)) BETWEEN 1 AND 512),
 source_key TEXT NOT NULL CHECK(length(CAST(source_key AS BLOB))=64),
 epoch_id TEXT NOT NULL CHECK(length(CAST(epoch_id AS BLOB)) BETWEEN 1 AND 128),
 record_digest TEXT NOT NULL CHECK(length(CAST(record_digest AS BLOB))=64),
 call_id TEXT NOT NULL,registration_digest TEXT NOT NULL,result_digest TEXT NOT NULL,
 data_json TEXT NOT NULL CHECK(length(CAST(data_json AS BLOB))>0),
 PRIMARY KEY(session_id,epoch_id,call_id),
 FOREIGN KEY(session_id,epoch_id,call_id,registration_digest) REFERENCES initial_consumer_calls(session_id,epoch_id,call_id,record_digest) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(session_id,epoch_id,call_id,result_digest) REFERENCES initial_consumer_results(session_id,epoch_id,call_id,record_digest) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(session_id,source_key,epoch_id) REFERENCES initial_consumer_origins(session_id,source_key,epoch_id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE initial_consumer_reverse (
 session_id TEXT NOT NULL CHECK(length(CAST(session_id AS BLOB)) BETWEEN 1 AND 512),
 source_key TEXT NOT NULL CHECK(length(CAST(source_key AS BLOB))=64),
 epoch_id TEXT NOT NULL CHECK(length(CAST(epoch_id AS BLOB)) BETWEEN 1 AND 128),
 record_digest TEXT NOT NULL CHECK(length(CAST(record_digest AS BLOB))=64),
 call_id TEXT NOT NULL,kind TEXT NOT NULL,member_key TEXT NOT NULL,result_digest TEXT NOT NULL,
 data_json TEXT NOT NULL CHECK(length(CAST(data_json AS BLOB))>0),
 PRIMARY KEY(kind,member_key),
 FOREIGN KEY(session_id,epoch_id,call_id,result_digest) REFERENCES initial_consumer_results(session_id,epoch_id,call_id,record_digest) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(session_id,source_key,epoch_id) REFERENCES initial_consumer_origins(session_id,source_key,epoch_id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE initial_consumer_publications (
 session_id TEXT NOT NULL CHECK(length(CAST(session_id AS BLOB)) BETWEEN 1 AND 512),
 source_key TEXT NOT NULL CHECK(length(CAST(source_key AS BLOB))=64),
 epoch_id TEXT NOT NULL CHECK(length(CAST(epoch_id AS BLOB)) BETWEEN 1 AND 128),
 record_digest TEXT NOT NULL CHECK(length(CAST(record_digest AS BLOB))=64),
 sequence BIGINT NOT NULL CHECK(sequence>0),kind TEXT NOT NULL,original_digest TEXT NOT NULL,previous_digest TEXT,
 data_json TEXT NOT NULL CHECK(length(CAST(data_json AS BLOB))>0),
 PRIMARY KEY(session_id,epoch_id,sequence),
 UNIQUE(session_id,epoch_id,sequence,record_digest),
 FOREIGN KEY(session_id,source_key,epoch_id) REFERENCES initial_consumer_origins(session_id,source_key,epoch_id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE initial_consumer_heads (
 session_id TEXT NOT NULL CHECK(length(CAST(session_id AS BLOB)) BETWEEN 1 AND 512),
 source_key TEXT NOT NULL CHECK(length(CAST(source_key AS BLOB))=64),
 epoch_id TEXT NOT NULL CHECK(length(CAST(epoch_id AS BLOB)) BETWEEN 1 AND 128),
 record_digest TEXT NOT NULL CHECK(length(CAST(record_digest AS BLOB))=64),
 sequence BIGINT NOT NULL CHECK(sequence>0),origin_digest TEXT NOT NULL,
 data_json TEXT NOT NULL CHECK(length(CAST(data_json AS BLOB))>0),
 PRIMARY KEY(session_id,epoch_id),
 FOREIGN KEY(session_id,epoch_id,sequence,record_digest) REFERENCES initial_consumer_publications(session_id,epoch_id,sequence,record_digest) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(session_id,source_key,epoch_id) REFERENCES initial_consumer_origins(session_id,source_key,epoch_id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE initial_consumer_closes (
 session_id TEXT NOT NULL CHECK(length(CAST(session_id AS BLOB)) BETWEEN 1 AND 512),
 source_key TEXT NOT NULL CHECK(length(CAST(source_key AS BLOB))=64),
 epoch_id TEXT NOT NULL CHECK(length(CAST(epoch_id AS BLOB)) BETWEEN 1 AND 128),
 record_digest TEXT NOT NULL CHECK(length(CAST(record_digest AS BLOB))=64),
 anchor_digest TEXT NOT NULL,birth_close_digest TEXT NOT NULL,
 data_json TEXT NOT NULL CHECK(length(CAST(data_json AS BLOB))>0),
 PRIMARY KEY(session_id,epoch_id),
 FOREIGN KEY(session_id,source_key,epoch_id) REFERENCES initial_consumer_origins(session_id,source_key,epoch_id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TRIGGER initial_consumer_origins_insert BEFORE INSERT ON initial_consumer_origins BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE(ABORT,'crabber_session_deleted') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM initial_consumer_origins WHERE (session_id=NEW.session_id AND source_key=NEW.source_key) OR epoch_id=NEW.epoch_id) THEN RAISE(ABORT,'crabber_initial_consumer_immutable') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM initial_consumer_closes WHERE session_id=NEW.session_id AND epoch_id=NEW.epoch_id) THEN RAISE(ABORT,'crabber_initial_consumer_closed') END;
END;
CREATE TRIGGER initial_consumer_origins_update BEFORE UPDATE ON initial_consumer_origins WHEN 1 BEGIN SELECT RAISE(ABORT,'crabber_initial_consumer_immutable'); END;
CREATE TRIGGER initial_consumer_origins_delete BEFORE DELETE ON initial_consumer_origins WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_initial_consumer_immutable'); END;
CREATE TRIGGER initial_consumer_cohorts_insert BEFORE INSERT ON initial_consumer_cohorts BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE(ABORT,'crabber_session_deleted') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM initial_consumer_cohorts WHERE (session_id=NEW.session_id AND source_key=NEW.source_key) OR epoch_id=NEW.epoch_id) THEN RAISE(ABORT,'crabber_initial_consumer_immutable') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM initial_consumer_closes WHERE session_id=NEW.session_id AND epoch_id=NEW.epoch_id) THEN RAISE(ABORT,'crabber_initial_consumer_closed') END;
END;
CREATE TRIGGER initial_consumer_cohorts_update BEFORE UPDATE ON initial_consumer_cohorts WHEN 1 BEGIN SELECT RAISE(ABORT,'crabber_initial_consumer_immutable'); END;
CREATE TRIGGER initial_consumer_cohorts_delete BEFORE DELETE ON initial_consumer_cohorts WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_initial_consumer_immutable'); END;
CREATE TRIGGER initial_consumer_assignments_insert BEFORE INSERT ON initial_consumer_assignments BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE(ABORT,'crabber_session_deleted') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM initial_consumer_assignments WHERE (session_id=NEW.session_id AND epoch_id=NEW.epoch_id) OR run_id=NEW.run_id) THEN RAISE(ABORT,'crabber_initial_consumer_immutable') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM initial_consumer_closes WHERE session_id=NEW.session_id AND epoch_id=NEW.epoch_id) THEN RAISE(ABORT,'crabber_initial_consumer_closed') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM initial_consumer_origins WHERE epoch_id=NEW.epoch_id AND origin_kind='no_opened_member') THEN RAISE(ABORT,'crabber_initial_consumer_closed') END;
END;
CREATE TRIGGER initial_consumer_assignments_update BEFORE UPDATE ON initial_consumer_assignments WHEN 1 BEGIN SELECT RAISE(ABORT,'crabber_initial_consumer_immutable'); END;
CREATE TRIGGER initial_consumer_assignments_delete BEFORE DELETE ON initial_consumer_assignments WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_initial_consumer_immutable'); END;
CREATE TRIGGER initial_consumer_calls_insert BEFORE INSERT ON initial_consumer_calls BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE(ABORT,'crabber_session_deleted') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM initial_consumer_calls WHERE (session_id=NEW.session_id AND epoch_id=NEW.epoch_id AND call_id=NEW.call_id) OR call_id=NEW.call_id) THEN RAISE(ABORT,'crabber_initial_consumer_immutable') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM initial_consumer_closes WHERE session_id=NEW.session_id AND epoch_id=NEW.epoch_id) THEN RAISE(ABORT,'crabber_initial_consumer_closed') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM initial_consumer_origins WHERE epoch_id=NEW.epoch_id AND origin_kind='no_opened_member') THEN RAISE(ABORT,'crabber_initial_consumer_closed') END;
END;
CREATE TRIGGER initial_consumer_calls_update BEFORE UPDATE ON initial_consumer_calls WHEN 1 BEGIN SELECT RAISE(ABORT,'crabber_initial_consumer_immutable'); END;
CREATE TRIGGER initial_consumer_calls_delete BEFORE DELETE ON initial_consumer_calls WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_initial_consumer_immutable'); END;
CREATE TRIGGER initial_consumer_results_insert BEFORE INSERT ON initial_consumer_results BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE(ABORT,'crabber_session_deleted') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM initial_consumer_results WHERE session_id=NEW.session_id AND epoch_id=NEW.epoch_id AND call_id=NEW.call_id) THEN RAISE(ABORT,'crabber_initial_consumer_immutable') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM initial_consumer_closes WHERE session_id=NEW.session_id AND epoch_id=NEW.epoch_id) THEN RAISE(ABORT,'crabber_initial_consumer_closed') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM initial_consumer_origins WHERE epoch_id=NEW.epoch_id AND origin_kind='no_opened_member') THEN RAISE(ABORT,'crabber_initial_consumer_closed') END;
END;
CREATE TRIGGER initial_consumer_results_update BEFORE UPDATE ON initial_consumer_results WHEN 1 BEGIN SELECT RAISE(ABORT,'crabber_initial_consumer_immutable'); END;
CREATE TRIGGER initial_consumer_results_delete BEFORE DELETE ON initial_consumer_results WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_initial_consumer_immutable'); END;
CREATE TRIGGER initial_consumer_roster_insert BEFORE INSERT ON initial_consumer_roster BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE(ABORT,'crabber_session_deleted') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM initial_consumer_roster WHERE session_id=NEW.session_id AND epoch_id=NEW.epoch_id AND call_id=NEW.call_id) THEN RAISE(ABORT,'crabber_initial_consumer_immutable') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM initial_consumer_closes WHERE session_id=NEW.session_id AND epoch_id=NEW.epoch_id) THEN RAISE(ABORT,'crabber_initial_consumer_closed') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM initial_consumer_origins WHERE epoch_id=NEW.epoch_id AND origin_kind='no_opened_member') THEN RAISE(ABORT,'crabber_initial_consumer_closed') END;
END;
CREATE TRIGGER initial_consumer_roster_update BEFORE UPDATE ON initial_consumer_roster WHEN 1 BEGIN SELECT RAISE(ABORT,'crabber_initial_consumer_immutable'); END;
CREATE TRIGGER initial_consumer_roster_delete BEFORE DELETE ON initial_consumer_roster WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_initial_consumer_immutable'); END;
CREATE TRIGGER initial_consumer_reverse_insert BEFORE INSERT ON initial_consumer_reverse BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE(ABORT,'crabber_session_deleted') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM initial_consumer_reverse WHERE kind=NEW.kind AND member_key=NEW.member_key) THEN RAISE(ABORT,'crabber_initial_consumer_immutable') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM initial_consumer_closes WHERE session_id=NEW.session_id AND epoch_id=NEW.epoch_id) THEN RAISE(ABORT,'crabber_initial_consumer_closed') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM initial_consumer_origins WHERE epoch_id=NEW.epoch_id AND origin_kind='no_opened_member') THEN RAISE(ABORT,'crabber_initial_consumer_closed') END;
END;
CREATE TRIGGER initial_consumer_reverse_update BEFORE UPDATE ON initial_consumer_reverse WHEN 1 BEGIN SELECT RAISE(ABORT,'crabber_initial_consumer_immutable'); END;
CREATE TRIGGER initial_consumer_reverse_delete BEFORE DELETE ON initial_consumer_reverse WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_initial_consumer_immutable'); END;
CREATE TRIGGER initial_consumer_publications_insert BEFORE INSERT ON initial_consumer_publications BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE(ABORT,'crabber_session_deleted') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM initial_consumer_publications WHERE session_id=NEW.session_id AND epoch_id=NEW.epoch_id AND sequence=NEW.sequence) THEN RAISE(ABORT,'crabber_initial_consumer_immutable') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM initial_consumer_closes WHERE session_id=NEW.session_id AND epoch_id=NEW.epoch_id) THEN RAISE(ABORT,'crabber_initial_consumer_closed') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM initial_consumer_origins WHERE epoch_id=NEW.epoch_id AND origin_kind='no_opened_member') AND NEW.sequence<>1 THEN RAISE(ABORT,'crabber_initial_consumer_closed') END;
END;
CREATE TRIGGER initial_consumer_publications_update BEFORE UPDATE ON initial_consumer_publications WHEN 1 BEGIN SELECT RAISE(ABORT,'crabber_initial_consumer_immutable'); END;
CREATE TRIGGER initial_consumer_publications_delete BEFORE DELETE ON initial_consumer_publications WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_initial_consumer_immutable'); END;
CREATE TRIGGER initial_consumer_heads_insert BEFORE INSERT ON initial_consumer_heads BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE(ABORT,'crabber_session_deleted') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM initial_consumer_heads WHERE session_id=NEW.session_id AND epoch_id=NEW.epoch_id) THEN RAISE(ABORT,'crabber_initial_consumer_immutable') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM initial_consumer_closes WHERE session_id=NEW.session_id AND epoch_id=NEW.epoch_id) THEN RAISE(ABORT,'crabber_initial_consumer_closed') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM initial_consumer_origins WHERE epoch_id=NEW.epoch_id AND origin_kind='no_opened_member') AND NEW.sequence<>1 THEN RAISE(ABORT,'crabber_initial_consumer_closed') END;
END;
CREATE TRIGGER initial_consumer_heads_update BEFORE UPDATE ON initial_consumer_heads WHEN NOT(NEW.session_id=OLD.session_id AND NEW.source_key=OLD.source_key AND NEW.epoch_id=OLD.epoch_id AND NEW.origin_digest=OLD.origin_digest AND NEW.sequence=OLD.sequence+1 AND NEW.record_digest<>OLD.record_digest AND NOT EXISTS(SELECT 1 FROM initial_consumer_closes WHERE session_id=OLD.session_id AND epoch_id=OLD.epoch_id) AND NOT EXISTS(SELECT 1 FROM initial_consumer_origins WHERE epoch_id=OLD.epoch_id AND origin_kind='no_opened_member')) BEGIN SELECT RAISE(ABORT,'crabber_initial_consumer_immutable'); END;
CREATE TRIGGER initial_consumer_heads_delete BEFORE DELETE ON initial_consumer_heads WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_initial_consumer_immutable'); END;
CREATE TRIGGER initial_consumer_closes_insert BEFORE INSERT ON initial_consumer_closes BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE(ABORT,'crabber_session_deleted') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM initial_consumer_closes WHERE session_id=NEW.session_id AND epoch_id=NEW.epoch_id) THEN RAISE(ABORT,'crabber_initial_consumer_immutable') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM initial_consumer_closes WHERE session_id=NEW.session_id AND epoch_id=NEW.epoch_id) THEN RAISE(ABORT,'crabber_initial_consumer_closed') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM initial_consumer_origins WHERE epoch_id=NEW.epoch_id AND origin_kind='no_opened_member') THEN RAISE(ABORT,'crabber_initial_consumer_closed') END;
END;
CREATE TRIGGER initial_consumer_closes_update BEFORE UPDATE ON initial_consumer_closes WHEN 1 BEGIN SELECT RAISE(ABORT,'crabber_initial_consumer_immutable'); END;
CREATE TRIGGER initial_consumer_closes_delete BEFORE DELETE ON initial_consumer_closes WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_initial_consumer_immutable'); END;
