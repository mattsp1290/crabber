-- Genuine first Pending producer. No legacy backfill, Session or Run FK before birth.
CREATE UNIQUE INDEX native_execution_pending_scope ON native_execution_origins(session_id,run_id,birth_kind);
CREATE TABLE pending_initial_cohorts (
 session_id TEXT NOT NULL CHECK(length(CAST(session_id AS BLOB)) BETWEEN 1 AND 512),
 source_key TEXT NOT NULL CHECK(length(source_key)=64 AND source_key NOT GLOB '*[^0-9a-f]*'),
 epoch_id TEXT NOT NULL CHECK(length(CAST(epoch_id AS BLOB)) BETWEEN 1 AND 128),
 epoch_digest TEXT NOT NULL CHECK(length(epoch_digest)=64 AND epoch_digest NOT GLOB '*[^0-9a-f]*'),
 PRIMARY KEY(session_id,source_key), UNIQUE(epoch_id), UNIQUE(session_id,source_key,epoch_id,epoch_digest),
 FOREIGN KEY(session_id,source_key,epoch_id,epoch_digest) REFERENCES pending_initial_source_slots(session_id,source_key,epoch_id,epoch_digest) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE pending_initial_epochs (
 session_id TEXT NOT NULL CHECK(length(CAST(session_id AS BLOB)) BETWEEN 1 AND 512),
 source_key TEXT NOT NULL CHECK(length(CAST(source_key AS BLOB)) BETWEEN 1 AND 64),
 epoch_id TEXT NOT NULL CHECK(length(CAST(epoch_id AS BLOB)) BETWEEN 1 AND 128),
 epoch_digest TEXT NOT NULL CHECK(length(CAST(epoch_digest AS BLOB)) BETWEEN 1 AND 64),
 kind TEXT NOT NULL CHECK(length(CAST(kind AS BLOB)) BETWEEN 1 AND 32),
 complete_call TEXT NOT NULL CHECK(length(CAST(complete_call AS BLOB)) BETWEEN 1 AND 128),
 required_complete TEXT,
 required_close TEXT,
 data_json TEXT NOT NULL CHECK(length(CAST(data_json AS BLOB)) BETWEEN 1 AND 8192),
 CHECK((kind='Opened' AND required_complete IS NOT NULL AND required_complete=complete_call AND required_close IS NULL)
 OR (kind='SealedBeforeOpen' AND required_complete IS NULL AND required_close IS NOT NULL)),
 PRIMARY KEY(session_id,epoch_id), UNIQUE(epoch_id), UNIQUE(session_id,source_key,epoch_id,epoch_digest),
 FOREIGN KEY(session_id,source_key,epoch_id,epoch_digest) REFERENCES pending_initial_source_slots(session_id,source_key,epoch_id,epoch_digest) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(session_id,epoch_id,required_complete) REFERENCES pending_initial_registered_completes(session_id,epoch_id,call_id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(session_id,epoch_id,required_close) REFERENCES pending_initial_closes(session_id,epoch_id,seal_id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(session_id,epoch_id) REFERENCES pending_initial_heads(session_id,epoch_id) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE pending_initial_source_slots (
 session_id TEXT NOT NULL CHECK(length(CAST(session_id AS BLOB)) BETWEEN 1 AND 512),
 source_key TEXT NOT NULL CHECK(length(CAST(source_key AS BLOB)) BETWEEN 1 AND 64),
 epoch_id TEXT NOT NULL CHECK(length(CAST(epoch_id AS BLOB)) BETWEEN 1 AND 128),
 epoch_digest TEXT NOT NULL CHECK(length(CAST(epoch_digest AS BLOB)) BETWEEN 1 AND 64),
 data_json TEXT NOT NULL CHECK(length(CAST(data_json AS BLOB)) BETWEEN 1 AND 8192),
 PRIMARY KEY(session_id,source_key), UNIQUE(epoch_id), UNIQUE(session_id,source_key,epoch_id,epoch_digest),
 FOREIGN KEY(session_id,source_key,epoch_id,epoch_digest) REFERENCES pending_initial_epochs(session_id,source_key,epoch_id,epoch_digest) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(session_id,source_key,epoch_id,epoch_digest) REFERENCES pending_initial_cohorts(session_id,source_key,epoch_id,epoch_digest) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE pending_initial_registered_completes (
 session_id TEXT NOT NULL CHECK(length(CAST(session_id AS BLOB)) BETWEEN 1 AND 512),
 source_key TEXT NOT NULL CHECK(length(CAST(source_key AS BLOB)) BETWEEN 1 AND 64),
 epoch_id TEXT NOT NULL CHECK(length(CAST(epoch_id AS BLOB)) BETWEEN 1 AND 128),
 epoch_digest TEXT NOT NULL CHECK(length(CAST(epoch_digest AS BLOB)) BETWEEN 1 AND 64),
 call_id TEXT NOT NULL CHECK(length(CAST(call_id AS BLOB)) BETWEEN 1 AND 128),
 data_json TEXT NOT NULL CHECK(length(CAST(data_json AS BLOB)) BETWEEN 1 AND 8192),
 PRIMARY KEY(session_id,epoch_id), UNIQUE(call_id), UNIQUE(session_id,epoch_id,call_id),
 FOREIGN KEY(session_id,source_key,epoch_id,epoch_digest) REFERENCES pending_initial_epochs(session_id,source_key,epoch_id,epoch_digest) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE pending_initial_assignments (
 session_id TEXT NOT NULL CHECK(length(CAST(session_id AS BLOB)) BETWEEN 1 AND 512),
 source_key TEXT NOT NULL CHECK(length(CAST(source_key AS BLOB)) BETWEEN 1 AND 64),
 epoch_id TEXT NOT NULL CHECK(length(CAST(epoch_id AS BLOB)) BETWEEN 1 AND 128),
 epoch_digest TEXT NOT NULL CHECK(length(CAST(epoch_digest AS BLOB)) BETWEEN 1 AND 64),
 call_id TEXT NOT NULL CHECK(length(CAST(call_id AS BLOB)) BETWEEN 1 AND 128),
 run_id TEXT NOT NULL CHECK(length(CAST(run_id AS BLOB)) BETWEEN 1 AND 128),
 data_json TEXT NOT NULL CHECK(length(CAST(data_json AS BLOB)) BETWEEN 1 AND 8192),
 birth_kind TEXT NOT NULL DEFAULT 'pending' CHECK(birth_kind='pending'),
 PRIMARY KEY(session_id,epoch_id), UNIQUE(run_id), UNIQUE(session_id,epoch_id,run_id),
 FOREIGN KEY(session_id,epoch_id,call_id) REFERENCES pending_initial_registered_completes(session_id,epoch_id,call_id) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(session_id,source_key,epoch_id,epoch_digest) REFERENCES pending_initial_epochs(session_id,source_key,epoch_id,epoch_digest) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(session_id,run_id,birth_kind) REFERENCES native_execution_origins(session_id,run_id,birth_kind) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE pending_initial_closes (
 session_id TEXT NOT NULL CHECK(length(CAST(session_id AS BLOB)) BETWEEN 1 AND 512),
 source_key TEXT NOT NULL CHECK(length(CAST(source_key AS BLOB)) BETWEEN 1 AND 64),
 epoch_id TEXT NOT NULL CHECK(length(CAST(epoch_id AS BLOB)) BETWEEN 1 AND 128),
 epoch_digest TEXT NOT NULL CHECK(length(CAST(epoch_digest AS BLOB)) BETWEEN 1 AND 64),
 seal_id TEXT NOT NULL CHECK(length(CAST(seal_id AS BLOB)) BETWEEN 1 AND 128),
 data_json TEXT NOT NULL CHECK(length(CAST(data_json AS BLOB)) BETWEEN 1 AND 8192),
 PRIMARY KEY(session_id,epoch_id), UNIQUE(seal_id), UNIQUE(session_id,epoch_id,seal_id),
 FOREIGN KEY(session_id,source_key,epoch_id,epoch_digest) REFERENCES pending_initial_epochs(session_id,source_key,epoch_id,epoch_digest) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE pending_initial_publications (
 session_id TEXT NOT NULL CHECK(length(CAST(session_id AS BLOB)) BETWEEN 1 AND 512),
 source_key TEXT NOT NULL CHECK(length(CAST(source_key AS BLOB)) BETWEEN 1 AND 64),
 epoch_id TEXT NOT NULL CHECK(length(CAST(epoch_id AS BLOB)) BETWEEN 1 AND 128),
 epoch_digest TEXT NOT NULL CHECK(length(CAST(epoch_digest AS BLOB)) BETWEEN 1 AND 64),
 sequence BIGINT NOT NULL CHECK(sequence BETWEEN 1 AND 3),
 kind TEXT NOT NULL CHECK(length(CAST(kind AS BLOB)) BETWEEN 1 AND 32),
 publication_digest TEXT NOT NULL CHECK(length(CAST(publication_digest AS BLOB)) BETWEEN 1 AND 64),
 data_json TEXT NOT NULL CHECK(length(CAST(data_json AS BLOB)) BETWEEN 1 AND 8192),
 PRIMARY KEY(session_id,epoch_id,sequence), UNIQUE(session_id,epoch_id,sequence,publication_digest),
 FOREIGN KEY(session_id,source_key,epoch_id,epoch_digest) REFERENCES pending_initial_epochs(session_id,source_key,epoch_id,epoch_digest) DEFERRABLE INITIALLY DEFERRED
);
CREATE TABLE pending_initial_heads (
 session_id TEXT NOT NULL CHECK(length(CAST(session_id AS BLOB)) BETWEEN 1 AND 512),
 source_key TEXT NOT NULL CHECK(length(CAST(source_key AS BLOB)) BETWEEN 1 AND 64),
 epoch_id TEXT NOT NULL CHECK(length(CAST(epoch_id AS BLOB)) BETWEEN 1 AND 128),
 epoch_digest TEXT NOT NULL CHECK(length(CAST(epoch_digest AS BLOB)) BETWEEN 1 AND 64),
 sequence BIGINT NOT NULL CHECK(sequence BETWEEN 1 AND 3),
 publication_digest TEXT NOT NULL CHECK(length(CAST(publication_digest AS BLOB)) BETWEEN 1 AND 64),
 data_json TEXT NOT NULL CHECK(length(CAST(data_json AS BLOB)) BETWEEN 1 AND 8192),
 PRIMARY KEY(session_id,epoch_id),
 FOREIGN KEY(session_id,source_key,epoch_id,epoch_digest) REFERENCES pending_initial_epochs(session_id,source_key,epoch_id,epoch_digest) DEFERRABLE INITIALLY DEFERRED,
 FOREIGN KEY(session_id,epoch_id,sequence,publication_digest) REFERENCES pending_initial_publications(session_id,epoch_id,sequence,publication_digest) DEFERRABLE INITIALLY DEFERRED
);

ALTER TABLE native_execution_origins ADD COLUMN pending_epoch TEXT
 REFERENCES pending_initial_epochs(epoch_id) DEFERRABLE INITIALLY DEFERRED
 CHECK((birth_kind='raw' AND pending_epoch IS NULL) OR (birth_kind='pending' AND pending_epoch IS NOT NULL));
CREATE TRIGGER pending_origin_assignment BEFORE INSERT ON native_execution_origins WHEN NEW.birth_kind='pending' BEGIN
 SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM pending_initial_assignments a WHERE a.session_id=NEW.session_id AND a.epoch_id=NEW.pending_epoch AND a.run_id=NEW.run_id)
 THEN RAISE(ABORT,'crabber_pending_assignment_required') END;
END;
CREATE TRIGGER pending_initial_epochs_insert BEFORE INSERT ON pending_initial_epochs BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM pending_initial_closes WHERE session_id=NEW.session_id AND epoch_id=NEW.epoch_id) THEN RAISE(ABORT,'crabber_pending_initial_closed') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE(ABORT,'crabber_session_deleted') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM pending_initial_epochs WHERE (session_id=NEW.session_id AND epoch_id=NEW.epoch_id) OR epoch_id=NEW.epoch_id) THEN RAISE(ABORT,'crabber_pending_initial_immutable') END;
END;
CREATE TRIGGER pending_initial_epochs_delete BEFORE DELETE ON pending_initial_epochs WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_pending_initial_immutable'); END;
CREATE TRIGGER pending_initial_epochs_update BEFORE UPDATE ON pending_initial_epochs BEGIN SELECT RAISE(ABORT,'crabber_pending_initial_immutable'); END;
CREATE TRIGGER pending_initial_source_slots_insert BEFORE INSERT ON pending_initial_source_slots BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM pending_initial_closes WHERE session_id=NEW.session_id AND epoch_id=NEW.epoch_id) THEN RAISE(ABORT,'crabber_pending_initial_closed') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE(ABORT,'crabber_session_deleted') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM pending_initial_source_slots WHERE (session_id=NEW.session_id AND source_key=NEW.source_key) OR epoch_id=NEW.epoch_id) THEN RAISE(ABORT,'crabber_pending_initial_immutable') END;
END;
CREATE TRIGGER pending_initial_source_slots_delete BEFORE DELETE ON pending_initial_source_slots WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_pending_initial_immutable'); END;
CREATE TRIGGER pending_initial_source_slots_update BEFORE UPDATE ON pending_initial_source_slots BEGIN SELECT RAISE(ABORT,'crabber_pending_initial_immutable'); END;
CREATE TRIGGER pending_initial_registered_completes_insert BEFORE INSERT ON pending_initial_registered_completes BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM pending_initial_closes WHERE session_id=NEW.session_id AND epoch_id=NEW.epoch_id) THEN RAISE(ABORT,'crabber_pending_initial_closed') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE(ABORT,'crabber_session_deleted') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM pending_initial_registered_completes WHERE (session_id=NEW.session_id AND epoch_id=NEW.epoch_id) OR call_id=NEW.call_id) THEN RAISE(ABORT,'crabber_pending_initial_immutable') END;
END;
CREATE TRIGGER pending_initial_registered_completes_delete BEFORE DELETE ON pending_initial_registered_completes WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_pending_initial_immutable'); END;
CREATE TRIGGER pending_initial_registered_completes_update BEFORE UPDATE ON pending_initial_registered_completes BEGIN SELECT RAISE(ABORT,'crabber_pending_initial_immutable'); END;
CREATE TRIGGER pending_initial_assignments_insert BEFORE INSERT ON pending_initial_assignments BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM pending_initial_closes WHERE session_id=NEW.session_id AND epoch_id=NEW.epoch_id) THEN RAISE(ABORT,'crabber_pending_initial_closed') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE(ABORT,'crabber_session_deleted') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM pending_initial_assignments WHERE (session_id=NEW.session_id AND epoch_id=NEW.epoch_id) OR run_id=NEW.run_id) THEN RAISE(ABORT,'crabber_pending_initial_immutable') END;
END;
CREATE TRIGGER pending_initial_assignments_delete BEFORE DELETE ON pending_initial_assignments WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_pending_initial_immutable'); END;
CREATE TRIGGER pending_initial_assignments_update BEFORE UPDATE ON pending_initial_assignments BEGIN SELECT RAISE(ABORT,'crabber_pending_initial_immutable'); END;
CREATE TRIGGER pending_initial_closes_insert BEFORE INSERT ON pending_initial_closes BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM pending_initial_closes WHERE session_id=NEW.session_id AND epoch_id=NEW.epoch_id) THEN RAISE(ABORT,'crabber_pending_initial_closed') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE(ABORT,'crabber_session_deleted') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM pending_initial_closes WHERE (session_id=NEW.session_id AND epoch_id=NEW.epoch_id) OR seal_id=NEW.seal_id) THEN RAISE(ABORT,'crabber_pending_initial_immutable') END;
END;
CREATE TRIGGER pending_initial_closes_delete BEFORE DELETE ON pending_initial_closes WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_pending_initial_immutable'); END;
CREATE TRIGGER pending_initial_closes_update BEFORE UPDATE ON pending_initial_closes BEGIN SELECT RAISE(ABORT,'crabber_pending_initial_immutable'); END;
CREATE TRIGGER pending_initial_publications_insert BEFORE INSERT ON pending_initial_publications BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM pending_initial_closes WHERE session_id=NEW.session_id AND epoch_id=NEW.epoch_id) THEN RAISE(ABORT,'crabber_pending_initial_closed') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE(ABORT,'crabber_session_deleted') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM pending_initial_publications WHERE (session_id=NEW.session_id AND epoch_id=NEW.epoch_id AND sequence=NEW.sequence)) THEN RAISE(ABORT,'crabber_pending_initial_immutable') END;
END;
CREATE TRIGGER pending_initial_publications_delete BEFORE DELETE ON pending_initial_publications WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_pending_initial_immutable'); END;
CREATE TRIGGER pending_initial_publications_update BEFORE UPDATE ON pending_initial_publications BEGIN SELECT RAISE(ABORT,'crabber_pending_initial_immutable'); END;
CREATE TRIGGER pending_initial_heads_insert BEFORE INSERT ON pending_initial_heads BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM pending_initial_closes WHERE session_id=NEW.session_id AND epoch_id=NEW.epoch_id) THEN RAISE(ABORT,'crabber_pending_initial_closed') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE(ABORT,'crabber_session_deleted') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM pending_initial_heads WHERE (session_id=NEW.session_id AND epoch_id=NEW.epoch_id)) THEN RAISE(ABORT,'crabber_pending_initial_immutable') END;
END;
CREATE TRIGGER pending_initial_heads_delete BEFORE DELETE ON pending_initial_heads WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_pending_initial_immutable'); END;
CREATE TRIGGER pending_initial_heads_update BEFORE UPDATE ON pending_initial_heads WHEN EXISTS(SELECT 1 FROM pending_initial_closes WHERE session_id=NEW.session_id AND epoch_id=NEW.epoch_id) OR NEW.session_id IS NOT OLD.session_id OR NEW.source_key IS NOT OLD.source_key OR NEW.epoch_id IS NOT OLD.epoch_id OR NEW.epoch_digest IS NOT OLD.epoch_digest OR NEW.sequence<>OLD.sequence+1 BEGIN SELECT RAISE(ABORT,'crabber_pending_initial_immutable'); END;
CREATE TRIGGER pending_initial_head_insert_shape BEFORE INSERT ON pending_initial_heads
 WHEN NEW.sequence<>1 OR NEW.sequence IS NOT (SELECT MAX(sequence) FROM pending_initial_publications WHERE session_id=NEW.session_id AND epoch_id=NEW.epoch_id)
 BEGIN SELECT RAISE(ABORT,'crabber_pending_initial_head'); END;
CREATE TRIGGER pending_initial_head_update_shape BEFORE UPDATE ON pending_initial_heads
 WHEN NEW.sequence IS NOT (SELECT MAX(sequence) FROM pending_initial_publications WHERE session_id=NEW.session_id AND epoch_id=NEW.epoch_id)
 BEGIN SELECT RAISE(ABORT,'crabber_pending_initial_head'); END;
CREATE TRIGGER pending_initial_cohorts_insert BEFORE INSERT ON pending_initial_cohorts BEGIN
 SELECT CASE WHEN EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=NEW.session_id) THEN RAISE(ABORT,'crabber_session_deleted') END;
 SELECT CASE WHEN EXISTS(SELECT 1 FROM pending_initial_cohorts WHERE (session_id=NEW.session_id AND source_key=NEW.source_key) OR epoch_id=NEW.epoch_id) THEN RAISE(ABORT,'crabber_pending_initial_immutable') END;
END;
CREATE TRIGGER pending_initial_cohorts_update BEFORE UPDATE ON pending_initial_cohorts BEGIN SELECT RAISE(ABORT,'crabber_pending_initial_immutable'); END;
CREATE TRIGGER pending_initial_cohorts_delete BEFORE DELETE ON pending_initial_cohorts WHEN NOT EXISTS(SELECT 1 FROM session_tombstones WHERE session_id=OLD.session_id) BEGIN SELECT RAISE(ABORT,'crabber_pending_initial_immutable'); END;
