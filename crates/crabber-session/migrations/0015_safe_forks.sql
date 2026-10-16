-- Imported calls are presentation history, never native executable records.
ALTER TABLE operation_receipts DROP CONSTRAINT IF EXISTS operation_receipts_audit_kind;
ALTER TABLE operation_receipts ADD CONSTRAINT operation_receipts_audit_kind CHECK ((kind IN ('session_management','future_queue','session_fork') AND event_seq IS NULL) OR (kind NOT IN ('session_management','future_queue','session_fork') AND event_seq IS NOT NULL));
CREATE TABLE IF NOT EXISTS tool_call_identities (
 id text PRIMARY KEY,
 kind text NOT NULL CHECK(kind IN ('native','imported'))
);
INSERT INTO tool_call_identities(id,kind) SELECT id,'native' FROM tool_calls ON CONFLICT DO NOTHING;
DO $$ BEGIN
 IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='tool_calls_identity' AND conrelid='tool_calls'::regclass) THEN
  ALTER TABLE tool_calls ADD CONSTRAINT tool_calls_identity FOREIGN KEY(id) REFERENCES tool_call_identities(id);
 END IF;
END $$;
CREATE TABLE IF NOT EXISTS imported_tool_calls (
 id text PRIMARY KEY REFERENCES tool_call_identities(id), session_id text NOT NULL REFERENCES sessions(id),
 ordinal bigint NOT NULL CHECK(ordinal>0), data_json text NOT NULL,
 snapshot_parts bigint NOT NULL CHECK(snapshot_parts=0),
 snapshot_text bigint NOT NULL CHECK(snapshot_text>=0),
 snapshot_bytes bigint NOT NULL CHECK(snapshot_bytes>0 AND snapshot_bytes<=8388608),
 UNIQUE(session_id,ordinal)
);
CREATE TABLE IF NOT EXISTS imported_message_provenance (
 message_id text PRIMARY KEY REFERENCES messages(id),
 session_id text NOT NULL REFERENCES sessions(id),
 ordinal bigint NOT NULL CHECK(ordinal>0), data_json text NOT NULL,
 snapshot_bytes bigint NOT NULL CHECK(snapshot_bytes>0 AND snapshot_bytes<=8388608),
 UNIQUE(session_id,ordinal)
);
CREATE TABLE IF NOT EXISTS session_fork_audit (
 session_id text PRIMARY KEY REFERENCES sessions(id), operation_id text NOT NULL,
 data_json text NOT NULL,
 FOREIGN KEY(session_id,operation_id) REFERENCES operation_receipts(session_id,operation_id) DEFERRABLE INITIALLY DEFERRED
);
-- The unique registry arbitrates cross-table races before executable insertion.
CREATE OR REPLACE FUNCTION reserve_tool_call_identity() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected text := TG_ARGV[0]; actual text;
BEGIN
 INSERT INTO tool_call_identities(id,kind) VALUES(NEW.id,expected) ON CONFLICT DO NOTHING;
 SELECT kind INTO actual FROM tool_call_identities WHERE id=NEW.id FOR KEY SHARE;
 IF actual IS DISTINCT FROM expected THEN RAISE EXCEPTION 'tool call identity already reserved' USING ERRCODE='23505'; END IF;
 RETURN NEW;
END $$;
CREATE OR REPLACE FUNCTION release_tool_call_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 DELETE FROM tool_call_identities WHERE id=OLD.id AND kind=TG_ARGV[0];
 RETURN OLD;
END $$;
DROP TRIGGER IF EXISTS native_call_identity_insert ON tool_calls;
CREATE TRIGGER native_call_identity_insert BEFORE INSERT OR UPDATE OF id ON tool_calls FOR EACH ROW EXECUTE FUNCTION reserve_tool_call_identity('native');
DROP TRIGGER IF EXISTS native_call_identity_delete ON tool_calls;
CREATE TRIGGER native_call_identity_delete AFTER DELETE ON tool_calls FOR EACH ROW EXECUTE FUNCTION release_tool_call_identity('native');
DROP TRIGGER IF EXISTS native_call_identity_update ON tool_calls;
CREATE TRIGGER native_call_identity_update AFTER UPDATE OF id ON tool_calls FOR EACH ROW WHEN(OLD.id IS DISTINCT FROM NEW.id) EXECUTE FUNCTION release_tool_call_identity('native');
DROP TRIGGER IF EXISTS imported_call_identity_insert ON imported_tool_calls;
CREATE TRIGGER imported_call_identity_insert BEFORE INSERT OR UPDATE OF id ON imported_tool_calls FOR EACH ROW EXECUTE FUNCTION reserve_tool_call_identity('imported');
DROP TRIGGER IF EXISTS imported_call_identity_delete ON imported_tool_calls;
CREATE TRIGGER imported_call_identity_delete AFTER DELETE ON imported_tool_calls FOR EACH ROW EXECUTE FUNCTION release_tool_call_identity('imported');
DROP TRIGGER IF EXISTS imported_call_identity_update ON imported_tool_calls;
CREATE TRIGGER imported_call_identity_update AFTER UPDATE OF id ON imported_tool_calls FOR EACH ROW WHEN(OLD.id IS DISTINCT FROM NEW.id) EXECUTE FUNCTION release_tool_call_identity('imported');
INSERT INTO schema_version(version) VALUES(15) ON CONFLICT DO NOTHING;
