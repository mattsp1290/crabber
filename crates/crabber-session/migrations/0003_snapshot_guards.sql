ALTER TABLE inbox ALTER COLUMN snapshot_record SET NOT NULL;
ALTER TABLE inbox ADD CONSTRAINT inbox_snapshot_record_matches CHECK (snapshot_record::jsonb=data);
ALTER TABLE messages ALTER COLUMN snapshot_record SET NOT NULL;
ALTER TABLE messages ALTER COLUMN snapshot_parts SET NOT NULL;
ALTER TABLE messages ALTER COLUMN snapshot_text SET NOT NULL;
ALTER TABLE messages ALTER COLUMN snapshot_bytes SET NOT NULL;
ALTER TABLE tool_calls ALTER COLUMN snapshot_record SET NOT NULL;
ALTER TABLE tool_calls ALTER COLUMN snapshot_parts SET NOT NULL;
ALTER TABLE tool_calls ALTER COLUMN snapshot_text SET NOT NULL;
ALTER TABLE tool_calls ALTER COLUMN snapshot_bytes SET NOT NULL;
CREATE FUNCTION snapshot_record_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE sid text;
BEGIN
  IF TG_OP <> 'DELETE' THEN
    IF NEW.snapshot_record::jsonb <> NEW.data OR octet_length(NEW.snapshot_record) <> NEW.snapshot_bytes THEN
      RAISE EXCEPTION 'snapshot accounting mismatch';
    END IF;
  END IF;
  IF TG_OP='INSERT' THEN RETURN NEW; END IF;
  IF TG_TABLE_NAME='messages' THEN
    sid := OLD.session_id;
  ELSE
    SELECT session_id INTO sid FROM runs WHERE id=OLD.run_id;
  END IF;
  UPDATE sessions SET snapshot_revision=snapshot_revision+1 WHERE id=sid;
  IF TG_OP='DELETE' THEN RETURN OLD; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER snapshot_messages BEFORE INSERT OR UPDATE OR DELETE ON messages FOR EACH ROW EXECUTE FUNCTION snapshot_record_guard();
CREATE TRIGGER snapshot_calls BEFORE INSERT OR UPDATE OR DELETE ON tool_calls FOR EACH ROW EXECUTE FUNCTION snapshot_record_guard();
INSERT INTO schema_version(version) VALUES (3);
