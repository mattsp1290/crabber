-- Applied once under the migration advisory lock and exclusive table locks.
-- Existing tool calls lack creation timestamps; recover pending-event order when
-- available, with deterministic ID order for legacy calls without such evidence.
ALTER TABLE sessions ADD COLUMN snapshot_revision bigint NOT NULL DEFAULT 0;
CREATE TABLE snapshot_auth (singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton), secret text NOT NULL);
ALTER TABLE messages ADD COLUMN snapshot_record text;
ALTER TABLE messages ADD COLUMN snapshot_parts bigint;
ALTER TABLE messages ADD COLUMN snapshot_text bigint;
ALTER TABLE messages ADD COLUMN snapshot_bytes bigint;
CREATE UNIQUE INDEX messages_snapshot_order ON messages(seq);
ALTER TABLE tool_calls ADD COLUMN seq bigint;
CREATE SEQUENCE tool_calls_snapshot_seq;
WITH ordered AS (
  SELECT t.id, row_number() OVER (ORDER BY
    (SELECT min(e.seq) FROM events e WHERE e.run_id=t.run_id AND e.data->>'kind'='tool_call_pending' AND e.data->'payload'->>'call_id'=t.id) NULLS LAST,
    t.id) AS position FROM tool_calls t
) UPDATE tool_calls t SET seq=o.position FROM ordered o WHERE t.id=o.id;
SELECT setval('tool_calls_snapshot_seq', greatest(coalesce((SELECT max(seq) FROM tool_calls),0)+1,1), false);
ALTER TABLE tool_calls ALTER COLUMN seq SET DEFAULT nextval('tool_calls_snapshot_seq');
ALTER TABLE tool_calls ALTER COLUMN seq SET NOT NULL;
CREATE UNIQUE INDEX tool_calls_snapshot_order ON tool_calls(seq);
ALTER TABLE tool_calls ADD COLUMN snapshot_record text;
ALTER TABLE tool_calls ADD COLUMN snapshot_parts bigint;
ALTER TABLE tool_calls ADD COLUMN snapshot_text bigint;
ALTER TABLE tool_calls ADD COLUMN snapshot_bytes bigint;
