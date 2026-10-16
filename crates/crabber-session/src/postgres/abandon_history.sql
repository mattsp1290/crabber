-- Fixed narrow projections; checkpoint/error are tested only for absence.
WITH original AS (
SELECT (r.data->>'id'=r.id AND r.data->>'session_id'=r.session_id AND r.data->>'status'=r.status AND r.data->>'claim_token'=r.claim_token) AS coherent,
a.event_seq AS committed_seq, e.seq AS source_seq,
jsonb_build_object(
  'id', (r.data)->'id',
  'session_id', (r.data)->'session_id',
  'status', (r.data)->'status',
  'owner', (r.data)->'owner',
  'claim_token', (r.data)->'claim_token',
  'lease_until', (r.data)->'lease_until',
  'epoch_id', (r.data)->'epoch_id',
  'config_hash', (r.data)->'config_hash',
  'plan_fingerprint', (r.data)->'plan_fingerprint',
  'usage', (r.data)->'usage',
  'created_at', (r.data)->'created_at',
  'updated_at', (r.data)->'updated_at',
  'checkpoint_absent', ((r.data)->'checkpoint')='null'::jsonb,
  'error_absent', ((r.data)->'error')='null'::jsonb
) AS current_run,
jsonb_build_object(
  'request', (a.data)->'request',
  'no_effect_v1', (a.data)->'no_effect_v1',
  'run', jsonb_build_object(
  'id', (((a.data)->'outcome')->'run')->'id',
  'session_id', (((a.data)->'outcome')->'run')->'session_id',
  'status', (((a.data)->'outcome')->'run')->'status',
  'owner', (((a.data)->'outcome')->'run')->'owner',
  'claim_token', (((a.data)->'outcome')->'run')->'claim_token',
  'lease_until', (((a.data)->'outcome')->'run')->'lease_until',
  'epoch_id', (((a.data)->'outcome')->'run')->'epoch_id',
  'config_hash', (((a.data)->'outcome')->'run')->'config_hash',
  'plan_fingerprint', (((a.data)->'outcome')->'run')->'plan_fingerprint',
  'usage', (((a.data)->'outcome')->'run')->'usage',
  'created_at', (((a.data)->'outcome')->'run')->'created_at',
  'updated_at', (((a.data)->'outcome')->'run')->'updated_at',
  'checkpoint_absent', ((((a.data)->'outcome')->'run')->'checkpoint')='null'::jsonb,
  'error_absent', ((((a.data)->'outcome')->'run')->'error')='null'::jsonb
),
  'event', jsonb_build_object(
  'cursor', (((a.data)->'outcome')->'terminal_event')->'cursor',
  'session_id', (((a.data)->'outcome')->'terminal_event')->'session_id',
  'run_id', (((a.data)->'outcome')->'terminal_event')->'run_id',
  'turn_id', (((a.data)->'outcome')->'terminal_event')->'turn_id',
  'kind', (((a.data)->'outcome')->'terminal_event')->'kind',
  'correlation', (((a.data)->'outcome')->'terminal_event')->'correlation',
  'live_only', (((a.data)->'outcome')->'terminal_event')->'live_only',
  'created_at', (((a.data)->'outcome')->'terminal_event')->'created_at',
  'status', ((((a.data)->'outcome')->'terminal_event')->'payload')->'status',
  'usage', ((((a.data)->'outcome')->'terminal_event')->'payload')->'usage',
  'evidence', jsonb_build_object(
  'request', (((((a.data)->'outcome')->'terminal_event')->'payload')->'abandonment_v1')->'request',
  'no_effect_v1', (((((a.data)->'outcome')->'terminal_event')->'payload')->'abandonment_v1')->'no_effect_v1',
  'run', jsonb_build_object(
  'id', ((((((a.data)->'outcome')->'terminal_event')->'payload')->'abandonment_v1')->'run')->'id',
  'session_id', ((((((a.data)->'outcome')->'terminal_event')->'payload')->'abandonment_v1')->'run')->'session_id',
  'status', ((((((a.data)->'outcome')->'terminal_event')->'payload')->'abandonment_v1')->'run')->'status',
  'owner', ((((((a.data)->'outcome')->'terminal_event')->'payload')->'abandonment_v1')->'run')->'owner',
  'claim_token', ((((((a.data)->'outcome')->'terminal_event')->'payload')->'abandonment_v1')->'run')->'claim_token',
  'lease_until', ((((((a.data)->'outcome')->'terminal_event')->'payload')->'abandonment_v1')->'run')->'lease_until',
  'epoch_id', ((((((a.data)->'outcome')->'terminal_event')->'payload')->'abandonment_v1')->'run')->'epoch_id',
  'config_hash', ((((((a.data)->'outcome')->'terminal_event')->'payload')->'abandonment_v1')->'run')->'config_hash',
  'plan_fingerprint', ((((((a.data)->'outcome')->'terminal_event')->'payload')->'abandonment_v1')->'run')->'plan_fingerprint',
  'usage', ((((((a.data)->'outcome')->'terminal_event')->'payload')->'abandonment_v1')->'run')->'usage',
  'created_at', ((((((a.data)->'outcome')->'terminal_event')->'payload')->'abandonment_v1')->'run')->'created_at',
  'updated_at', ((((((a.data)->'outcome')->'terminal_event')->'payload')->'abandonment_v1')->'run')->'updated_at',
  'checkpoint_absent', (((((((a.data)->'outcome')->'terminal_event')->'payload')->'abandonment_v1')->'run')->'checkpoint')='null'::jsonb,
  'error_absent', (((((((a.data)->'outcome')->'terminal_event')->'payload')->'abandonment_v1')->'run')->'error')='null'::jsonb
),
  'interrupted', jsonb_array_length((((((a.data)->'outcome')->'terminal_event')->'payload')->'abandonment_v1')->'interrupted_tools')
)
),
  'interrupted', jsonb_array_length(((a.data)->'outcome')->'interrupted_tools')
) AS commit_record,
jsonb_build_object(
  'cursor', (e.data)->'cursor',
  'session_id', (e.data)->'session_id',
  'run_id', (e.data)->'run_id',
  'turn_id', (e.data)->'turn_id',
  'kind', (e.data)->'kind',
  'correlation', (e.data)->'correlation',
  'live_only', (e.data)->'live_only',
  'created_at', (e.data)->'created_at',
  'status', ((e.data)->'payload')->'status',
  'usage', ((e.data)->'payload')->'usage',
  'evidence', jsonb_build_object(
  'request', (((e.data)->'payload')->'abandonment_v1')->'request',
  'no_effect_v1', (((e.data)->'payload')->'abandonment_v1')->'no_effect_v1',
  'run', jsonb_build_object(
  'id', ((((e.data)->'payload')->'abandonment_v1')->'run')->'id',
  'session_id', ((((e.data)->'payload')->'abandonment_v1')->'run')->'session_id',
  'status', ((((e.data)->'payload')->'abandonment_v1')->'run')->'status',
  'owner', ((((e.data)->'payload')->'abandonment_v1')->'run')->'owner',
  'claim_token', ((((e.data)->'payload')->'abandonment_v1')->'run')->'claim_token',
  'lease_until', ((((e.data)->'payload')->'abandonment_v1')->'run')->'lease_until',
  'epoch_id', ((((e.data)->'payload')->'abandonment_v1')->'run')->'epoch_id',
  'config_hash', ((((e.data)->'payload')->'abandonment_v1')->'run')->'config_hash',
  'plan_fingerprint', ((((e.data)->'payload')->'abandonment_v1')->'run')->'plan_fingerprint',
  'usage', ((((e.data)->'payload')->'abandonment_v1')->'run')->'usage',
  'created_at', ((((e.data)->'payload')->'abandonment_v1')->'run')->'created_at',
  'updated_at', ((((e.data)->'payload')->'abandonment_v1')->'run')->'updated_at',
  'checkpoint_absent', (((((e.data)->'payload')->'abandonment_v1')->'run')->'checkpoint')='null'::jsonb,
  'error_absent', (((((e.data)->'payload')->'abandonment_v1')->'run')->'error')='null'::jsonb
),
  'interrupted', jsonb_array_length((((e.data)->'payload')->'abandonment_v1')->'interrupted_tools')
)
) AS source_event
FROM runs r
JOIN abandonment_commits a ON a.run_id=r.id
LEFT JOIN events e ON e.seq=(
 SELECT seq FROM events e0
 WHERE e0.session_id=r.session_id AND e0.run_id=r.id AND e0.data->>'kind'='run_settled'
 ORDER BY seq DESC LIMIT 1
)
WHERE r.session_id=$1 AND r.id=$2
)
SELECT coherent,committed_seq,source_seq,
CASE WHEN octet_length(current_run::text)<=32768 THEN current_run::text END AS current_run,
CASE WHEN octet_length(commit_record::text)<=32768 THEN commit_record::text END AS commit_record,
CASE WHEN octet_length(source_event::text)<=32768 THEN source_event::text END AS source_event
FROM original;
