-- Fixed narrow projections; checkpoint/error are tested only for absence.
WITH original AS (
SELECT (json_extract(r.data,'$.id')=r.id AND json_extract(r.data,'$.session_id')=r.session_id AND json_extract(r.data,'$.status')=r.status AND json_extract(r.data,'$.claim_token')=r.claim_token) AS coherent,
a.event_seq AS committed_seq, e.seq AS source_seq,
json_object(
  'id', json_extract(r.data,'$.id'),
  'session_id', json_extract(r.data,'$.session_id'),
  'status', json_extract(r.data,'$.status'),
  'owner', json_extract(r.data,'$.owner'),
  'claim_token', json_extract(r.data,'$.claim_token'),
  'lease_until', json_extract(r.data,'$.lease_until'),
  'epoch_id', json_extract(r.data,'$.epoch_id'),
  'config_hash', json_extract(r.data,'$.config_hash'),
  'plan_fingerprint', json_extract(r.data,'$.plan_fingerprint'),
  'usage', json_extract(r.data,'$.usage'),
  'created_at', json_extract(r.data,'$.created_at'),
  'updated_at', json_extract(r.data,'$.updated_at'),
  'checkpoint_absent', json(CASE WHEN json_type(r.data,'$.checkpoint')='null' THEN 'true' ELSE 'false' END),
  'error_absent', json(CASE WHEN json_type(r.data,'$.error')='null' THEN 'true' ELSE 'false' END)
) AS current_run,
json_object(
  'request', json_extract(a.data,'$.request'),
  'no_effect_v1', json_extract(a.data,'$.no_effect_v1'),
  'run', json_object(
  'id', json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.run'),'$.id'),
  'session_id', json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.run'),'$.session_id'),
  'status', json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.run'),'$.status'),
  'owner', json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.run'),'$.owner'),
  'claim_token', json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.run'),'$.claim_token'),
  'lease_until', json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.run'),'$.lease_until'),
  'epoch_id', json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.run'),'$.epoch_id'),
  'config_hash', json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.run'),'$.config_hash'),
  'plan_fingerprint', json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.run'),'$.plan_fingerprint'),
  'usage', json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.run'),'$.usage'),
  'created_at', json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.run'),'$.created_at'),
  'updated_at', json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.run'),'$.updated_at'),
  'checkpoint_absent', json(CASE WHEN json_type(json_extract(json_extract(a.data,'$.outcome'),'$.run'),'$.checkpoint')='null' THEN 'true' ELSE 'false' END),
  'error_absent', json(CASE WHEN json_type(json_extract(json_extract(a.data,'$.outcome'),'$.run'),'$.error')='null' THEN 'true' ELSE 'false' END)
),
  'event', json_object(
  'cursor', json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.terminal_event'),'$.cursor'),
  'session_id', json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.terminal_event'),'$.session_id'),
  'run_id', json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.terminal_event'),'$.run_id'),
  'turn_id', json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.terminal_event'),'$.turn_id'),
  'kind', json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.terminal_event'),'$.kind'),
  'correlation', json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.terminal_event'),'$.correlation'),
  'live_only', json(CASE json_type(json_extract(json_extract(a.data,'$.outcome'),'$.terminal_event'),'$.live_only') WHEN 'true' THEN 'true' WHEN 'false' THEN 'false' ELSE 'null' END),
  'created_at', json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.terminal_event'),'$.created_at'),
  'status', json_extract(json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.terminal_event'),'$.payload'),'$.status'),
  'usage', json_extract(json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.terminal_event'),'$.payload'),'$.usage'),
  'evidence', json_object(
  'request', json_extract(json_extract(json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.terminal_event'),'$.payload'),'$.abandonment_v1'),'$.request'),
  'no_effect_v1', json_extract(json_extract(json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.terminal_event'),'$.payload'),'$.abandonment_v1'),'$.no_effect_v1'),
  'run', json_object(
  'id', json_extract(json_extract(json_extract(json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.terminal_event'),'$.payload'),'$.abandonment_v1'),'$.run'),'$.id'),
  'session_id', json_extract(json_extract(json_extract(json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.terminal_event'),'$.payload'),'$.abandonment_v1'),'$.run'),'$.session_id'),
  'status', json_extract(json_extract(json_extract(json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.terminal_event'),'$.payload'),'$.abandonment_v1'),'$.run'),'$.status'),
  'owner', json_extract(json_extract(json_extract(json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.terminal_event'),'$.payload'),'$.abandonment_v1'),'$.run'),'$.owner'),
  'claim_token', json_extract(json_extract(json_extract(json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.terminal_event'),'$.payload'),'$.abandonment_v1'),'$.run'),'$.claim_token'),
  'lease_until', json_extract(json_extract(json_extract(json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.terminal_event'),'$.payload'),'$.abandonment_v1'),'$.run'),'$.lease_until'),
  'epoch_id', json_extract(json_extract(json_extract(json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.terminal_event'),'$.payload'),'$.abandonment_v1'),'$.run'),'$.epoch_id'),
  'config_hash', json_extract(json_extract(json_extract(json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.terminal_event'),'$.payload'),'$.abandonment_v1'),'$.run'),'$.config_hash'),
  'plan_fingerprint', json_extract(json_extract(json_extract(json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.terminal_event'),'$.payload'),'$.abandonment_v1'),'$.run'),'$.plan_fingerprint'),
  'usage', json_extract(json_extract(json_extract(json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.terminal_event'),'$.payload'),'$.abandonment_v1'),'$.run'),'$.usage'),
  'created_at', json_extract(json_extract(json_extract(json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.terminal_event'),'$.payload'),'$.abandonment_v1'),'$.run'),'$.created_at'),
  'updated_at', json_extract(json_extract(json_extract(json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.terminal_event'),'$.payload'),'$.abandonment_v1'),'$.run'),'$.updated_at'),
  'checkpoint_absent', json(CASE WHEN json_type(json_extract(json_extract(json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.terminal_event'),'$.payload'),'$.abandonment_v1'),'$.run'),'$.checkpoint')='null' THEN 'true' ELSE 'false' END),
  'error_absent', json(CASE WHEN json_type(json_extract(json_extract(json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.terminal_event'),'$.payload'),'$.abandonment_v1'),'$.run'),'$.error')='null' THEN 'true' ELSE 'false' END)
),
  'interrupted', json_array_length(json_extract(json_extract(json_extract(json_extract(json_extract(a.data,'$.outcome'),'$.terminal_event'),'$.payload'),'$.abandonment_v1'),'$.interrupted_tools'))
)
),
  'interrupted', json_array_length(json_extract(json_extract(a.data,'$.outcome'),'$.interrupted_tools'))
) AS commit_record,
json_object(
  'cursor', json_extract(e.data,'$.cursor'),
  'session_id', json_extract(e.data,'$.session_id'),
  'run_id', json_extract(e.data,'$.run_id'),
  'turn_id', json_extract(e.data,'$.turn_id'),
  'kind', json_extract(e.data,'$.kind'),
  'correlation', json_extract(e.data,'$.correlation'),
  'live_only', json(CASE json_type(e.data,'$.live_only') WHEN 'true' THEN 'true' WHEN 'false' THEN 'false' ELSE 'null' END),
  'created_at', json_extract(e.data,'$.created_at'),
  'status', json_extract(json_extract(e.data,'$.payload'),'$.status'),
  'usage', json_extract(json_extract(e.data,'$.payload'),'$.usage'),
  'evidence', json_object(
  'request', json_extract(json_extract(json_extract(e.data,'$.payload'),'$.abandonment_v1'),'$.request'),
  'no_effect_v1', json_extract(json_extract(json_extract(e.data,'$.payload'),'$.abandonment_v1'),'$.no_effect_v1'),
  'run', json_object(
  'id', json_extract(json_extract(json_extract(json_extract(e.data,'$.payload'),'$.abandonment_v1'),'$.run'),'$.id'),
  'session_id', json_extract(json_extract(json_extract(json_extract(e.data,'$.payload'),'$.abandonment_v1'),'$.run'),'$.session_id'),
  'status', json_extract(json_extract(json_extract(json_extract(e.data,'$.payload'),'$.abandonment_v1'),'$.run'),'$.status'),
  'owner', json_extract(json_extract(json_extract(json_extract(e.data,'$.payload'),'$.abandonment_v1'),'$.run'),'$.owner'),
  'claim_token', json_extract(json_extract(json_extract(json_extract(e.data,'$.payload'),'$.abandonment_v1'),'$.run'),'$.claim_token'),
  'lease_until', json_extract(json_extract(json_extract(json_extract(e.data,'$.payload'),'$.abandonment_v1'),'$.run'),'$.lease_until'),
  'epoch_id', json_extract(json_extract(json_extract(json_extract(e.data,'$.payload'),'$.abandonment_v1'),'$.run'),'$.epoch_id'),
  'config_hash', json_extract(json_extract(json_extract(json_extract(e.data,'$.payload'),'$.abandonment_v1'),'$.run'),'$.config_hash'),
  'plan_fingerprint', json_extract(json_extract(json_extract(json_extract(e.data,'$.payload'),'$.abandonment_v1'),'$.run'),'$.plan_fingerprint'),
  'usage', json_extract(json_extract(json_extract(json_extract(e.data,'$.payload'),'$.abandonment_v1'),'$.run'),'$.usage'),
  'created_at', json_extract(json_extract(json_extract(json_extract(e.data,'$.payload'),'$.abandonment_v1'),'$.run'),'$.created_at'),
  'updated_at', json_extract(json_extract(json_extract(json_extract(e.data,'$.payload'),'$.abandonment_v1'),'$.run'),'$.updated_at'),
  'checkpoint_absent', json(CASE WHEN json_type(json_extract(json_extract(json_extract(e.data,'$.payload'),'$.abandonment_v1'),'$.run'),'$.checkpoint')='null' THEN 'true' ELSE 'false' END),
  'error_absent', json(CASE WHEN json_type(json_extract(json_extract(json_extract(e.data,'$.payload'),'$.abandonment_v1'),'$.run'),'$.error')='null' THEN 'true' ELSE 'false' END)
),
  'interrupted', json_array_length(json_extract(json_extract(json_extract(e.data,'$.payload'),'$.abandonment_v1'),'$.interrupted_tools'))
)
) AS source_event
FROM runs r
JOIN abandonment_commits a ON a.run_id=r.id
LEFT JOIN events e ON e.seq=(
 SELECT seq FROM events e0
 WHERE e0.session_id=r.session_id AND e0.run_id=r.id AND json_extract(e0.data,'$.kind')='run_settled'
 ORDER BY seq DESC LIMIT 1
)
WHERE r.session_id=$1 AND r.id=$2
)
SELECT coherent,committed_seq,source_seq,
CASE WHEN length(CAST(current_run AS BLOB))<=32768 THEN current_run END AS current_run,
CASE WHEN length(CAST(commit_record AS BLOB))<=32768 THEN commit_record END AS commit_record,
CASE WHEN length(CAST(source_event AS BLOB))<=32768 THEN source_event END AS source_event
FROM original;
