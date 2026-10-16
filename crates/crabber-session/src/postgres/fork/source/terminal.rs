use super::{Error, Failure, ForkSession, Proof, ToolCallRecord, decode_event, unknown};
use sqlx::{Postgres, Transaction};
pub(super) async fn terminal(
    tx: &mut Transaction<'_, Postgres>,
    request: &ForkSession,
    call: &ToolCallRecord,
    proof: &mut Proof,
) -> Result<crate::PhaseQuiescence, Failure> {
    let raw = proof
        .read(
            tx,
            PHASE,
            &request.source_session_id.0,
            &call.id.0,
            4_210_000,
        )
        .await?
        .ok_or(Error::Unsupported)?;
    let phase = crate::phase::sql::decode_record(Some(raw)).map_err(unknown)?;
    let mut evidence = Vec::new();
    for query in [CURRENT_EVENT, TERMINAL_EVENT] {
        let raw = proof
            .read(tx, query, &request.source_session_id.0, &call.id.0, 131_072)
            .await?
            .ok_or(Error::UnknownStoreOutcome)?;
        let (seq, value): (i64, serde_json::Value) = serde_json::from_str(&raw).map_err(unknown)?;
        let mut event = decode_event(&serde_json::to_string(&value).map_err(unknown)?)?;
        let cursor = crabber_core::EventCursor(u64::try_from(seq).map_err(unknown)?);
        if event.cursor.is_some_and(|old| old != cursor) {
            return Err(Error::UnknownStoreOutcome.into());
        }
        event.cursor = Some(cursor);
        let id = event
            .correlation
            .as_deref()
            .ok_or(Error::UnknownStoreOutcome)?;
        crate::attempt::validate_identifier(id).map_err(unknown)?;
        let raw = proof
            .read(tx, PHASE_RECEIPT, &request.source_session_id.0, id, 32768)
            .await?
            .ok_or(Error::UnknownStoreOutcome)?;
        let (kind, digest, event_seq, encoded): (String, String, Option<i64>, String) =
            serde_json::from_str(&raw).map_err(unknown)?;
        let saved: crate::PhaseReceipt = serde_json::from_str(&encoded).map_err(unknown)?;
        if kind != "phase"
            || event_seq != Some(seq)
            || digest != saved.semantic_digest.as_str()
            || saved.event_cursor != cursor
        {
            return Err(Error::UnknownStoreOutcome.into());
        }
        evidence.push((saved, event));
    }
    let raw = proof
        .read(
            tx,
            SETTLED,
            &request.source_session_id.0,
            &call.id.0,
            2 * crate::phase::terminal_history::MAX_SETTLED_EVENT_BYTES + 16_384,
        )
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;
    let (seq, value): (i64, serde_json::Value) = serde_json::from_str(&raw).map_err(unknown)?;
    let mut settled = decode_event(&serde_json::to_string(&value).map_err(unknown)?)?;
    let cursor = crabber_core::EventCursor(u64::try_from(seq).map_err(unknown)?);
    if settled.cursor.is_some_and(|old| old != cursor) {
        return Err(Error::UnknownStoreOutcome.into());
    }
    settled.cursor = Some(cursor);
    crate::phase::terminal_history::verify_history_scope(
        &request.source_session_id,
        &call.run_id,
        &phase,
        call,
        (&evidence[0].0, &evidence[0].1),
        (&evidence[1].0, &evidence[1].1),
        &settled,
    )
    .map_err(unknown)
    .map_err(Failure::from)
}
const PHASE: &str = r"SELECT octet_length(CASE WHEN octet_length(p.outcome_json)<=4198400 AND octet_length(p.claim_token)<=512 THEN json_build_object('version',p.outcome_json::json->'version','snapshot',json_build_object('session_id',r.session_id,'run_id',p.run_id,'target',json_build_object('id',p.id,'generation',p.generation,'call_id',p.call_id,'attempt',json_build_object('id',p.attempt_id,'generation',a.generation)),'call_binding',(p.outcome_json::json->>'call_binding'),'state',p.state,'quiescence',p.quiescence_state),'token',p.claim_token,'output',p.outcome_json::json->'output')::text END) AS bytes,CASE WHEN octet_length(CASE WHEN octet_length(p.outcome_json)<=4198400 AND octet_length(p.claim_token)<=512 THEN json_build_object('version',p.outcome_json::json->'version','snapshot',json_build_object('session_id',r.session_id,'run_id',p.run_id,'target',json_build_object('id',p.id,'generation',p.generation,'call_id',p.call_id,'attempt',json_build_object('id',p.attempt_id,'generation',a.generation)),'call_binding',(p.outcome_json::json->>'call_binding'),'state',p.state,'quiescence',p.quiescence_state),'token',p.claim_token,'output',p.outcome_json::json->'output')::text END)<=$3 THEN CASE WHEN octet_length(p.outcome_json)<=4198400 AND octet_length(p.claim_token)<=512 THEN json_build_object('version',p.outcome_json::json->'version','snapshot',json_build_object('session_id',r.session_id,'run_id',p.run_id,'target',json_build_object('id',p.id,'generation',p.generation,'call_id',p.call_id,'attempt',json_build_object('id',p.attempt_id,'generation',a.generation)),'call_binding',(p.outcome_json::json->>'call_binding'),'state',p.state,'quiescence',p.quiescence_state),'token',p.claim_token,'output',p.outcome_json::json->'output')::text END END AS record FROM tool_phases p JOIN execution_attempts a ON a.run_id=p.run_id AND a.id=p.attempt_id JOIN runs r ON r.id=p.run_id WHERE r.session_id=$1 AND p.call_id=$2 ORDER BY p.generation DESC LIMIT 1";

const PHASE_RECEIPT: &str = r"SELECT octet_length(json_build_array(kind,semantic_digest,event_seq,data_json)::text) AS bytes,CASE WHEN octet_length(json_build_array(kind,semantic_digest,event_seq,data_json)::text)<=$3 THEN json_build_array(kind,semantic_digest,event_seq,data_json)::text END AS record FROM operation_receipts WHERE session_id=$1 AND operation_id=$2";

const CURRENT_EVENT: &str = r#"WITH decoded AS (SELECT e.seq,e.data,CASE WHEN e.data->>'encoding'='crabber.event.json-text.v1' THEN CASE WHEN split_part(e.data->>'record', ',"payload":',1) ~ ',"kind":"tool_phase_changed"$' THEN (e.data->>'record')::json ELSE '{}'::json END ELSE e.data::json END AS d FROM events e JOIN tool_calls c ON c.run_id=e.run_id WHERE e.session_id=$1 AND c.id=$2) SELECT octet_length(json_build_array(seq,data)::text) AS bytes,CASE WHEN octet_length(json_build_array(seq,data)::text)<=$3 THEN json_build_array(seq,data)::text END AS record FROM decoded WHERE d->>'kind'='tool_phase_changed' AND d #>> '{payload,phase,target,call_id}'=$2 ORDER BY seq DESC LIMIT 1"#;

const TERMINAL_EVENT: &str = r#"WITH decoded AS (SELECT e.seq,e.data,CASE WHEN e.data->>'encoding'='crabber.event.json-text.v1' THEN CASE WHEN split_part(e.data->>'record', ',"payload":',1) ~ ',"kind":"tool_phase_changed"$' THEN (e.data->>'record')::json ELSE '{}'::json END ELSE e.data::json END AS d FROM events e JOIN tool_calls c ON c.run_id=e.run_id WHERE e.session_id=$1 AND c.id=$2) SELECT octet_length(json_build_array(seq,data)::text) AS bytes,CASE WHEN octet_length(json_build_array(seq,data)::text)<=$3 THEN json_build_array(seq,data)::text END AS record FROM decoded WHERE d->>'kind'='tool_phase_changed' AND d #>> '{payload,phase,target,call_id}'=$2 AND d #>> '{payload,operation}' IN ('complete','resolve_success','resolve_failure') ORDER BY seq DESC LIMIT 1"#;

const SETTLED: &str = r#"WITH terminal AS (WITH decoded AS (SELECT e.seq,e.data,CASE WHEN e.data->>'encoding'='crabber.event.json-text.v1' THEN CASE WHEN split_part(e.data->>'record', ',"payload":',1) ~ ',"kind":"tool_phase_changed"$' THEN (e.data->>'record')::json ELSE '{}'::json END ELSE e.data::json END AS d FROM events e JOIN tool_calls c ON c.run_id=e.run_id WHERE e.session_id=$1 AND c.id=$2) SELECT seq FROM decoded WHERE d->>'kind'='tool_phase_changed' AND d #>> '{payload,phase,target,call_id}'=$2 AND d #>> '{payload,operation}' IN ('complete','resolve_success','resolve_failure') ORDER BY seq DESC LIMIT 1), previous AS (SELECT e.seq,e.data FROM events e JOIN tool_calls c ON c.run_id=e.run_id WHERE e.session_id=$1 AND c.id=$2 AND e.seq<(SELECT seq FROM terminal) ORDER BY e.seq DESC LIMIT 1) SELECT octet_length(json_build_array(seq,data)::text) AS bytes,CASE WHEN octet_length(json_build_array(seq,data)::text)<=$3 THEN json_build_array(seq,data)::text END AS record FROM previous"#;
