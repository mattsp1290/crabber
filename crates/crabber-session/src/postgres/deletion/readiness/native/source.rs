//! Independent latest immutable sources, never a mutable receipt pointer.
use crate::postgres::transaction::Transaction;
use crate::public_session::v2::proof::Proof;
use crate::sql_deletion::readiness::{Result, unknown};
use crabber_core::{ControlId, EventCursor, EventRecord, RunId, SessionId};
use sqlx::Postgres;
#[derive(Clone, Copy)]
pub(super) enum Kind {
    Attempt,
    Phase,
    TerminalPhase,
}
pub(super) fn operation(event: &EventRecord) -> Result<ControlId> {
    let id = event
        .correlation
        .as_deref()
        .ok_or_else(|| unknown("source operation"))?;
    crate::attempt::validate_identifier(id).map_err(unknown)?;
    Ok(ControlId(id.to_owned()))
}
pub(super) async fn latest(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    id: &str,
    kind: Kind,
    proof: &mut Proof,
) -> Result<EventRecord> {
    let filter = match kind {
        Kind::Attempt => {
            "d->>'kind'='execution_attempt_changed' AND d #>> '{payload,attempt,target,id}'=$3"
        }
        Kind::Phase => "d->>'kind'='tool_phase_changed' AND d #>> '{payload,phase,target,id}'=$3",
        Kind::TerminalPhase => {
            "d->>'kind'='tool_phase_changed' AND d #>> '{payload,phase,target,id}'=$3 AND d #>> '{payload,operation}' IN ('complete','resolve_success','resolve_failure')"
        }
    };
    let query = format!(
        "WITH decoded AS (SELECT e.seq,CASE WHEN e.data->>'encoding'='crabber.event.json-text.v1' THEN CASE WHEN split_part(e.data->>'record', ',\"payload\":',1) ~ ',\"kind\":\"(execution_attempt_changed|tool_phase_changed)\"$' THEN (e.data->>'record')::json ELSE '{{}}'::json END ELSE e.data::json END AS d FROM events e WHERE e.session_id=$1 AND e.run_id=$2) SELECT seq FROM decoded WHERE {filter} ORDER BY seq DESC LIMIT 1"
    );
    let seq: Option<i64> = sqlx::query_scalar(&query)
        .bind(&session.0)
        .bind(&run.0)
        .bind(id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?;
    let seq = EventCursor(
        u64::try_from(seq.ok_or_else(|| unknown("immutable native source"))?).map_err(unknown)?,
    );
    let event =
        super::super::super::super::read_proof::event(tx, session, seq, false, proof).await?;
    if event.run_id != *run {
        return Err(unknown("native event run"));
    }
    Ok(event)
}

pub(super) async fn membership(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    phases: bool,
) -> Result<()> {
    let query = if phases {
        PHASE_MEMBERSHIP
    } else {
        ATTEMPT_MEMBERSHIP
    };
    let orphan: bool = sqlx::query_scalar(query)
        .bind(&session.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    if orphan {
        return Err(unknown("native source membership"));
    }
    Ok(())
}
const ATTEMPT_MEMBERSHIP: &str = r#"WITH decoded AS (SELECT e.run_id,CASE WHEN e.data->>'encoding'='crabber.event.json-text.v1' THEN CASE WHEN split_part(e.data->>'record', ',"payload":',1) ~ ',"kind":"(execution_attempt_changed|tool_phase_changed)"$' THEN (e.data->>'record')::json ELSE '{}'::json END ELSE e.data::json END AS d FROM events e WHERE e.session_id=$1) SELECT EXISTS(SELECT 1 FROM decoded WHERE d->>'kind'='execution_attempt_changed' AND NOT EXISTS(SELECT 1 FROM execution_attempts a WHERE a.run_id=decoded.run_id AND a.id=d #>> '{payload,attempt,target,id}'))"#;
const PHASE_MEMBERSHIP: &str = r#"WITH decoded AS (SELECT e.run_id,CASE WHEN e.data->>'encoding'='crabber.event.json-text.v1' THEN CASE WHEN split_part(e.data->>'record', ',"payload":',1) ~ ',"kind":"(execution_attempt_changed|tool_phase_changed)"$' THEN (e.data->>'record')::json ELSE '{}'::json END ELSE e.data::json END AS d FROM events e WHERE e.session_id=$1) SELECT EXISTS(SELECT 1 FROM decoded WHERE d->>'kind'='tool_phase_changed' AND NOT EXISTS(SELECT 1 FROM tool_phases p WHERE p.run_id=decoded.run_id AND p.id=d #>> '{payload,phase,target,id}'))"#;
