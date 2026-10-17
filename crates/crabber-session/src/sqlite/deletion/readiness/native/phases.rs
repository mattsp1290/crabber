//! All historical phases are authenticated before cleanup classification.
use super::{scan, source};
use crate::public_session::v2::proof::Proof;
use crate::sql_deletion::readiness::{Result, blocked, unknown};
use crate::transaction::Transaction;
use crate::{DeletionBlocker, PhaseQuiescence, PhaseState};
use crabber_core::{PhaseId, RunId, SessionId, ToolCallId, ToolCallRecord, ToolCallStatus};
use sqlx::{Row, Sqlite};
pub(in super::super) async fn phases(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<bool> {
    source::membership(tx, session, true).await?;
    let mut after = String::new();
    let mut retained = false;
    loop {
        let runs = super::run_ids(tx, session, &after, proof).await?;
        if runs.is_empty() {
            return Ok(retained);
        }
        for run in runs {
            retained = true;
            phase_run(tx, session, &run, proof).await?;
            call_run(tx, session, &run, proof).await?;
            after = run.0;
        }
    }
}
async fn phase_run(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    proof: &mut Proof,
) -> Result<()> {
    let mut after = String::new();
    loop {
        let ids = scan::page(tx, session, Some(run), &after, scan::Kind::Phase, proof).await?;
        if ids.is_empty() {
            return Ok(());
        }
        for id in ids {
            let id = PhaseId(id);
            let phase = super::super::super::super::phase::load_with_proof(tx, run, &id, proof)
                .await?
                .ok_or_else(|| unknown("missing phase"))?;
            if phase.snapshot.session_id != *session
                || phase.snapshot.run_id != *run
                || phase.snapshot.target.id != id
            {
                return Err(unknown("phase scope"));
            }
            let call = call(tx, run, &phase.snapshot.target.call_id, proof).await?;
            if crate::phase::call_binding(&call).map_err(unknown)? != phase.snapshot.call_binding {
                return Err(unknown("phase call binding"));
            }
            if phase.snapshot.state == PhaseState::Suspended
                && phase.snapshot.quiescence == PhaseQuiescence::Complete
            {
                after = id.0;
                continue;
            }
            let event = source::latest(tx, session, run, &id.0, source::Kind::Phase, proof).await?;
            let receipt = super::super::super::super::phase::receipt_with_proof(
                tx,
                session,
                &source::operation(&event)?,
                proof,
            )
            .await?
            .ok_or_else(|| unknown("missing phase receipt"))?;
            crate::phase::verify_receipt(&receipt, &event).map_err(unknown)?;
            if receipt.phase != phase.snapshot {
                return Err(unknown("phase source"));
            }
            if matches!(
                phase.snapshot.state,
                PhaseState::Completed | PhaseState::Failed
            ) {
                terminal(tx, session, run, &phase, &call, (&receipt, &event), proof).await?;
            }
            if matches!(
                phase.snapshot.state,
                PhaseState::Claimed | PhaseState::Started | PhaseState::Uncertain
            ) || phase.snapshot.quiescence != PhaseQuiescence::Complete
            {
                return Err(blocked(DeletionBlocker::Phase));
            }
            after = id.0;
        }
    }
}
async fn terminal(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    phase: &crate::phase::PhaseRecord,
    call: &ToolCallRecord,
    current: (&crate::PhaseReceipt, &crabber_core::EventRecord),
    proof: &mut Proof,
) -> Result<()> {
    let event = source::latest(
        tx,
        session,
        run,
        &phase.snapshot.target.id.0,
        source::Kind::TerminalPhase,
        proof,
    )
    .await?;
    let receipt = super::super::super::super::phase::receipt_with_proof(
        tx,
        session,
        &source::operation(&event)?,
        proof,
    )
    .await?
    .ok_or_else(|| unknown("missing terminal phase receipt"))?;
    let settled = super::super::super::super::read_proof::event_with_limit(
        tx,
        session,
        receipt.event_cursor,
        true,
        crate::phase::terminal_history::MAX_SETTLED_EVENT_BYTES,
        proof,
    )
    .await?;
    crate::phase::terminal_history::verify_history_scope(
        session,
        run,
        phase,
        call,
        current,
        (&receipt, &event),
        &settled,
    )
    .map_err(unknown)?;
    Ok(())
}
async fn call_run(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    proof: &mut Proof,
) -> Result<()> {
    let mut after = String::new();
    loop {
        let ids = scan::page(tx, session, Some(run), &after, scan::Kind::Call, proof).await?;
        if ids.is_empty() {
            return Ok(());
        }
        for id in ids {
            let value = call(tx, run, &ToolCallId(id.clone()), proof).await?;
            if matches!(
                value.status,
                ToolCallStatus::Pending | ToolCallStatus::Running
            ) {
                return Err(blocked(DeletionBlocker::Phase));
            }
            after = id;
        }
    }
}
async fn call(
    tx: &mut Transaction<'_, Sqlite>,
    run: &RunId,
    id: &ToolCallId,
    proof: &mut Proof,
) -> Result<ToolCallRecord> {
    let remaining = i64::try_from(proof.budget().remaining().min(4_194_304)).map_err(unknown)?;
    let row = sqlx::query(CALL)
        .bind(&run.0)
        .bind(&id.0)
        .bind(remaining)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
        .ok_or_else(|| unknown("missing native call"))?;
    let bytes =
        usize::try_from(row.try_get::<i64, _>("bytes").map_err(unknown)?).map_err(unknown)?;
    proof.charge(bytes)?;
    let record = row
        .try_get::<Option<String>, _>("record")
        .map_err(unknown)?
        .ok_or_else(|| unknown("native call bound"))?;
    let call: ToolCallRecord = serde_json::from_str(&record).map_err(unknown)?;
    if call.id != *id || call.run_id != *run || record.len() != bytes {
        return Err(unknown("native call scope"));
    }
    let accounting = crate::sql_snapshot::call_record(&call).map_err(unknown)?;
    if accounting.record != record {
        return Err(unknown("native call encoding"));
    }
    Ok(call)
}
const CALL: &str = "SELECT CAST(length(CAST(data AS BLOB)) AS BIGINT) AS bytes,CASE WHEN length(CAST(data AS BLOB))<=$3 THEN CASE WHEN json_extract(data,'$.id')=id AND json_extract(data,'$.run_id')=run_id AND json_extract(data,'$.status')=status THEN data END END AS record FROM tool_calls WHERE run_id=$1 AND id=$2";
mod suspended;
pub(in super::super) use suspended::released_suspensions;
