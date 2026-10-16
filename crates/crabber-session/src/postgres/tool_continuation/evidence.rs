//! Keyset scans validate every native/private row without collecting run history.
use super::{
    Error, Postgres, Run, ToolContinuationRecord, ToolContinuationSuspension, Transaction, read,
    unknown,
};
use crate::{PhaseQuiescence, PhaseState};
use crabber_core::{PhaseId, ToolCallId, ToolCallStatus};
pub(super) async fn suspend(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    batch: &ToolContinuationSuspension,
) -> Result<(), Error> {
    let mut after: Option<String> = None;
    let mut unfinished = 0usize;
    loop {
        let ids:Vec<Option<String>>=sqlx::query_scalar("SELECT CASE WHEN octet_length(id)<=512 THEN id END FROM tool_calls WHERE run_id=$1 AND (CAST($2 AS TEXT) IS NULL OR id COLLATE \"C\">$2) ORDER BY id COLLATE \"C\" LIMIT 16").bind(&run.id.0).bind(&after).fetch_all(&mut **tx).await.map_err(unknown)?;
        if ids.is_empty() {
            break;
        }
        for id in ids {
            let id = id.ok_or(Error::UnknownStoreOutcome)?;
            let call = super::super::phase::load_call(tx, run, &ToolCallId::from(id.clone()))
                .await
                .map_err(unknown)?;
            let latest = super::super::phase::latest_host_phase(tx, &run.id, &call.id)
                .await
                .map_err(unknown)?;
            if matches!(
                call.status,
                ToolCallStatus::Pending | ToolCallStatus::Running | ToolCallStatus::Waiting
            ) {
                unfinished += 1;
                if unfinished > 64 {
                    return Err(Error::InvalidTransition);
                }
                let prepared = batch
                    .calls
                    .iter()
                    .find(|p| p.target.call_id == call.id)
                    .ok_or(Error::InvalidTransition)?;
                prepared
                    .verify_call(&call)
                    .map_err(|_| Error::SemanticConflict)?;
                if call.result.is_some() {
                    return Err(Error::InvalidTransition);
                }
                match (
                    batch.phases.iter().find(|p| p.previous.call_id == call.id),
                    latest,
                ) {
                    (Some(frozen), Some(native))
                        if native.target == frozen.previous
                            && call.status == ToolCallStatus::Running => {}
                    (None, None)
                        if call.status == ToolCallStatus::Pending
                            && prepared.target.generation
                                == crabber_core::PhaseGeneration::INITIAL => {}
                    _ => return Err(Error::CleanupBlocked),
                }
            } else {
                crate::attempt_control::validate_logical(&call).map_err(unknown)?;
            }
            after = Some(id);
        }
    }
    if unfinished != batch.calls.len() {
        return Err(Error::InvalidTransition);
    }
    phases(tx, run, Some(batch), None, None).await
}
pub(super) async fn ready(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    record: &ToolContinuationRecord,
) -> Result<(), Error> {
    let mut after: Option<String> = None;
    let mut unfinished = 0usize;
    loop {
        let ids:Vec<Option<String>>=sqlx::query_scalar("SELECT CASE WHEN octet_length(id)<=512 THEN id END FROM tool_calls WHERE run_id=$1 AND (CAST($2 AS TEXT) IS NULL OR id COLLATE \"C\">$2) ORDER BY id COLLATE \"C\" LIMIT 16").bind(&run.id.0).bind(&after).fetch_all(&mut **tx).await.map_err(unknown)?;
        if ids.is_empty() {
            break;
        }
        for id in ids {
            let id = id.ok_or(Error::UnknownStoreOutcome)?;
            let call = super::super::phase::load_call(tx, run, &ToolCallId::from(id.clone()))
                .await
                .map_err(unknown)?;
            let latest = super::super::phase::latest_host_phase(tx, &run.id, &call.id)
                .await
                .map_err(unknown)?;
            if matches!(
                call.status,
                ToolCallStatus::Pending | ToolCallStatus::Running | ToolCallStatus::Waiting
            ) {
                unfinished += 1;
                if unfinished > 64 {
                    return Err(Error::InvalidTransition);
                }
                let prepared = record
                    .batch
                    .calls
                    .iter()
                    .find(|p| p.target.call_id == call.id)
                    .ok_or(Error::SemanticConflict)?;
                prepared
                    .verify_call(&call)
                    .map_err(|_| Error::SemanticConflict)?;
                if call.result.is_some() {
                    return Err(Error::SemanticConflict);
                }
                match (
                    record
                        .batch
                        .phases
                        .iter()
                        .find(|p| p.previous.call_id == call.id),
                    latest,
                ) {
                    (Some(frozen), Some(native))
                        if native.target == frozen.previous
                            && call.status == ToolCallStatus::Waiting
                            && native.state == PhaseState::Suspended
                            && native.quiescence == PhaseQuiescence::Complete => {}
                    (None, None) if call.status == ToolCallStatus::Pending => (),
                    _ => return Err(Error::CleanupBlocked),
                }
            } else {
                crate::attempt_control::validate_logical(&call).map_err(unknown)?;
            }
            after = Some(id);
        }
    }
    if unfinished != record.batch.calls.len() {
        return Err(Error::SemanticConflict);
    }
    phases(tx, run, None, Some(record), None).await
}
async fn phases(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    suspending: Option<&ToolContinuationSuspension>,
    waiting: Option<&ToolContinuationRecord>,
    claimed: Option<&ToolContinuationRecord>,
) -> Result<(), Error> {
    let mut after: Option<String> = None;
    loop {
        let ids:Vec<Option<String>>=sqlx::query_scalar("SELECT CASE WHEN octet_length(id)<=128 THEN id END FROM tool_phases WHERE run_id=$1 AND (CAST($2 AS TEXT) IS NULL OR id COLLATE \"C\">$2) ORDER BY id COLLATE \"C\" LIMIT 16").bind(&run.id.0).bind(&after).fetch_all(&mut **tx).await.map_err(unknown)?;
        if ids.is_empty() {
            break;
        }
        for id in ids {
            let id = id.ok_or(Error::UnknownStoreOutcome)?;
            let native =
                super::super::phase::load_host_record(tx, &run.id, &PhaseId::from(id.clone()))
                    .await
                    .map_err(unknown)?
                    .ok_or(Error::UnknownStoreOutcome)?;
            native.validate().map_err(unknown)?;
            let call = super::super::phase::load_call(tx, run, &native.snapshot.target.call_id)
                .await
                .map_err(unknown)?;
            if crate::phase::call_binding(&call).map_err(unknown)? != native.snapshot.call_binding
                || native.snapshot.session_id != run.session_id
                || native.snapshot.run_id != run.id
            {
                return Err(Error::UnknownStoreOutcome);
            }
            let selected = suspending.is_some_and(|batch| {
                batch
                    .phases
                    .iter()
                    .any(|p| p.previous == native.snapshot.target)
            }) || claimed.is_some_and(|record| {
                record
                    .batch
                    .phases
                    .iter()
                    .any(|phase| phase.next == native.snapshot.target)
            });
            if !selected {
                if native.snapshot.quiescence != PhaseQuiescence::Complete
                    || matches!(
                        native.snapshot.state,
                        PhaseState::Started | PhaseState::Claimed | PhaseState::Uncertain
                    )
                {
                    return Err(Error::CleanupBlocked);
                }
                if native.snapshot.state == PhaseState::Suspended {
                    let key:Option<String>=sqlx::query_scalar("SELECT CASE WHEN octet_length(continuation_id)<=128 THEN continuation_id END FROM tool_continuation_phases WHERE run_id=$1 AND phase_id=$2").bind(&run.id.0).bind(&id).fetch_optional(&mut **tx).await.map_err(unknown)?.ok_or(Error::UnknownStoreOutcome)?;
                    let key = crabber_core::ContinuationId(key.ok_or(Error::UnknownStoreOutcome)?);
                    let origin = if waiting
                        .or(claimed)
                        .is_some_and(|record| record.batch.continuation.id == key)
                    {
                        waiting
                            .or(claimed)
                            .cloned()
                            .ok_or(Error::UnknownStoreOutcome)?
                    } else {
                        read::load(tx, &run.session_id, &run.id, Some(&key))
                            .await?
                            .ok_or(Error::UnknownStoreOutcome)?
                    };
                    if !origin.batch.phases.iter().any(|p| {
                        p.previous == native.snapshot.target
                            && p.call_binding == native.snapshot.call_binding
                    }) {
                        return Err(Error::UnknownStoreOutcome);
                    }
                } else {
                    super::super::phase::terminal_history::validate(tx, run, &native, &call)
                        .await
                        .map_err(unknown)?;
                }
            }
            after = Some(id);
        }
    }
    Ok(())
}

pub(super) async fn recover(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    record: &ToolContinuationRecord,
) -> Result<(), Error> {
    let mut after: Option<String> = None;
    let mut unfinished = 0usize;
    loop {
        let ids:Vec<Option<String>>=sqlx::query_scalar("SELECT CASE WHEN octet_length(id)<=512 THEN id END FROM tool_calls WHERE run_id=$1 AND (CAST($2 AS TEXT) IS NULL OR id COLLATE \"C\">$2) ORDER BY id COLLATE \"C\" LIMIT 16").bind(&run.id.0).bind(&after).fetch_all(&mut **tx).await.map_err(unknown)?;
        if ids.is_empty() {
            break;
        }
        for id in ids {
            let id = id.ok_or(Error::UnknownStoreOutcome)?;
            let call = super::super::phase::load_call(tx, run, &ToolCallId::from(id.clone()))
                .await
                .map_err(unknown)?;
            if matches!(
                call.status,
                ToolCallStatus::Pending | ToolCallStatus::Running | ToolCallStatus::Waiting
            ) {
                unfinished += 1;
                if unfinished > 64 {
                    return Err(Error::CleanupBlocked);
                }
                let prepared = record
                    .batch
                    .calls
                    .iter()
                    .find(|prepared| prepared.target.call_id == call.id)
                    .ok_or(Error::CleanupBlocked)?;
                prepared
                    .verify_call(&call)
                    .map_err(|_| Error::SemanticConflict)?;
                if call.status != ToolCallStatus::Pending || call.result.is_some() {
                    return Err(Error::CleanupBlocked);
                }
                let latest = super::super::phase::latest_host_phase(tx, &run.id, &call.id)
                    .await
                    .map_err(unknown)?;
                match (
                    record
                        .batch
                        .phases
                        .iter()
                        .find(|phase| phase.next.call_id == call.id),
                    latest,
                ) {
                    (Some(frozen), Some(native))
                        if native.target == frozen.next
                            && native.call_binding == frozen.call_binding
                            && native.state == PhaseState::Claimed
                            && native.quiescence == PhaseQuiescence::Pending => {}
                    (None, None) => (),
                    _ => return Err(Error::CleanupBlocked),
                }
            } else {
                crate::attempt_control::validate_logical(&call).map_err(unknown)?;
            }
            after = Some(id);
        }
    }
    if unfinished != record.batch.calls.len() {
        return Err(Error::CleanupBlocked);
    }
    phases(tx, run, None, None, Some(record)).await
}
