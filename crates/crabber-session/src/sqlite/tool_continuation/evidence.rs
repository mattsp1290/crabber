//! Keyset scans validate every native/private row without collecting run history.
use super::{
    Error, Run, Sqlite, ToolContinuationRecord, ToolContinuationSuspension, Transaction, read,
    unknown,
};
use crate::public_session::v2::proof::Proof;
use crate::{PhaseQuiescence, PhaseState};
use crabber_core::{PhaseId, ToolCallId, ToolCallStatus};
pub(super) async fn suspend(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    batch: &ToolContinuationSuspension,
) -> Result<(), Error> {
    let mut after: Option<String> = None;
    let mut unfinished = 0usize;
    loop {
        let ids:Vec<Option<String>>=sqlx::query_scalar("SELECT CASE WHEN length(CAST(id AS BLOB))<=512 THEN id END FROM tool_calls WHERE run_id=$1 AND (CAST($2 AS TEXT) IS NULL OR id COLLATE BINARY>$2) ORDER BY id COLLATE BINARY LIMIT 16").bind(&run.id.0).bind(&after).fetch_all(&mut **tx).await.map_err(unknown)?;
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
                if !super::super::frontend::pending_call(
                    tx,
                    run,
                    &call,
                    batch,
                    latest.as_ref(),
                    false,
                )
                .await?
                {
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
    phases(tx, run, Some(batch), None, None, &mut Proof::legacy()).await
}
pub(super) async fn ready_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    record: &ToolContinuationRecord,
    proof: &mut Proof,
) -> Result<(), Error> {
    let mut after: Option<String> = None;
    let mut unfinished = 0usize;
    loop {
        super::super::read_proof::charge(tx,sqlx::query_scalar("SELECT CAST(COALESCE(SUM(bytes),0) AS BIGINT) FROM (SELECT length(CAST(id AS BLOB)) AS bytes FROM tool_calls WHERE run_id=$1 AND (CAST($2 AS TEXT) IS NULL OR id COLLATE BINARY>$2) ORDER BY id COLLATE BINARY LIMIT 16) bounded_ids").bind(&run.id.0).bind(&after),proof).await.map_err(unknown)?;
        let ids:Vec<Option<String>>=sqlx::query_scalar("SELECT CASE WHEN length(CAST(id AS BLOB))<=512 THEN id END FROM tool_calls WHERE run_id=$1 AND (CAST($2 AS TEXT) IS NULL OR id COLLATE BINARY>$2) ORDER BY id COLLATE BINARY LIMIT 16").bind(&run.id.0).bind(&after).fetch_all(&mut **tx).await.map_err(unknown)?;
        if ids.is_empty() {
            break;
        }
        for id in ids {
            let id = id.ok_or(Error::UnknownStoreOutcome)?;
            let call = super::super::phase::load_call_with_proof(
                tx,
                run,
                &ToolCallId::from(id.clone()),
                proof,
            )
            .await
            .map_err(unknown)?;
            let latest =
                super::super::phase::latest_host_phase_with_proof(tx, &run.id, &call.id, proof)
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
                if !super::super::frontend::pending_call_with_proof(
                    tx,
                    run,
                    &call,
                    &record.batch,
                    latest.as_ref(),
                    true,
                    proof,
                )
                .await?
                {
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
    phases(tx, run, None, Some(record), None, proof).await
}
async fn phases(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    suspending: Option<&ToolContinuationSuspension>,
    waiting: Option<&ToolContinuationRecord>,
    claimed: Option<&ToolContinuationRecord>,
    proof: &mut Proof,
) -> Result<(), Error> {
    let mut after: Option<String> = None;
    loop {
        super::super::read_proof::charge(tx,sqlx::query_scalar("SELECT CAST(COALESCE(SUM(bytes),0) AS BIGINT) FROM (SELECT length(CAST(id AS BLOB)) AS bytes FROM tool_phases WHERE run_id=$1 AND (CAST($2 AS TEXT) IS NULL OR id COLLATE BINARY>$2) ORDER BY id COLLATE BINARY LIMIT 16) bounded_ids").bind(&run.id.0).bind(&after),proof).await.map_err(unknown)?;
        let ids:Vec<Option<String>>=sqlx::query_scalar("SELECT CASE WHEN length(CAST(id AS BLOB))<=128 THEN id END FROM tool_phases WHERE run_id=$1 AND (CAST($2 AS TEXT) IS NULL OR id COLLATE BINARY>$2) ORDER BY id COLLATE BINARY LIMIT 16").bind(&run.id.0).bind(&after).fetch_all(&mut **tx).await.map_err(unknown)?;
        if ids.is_empty() {
            break;
        }
        for id in ids {
            let id = id.ok_or(Error::UnknownStoreOutcome)?;
            let native = super::super::phase::load_with_proof(
                tx,
                &run.id,
                &PhaseId::from(id.clone()),
                proof,
            )
            .await
            .map_err(unknown)?
            .ok_or(Error::UnknownStoreOutcome)?;
            native.validate().map_err(unknown)?;
            let call = super::super::phase::load_call_with_proof(
                tx,
                run,
                &native.snapshot.target.call_id,
                proof,
            )
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
                    || record
                        .batch
                        .frontend
                        .iter()
                        .any(|call| call.target == native.snapshot.target)
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
                    super::super::read_proof::charge(tx,sqlx::query_scalar("SELECT CAST(length(CAST(continuation_id AS BLOB)) AS BIGINT) FROM tool_continuation_phases WHERE run_id=$1 AND phase_id=$2").bind(&run.id.0).bind(&id),proof).await.map_err(unknown)?;
                    let key:Option<String>=sqlx::query_scalar("SELECT CASE WHEN length(CAST(continuation_id AS BLOB))<=128 THEN continuation_id END FROM tool_continuation_phases WHERE run_id=$1 AND phase_id=$2").bind(&run.id.0).bind(&id).fetch_optional(&mut **tx).await.map_err(unknown)?.ok_or(Error::UnknownStoreOutcome)?;
                    let key = crabber_core::ContinuationId(key.ok_or(Error::UnknownStoreOutcome)?);
                    let origin = if waiting
                        .or(claimed)
                        .is_some_and(|record| record.batch.continuation.id == key)
                    {
                        proof.measure(&waiting.or(claimed)).map_err(unknown)?;
                        waiting
                            .or(claimed)
                            .cloned()
                            .ok_or(Error::UnknownStoreOutcome)?
                    } else {
                        read::load_with_proof(tx, &run.session_id, &run.id, Some(&key), proof)
                            .await
                            .map_err(|e| e.into_domain_or(Error::UnknownStoreOutcome))?
                            .ok_or(Error::UnknownStoreOutcome)?
                    };
                    if !origin.batch.phases.iter().any(|p| {
                        p.previous == native.snapshot.target
                            && p.call_binding == native.snapshot.call_binding
                    }) {
                        return Err(Error::UnknownStoreOutcome);
                    }
                } else {
                    super::super::phase::terminal_history::validate_with_proof(
                        tx, run, &native, &call, proof,
                    )
                    .await
                    .map_err(unknown)?;
                }
            }
            after = Some(id);
        }
    }
    Ok(())
}

pub(super) async fn recover_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    record: &ToolContinuationRecord,
    proof: &mut Proof,
) -> Result<(), Error> {
    let mut after: Option<String> = None;
    let mut unfinished = 0usize;
    loop {
        super::super::read_proof::charge(tx,sqlx::query_scalar("SELECT CAST(COALESCE(SUM(bytes),0) AS BIGINT) FROM (SELECT length(CAST(id AS BLOB)) AS bytes FROM tool_calls WHERE run_id=$1 AND (CAST($2 AS TEXT) IS NULL OR id COLLATE BINARY>$2) ORDER BY id COLLATE BINARY LIMIT 16) bounded_ids").bind(&run.id.0).bind(&after),proof).await.map_err(unknown)?;
        let ids:Vec<Option<String>>=sqlx::query_scalar("SELECT CASE WHEN length(CAST(id AS BLOB))<=512 THEN id END FROM tool_calls WHERE run_id=$1 AND (CAST($2 AS TEXT) IS NULL OR id COLLATE BINARY>$2) ORDER BY id COLLATE BINARY LIMIT 16").bind(&run.id.0).bind(&after).fetch_all(&mut **tx).await.map_err(unknown)?;
        if ids.is_empty() {
            break;
        }
        for id in ids {
            let id = id.ok_or(Error::UnknownStoreOutcome)?;
            let call = super::super::phase::load_call_with_proof(
                tx,
                run,
                &ToolCallId::from(id.clone()),
                proof,
            )
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
                let latest =
                    super::super::phase::latest_host_phase_with_proof(tx, &run.id, &call.id, proof)
                        .await
                        .map_err(unknown)?;
                if !super::super::frontend::recover_call(&call, record, latest.as_ref())? {
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
    phases(tx, run, None, None, Some(record), proof).await
}
