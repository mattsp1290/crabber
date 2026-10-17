//! Atomically terminate a quiescent pre-effect approval without dispatch authority.
#[path = "terminal_phases.rs"]
mod terminal_phases;
use super::super::SqliteStore;
use super::{Error, approval, finish, load, receipt, sql, write};
use crate::{ApprovalReceipt, ApprovalTermination, AttemptCleanup, AttemptState};
use crabber_core::Run;
use sqlx::{Sqlite, Transaction};

impl SqliteStore {
    pub(in super::super) async fn terminate_approval_inner(
        &self,
        request: &ApprovalTermination,
    ) -> Result<ApprovalReceipt, Error> {
        request.fingerprint()?;
        #[cfg(test)]
        let fault = self
            .approval_fault
            .load(std::sync::atomic::Ordering::SeqCst);
        #[cfg(not(test))]
        let fault = 0;
        let (mut tx, run) = self
            .approval_transaction(&request.session_id, &request.run_id)
            .await?;
        let result = async {
            let result = apply_termination(&mut tx, &run, request, self.clock.now()).await?;
            if result.1 {
                sql::before_commit(fault)?;
            }
            Ok(result)
        }
        .await;
        sql::after_commit(finish(tx, result).await, fault)
    }
}
pub(in super::super) async fn apply_termination(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    request: &ApprovalTermination,
    now: time::OffsetDateTime,
) -> Result<(ApprovalReceipt, bool), Error> {
    let digest = request.fingerprint()?;
    if let Some(saved) = write::replay(tx, run, &request.id, &digest).await? {
        request.verify_receipt(&saved)?;
        return Ok((saved, false));
    }
    if write::revision(tx, &run.session_id).await? != request.expected_revision {
        return Err(Error::RevisionConflict);
    }
    let (mut record, _) = load(tx, &run.session_id, &run.id)
        .await?
        .ok_or(Error::InvalidTransition)?;
    write::active(tx, &record).await?;

    validate_provenance(tx, &record).await?;
    validate_attempt(tx, run, &record).await?;
    terminal_phases::validate(tx, run, request.expected_revision).await?;
    super::suspend::validate_calls(tx, run, &record.batch).await?;
    let outcome = record.terminate(request, run, now)?;
    materialize_calls(tx, run, &record, request, now).await?;
    if let Some(event) = request.turn_event(run, record.batch.source_turn_id.clone(), now)? {
        super::super::records::insert_event(tx, run, &event)
            .await
            .map_err(sql::unknown)?;
    }
    let mut event = request.run_event(run, record.batch.source_turn_id.clone(), now)?;
    event.cursor = Some(
        super::super::records::insert_event_captured(tx, run, &event)
            .await
            .map_err(sql::unknown)?,
    );
    let mut terminal = run.clone();
    request.apply_run(&mut terminal, now)?;
    super::super::run_control::finalize(tx, &terminal, now)
        .await
        .map_err(|_| Error::UnknownStoreOutcome)?;
    super::super::records::save_run(tx, &terminal)
        .await
        .map_err(sql::unknown)?;
    write::save(tx, &record).await?;
    release_barrier(tx, &record).await?;
    let saved = write::publish_terminal(tx, run, request, &event, outcome, now).await?;
    request.verify_receipt(&saved)?;

    Ok((saved, true))
}

async fn validate_provenance(
    tx: &mut Transaction<'_, Sqlite>,
    record: &approval::ApprovalRecord,
) -> Result<(), Error> {
    let saved = receipt(tx, &record.session_id, &record.batch.id)
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;
    record.verify_recovery_batch(&saved)?;
    for question in &record.questions {
        if let Some(answer) = &question.answer {
            let saved = receipt(tx, &record.session_id, &answer.id)
                .await?
                .ok_or(Error::UnknownStoreOutcome)?;
            approval::verify_recovery_answer(answer, &saved)?;
        }
    }
    Ok(())
}
async fn validate_attempt(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    record: &approval::ApprovalRecord,
) -> Result<(), Error> {
    let native = super::super::attempt::load_owned(tx, &run.id, &record.batch.attempt.id)
        .await
        .map_err(approval::attempt_error)?
        .ok_or(Error::NotFound)?;
    if native.snapshot.target != record.batch.attempt
        || native.snapshot.session_id != run.session_id
        || native.snapshot.state != AttemptState::Suspended
        || native.snapshot.cleanup != AttemptCleanup::Complete
    {
        return Err(Error::CleanupBlocked);
    }
    let latest: Option<i64> =
        sqlx::query_scalar("SELECT MAX(generation) FROM execution_attempts WHERE run_id=$1")
            .bind(&run.id.0)
            .fetch_one(&mut **tx)
            .await
            .map_err(super::super::db)
            .map_err(sql::unknown)?;
    if latest
        != Some(
            i64::try_from(record.batch.attempt.generation.get())
                .map_err(|_| Error::GenerationConflict)?,
        )
    {
        return Err(Error::GenerationConflict);
    }
    let unresolved: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tool_phases WHERE run_id=$1 AND (quiescence_state<>'Complete' OR state IN ('Claimed','Started','Uncertain')))")
        .bind(&run.id.0).fetch_one(&mut **tx).await.map_err(super::super::db).map_err(sql::unknown)?;
    if unresolved {
        return Err(Error::CleanupBlocked);
    }
    Ok(())
}
async fn materialize_calls(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    record: &approval::ApprovalRecord,
    request: &ApprovalTermination,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    for prepared in &record.batch.calls {
        let call = super::super::phase::load_call(tx, run, &prepared.target.call_id)
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let records = request.materialize_call(
            run,
            &call,
            &prepared.target,
            now,
            record.batch.source_turn_id.clone(),
        )?;
        super::super::tool_result::apply(tx, run, call, records)
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    Ok(())
}
async fn release_barrier(
    tx: &mut Transaction<'_, Sqlite>,
    record: &approval::ApprovalRecord,
) -> Result<(), Error> {
    let result = sqlx::query("DELETE FROM continuation_barriers WHERE session_id=$1 AND run_id=$2 AND continuation_id=$3")
        .bind(&record.session_id.0).bind(&record.run_id.0).bind(&record.batch.continuation.id.0)
        .execute(&mut **tx).await.map_err(super::super::db).map_err(sql::unknown)?;
    if result.rows_affected() != 1 {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}

pub(in super::super) async fn cancel_waiting(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    request: &crate::RunControlRequest,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    let crate::RunControlAction::Cancel { resolver } = &request.action else {
        return Ok(());
    };
    if run.status != crabber_core::RunStatus::Paused {
        return Ok(());
    }
    let barrier: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM continuation_barriers WHERE session_id=$1)",
    )
    .bind(&run.session_id.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(super::super::db)
    .map_err(sql::unknown)?;
    if !barrier {
        return Ok(());
    }
    let (record, expected_revision) = load(tx, &run.session_id, &run.id)
        .await?
        .ok_or(Error::UnknownStoreOutcome)?;
    let termination = ApprovalTermination {
        id: crate::approval::derived_id("run-control-cancel", &request.id),
        session_id: run.session_id.clone(),
        run_id: run.id.clone(),
        continuation: record.batch.continuation,
        expected_revision,
        resolver: resolver.clone(),
        kind: crate::ApprovalTerminationKind::Cancel,
    };
    apply_termination(tx, run, &termination, now).await?;
    Ok(())
}

pub(in super::super) async fn validate_terminal_phases(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    revision: crabber_core::SessionRevision,
) -> Result<(), Error> {
    terminal_phases::validate(tx, run, revision).await
}

pub(in super::super) async fn validate_terminal_phases_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    revision: crabber_core::SessionRevision,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), Error> {
    terminal_phases::validate_with_proof(tx, run, revision, proof).await
}
