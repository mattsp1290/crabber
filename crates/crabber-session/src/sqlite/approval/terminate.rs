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
        let digest = request.fingerprint()?;
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
            if let Some(saved) = write::replay(&mut tx, &run, &request.id, &digest).await? {
                request.verify_receipt(&saved)?;
                return Ok((saved, false));
            }
            if write::revision(&mut tx, &run.session_id).await? != request.expected_revision {
                return Err(Error::RevisionConflict);
            }
            let (mut record, _) = load(&mut tx, &run.session_id, &run.id)
                .await?
                .ok_or(Error::InvalidTransition)?;
            write::active(&mut tx, &record).await?;
            let now = self.clock.now();
            validate_provenance(&mut tx, &record).await?;
            validate_attempt(&mut tx, &run, &record).await?;
            terminal_phases::validate(&mut tx, &run, request.expected_revision).await?;
            super::suspend::validate_calls(&mut tx, &run, &record.batch).await?;
            let outcome = record.terminate(request, &run, now)?;
            materialize_calls(&mut tx, &run, &record, request, now).await?;
            if let Some(event) =
                request.turn_event(&run, record.batch.source_turn_id.clone(), now)?
            {
                super::super::records::insert_event(&mut tx, &run, &event)
                    .await
                    .map_err(sql::unknown)?;
            }
            let event = request.run_event(&run, record.batch.source_turn_id.clone(), now)?;
            super::super::records::insert_event(&mut tx, &run, &event)
                .await
                .map_err(sql::unknown)?;
            let mut terminal = run.clone();
            request.apply_run(&mut terminal, now)?;
            super::super::records::save_run(&mut tx, &terminal)
                .await
                .map_err(sql::unknown)?;
            write::save(&mut tx, &record).await?;
            release_barrier(&mut tx, &record).await?;
            let saved =
                write::publish(&mut tx, &run, request.id.clone(), digest, outcome, now).await?;
            request.verify_receipt(&saved)?;
            sql::before_commit(fault)?;
            Ok((saved, true))
        }
        .await;
        sql::after_commit(finish(tx, result).await, fault)
    }
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
