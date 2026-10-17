//! Whole-batch suspension and native attempt conclusion share one transaction.
use super::super::{SqliteExecution, db};
use super::{Error, approval, finish, load, sql, write};
use crate::transaction::Transaction;
use crate::{
    ApprovalContinuationState, ApprovalReceipt, ApprovalSuspension, AttemptAuthority,
    AttemptMutationOutcome,
};
use crabber_core::{ContinuationGeneration, Run, RunStatus, SessionId, ToolCallStatus};
use sqlx::Sqlite;

impl SqliteExecution {
    pub(in super::super) async fn suspend_approval(
        &self,
        batch: ApprovalSuspension,
        authority: &AttemptAuthority,
    ) -> Result<ApprovalReceipt, Error> {
        #[cfg(test)]
        let fault = self
            .store
            .approval_fault
            .load(std::sync::atomic::Ordering::SeqCst);
        #[cfg(not(test))]
        let fault = 0;
        let scope: Option<String> = sqlx::query_scalar("SELECT CASE WHEN length(CAST(session_id AS BLOB))<=512 THEN session_id END FROM runs WHERE id=$1")
            .bind(&self.fence.run_id.0).fetch_optional(&self.store.readers).await.map_err(db).map_err(sql::unknown)?
            .ok_or(Error::NotFound)?;
        let scope = SessionId(scope.ok_or(Error::UnknownStoreOutcome)?);
        let (mut tx, run) = self
            .store
            .approval_transaction(&scope, &self.fence.run_id)
            .await?;
        let result = async {
            let digest = batch.fingerprint(&run.session_id, &run.id)?;
            if let Some(saved) = write::replay(&mut tx, &run, &batch.id, &digest).await? { return Ok((saved, false)); }
            let now = self.store.clock.now();
            crate::attempt::verify_run(&run, &self.fence, now).map_err(approval::attempt_error)?;
            if write::revision(&mut tx, &run.session_id).await? != batch.expected_revision { return Err(Error::RevisionConflict); }
            let previous = predecessor(&mut tx, &run, &batch).await?;
            validate_batch(&mut tx, &run, &batch).await?;
            let workspace: Option<String> = sqlx::query_scalar("SELECT CASE WHEN length(CAST(json_extract(data, '$.workspace_id') AS BLOB))<=512 THEN json_extract(data, '$.workspace_id') END FROM sessions WHERE id=$1")
                .bind(&run.session_id.0).fetch_one(&mut *tx).await.map_err(db).map_err(sql::unknown)?;
            let mut record = approval::ApprovalRecord::new(batch.clone(), run.session_id.clone(), run.id.clone(),
                workspace.ok_or(Error::UnknownStoreOutcome)?, now)?;
            record.handoff=super::super::continuation_handoff::prepare(&mut tx,&run,crate::continuation_handoff::Domain::Approval,&batch.continuation,&batch.id,&digest,&batch.attempt).await?.map(Box::new);
            let conclusion = batch.conclusion(&run, now)?;
            let applied = self.apply_attempt(&mut tx, &run, &conclusion.finish, Some(authority), Some(&conclusion))
                .await.map_err(approval::attempt_error)?;
            if !matches!(applied, AttemptMutationOutcome::Applied { .. }) { return Err(Error::SemanticConflict); }
            if let Some(mut previous) = previous {
                previous.state = ApprovalContinuationState::Released;
                write::save(&mut tx, &previous).await?;
            }
            if let Some(proof)=&record.handoff {super::super::continuation_handoff::release(&mut tx,&run,proof).await?;}
            write::insert(&mut tx, &record).await?;
            sqlx::query("INSERT INTO continuation_barriers(session_id,run_id,continuation_id) VALUES($1,$2,$3) ON CONFLICT(session_id) DO UPDATE SET run_id=excluded.run_id,continuation_id=excluded.continuation_id")
                .bind(&run.session_id.0).bind(&run.id.0).bind(&batch.continuation.id.0)
                .execute(&mut *tx).await.map_err(db).map_err(sql::unknown)?;
            let receipt = write::publish_handoff(&mut tx, &run, batch.id.clone(), digest,
                crate::ApprovalOutcome::Suspended { continuation: batch.continuation.clone() }, now, record.handoff.as_deref()).await?;
            super::super::continuation_input::accept(&mut tx, crate::continuation_input::Input::ApprovalSuspension(&batch), crate::continuation_input::Receipt::Approval(&receipt)).await.map_err(|_| Error::UnknownStoreOutcome)?;
            sql::before_commit(fault)?;
            Ok((receipt, true))
        }.await;
        sql::after_commit(finish(tx, result).await, fault)
    }
}
async fn predecessor(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    batch: &ApprovalSuspension,
) -> Result<Option<approval::ApprovalRecord>, Error> {
    let active: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM continuation_barriers WHERE session_id=$1)",
    )
    .bind(&run.session_id.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(db)
    .map_err(sql::unknown)?;
    if !active {
        let previous = load(tx, &run.session_id, &run.id).await?;
        let generation = if let Some((previous, _)) = previous {
            if previous.state != ApprovalContinuationState::Released {
                return Err(Error::UnknownStoreOutcome);
            }
            previous
                .batch
                .continuation
                .generation
                .next()
                .map_err(|_| Error::GenerationConflict)?
        } else {
            ContinuationGeneration::INITIAL
        };
        if batch.continuation.generation != generation {
            return Err(Error::GenerationConflict);
        }
        return Ok(None);
    }
    let (previous, _) = load(tx, &run.session_id, &run.id)
        .await?
        .ok_or(Error::GenerationConflict)?;
    write::active(tx, &previous).await?;
    if previous.state != ApprovalContinuationState::Claimed
        || previous.batch.next_attempt != batch.attempt
        || previous.batch.continuation.generation.next().ok() != Some(batch.continuation.generation)
    {
        return Err(Error::GenerationConflict);
    }
    Ok(Some(previous))
}
async fn validate_batch(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    batch: &ApprovalSuspension,
) -> Result<(), Error> {
    if run.status != RunStatus::Running || batch.expected_plan.as_str() != run.plan_fingerprint {
        return Err(Error::InvalidTransition);
    }
    validate_calls(tx, run, batch).await
}
pub(super) async fn validate_calls(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    batch: &ApprovalSuspension,
) -> Result<(), Error> {
    validate_calls_with_proof(
        tx,
        run,
        batch,
        &mut crate::public_session::v2::proof::Proof::legacy(),
    )
    .await
}
pub(super) async fn validate_calls_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    batch: &ApprovalSuspension,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), Error> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM tool_calls WHERE run_id=$1 AND status IN ('pending','running','waiting')",
    )
    .bind(&run.id.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(db)
    .map_err(sql::unknown)?;
    if usize::try_from(count).ok() != Some(batch.calls.len()) {
        return Err(Error::InvalidTransition);
    }
    for prepared in &batch.calls {
        let call =
            super::super::phase::load_call_with_proof(tx, run, &prepared.target.call_id, proof)
                .await
                .map_err(|error| match error {
                    crate::PhaseError::NotFound => Error::NotFound,
                    _ => Error::UnknownStoreOutcome,
                })?;
        if call.status != ToolCallStatus::Pending || call.result.is_some() {
            return Err(Error::InvalidTransition);
        }
        if crate::phase::call_binding(&call).map_err(|_| Error::UnknownStoreOutcome)?
            != prepared.call_binding
        {
            return Err(Error::SemanticConflict);
        }
        let started: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM tool_phases WHERE run_id=$1 AND call_id=$2)",
        )
        .bind(&run.id.0)
        .bind(&call.id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(db)
        .map_err(sql::unknown)?;
        if started {
            return Err(Error::SemanticConflict);
        }
    }
    Ok(())
}
