use super::{
    Error, SqliteExecution, ToolContinuationOutcome, ToolContinuationReceipt,
    ToolContinuationRecord, ToolContinuationState, ToolContinuationSuspension,
    ToolSuspensionAuthority, after, before, evidence, fault, finish, revision, unknown, write,
};

use crate::{AttemptMutationOutcome, AttemptState};

use crabber_core::{ContinuationGeneration, RunStatus, SessionId};

const SCOPE: &str = "SELECT CASE WHEN length(CAST(session_id AS BLOB))<=512 THEN session_id END FROM runs WHERE id=$1";
const BARRIER: &str = "INSERT INTO tool_continuation_barriers(session_id,run_id,continuation_id) VALUES($1,$2,$3) ON CONFLICT(session_id) DO UPDATE SET run_id=excluded.run_id,continuation_id=excluded.continuation_id";

impl SqliteExecution {
    pub(in super::super) async fn suspend_tools(
        &self,
        batch: ToolContinuationSuspension,
        authority: ToolSuspensionAuthority,
    ) -> Result<ToolContinuationReceipt, Error> {
        let scope = scope(self).await?;

        let (mut tx, run) = self
            .store
            .tool_transaction(&scope, &self.fence.run_id)
            .await?;

        let fault = fault(&self.store);

        let result = async {
            let digest = batch.fingerprint(&run.session_id, &run.id)?;

            if let Some(saved) = write::replay(&mut tx, &run, &batch.id, &digest).await? {
                return Ok((saved, false));
            }

            if revision(&mut tx, &run.session_id).await? != batch.expected_revision {
                return Err(Error::RevisionConflict);
            }

            let now = self.store.clock.now();
            crate::attempt::verify_run(&run, &self.fence, now).map_err(|_| Error::StaleOwner)?;

            if run.status != RunStatus::Running
                || batch.expected_plan.as_str() != run.plan_fingerprint
            {
                return Err(Error::InvalidTransition);
            }

            let previous = predecessor(&mut tx, &run, &batch).await?;

            evidence::suspend(&mut tx, &run, &batch).await?;

            let parent = parent(&mut tx, &run, &batch, &authority).await?;

            // The pre-effect barrier is consumed before checkpoint handover; its
            // frozen answers/bindings must still match their original receipts.
            super::super::approval::retirement_context(&mut tx, &run)
                .await
                .map_err(unknown)?;

            let mut record = new_record(&mut tx, &run, &batch, now).await?;
            record.handoff = super::super::continuation_handoff::prepare(
                &mut tx,
                &run,
                crate::continuation_handoff::Domain::Tool,
                &batch.continuation,
                &batch.id,
                &digest,
                &batch.attempt,
            )
            .await
            .map_err(unknown)?
            .map(Box::new);

            super::phases::suspend(&mut tx, &run, &parent, &batch, &authority, now).await?;

            let conclusion = batch.conclusion(&run)?;

            if !matches!(
                self.apply_attempt(
                    &mut tx,
                    &run,
                    &conclusion.finish,
                    Some(&authority.attempt),
                    Some(&conclusion)
                )
                .await
                .map_err(unknown)?,
                AttemptMutationOutcome::Applied { .. }
            ) {
                return Err(Error::SemanticConflict);
            }

            if let Some(mut previous) = previous {
                previous.state = ToolContinuationState::Released;
                write::save(&mut tx, &previous, false).await?;
            }

            if let Some(proof) = &record.handoff {
                super::super::continuation_handoff::release(&mut tx, &run, proof)
                    .await
                    .map_err(unknown)?;
            }

            write::save(&mut tx, &record, true).await?;

            sqlx::query(BARRIER)
                .bind(&run.session_id.0)
                .bind(&run.id.0)
                .bind(&batch.continuation.id.0)
                .execute(&mut *tx)
                .await
                .map_err(unknown)?;

            let receipt = write::publish_handoff(
                &mut tx,
                &run,
                batch.id.clone(),
                digest,
                ToolContinuationOutcome::Suspended {
                    continuation: batch.continuation.clone(),
                },
                now,
                record.handoff.as_deref(),
            )
            .await?;

            before(fault)?;
            Ok((receipt, true))
        }
        .await;

        after(finish(tx, result).await, fault)
    }
}

async fn scope(execution: &SqliteExecution) -> Result<SessionId, Error> {
    let scope: Option<String> = sqlx::query_scalar(SCOPE)
        .bind(&execution.fence.run_id.0)
        .fetch_optional(&execution.store.readers)
        .await
        .map_err(unknown)?
        .ok_or(Error::NotFound)?;
    Ok(SessionId(scope.ok_or(Error::UnknownStoreOutcome)?))
}
async fn new_record(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    run: &crabber_core::Run,
    batch: &ToolContinuationSuspension,
    now: time::OffsetDateTime,
) -> Result<ToolContinuationRecord, Error> {
    let binding = super::super::execution_binding::read(tx, &run.session_id, &run.id)
        .await
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    ToolContinuationRecord::new(
        run.session_id.clone(),
        run.id.clone(),
        binding.capsule.request.workspace_id,
        batch.clone(),
        now,
    )
}

async fn predecessor(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    run: &crabber_core::Run,
    batch: &ToolContinuationSuspension,
) -> Result<Option<ToolContinuationRecord>, Error> {
    let previous = write::active(tx, run).await?;

    if let Some(previous) = &previous {
        if previous.state != ToolContinuationState::Claimed
            || previous.batch.next_attempt != batch.attempt
            || previous.batch.continuation.generation.next().ok()
                != Some(batch.continuation.generation)
        {
            return Err(Error::GenerationConflict);
        }
    } else {
        let previous = super::read::load(tx, &run.session_id, &run.id, None).await?;
        let generation = previous
            .map(|r| {
                r.batch
                    .continuation
                    .generation
                    .next()
                    .map_err(|_| Error::GenerationConflict)
            })
            .transpose()?
            .unwrap_or(ContinuationGeneration::INITIAL);
        if batch.continuation.generation != generation {
            return Err(Error::GenerationConflict);
        }
    }

    Ok(previous)
}

async fn parent(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    run: &crabber_core::Run,
    batch: &ToolContinuationSuspension,
    authority: &ToolSuspensionAuthority,
) -> Result<crate::attempt::AttemptRecord, Error> {
    let (parent, _) = super::super::attempt::load_host_attempt(tx, &run.id, None, true)
        .await
        .map_err(unknown)?
        .ok_or(Error::NotFound)?;

    if parent.snapshot.target != batch.attempt || parent.snapshot.state != AttemptState::Started {
        return Err(Error::GenerationConflict);
    }

    if authority.phases.len() != batch.phases.len() {
        return Err(Error::InvalidRequest);
    }

    Ok(parent)
}
