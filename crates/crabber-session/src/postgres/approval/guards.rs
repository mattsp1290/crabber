//! Active barriers constrain native attempts and terminal release in their transaction.
use crate::postgres::transaction::Transaction;
use crate::{ApprovalContinuationState, AttemptError as Error, AttemptMutation};
use crabber_core::Run;
use sqlx::Postgres;

pub(in super::super) async fn guard_attempt(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    request: &AttemptMutation,
) -> Result<(), Error> {
    let Some(record) = active(tx, run).await? else {
        return Ok(());
    };
    if record.state != ApprovalContinuationState::Claimed
        || record.batch.next_attempt != *request.command.target()
    {
        return Err(Error::Busy);
    }
    Ok(())
}
pub(in super::super) async fn release_terminal(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    request: &AttemptMutation,
) -> Result<(), Error> {
    let Some(mut record) = active_mode(tx, run, true).await? else {
        return Ok(());
    };
    if record.state != ApprovalContinuationState::Claimed
        || record.batch.next_attempt != *request.command.target()
        || record.questions.iter().any(|q| !q.view.consumed)
    {
        return Err(Error::CleanupBlocked);
    }
    record.state = ApprovalContinuationState::Released;
    super::write::save(tx, &record)
        .await
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let result = sqlx::query("DELETE FROM continuation_barriers WHERE session_id=$1 AND run_id=$2 AND continuation_id=$3")
        .bind(&run.session_id.0).bind(&run.id.0).bind(&record.batch.continuation.id.0)
        .execute(&mut **tx).await.map_err(super::super::db).map_err(crate::attempt::sql::unknown)?;
    if result.rows_affected() != 1 {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
async fn active(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
) -> Result<Option<crate::approval::ApprovalRecord>, Error> {
    super::current::session(tx, &run.session_id)
        .await
        .map_err(|_| Error::UnknownStoreOutcome)?;
    active_mode(tx, run, false).await
}
async fn active_mode(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    releasing: bool,
) -> Result<Option<crate::approval::ApprovalRecord>, Error> {
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM continuation_barriers WHERE session_id=$1)",
    )
    .bind(&run.session_id.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(super::super::db)
    .map_err(crate::attempt::sql::unknown)?;
    if !exists {
        return Ok(None);
    }
    let (record, _) = super::load_inner(tx, &run.session_id, &run.id, !releasing)
        .await
        .map_err(|_| Error::UnknownStoreOutcome)?
        .ok_or(Error::Busy)?;
    if releasing {
        super::current::verify(tx, run, &record, true)
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    super::write::active(tx, &record)
        .await
        .map_err(|error| match error {
            crate::ApprovalError::GenerationConflict => Error::Busy,
            _ => Error::UnknownStoreOutcome,
        })?;
    Ok(Some(record))
}

pub(in super::super) async fn retirement_context(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
) -> Result<crate::attempt_control::RetirementContext, Error> {
    let Some(record) = active(tx, run).await? else {
        return Ok(crate::attempt_control::RetirementContext::default());
    };
    super::recover::validate_provenance(tx, &record)
        .await
        .map_err(|_| Error::UnknownStoreOutcome)?;
    for prepared in &record.batch.calls {
        let call = super::super::phase::load_call(tx, run, &prepared.target.call_id)
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?;
        prepared
            .verify_call(&call)
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    Ok(crate::attempt_control::RetirementContext {
        source: record.batch.source_turn_id.clone(),
        order: Some(
            record
                .batch
                .calls
                .iter()
                .map(|call| call.target.call_id.clone())
                .collect(),
        ),
    })
}

pub(in super::super) async fn guard_claiming(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    request: &AttemptMutation,
) -> Result<(), Error> {
    let (record, _) = super::load(tx, &run.session_id, &run.id)
        .await
        .map_err(|_| Error::UnknownStoreOutcome)?
        .ok_or(Error::UnknownStoreOutcome)?;
    if record.state != crate::ApprovalContinuationState::Waiting
        || !matches!(&request.command,crate::AttemptCommand::Reserve {target} if *target==record.batch.next_attempt)
    {
        return Err(Error::Busy);
    }
    Ok(())
}
