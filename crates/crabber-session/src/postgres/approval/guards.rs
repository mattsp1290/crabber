//! Active barriers constrain native attempts and terminal release in their transaction.
use crate::{ApprovalContinuationState, AttemptError as Error, AttemptMutation};
use crabber_core::Run;
use sqlx::{Postgres, Transaction};

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
    let Some(mut record) = active(tx, run).await? else {
        return Ok(());
    };
    if record.state != ApprovalContinuationState::Claimed
        || record.batch.next_attempt != *request.command.target()
        || record.questions.iter().any(|q| !q.view.consumed)
    {
        return Err(Error::CleanupBlocked);
    }
    record.state = ApprovalContinuationState::Released;
    super::answer::save(tx, &record)
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
    let (record, _) = super::load(tx, &run.session_id, &run.id)
        .await
        .map_err(|_| Error::UnknownStoreOutcome)?
        .ok_or(Error::Busy)?;
    super::answer::active(tx, &record)
        .await
        .map_err(|error| match error {
            crate::ApprovalError::GenerationConflict => Error::Busy,
            _ => Error::UnknownStoreOutcome,
        })?;
    Ok(Some(record))
}
