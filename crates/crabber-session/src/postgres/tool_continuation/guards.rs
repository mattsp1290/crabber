use super::{Error, Postgres, Run, ToolContinuationState, Transaction, unknown, write};
pub(in super::super) async fn guard_attempt(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
    request: &crate::AttemptMutation,
) -> Result<(), crate::AttemptError> {
    let Some(record) = write::active(tx, run)
        .await
        .map_err(|_| crate::AttemptError::UnknownStoreOutcome)?
    else {
        return Ok(());
    };
    if record.state != ToolContinuationState::Claimed
        || record.batch.next_attempt != *request.command.target()
    {
        return Err(crate::AttemptError::Busy);
    }
    Ok(())
}
pub(in super::super) async fn release_terminal(
    tx: &mut Transaction<'_, Postgres>,
    run: &Run,
) -> Result<(), Error> {
    let Some(mut record) = write::active(tx, run).await? else {
        return Ok(());
    };
    if record.state != ToolContinuationState::Claimed
        || record.questions.iter().any(|q| !q.view.consumed)
    {
        return Err(Error::CleanupBlocked);
    }
    record.state = ToolContinuationState::Released;
    write::save(tx, &record, false).await?;
    let result=sqlx::query("DELETE FROM tool_continuation_barriers WHERE session_id=$1 AND run_id=$2 AND continuation_id=$3").bind(&run.session_id.0).bind(&run.id.0).bind(&record.batch.continuation.id.0).execute(&mut **tx).await.map_err(unknown)?;
    if result.rows_affected() != 1 {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
