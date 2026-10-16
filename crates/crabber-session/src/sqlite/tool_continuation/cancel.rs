use super::{
    Error, Sqlite, ToolContinuationOutcome, ToolContinuationRecord, ToolContinuationState,
    Transaction, domain, unknown, write,
};
use crabber_core::{InteractionState, Run};
pub(in super::super) async fn cancel_waiting(
    tx: &mut Transaction<'_, Sqlite>,
    run: &Run,
    request: &crate::RunControlRequest,
    accepted: crate::RunControlReceipt,
    now: time::OffsetDateTime,
) -> Result<bool, Error> {
    let Some(mut record) = super::waiting_context(tx, run).await? else {
        return Ok(false);
    };
    let proof = domain::ToolContinuationTermination::new(&record, request.clone(), accepted, now)?;
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM operation_receipts WHERE session_id=$1 AND operation_id=$2)",
    )
    .bind(&run.session_id.0)
    .bind(&proof.id.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(unknown)?;
    if exists {
        return Err(Error::SemanticConflict);
    }
    record.state = ToolContinuationState::Released;
    for question in &mut record.questions {
        if question.answer.is_none() {
            question.view.state = InteractionState::Cancelled;
        }
        question.view.consumed = true;
    }
    record.termination = Some(proof.clone());
    write::save(tx, &record, false).await?;
    write::publish(
        tx,
        run,
        proof.id.clone(),
        proof.fingerprint()?,
        ToolContinuationOutcome::Terminated {
            continuation: record.batch.continuation.clone(),
            original_cancel_id: request.id.clone(),
        },
        now,
    )
    .await?;
    let removed = sqlx::query(
        "DELETE FROM tool_continuation_barriers WHERE session_id=$1 AND continuation_id=$2",
    )
    .bind(&run.session_id.0)
    .bind(&record.batch.continuation.id.0)
    .execute(&mut **tx)
    .await
    .map_err(unknown)?;
    if removed.rows_affected() != 1 {
        return Err(Error::UnknownStoreOutcome);
    }
    let calls = record
        .batch
        .calls
        .iter()
        .map(|p| p.target.call_id.clone())
        .collect();
    super::super::run_control::suspension::apply_verified(
        tx,
        run,
        request,
        calls,
        record.batch.source_turn_id.clone(),
        now,
    )
    .await
    .map_err(unknown)?;
    Ok(true)
}
pub(super) async fn verify(
    tx: &mut Transaction<'_, Sqlite>,
    record: &ToolContinuationRecord,
) -> Result<(), Error> {
    let Some(proof) = &record.termination else {
        return Ok(());
    };
    super::super::run_control::verify_original_cancel(tx, &proof.accepted)
        .await
        .map_err(unknown)?;
    super::super::run_control::verify_cancelled_target(tx, &proof.original.target)
        .await
        .map_err(unknown)?;
    let (latest, _) = super::super::attempt::load_host_attempt(tx, &record.run_id, None, false)
        .await
        .map_err(unknown)?
        .ok_or(Error::UnknownStoreOutcome)?;
    latest.validate().map_err(unknown)?;
    proof.verify_attempt(record, &latest.snapshot)
}
