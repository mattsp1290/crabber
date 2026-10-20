//! Host-supervised progress of an already accepted cancellation.
use super::{PostgresStore, finish, reads, suspension};
use crate::postgres::transaction::Transaction;
use crate::{
    RunControlError as Error, RunControlKind, RunControlStatus, RunControlSummary,
    WaitingCancellation,
    run_control::{unknown, valid_scope, verify_terminal},
};
use sqlx::Postgres;
async fn current(
    tx: &mut Transaction<'_, Postgres>,
    request: &WaitingCancellation,
) -> Result<RunControlSummary, Error> {
    let original = reads::receipt(tx, &request.target.session_id, &request.original_cancel_id)
        .await?
        .ok_or(Error::NotFound)?;
    if original.target != request.target || original.kind != RunControlKind::Cancel {
        return Err(Error::SemanticConflict);
    }
    reads::summaries(tx, &request.target)
        .await?
        .into_iter()
        .find(|s| s.receipt.operation_id == request.original_cancel_id)
        .ok_or(Error::UnknownStoreOutcome)
}
impl PostgresStore {
    pub(in super::super) async fn control_settle_waiting(
        &self,
        request: WaitingCancellation,
    ) -> Result<RunControlSummary, Error> {
        crate::run_control::validate_target(&request.target)?;
        valid_scope(&request.target.session_id, &request.original_cancel_id)?;
        let mut tx = self
            .session_transaction(&request.target.session_id)
            .await
            .map_err(unknown)?;
        let result = async {
            let summary = current(&mut tx, &request).await?;
            reads::verify_public_target(&mut tx, &request.target).await?;
            let status = reads::status(&mut tx, &request.target).await?;
            verify_terminal(&summary, status)?;
            if matches!(summary.status, RunControlStatus::TerminalCommitted { .. }) {
                return Ok((summary, false));
            }
            if reads::revision(&mut tx, &request.target.session_id).await?
                != request.expected_revision
            {
                return Err(Error::RevisionConflict);
            }
            if status != crabber_core::RunStatus::Paused {
                return Err(Error::Busy);
            }
            let run = reads::load_run(&mut tx, &request.target).await?;
            let record = reads::records(&mut tx, &request.target)
                .await?
                .into_iter()
                .find(|r| r.request.id == request.original_cancel_id)
                .ok_or(Error::UnknownStoreOutcome)?;
            apply(
                &mut tx,
                &run,
                &record.request,
                summary.receipt,
                self.clock.now(),
            )
            .await?;
            let summary = current(&mut tx, &request).await?;
            #[cfg(test)]
            {
                crate::run_control_contract::process::pause("waiting_before");
                crate::binding_fault::trip(&self.control_fault, 1).map_err(unknown)?;
            }
            Ok((summary, true))
        }
        .await;
        let result = finish(tx, result).await;
        #[cfg(test)]
        if result.as_ref().is_ok_and(|(_, changed)| *changed) {
            crate::run_control_contract::process::pause("waiting_after");
            crate::binding_fault::trip(&self.control_fault, 2).map_err(unknown)?;
        }
        result.map(|(summary, _)| summary)
    }
}

pub(super) async fn apply(
    tx: &mut Transaction<'_, Postgres>,
    run: &crabber_core::Run,
    request: &crate::RunControlRequest,
    accepted: crate::RunControlReceipt,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    if !matches!(request.action, crate::RunControlAction::Cancel { .. })
        || run.status != crabber_core::RunStatus::Paused
    {
        return Ok(());
    }
    if super::super::tool_continuation::cancel_waiting(tx, run, request, accepted, now)
        .await
        .map_err(|error| match error {
            crate::ToolContinuationError::UnknownStoreOutcome => Error::UnknownStoreOutcome,
            _ => Error::Busy,
        })?
    {
        return Ok(());
    }
    let barrier: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM continuation_barriers WHERE session_id=$1)",
    )
    .bind(&run.session_id.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(unknown)?;
    if barrier {
        super::super::approval::cancel_waiting(tx, run, request, now)
            .await
            .map_err(|error| match error {
                crate::ApprovalError::UnknownStoreOutcome => Error::UnknownStoreOutcome,
                _ => Error::Busy,
            })?;
    } else {
        suspension::cancel(tx, run, request, now).await?;
    }
    Ok(())
}
