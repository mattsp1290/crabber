//! Original frozen batches are authenticated independently of mutable continuation state.
use crate::ContinuationTarget;
use crabber_core::{InputFingerprint, RunId, SessionId};
pub(crate) fn approval(
    batch: &crate::ApprovalSuspension,
    session: &SessionId,
    run: &RunId,
    target: &ContinuationTarget,
    receipt: &crate::ApprovalReceipt,
) -> Result<InputFingerprint, crate::ApprovalError> {
    use crate::{ApprovalError as Error, ApprovalOutcome};
    if batch.continuation != *target
        || receipt.session_id != *session
        || receipt.run_id != *run
        || receipt.operation_id != batch.id
        || receipt.outcome
            != (ApprovalOutcome::Suspended {
                continuation: target.clone(),
            })
        || batch
            .fingerprint(session, run)
            .map_err(|_| Error::UnknownStoreOutcome)?
            != receipt.semantic_digest
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(batch.expected_plan.clone())
}
pub(crate) fn tool(
    batch: &crate::ToolContinuationSuspension,
    session: &SessionId,
    run: &RunId,
    target: &ContinuationTarget,
    receipt: &crate::ToolContinuationReceipt,
) -> Result<InputFingerprint, crate::ToolContinuationError> {
    use crate::{ToolContinuationError as Error, ToolContinuationOutcome};
    if batch.continuation != *target
        || receipt.session_id != *session
        || receipt.run_id != *run
        || receipt.operation_id != batch.id
        || receipt.outcome
            != (ToolContinuationOutcome::Suspended {
                continuation: target.clone(),
            })
        || batch
            .fingerprint(session, run)
            .map_err(|_| Error::UnknownStoreOutcome)?
            != receipt.semantic_digest
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(batch.expected_plan.clone())
}

pub(crate) fn validate_scope(
    session: &SessionId,
    run: &RunId,
    target: &ContinuationTarget,
) -> Result<(), ()> {
    for (text, max) in [(&session.0, 512), (&run.0, 128), (&target.id.0, 128)] {
        if text.is_empty() || text.len() > max || text.chars().any(char::is_control) {
            return Err(());
        }
    }
    Ok(())
}
