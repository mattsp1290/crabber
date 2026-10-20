//! Pure source eligibility; storage callers supply authenticated current carriers.
use super::PendingApprovalSource;
use crate::session_owner::SessionOwnerError as Error;
use crate::{ApprovalContinuationState, AttemptCleanup, AttemptState};
use crabber_core::{Run, RunStatus, ToolCallRecord, ToolCallStatus};
pub(crate) fn eligible(
    record: &crate::approval::ApprovalRecord,
    run: &Run,
    source: &PendingApprovalSource,
    original: Option<&crate::ApprovalReceipt>,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    match source {
        PendingApprovalSource::Claim(claim) => {
            if record.state != ApprovalContinuationState::Waiting || run.status != RunStatus::Paused
            {
                return Err(Error::InvalidTransition);
            }
            if record.batch.continuation != claim.continuation
                || record.batch.expected_plan != claim.expected_plan
                || run.plan_fingerprint != claim.expected_plan.as_str()
                || run.usage != record.batch.usage
                || run.checkpoint.as_ref()
                    != Some(
                        &serde_json::json!({"version":4,"approval":record.batch.continuation,"turns_used":record.batch.turns_used}),
                    )
            {
                return Err(Error::SemanticConflict);
            }
            record.grants().map_err(Error::ApprovalSource)?;
        }
        PendingApprovalSource::Recovery(recovery) => {
            record
                .recovery_grants(run, recovery, original.ok_or(Error::NotFound)?, now)
                .map_err(Error::ApprovalSource)?;
        }
    }
    Ok(())
}
pub(crate) fn parent(
    record: &crate::approval::ApprovalRecord,
    run: &Run,
    source: &PendingApprovalSource,
    current: &crate::attempt::AttemptRecord,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    current.validate().map_err(|_| Error::UnknownStoreOutcome)?;
    if current.snapshot.session_id != run.session_id || current.snapshot.run_id != run.id {
        return Err(Error::IdentityMismatch);
    }
    match source {
        PendingApprovalSource::Claim(_) => {
            if current.snapshot.target != record.batch.attempt
                || current.snapshot.state != AttemptState::Suspended
                || current.snapshot.cleanup != AttemptCleanup::Complete
            {
                return Err(Error::Busy);
            }
        }
        PendingApprovalSource::Recovery(recovery) => {
            if current.snapshot.target != recovery.attempt
                || current.snapshot.state != AttemptState::Unstarted
                || current.snapshot.cleanup != AttemptCleanup::Pending
                || current.run_token != run.claim_token
            {
                return Err(Error::StaleOwner);
            }
            if current.lease_until > now {
                return Err(Error::LiveOwner);
            }
        }
    }
    Ok(())
}
pub(crate) fn call(
    record: &crate::approval::ApprovalRecord,
    call: &ToolCallRecord,
) -> Result<(), Error> {
    let frozen = record
        .batch
        .calls
        .iter()
        .find(|f| f.target.call_id == call.id)
        .ok_or(Error::SemanticConflict)?;
    if call.run_id != record.run_id
        || call.status != ToolCallStatus::Pending
        || call.result.is_some()
        || crate::phase::call_binding(call).map_err(|_| Error::UnknownStoreOutcome)?
            != frozen.call_binding
    {
        return Err(Error::SemanticConflict);
    }
    Ok(())
}
