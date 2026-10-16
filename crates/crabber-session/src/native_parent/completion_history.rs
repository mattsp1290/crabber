//! Borrowed original sources authenticate one noninitial continuation issuance.
//! This establishes source association only, never resource policy or authority.
use super::{CompletionOperation as Operation, Error, RunIssuance};
use crate::session_owner::{
    SessionOwnerOperation, SessionOwnerTermination,
    audit::{Audit, verify_current},
    preparation::{ExistingWorkTarget, PreparationKey, PreparationPurpose},
};
use crabber_core::{ControlId, EventRecord, InputFingerprint, RunId, SessionId, SessionRevision};

pub(crate) enum Control<'a> {
    Approval(&'a crate::ApprovalReceipt, &'a EventRecord),
    Tool(&'a crate::ToolContinuationReceipt, &'a EventRecord),
}
pub(crate) fn metadata(
    completion: &crate::session_owner::preparation::PreparationCompletion,
) -> Option<(&SessionId, &RunId, crabber_core::EventCursor)> {
    use crate::session_owner::preparation::PreparationCompletion;
    match completion {
        PreparationCompletion::Approval(
            crate::ApprovalClaimOutcome::Applied { receipt, .. }
            | crate::ApprovalClaimOutcome::Replayed(receipt),
        ) => Some((&receipt.session_id, &receipt.run_id, receipt.event_cursor)),
        PreparationCompletion::ToolContinuation(
            crate::ToolContinuationClaimOutcome::Applied { receipt, .. }
            | crate::ToolContinuationClaimOutcome::Replayed(receipt),
        ) => Some((&receipt.session_id, &receipt.run_id, receipt.event_cursor)),
        _ => None,
    }
}
pub(crate) fn control<'a>(
    completion: &'a crate::session_owner::preparation::PreparationCompletion,
    event: &'a EventRecord,
) -> Result<Control<'a>, Error> {
    use crate::session_owner::preparation::PreparationCompletion;
    match completion {
        PreparationCompletion::Approval(
            crate::ApprovalClaimOutcome::Applied { receipt, .. }
            | crate::ApprovalClaimOutcome::Replayed(receipt),
        ) => Ok(Control::Approval(receipt, event)),
        PreparationCompletion::ToolContinuation(
            crate::ToolContinuationClaimOutcome::Applied { receipt, .. }
            | crate::ToolContinuationClaimOutcome::Replayed(receipt),
        ) => Ok(Control::Tool(receipt, event)),
        _ => Err(Error),
    }
}
struct Facts<'a> {
    session: &'a SessionId,
    run: &'a RunId,
    id: &'a ControlId,
    digest: &'a InputFingerprint,
    revision: SessionRevision,
    operation: Operation,
    continuation: &'a crate::ContinuationTarget,
    attempt: &'a crate::AttemptSnapshot,
}
impl Control<'_> {
    fn facts(&self) -> Result<Facts<'_>, Error> {
        let (session, run, id, digest, revision, operation, continuation, attempt) = match self {
            Self::Approval(receipt, event) => {
                bounded(*receipt, event)?;
                crate::approval::verify_receipt(receipt, event).map_err(|_| Error)?;
                let (operation, continuation, attempt) = match &receipt.outcome {
                    crate::ApprovalOutcome::Claimed {
                        continuation,
                        attempt,
                    } => (Operation::ApprovalClaim, continuation, attempt),
                    crate::ApprovalOutcome::ReclaimedUnstarted {
                        continuation,
                        attempt,
                        ..
                    } => (Operation::ApprovalRecovery, continuation, attempt),
                    _ => return Err(Error),
                };
                (
                    &receipt.session_id,
                    &receipt.run_id,
                    &receipt.operation_id,
                    &receipt.semantic_digest,
                    receipt.revision,
                    operation,
                    continuation,
                    attempt,
                )
            }
            Self::Tool(receipt, event) => {
                bounded(*receipt, event)?;
                crate::tool_continuation::verify_receipt(receipt, event).map_err(|_| Error)?;
                let (operation, continuation, attempt) = match &receipt.outcome {
                    crate::ToolContinuationOutcome::Claimed {
                        continuation,
                        attempt,
                    } => (Operation::ToolClaim, continuation, attempt),
                    crate::ToolContinuationOutcome::FrontendClaimed {
                        continuation,
                        attempt,
                    } => (Operation::FrontendClaim, continuation, attempt),
                    crate::ToolContinuationOutcome::ReclaimedUnstarted {
                        continuation,
                        attempt,
                        ..
                    } => (Operation::ToolRecovery, continuation, attempt),
                    crate::ToolContinuationOutcome::FrontendReclaimedUnstarted {
                        continuation,
                        attempt,
                        ..
                    } => (Operation::FrontendRecovery, continuation, attempt),
                    _ => return Err(Error),
                };
                (
                    &receipt.session_id,
                    &receipt.run_id,
                    &receipt.operation_id,
                    &receipt.semantic_digest,
                    receipt.revision,
                    operation,
                    continuation,
                    attempt,
                )
            }
        };
        if attempt.session_id != *session
            || attempt.run_id != *run
            || attempt.state != crate::AttemptState::Unstarted
            || attempt.cleanup != crate::AttemptCleanup::Pending
        {
            return Err(Error);
        }
        Ok(Facts {
            session,
            run,
            id,
            digest,
            revision,
            operation,
            continuation,
            attempt,
        })
    }
}

fn bounded(receipt: &impl serde::Serialize, event: &EventRecord) -> Result<(), Error> {
    if crate::snapshot::encoded_bytes(receipt, 32768).is_none()
        || crate::snapshot::encoded_bytes(event, 65536).is_none()
    {
        return Err(Error);
    }
    Ok(())
}

/// Callers independently select/charge the actual adjacent issuances and original
/// audit/event carriers. Legacy and other native domains return unproven (`false`).
/// A supported source with missing or inconsistent evidence returns an error.
/// Whole-history consumers must additionally require strictly increasing original
/// control cursors across rotating claims; this local association is not an
/// independent chronological-history verdict.
pub(crate) fn verify_control(
    previous: &RunIssuance,
    current: &RunIssuance,
    reserve: &Audit,
    begin: &Audit,
    join: &Audit,
    control: &Control<'_>,
) -> Result<bool, Error> {
    for audit in [reserve, begin, join] {
        if crate::snapshot::encoded_bytes(audit, 32768).is_none() {
            return Err(Error);
        }
    }
    previous.validate()?;
    current.validate()?;
    let Some(source) = &current.completion_source else {
        return Ok(false);
    };
    if matches!(
        source.native_operation,
        Operation::AdmissionRecovery | Operation::ExpiredRunResume
    ) {
        return Ok(false);
    }
    let facts = control.facts()?;
    verify_current(&begin.record, reserve, begin).map_err(|_| Error)?;
    verify_current(&join.record, reserve, join).map_err(|_| Error)?;
    let origin = reserve.record.preparation.as_ref().ok_or(Error)?;
    let ids = crate::session_owner::preparation::identity::ids(&origin.key, facts.session)
        .map_err(|_| Error)?;
    let previous_status = if matches!(
        facts.operation,
        Operation::ApprovalClaim | Operation::ToolClaim | Operation::FrontendClaim
    ) {
        crabber_core::RunStatus::Paused
    } else {
        crabber_core::RunStatus::Running
    };
    let current_status = if facts.operation == Operation::ApprovalClaim {
        crabber_core::RunStatus::Paused
    } else {
        crabber_core::RunStatus::Running
    };
    if current.version != 3
        || previous.state.status != previous_status
        || current.state.status != current_status
        || current.sequence != previous.sequence.checked_add(1).ok_or(Error)?
        || previous.state.session_id != *facts.session
        || current.state.session_id != *facts.session
        || previous.state.run_id != *facts.run
        || current.state.run_id != *facts.run
        || previous.state.token == current.state.token
        || source.native_operation != facts.operation
        || source.native_semantic_digest != *facts.digest
        || source.root_join_id != join.receipt.operation_id
        || source.root_join_digest != join.receipt.semantic_digest
        || reserve.receipt.operation_id != ids.reserve
        || begin.receipt.operation_id != ids.begin
        || join.receipt.operation_id != ids.join
        || begin.receipt.operation != SessionOwnerOperation::Begin
        || join.receipt.operation != SessionOwnerOperation::Join
        || join.receipt.owner.termination != Some(SessionOwnerTermination::Returned)
        || begin.record.snapshot.target != join.record.snapshot.target
        || origin.intent != *facts.digest
        || !matches!(join.publication.map(|p|p.after), Some(crate::session_owner::audit::RevisionState::Present(revision)) if facts.revision > revision)
        || !matches!(&origin.purpose, PreparationPurpose::ExistingRun { run_id, work }
            if run_id == facts.run && matches_work(work, &facts))
        || origin.key
            != match facts.operation {
                Operation::ApprovalClaim | Operation::ApprovalRecovery => {
                    PreparationKey::Approval(facts.id.clone())
                }
                _ => PreparationKey::ToolContinuation(facts.id.clone()),
            }
    {
        return Err(Error);
    }
    Ok(true)
}
fn matches_work(work: &ExistingWorkTarget, facts: &Facts<'_>) -> bool {
    match (work, facts.operation) {
        (ExistingWorkTarget::ApprovalClaim(target), Operation::ApprovalClaim)
        | (ExistingWorkTarget::ToolClaim(target), Operation::ToolClaim)
        | (ExistingWorkTarget::FrontendClaim(target), Operation::FrontendClaim) => {
            target == facts.continuation
        }
        (
            ExistingWorkTarget::ApprovalRecovery {
                continuation,
                attempt,
            },
            Operation::ApprovalRecovery,
        )
        | (
            ExistingWorkTarget::ToolRecovery {
                continuation,
                attempt,
            },
            Operation::ToolRecovery,
        )
        | (
            ExistingWorkTarget::FrontendRecovery {
                continuation,
                attempt,
            },
            Operation::FrontendRecovery,
        ) => continuation == facts.continuation && attempt == &facts.attempt.target,
        _ => false,
    }
}
