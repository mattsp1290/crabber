//! Pure current compound carriers. Original payload provenance is checked separately.
use crate::session_owner::SessionOwnerError as Error;
use crate::{AttemptCleanup, AttemptState, AttemptTarget, PhaseQuiescence, PhaseState};
use crabber_core::{Run, ToolCallRecord, ToolCallStatus};

pub(crate) fn parent(
    run: &Run,
    record: &crate::attempt::AttemptRecord,
    target: &AttemptTarget,
    current: bool,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    record.validate().map_err(unknown)?;
    if record.snapshot.session_id != run.session_id
        || record.snapshot.run_id != run.id
        || record.snapshot.target != *target
        || record.snapshot.state
            != if current {
                AttemptState::Unstarted
            } else {
                AttemptState::Suspended
            }
        || record.snapshot.cleanup
            != if current {
                AttemptCleanup::Pending
            } else {
                AttemptCleanup::Complete
            }
        || (current && (record.run_token != run.claim_token || record.lease_until <= now))
    {
        return Err(Error::StaleOwner);
    }
    Ok(())
}

pub(crate) fn phase(
    run: &Run,
    record: &crate::phase::PhaseRecord,
    target: &crate::PhaseTarget,
    binding: &crabber_core::InputFingerprint,
    current: bool,
) -> Result<(), Error> {
    record.validate().map_err(unknown)?;
    if record.snapshot.session_id != run.session_id
        || record.snapshot.run_id != run.id
        || record.snapshot.target != *target
        || record.snapshot.call_binding != *binding
        || record.snapshot.state
            != if current {
                PhaseState::Claimed
            } else {
                PhaseState::Suspended
            }
        || record.snapshot.quiescence
            != if current {
                PhaseQuiescence::Pending
            } else {
                PhaseQuiescence::Complete
            }
        || record.output.is_some()
    {
        return Err(Error::StaleOwner);
    }
    Ok(())
}

pub(crate) fn call(
    run: &Run,
    call: &ToolCallRecord,
    frozen: &crate::PreparedApproval,
) -> Result<(), Error> {
    if call.run_id != run.id
        || call.id != frozen.target.call_id
        || call.status != ToolCallStatus::Pending
        || call.result.is_some()
        || crate::phase::call_binding(call).map_err(unknown)? != frozen.call_binding
    {
        return Err(Error::SemanticConflict);
    }
    Ok(())
}
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}

/// Current mutable projections must remain the exact authenticated source family.
pub(crate) fn source(
    record: &crate::tool_continuation::ToolContinuationRecord,
    receipt: &crate::ToolContinuationReceipt,
) -> Result<(), Error> {
    use crate::ToolContinuationOutcome as Outcome;
    if record.session_id != receipt.session_id
        || record.run_id != receipt.run_id
        || record.latest_claim_source().map_err(unknown)?
            != (
                receipt.operation_id.clone(),
                receipt.semantic_digest.clone(),
            )
    {
        return Err(Error::StaleOwner);
    }
    let family = match &receipt.outcome {
        Outcome::Claimed { .. } => {
            record.frontend_claim.is_none()
                && record.recovery.is_none()
                && record.frontend_recovery.is_none()
        }
        Outcome::ReclaimedUnstarted { .. } => {
            record.frontend_claim.is_none()
                && record.recovery.is_some()
                && record.frontend_recovery.is_none()
        }
        Outcome::FrontendClaimed { .. } => {
            record.frontend_claim.is_some()
                && record.recovery.is_none()
                && record.frontend_recovery.is_none()
        }
        Outcome::FrontendReclaimedUnstarted { .. } => {
            record.frontend_claim.is_some()
                && record.recovery.is_none()
                && record.frontend_recovery.is_some()
        }
        _ => false,
    };
    if !family {
        return Err(Error::StaleOwner);
    }
    Ok(())
}
/// Exactly the compound checkpoint/result-processing phase set, excluding ordinary calls.
pub(crate) fn expected_phases(
    batch: &crate::ToolContinuationSuspension,
) -> impl Iterator<Item = (&crate::PhaseTarget, &crabber_core::InputFingerprint)> {
    batch
        .phases
        .iter()
        .map(|v| (&v.next, &v.call_binding))
        .chain(batch.frontend.iter().map(|v| (&v.target, &v.call_binding)))
}
