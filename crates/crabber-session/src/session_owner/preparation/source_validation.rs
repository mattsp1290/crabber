use super::{
    ExistingWorkTarget, PreparationKey, PreparationPurpose, PreparationSource, ReservePreparation,
};
use crate::session_owner::SessionOwnerError as Error;
pub(crate) fn validate(
    source: &PreparationSource,
    request: &ReservePreparation,
) -> Result<(), Error> {
    let PreparationPurpose::ExistingRun { run_id, work } = &request.purpose else {
        return Err(Error::InvalidRequest);
    };
    let (key, session, run, target, revision, digest) = match source {
        PreparationSource::ApprovalClaim(claim) => (
            PreparationKey::Approval(claim.id.clone()),
            &claim.session_id,
            &claim.run_id,
            ExistingWorkTarget::ApprovalClaim(claim.continuation.clone()),
            claim.expected_revision,
            claim.fingerprint().map_err(|_| Error::InvalidRequest)?,
        ),
        PreparationSource::ToolClaim(claim) => (
            PreparationKey::ToolContinuation(claim.id.clone()),
            &claim.session_id,
            &claim.run_id,
            ExistingWorkTarget::ToolClaim(claim.continuation.clone()),
            claim.expected_revision,
            claim.fingerprint().map_err(|_| Error::InvalidRequest)?,
        ),
        PreparationSource::ApprovalRecovery(recovery) => (
            PreparationKey::Approval(recovery.id.clone()),
            &recovery.original.session_id,
            &recovery.original.run_id,
            ExistingWorkTarget::ApprovalRecovery {
                continuation: recovery.original.continuation.clone(),
                attempt: recovery.attempt.clone(),
            },
            recovery.expected_revision,
            recovery.fingerprint().map_err(|_| Error::InvalidRequest)?,
        ),
        PreparationSource::ToolRecovery(recovery) => (
            PreparationKey::ToolContinuation(recovery.id.clone()),
            &recovery.original.session_id,
            &recovery.original.run_id,
            ExistingWorkTarget::ToolRecovery {
                continuation: recovery.original.continuation.clone(),
                attempt: recovery.attempt.clone(),
            },
            recovery.expected_revision,
            recovery.fingerprint().map_err(|_| Error::InvalidRequest)?,
        ),
    };
    if key != request.key
        || *session != request.owner.binding.session_id
        || run != run_id
        || target != *work
        || revision != request.expected_revision
        || digest != request.intent
    {
        return Err(Error::InvalidRequest);
    }
    Ok(())
}
