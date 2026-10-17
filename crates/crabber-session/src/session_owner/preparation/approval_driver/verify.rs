//! Original metadata checks, independent of current projections.
use super::{InspectPreparedApprovalDriver, PreparedApprovalDriverMetadata};
use crate::session_owner::{
    SessionOwnerError as Error, driver::SuccessorExecutionDriverOrigin as Origin,
};
use crabber_core::InputFingerprint;

pub(crate) fn validate(request: &InspectPreparedApprovalDriver) -> Result<(), Error> {
    request.request.fingerprint()?;
    crate::session_owner::identifier(&request.operation_id.0, 128, false)?;
    InputFingerprint::new(request.semantic_digest.as_str().to_owned())
        .map_err(|_| Error::InvalidRequest)?;
    if !matches!(
        request.origin,
        Origin::ApprovalClaim | Origin::ApprovalRecovery
    ) {
        return Err(Error::Unsupported);
    }
    Ok(())
}
pub(crate) fn metadata(
    request: &InspectPreparedApprovalDriver,
    receipt: crate::ApprovalReceipt,
) -> Result<PreparedApprovalDriverMetadata, Error> {
    let valid = matches!(
        (&receipt.outcome, request.origin),
        (
            crate::ApprovalOutcome::Claimed { .. },
            Origin::ApprovalClaim
        ) | (
            crate::ApprovalOutcome::ReclaimedUnstarted { .. },
            Origin::ApprovalRecovery
        )
    );
    if !valid
        || receipt.operation_id != request.operation_id
        || receipt.semantic_digest != request.semantic_digest
        || &receipt.session_id != request.request.context.session_id()
        || receipt.run_id != request.request.fence.run_id
    {
        return Err(Error::SemanticConflict);
    }
    Ok(PreparedApprovalDriverMetadata {
        claim: request.request.claim.clone(),
        receipt,
    })
}
pub(crate) fn scope(
    receipt: &crate::ApprovalReceipt,
) -> Result<crate::continuation_input::Scope, Error> {
    let (crate::ApprovalOutcome::Claimed { continuation, .. }
    | crate::ApprovalOutcome::ReclaimedUnstarted { continuation, .. }) = &receipt.outcome
    else {
        return Err(Error::SemanticConflict);
    };
    Ok(crate::continuation_input::Scope {
        domain: crate::continuation_input::Domain::Approval,
        session: receipt.session_id.clone(),
        run: receipt.run_id.clone(),
        continuation: continuation.clone(),
    })
}
