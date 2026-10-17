//! Original metadata checks, independent of current projections.
use super::{InspectPreparedToolDriver, PreparedToolDriverMetadata};
use crate::session_owner::{
    SessionOwnerError as Error, driver::SuccessorExecutionDriverOrigin as Origin,
};
use crabber_core::InputFingerprint;

pub(crate) fn validate(request: &InspectPreparedToolDriver) -> Result<(), Error> {
    request.request.fingerprint()?;
    crate::session_owner::identifier(&request.operation_id.0, 128, false)?;
    InputFingerprint::new(request.semantic_digest.as_str().to_owned())
        .map_err(|_| Error::InvalidRequest)?;
    if !matches!(
        request.origin,
        Origin::ToolClaim | Origin::ToolRecovery | Origin::FrontendClaim | Origin::FrontendRecovery
    ) {
        return Err(Error::Unsupported);
    }
    Ok(())
}
pub(crate) fn metadata(
    request: &InspectPreparedToolDriver,
    receipt: crate::ToolContinuationReceipt,
) -> Result<PreparedToolDriverMetadata, Error> {
    let valid = matches!(
        (&receipt.outcome, request.origin),
        (
            crate::ToolContinuationOutcome::Claimed { .. },
            Origin::ToolClaim
        ) | (
            crate::ToolContinuationOutcome::ReclaimedUnstarted { .. },
            Origin::ToolRecovery
        ) | (
            crate::ToolContinuationOutcome::FrontendClaimed { .. },
            Origin::FrontendClaim
        ) | (
            crate::ToolContinuationOutcome::FrontendReclaimedUnstarted { .. },
            Origin::FrontendRecovery
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
    Ok(PreparedToolDriverMetadata {
        claim: request.request.claim.clone(),
        receipt,
    })
}
