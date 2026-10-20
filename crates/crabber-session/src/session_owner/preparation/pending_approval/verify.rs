use super::{
    InspectPendingApprovalPreparation, PendingApprovalPreparationMetadata, PendingApprovalSource,
};
use crate::session_owner::{SessionOwnerError as Error, driver::SuccessorExecutionDriverOrigin};
use crabber_core::{InputFingerprint, SessionRevision};
use sha2::{Digest, Sha256};

pub(crate) fn claim(
    request: &InspectPendingApprovalPreparation,
) -> &crate::ApprovalContinuationClaim {
    match &request.source {
        PendingApprovalSource::Claim(claim) => claim,
        PendingApprovalSource::Recovery(recovery) => &recovery.original,
    }
}
pub(crate) fn revision(request: &InspectPendingApprovalPreparation) -> SessionRevision {
    match &request.source {
        PendingApprovalSource::Claim(claim) => claim.expected_revision,
        PendingApprovalSource::Recovery(recovery) => recovery.expected_revision,
    }
}
pub(crate) fn metadata(
    request: &InspectPendingApprovalPreparation,
) -> Result<PendingApprovalPreparationMetadata, Error> {
    let claim = claim(request);
    crate::session_owner::identifier(&request.context.session_id().0, 512, false)?;
    if request.context.workspace_id().len() > 512
        || request.context.directory().len() > 16384
        || request.context.workspace_id().contains('\0')
        || request.context.directory().contains('\0')
    {
        return Err(Error::InvalidRequest);
    }
    if request.context.session_id() != &claim.session_id {
        return Err(Error::IdentityMismatch);
    }
    let (semantic_digest, origin) = match &request.source {
        PendingApprovalSource::Claim(claim) => (
            claim.fingerprint().map_err(|_| Error::InvalidRequest)?,
            SuccessorExecutionDriverOrigin::ApprovalClaim,
        ),
        PendingApprovalSource::Recovery(recovery) => (
            recovery.fingerprint().map_err(|_| Error::InvalidRequest)?,
            SuccessorExecutionDriverOrigin::ApprovalRecovery,
        ),
    };
    let source = (
        "crabber.pending-approval.source.v1",
        request.context.session_id(),
        request.context.workspace_id(),
        request.context.directory(),
        &request.source,
    );
    crate::snapshot::encoded_bytes(&source, 32768).ok_or(Error::InvalidRequest)?;
    let bytes = serde_json::to_vec(&source).map_err(|_| Error::InvalidRequest)?;
    Ok(PendingApprovalPreparationMetadata {
        session_id: claim.session_id.clone(),
        run_id: claim.run_id.clone(),
        continuation: claim.continuation.clone(),
        origin,
        expected_revision: revision(request),
        semantic_digest,
        source_digest: InputFingerprint::new(format!("{:x}", Sha256::digest(bytes)))
            .map_err(|_| Error::InvalidRequest)?,
    })
}
