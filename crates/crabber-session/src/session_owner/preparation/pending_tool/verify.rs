use super::{
    InspectPendingToolPreparation as Request, PendingToolPreparationMetadata as Metadata,
    PendingToolSource as Source,
};
use crate::session_owner::{
    SessionOwnerError as Error, driver::SuccessorExecutionDriverOrigin as Origin,
};
use crabber_core::{InputFingerprint, SessionRevision};
use sha2::{Digest, Sha256};
pub(crate) fn claim(request: &Request) -> &crate::ToolContinuationClaim {
    match &request.source {
        Source::Claim(c) => c,
        Source::Recovery(r) => &r.original,
        Source::FrontendClaim(c) => &c.claim,
        Source::FrontendRecovery(r) => &r.original.claim,
    }
}
pub(crate) fn frontend(request: &Request) -> Option<&crate::FrontendContinuationClaim> {
    match &request.source {
        Source::FrontendClaim(c) => Some(c),
        Source::FrontendRecovery(r) => Some(&r.original),
        _ => None,
    }
}
pub(crate) fn recovery(request: &Request) -> Option<&crate::AttemptTarget> {
    match &request.source {
        Source::Recovery(r) => Some(&r.attempt),
        Source::FrontendRecovery(r) => Some(&r.attempt),
        _ => None,
    }
}
pub(crate) fn revision(request: &Request) -> SessionRevision {
    match &request.source {
        Source::Claim(c) => c.expected_revision,
        Source::Recovery(r) => r.expected_revision,
        Source::FrontendClaim(c) => c.claim.expected_revision,
        Source::FrontendRecovery(r) => r.expected_revision,
    }
}
pub(crate) fn metadata(request: &Request) -> Result<Metadata, Error> {
    let c = claim(request);
    crate::session_owner::identifier(&request.context.session_id().0, 512, false)?;
    if request.context.workspace_id().len() > 512
        || request.context.directory().len() > 16384
        || request.context.workspace_id().contains('\0')
        || request.context.directory().contains('\0')
    {
        return Err(Error::InvalidRequest);
    }
    if request.context.session_id() != &c.session_id {
        return Err(Error::IdentityMismatch);
    }
    let (digest, origin) = match &request.source {
        Source::Claim(c) => (c.fingerprint(), Origin::ToolClaim),
        Source::Recovery(r) => (r.fingerprint(), Origin::ToolRecovery),
        Source::FrontendClaim(c) => (c.fingerprint(), Origin::FrontendClaim),
        Source::FrontendRecovery(r) => (r.fingerprint(), Origin::FrontendRecovery),
    };
    let semantic_digest = digest.map_err(|_| Error::InvalidRequest)?;
    let source = (
        "crabber.pending-tool.source.v1",
        request.context.session_id(),
        request.context.workspace_id(),
        request.context.directory(),
        &request.source,
    );
    // Preserve every legal native result set and reserve bounded correlation overhead.
    crate::snapshot::encoded_bytes(&source, (4 << 20) + 32768).ok_or(Error::InvalidRequest)?;
    let bytes = serde_json::to_vec(&source).map_err(|_| Error::InvalidRequest)?;
    Ok(Metadata {
        session_id: c.session_id.clone(),
        run_id: c.run_id.clone(),
        continuation: c.continuation.clone(),
        origin,
        expected_revision: revision(request),
        semantic_digest,
        source_digest: InputFingerprint::new(format!("{:x}", Sha256::digest(bytes)))
            .map_err(|_| Error::InvalidRequest)?,
    })
}

pub(crate) fn metadata_with_proof(
    request: &Request,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<Metadata, Error> {
    let source = (
        "crabber.pending-tool.source.v1",
        request.context.session_id(),
        request.context.workspace_id(),
        request.context.directory(),
        &request.source,
    );
    let bytes =
        crate::snapshot::encoded_bytes(&source, (4 << 20) + 32768).ok_or(Error::InvalidRequest)?;
    proof
        .charge(bytes)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    metadata(request)
}
