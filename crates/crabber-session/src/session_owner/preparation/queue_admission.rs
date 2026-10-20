//! Private authority preflight and shared pure original Queue admission rules.
mod verify;
use super::{JoinedPreparation, PreparationCommitError};
use crate::public_session::v2::proof::Proof;
use crate::session_owner::SessionOwnerError as Error;
pub(crate) use verify::{AdmissionSources, prepared, publication_revision, select, verify};
pub(crate) fn original<'a>(
    joined: &'a JoinedPreparation,
    proof: &mut Proof,
) -> Result<(&'a crate::QueueMutation, &'a crate::BoundKeyedAdmitRequest), PreparationCommitError> {
    let grant = joined.observed_grant();
    let root = grant.owner_authority();
    let child = grant
        .retained_resources_authority()
        .ok_or(Error::Unsupported)?;
    // Actual private authority carriers precede hashing or an original-task clone.
    proof
        .measure(&(&root.token, &child.token))
        .map_err(|_| Error::Capacity)?;
    verify::body(joined, proof)
}
