//! Genuine external authorities keep tokens private. This fixture conservatively
//! charges their public constructor maximum; native wrappers measure actual tokens.
#[path = "../../../crabber-session/src/session_owner/preparation/queue_admission/verify.rs"]
mod verify;
use super::{JoinedPreparation, PreparationCommitError};
use crate::public_session::v2::proof::Proof;
use crate::session_owner::SessionOwnerError as Error;
pub(crate) use verify::{AdmissionSources, prepared, publication_revision, select, verify};
pub(crate) fn original<'a>(
    joined: &'a JoinedPreparation,
    proof: &mut Proof,
) -> Result<(&'a crate::QueueMutation, &'a crate::BoundKeyedAdmitRequest), PreparationCommitError> {
    // from_store accepts at most 512 ASCII-graphic bytes per token. JSON can
    // double each byte; include two quoted strings, separator and tuple brackets.
    proof
        .charge(2 * (2 * 512 + 2) + 3)
        .map_err(|_| Error::Capacity)?;
    verify::body(joined, proof)
}
