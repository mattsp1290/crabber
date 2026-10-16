//! Shared finite results for the internal SQL proof pipeline.
use super::VerifiedScope;
use crate::public_session::v2::proof::{Failure as ProofFailure, ReadError};
use crate::{DeletionBlocker, DeletionError};
pub(crate) const MAX_PROOF_BYTES: usize = 32 * 1024 * 1024;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "guarded deletion coordinator consumes these typed results"
    )
)]
pub(crate) enum Readiness {
    Ready(VerifiedScope),
    Blocked(DeletionBlocker),
}
pub(crate) enum Failure {
    Blocked(DeletionBlocker),
    Error(DeletionError),
}
pub(crate) type Result<T> = std::result::Result<T, Failure>;
pub(crate) fn unknown(_: impl std::fmt::Debug) -> Failure {
    Failure::Error(DeletionError::UnknownStoreOutcome)
}
pub(crate) fn blocked(reason: DeletionBlocker) -> Failure {
    Failure::Blocked(reason)
}
impl From<ProofFailure> for Failure {
    fn from(value: ProofFailure) -> Self {
        match value {
            ProofFailure::Limited(_) => Self::Blocked(DeletionBlocker::ProofLimit),
            ProofFailure::Error(_) => unknown("private proof"),
        }
    }
}
impl<E> From<ReadError<E>> for Failure {
    fn from(value: ReadError<E>) -> Self {
        match value {
            ReadError::Domain(_) => unknown("domain proof"),
            ReadError::Budget(error) => error.into(),
        }
    }
}

#[cfg(test)]
pub(crate) mod contract;
