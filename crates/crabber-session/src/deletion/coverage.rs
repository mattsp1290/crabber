//! Opaque scope for positive ownership coverage. It is separate from native
//! cleanup/readiness and never follows from an empty owner roster.
use crabber_core::{SessionId, SessionRevision};

// These expectations are temporary integration markers. The guarded erasure
// coordinator removes them when its positive-origin verifier is connected.
#[expect(
    dead_code,
    reason = "positive ownership coverage verifier is being integrated"
)]
pub(crate) struct VerifiedDeletionCoverage {
    session_id: SessionId,
    revision: SessionRevision,
    proof_bytes: usize,
}

#[expect(dead_code, reason = "guarded deletion readiness is being integrated")]
impl VerifiedDeletionCoverage {
    pub(crate) fn session_id(&self) -> &SessionId {
        &self.session_id
    }
    pub(crate) fn revision(&self) -> SessionRevision {
        self.revision
    }
    /// Already charged original coverage carriers. Native readiness must debit
    /// these from the same whole-operation proof allowance before more reads.
    pub(crate) fn proof_bytes(&self) -> usize {
        self.proof_bytes
    }
    /// Only isolated storage-purge tests can assert this fixture scope. There
    /// is no production constructor from caller claims or public summaries.
    #[cfg(test)]
    pub(crate) fn fixture(session_id: SessionId, revision: SessionRevision) -> Self {
        Self {
            session_id,
            revision,
            proof_bytes: 0,
        }
    }
}
