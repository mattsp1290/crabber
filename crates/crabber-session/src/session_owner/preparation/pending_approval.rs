//! Pure pre-effect source evidence. A successful read never authorizes dispatch.
use crate::session_owner::{SessionOwnerError as Error, driver::SuccessorExecutionDriverOrigin};
use crabber_core::{InputFingerprint, RunId, SessionContext, SessionId, SessionRevision};
use serde::Serialize;

/// The complete original native choice; no preparation or native authority.
#[derive(Clone, Debug, Serialize)]
pub enum PendingApprovalSource {
    Claim(Box<crate::ApprovalContinuationClaim>),
    Recovery(Box<crate::ApprovalContinuationRecovery>),
}
/// Scope comes from the caller's authorized context, never inferred from storage.
#[derive(Clone, Debug)]
pub struct InspectPendingApprovalPreparation {
    pub context: SessionContext,
    pub source: PendingApprovalSource,
}
/// Bounded evidence only. Later Reserve/Complete/Register/Activate still revalidate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingApprovalPreparationMetadata {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub continuation: crate::ContinuationTarget,
    pub origin: SuccessorExecutionDriverOrigin,
    pub expected_revision: SessionRevision,
    pub semantic_digest: InputFingerprint,
    pub source_digest: InputFingerprint,
}

mod eligibility;
pub(crate) use eligibility::*;
mod verify;
pub(crate) use verify::*;
impl InspectPendingApprovalPreparation {
    /// Check the entire original source and response correlation before Begin.
    /// This is an ACK check, never a permission or a revision refresh.
    /// # Errors
    /// Refuses any other source, scope, revision, kind or semantic choice.
    pub fn verify_metadata(
        &self,
        metadata: &PendingApprovalPreparationMetadata,
    ) -> Result<(), Error> {
        if verify::metadata(self)? != *metadata {
            return Err(Error::SemanticConflict);
        }
        Ok(())
    }
}
