//! Pure pre-effect Tool/Frontend availability. Never a locator or dispatch right.
use crate::session_owner::{SessionOwnerError as Error, driver::SuccessorExecutionDriverOrigin};
use crabber_core::{InputFingerprint, RunId, SessionContext, SessionId, SessionRevision};
use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub enum PendingToolSource {
    Claim(Box<crate::ToolContinuationClaim>),
    Recovery(Box<crate::ToolContinuationRecovery>),
    FrontendClaim(Box<crate::FrontendContinuationClaim>),
    FrontendRecovery(Box<crate::FrontendContinuationRecovery>),
}
#[derive(Clone, Debug)]
pub struct InspectPendingToolPreparation {
    pub context: SessionContext,
    pub source: PendingToolSource,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingToolPreparationMetadata {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub continuation: crate::ContinuationTarget,
    pub origin: SuccessorExecutionDriverOrigin,
    pub expected_revision: SessionRevision,
    pub semantic_digest: InputFingerprint,
    pub source_digest: InputFingerprint,
}
mod verify;
pub(crate) use verify::*;
mod eligibility;
pub(crate) use eligibility::*;
impl InspectPendingToolPreparation {
    /// Corroborate the entire original source and request. This ACK is no grant.
    /// # Errors
    /// Refuses a changed scope, source, kind, revision or semantic choice.
    pub fn verify_metadata(&self, metadata: &PendingToolPreparationMetadata) -> Result<(), Error> {
        if verify::metadata(self)? != *metadata {
            return Err(Error::SemanticConflict);
        }
        Ok(())
    }
}
