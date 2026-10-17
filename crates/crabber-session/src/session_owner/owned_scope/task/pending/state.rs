use super::super::OwnedScopeActivation;
use super::{Error, PendingSuccessorCompletion, core};
use crate::session_owner::driver::SuccessorExecutionDriverGrant;
/// The actual fresh native grant and its once-bound activation.
#[derive(Clone)]
pub struct BoundSuccessorControl {
    pub(super) grant: SuccessorExecutionDriverGrant,
    pub(super) activation: OwnedScopeActivation,
}
impl BoundSuccessorControl {
    #[must_use]
    pub fn activation(&self) -> &OwnedScopeActivation {
        &self.activation
    }
    #[must_use]
    pub fn grant(&self) -> &SuccessorExecutionDriverGrant {
        &self.grant
    }
}
/// Signals/observes this original successor task, without creating authority.
#[derive(Clone)]
pub struct PendingSuccessorControl(pub(super) core::Control);
impl PendingSuccessorControl {
    /// Stops new calls/dispatch, retaining already admitted Store futures.
    pub fn request_stop(&self) {
        self.0.request_stop();
    }
    /// Same-task completion metadata only.
    /// # Errors
    /// No original acknowledged completion, or a mismatched internal family.
    pub async fn completion(&self) -> Result<PendingSuccessorCompletion, Error> {
        match self.0.completion().await? {
            core::Notice::Successor(value) => Ok(value),
            core::Notice::Initial(_) | core::Notice::Queue(_) => Err(Error::SemanticConflict),
        }
    }
    /// Original actual acknowledged grant, never reconstructed from metadata.
    /// # Errors
    /// The original task ended unbound, or its family is inconsistent.
    pub async fn bound(&self) -> Result<BoundSuccessorControl, Error> {
        match self.0.bound().await? {
            core::Binding::Successor(value) => Ok(*value),
            core::Binding::Initial(_) | core::Binding::Queue(_) => Err(Error::SemanticConflict),
        }
    }
    #[cfg(test)]
    pub(super) fn finished_disposition(&self) -> Option<super::OwnedMainDisposition> {
        self.0.finished_disposition()
    }
}
