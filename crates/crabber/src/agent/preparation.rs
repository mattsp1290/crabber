//! Metadata remains available after host ingress closes.
use super::Agent;
use crabber_core::SessionId;
use crabber_session::session_owner::{
    SessionOwnerError,
    preparation::{PreparationKey, PreparationReconciliation},
};
impl Agent {
    /// Bounded local retained completion and resource metadata. Empty results
    /// do not certify durable or cross-controller quiescence.
    #[must_use]
    pub fn retained_preparation_status(
        &self,
        session: &SessionId,
    ) -> Vec<crabber_runtime::RetainedPreparationProgress> {
        self.runtime.retained_preparation_status(session)
    }
    /// Retry the exact locally retained joined completion without factories or
    /// business dispatch, including after ordinary ingress closes.
    /// # Errors
    /// Rejects missing local evidence, changed correlation or native refusal.
    pub async fn retry_preparation_completion(
        &self,
        descriptor: &PreparationReconciliation,
    ) -> Result<crabber_runtime::PreparationRetryProgress, crabber_runtime::RuntimeError> {
        self.runtime.retry_preparation_completion(descriptor).await
    }
    /// Signal locally retained scoped cleanup; successful return is a request,
    /// not an actual task/resource Join. Available after ingress closes.
    /// # Errors
    /// Refuses missing local evidence and unsupported legacy cleanup scopes.
    pub async fn cleanup_retained_preparation(
        &self,
        descriptor: &PreparationReconciliation,
    ) -> Result<(), crabber_runtime::RuntimeError> {
        self.runtime.cleanup_retained_preparation(descriptor).await
    }
    /// Read original preparation correlation without factories or authority.
    /// Receiving this descriptor grants no Join or cleanup right.
    /// # Errors
    /// Returns unsupported or immutable source-integrity failures.
    pub async fn lookup_preparation_operation(
        &self,
        session: &SessionId,
        key: &PreparationKey,
    ) -> Result<Option<PreparationReconciliation>, SessionOwnerError> {
        self.runtime
            .lookup_preparation_operation(session, key)
            .await
    }
}
