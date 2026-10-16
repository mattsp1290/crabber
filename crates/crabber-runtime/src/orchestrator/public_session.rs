//! Public session reads and host-driven polling carry no execution authority.
use super::Orchestrator;
use crabber_session::{
    PublicSessionError, PublicSessionOutcome, PublicSessionPoll, PublicSessionRequest,
    PublicSessionSubscription,
};
impl Orchestrator {
    /// Capture coherent redacted native metadata and bounded public history.
    /// # Errors
    /// Rejects unsupported stores, invalid requests and inconsistent durable proof.
    pub async fn public_session_snapshot(
        &self,
        request: PublicSessionRequest,
    ) -> Result<PublicSessionOutcome, PublicSessionError> {
        self.store.public_session_snapshot(request).await
    }
    /// Poll durable revisions using host-owned state; no task or provider is started.
    /// # Errors
    /// Returns bounded metadata/proof failures, including after host shutdown.
    pub async fn poll_public_session(
        &self,
        subscription: &mut PublicSessionSubscription,
    ) -> Result<PublicSessionPoll, PublicSessionError> {
        subscription.poll(self.store.as_ref()).await
    }
}
