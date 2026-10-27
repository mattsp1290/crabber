//! Public session reads and host-driven polling carry no execution authority.
use super::Orchestrator;
use crabber_session::{
    PublicSessionError, PublicSessionOutcome, PublicSessionPoll, PublicSessionRequest,
    PublicSessionSubscription,
};
impl Orchestrator {
    /// Fixed V2 metadata and profile-bound redacted history; available after close.
    /// # Errors
    /// Returns unsupported, invalid request, or durable proof failures.
    pub async fn public_session_snapshot_v2(
        &self,
        request: crabber_session::PublicSessionRequestV2,
    ) -> Result<crabber_session::PublicSessionOutcomeV2, PublicSessionError> {
        self.store.public_session_snapshot_v2(request).await
    }
    /// Capture a revision-coherent newest replacement window, in display order.
    /// # Errors
    /// Rejects continuations, unsupported stores and inconsistent durable proof.
    pub async fn public_session_newest_snapshot_v2(
        &self,
        request: crabber_session::PublicSessionRequestV2,
    ) -> Result<crabber_session::PublicSessionOutcomeV2, PublicSessionError> {
        self.store.public_session_newest_snapshot_v2(request).await
    }
    /// Poll the fixed V2 aggregate using host-owned state; starts no task.
    /// # Errors
    /// Returns bounded snapshot and integrity failures.
    pub async fn poll_public_session_v2(
        &self,
        subscription: &mut crabber_session::PublicSessionSubscriptionV2,
    ) -> Result<crabber_session::PublicSessionPollV2, PublicSessionError> {
        subscription.poll(self.store.as_ref()).await
    }
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
