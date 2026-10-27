//! Public reads remain available after closing extension ingress.
use super::Agent;
use crabber_session::{
    PublicSessionError, PublicSessionOutcome, PublicSessionPoll, PublicSessionRequest,
    PublicSessionSubscription,
};
impl Agent {
    /// Fixed V2 metadata and profile-bound redacted history; available after close.
    /// # Errors
    /// Returns unsupported, invalid request, or durable proof failures.
    pub async fn public_session_snapshot_v2(
        &self,
        request: crabber_session::PublicSessionRequestV2,
    ) -> Result<crabber_session::PublicSessionOutcomeV2, PublicSessionError> {
        self.runtime.public_session_snapshot_v2(request).await
    }
    /// Capture a revision-coherent newest replacement window, in display order.
    /// # Errors
    /// Rejects continuations, unsupported stores and inconsistent durable proof.
    pub async fn public_session_newest_snapshot_v2(
        &self,
        request: crabber_session::PublicSessionRequestV2,
    ) -> Result<crabber_session::PublicSessionOutcomeV2, PublicSessionError> {
        self.runtime
            .public_session_newest_snapshot_v2(request)
            .await
    }
    /// Poll the fixed V2 aggregate using host-owned state; starts no task.
    /// # Errors
    /// Returns bounded snapshot and integrity failures.
    pub async fn poll_public_session_v2(
        &self,
        subscription: &mut crabber_session::PublicSessionSubscriptionV2,
    ) -> Result<crabber_session::PublicSessionPollV2, PublicSessionError> {
        self.runtime.poll_public_session_v2(subscription).await
    }
    /// Coherent redacted native session/control metadata and bounded history.
    /// # Errors
    /// Returns unsupported, scope, limit or durable integrity failures.
    pub async fn public_session_snapshot(
        &self,
        request: PublicSessionRequest,
    ) -> Result<PublicSessionOutcome, PublicSessionError> {
        self.runtime.public_session_snapshot(request).await
    }
    /// Drive a host-owned durable poller without a background dispatcher.
    /// # Errors
    /// Returns the same read failures before or after host close.
    pub async fn poll_public_session(
        &self,
        subscription: &mut PublicSessionSubscription,
    ) -> Result<PublicSessionPoll, PublicSessionError> {
        self.runtime.poll_public_session(subscription).await
    }
}
