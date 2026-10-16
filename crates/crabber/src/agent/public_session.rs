//! Public reads remain available after closing extension ingress.
use super::Agent;
use crabber_session::{
    PublicSessionError, PublicSessionOutcome, PublicSessionPoll, PublicSessionRequest,
    PublicSessionSubscription,
};
impl Agent {
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
