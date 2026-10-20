//! Existing-owner cleanup controls do not restore extensions or prepare new work.
use super::Orchestrator;
use crabber_core::{SessionId, SessionOwnerId, SessionOwnerOperationId};
use crabber_session::session_owner::{
    RequestSessionOwnerCancel, SessionOwnerError, SessionOwnerOutcome, SessionOwnerReceipt,
    SessionOwnerSnapshot,
};
impl Orchestrator {
    /// Request cooperative cancellation of one exact durable owner.
    /// # Errors
    /// Refuses stale scope/revision, conflicting original keys or corrupt sources.
    pub async fn request_session_owner_cancel(
        &self,
        request: RequestSessionOwnerCancel,
    ) -> Result<SessionOwnerOutcome, SessionOwnerError> {
        self.store.request_session_owner_cancel(request).await
    }
    /// Read current bounded ownership metadata without dispatch or cleanup authority.
    /// # Errors
    /// Refuses missing owners or invalid immutable current evidence.
    pub async fn session_owner_snapshot(
        &self,
        session: &SessionId,
        owner: &SessionOwnerId,
    ) -> Result<SessionOwnerSnapshot, SessionOwnerError> {
        self.store.get_session_owner(session, owner).await
    }
    /// Original metadata only, including after local extension shutdown.
    /// # Errors
    /// Refuses malformed identifiers or a corrupt original receipt.
    pub async fn lookup_session_owner(
        &self,
        session: &SessionId,
        operation: &SessionOwnerOperationId,
    ) -> Result<Option<SessionOwnerReceipt>, SessionOwnerError> {
        self.store.lookup_session_owner(session, operation).await
    }
}
