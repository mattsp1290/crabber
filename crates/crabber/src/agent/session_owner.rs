use super::Agent;
use crabber_core::{SessionId, SessionOwnerId, SessionOwnerOperationId};
use crabber_session::session_owner::{
    RequestSessionOwnerCancel, SessionOwnerError, SessionOwnerOutcome, SessionOwnerReceipt,
    SessionOwnerSnapshot,
};
impl Agent {
    /// Request cooperative cancellation of one exact retained owner.
    /// # Errors
    /// Refuses stale revisions/targets and conflicting original operations.
    pub async fn request_session_owner_cancel(
        &self,
        request: RequestSessionOwnerCancel,
    ) -> Result<SessionOwnerOutcome, SessionOwnerError> {
        self.runtime.request_session_owner_cancel(request).await
    }
    /// Bounded current metadata, with no authority to dispatch or claim a join.
    /// # Errors
    /// Refuses missing owners and corrupt current evidence.
    pub async fn session_owner_snapshot(
        &self,
        session: &SessionId,
        owner: &SessionOwnerId,
    ) -> Result<SessionOwnerSnapshot, SessionOwnerError> {
        self.runtime.session_owner_snapshot(session, owner).await
    }
    /// Read an original receipt without restoring extensions.
    /// # Errors
    /// Refuses malformed identifiers and corrupt original evidence.
    pub async fn lookup_session_owner(
        &self,
        session: &SessionId,
        operation: &SessionOwnerOperationId,
    ) -> Result<Option<SessionOwnerReceipt>, SessionOwnerError> {
        self.runtime.lookup_session_owner(session, operation).await
    }
}
