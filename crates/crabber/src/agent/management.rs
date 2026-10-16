//! Pure host controls do not initialize extension factories.
use super::Agent;
use crabber_core::{ControlId, SessionId};
use crabber_session::{
    ListSessions, ManageSession, ManagementError, ManagementReceipt, SessionListPage,
    SessionSummary,
};
impl Agent {
    /// Erase a session only after the Store proves that accepted work and
    /// session resources have settled. Does not initialize or stop extensions.
    /// # Errors
    /// Returns typed unsupported, revision, erasure or storage failures.
    pub async fn delete_session(
        &self,
        request: crabber_session::DeleteSession,
    ) -> Result<crabber_session::DeletionOutcome, crabber_session::DeletionError> {
        self.runtime.delete_session(request).await
    }
    /// Reconcile the original minimal erasure receipt, including after close.
    /// # Errors
    /// Returns typed unsupported or storage-integrity failures.
    pub async fn lookup_session_deletion(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<crabber_session::DeletionReceipt>, crabber_session::DeletionError> {
        self.runtime.lookup_session_deletion(session, id).await
    }
    /// Read authoritative erasure disposition without restoring a runtime.
    /// # Errors
    /// Returns typed unsupported or storage-integrity failures.
    pub async fn session_disposition(
        &self,
        session: &SessionId,
    ) -> Result<crabber_session::SessionDisposition, crabber_session::DeletionError> {
        self.runtime.session_disposition(session).await
    }

    /// Mutate bounded session metadata without starting provider/tool work.
    /// # Errors
    /// Returns typed scope, replay, revision or storage failures.
    pub async fn manage_session(
        &self,
        request: ManageSession,
    ) -> Result<ManagementReceipt, ManagementError> {
        self.runtime.manage_session(request).await
    }
    /// Reconcile the original management operation, including after close.
    /// # Errors
    /// Returns typed unsupported or storage-integrity errors.
    pub async fn lookup_session_management(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<ManagementReceipt>, ManagementError> {
        self.runtime.lookup_session_management(session, id).await
    }
    /// Read safe summary fields without fences, leases or checkpoints.
    /// # Errors
    /// Returns typed unsupported, missing or storage-integrity errors.
    pub async fn session_summary(
        &self,
        session: &SessionId,
    ) -> Result<SessionSummary, ManagementError> {
        self.runtime.session_summary(session).await
    }
    /// List bounded workspace-scoped summaries in session ID order.
    /// # Errors
    /// Rejects invalid limits, cursor scope or storage failures.
    pub async fn list_sessions(
        &self,
        request: ListSessions,
    ) -> Result<SessionListPage, ManagementError> {
        self.runtime.list_sessions(request).await
    }
}
