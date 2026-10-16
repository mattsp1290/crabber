//! Metadata controls bypass plan restoration and extension initialization.
use super::Orchestrator;
use crabber_core::{ControlId, SessionId};
use crabber_session::{
    ListSessions, ManageSession, ManagementError, ManagementReceipt, SessionListPage,
    SessionSummary,
};
impl Orchestrator {
    /// Erase a session only after the Store proves that accepted work and
    /// session resources have settled. Does not initialize or stop extensions.
    /// # Errors
    /// Returns typed unsupported, revision, erasure or storage failures.
    pub async fn delete_session(
        &self,
        request: crabber_session::DeleteSession,
    ) -> Result<crabber_session::DeletionOutcome, crabber_session::DeletionError> {
        self.store.delete_session(request).await
    }
    /// Reconcile the original minimal erasure receipt, including after close.
    /// # Errors
    /// Returns typed unsupported or storage-integrity failures.
    pub async fn lookup_session_deletion(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<crabber_session::DeletionReceipt>, crabber_session::DeletionError> {
        self.store.lookup_session_deletion(session, id).await
    }
    /// Read authoritative erasure disposition without restoring a runtime.
    /// # Errors
    /// Returns typed unsupported or storage-integrity failures.
    pub async fn session_disposition(
        &self,
        session: &SessionId,
    ) -> Result<crabber_session::SessionDisposition, crabber_session::DeletionError> {
        self.store.session_disposition(session).await
    }

    /// Mutate bounded session display metadata, without acquiring a worker.
    /// # Errors
    /// Returns typed scope, replay, revision or storage failures.
    pub async fn manage_session(
        &self,
        request: ManageSession,
    ) -> Result<ManagementReceipt, ManagementError> {
        self.store.manage_session(request).await
    }
    /// Reconcile a host control key without initializing extensions.
    /// # Errors
    /// Returns typed unsupported or storage-integrity errors.
    pub async fn lookup_session_management(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<ManagementReceipt>, ManagementError> {
        self.store.lookup_session_management(session, id).await
    }
    /// Read a safe summary without execution records or authority.
    /// # Errors
    /// Returns typed unsupported, missing or storage-integrity errors.
    pub async fn session_summary(
        &self,
        session: &SessionId,
    ) -> Result<SessionSummary, ManagementError> {
        self.store.session_summary(session).await
    }
    /// List bounded workspace-scoped summaries in stable session ID order.
    /// # Errors
    /// Rejects invalid limits, cursor scope or storage failures.
    pub async fn list_sessions(
        &self,
        request: ListSessions,
    ) -> Result<SessionListPage, ManagementError> {
        self.store.list_sessions(request).await
    }
}
