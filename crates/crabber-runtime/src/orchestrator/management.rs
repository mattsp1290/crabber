//! Metadata controls bypass plan restoration and extension initialization.
use super::Orchestrator;
use crabber_core::{ControlId, SessionId};
use crabber_session::{
    ListSessions, ManageSession, ManagementError, ManagementReceipt, SessionListPage,
    SessionSummary,
};
impl Orchestrator {
    /// Copy one closed committed history prefix without acquiring a worker.
    /// # Errors
    /// Rejects unsupported stores, changed source scope/revision or corrupt provenance.
    pub async fn fork_session(
        &self,
        request: crabber_session::ForkSession,
    ) -> Result<crabber_session::ForkOutcome, crabber_session::ForkError> {
        self.store.fork_session(request).await
    }
    /// Original metadata reconciliation, independent of mutable source state.
    /// # Errors
    /// Returns typed unsupported or integrity failures.
    pub async fn lookup_session_fork(
        &self,
        target: &SessionId,
        id: &ControlId,
    ) -> Result<Option<crabber_session::ForkReceipt>, crabber_session::ForkError> {
        self.store.lookup_session_fork(target, id).await
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
