//! Native errors retain their domain when a preparation commits atomically.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum PreparationCommitError {
    #[error("store: {0}")]
    Store(#[from] crate::StoreError),
    #[error("session ownership: {0}")]
    Owner(#[from] crate::session_owner::SessionOwnerError),
    #[error("approval: {0}")]
    Approval(#[from] crate::ApprovalError),
    #[error("tool continuation: {0}")]
    ToolContinuation(#[from] crate::ToolContinuationError),
    #[error("admission recovery: {0}")]
    Admission(#[from] crate::AdmissionExecutionError),
}
