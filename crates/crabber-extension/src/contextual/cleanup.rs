use crate::{CleanupOwner, CleanupTracker, ExtensionError, ToolResourceOwner};

pub(super) enum RecordCleanup {
    Shared(CleanupOwner),
    Factory(ToolResourceOwner),
}

impl RecordCleanup {
    pub(super) fn tracker(&self) -> CleanupTracker {
        match self {
            Self::Shared(owner) => owner.tracker(),
            Self::Factory(owner) => owner.cleanup_tracker(),
        }
    }

    pub(super) fn close(&self) {
        match self {
            Self::Shared(owner) => owner.close(),
            Self::Factory(owner) => owner.close(),
        }
    }

    pub(super) async fn wait_drained(&self) -> Result<(), ExtensionError> {
        match self {
            Self::Shared(owner) => owner.wait_drained().await,
            Self::Factory(owner) => {
                let report = owner.join().await;
                if !report.sealed || report.pending != 0 || !report.failures.is_empty() {
                    return Err(ExtensionError::ContextCleanupIncomplete);
                }
            }
        }
        Ok(())
    }
}
