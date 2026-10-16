use crate::{CleanupOwner, CleanupTracker, ExtensionError, ToolResourceOwner};

pub(super) enum RecordCleanup {
    Shared(CleanupOwner),
    Factory(FactoryCleanup),
}

impl RecordCleanup {
    pub(super) fn tracker(&self) -> CleanupTracker {
        match self {
            Self::Shared(owner) => owner.tracker(),
            Self::Factory(factory) => factory.owner.cleanup_tracker(),
        }
    }

    pub(super) fn close(&self) {
        match self {
            Self::Shared(owner) => owner.close(),
            Self::Factory(factory) => factory.owner.close(),
        }
    }

    pub(super) async fn wait_drained(&self) -> Result<(), ExtensionError> {
        match self {
            Self::Shared(owner) => owner.wait_drained().await,
            Self::Factory(factory) => {
                factory.owner.close();
                let driver = factory.driver.join().await;
                let report = factory.owner.snapshot();
                if !driver.sealed
                    || driver.joined != 1
                    || driver.pending != 0
                    || !driver.failures.is_empty()
                    || !report.sealed
                    || report.pending != 0
                    || !report.failures.is_empty()
                {
                    return Err(ExtensionError::ContextCleanupIncomplete);
                }
            }
        }
        Ok(())
    }
}

pub(super) struct FactoryCleanup {
    owner: ToolResourceOwner,
    driver: ToolResourceOwner,
}
impl FactoryCleanup {
    pub(super) fn new() -> Self {
        let owner = ToolResourceOwner::new();
        let closing = owner.resources().closing().expect("owned factory scope");
        let retained = owner.clone();
        let driver = ToolResourceOwner::new();
        driver
            .resources()
            .spawn(async move {
                closing.await;
                let report = retained.join().await;
                if report.sealed && report.pending == 0 && report.failures.is_empty() {
                    Ok(())
                } else {
                    Err(crate::ResourceFailure::CleanupFailed)
                }
            })
            .expect("fresh factory drain supervisor");
        Self { owner, driver }
    }
}

#[derive(Clone)]
pub(super) enum RecordMount {
    Legacy(crate::MountHandle),
    Owned(crate::OwnedMountHandle),
}
impl RecordMount {
    pub(super) fn check_close_allowed(&self) -> Result<(), ExtensionError> {
        match self {
            Self::Legacy(mount) => mount.check_close_allowed(),
            Self::Owned(mount) => mount.check_close_allowed(),
        }
    }
    pub(super) fn signal(&self) {
        match self {
            Self::Legacy(mount) => mount.deactivate(),
            Self::Owned(mount) => mount.signal_close(),
        }
    }
    pub(super) async fn close(&self) -> Result<(), ExtensionError> {
        match self {
            Self::Legacy(mount) => mount.close().await,
            Self::Owned(mount) => {
                let _actual_join = mount.close_registered().await?;
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn factory_supervisor_drives_actual_cleanup_join_before_initializer_can_finish() {
        let factory = FactoryCleanup::new();
        factory.owner.resources().spawn(async { Ok(()) }).unwrap();
        assert_eq!(factory.owner.snapshot().joined, 0);
        factory.owner.close();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while factory.owner.snapshot().joined != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // The caller still must observe the actual supervisor join, not merely
        // its internal owner's completed resources or a finished marker.
        let cleanup = RecordCleanup::Factory(factory);
        cleanup.wait_drained().await.unwrap();
        let RecordCleanup::Factory(factory) = cleanup else {
            unreachable!()
        };
        assert_eq!(factory.driver.snapshot().joined, 1);
    }
}
