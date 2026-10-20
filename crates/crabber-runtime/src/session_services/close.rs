//! Selection owns the actual entries; status is independent of native settlement.
use super::{SessionServices, entry::Entry};
use crabber_core::SessionId;
use crabber_extension::{ResourceDrain, SessionServiceDependencyControl};
use std::sync::{Arc, atomic::Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionServiceStatus {
    Registering,
    Active,
    Uncertain,
    Closing,
    Joined,
    Replayed,
    Cancelled,
    Rejected,
    Deleted,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionServiceReport {
    pub session_id: SessionId,
    pub generation: String,
    pub status: SessionServiceStatus,
    pub local_joined: bool,
    pub supervisor_failed: bool,
    pub resources: ResourceDrain,
}
tokio::task_local! { static CURRENT_OWNER: Arc<Entry>; }
pub(super) async fn scope<T>(owner: Arc<Entry>, future: impl std::future::Future<Output = T>) -> T {
    CURRENT_OWNER.scope(owner, future).await
}

pub(crate) struct ServiceCloseSelection {
    entries: Vec<Arc<Entry>>,
}
impl SessionServices {
    pub(crate) fn begin_close(&self) -> ServiceCloseSelection {
        let entries = self.entries.lock().unwrap();
        self.closed.store(true, Ordering::Release);
        ServiceCloseSelection {
            entries: entries.values().cloned().collect(),
        }
    }
    pub(crate) fn snapshot_close(&self) -> Vec<SessionServiceReport> {
        let entries = self
            .entries
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        entries.iter().map(|entry| entry.close_report()).collect()
    }
    pub(crate) fn is_current_owner(&self) -> bool {
        let entries = self
            .entries
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        CURRENT_OWNER
            .try_with(|current| entries.iter().any(|entry| Arc::ptr_eq(current, entry)))
            .unwrap_or(false)
            || entries
                .iter()
                .any(|entry| entry.resources.is_current_callback_parent())
    }
}
impl Entry {
    fn close_report(&self) -> SessionServiceReport {
        let local_joined = *self.observed.lock().unwrap();
        SessionServiceReport {
            session_id: self.source.root.session_id.clone(),
            generation: self.source.generation.id.clone(),
            status: self.status(),
            local_joined,
            supervisor_failed: self.join_failed.load(Ordering::Acquire),
            resources: self.resources.snapshot(),
        }
    }
}
impl ServiceCloseSelection {
    pub(crate) fn signal_all(&self) {
        for entry in &self.entries {
            entry.cancel.cancel();
        }
    }
    pub(crate) fn snapshot(&self) -> Vec<SessionServiceReport> {
        self.entries
            .iter()
            .map(|entry| entry.close_report())
            .collect()
    }
    pub(crate) async fn observe_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> Vec<SessionServiceReport> {
        for entry in &self.entries {
            let _ = entry
                .close_and_join(deadline.saturating_duration_since(tokio::time::Instant::now()))
                .await;
        }
        self.snapshot()
    }
}
