//! Cached routing remains available while a preparation owns its async lock.
use super::{IndexedEntry, RetainedPreparations};
use crate::orchestrator::{
    Orchestrator,
    preparation::progress::{RetainedPreparationProgress, RetainedPreparationStatus},
};
use std::sync::atomic::Ordering;

pub(in crate::orchestrator) struct PreparationCloseSelection {
    registry: RetainedPreparations,
}
impl RetainedPreparations {
    pub(in crate::orchestrator) fn begin_close(&self) -> PreparationCloseSelection {
        let _entries = self.entries.lock().unwrap();
        self.closed.store(true, Ordering::Release);
        PreparationCloseSelection {
            registry: self.clone(),
        }
    }
    pub(in crate::orchestrator) fn publish_resources(
        &self,
        descriptor: &crabber_session::session_owner::preparation::PreparationReconciliation,
        resources: &crabber_extension::ToolResourceOwner,
    ) {
        if let Some(indexed) = self
            .entries
            .lock()
            .unwrap()
            .get_mut(&super::key(descriptor))
        {
            indexed.resources = Some(resources.clone());
        }
    }
    pub(in crate::orchestrator) fn is_current_callback_parent(&self) -> bool {
        let resources = self
            .entries
            .lock()
            .unwrap()
            .values()
            .filter_map(|indexed| indexed.resources.clone())
            .collect::<Vec<_>>();
        resources
            .iter()
            .any(crabber_extension::ToolResourceOwner::is_current_callback_parent)
    }
    pub(in crate::orchestrator) fn snapshot_close(&self) -> Vec<RetainedPreparationProgress> {
        self.entries
            .lock()
            .unwrap()
            .values()
            .map(IndexedEntry::close_report)
            .collect()
    }
}
impl IndexedEntry {
    fn close_report(&self) -> RetainedPreparationProgress {
        let status = self.entry.try_lock().map_or(
            RetainedPreparationStatus::ObservationInProgress,
            |held| {
                if held
                    .pending
                    .as_ref()
                    .is_some_and(|pending| pending.failed())
                    || held.heartbeat.failed()
                    || held.cleanup_supervisor.is_finished()
                {
                    RetainedPreparationStatus::CleanupUncertain
                } else if held.disposing {
                    RetainedPreparationStatus::CleanupRequested
                } else if held.committed
                    || held
                        .pending
                        .as_ref()
                        .is_some_and(|pending| pending.committed())
                {
                    RetainedPreparationStatus::ResourcesRetained
                } else if held.joined.is_some() {
                    RetainedPreparationStatus::NativeCommitPending
                } else {
                    RetainedPreparationStatus::ObservationInProgress
                }
            },
        );
        RetainedPreparationProgress {
            preparation: self.descriptor.clone(),
            status,
        }
    }
}
impl PreparationCloseSelection {
    pub(in crate::orchestrator) fn signal_all(&self) {
        let selected = self
            .registry
            .entries
            .lock()
            .unwrap()
            .values()
            .map(|indexed| (indexed.root_scope.clone(), indexed.entry.clone()))
            .collect::<Vec<_>>();
        for (root, entry) in selected {
            if let Some(root) = root {
                root.stop();
            }
            let pending = entry.try_lock().ok().and_then(|held| held.pending.clone());
            if let Some(pending) = pending {
                pending.stop();
            }
        }
    }
    pub(in crate::orchestrator) fn snapshot(&self) -> Vec<RetainedPreparationProgress> {
        self.registry.snapshot_close()
    }
    pub(in crate::orchestrator) async fn observe_until(
        &self,
        runtime: &Orchestrator,
        deadline: tokio::time::Instant,
    ) -> Vec<RetainedPreparationProgress> {
        let changed = self.registry.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        self.signal_all();
        let descriptors = self
            .registry
            .entries
            .lock()
            .unwrap()
            .values()
            .map(|indexed| indexed.descriptor.clone())
            .collect::<Vec<_>>();
        // Every stop has already been signalled. Independent entries may await
        // their actual owner locks concurrently under the SAME deadline.
        futures::future::join_all(descriptors.iter().map(|descriptor| {
            tokio::time::timeout_at(deadline, runtime.close_retained_preparation(descriptor))
        }))
        .await;
        loop {
            let snapshot = self.snapshot();
            if snapshot.is_empty() {
                return snapshot;
            }
            if tokio::time::timeout_at(deadline, changed.as_mut())
                .await
                .is_err()
            {
                return self.snapshot();
            }
            changed.set(self.registry.changed.notified());
            changed.as_mut().enable();
            self.signal_all();
        }
    }
}
