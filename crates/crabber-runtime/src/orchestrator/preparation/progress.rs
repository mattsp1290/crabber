//! Explicit local progress uses retained observations, never recovered grants.
use super::{Orchestrator, RuntimeError, retained};
use crabber_session::session_owner::preparation::{
    PreparationAdmissionReceipt, PreparationCompletion, PreparationKey, PreparationOperationKind,
    PreparationReconciliation,
};

/// Metadata only. Native work recovered from an Unstarted admission requires
/// the separate authenticated recovery API; this result never grants dispatch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparationRetryProgress {
    pub operation: PreparationOperationKind,
    pub replayed: bool,
    pub unkeyed_admission: Option<PreparationAdmissionReceipt>,
}
/// Local observation only. Absence never proves remote or durable quiescence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetainedPreparationStatus {
    ObservationInProgress,
    NativeCommitPending,
    ResourcesRetained,
    CleanupRequested,
    CleanupUncertain,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetainedPreparationProgress {
    pub preparation: PreparationReconciliation,
    pub status: RetainedPreparationStatus,
}
impl Orchestrator {
    /// Bounded local metadata, including returned resources whose original
    /// native completion or actual cleanup still needs explicit progress.
    /// Empty results never prove that another controller has no retained work.
    #[must_use]
    pub fn retained_preparation_status(
        &self,
        session: &crabber_core::SessionId,
    ) -> Vec<RetainedPreparationProgress> {
        self.retained_preparations.status(session)
    }
    /// Retry only the actual joined task's exact original native commit. No
    /// factory, resolver, plan restoration or business work is invoked here.
    /// Initial and first-two successor pending mains only expose their original
    /// same-task completion notice; this method never repeats their native call.
    /// An unknown acknowledgement with no notice retains its uncertainty.
    /// On success, retained output disposal requests cleanup; actual scope joins
    /// and their durable acknowledgement remain independently supervised.
    /// # Errors
    /// Refuses missing local observations or changed correlation. A descriptor
    /// alone cannot recreate authority after a process restart.
    pub async fn retry_preparation_completion(
        &self,
        descriptor: &PreparationReconciliation,
    ) -> Result<PreparationRetryProgress, RuntimeError> {
        let entry = self
            .retained_preparations
            .lookup(descriptor)
            .ok_or(RuntimeError::PreparationRecoveryRequired)?;
        let mut held = entry.try_lock().map_err(|_| RuntimeError::SessionBusy)?;
        if held.descriptor != *descriptor || held.disposing {
            return Err(RuntimeError::PreparationRecoveryRequired);
        }
        if let Some(pending) = held.pending.clone() {
            let operation = held
                .joined
                .as_ref()
                .ok_or(RuntimeError::PreparationRecoveryRequired)?
                .operation_kind();
            drop(held);
            return pending
                .retry_progress(operation)
                .map_err(|error| super::incomplete(descriptor.clone(), error));
        }
        let proof = held
            .joined
            .clone()
            .ok_or(RuntimeError::PreparationRecoveryRequired)?;
        let operation = proof.operation_kind();
        let completed = self
            .store
            .complete_preparation(proof)
            .await
            .map_err(|error| super::incomplete(descriptor.clone(), error.into()))?;
        let unkeyed_admission = match (&completed, &held.grant.original_reservation().key) {
            (PreparationCompletion::AdmissionReplayed(receipt), _) => Some(receipt.clone()),
            (PreparationCompletion::Admitted(admitted), PreparationKey::Unkeyed(id)) => {
                Some(PreparationAdmissionReceipt {
                    session_id: admitted.session.id.clone(),
                    run_id: admitted.run.id.clone(),
                    operation_id: id.clone(),
                })
            }
            _ => None,
        };
        held.committed = true;
        // This relinquishes output ownership, not cleanup evidence. Only the
        // separate actual main/resource joins can retire the child owner.
        drop(held.value.take());
        let legacy = held.owned.is_none();
        if legacy {
            if let Err((error, payload)) = stop_legacy(&mut held).await {
                drop(held);
                retained::heartbeat::discard_join_error(payload);
                return Err(error);
            }
            self.retained_preparations.remove(descriptor, &entry);
        }
        drop(held);
        if !legacy {
            retained::cleanup::start(
                self.retained_preparations.clone(),
                entry,
                self.store.clone(),
            )
            .await;
        }
        Ok(PreparationRetryProgress {
            operation,
            replayed: completed.is_replayed(),
            unkeyed_admission,
        })
    }
    /// Request local cleanup of an owned preparation after a failure. The host
    /// retains actual main/resource joins, including non-cooperative cleanup;
    /// this method reports a request, never Joined or remote quiescence.
    /// # Errors
    /// Rejects another descriptor, unknown local state, or a legacy bridge whose
    /// external resources cannot be physically accounted for by this scope.
    pub async fn cleanup_retained_preparation(
        &self,
        descriptor: &PreparationReconciliation,
    ) -> Result<(), RuntimeError> {
        let entry = self
            .retained_preparations
            .lookup(descriptor)
            .ok_or(RuntimeError::PreparationRecoveryRequired)?;
        let held = entry.try_lock().map_err(|_| RuntimeError::SessionBusy)?;
        self.cleanup_entry(descriptor, &entry, held).await
    }
    pub(in crate::orchestrator) async fn close_retained_preparation(
        &self,
        descriptor: &PreparationReconciliation,
    ) -> Result<(), RuntimeError> {
        let entry = self
            .retained_preparations
            .lookup(descriptor)
            .ok_or(RuntimeError::PreparationRecoveryRequired)?;
        let held = entry.lock().await;
        self.cleanup_entry(descriptor, &entry, held).await
    }
    async fn cleanup_entry(
        &self,
        descriptor: &PreparationReconciliation,
        entry: &retained::SharedEntry,
        mut held: tokio::sync::MutexGuard<'_, retained::Entry>,
    ) -> Result<(), RuntimeError> {
        if held.descriptor != *descriptor {
            return Err(RuntimeError::PreparationRecoveryRequired);
        }
        if held.legacy_failure.is_some() {
            drop(held);
            return retained::cleanup::cancel_failed_legacy(
                self.retained_preparations.clone(),
                entry.clone(),
                self.store.clone(),
            )
            .await;
        }
        if let Some(pending) = held.pending.clone() {
            held.disposing = true;
            drop(held.value.take());
            drop(held);
            super::pending::cleanup_request(self.clone(), &pending, entry.clone());
            return Ok(());
        }
        if held.descriptor != *descriptor || held.owned.is_none() {
            return Err(RuntimeError::PreparationRecoveryRequired);
        }
        held.disposing = true;
        drop(held.value.take());
        held.owned
            .as_ref()
            .ok_or(RuntimeError::PreparationRecoveryRequired)?
            .disposed
            .cancel();
        drop(held);
        retained::cleanup::start(
            self.retained_preparations.clone(),
            entry.clone(),
            self.store.clone(),
        )
        .await;
        Ok(())
    }
}
pub(super) async fn stop_legacy(
    held: &mut retained::Entry,
) -> Result<(), (RuntimeError, Option<tokio::task::JoinError>)> {
    retained::heartbeat::stop(held).await.map_err(|payload| {
        (
            super::incomplete(held.descriptor.clone(), RuntimeError::TaskStopped),
            payload,
        )
    })
}

#[cfg(test)]
impl Orchestrator {
    pub(crate) fn retained_preparation_heartbeat_stop(
        &self,
        descriptor: &PreparationReconciliation,
    ) -> Option<tokio_util::sync::CancellationToken> {
        let entry = self.retained_preparations.lookup(descriptor)?;
        let held = entry.try_lock().ok()?;
        Some(held.heartbeat_stop.clone())
    }
}
