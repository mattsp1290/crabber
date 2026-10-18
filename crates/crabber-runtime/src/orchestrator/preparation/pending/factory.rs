#[path = "factory/cancel.rs"]
mod cancel;
use super::{
    Arc, AtomicBool, CancellationToken, DurablePreparation, Future, Mutex, OnceLock, Orchestrator,
    PendingPreparation, RuntimeError, retained,
};
pub(super) use cancel::cancel_unstarted_root;
use crabber_session::{
    Store,
    session_owner::{
        owned_scope::OwnedScopeActivationOutcome,
        preparation::{JoinedPreparation, PreparationResourceOutcome},
    },
};
use retained::PreparationValue;

impl Orchestrator {
    pub(super) async fn observe_pending_factory<T, F, W>(
        &self,
        prepared: DurablePreparation,
        work: F,
    ) -> Result<
        (
            retained::SharedEntry,
            Arc<PendingPreparation>,
            T,
            JoinedPreparation,
        ),
        RuntimeError,
    >
    where
        T: PreparationValue,
        F: FnOnce(Option<super::super::scope::PlanScope>) -> W + Send + 'static,
        W: Future<Output = Result<(T, super::super::PreparedOperation), RuntimeError>>
            + Send
            + 'static,
    {
        let DurablePreparation { grant, capacity } = prepared;
        let descriptor = grant.reconciliation()?;
        let entry = self.retain_preparation_entry(grant.clone(), capacity)?;
        let result = async {
            let PreparationResourceOutcome::Applied { receipt, grant } = self.store.retain_preparation_resources(grant).await? else {
                return Err(RuntimeError::PreparationRecoveryRequired);
            };
            let activation = self.register_owned_preparation(&entry, &grant, &receipt, work).await?;
            let pending = {
                let mut held = entry.lock().await;
                install_pending(&mut held, &grant)?
            };
            if pending.cancellation.is_cancelled() { return Err(RuntimeError::Interrupted); }
            let authority = grant.retained_resources_authority().ok_or(RuntimeError::PreparationRecoveryRequired)?.clone();
            let OwnedScopeActivationOutcome::Applied { permit, .. } = self.store.activate_owned_scope(
                activation, authority, Some(crabber_session::session_owner::owned_scope::OwnedScopeParentAuthority::preparation((*grant).clone())),
            ).await? else { return Err(RuntimeError::PreparationRecoveryRequired); };
            let (value, proof) = {
                let mut factory = pending.factory.lock().await;
                factory.task.dispatch(*permit)?;
                JoinedPreparation::observe_registered(*grant, &mut factory.task).await??
            };
            entry.lock().await.joined = Some(proof.clone());
            let value = match T::recover(value) {
                Ok(value) => value,
                Err(value) => {
                    entry.lock().await.value = Some(value);
                    return Err(RuntimeError::PreparationRecoveryRequired);
                }
            };
            Ok((entry.clone(), pending, value, proof))
        }.await;
        result.map_err(|error| super::super::incomplete(descriptor, error))
    }
}

// Caller holds the original Entry lock across the full ownership transition.
fn install_pending(
    held: &mut retained::Entry,
    grant: &crabber_session::session_owner::preparation::PreparationGrant,
) -> Result<Arc<PendingPreparation>, RuntimeError> {
    if held.disposing
        || held.cleanup_supervisor.is_running()
        || held.cleanup_supervisor.is_finished()
    {
        return Err(RuntimeError::PreparationRecoveryRequired);
    }
    let owned = held
        .owned
        .take()
        .ok_or(RuntimeError::PreparationRecoveryRequired)?;
    let resources = owned.resources.clone();
    let pending = Arc::new(PendingPreparation {
        resources,
        factory: Mutex::new(owned),
        heartbeat: Mutex::new(std::mem::take(&mut held.heartbeat)),
        heartbeat_stop: held.heartbeat_stop.clone(),
        grant: grant.clone(),
        main: OnceLock::new(),
        enrollment: std::sync::Mutex::new(()),
        control: OnceLock::new(),
        cancellation: held.root_scope.as_ref().map_or_else(
            CancellationToken::new,
            crate::execution_tasks::RootScope::cancellation,
        ),
        root_scope: held.root_scope.clone(),
        failed: AtomicBool::new(false),
        cleanup_active: AtomicBool::new(false),
        cleanup_failed: AtomicBool::new(false),
        committed: AtomicBool::new(false),
        completion: std::sync::Mutex::new(None),
        cleanup: std::sync::Mutex::new(None),
        native_done: CancellationToken::new(),
    });
    held.pending = Some(pending.clone());
    Ok(pending)
}

pub(super) async fn finish(pending: &PendingPreparation, store: &dyn Store) -> Result<(), ()> {
    pending.heartbeat_stop.cancel();
    pending.resources.close();
    let main = async {
        let mut factory = pending.factory.lock().await;
        match retained::cleanup::observation::observe(&mut factory).await {
            Ok(proof) => {
                // Closing may start cleanup while a stopped native ACK is
                // retained, but the original returned plan's lease must ALSO
                // actually dispose before its resource owner can Join.
                let disposed = factory.disposed.clone();
                drop(factory);
                disposed.cancelled().await;
                Ok(proof)
            }
            Err(Some(failure)) => {
                // A real observed cleanup failure retains its original source;
                // retry reconciles this exact uncertainty, never a fake Join.
                let _ = retained::cleanup::failure::publish_owned(
                    &pending.grant,
                    &factory,
                    store,
                    failure,
                )
                .await;
                Err(())
            }
            Err(None) => Err(()),
        }
    };
    let heartbeat = async {
        let mut retained = pending.heartbeat.lock().await;
        let observed = retained.observe_join().await;
        drop(retained);
        match observed {
            Ok(()) => Ok(()),
            Err(payload) => {
                retained::heartbeat::discard_join_error(payload);
                Err(())
            }
        }
    };
    // Observe both original tasks even if either observation fails. A failed
    // heartbeat can never be replaced by an empty RetainedTask on retry.
    let (proof, heartbeat) = futures::join!(main, heartbeat);
    heartbeat?;
    let proof = proof?;
    retained::cleanup::publish_join(&pending.grant, store, proof).await
}

#[cfg(test)]
#[path = "factory/tests.rs"]
mod tests;
