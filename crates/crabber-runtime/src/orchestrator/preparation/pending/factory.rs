use super::{
    Arc, AtomicBool, CancellationToken, DurablePreparation, Future, Mutex, OnceLock, Orchestrator,
    PendingExisting, RuntimeError, retained,
};
use crabber_session::{
    Store,
    session_owner::{
        owned_scope::OwnedScopeActivationOutcome,
        preparation::{JoinedPreparation, PreparationResourceOutcome},
    },
};
use retained::PreparationValue;

impl Orchestrator {
    pub(super) async fn observe_existing_factory<T, F, W>(
        &self,
        prepared: DurablePreparation,
        work: F,
    ) -> Result<
        (
            retained::SharedEntry,
            Arc<PendingExisting>,
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
        let entry = self
            .retained_preparations
            .reserve(grant.clone(), capacity)?;
        let result = async {
            let PreparationResourceOutcome::Applied { receipt, grant } = self.store.retain_preparation_resources(grant).await? else {
                return Err(RuntimeError::PreparationRecoveryRequired);
            };
            let activation = self.register_owned_preparation(&entry, &grant, &receipt, work).await?;
            let pending = {
                let mut held = entry.lock().await;
                let owned = held.owned.take().ok_or(RuntimeError::PreparationRecoveryRequired)?;
                let resources = owned.resources.clone();
                let pending = Arc::new(PendingExisting {
                    resources,
                    factory: Mutex::new(owned),
                    heartbeat: Mutex::new(std::mem::take(&mut held.heartbeat)),
                    heartbeat_stop: held.heartbeat_stop.clone(), grant: (*grant).clone(),
                    main: OnceLock::new(), control: OnceLock::new(), cancellation: CancellationToken::new(),
                    failed: AtomicBool::new(false), committed: AtomicBool::new(false), replayed: std::sync::Mutex::new(None),
                    cleanup: std::sync::Mutex::new(None), native_done: CancellationToken::new(),
                });
                held.pending = Some(pending.clone());
                pending
            };
            let authority = grant.retained_resources_authority().ok_or(RuntimeError::PreparationRecoveryRequired)?.clone();
            let OwnedScopeActivationOutcome::Applied { permit, .. } = self.store.activate_owned_scope(
                activation, authority, Some(crabber_session::session_owner::owned_scope::OwnedScopeParentAuthority::preparation((*grant).clone())),
            ).await? else { return Err(RuntimeError::PreparationRecoveryRequired); };
            let (value, proof) = {
                let mut factory = pending.factory.lock().await;
                factory.task.dispatch(*permit)?;
                JoinedPreparation::observe_registered(*grant, &mut factory.task).await??
            };
            let value = T::recover(value).map_err(|_| RuntimeError::PreparationRecoveryRequired)?;
            entry.lock().await.joined = Some(proof.clone());
            Ok((entry.clone(), pending, value, proof))
        }.await;
        result.map_err(|error| super::super::incomplete(descriptor, error))
    }
}

pub(super) async fn finish(pending: &PendingExisting, store: &dyn Store) -> Result<(), ()> {
    pending.heartbeat_stop.cancel();
    let main = async {
        let mut factory = pending.factory.lock().await;
        factory.resources.close();
        retained::cleanup::observation::observe(&mut factory)
            .await
            .map_err(|_| ())
    };
    let heartbeat = async { pending.heartbeat.lock().await.join().await };
    let (proof, heartbeat) = futures::join!(main, heartbeat);
    heartbeat?;
    let proof = proof?;
    retained::cleanup::publish_join(&pending.grant, store, proof).await
}
