//! Returned plans remain retained across failed or unknown native completion.
use super::{
    DurablePreparation, JoinedPreparation, Orchestrator, PreparationCompletion, PreparationGrant,
    PreparedOperation, RuntimeError, incomplete,
    progress::stop_legacy,
    retained::{self, PreparationValue, SharedEntry},
    scope::PlanScope,
};
use crabber_session::{
    StoreError,
    session_owner::{
        owned_scope::{
            OwnedScopeActivationOutcome, OwnedScopeParentAuthority, OwnedScopeSource,
            OwnedTaskRegistration,
        },
        preparation::PreparationResourceOutcome,
    },
};
use std::future::Future;

impl Orchestrator {
    pub(in crate::orchestrator) async fn finish_durable_preparation<T, F, W>(
        &self,
        prepared: DurablePreparation,
        work: F,
    ) -> Result<(T, PreparationCompletion), RuntimeError>
    where
        T: PreparationValue,
        F: FnOnce(Option<PlanScope>) -> W + Send + 'static,
        W: Future<Output = Result<(T, PreparedOperation), RuntimeError>> + Send + 'static,
    {
        let DurablePreparation { grant, capacity } = prepared;
        let descriptor = grant.reconciliation()?;
        let entry = self
            .retained_preparations
            .reserve(grant.clone(), capacity)
            .map_err(|error| incomplete(descriptor.clone(), error))?;
        let observed = if grant.original_reservation().resource_contract.is_some() {
            self.observe_owned_preparation(&entry, grant, work).await
        } else {
            self.observe_legacy_preparation(&entry, grant, work).await
        };
        observed.map_err(|error| incomplete(descriptor.clone(), error))?;
        let mut held = entry.lock().await;
        let proof = held
            .joined
            .clone()
            .ok_or(RuntimeError::PreparationRecoveryRequired)?;
        let completed = self
            .store
            .complete_preparation(proof)
            .await
            .map_err(|error| incomplete(descriptor.clone(), error.into()))?;
        held.committed = true;
        let value = held
            .value
            .take()
            .ok_or(RuntimeError::PreparationRecoveryRequired)?;
        let value = match T::recover(value) {
            Ok(value) => value,
            Err(value) => {
                held.value = Some(value);
                return Err(incomplete(
                    descriptor,
                    RuntimeError::PreparationRecoveryRequired,
                ));
            }
        };
        let legacy = held.owned.is_none();
        if legacy {
            stop_legacy(&mut held).await?;
            self.retained_preparations.remove(&descriptor, &entry);
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
        Ok((value, completed))
    }

    async fn observe_legacy_preparation<T, F, W>(
        &self,
        entry: &SharedEntry,
        grant: PreparationGrant,
        work: F,
    ) -> Result<(), RuntimeError>
    where
        T: PreparationValue,
        F: FnOnce(Option<PlanScope>) -> W + Send + 'static,
        W: Future<Output = Result<(T, PreparedOperation), RuntimeError>> + Send + 'static,
    {
        let mut held = entry.lock().await;
        held.heartbeat = retained::heartbeat::RetainedTask::running(retained::heartbeat::start(
            self.store.clone(),
            vec![grant.owner_authority().clone()],
            held.heartbeat_stop.clone(),
            None,
        ));
        let task = tokio::spawn(async move { work(None).await });
        let (value, proof) = JoinedPreparation::observe(grant, task)
            .await
            .map_err(|_| RuntimeError::TaskStopped)??;
        held.value = Some(value.retain());
        held.joined = Some(proof);
        Ok(())
    }

    async fn observe_owned_preparation<T, F, W>(
        &self,
        entry: &SharedEntry,
        grant: PreparationGrant,
        work: F,
    ) -> Result<(), RuntimeError>
    where
        T: PreparationValue,
        F: FnOnce(Option<PlanScope>) -> W + Send + 'static,
        W: Future<Output = Result<(T, PreparedOperation), RuntimeError>> + Send + 'static,
    {
        if !self.store.supports_preparation_resources() || !self.store.supports_owned_scopes() {
            return Err(StoreError::HostUnsupported.into());
        }
        let PreparationResourceOutcome::Applied { receipt, grant } =
            self.store.retain_preparation_resources(grant).await?
        else {
            return Err(RuntimeError::PreparationRecoveryRequired);
        };
        let scope = self
            .register_owned_preparation(entry, &grant, &receipt, work)
            .await?;
        let authority = grant
            .retained_resources_authority()
            .ok_or(RuntimeError::PreparationRecoveryRequired)?
            .clone();
        let outcome = self
            .store
            .activate_owned_scope(
                scope,
                authority,
                Some(OwnedScopeParentAuthority::preparation((*grant).clone())),
            )
            .await?;
        let OwnedScopeActivationOutcome::Applied { permit, .. } = outcome else {
            return Err(RuntimeError::PreparationRecoveryRequired);
        };
        let mut held = entry.lock().await;
        let owned = held
            .owned
            .as_mut()
            .ok_or(RuntimeError::PreparationRecoveryRequired)?;
        owned.task.dispatch(*permit)?;
        let (value, proof) =
            JoinedPreparation::observe_registered(*grant, &mut owned.task).await??;
        held.value = Some(value);
        held.joined = Some(proof);
        Ok(())
    }

    async fn register_owned_preparation<T, F, W>(
        &self,
        entry: &SharedEntry,
        grant: &PreparationGrant,
        receipt: &crabber_session::session_owner::preparation::PreparationResourceReceipt,
        work: F,
    ) -> Result<crabber_session::session_owner::owned_scope::OwnedScopeActivation, RuntimeError>
    where
        T: PreparationValue,
        F: FnOnce(Option<PlanScope>) -> W + Send + 'static,
        W: Future<Output = Result<(T, PreparedOperation), RuntimeError>> + Send + 'static,
    {
        let resources = crabber_extension::ToolResourceOwner::new();
        let disposed = tokio_util::sync::CancellationToken::new();
        let reservation = grant.original_reservation();
        let context = crabber_core::SessionContext::new(
            reservation.owner.binding.session_id.clone(),
            reservation.owner.binding.workspace_id.clone(),
            reservation.owner.binding.directory.clone(),
        );
        let parent_done = tokio_util::sync::CancellationToken::new();
        let context = crabber_extension::PreparationResourceContext::from_host(
            context,
            resources.resources(),
        )
        .map_err(|_| StoreError::HostUnsupported)?
        .with_service_dependencies(self.session_services.bind(
            self.store.clone(),
            grant.clone(),
            parent_done.clone(),
        ));
        let scope = PlanScope {
            contract: reservation
                .resource_contract
                .clone()
                .ok_or(RuntimeError::PreparationRecoveryRequired)?,
            provider_services: crate::session_services::provider::ProviderServices::new(
                context.clone(),
                parent_done.clone(),
            ),
            context,
            lease: crabber_extension::PreparationResourceLease::from_host(disposed.clone()),
        };
        let source = OwnedScopeSource::from_receipts(&receipt.reserved, &receipt.began)?;
        let task = OwnedTaskRegistration::register(source, move || async move {
            // Closing this ingress is not a main/resource Join acknowledgement.
            // Escaped preparation capabilities cannot start another factory
            // after this actual initializer future has returned or been dropped.
            let _ingress = parent_done.drop_guard();
            work(Some(scope))
                .await
                .map(|(value, operation)| (value.retain(), operation))
        })?;
        let activation = task.activation().clone();
        let mut held = entry.lock().await;
        held.grant = grant.clone();
        held.heartbeat = retained::heartbeat::RetainedTask::running(retained::heartbeat::start(
            self.store.clone(),
            vec![
                grant.owner_authority().clone(),
                grant
                    .retained_resources_authority()
                    .ok_or(RuntimeError::PreparationRecoveryRequired)?
                    .clone(),
            ],
            held.heartbeat_stop.clone(),
            Some(resources.clone()),
        ));
        held.owned = Some(retained::OwnedEntry {
            task,
            resources,
            disposed,
            cleanup: None,
            cleanup_source: None,
            joined: None,
            cleanup_failure: None,
        });
        Ok(activation)
    }
}
