use super::{
    Arc, Error, FactoryOutput, FixedPreparationLoan, FixedPreparationOwner, JoinedPreparation,
    Mutex, OwnedTaskRegistration, PreparationCommitError, PreparationGrant,
    PreparationResourceOutcome, Proof, SessionContext, SessionOwnerAuthority, SessionOwnerOutcome,
    Store,
};
use crate::native_enrollment::caller::measure_owner_authority;
use crate::session_owner::owned_scope::{
    OwnedScopeActivationOutcome, OwnedScopeParentAuthority, OwnedScopeSource,
};
use crate::session_owner::{MutateSessionOwner, SessionOwnerCommand};
use futures::future::BoxFuture;

pub(super) async fn native<'a, T: Send>(
    owner: &FixedPreparationOwner,
    slot: &Mutex<Option<T>>,
    construct: impl FnOnce() -> BoxFuture<'a, T>,
) -> Result<(), PreparationCommitError> {
    let result = super::super::enrolled_initial::observe::original(
        construct,
        |value| {
            *slot
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(value);
        },
        Some(tokio::time::Instant::now() + std::time::Duration::from_secs(5)),
        || {},
        || owner.fail(),
    )
    .await;
    if let Err(error) = result {
        if error == super::super::enrolled_initial::observe::Failure::Deadline {
            owner.request_stop();
        } else {
            owner.fail();
        }
        return Err(Error::UnknownStoreOutcome.into());
    }
    Ok(())
}
impl FixedPreparationOwner {
    pub(in crate::session_owner::owned_scope::task::pending) async fn run(
        self: &Arc<Self>,
        proof: &mut Proof,
    ) -> Result<(SessionContext, JoinedPreparation, FixedPreparationLoan), PreparationCommitError>
    {
        self.reserve(proof).await?;
        self.begin(proof).await?;
        self.retain(proof).await?;
        self.factory(proof).await?;
        let mut registration = self.registration.lock().await;
        let task = registration.as_mut().ok_or(Error::InvalidTransition)?;
        let grant = self.grant.get().ok_or(Error::InvalidTransition)?;
        measure_grant(grant, proof)?;
        proof
            .measure(task.activation())
            .map_err(|_| Error::Capacity)?;
        let value = JoinedPreparation::observe_registered(grant.clone(), task).await??;
        // This is the actual factory output. Install it before validation/copy;
        // the original registration stays retained until explicit output disposal.
        self.lock().output = Some(value.0);
        let context = {
            let state = self.lock();
            let context = &state
                .output
                .as_ref()
                .ok_or(Error::InvalidTransition)?
                .context;
            proof
                .measure(&(
                    context.session_id(),
                    context.workspace_id(),
                    context.directory(),
                ))
                .map_err(|_| Error::Capacity)?;
            context.clone()
        };
        Ok((
            context,
            value.1,
            FixedPreparationLoan {
                owner: self.clone(),
            },
        ))
    }
    async fn reserve(&self, proof: &mut Proof) -> Result<(), PreparationCommitError> {
        self.entering()?;
        proof
            .measure(&self.reservation)
            .map_err(|_| Error::Capacity)?;
        native(self, &self.reserved, || {
            self.core
                .store()
                .reserve_preparation(self.reservation.clone())
        })
        .await?;
        let reserved = self
            .reserved
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let outcome = reserved
            .as_ref()
            .ok_or(Error::UnknownStoreOutcome)?
            .as_ref()
            .map_err(|error| *error)?;
        let SessionOwnerOutcome::Applied {
            receipt,
            authority: Some(authority),
        } = outcome
        else {
            return Err(Error::UnknownStoreOutcome.into());
        };
        proof.measure(receipt).map_err(|_| Error::Capacity)?;
        measure_owner_authority(authority, proof).map_err(|_| Error::Capacity)?;
        self.root
            .set(authority.clone())
            .map_err(|_| Error::InvalidTransition)?;
        Ok(())
    }
    async fn begin(&self, proof: &mut Proof) -> Result<(), PreparationCommitError> {
        self.entering()?;
        let authority = self.root.get().ok_or(Error::InvalidTransition)?;
        measure_owner_authority(authority, proof).map_err(|_| Error::Capacity)?;
        proof
            .measure(&self.reservation)
            .map_err(|_| Error::Capacity)?;
        let id = self.reservation.reconciliation()?.begin_id;
        let command = SessionOwnerCommand::Begin;
        measure_mutation(&id, authority, &command, proof)?;
        let request = MutateSessionOwner {
            id,
            session_id: authority.session_id().clone(),
            target: authority.target().clone(),
            command,
        };
        native(self, &self.began, || {
            self.core
                .store()
                .mutate_session_owner(request, Some(authority.clone()))
        })
        .await?;
        let began = self
            .began
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = began
            .as_ref()
            .ok_or(Error::UnknownStoreOutcome)?
            .as_ref()
            .map_err(|error| *error)?;
        let SessionOwnerOutcome::Applied { receipt, .. } = result else {
            return Err(Error::UnknownStoreOutcome.into());
        };
        proof.measure(receipt).map_err(|_| Error::Capacity)?;
        Ok(())
    }
    async fn retain(&self, proof: &mut Proof) -> Result<(), PreparationCommitError> {
        self.entering()?;
        let authority = self.root.get().ok_or(Error::InvalidTransition)?;
        proof
            .measure(&self.reservation)
            .map_err(|_| Error::Capacity)?;
        measure_owner_authority(authority, proof).map_err(|_| Error::Capacity)?;
        let grant =
            PreparationGrant::from_fresh_begin(self.reservation.clone(), authority.clone())?;
        native(self, &self.retained, || {
            self.core.store().retain_preparation_resources(grant)
        })
        .await?;
        let retained = self
            .retained
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = retained
            .as_ref()
            .ok_or(Error::UnknownStoreOutcome)?
            .as_ref()
            .map_err(|error| *error)?;
        let PreparationResourceOutcome::Applied { grant, .. } = result else {
            return Err(Error::UnknownStoreOutcome.into());
        };
        measure_grant(grant, proof)?;
        self.grant
            .set((**grant).clone())
            .map_err(|_| Error::InvalidTransition)?;
        Ok(())
    }
    async fn factory(self: &Arc<Self>, proof: &mut Proof) -> Result<(), PreparationCommitError> {
        self.entering()?;
        let source = {
            let retained = self
                .retained
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(Ok(PreparationResourceOutcome::Applied { receipt, .. })) = retained.as_ref()
            else {
                return Err(Error::UnknownStoreOutcome.into());
            };
            proof
                .measure(&(&receipt.reserved, &receipt.began))
                .map_err(|_| Error::Capacity)?;
            OwnedScopeSource::from_receipts(&receipt.reserved, &receipt.began)?
        };
        let current = self.clone();
        let mut slot = self.registration.lock().await;
        let task = OwnedTaskRegistration::register(source, move || async move {
            current.entering().map_err(|_| Error::InvalidTransition)?;
            #[cfg(test)]
            if let Some(test) = current.test.get() {
                test.factory().await;
            }
            current.entering().map_err(|_| Error::InvalidTransition)?;
            let mut state = current.lock();
            Ok((
                FactoryOutput {
                    context: state.context.take().ok_or(Error::InvalidTransition)?,
                    #[cfg(test)]
                    _probe: current.test.get().and_then(|test| test.output()),
                },
                state.operation.take().ok_or(Error::InvalidTransition)?,
            ))
        })?;
        *slot = Some(task);
        let task = slot.as_ref().ok_or(Error::InvalidTransition)?;
        proof
            .measure(task.activation())
            .map_err(|_| Error::Capacity)?;
        self.activation
            .set(task.activation().clone())
            .map_err(|_| Error::InvalidTransition)?;
        drop(slot);
        self.entering()?;
        let activation = self.activation.get().ok_or(Error::InvalidTransition)?;
        let grant = self.grant.get().ok_or(Error::InvalidTransition)?;
        let authority = grant
            .retained_resources_authority()
            .ok_or(Error::InvalidTransition)?;
        measure_grant(grant, proof)?;
        proof.measure(activation).map_err(|_| Error::Capacity)?;
        measure_owner_authority(authority, proof).map_err(|_| Error::Capacity)?;
        #[cfg(test)]
        let fault = self.activation_fault();
        let observed = native(self, &self.activated, || {
            self.core.store().activate_owned_scope(
                activation.clone(),
                authority.clone(),
                Some(OwnedScopeParentAuthority::preparation(grant.clone())),
            )
        })
        .await;
        #[cfg(test)]
        drop(fault);
        observed?;
        let permit = {
            let mut activated = self
                .activated
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let result = activated.take().ok_or(Error::UnknownStoreOutcome)??;
            match result {
                OwnedScopeActivationOutcome::Applied { receipt, permit } => {
                    self.activation_ack
                        .set(receipt)
                        .map_err(|_| Error::InvalidTransition)?;
                    proof
                        .measure(self.activation_ack.get().ok_or(Error::InvalidTransition)?)
                        .map_err(|_| Error::Capacity)?;
                    permit
                }
                OwnedScopeActivationOutcome::Replayed(_) => {
                    return Err(Error::UnknownStoreOutcome.into());
                }
            }
        };
        self.entering()?;
        self.registration
            .lock()
            .await
            .as_mut()
            .ok_or(Error::InvalidTransition)?
            .dispatch(*permit)?;
        Ok(())
    }
}
pub(super) fn measure_grant(
    grant: &PreparationGrant,
    proof: &mut Proof,
) -> Result<(), PreparationCommitError> {
    proof
        .measure(grant.original_reservation())
        .map_err(|_| Error::Capacity)?;
    measure_owner_authority(grant.owner_authority(), proof).map_err(|_| Error::Capacity)?;
    if let Some(authority) = grant.retained_resources_authority() {
        measure_owner_authority(authority, proof).map_err(|_| Error::Capacity)?;
    }
    Ok(())
}

pub(super) fn measure_mutation(
    id: &crabber_core::SessionOwnerOperationId,
    authority: &SessionOwnerAuthority,
    command: &SessionOwnerCommand,
    proof: &mut Proof,
) -> Result<(), PreparationCommitError> {
    #[derive(serde::Serialize)]
    struct View<'a> {
        id: &'a crabber_core::SessionOwnerOperationId,
        session_id: &'a crabber_core::SessionId,
        target: &'a crate::session_owner::SessionOwnerTarget,
        command: &'a SessionOwnerCommand,
    }
    proof
        .measure(&View {
            id,
            session_id: authority.session_id(),
            target: authority.target(),
            command,
        })
        .map_err(|_| Error::Capacity)?;
    Ok(())
}
