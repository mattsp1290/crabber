//! Actual literal preparation ownership, retained before the first native call.
mod policy;
mod run;
#[cfg(test)]
mod testing;
use super::{enrolled_initial::InitialResources, fixed_memory::FixedMemoryInitialControl};
use crate::public_session::v2::proof::Proof;
use crate::session_owner::{
    SessionOwnerAuthority, SessionOwnerError as Error, SessionOwnerOutcome,
    owned_scope::{JoinedOwnedTask, OwnedScopeActivation, OwnedTaskRegistration},
    preparation::{
        JoinedPreparation, PreparationCommitError, PreparationGrant, PreparationResourceOutcome,
        PreparedOperation, ReservePreparation,
    },
};
use crate::{MemoryStore, Store};
use crabber_core::SessionContext;
pub(crate) use policy::{OriginalPolicyRetirementView, OriginalPolicyView};
use std::sync::{
    Arc, Mutex, OnceLock, Weak,
    atomic::{AtomicBool, Ordering},
};
#[cfg(test)]
pub(crate) use testing::FixedInitialTestHooks;

struct FactoryOutput {
    context: SessionContext,
    #[cfg(test)]
    _probe: Option<testing::OutputProbe>,
}
type FactoryResult = Result<(FactoryOutput, PreparedOperation), Error>;
struct State {
    context: Option<SessionContext>,
    operation: Option<PreparedOperation>,
    output: Option<FactoryOutput>,
    output_disposed: bool,
    failed: bool,
}
pub(in crate::session_owner::owned_scope::task::pending) struct FixedPreparationOwner {
    core: Arc<FixedMemoryInitialControl>,
    reservation: ReservePreparation,
    state: Mutex<State>,
    stopped: AtomicBool,
    reserved: Mutex<Option<Result<SessionOwnerOutcome, Error>>>,
    began: Mutex<Option<Result<SessionOwnerOutcome, Error>>>,
    retained: Mutex<Option<Result<PreparationResourceOutcome, Error>>>,
    activated: Mutex<
        Option<Result<crate::session_owner::owned_scope::OwnedScopeActivationOutcome, Error>>,
    >,
    root_cleanup: Mutex<Option<Result<SessionOwnerOutcome, Error>>>,
    child_cleanup: Mutex<Option<Result<SessionOwnerOutcome, Error>>>,
    root: OnceLock<SessionOwnerAuthority>,
    grant: OnceLock<PreparationGrant>,
    activation: OnceLock<OwnedScopeActivation>,
    activation_ack: OnceLock<crate::session_owner::SessionOwnerReceipt>,
    registration: tokio::sync::Mutex<Option<OwnedTaskRegistration<FactoryResult>>>,
    joined: OnceLock<JoinedOwnedTask<FactoryResult>>,
    holder: OnceLock<Weak<InitialResources>>,
    inner_task: OnceLock<tokio::task::Id>,
    drained: tokio::sync::watch::Sender<bool>,
    #[cfg(test)]
    test: OnceLock<Arc<FixedInitialTestHooks>>,
    #[cfg(test)]
    seal_presentation: OnceLock<time::OffsetDateTime>,
}
impl FixedPreparationOwner {
    pub(super) fn new(
        core: Arc<FixedMemoryInitialControl>,
        context: SessionContext,
        operation: PreparedOperation,
        reservation: ReservePreparation,
    ) -> Arc<Self> {
        Arc::new(Self {
            core,
            reservation,
            state: Mutex::new(State {
                context: Some(context),
                operation: Some(operation),
                output: None,
                output_disposed: false,
                failed: false,
            }),
            stopped: AtomicBool::new(false),
            reserved: Mutex::new(None),
            began: Mutex::new(None),
            retained: Mutex::new(None),
            activated: Mutex::new(None),
            root_cleanup: Mutex::new(None),
            child_cleanup: Mutex::new(None),
            root: OnceLock::new(),
            grant: OnceLock::new(),
            activation: OnceLock::new(),
            activation_ack: OnceLock::new(),
            registration: tokio::sync::Mutex::new(None),
            joined: OnceLock::new(),
            holder: OnceLock::new(),
            inner_task: OnceLock::new(),
            drained: tokio::sync::watch::channel(false).0,
            #[cfg(test)]
            test: OnceLock::new(),
            #[cfg(test)]
            seal_presentation: OnceLock::new(),
        })
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub(super) fn request_stop(&self) {
        self.stopped.store(true, Ordering::Release);
    }
    fn entering(&self) -> Result<(), PreparationCommitError> {
        if self.stopped.load(Ordering::Acquire) {
            return Err(Error::InvalidTransition.into());
        }
        Ok(())
    }
    fn fail(&self) {
        self.lock().failed = true;
        self.request_stop();
    }
    pub(super) fn resources_pending(&self) -> bool {
        // An installed registration remains an actual owner until its real join.
        self.registration
            .try_lock()
            .map_or(true, |registration| registration.is_some())
            || !self.lock().output_disposed
    }
    pub(super) async fn join_original_resources(&self) {
        if self.inner_task.get().is_some() {
            let mut changed = self.drained.subscribe();
            while !*changed.borrow_and_update() {
                if changed.changed().await.is_err() {
                    return;
                }
            }
        } else {
            let _ = self.drain().await;
        }
    }
    pub(super) async fn cleanup_partial(&self) -> Result<(), PreparationCommitError> {
        // Once the genuine inner registration attaches, its existing supervisor
        // owns the loan and the atomic native child/Driver retirement.
        if self.inner_task.get().is_some() {
            return Ok(());
        }
        self.cleanup_local().await
    }
    async fn cleanup_local(&self) -> Result<(), PreparationCommitError> {
        self.request_stop();
        self.drain().await?;
        let mut proof = Proof::bounded(32 << 20);
        self.capture_cleanup_authorities(&mut proof)?;
        if let Some(grant) = self.grant.get()
            && let Some(authority) = grant.retained_resources_authority()
        {
            if self.activation.get().is_some() {
                // Native join_candidate authenticates exact Reserve/Begin,
                // the actual activation audit OR its genuine NoDispatch
                // absence under the same native gate. No replay permit is used.
                let joined = self.joined_scope(&mut proof)?;
                crate::native_enrollment::caller::measure_owner_authority(authority, &mut proof)
                    .map_err(|_| Error::Capacity)?;
                run::native(self, &self.child_cleanup, || {
                    self.core
                        .store()
                        .join_owned_scope(joined, authority.clone())
                })
                .await?;
                Self::cleanup_ack(&self.child_cleanup, &mut proof)?;
            } else {
                self.cancel(authority, &mut proof).await?;
            }
        }
        if let Some(root) = self.root.get() {
            self.cancel(root, &mut proof).await?;
        }
        Ok(())
    }
    fn capture_cleanup_authorities(&self, proof: &mut Proof) -> Result<(), PreparationCommitError> {
        if self.root.get().is_none() {
            let slot = self
                .reserved
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(Ok(SessionOwnerOutcome::Applied {
                authority: Some(authority),
                ..
            })) = slot.as_ref()
            {
                crate::native_enrollment::caller::measure_owner_authority(authority, proof)
                    .map_err(|_| Error::Capacity)?;
                self.root
                    .set(authority.clone())
                    .map_err(|_| Error::InvalidTransition)?;
            }
        }
        if self.grant.get().is_none() {
            let slot = self
                .retained
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(Ok(PreparationResourceOutcome::Applied { grant, .. })) = slot.as_ref() {
                run::measure_grant(grant, proof)?;
                self.grant
                    .set((**grant).clone())
                    .map_err(|_| Error::InvalidTransition)?;
            }
        }
        Ok(())
    }
    async fn cancel(
        &self,
        authority: &SessionOwnerAuthority,
        proof: &mut Proof,
    ) -> Result<(), PreparationCommitError> {
        use crate::session_owner::{
            MutateSessionOwner, SessionOwnerCommand, SessionOwnerTermination,
        };
        crate::native_enrollment::caller::measure_owner_authority(authority, proof)
            .map_err(|_| Error::Capacity)?;
        proof
            .measure(&self.reservation)
            .map_err(|_| Error::Capacity)?;
        let root = self
            .root
            .get()
            .is_some_and(|root| root.target() == authority.target());
        let id = if root {
            self.reservation.reconciliation()?.join_id
        } else {
            crate::session_owner::preparation::PreparationResourceLink::for_grant(
                self.grant.get().ok_or(Error::InvalidTransition)?,
            )?
            .identities()?
            .join
        };
        let command = SessionOwnerCommand::Join {
            termination: SessionOwnerTermination::Cancelled,
        };
        run::measure_mutation(&id, authority, &command, proof)?;
        let request = MutateSessionOwner {
            id,
            session_id: authority.session_id().clone(),
            target: authority.target().clone(),
            command,
        };
        let slot = if root {
            &self.root_cleanup
        } else {
            &self.child_cleanup
        };
        run::native(self, slot, || {
            self.core
                .store()
                .mutate_session_owner(request, Some(authority.clone()))
        })
        .await?;
        Self::cleanup_ack(slot, proof)?;
        Ok(())
    }
    fn cleanup_ack(
        slot: &Mutex<Option<Result<SessionOwnerOutcome, Error>>>,
        proof: &mut Proof,
    ) -> Result<(), PreparationCommitError> {
        let slot = slot
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let result = slot
            .as_ref()
            .ok_or(Error::UnknownStoreOutcome)?
            .as_ref()
            .map_err(|error| *error)?;
        let (SessionOwnerOutcome::Applied {
            receipt,
            authority: None,
        }
        | SessionOwnerOutcome::Replayed(receipt)) = result
        else {
            return Err(Error::UnknownStoreOutcome.into());
        };
        proof.measure(receipt).map_err(|_| Error::Capacity)?;
        Ok(())
    }
    pub(super) async fn drain(&self) -> Result<(), PreparationCommitError> {
        #[cfg(test)]
        if let Some(test) = self.test.get() {
            test.before_output_drain().await;
        }
        {
            let mut registration = self.registration.lock().await;
            if let Some(task) = registration.as_mut() {
                task.join().await?;
                // A stopped undispatched factory can still own its input in the
                // actual task allocation. Its real join releases that capture.
                if let Some(value) = task.take_value().await? {
                    let disposed =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(value)));
                    if let Err(payload) = disposed {
                        self.fail();
                        super::retain::discard(payload);
                    }
                }
                let joined = task.take_fixed_joined()?;
                self.joined
                    .set(joined)
                    .map_err(|_| Error::InvalidTransition)?;
                *registration = None;
            }
        }
        let (output, context, operation) = {
            let mut state = self.lock();
            (
                state.output.take(),
                state.context.take(),
                state.operation.take(),
            )
        };
        let disposed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            drop((output, context, operation));
        }));
        if let Err(payload) = disposed {
            self.fail();
            super::retain::discard(payload);
        }
        let mut state = self.lock();
        state.output_disposed = true;
        self.drained.send_replace(true);
        if state.failed {
            return Err(Error::UnknownStoreOutcome.into());
        }
        Ok(())
    }
    fn joined_scope(
        &self,
        proof: &mut Proof,
    ) -> Result<crate::session_owner::owned_scope::JoinedOwnedScope, PreparationCommitError> {
        crate::session_owner::owned_scope::JoinedOwnedScope::from_fixed_preparation(
            &OriginalPolicyRetirementView::partial(self),
            proof,
        )
    }
    fn observed_join(
        &self,
        proof: &mut Proof,
    ) -> Result<(&OwnedScopeActivation, super::OwnedMainDisposition), PreparationCommitError> {
        let state = self.lock();
        if !state.output_disposed || state.failed {
            return Err(Error::UnknownStoreOutcome.into());
        }
        let joined = self.joined.get().ok_or(Error::InvalidTransition)?;
        let activation = self.activation.get().ok_or(Error::InvalidTransition)?;
        proof
            .measure(&(activation, joined.disposition()))
            .map_err(|_| Error::Capacity)?;
        if joined.activation() != activation {
            return Err(Error::InvalidTransition.into());
        }
        Ok((activation, joined.disposition()))
    }
}

/// This loan is moved into the actual inner retained owner; no public constructor.
pub(in crate::session_owner::owned_scope::task::pending) struct FixedPreparationLoan {
    owner: Arc<FixedPreparationOwner>,
}
impl FixedPreparationLoan {
    pub(super) fn verify_intake(
        &self,
        core: &Arc<FixedMemoryInitialControl>,
        original: &JoinedPreparation,
        proof: &mut Proof,
    ) -> Result<(), PreparationCommitError> {
        if !Arc::ptr_eq(core, &self.owner.core) {
            return Err(Error::IdentityMismatch.into());
        }
        let grant = self.owner.grant.get().ok_or(Error::InvalidTransition)?;
        let child = grant
            .retained_resources_authority()
            .ok_or(Error::InvalidTransition)?;
        let observed = original.observed_grant();
        let observed_child = observed
            .retained_resources_authority()
            .ok_or(Error::InvalidTransition)?;
        crate::native_enrollment::caller::measure_owner_authority(child, proof)
            .map_err(|_| Error::Capacity)?;
        crate::native_enrollment::caller::measure_owner_authority(grant.owner_authority(), proof)
            .map_err(|_| Error::Capacity)?;
        proof
            .measure(
                self.owner
                    .activation
                    .get()
                    .ok_or(Error::InvalidTransition)?,
            )
            .map_err(|_| Error::Capacity)?;
        if !observed_child.authenticates(child.session_id(), child.target(), &child.token)
            || !observed.owner_authority().authenticates(
                grant.owner_authority().session_id(),
                grant.owner_authority().target(),
                &grant.owner_authority().token,
            )
            || original.observed_registration() != self.owner.activation.get()
            || self.owner.lock().failed
        {
            return Err(Error::IdentityMismatch.into());
        }
        Ok(())
    }

    pub(super) fn bind(
        &self,
        resources: &Arc<InitialResources>,
    ) -> Result<(), PreparationCommitError> {
        self.owner
            .holder
            .set(Arc::downgrade(resources))
            .map_err(|_| Error::InvalidTransition.into())
    }
    pub(super) fn registered(&self, task: tokio::task::Id) -> Result<(), Error> {
        self.owner
            .inner_task
            .set(task)
            .map_err(|_| Error::InvalidTransition)
    }
    pub(super) fn view<'a>(
        &'a self,
        association: &'a super::enrolled_initial::InitialResourceAssociation<'a>,
    ) -> OriginalPolicyView<'a> {
        OriginalPolicyView::new(&self.owner, association)
    }
    pub(super) async fn drain(&self) -> Result<(), PreparationCommitError> {
        self.owner.drain().await
    }
    pub(super) fn resources_pending(&self) -> bool {
        self.owner.resources_pending()
    }
    pub(super) async fn join_original_resources(&self) {
        self.owner.join_original_resources().await;
    }
    pub(super) async fn cleanup_partial(&self) -> Result<(), PreparationCommitError> {
        self.owner.cleanup_local().await
    }
    pub(super) async fn cleanup_unpublished_root(&self) -> Result<(), PreparationCommitError> {
        if let Some(root) = self.owner.root.get() {
            let mut proof = Proof::bounded(32 << 20);
            self.owner.cancel(root, &mut proof).await?;
        }
        Ok(())
    }
}
