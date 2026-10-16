//! An actual Contextual factory member, never a reconstructible metadata claim.
use super::{
    ContextualPlanProvider, Inner, ServiceGenerationDescriptor, ServiceGenerationScope, Slot,
};
use crate::{ExtensionError, SessionServiceDependencies};
use crabber_core::SessionContext;
use std::sync::{Arc, Weak};

/// Borrow-only source minted by an actual factory context. Cloning it pins no
/// acquisition or physical generation; native and local gates are rechecked.
#[derive(Clone)]
pub struct ContextualServiceBorrowSource {
    owner: Weak<Inner>,
    slot: Weak<Slot>,
    generation: ServiceGenerationDescriptor,
    record: usize,
}
impl ContextualServiceBorrowSource {
    pub(super) fn new(
        provider: &ContextualPlanProvider,
        slot: &Arc<Slot>,
        generation: ServiceGenerationDescriptor,
        record: usize,
    ) -> Self {
        Self {
            owner: Arc::downgrade(&provider.inner),
            slot: Arc::downgrade(slot),
            generation,
            record,
        }
    }
    /// Obtain real member and dependency leases of this ready original producer.
    /// This cannot initialize services or certify physical generation cleanup.
    /// # Errors
    /// Refuses context drift, closed/replaced members and uncertain registration.
    /// # Panics
    /// Panics if an earlier internal failure poisoned an ownership lock.
    pub async fn borrow_in(
        &self,
        context: &SessionContext,
        services: &SessionServiceDependencies,
    ) -> Result<ContextualServiceBorrowLease, ExtensionError> {
        let owner = self
            .owner
            .upgrade()
            .ok_or(ExtensionError::ContextOwnerClosed)?;
        let slot = self
            .slot
            .upgrade()
            .ok_or(ExtensionError::ContextOwnerClosed)?;
        match self.generation.scope() {
            ServiceGenerationScope::Workspace {
                workspace_id,
                directory,
            } if workspace_id == context.workspace_id() && directory == context.directory() => {}
            ServiceGenerationScope::Session {
                session_id,
                workspace_id,
                directory,
            } if session_id == context.session_id()
                && workspace_id == context.workspace_id()
                && directory == context.directory() => {}
            _ => return Err(ExtensionError::ContextIdentityMismatch),
        }
        let provider = ContextualPlanProvider {
            inner: owner.clone(),
        };
        let dependency = {
            let mut owners = owner.owners.lock().unwrap();
            provider.check_open()?;
            owners.validate(context)?;
            self.check_member(&slot.state.lock().unwrap())?;
            owner
                .dependencies
                .prepare_registration(context, &self.generation)?
        };
        dependency
            .registration
            .acquire(services, dependency.clone())
            .await?;
        let mut owners = owner.owners.lock().unwrap();
        provider.check_open()?;
        owners.validate(context)?;
        let state = slot.state.lock().unwrap();
        let mount = self.check_member(&state)?;
        let reference =
            owner
                .dependencies
                .track_member(context, self.generation.clone(), || mount.borrow_member())?;
        Ok(ContextualServiceBorrowLease {
            reference: Some(reference),
            owner: Some(owner.clone()),
        })
    }
    fn check_member<'a>(
        &self,
        state: &'a super::SlotState,
    ) -> Result<&'a crate::OwnedMountHandle, ExtensionError> {
        if state.closed || !state.ready || state.generation.as_ref() != Some(&self.generation) {
            return Err(ExtensionError::ContextOwnerClosed);
        }
        let member = state
            .records
            .get(self.record)
            .ok_or(ExtensionError::ContextOwnerClosed)?;
        let Some(super::cleanup::RecordMount::Owned(mount)) = &member.mount else {
            return Err(ExtensionError::PreparationResourcesUnsupported);
        };
        Ok(mount)
    }
}
impl crabber_providers::ProviderServiceSource for ContextualServiceBorrowSource {}
/// Real counted member references. This is not a physical or native Join proof.
pub struct ContextualServiceBorrowLease {
    reference: Option<super::dependencies::MemberReference>,
    owner: Option<Arc<Inner>>,
}

impl Drop for ContextualServiceBorrowLease {
    fn drop(&mut self) {
        let reference = self.reference.take();
        if let Err(payload) =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(self.owner.take())))
        {
            // Failed original disposal cannot acknowledge this dependency.
            std::mem::forget(reference);
            std::panic::resume_unwind(payload);
        }
        drop(reference);
    }
}
