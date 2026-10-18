//! Publish exactly one retained initializer while holding the owner gate.
use super::{ContextualPlanProvider, ExtensionError, Initialization, Owner, Slot, SlotState};
use crabber_core::SessionContext;
use futures::FutureExt;
use std::{
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
};
use tokio::sync::Mutex as AsyncMutex;

impl ContextualPlanProvider {
    pub(super) fn select_owner(
        &self,
        owner: Owner,
        context: Option<&SessionContext>,
        binding: Option<super::native_dependency::Binding>,
    ) -> Result<(Arc<Slot>, Option<Arc<Initialization>>), ExtensionError> {
        let mut owners = self.inner.owners.lock().unwrap();
        self.check_open()?;
        if let Some(context) = &context {
            owners.validate(context)?;
        }
        let slot = owners
            .slots
            .entry(owner.clone())
            .or_insert_with(|| {
                Arc::new(Slot {
                    owner: owner.clone(),
                    context: context.cloned(),
                    gate: AsyncMutex::new(()),
                    state: Mutex::new(SlotState::default()),
                })
            })
            .clone();
        if matches!(owner, Owner::Session(_)) && slot.context.as_ref() != context {
            return Err(ExtensionError::ContextIdentityMismatch);
        }
        let mut state = slot.state.lock().unwrap();
        if state.closed {
            return Err(ExtensionError::ContextOwnerClosed);
        }
        let initialization = if state.ready && state.initialization.is_none() {
            None
        } else if let Some(initialization) = &state.initialization {
            Some(initialization.clone())
        } else {
            let initialization = Arc::new(Initialization::default());
            state.initialization = Some(initialization.clone());
            let provider = self.clone();
            let identity = provider.inner.initializer_identity.clone();
            let task_slot = slot.clone();
            let ticket = initialization.clone();
            // Retained before publication: close cannot miss the actual task.
            let task = self.inner.initializers.tracker().spawn(async move {
                let result = super::scoped_initializer(identity, owner, async {
                    AssertUnwindSafe(provider.initialize_slot(task_slot, binding))
                        .catch_unwind()
                        .await
                        .unwrap_or_else(|payload| {
                            crate::dispatch::discard_panic_payload(payload);
                            Err(ExtensionError::ContextInitializationFailed {
                                component: "contextual-profile".into(),
                            })
                        })
                })
                .await;
                *ticket.result.lock().unwrap() = Some(result);
                #[cfg(test)]
                {
                    let tail = ticket.tail.lock().unwrap().take();
                    if let Some(tail) = tail {
                        let _ = tail.await;
                    }
                }
            });
            initialization.retain(task);
            Some(initialization)
        };
        drop(state);
        Ok((slot, initialization))
    }
}
