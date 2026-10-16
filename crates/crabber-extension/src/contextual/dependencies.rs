//! Actual local acquisition references. Neither an empty count nor release of
//! these references certifies shutdown of a shared physical generation.
use super::{ServiceGenerationDescriptor, ServiceGenerationId};
use crate::{ExtensionError, RunPlan};
use crabber_core::{SessionContext, SessionId};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio::sync::Notify;

#[derive(Default)]
pub(super) struct Dependencies(Mutex<State>);
#[derive(Default)]
struct State {
    contexts: BTreeMap<SessionId, SessionContext>,
    entries: BTreeMap<(SessionId, ServiceGenerationId), Arc<Dependency>>,
}
pub(super) struct Dependency {
    context: SessionContext,
    generation: ServiceGenerationDescriptor,
    state: Mutex<References>,
    changed: Notify,
}
#[derive(Default)]
struct References {
    acquisitions: usize,
    sealed: bool,
}

impl Dependencies {
    pub(super) fn check_context(&self, context: &SessionContext) -> Result<(), ExtensionError> {
        check_context(&self.0.lock().unwrap(), context)
    }

    // Called under the owners lock before initialization can have effects.
    pub(super) fn bind_context(&self, context: &SessionContext) -> Result<(), ExtensionError> {
        let mut state = self.0.lock().unwrap();
        check_context(&state, context)?;
        state
            .contexts
            .entry(context.session_id().clone())
            .or_insert_with(|| context.clone());
        Ok(())
    }

    pub(super) fn track(
        &self,
        context: &SessionContext,
        generations: Vec<ServiceGenerationDescriptor>,
        plan: RunPlan,
    ) -> Result<RunPlan, ExtensionError> {
        let mut state = self.0.lock().unwrap();
        check_context(&state, context)?;
        let dependencies = generations
            .into_iter()
            .map(|generation| {
                let dependency = state
                    .entries
                    .entry((context.session_id().clone(), generation.id().clone()))
                    .or_insert_with(|| {
                        Arc::new(Dependency {
                            context: context.clone(),
                            generation: generation.clone(),
                            state: Mutex::new(References::default()),
                            changed: Notify::new(),
                        })
                    })
                    .clone();
                if dependency.generation != generation {
                    return Err(ExtensionError::ContextIdentityMismatch);
                }
                Ok(dependency)
            })
            .collect::<Result<Vec<_>, ExtensionError>>()?;
        // Validate the complete set before changing any acquisition count.
        for dependency in &dependencies {
            let references = dependency.state.lock().unwrap();
            if references.sealed {
                return Err(ExtensionError::ContextOwnerClosed);
            }
            if references.acquisitions == usize::MAX {
                return Err(ExtensionError::ContextCleanupIncomplete);
            }
        }
        for dependency in &dependencies {
            dependency.state.lock().unwrap().acquisitions += 1;
        }
        Ok(plan.after_lease_release(move || {
            for dependency in dependencies {
                let mut references = dependency.state.lock().unwrap();
                references.acquisitions -= 1;
                drop(references);
                dependency.changed.notify_waiters();
            }
        }))
    }

    pub(super) fn seal_context(
        &self,
        context: &SessionContext,
        workspace: bool,
    ) -> Vec<Arc<Dependency>> {
        self.seal(|dependency| {
            if workspace {
                dependency.context.workspace_id() == context.workspace_id()
            } else {
                dependency.context.session_id() == context.session_id()
            }
        })
    }

    pub(super) fn seal_all(&self) -> Vec<Arc<Dependency>> {
        self.seal(|_| true)
    }

    fn seal(&self, select: impl Fn(&Dependency) -> bool) -> Vec<Arc<Dependency>> {
        self.0
            .lock()
            .unwrap()
            .entries
            .values()
            .filter(|dependency| select(dependency))
            .map(|dependency| {
                dependency.state.lock().unwrap().sealed = true;
                dependency.clone()
            })
            .collect()
    }
}

fn check_context(state: &State, context: &SessionContext) -> Result<(), ExtensionError> {
    if state
        .contexts
        .get(context.session_id())
        .is_some_and(|stored| stored != context)
    {
        return Err(ExtensionError::ContextIdentityMismatch);
    }
    Ok(())
}

pub(super) async fn join(dependencies: &[Arc<Dependency>]) {
    futures::future::join_all(dependencies.iter().map(|dependency| async {
        loop {
            let changed = dependency.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let references = dependency.state.lock().unwrap();
                if references.sealed && references.acquisitions == 0 {
                    return;
                }
            }
            changed.await;
        }
    }))
    .await;
}
