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
    closing: tokio_util::sync::CancellationToken,
    pub(super) registration: super::native_dependency::Registration,
}
#[derive(Default)]
struct References {
    acquisitions: usize,
    sealed: bool,
}

impl Dependencies {
    pub(super) async fn register(
        &self,
        context: &SessionContext,
        generation: &ServiceGenerationDescriptor,
        services: &crate::SessionServiceDependencies,
    ) -> Result<(), ExtensionError> {
        let dependency = {
            let mut state = self.0.lock().unwrap();
            check_context(&state, context)?;
            dependency(&mut state, context, generation)?
        };
        dependency
            .registration
            .acquire(services, dependency.clone())
            .await
    }

    pub(super) fn generation_uncertain(&self, id: &ServiceGenerationId) -> bool {
        self.0
            .lock()
            .unwrap()
            .entries
            .values()
            .any(|entry| entry.generation.id() == id && entry.registration.is_uncertain())
    }

    pub(super) fn check_legacy(&self, context: &SessionContext) -> Result<(), ExtensionError> {
        if self.0.lock().unwrap().entries.values().any(|entry| {
            entry.context.session_id() == context.session_id() && entry.registration.is_registered()
        }) {
            return Err(ExtensionError::PreparationResourcesUnsupported);
        }
        Ok(())
    }
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
        factory: impl FnOnce() -> Result<RunPlan, ExtensionError>,
    ) -> Result<RunPlan, ExtensionError> {
        let mut state = self.0.lock().unwrap();
        check_context(&state, context)?;
        let mut dependencies = generations
            .into_iter()
            .map(|generation| dependency(&mut state, context, &generation))
            .collect::<Result<Vec<_>, ExtensionError>>()?;
        dependencies.sort_by(|a, b| a.generation.id().cmp(b.generation.id()));
        dependencies.dedup_by(|a, b| Arc::ptr_eq(a, b));
        // Direct native cancellation seals an entry without the map lock.
        // Retain the complete selected lock set through validation/reservation,
        // so it cannot observe sealed+zero before this acquisition is counted.
        let mut references = dependencies
            .iter()
            .map(|dependency| dependency.state.lock().unwrap())
            .collect::<Vec<_>>();
        for references in &references {
            if references.sealed {
                return Err(ExtensionError::ContextOwnerClosed);
            }
            if references.acquisitions == usize::MAX {
                return Err(ExtensionError::ContextCleanupIncomplete);
            }
        }
        for references in &mut references {
            references.acquisitions += 1;
        }
        drop(references);
        drop(state);
        // The factory can clone/expose mount resources only after reservation.
        // A panic deliberately retains these counts: no disposal was proven.
        let plan = match factory() {
            Ok(plan) => plan,
            Err(error) => {
                release(&dependencies);
                return Err(error);
            }
        };
        Ok(plan.after_lease_release(move || release(&dependencies)))
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
                dependency.seal();
                dependency.clone()
            })
            .collect()
    }
}

fn release(dependencies: &[Arc<Dependency>]) {
    for dependency in dependencies {
        let mut references = dependency.state.lock().unwrap();
        references.acquisitions -= 1;
        drop(references);
        dependency.changed.notify_waiters();
    }
}

fn dependency(
    state: &mut State,
    context: &SessionContext,
    generation: &ServiceGenerationDescriptor,
) -> Result<Arc<Dependency>, ExtensionError> {
    let dependency = state
        .entries
        .entry((context.session_id().clone(), generation.id().clone()))
        .or_insert_with(|| {
            Arc::new(Dependency {
                context: context.clone(),
                generation: generation.clone(),
                state: Mutex::new(References::default()),
                changed: Notify::new(),
                closing: tokio_util::sync::CancellationToken::new(),
                registration: super::native_dependency::Registration::default(),
            })
        })
        .clone();
    if &dependency.generation != generation {
        return Err(ExtensionError::ContextIdentityMismatch);
    }
    Ok(dependency)
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

impl Dependency {
    pub(super) fn generation(&self) -> &ServiceGenerationDescriptor {
        &self.generation
    }
    pub(super) fn seal(&self) {
        self.state.lock().unwrap().sealed = true;
        self.closing.cancel();
    }
    pub(super) fn is_sealed(&self) -> bool {
        self.state.lock().unwrap().sealed
    }
    pub(super) async fn closing(&self) {
        self.closing.cancelled().await;
    }
    pub(super) async fn wait_references(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let references = self.state.lock().unwrap();
                if references.sealed && references.acquisitions == 0 {
                    return;
                }
            }
            changed.await;
        }
    }
}

pub(super) async fn join(
    dependencies: &[Arc<Dependency>],
    bound: std::time::Duration,
) -> Result<(), ExtensionError> {
    let results = futures::future::join_all(dependencies.iter().map(|dependency| async {
        dependency.wait_references().await;
        dependency.registration.close_and_join(bound).await
    }))
    .await;
    results
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::super::{Owner, Slot, SlotState};
    use super::*;
    use crate::{ContextualPlanProvider, Registry};
    use std::{
        panic::AssertUnwindSafe,
        sync::atomic::{AtomicBool, Ordering},
        time::Duration,
    };

    struct Fixture {
        dependencies: Dependencies,
        context: SessionContext,
        generations: Vec<ServiceGenerationDescriptor>,
        entries: Vec<Arc<Dependency>>,
    }
    fn fixture() -> Fixture {
        let provider =
            ContextualPlanProvider::new(Registry::new(), vec![], vec![], Duration::from_secs(1))
                .unwrap();
        let slot = Slot {
            owner: Owner::Shared,
            context: None,
            gate: tokio::sync::Mutex::new(()),
            state: Mutex::new(SlotState::default()),
        };
        let generations = (0..2)
            .map(|_| provider.new_generation(&slot).unwrap())
            .collect::<Vec<_>>();
        let context = SessionContext::new(SessionId::new(), "workspace", "/workspace");
        let dependencies = Dependencies::default();
        let entries = {
            let mut state = dependencies.0.lock().unwrap();
            generations
                .iter()
                .map(|generation| dependency(&mut state, &context, generation).unwrap())
                .collect()
        };
        Fixture {
            dependencies,
            context,
            generations,
            entries,
        }
    }
    #[test]
    fn sealed_dependency_refuses_before_plan_factory_and_overflow_is_atomic() {
        let f = fixture();
        f.entries[1].seal();
        let invoked = AtomicBool::new(false);
        assert!(matches!(
            f.dependencies.track(&f.context, f.generations.clone(), || {
                invoked.store(true, Ordering::SeqCst);
                Registry::new().try_acquire_in(&f.context)
            }),
            Err(ExtensionError::ContextOwnerClosed)
        ));
        assert!(!invoked.load(Ordering::SeqCst));
        assert_eq!(f.entries[0].state.lock().unwrap().acquisitions, 0);
        let f = fixture();
        f.entries[1].state.lock().unwrap().acquisitions = usize::MAX;
        assert!(matches!(
            f.dependencies
                .track(&f.context, f.generations.clone(), || panic!(
                    "overflow must reject before factory"
                )),
            Err(ExtensionError::ContextCleanupIncomplete)
        ));
        assert_eq!(f.entries[0].state.lock().unwrap().acquisitions, 0);
    }
    #[test]
    fn duplicate_generation_is_reserved_and_released_once() {
        let f = fixture();
        let plan = f
            .dependencies
            .track(
                &f.context,
                vec![f.generations[0].clone(), f.generations[0].clone()],
                || {
                    assert_eq!(f.entries[0].state.lock().unwrap().acquisitions, 1);
                    Registry::new().try_acquire_in(&f.context)
                },
            )
            .unwrap();
        plan.release();
        assert_eq!(f.entries[0].state.lock().unwrap().acquisitions, 0);
        assert_eq!(f.entries[1].state.lock().unwrap().acquisitions, 0);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn complete_reference_set_is_reserved_before_factory_or_native_join() {
        let f = fixture();
        let entry = f.entries[0].clone();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let task_barrier = barrier.clone();
        let (sealed, observed) = std::sync::mpsc::channel();
        let joined = Arc::new(AtomicBool::new(false));
        let task_joined = joined.clone();
        let native = tokio::spawn(async move {
            task_barrier.wait();
            entry.seal();
            sealed.send(()).unwrap();
            entry.wait_references().await;
            task_joined.store(true, Ordering::SeqCst);
        });
        let plan = f
            .dependencies
            .track(&f.context, f.generations.clone(), || {
                for entry in &f.entries {
                    assert_eq!(entry.state.lock().unwrap().acquisitions, 1);
                }
                barrier.wait();
                observed.recv_timeout(Duration::from_secs(2)).unwrap();
                assert!(!joined.load(Ordering::SeqCst));
                Registry::new().try_acquire_in(&f.context)
            })
            .unwrap();
        assert!(!joined.load(Ordering::SeqCst));
        plan.release();
        tokio::time::timeout(Duration::from_secs(2), native)
            .await
            .unwrap()
            .unwrap();
        assert!(joined.load(Ordering::SeqCst));
        for entry in &f.entries {
            assert_eq!(entry.state.lock().unwrap().acquisitions, 0);
        }
    }
    #[test]
    fn factory_error_releases_reservation_and_lease_drop_precedes_reference_release() {
        let f = fixture();
        assert!(matches!(
            f.dependencies.track(&f.context, f.generations.clone(), || {
                for entry in &f.entries {
                    assert_eq!(entry.state.lock().unwrap().acquisitions, 1);
                }
                Err(ExtensionError::ContextIdentityMismatch)
            }),
            Err(ExtensionError::ContextIdentityMismatch)
        ));
        for entry in &f.entries {
            assert_eq!(entry.state.lock().unwrap().acquisitions, 0);
        }
        let entry = f.entries[0].clone();
        let observed = Arc::new(Mutex::new(None));
        let dropped = observed.clone();
        let plan = f
            .dependencies
            .track(&f.context, f.generations.clone(), || {
                Registry::new().try_acquire_in(&f.context).map(|plan| {
                    plan.after_lease_release(move || {
                        *dropped.lock().unwrap() = Some(entry.state.lock().unwrap().acquisitions);
                    })
                })
            })
            .unwrap();
        plan.release();
        assert_eq!(*observed.lock().unwrap(), Some(1));
        for entry in &f.entries {
            assert_eq!(entry.state.lock().unwrap().acquisitions, 0);
        }
    }
    #[test]
    fn panicking_factory_keeps_unproven_reservations_without_poisoning_owner_locks() {
        let f = fixture();
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            f.dependencies.track(&f.context, f.generations.clone(), || {
                panic!("factory disposal unknown")
            })
        }));
        assert!(result.is_err());
        assert!(f.dependencies.0.lock().is_ok());
        for entry in &f.entries {
            assert_eq!(entry.state.lock().unwrap().acquisitions, 1);
        }
    }
}
