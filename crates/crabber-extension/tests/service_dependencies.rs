//! Public registration refuses execution before a fresh dependency grant.
use async_trait::async_trait;
use crabber_extension::{
    ContextualPlanProvider, Registry, ServiceGenerationDescriptor, SessionServiceDependencies,
    SessionServiceDependencyAdmission as Admission, SessionServiceDependencyError as Error,
    SessionServiceDependencyTask, SessionServiceRegistrar,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

async fn generation() -> ServiceGenerationDescriptor {
    let provider = ContextualPlanProvider::new(
        Registry::new(),
        Vec::new(),
        Vec::new(),
        Duration::from_secs(1),
    )
    .unwrap();
    provider.ready().await.unwrap();
    provider.service_generations().pop().unwrap()
}

#[tokio::test]
async fn standalone_dependency_refuses_before_factory() {
    let effects = Arc::new(AtomicUsize::new(0));
    let task_effects = effects.clone();
    let result = SessionServiceDependencies::default()
        .register(generation().await, move |_| {
            task_effects.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        })
        .await;
    assert!(matches!(result, Err(Error::Unsupported)));
    assert_eq!(effects.load(Ordering::SeqCst), 0);
}

struct MetadataHost {
    generation: ServiceGenerationDescriptor,
    cancelled: bool,
    calls: AtomicUsize,
}
#[async_trait]
impl SessionServiceRegistrar for MetadataHost {
    async fn register(
        &self,
        generation: ServiceGenerationDescriptor,
        _task: SessionServiceDependencyTask,
    ) -> Result<Admission, Error> {
        assert_eq!(generation, self.generation);
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(if self.cancelled {
            Admission::Cancelled
        } else {
            Admission::Replayed
        })
    }
}

#[tokio::test]
async fn metadata_replay_and_cancel_never_invoke_supplied_factory() {
    let generation = generation().await;
    for cancelled in [false, true] {
        let host = Arc::new(MetadataHost {
            generation: generation.clone(),
            cancelled,
            calls: AtomicUsize::new(0),
        });
        let effects = Arc::new(AtomicUsize::new(0));
        let task_effects = effects.clone();
        let dependencies = SessionServiceDependencies::from_host(host.clone());
        let outcome = dependencies
            .register(generation.clone(), move |_| {
                task_effects.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            })
            .await
            .unwrap();
        assert_eq!(matches!(outcome, Admission::Cancelled), cancelled);
        assert_eq!(matches!(outcome, Admission::Replayed), !cancelled);
        assert_eq!(host.calls.load(Ordering::SeqCst), 1);
        assert_eq!(effects.load(Ordering::SeqCst), 0);
    }
}
