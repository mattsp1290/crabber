mod contextual_support;
use async_trait::async_trait;
use contextual_support::*;
use crabber_core::SessionContext;
use crabber_extension::*;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
    time::Duration,
};
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;

type Main = tokio::task::JoinHandle<Result<(), ResourceFailure>>;
#[derive(Default)]
struct Control {
    cancel: CancellationToken,
    resources: ToolResourceOwner,
    pending: Mutex<Option<SessionServiceDependencyTask>>,
    main: AsyncMutex<Option<Main>>,
    uncertain: AtomicBool,
    joined: AtomicBool,
    failed: AtomicBool,
}
impl Control {
    async fn activate(&self) {
        let task = self.pending.lock().unwrap().take();
        if let Some(task) = task {
            let context = SessionServiceDependencyContext {
                cancel: self.cancel.clone(),
                cleanup: self.resources.cleanup_tracker(),
            };
            *self.main.lock().await = Some(tokio::spawn(task(context)));
        }
        self.uncertain.store(false, Ordering::SeqCst);
    }
}
#[async_trait]
impl SessionServiceDependencyControl for Control {
    async fn close_and_join(
        &self,
        bound: Duration,
    ) -> Result<ResourceDrain, SessionServiceDependencyError> {
        if self.uncertain.load(Ordering::SeqCst) {
            return Err(SessionServiceDependencyError::UnknownOutcome);
        }
        self.cancel.cancel();
        self.resources.close();
        let mut slot = self.main.lock().await;
        if let Some(main) = slot.as_mut() {
            let result = tokio::time::timeout(bound, main)
                .await
                .map_err(|_| SessionServiceDependencyError::Incomplete)?;
            if !matches!(result, Ok(Ok(()))) {
                self.failed.store(true, Ordering::SeqCst);
            }
            *slot = None;
        }
        if self.failed.load(Ordering::SeqCst) {
            return Err(SessionServiceDependencyError::Incomplete);
        }
        let report = self.resources.drain(bound).await;
        if report.pending != 0 || !report.failures.is_empty() {
            return Err(SessionServiceDependencyError::Incomplete);
        }
        self.joined.store(true, Ordering::SeqCst);
        Ok(report)
    }
}
#[derive(Default)]
struct Host {
    // 0: grant, 1: uncertain with original control, 2: error without control,
    // 3: metadata-only replay. These exercise the trusted SPI, not native proof.
    mode: AtomicU8,
    entries: AsyncMutex<BTreeMap<ServiceGenerationId, Arc<Control>>>,
    seen: Mutex<Vec<ServiceGenerationDescriptor>>,
    blocked: tokio::sync::Notify,
    acknowledge: tokio::sync::Notify,
}
#[async_trait]
impl SessionServiceRegistrar for Host {
    async fn register(
        &self,
        generation: ServiceGenerationDescriptor,
        task: SessionServiceDependencyTask,
    ) -> Result<SessionServiceDependencyAdmission, SessionServiceDependencyError> {
        self.seen.lock().unwrap().push(generation.clone());
        let mode = self.mode.load(Ordering::SeqCst);
        if mode == 2 {
            return Err(SessionServiceDependencyError::UnknownOutcome);
        }
        if mode == 3 {
            return Ok(SessionServiceDependencyAdmission::Replayed);
        }
        let mut entries = self.entries.lock().await;
        let control = entries
            .entry(generation.id().clone())
            .or_insert_with(|| {
                Arc::new(Control {
                    pending: Mutex::new(Some(task)),
                    uncertain: AtomicBool::new(mode == 1),
                    ..Control::default()
                })
            })
            .clone();
        if control.joined.load(Ordering::SeqCst) {
            return Ok(SessionServiceDependencyAdmission::Replayed);
        }
        if mode == 1 {
            return Ok(SessionServiceDependencyAdmission::Uncertain(
                SessionServiceDependencyHandle::from_host(control),
            ));
        }
        control.activate().await;
        if mode == 4 {
            self.blocked.notify_one();
            self.acknowledge.notified().await;
        }
        Ok(SessionServiceDependencyAdmission::Acquired(
            SessionServiceDependencyHandle::from_host(control),
        ))
    }
}

fn preparation(
    context: &SessionContext,
    host: Arc<Host>,
) -> (PreparationResourceContext, ToolResourceOwner) {
    let owner = ToolResourceOwner::new();
    let context = PreparationResourceContext::from_host(context.clone(), owner.resources())
        .unwrap()
        .with_service_dependencies(SessionServiceDependencies::from_host(host));
    (context, owner)
}
fn configured(observed: Arc<Observed>) -> ContextualPlanProvider {
    provider(vec![Arc::new(Factory {
        name: "workspace_counter",
        scope: FactoryScope::Workspace,
        observed,
    })])
}

#[tokio::test]
async fn owned_dependency_release_joins_only_the_closing_sessions_actual_tasks() {
    let observed = Arc::new(Observed::default());
    let provider = configured(observed.clone());
    let a = context("a", "workspace");
    let b = context("b", "workspace");
    let host_a = Arc::new(Host::default());
    let host_b = Arc::new(Host::default());
    let (scope_a, owner_a) = preparation(&a, host_a.clone());
    let (scope_b, owner_b) = preparation(&b, host_b.clone());
    let plan_a = provider
        .acquire_plan_in_preparation(&scope_a)
        .await
        .unwrap();
    let plan_b = provider
        .acquire_plan_in_preparation(&scope_b)
        .await
        .unwrap();
    assert!(
        provider
            .preparation_resource_contract(&a)
            .unwrap()
            .is_none()
    );
    assert_eq!(observed.created.load(Ordering::SeqCst), 1);
    assert_eq!(host_a.entries.lock().await.len(), 2);
    assert_eq!(host_b.entries.lock().await.len(), 2);
    assert!(provider.close_session(&a, Duration::ZERO).await.is_err());
    plan_a.release();
    provider
        .close_session(&a, Duration::from_secs(1))
        .await
        .unwrap();
    assert!(
        host_a
            .entries
            .lock()
            .await
            .values()
            .all(|entry| entry.joined.load(Ordering::SeqCst))
    );
    assert!(
        host_b
            .entries
            .lock()
            .await
            .values()
            .all(|entry| !entry.joined.load(Ordering::SeqCst))
    );
    assert_eq!(count(&plan_b, "workspace_counter").await, 1);
    assert_eq!(observed.shutdown.lock().unwrap().len(), 0);
    assert_eq!(owner_a.drain(Duration::from_secs(1)).await.pending, 0);
    plan_b.release();
    assert_eq!(owner_b.drain(Duration::from_secs(1)).await.pending, 0);
    provider.close_all(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn uncertain_registration_keeps_generation_and_requires_original_live_control() {
    let observed = Arc::new(Observed::default());
    let provider = configured(observed.clone());
    let context = context("a", "workspace");
    let host = Arc::new(Host {
        mode: AtomicU8::new(1),
        ..Host::default()
    });
    for _ in 0..2 {
        let (scope, owner) = preparation(&context, host.clone());
        assert!(matches!(
            provider.acquire_plan_in_preparation(&scope).await,
            Err(ExtensionError::SessionServiceDependency(
                SessionServiceDependencyError::UnknownOutcome
            ))
        ));
        assert_eq!(owner.drain(Duration::from_secs(1)).await.pending, 0);
    }
    assert_eq!(observed.created.load(Ordering::SeqCst), 0);
    let seen = host.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0], seen[1]);
    assert!(matches!(
        provider.acquire_plan_in(&context).await,
        Err(ExtensionError::PreparationResourcesUnsupported)
    ));
    host.mode.store(0, Ordering::SeqCst);
    let (scope, owner) = preparation(&context, host.clone());
    let plan = provider.acquire_plan_in_preparation(&scope).await.unwrap();
    assert_eq!(observed.created.load(Ordering::SeqCst), 1);
    assert_eq!(host.seen.lock().unwrap()[2], seen[0]);
    plan.release();
    owner.drain(Duration::from_secs(1)).await;
    provider.close_all(Duration::from_secs(1)).await.unwrap();
}

#[tokio::test]
async fn zero_acquisitions_and_metadata_do_not_erase_unknown_registration() {
    for mode in [1, 2, 3] {
        let observed = Arc::new(Observed::default());
        let provider = configured(observed.clone());
        let context = context("a", "workspace");
        let host = Arc::new(Host {
            mode: AtomicU8::new(mode),
            ..Host::default()
        });
        let (scope, owner) = preparation(&context, host.clone());
        assert!(provider.acquire_plan_in_preparation(&scope).await.is_err());
        assert_eq!(observed.created.load(Ordering::SeqCst), 0);
        assert_eq!(owner.drain(Duration::from_secs(1)).await.pending, 0);
        for _ in 0..2 {
            assert!(matches!(
                provider
                    .close_session(&context, Duration::from_secs(1))
                    .await,
                Err(ExtensionError::SessionServiceDependency(
                    SessionServiceDependencyError::UnknownOutcome
                ))
            ));
        }
        assert!(provider.close_all(Duration::from_secs(1)).await.is_err());
        // Release the mock's undispatched task after checking retained unknown
        // ownership. This does not acknowledge its simulated durable outcome.
        for entry in host.entries.lock().await.values() {
            entry.pending.lock().unwrap().take();
        }
    }
}

#[tokio::test]
async fn dropped_ready_generation_waiter_retains_registration_until_actual_ack_and_join() {
    let provider = configured(Arc::new(Observed::default()));
    provider.ready().await.unwrap();
    let context = context("a", "workspace");
    let host = Arc::new(Host {
        mode: AtomicU8::new(4),
        ..Host::default()
    });
    let (scope, owner) = preparation(&context, host.clone());
    let waiter = tokio::spawn({
        let provider = provider.clone();
        async move { provider.acquire_plan_in_preparation(&scope).await }
    });
    host.blocked.notified().await;
    waiter.abort();
    let Err(error) = waiter.await else {
        panic!("waiter must cancel")
    };
    assert!(error.is_cancelled());
    assert!(
        provider
            .close_session(&context, Duration::from_millis(10))
            .await
            .is_err()
    );
    host.acknowledge.notify_one();
    provider
        .close_session(&context, Duration::from_secs(1))
        .await
        .unwrap();
    assert!(
        host.entries
            .lock()
            .await
            .values()
            .all(|entry| entry.joined.load(Ordering::SeqCst))
    );
    assert_eq!(owner.drain(Duration::from_secs(1)).await.pending, 0);
    provider.close_all(Duration::from_secs(1)).await.unwrap();
}

#[path = "contextual_service_dependencies/provider_borrows.rs"]
mod provider_borrows;
