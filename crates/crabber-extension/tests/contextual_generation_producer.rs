//! Local scheduling of real constructors, without native or physical proof.
use crabber_core::{SessionContext, SessionId};
use crabber_extension::*;
use futures::future::BoxFuture;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::{Notify, Semaphore};

#[derive(Clone, Copy)]
enum Mode {
    Admit,
    Refuse,
    Replay,
    Uncertain,
    ConstructPanic,
    PollPanic,
    DisposePanic,
    CloseRefuse,
}
struct Host {
    mode: Mode,
    calls: AtomicUsize,
    entered: Notify,
    release: Arc<Semaphore>,
    generations: Mutex<Vec<ServiceGenerationDescriptor>>,
    observations: Mutex<Vec<ContextualGenerationObservation>>,
}
impl Host {
    fn new(mode: Mode, held: bool) -> Arc<Self> {
        Arc::new(Self {
            mode,
            calls: AtomicUsize::new(0),
            entered: Notify::new(),
            release: Arc::new(Semaphore::new(usize::from(!held))),
            generations: Mutex::new(vec![]),
            observations: Mutex::new(vec![]),
        })
    }
}
impl ContextualGenerationHost for Host {
    fn register(
        &self,
        work: ContextualGenerationWork,
    ) -> BoxFuture<'static, ContextualGenerationAdmission> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.observations
            .lock()
            .unwrap()
            .push(work.observation().unwrap());
        assert_eq!(work.members().len(), 1);
        assert_eq!(work.members()[0].identity().id, "local-producer-fixture");
        self.generations
            .lock()
            .unwrap()
            .push(work.generation().clone());
        self.entered.notify_one();
        if matches!(self.mode, Mode::ConstructPanic) {
            panic!("host future construction panic");
        }
        let mode = self.mode;
        let release = self.release.clone();
        let future = Box::pin(async move {
            if matches!(mode, Mode::CloseRefuse) {
                work.closing().await;
                return ContextualGenerationAdmission::Refused;
            }
            release.acquire().await.unwrap().forget();
            match mode {
                Mode::Admit | Mode::DisposePanic => ContextualGenerationAdmission::Admitted(work),
                Mode::Refuse => ContextualGenerationAdmission::Refused,
                Mode::Replay => ContextualGenerationAdmission::Replayed,
                Mode::Uncertain => ContextualGenerationAdmission::Uncertain,
                Mode::PollPanic => std::panic::panic_any(PanicPayload),
                Mode::ConstructPanic | Mode::CloseRefuse => unreachable!(),
            }
        });
        if matches!(mode, Mode::DisposePanic) {
            Box::pin(DisposePanic(future))
        } else {
            future
        }
    }
}
struct PanicPayload;
impl Drop for PanicPayload {
    fn drop(&mut self) {
        panic!("panic payload destructor");
    }
}
struct DisposePanic(BoxFuture<'static, ContextualGenerationAdmission>);
impl Future for DisposePanic {
    type Output = ContextualGenerationAdmission;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}
impl Drop for DisposePanic {
    fn drop(&mut self) {
        panic!("host future destructor panic");
    }
}

struct Seen {
    created: AtomicUsize,
    shutdown: AtomicUsize,
    stopped: AtomicUsize,
    entered: Notify,
    create_release: Semaphore,
    cleanup_entered: Notify,
    cleanup_release: Semaphore,
    held_create: bool,
    held_cleanup: bool,
    panic_create: std::sync::atomic::AtomicBool,
    fail_cleanup: std::sync::atomic::AtomicBool,
}
impl Seen {
    fn new(held_create: bool, held_cleanup: bool, panic_create: bool) -> Arc<Self> {
        Arc::new(Self {
            created: AtomicUsize::new(0),
            shutdown: AtomicUsize::new(0),
            stopped: AtomicUsize::new(0),
            entered: Notify::new(),
            create_release: Semaphore::new(0),
            cleanup_entered: Notify::new(),
            cleanup_release: Semaphore::new(0),
            held_create,
            held_cleanup,
            panic_create: std::sync::atomic::AtomicBool::new(panic_create),
            fail_cleanup: std::sync::atomic::AtomicBool::new(false),
        })
    }
}
struct Factory {
    seen: Arc<Seen>,
    scope: FactoryScope,
}
#[async_trait::async_trait]
impl ExtensionFactory for Factory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "local-producer-fixture".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "fixture.v1".into()
    }
    fn scope(&self) -> FactoryScope {
        self.scope
    }
    async fn create(&self, context: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError> {
        self.seen.created.fetch_add(1, Ordering::SeqCst);
        let closing = context.cleanup().closing();
        let seen = self.seen.clone();
        context
            .cleanup()
            .spawn_checked(async move {
                closing.await;
                seen.cleanup_entered.notify_one();
                if seen.held_cleanup {
                    seen.cleanup_release.acquire().await.unwrap().forget();
                }
                seen.stopped.fetch_add(1, Ordering::SeqCst);
                if seen.fail_cleanup.load(Ordering::SeqCst) {
                    Err(ResourceFailure::CleanupFailed)
                } else {
                    Ok(())
                }
            })
            .unwrap();
        self.seen.entered.notify_one();
        if self.seen.held_create {
            self.seen.create_release.acquire().await.unwrap().forget();
        }
        assert!(
            !self.seen.panic_create.load(Ordering::SeqCst),
            "constructor panic after real cleanup registration"
        );
        Ok(Arc::new(Instance(self.seen.clone())))
    }
}
struct Instance(Arc<Seen>);
#[async_trait::async_trait]
impl Extension for Instance {
    fn id(&self) -> &'static str {
        "local-producer-fixture"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "fixture.v1".into()
    }
    async fn install(&self, _: &mut Registrar) -> Result<(), ExtensionError> {
        Ok(())
    }
    async fn shutdown(&self) {
        self.0.shutdown.fetch_add(1, Ordering::SeqCst);
    }
}
fn context(session: &str) -> SessionContext {
    SessionContext::new(SessionId(session.into()), "workspace", "/workspace")
}
fn provider(host: Arc<Host>, seen: Arc<Seen>, scope: FactoryScope) -> ContextualPlanProvider {
    ContextualPlanProvider::new_with_generation_host(
        Registry::new(),
        vec![],
        vec![Arc::new(Factory { seen, scope })],
        Duration::from_secs(3),
        host,
    )
    .unwrap()
}
async fn wait(signal: &Notify) {
    tokio::time::timeout(Duration::from_secs(3), signal.notified())
        .await
        .unwrap();
}

#[tokio::test]
async fn caller_drop_retains_host_call_and_actual_constructor_until_close_joins() {
    for scope in [FactoryScope::Workspace, FactoryScope::Session] {
        let host = Host::new(Mode::Admit, true);
        let seen = Seen::new(true, false, false);
        let provider = provider(host.clone(), seen.clone(), scope);
        let acquire = tokio::spawn({
            let provider = provider.clone();
            async move { provider.acquire_plan_in(&context("a")).await }
        });
        wait(&host.entered).await;
        assert_eq!(seen.created.load(Ordering::SeqCst), 0);
        acquire.abort();
        assert!(matches!(acquire.await, Err(error) if error.is_cancelled()));
        host.release.add_permits(1);
        wait(&seen.entered).await;
        assert_eq!(host.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            provider.close_all(Duration::from_millis(10)).await,
            Err(ExtensionError::ContextCleanupIncomplete)
        );
        assert_eq!(seen.created.load(Ordering::SeqCst), 1);
        seen.create_release.add_permits(1);
        provider.close_all(Duration::from_secs(3)).await.unwrap();
        assert_eq!(seen.stopped.load(Ordering::SeqCst), 1);
        assert_eq!(seen.shutdown.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn closed_owner_refuses_late_local_admission_without_factory() {
    let host = Host::new(Mode::Admit, true);
    let seen = Seen::new(false, false, false);
    let provider = provider(host.clone(), seen.clone(), FactoryScope::Session);
    let acquire = tokio::spawn({
        let provider = provider.clone();
        async move { provider.acquire_plan_in(&context("a")).await }
    });
    wait(&host.entered).await;
    assert_eq!(
        provider.close_all(Duration::from_millis(10)).await,
        Err(ExtensionError::ContextCleanupIncomplete)
    );
    host.release.add_permits(1);
    assert!(acquire.await.unwrap().is_err());
    provider.close_all(Duration::from_secs(3)).await.unwrap();
    assert_eq!(seen.created.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn metadata_and_unknown_outcomes_keep_original_generation_without_factory_retry() {
    for mode in [
        Mode::Refuse,
        Mode::Replay,
        Mode::Uncertain,
        Mode::ConstructPanic,
        Mode::PollPanic,
        Mode::DisposePanic,
    ] {
        let host = Host::new(mode, false);
        let seen = Seen::new(false, false, false);
        let provider = provider(host.clone(), seen.clone(), FactoryScope::Workspace);
        assert!(provider.acquire_plan_in(&context("a")).await.is_err());
        let original = provider.service_generations();
        assert!(provider.acquire_plan_in(&context("a")).await.is_err());
        assert_eq!(provider.service_generations(), original);
        assert_eq!(host.calls.load(Ordering::SeqCst), 1);
        assert_eq!(seen.created.load(Ordering::SeqCst), 0);
        let closed = provider.close_all(Duration::from_secs(3)).await;
        if matches!(mode, Mode::Refuse) {
            closed.unwrap();
        } else {
            assert_eq!(closed, Err(ExtensionError::ContextCleanupIncomplete));
        }
    }
}

#[tokio::test]
async fn constructor_panic_waits_for_actual_registered_cleanup() {
    let host = Host::new(Mode::Admit, false);
    let seen = Seen::new(false, true, true);
    let provider = provider(host, seen.clone(), FactoryScope::Session);
    let mut acquire = tokio::spawn({
        let provider = provider.clone();
        async move { provider.acquire_plan_in(&context("a")).await }
    });
    wait(&seen.cleanup_entered).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut acquire)
            .await
            .is_err()
    );
    assert_eq!(seen.stopped.load(Ordering::SeqCst), 0);
    seen.cleanup_release.add_permits(1);
    assert!(acquire.await.unwrap().is_err());
    provider.close_all(Duration::from_secs(3)).await.unwrap();
    assert_eq!(seen.stopped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn session_reference_release_keeps_other_session_workspace_generation_alive() {
    let host = Host::new(Mode::Admit, false);
    let seen = Seen::new(false, false, false);
    let provider = provider(host.clone(), seen.clone(), FactoryScope::Workspace);
    let a = provider.acquire_plan_in(&context("a")).await.unwrap();
    let b = provider.acquire_plan_in(&context("b")).await.unwrap();
    assert_eq!(host.calls.load(Ordering::SeqCst), 1);
    assert_eq!(seen.created.load(Ordering::SeqCst), 1);
    a.release();
    provider
        .close_session(&context("a"), Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(seen.stopped.load(Ordering::SeqCst), 0);
    assert_eq!(
        provider
            .close_workspace(&context("b"), Duration::from_millis(10))
            .await,
        Err(ExtensionError::ContextCleanupIncomplete)
    );
    b.release();
    provider.close_all(Duration::from_secs(3)).await.unwrap();
    assert_eq!(seen.stopped.load(Ordering::SeqCst), 1);
    assert_eq!(seen.shutdown.load(Ordering::SeqCst), 1);
}

struct SwapHost(Mutex<Option<ContextualGenerationWork>>);
impl ContextualGenerationHost for SwapHost {
    fn register(
        &self,
        work: ContextualGenerationWork,
    ) -> BoxFuture<'static, ContextualGenerationAdmission> {
        let mut original = self.0.lock().unwrap();
        let result = if let Some(other) = original.take() {
            ContextualGenerationAdmission::Admitted(other)
        } else {
            *original = Some(work);
            ContextualGenerationAdmission::Uncertain
        };
        Box::pin(async { result })
    }
}
#[tokio::test]
async fn another_slots_carrier_never_dispatches_this_constructor() {
    let host = Arc::new(SwapHost(Mutex::new(None)));
    let seen = Seen::new(false, false, false);
    let make = || {
        ContextualPlanProvider::new_with_generation_host(
            Registry::new(),
            vec![],
            vec![Arc::new(Factory {
                seen: seen.clone(),
                scope: FactoryScope::Workspace,
            })],
            Duration::from_secs(3),
            host.clone(),
        )
        .unwrap()
    };
    let a = make();
    let b = make();
    assert!(a.acquire_plan_in(&context("a")).await.is_err());
    assert!(b.acquire_plan_in(&context("b")).await.is_err());
    assert_eq!(seen.created.load(Ordering::SeqCst), 0);
    for provider in [a, b] {
        assert_eq!(
            provider.close_all(Duration::from_secs(3)).await,
            Err(ExtensionError::ContextCleanupIncomplete)
        );
    }
}

#[tokio::test]
async fn close_can_observe_retained_host_after_acquisition_waiter_drops() {
    let host = Host::new(Mode::Admit, true);
    let seen = Seen::new(false, false, false);
    let provider = provider(host.clone(), seen.clone(), FactoryScope::Session);
    let acquire = tokio::spawn({
        let provider = provider.clone();
        async move { provider.acquire_plan_in(&context("a")).await }
    });
    wait(&host.entered).await;
    acquire.abort();
    assert!(matches!(acquire.await, Err(error) if error.is_cancelled()));
    assert_eq!(
        provider.close_all(Duration::from_millis(10)).await,
        Err(ExtensionError::ContextCleanupIncomplete)
    );
    host.release.add_permits(1);
    provider.close_all(Duration::from_secs(3)).await.unwrap();
    assert_eq!(seen.created.load(Ordering::SeqCst), 0);
    assert_eq!(host.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cooperative_host_close_observes_original_call_without_constructing_factory() {
    let host = Host::new(Mode::CloseRefuse, true);
    let seen = Seen::new(false, false, false);
    let provider = provider(host.clone(), seen.clone(), FactoryScope::Session);
    let acquire = tokio::spawn({
        let provider = provider.clone();
        async move { provider.acquire_plan_in(&context("a")).await }
    });
    wait(&host.entered).await;
    provider.close_all(Duration::from_secs(3)).await.unwrap();
    assert!(acquire.await.unwrap().is_err());
    assert_eq!(seen.created.load(Ordering::SeqCst), 0);
    assert_eq!(host.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn failed_registered_cleanup_never_allows_another_constructor() {
    let host = Host::new(Mode::Admit, false);
    let seen = Seen::new(false, false, true);
    seen.fail_cleanup.store(true, Ordering::SeqCst);
    let provider = provider(host.clone(), seen.clone(), FactoryScope::Session);
    assert!(provider.acquire_plan_in(&context("a")).await.is_err());
    assert!(provider.acquire_plan_in(&context("a")).await.is_err());
    assert_eq!(
        provider.close_all(Duration::from_secs(3)).await,
        Err(ExtensionError::ContextCleanupIncomplete)
    );
    assert_eq!(seen.stopped.load(Ordering::SeqCst), 1);
    assert_eq!(seen.created.load(Ordering::SeqCst), 1);
    assert_eq!(host.calls.load(Ordering::SeqCst), 1);
}

#[path = "contextual_generation_producer/self_close.rs"]
mod self_close;

#[path = "contextual_generation_producer/retry.rs"]
mod retry;

#[path = "contextual_generation_producer/graph.rs"]
mod graph;
