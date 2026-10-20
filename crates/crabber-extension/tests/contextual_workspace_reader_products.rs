//! Real resolver/returned-reader two-tier Box ownership, never host authority.
use async_trait::async_trait;
use crabber_core::{SessionContext, SessionId};
use crabber_extension::*;
use futures::future::BoxFuture;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::Notify;
#[path = "contextual_workspace_reader_products/context.rs"]
mod context;
#[path = "contextual_workspace_reader_products/destructor.rs"]
mod destructor;
const BOUND: Duration = Duration::from_secs(3);
struct Gate {
    entered: Notify,
    open: Mutex<bool>,
    changed: Condvar,
}
impl Gate {
    fn new(open: bool) -> Arc<Self> {
        Arc::new(Self {
            entered: Notify::new(),
            open: Mutex::new(open),
            changed: Condvar::new(),
        })
    }
    fn block(&self) {
        self.entered.notify_one();
        let mut open = self.open.lock().unwrap();
        while !*open {
            open = self.changed.wait(open).unwrap();
        }
    }
    fn release(&self) {
        *self.open.lock().unwrap() = true;
        self.changed.notify_all();
    }
    async fn entered(&self) {
        tokio::time::timeout(BOUND, self.entered.notified())
            .await
            .unwrap();
    }
}
struct Release(Vec<Arc<Gate>>);
impl Drop for Release {
    fn drop(&mut self) {
        for gate in &self.0 {
            gate.release();
        }
    }
}
struct Seen {
    resolves: AtomicUsize,
    reads: AtomicUsize,
    resolver_drops: AtomicUsize,
    reader_drops: AtomicUsize,
    pending: AtomicBool,
    read_called: Notify,
    call_drop: Arc<Gate>,
    resolve_drop: Arc<Gate>,
    reader_drop: Arc<Gate>,
    frames: Mutex<Option<Arc<context::Frames>>>,
    destructor_frames: Mutex<Option<Arc<destructor::Frames>>>,
    cross_unblock: AtomicBool,
    read_panic: AtomicBool,
    poll_panic: AtomicBool,
    box_panic: AtomicBool,
    oversize: AtomicBool,
}
impl Seen {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            resolves: AtomicUsize::new(0),
            reads: AtomicUsize::new(0),
            resolver_drops: AtomicUsize::new(0),
            reader_drops: AtomicUsize::new(0),
            pending: AtomicBool::new(false),
            read_called: Notify::new(),
            call_drop: Gate::new(true),
            resolve_drop: Gate::new(true),
            reader_drop: Gate::new(true),
            frames: Mutex::new(None),
            destructor_frames: Mutex::new(None),
            cross_unblock: AtomicBool::new(false),
            read_panic: AtomicBool::new(false),
            poll_panic: AtomicBool::new(false),
            box_panic: AtomicBool::new(false),
            oversize: AtomicBool::new(false),
        })
    }
}
struct Resolver(Arc<Seen>);
impl Drop for Resolver {
    fn drop(&mut self) {
        self.0.resolver_drops.fetch_add(1, Ordering::SeqCst);
    }
}
struct Reader(Arc<Seen>);
impl Drop for Reader {
    fn drop(&mut self) {
        self.0.reader_drop.block();
        if self.0.cross_unblock.load(Ordering::SeqCst) {
            self.0.resolve_drop.release();
        }
        let frames = self.0.destructor_frames.lock().unwrap().clone();
        if let Some(frames) = &frames {
            frames.original_drop();
        }
        self.0.reader_drops.fetch_add(1, Ordering::SeqCst);
        if self.0.box_panic.load(Ordering::SeqCst)
            && let Some(frames) = frames
        {
            std::panic::panic_any(destructor::Payload(frames));
        }
        assert!(
            !self.0.box_panic.load(Ordering::SeqCst),
            "original reader Box Drop panic"
        );
    }
}
struct Created {
    product: Option<ContextualWorkspaceReaderProduct>,
    seen: Arc<Seen>,
}
impl Future for Created {
    type Output = Result<ContextualWorkspaceReaderProduct, WorkspaceReadError>;
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(self.product.take().unwrap()))
    }
}
impl Drop for Created {
    fn drop(&mut self) {
        self.seen.resolve_drop.block();
    }
}
// Implement the async-trait ABI directly so Created is the literal original
// method future. An inner awaited future would dispose BEFORE resolve returned
// its Box, which cannot test a Ready-output handoff.
impl ContextualWorkspaceReaderResolverBody for Resolver {
    fn resolve_box<'life0, 'life1, 'async_trait>(
        &'life0 self,
        workspace: &'life1 WorkspaceContext,
    ) -> BoxFuture<'async_trait, Result<ContextualWorkspaceReaderProduct, WorkspaceReadError>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        assert_eq!(workspace.workspace_id(), Some("workspace"));
        assert_eq!(workspace.directory(), Some("/workspace"));
        self.0.resolves.fetch_add(1, Ordering::SeqCst);
        Box::pin(Created {
            product: Some(ContextualWorkspaceReaderProduct::new(Box::new(Reader(
                self.0.clone(),
            )))),
            seen: self.0.clone(),
        })
    }
}

struct Read<'a> {
    seen: Arc<Seen>,
    body: BoxFuture<'a, Result<Vec<u8>, WorkspaceReadError>>,
}
impl Future for Read<'_> {
    type Output = Result<Vec<u8>, WorkspaceReadError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(
            !self.seen.poll_panic.load(Ordering::SeqCst),
            "original read poll panic"
        );
        self.body.as_mut().poll(cx)
    }
}
impl Drop for Read<'_> {
    fn drop(&mut self) {
        if let Some(frames) = self.seen.frames.lock().unwrap().clone() {
            frames.disposed();
        }
        self.seen.call_drop.block();
        assert!(
            !self.seen.read_panic.load(Ordering::SeqCst),
            "original read future Drop panic"
        );
    }
}
impl WorkspaceReader for Reader {
    fn read_limited<'life0, 'life1, 'async_trait>(
        &'life0 self,
        path: &'life1 str,
        maximum: usize,
    ) -> BoxFuture<'async_trait, Result<Vec<u8>, WorkspaceReadError>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(Read {
            seen: self.0.clone(),
            body: Box::pin(async move {
                let frames = self.0.frames.lock().unwrap().clone();
                if let Some(frames) = frames {
                    frames.read().await;
                }
                self.0.reads.fetch_add(1, Ordering::SeqCst);
                self.0.read_called.notify_one();
                if path == "denied" {
                    return Err(WorkspaceReadError::new(WorkspaceReadErrorKind::Denied));
                }
                if self.0.pending.load(Ordering::SeqCst) {
                    std::future::pending::<()>().await;
                }
                if self.0.oversize.load(Ordering::SeqCst) {
                    Ok(vec![1; maximum + 1])
                } else {
                    Ok(path.as_bytes().to_vec())
                }
            }),
        })
    }
}
struct Factory(Arc<Seen>, FactoryScope);
impl ContextualWorkspaceReaderResolverProductFactory for Factory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "unique-reader-resolver".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    fn scope(&self) -> FactoryScope {
        self.1
    }
    fn create_workspace_reader_resolver(
        &self,
        _: FactoryContext,
    ) -> BoxFuture<'_, Result<ContextualWorkspaceReaderResolverProduct, ExtensionError>> {
        Box::pin(async move {
            Ok(ContextualWorkspaceReaderResolverProduct::new(Box::new(
                Resolver(self.0.clone()),
            )))
        })
    }
}
#[derive(Default)]
struct Host(Mutex<Vec<ContextualGenerationObservation>>);
impl ContextualGenerationHost for Host {
    fn register(
        &self,
        work: ContextualGenerationWork,
    ) -> BoxFuture<'static, ContextualGenerationAdmission> {
        self.0.lock().unwrap().push(work.observation().unwrap());
        Box::pin(async move { ContextualGenerationAdmission::Admitted(work) })
    }
}
fn context() -> SessionContext {
    SessionContext::new(
        SessionId("reader-product".into()),
        "workspace",
        "/workspace",
    )
}
fn workspace() -> WorkspaceContext {
    WorkspaceContext::from_persisted("workspace", "/workspace")
}
fn setup(seen: &Arc<Seen>) -> (ContextualPlanProvider, Arc<Host>) {
    let host = Arc::new(Host::default());
    let provider = ContextualPlanProvider::new_with_workspace_reader_resolver_products(
        Registry::new(),
        vec![Arc::new(Factory(seen.clone(), FactoryScope::Session))],
        BOUND,
        host.clone(),
    )
    .unwrap();
    (provider, host)
}
fn graph(host: &Host) -> ContextualGenerationObservation {
    host.0.lock().unwrap().last().unwrap().clone()
}
async fn drain(owner: CallbackInvocationOwner, resources: &ToolResourceOwner) {
    drop(owner);
    resources.close();
    let joined = resources.drain(BOUND).await;
    assert_eq!(joined.pending, 0);
    assert!(joined.failures.is_empty(), "{joined:?}");
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_root_and_returned_reader_references_gate_real_boxes() {
    let seen = Seen::new();
    let (provider, host) = setup(&seen);
    let plan = provider.acquire_plan_in(&context()).await.unwrap();
    let resolver = plan.workspace_reader_resolver().unwrap().clone();
    drop(plan);
    assert!(resolver.resolve(&workspace()).await.is_err());
    assert_eq!(seen.resolves.load(Ordering::SeqCst), 0);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let reader = with_callback_invocations(owner.capability(), resolver.resolve(&workspace()))
        .await
        .unwrap();
    let last = reader.clone();
    let result =
        with_callback_invocations(owner.capability(), reader.read_limited("actual/path", 64))
            .await
            .unwrap();
    assert_eq!(result, b"actual/path");
    graph(&host).request_close().unwrap();
    assert!(
        with_callback_invocations(owner.capability(), reader.read_limited("after-close", 64))
            .await
            .is_err()
    );
    assert!(
        with_callback_invocations(owner.capability(), resolver.resolve(&workspace()))
            .await
            .is_err()
    );
    assert_eq!(seen.reads.load(Ordering::SeqCst), 1);
    assert_eq!(seen.resolves.load(Ordering::SeqCst), 1);

    drop(resolver);
    drop(reader);
    let observation = graph(&host).workspace_reader_resolver_product(0).unwrap();
    let pending = observation.disposed(Duration::from_millis(20)).await;
    let drops = seen.reader_drops.load(Ordering::SeqCst);
    drop(last);
    drain(owner, &resources).await;
    observation.disposed(BOUND).await.unwrap();
    provider.close_all(BOUND).await.unwrap();
    assert!(pending.is_err());
    assert_eq!(drops, 0);
    assert_eq!(seen.reader_drops.load(Ordering::SeqCst), 1);
    assert_eq!(seen.resolver_drops.load(Ordering::SeqCst), 1);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn final_reader_clone_seals_only_its_original_subset_and_other_readers_continue() {
    let seen = Seen::new();
    let (provider, host) = setup(&seen);
    let plan = provider.acquire_plan_in(&context()).await.unwrap();
    let resolver = plan.workspace_reader_resolver().unwrap().clone();
    drop(plan);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    with_callback_invocations(owner.capability(), async {
        let first = resolver.resolve(&workspace()).await.unwrap();
        let second = resolver.resolve(&workspace()).await.unwrap();
        drop(first);
        seen.reader_drop.entered().await;
        assert_eq!(
            second.read_limited("sibling", 64).await.unwrap(),
            b"sibling"
        );
        drop(second);
        for index in 0..80 {
            let reader = resolver.resolve(&workspace()).await.unwrap();
            let path = format!("sequence/{index}");
            assert_eq!(
                reader.read_limited(&path, 64).await.unwrap(),
                path.as_bytes()
            );
            drop(reader);
        }
    })
    .await;
    drop(resolver);
    drain(owner, &resources).await;
    graph(&host).request_close().unwrap();
    graph(&host)
        .workspace_reader_resolver_product(0)
        .unwrap()
        .disposed(BOUND)
        .await
        .unwrap();
    provider.close_all(BOUND).await.unwrap();
    assert_eq!(seen.reader_drops.load(Ordering::SeqCst), 82);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_reader_drop_can_unblock_literal_ready_resolve_future_disposal() {
    let seen = Seen::new();
    *seen.resolve_drop.open.lock().unwrap() = false;
    seen.cross_unblock.store(true, Ordering::SeqCst);
    let _release = Release(vec![seen.resolve_drop.clone()]);
    let (provider, host) = setup(&seen);
    let plan = provider.acquire_plan_in(&context()).await.unwrap();
    let resolver = plan.workspace_reader_resolver().unwrap().clone();
    drop(plan);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let task = tokio::spawn(with_callback_invocations(owner.capability(), async move {
        resolver.resolve(&workspace()).await
    }));
    seen.resolve_drop.entered().await;
    task.abort();
    let _ = task.await;
    seen.reader_drop.entered().await;
    drain(owner, &resources).await;
    graph(&host).request_close().unwrap();
    graph(&host)
        .workspace_reader_resolver_product(0)
        .unwrap()
        .disposed(BOUND)
        .await
        .unwrap();
    provider.close_all(BOUND).await.unwrap();
    assert_eq!(seen.reader_drops.load(Ordering::SeqCst), 1);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aborted_reader_waiter_keeps_actual_parent_and_mount_pending_through_original_disposal() {
    let seen = Seen::new();
    seen.pending.store(true, Ordering::SeqCst);
    *seen.call_drop.open.lock().unwrap() = false;
    let _release = Release(vec![seen.call_drop.clone()]);
    let (provider, host) = setup(&seen);
    let plan = provider.acquire_plan_in(&context()).await.unwrap();
    let resolver = plan.workspace_reader_resolver().unwrap().clone();
    drop(plan);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let reader = with_callback_invocations(owner.capability(), resolver.resolve(&workspace()))
        .await
        .unwrap();
    drop(resolver);
    let task = tokio::spawn(with_callback_invocations(owner.capability(), async move {
        reader.read_limited("held", 64).await
    }));
    seen.read_called.notified().await;
    task.abort();
    let _ = task.await;
    seen.call_drop.entered().await;
    drop(owner);
    resources.close();
    let pending = resources.drain(Duration::from_millis(20)).await;
    graph(&host).request_close().unwrap();
    let observation = graph(&host).workspace_reader_resolver_product(0).unwrap();
    let before = observation.disposed(Duration::from_millis(20)).await;
    seen.call_drop.release();
    let joined = resources.drain(BOUND).await;
    observation.disposed(BOUND).await.unwrap();
    provider.close_all(BOUND).await.unwrap();
    assert!(pending.pending > 0 && before.is_err());
    assert_eq!(joined.pending, 0);
    assert_eq!(joined.failures, [] as [ResourceFailure; 0]);
    assert_eq!(seen.reader_drops.load(Ordering::SeqCst), 1);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reader_capability_and_byte_limit_are_checked_without_changing_semantic_errors() {
    let seen = Seen::new();
    let (provider, host) = setup(&seen);
    let plan = provider.acquire_plan_in(&context()).await.unwrap();
    let resolver = plan.workspace_reader_resolver().unwrap().clone();
    drop(plan);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let reader = with_callback_invocations(owner.capability(), resolver.resolve(&workspace()))
        .await
        .unwrap();
    assert_eq!(
        reader.read_limited("missing", 64).await.unwrap_err().kind(),
        WorkspaceReadErrorKind::Denied
    );
    assert_eq!(seen.reads.load(Ordering::SeqCst), 0);
    let other_resources = ToolResourceOwner::new();
    let other = CallbackInvocationOwner::attach(other_resources.resources()).unwrap();
    assert!(
        with_callback_invocations(other.capability(), reader.read_limited("wrong-owner", 64))
            .await
            .is_err()
    );
    drain(other, &other_resources).await;
    with_callback_invocations(owner.capability(), async {
        assert_eq!(
            reader.read_limited("denied", 64).await.unwrap_err().kind(),
            WorkspaceReadErrorKind::Denied
        );
        seen.oversize.store(true, Ordering::SeqCst);
        assert_eq!(
            reader
                .read_limited("too-large", 64)
                .await
                .unwrap_err()
                .kind(),
            WorkspaceReadErrorKind::TooLarge
        );
    })
    .await;
    drop(reader);
    drop(resolver);
    drain(owner, &resources).await;
    graph(&host).request_close().unwrap();
    graph(&host)
        .workspace_reader_resolver_product(0)
        .unwrap()
        .disposed(BOUND)
        .await
        .unwrap();
    provider.close_all(BOUND).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn active_reader_capacity_refuses_before_resolve_and_failure_stays_sticky() {
    let seen = Seen::new();
    let (provider, host) = setup(&seen);
    let plan = provider.acquire_plan_in(&context()).await.unwrap();
    let resolver = plan.workspace_reader_resolver().unwrap().clone();
    drop(plan);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let readers = with_callback_invocations(owner.capability(), async {
        let mut readers = Vec::new();
        for _ in 0..64 {
            readers.push(resolver.resolve(&workspace()).await.unwrap());
        }
        assert!(resolver.resolve(&workspace()).await.is_err());
        readers
    })
    .await;
    assert_eq!(seen.resolves.load(Ordering::SeqCst), 64);
    drop(readers);
    drop(resolver);
    drain(owner, &resources).await;
    graph(&host).request_close().unwrap();
    graph(&host)
        .workspace_reader_resolver_product(0)
        .unwrap()
        .disposed(BOUND)
        .await
        .unwrap();
    provider.close_all(BOUND).await.unwrap();
    assert_eq!(seen.reader_drops.load(Ordering::SeqCst), 64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn literal_read_future_disposal_panic_is_not_a_successful_parent_or_product_join() {
    let seen = Seen::new();
    seen.read_panic.store(true, Ordering::SeqCst);
    let (provider, host) = setup(&seen);
    let plan = provider.acquire_plan_in(&context()).await.unwrap();
    let resolver = plan.workspace_reader_resolver().unwrap().clone();
    drop(plan);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let reader = with_callback_invocations(owner.capability(), resolver.resolve(&workspace()))
        .await
        .unwrap();
    let result =
        with_callback_invocations(owner.capability(), reader.read_limited("panic", 64)).await;
    drop(reader);
    drop(resolver);
    drop(owner);
    resources.close();
    let joined = resources.drain(BOUND).await;
    graph(&host).request_close().unwrap();
    let disposed = graph(&host)
        .workspace_reader_resolver_product(0)
        .unwrap()
        .disposed(BOUND)
        .await;
    let close = provider.close_all(BOUND).await;
    assert!(result.is_err());
    assert_ne!(joined.failures, [] as [ResourceFailure; 0]);
    assert!(disposed.is_err() && close.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_reader_destructor_panic_never_mints_disposed_observation() {
    let seen = Seen::new();
    seen.box_panic.store(true, Ordering::SeqCst);
    let (provider, host) = setup(&seen);
    let plan = provider.acquire_plan_in(&context()).await.unwrap();
    let resolver = plan.workspace_reader_resolver().unwrap().clone();
    drop(plan);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let reader = with_callback_invocations(owner.capability(), resolver.resolve(&workspace()))
        .await
        .unwrap();
    drop(reader);
    drop(resolver);
    drain(owner, &resources).await;
    graph(&host).request_close().unwrap();
    let observation = graph(&host).workspace_reader_resolver_product(0).unwrap();
    assert!(observation.disposed(BOUND).await.is_err());
    assert!(observation.disposed(BOUND).await.is_err());
    assert!(provider.close_all(BOUND).await.is_err());
}

struct Legacy(Arc<dyn WorkspaceReader>);
#[async_trait]
impl SystemPromptMiddleware for Legacy {
    async fn contribute(&self, _: ModelAttemptContext) -> Result<Option<String>, String> {
        self.0
            .read_limited("legacy", 64)
            .await
            .map(|bytes| Some(String::from_utf8(bytes).unwrap()))
            .map_err(|error| error.to_string())
    }
}
#[async_trait]
impl Extension for Legacy {
    fn id(&self) -> &'static str {
        "legacy-reader"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "{}".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        registrar.system_prompt_middleware(
            "legacy-reader",
            0,
            MiddlewareDescriptor::new("legacy-reader", "1", "a".repeat(64)).unwrap(),
            Arc::new(Self(self.0.clone())),
        );
        Ok(())
    }
}
#[tokio::test]
async fn legacy_held_reader_arc_is_not_reference_disposal_or_invocation_authority() {
    let seen = Seen::new();
    let reader: Arc<dyn WorkspaceReader> = Arc::new(Reader(seen.clone()));
    let registry = Registry::new();
    let mount = registry
        .mount_owned(Arc::new(Legacy(reader.clone())), Scope::Global)
        .await
        .unwrap();
    let plan = registry.acquire(context().session_id());
    assert!(plan.workspace_reader_resolver().is_none());
    drop(plan);
    mount.close(BOUND).await.unwrap();
    // Existing raw Arc interfaces remain explicitly outside the new product.
    assert_eq!(
        reader.read_limited("legacy-outside", 64).await.unwrap(),
        b"legacy-outside"
    );
    assert_eq!(seen.reads.load(Ordering::SeqCst), 1);
    assert_eq!(seen.reader_drops.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn session_reader_release_does_not_close_shared_workspace_resolver_generation() {
    let seen = Seen::new();
    let host = Arc::new(Host::default());
    let provider = ContextualPlanProvider::new_with_workspace_reader_resolver_products(
        Registry::new(),
        vec![Arc::new(Factory(seen.clone(), FactoryScope::Workspace))],
        BOUND,
        host.clone(),
    )
    .unwrap();
    let first = context();
    let second = SessionContext::new(
        SessionId("reader-sibling".into()),
        "workspace",
        "/workspace",
    );
    let first_plan = provider.acquire_plan_in(&first).await.unwrap();
    let first_resolver = first_plan.workspace_reader_resolver().unwrap().clone();
    drop(first_plan);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let reader =
        with_callback_invocations(owner.capability(), first_resolver.resolve(&workspace()))
            .await
            .unwrap();
    drop(first_resolver);
    drop(reader);
    drain(owner, &resources).await;
    provider.close_session(&first, BOUND).await.unwrap();
    assert_eq!(seen.resolver_drops.load(Ordering::SeqCst), 0);
    let second_plan = provider.acquire_plan_in(&second).await.unwrap();
    let second_resolver = second_plan.workspace_reader_resolver().unwrap().clone();
    drop(second_plan);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    with_callback_invocations(owner.capability(), async {
        let reader = second_resolver.resolve(&workspace()).await.unwrap();
        assert_eq!(
            reader.read_limited("still-live", 64).await.unwrap(),
            b"still-live"
        );
    })
    .await;
    drop(second_resolver);
    drain(owner, &resources).await;
    let observation = graph(&host).workspace_reader_resolver_product(0).unwrap();
    provider.close_all(BOUND).await.unwrap();
    observation.disposed(BOUND).await.unwrap();
    assert_eq!(seen.resolver_drops.load(Ordering::SeqCst), 1);
    assert_eq!(seen.reader_drops.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn creator_seal_during_original_read_disposal_refuses_bytes_after_actual_join() {
    let seen = Seen::new();
    *seen.call_drop.open.lock().unwrap() = false;
    let _release = Release(vec![seen.call_drop.clone()]);
    let (provider, host) = setup(&seen);
    let plan = provider.acquire_plan_in(&context()).await.unwrap();
    let resolver = plan.workspace_reader_resolver().unwrap().clone();
    drop(plan);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let capability = owner.capability();
    let reader = with_callback_invocations(capability.clone(), resolver.resolve(&workspace()))
        .await
        .unwrap();
    let task = tokio::spawn(with_callback_invocations(capability, async move {
        reader.read_limited("held-bytes", 64).await
    }));
    seen.call_drop.entered().await;
    drop(owner);
    resources.close();
    let pending = resources.drain(Duration::from_millis(20)).await;
    seen.call_drop.release();
    let result = task.await.unwrap();
    let joined = resources.drain(BOUND).await;
    drop(resolver);
    graph(&host).request_close().unwrap();
    graph(&host)
        .workspace_reader_resolver_product(0)
        .unwrap()
        .disposed(BOUND)
        .await
        .unwrap();
    provider.close_all(BOUND).await.unwrap();
    assert_ne!(pending.pending, 0);
    assert_eq!(joined.pending, 0);
    assert_eq!(joined.failures, [] as [ResourceFailure; 0]);
    assert!(result.is_err());
    assert_eq!(seen.reads.load(Ordering::SeqCst), 1);
    assert_eq!(seen.reader_drops.load(Ordering::SeqCst), 1);
}

#[test]
fn literal_read_poll_and_drop_panics_stay_inside_original_task() {
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "literal_read_double_panic_child", "--nocapture"])
        .env("CRABBER_READER_DOUBLE_PANIC_CHILD", "1")
        .env("RUST_BACKTRACE", "0")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "status={}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn literal_read_double_panic_child() {
    if std::env::var_os("CRABBER_READER_DOUBLE_PANIC_CHILD").is_none() {
        return;
    }
    let seen = Seen::new();
    seen.poll_panic.store(true, Ordering::SeqCst);
    seen.read_panic.store(true, Ordering::SeqCst);
    let (provider, host) = setup(&seen);
    let plan = provider.acquire_plan_in(&context()).await.unwrap();
    let resolver = plan.workspace_reader_resolver().unwrap().clone();
    drop(plan);
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let reader = with_callback_invocations(owner.capability(), resolver.resolve(&workspace()))
        .await
        .unwrap();
    let result =
        with_callback_invocations(owner.capability(), reader.read_limited("panic", 64)).await;
    drop(reader);
    drop(resolver);
    drop(owner);
    resources.close();
    let joined = resources.drain(BOUND).await;
    graph(&host).request_close().unwrap();
    let disposed = graph(&host)
        .workspace_reader_resolver_product(0)
        .unwrap()
        .disposed(BOUND)
        .await;
    let closed = provider.close_all(BOUND).await;
    assert!(result.is_err() && disposed.is_err() && closed.is_err());
    assert_ne!(joined.failures, [] as [ResourceFailure; 0]);
}
