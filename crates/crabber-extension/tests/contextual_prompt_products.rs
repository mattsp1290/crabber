//! Original callback/facade/Box joins, not a complete physical-resource policy.
use crabber_core::{RunId, SessionContext, SessionId, TurnId};
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
const BOUND: Duration = Duration::from_secs(3);
#[path = "contextual_prompt_products/legacy.rs"]
mod legacy;
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
    made: AtomicUsize,
    calls: AtomicUsize,
    futures: AtomicUsize,
    drops: AtomicUsize,
    mode: AtomicUsize,
    box_panic: AtomicBool,
    cross: AtomicBool,
    entered: Notify,
    future_drop: Arc<Gate>,
    box_drop: Arc<Gate>,
    constructor_drop: Arc<Gate>,
    hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    drop_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    payload_hook: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    reader: Mutex<Option<Arc<dyn WorkspaceReaderResolver>>>,
    reader_visits: AtomicUsize,
    text: Mutex<String>,
}
impl Seen {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            made: AtomicUsize::new(0),
            calls: AtomicUsize::new(0),
            futures: AtomicUsize::new(0),
            drops: AtomicUsize::new(0),
            mode: AtomicUsize::new(0),
            box_panic: AtomicBool::new(false),
            cross: AtomicBool::new(false),
            entered: Notify::new(),
            future_drop: Gate::new(true),
            box_drop: Gate::new(true),
            constructor_drop: Gate::new(true),
            hook: Mutex::new(None),
            drop_hook: Mutex::new(None),
            payload_hook: Mutex::new(None),
            reader: Mutex::new(None),
            reader_visits: AtomicUsize::new(0),
            text: Mutex::new("original text".into()),
        })
    }
    fn hook(&self) {
        let hook = self.hook.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook();
        }
    }
}
struct Original(Arc<Seen>);
impl Original {
    fn construct<E>(&self, semantic: fn() -> E) -> Returned<E> {
        self.0.calls.fetch_add(1, Ordering::SeqCst);
        self.0.hook();
        assert_ne!(
            self.0.mode.load(Ordering::SeqCst),
            6,
            "prompt construction panic"
        );
        Returned {
            seen: self.0.clone(),
            semantic,
        }
    }
    fn native(
        &self,
        _: PromptAttemptContext,
    ) -> BoxFuture<'static, Result<Option<String>, ExtensionError>> {
        Box::pin(self.construct(|| ExtensionError::Tool("semantic".into())))
    }
}
impl SystemPromptMiddleware for Original {
    fn contribute<'life0, 'async_trait>(
        &'life0 self,
        context: ModelAttemptContext,
    ) -> BoxFuture<'async_trait, Result<Option<String>, String>>
    where
        'life0: 'async_trait,
        Self: 'async_trait,
    {
        let expected = self.0.reader.lock().unwrap().clone();
        if let Some(expected) = expected {
            assert!(Arc::ptr_eq(
                context.workspace_reader_resolver().unwrap(),
                &expected
            ));
            let reader =
                futures::executor::block_on(expected.resolve(context.workspace())).unwrap();
            assert_eq!(
                futures::executor::block_on(reader.read_limited("file", 4)).unwrap(),
                b"data"
            );
            self.0.reader_visits.fetch_add(1, Ordering::SeqCst);
        }
        Box::pin(self.construct(|| "semantic".into()))
    }
}
impl Drop for Original {
    fn drop(&mut self) {
        let hook = self.0.drop_hook.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook();
        }
        if self.0.cross.load(Ordering::SeqCst) {
            self.0.constructor_drop.release();
        }
        self.0.box_drop.block();
        self.0.drops.fetch_add(1, Ordering::SeqCst);
        assert!(
            !self.0.box_panic.load(Ordering::SeqCst),
            "original Box Drop panic"
        );
    }
}
struct Payload(Arc<Seen>);
impl Drop for Payload {
    fn drop(&mut self) {
        let hook = self.0.payload_hook.lock().unwrap().clone();
        if let Some(hook) = hook {
            hook();
        }
    }
}
struct Returned<E> {
    seen: Arc<Seen>,
    semantic: fn() -> E,
}
impl<E> Future for Returned<E> {
    type Output = Result<Option<String>, E>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        self.seen.hook();
        self.seen.entered.notify_one();
        let mode = self.seen.mode.load(Ordering::SeqCst);
        if mode == 2 {
            return Poll::Pending;
        }
        if mode == 7 {
            std::panic::panic_any(Payload(self.seen.clone()));
        }
        assert!(!matches!(mode, 3 | 5), "literal prompt poll panic");
        Poll::Ready(if mode == 1 {
            Err((self.semantic)())
        } else {
            Ok(Some(self.seen.text.lock().unwrap().clone()))
        })
    }
}
impl<E> Drop for Returned<E> {
    fn drop(&mut self) {
        self.seen.hook();
        self.seen.future_drop.block();
        self.seen.futures.fetch_add(1, Ordering::SeqCst);
        assert!(
            !matches!(self.seen.mode.load(Ordering::SeqCst), 4 | 5),
            "literal prompt Drop panic"
        );
    }
}
struct Created {
    product: Option<ContextualPromptProduct>,
    seen: Arc<Seen>,
}
impl Future for Created {
    type Output = Result<ContextualPromptProduct, ExtensionError>;
    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Ok(self.product.take().unwrap()))
    }
}
impl Drop for Created {
    fn drop(&mut self) {
        self.seen.constructor_drop.block();
    }
}
struct Factory {
    seen: Arc<Seen>,
    typed: bool,
    scope: FactoryScope,
    name: String,
}
impl ContextualPromptProductFactory for Factory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "prompt-product".into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        format!("typed={}", self.typed)
    }
    fn scope(&self) -> FactoryScope {
        self.scope
    }
    fn create_prompt(
        &self,
        _: FactoryContext,
    ) -> BoxFuture<'_, Result<ContextualPromptProduct, ExtensionError>> {
        self.seen.made.fetch_add(1, Ordering::SeqCst);
        let original = Original(self.seen.clone());
        let product = if self.typed {
            ContextualPromptProduct::system_prompt(
                self.name.clone(),
                7,
                MiddlewareDescriptor::new("prompt", "1", "0".repeat(64)).unwrap(),
                Box::new(original),
            )
        } else {
            ContextualPromptProduct::native(
                self.name.clone(),
                7,
                Box::new(move |context| original.native(context)),
            )
        };
        Box::pin(Created {
            product: Some(product),
            seen: self.seen.clone(),
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
        Box::pin(async { ContextualGenerationAdmission::Admitted(work) })
    }
}
fn session() -> SessionContext {
    SessionContext::new(SessionId::new(), "workspace", "/workspace")
}
fn context() -> PromptAttemptContext {
    PromptAttemptContext::new(
        SessionId::new(),
        RunId::new(),
        TurnId::new(),
        WorkspaceContext::from_persisted("workspace", "/workspace"),
        "provider".into(),
        "model".into(),
        1,
        false,
    )
}
fn provider(
    seen: &Arc<Seen>,
    typed: bool,
    scope: FactoryScope,
    name: &str,
) -> (ContextualPlanProvider, Arc<Host>, Registry) {
    let host = Arc::new(Host::default());
    let registry = Registry::new();
    let provider = ContextualPlanProvider::new_with_owned_products(
        registry.clone(),
        vec![ContextualOwnedProductFactory::Prompt(Arc::new(Factory {
            seen: seen.clone(),
            typed,
            scope,
            name: name.into(),
        }))],
        BOUND,
        host.clone(),
    )
    .unwrap();
    (provider, host, registry)
}
async fn call(
    contributors: &[MountedPromptContributor],
) -> (PromptContributionOutcome, ResourceDrain) {
    let resources = ToolResourceOwner::new();
    let owner = CallbackInvocationOwner::attach(resources.resources()).unwrap();
    let outcome = with_callback_invocations(
        owner.capability(),
        collect_prompt_contributions(contributors, context()),
    )
    .await;
    drop(owner);
    resources.close();
    let joined = resources.drain(BOUND).await;
    (outcome, joined)
}
fn completed(outcome: PromptContributionOutcome) {
    let PromptContributionOutcome::Completed { sections } = outcome else {
        panic!("expected completed prompt sections: {outcome:?}");
    };
    assert_eq!(
        sections,
        vec![PromptContribution {
            name: "original".into(),
            text: "original text".into()
        }]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exact_box_reference_release_and_closed_effect_refusal() {
    for typed in [false, true] {
        let seen = Seen::new();
        let (provider, host, _) = provider(&seen, typed, FactoryScope::Workspace, "original");
        let plan = provider.acquire_plan_in(&session()).await.unwrap();
        let callbacks = plan.prompt_contributors.clone();
        let last = callbacks.clone();
        drop(plan);
        let no_owner = collect_prompt_contributions(&callbacks, context()).await;
        assert!(matches!(no_owner, PromptContributionOutcome::Failed { .. }));
        assert_eq!(seen.calls.load(Ordering::SeqCst), 0);
        completed(call(&callbacks).await.0);
        let close = provider.close_all(Duration::from_millis(20)).await;
        let before = seen.drops.load(Ordering::SeqCst);
        let closed = call(&callbacks).await.0;
        drop(callbacks);
        let after_first = seen.drops.load(Ordering::SeqCst);
        drop(last);
        provider.close_all(BOUND).await.unwrap();
        let node = host.0.lock().unwrap()[0].prompt_product(0).unwrap();
        node.disposed(BOUND).await.unwrap();
        assert!(close.is_err());
        assert!(matches!(closed, PromptContributionOutcome::Failed { .. }));
        assert_eq!(
            (before, after_first, seen.drops.load(Ordering::SeqCst)),
            (0, 0, 1)
        );
        assert_eq!(seen.calls.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn original_semantic_errors_and_successful_reaping_are_distinct() {
    for typed in [false, true] {
        let seen = Seen::new();
        let (provider, host, _) = provider(&seen, typed, FactoryScope::Session, "original");
        let plan = provider.acquire_plan_in(&session()).await.unwrap();
        for _ in 0..130 {
            let (outcome, joined) = call(&plan.prompt_contributors).await;
            completed(outcome);
            assert_eq!(joined.pending, 0);
            assert_eq!(joined.failures, [] as [ResourceFailure; 0]);
        }
        seen.mode.store(1, Ordering::SeqCst);
        let (outcome, joined) = call(&plan.prompt_contributors).await;
        assert!(matches!(outcome, PromptContributionOutcome::Failed { .. }));
        assert_eq!(joined.failures, [] as [ResourceFailure; 0]);
        drop(plan);
        provider.close_all(BOUND).await.unwrap();
        let node = host.0.lock().unwrap()[0].prompt_product(0).unwrap();
        node.disposed(BOUND).await.unwrap();
        assert_eq!(seen.calls.load(Ordering::SeqCst), 131);
        assert_eq!(seen.drops.load(Ordering::SeqCst), 1);
    }
}
#[path = "contextual_prompt_products/frames.rs"]
mod frames;
#[path = "contextual_prompt_products/limits.rs"]
mod limits;
#[path = "contextual_prompt_products/panics.rs"]
mod panics;
#[path = "contextual_prompt_products/retention.rs"]
mod retention;
#[path = "contextual_prompt_products/scopes.rs"]
mod scopes;
