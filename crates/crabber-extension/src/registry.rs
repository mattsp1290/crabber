//! Atomic native extension mounting and immutable plan acquisition.
#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
use crate::dispatch::{self, AroundCallback, Callback, Dispatcher, Handler, HandlerFn, Mode};
use crate::{
    ComponentIdentity, ExtensionError, PromptSection, RunPlan, RunPlanProvider, ToolDefinition,
    compute_fingerprint,
};
use async_trait::async_trait;
use crabber_core::{RunId, SessionId, ToolCallId, ToolInfo};
use crabber_providers::ProviderAdapter;
use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::Notify;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Scope {
    Global,
    Session(SessionId),
}
impl Scope {
    fn applies(&self, session: &SessionId) -> bool {
        matches!(self, Self::Global) || matches!(self, Self::Session(id) if id == session)
    }
    fn rank(&self) -> u8 {
        u8::from(!matches!(self, Self::Global))
    }
}

#[async_trait]
pub trait Extension: Send + Sync {
    fn id(&self) -> &str;
    fn version(&self) -> &str;
    fn config_hash(&self) -> String;
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError>;
    async fn shutdown(&self) {}
}

type Cleanup = Box<dyn FnOnce() + Send>;
pub struct Registrar {
    tools: Vec<Arc<ToolDefinition>>,
    prompts: Vec<Arc<PromptSection>>,
    guards: Vec<Arc<dyn ToolGuard>>,
    restrictions: Vec<Vec<String>>,
    handlers: Vec<Handler>,
    providers: Vec<Arc<dyn ProviderAdapter>>,
    cleanups: Vec<Cleanup>,
}
impl Registrar {
    fn new() -> Self {
        Self {
            tools: vec![],
            prompts: vec![],
            guards: vec![],
            restrictions: vec![],
            handlers: vec![],
            providers: vec![],
            cleanups: vec![],
        }
    }
    pub fn tool(&mut self, tool: Arc<ToolDefinition>) {
        self.tools.push(tool);
    }
    pub fn prompt(&mut self, prompt: Arc<PromptSection>) {
        self.prompts.push(prompt);
    }
    pub fn guard(&mut self, guard: Arc<dyn ToolGuard>) {
        self.guards.push(guard);
    }
    pub fn restrict_tools(&mut self, names: impl IntoIterator<Item = String>) {
        self.restrictions.push(names.into_iter().collect());
    }
    pub fn provider(&mut self, provider: Arc<dyn ProviderAdapter>) {
        self.providers.push(provider);
    }
    pub fn defer(&mut self, cleanup: impl FnOnce() + Send + 'static) {
        self.cleanups.push(Box::new(cleanup));
    }
    fn handler(
        &mut self,
        point: &'static str,
        mode: Mode,
        order: i32,
        id: impl Into<String>,
        callback: HandlerFn,
    ) {
        self.handlers.push(Handler {
            point,
            mode,
            order,
            id: id.into(),
            scope_rank: 0,
            mount_seq: 0,
            registration_seq: self.handlers.len(),
            mount_id: 0,
            callback,
        });
    }
    pub fn on_notify(
        &mut self,
        point: &'static str,
        order: i32,
        id: impl Into<String>,
        cb: Callback,
    ) {
        self.handler(point, Mode::Notify, order, id, HandlerFn::Ordinary(cb));
    }
    pub fn on_hook(
        &mut self,
        point: &'static str,
        order: i32,
        id: impl Into<String>,
        cb: Callback,
    ) {
        self.handler(point, Mode::Hook, order, id, HandlerFn::Ordinary(cb));
    }
    pub fn on_transform(
        &mut self,
        point: &'static str,
        order: i32,
        id: impl Into<String>,
        cb: Callback,
    ) {
        self.handler(point, Mode::Transform, order, id, HandlerFn::Ordinary(cb));
    }
    pub fn on_gate(
        &mut self,
        point: &'static str,
        order: i32,
        id: impl Into<String>,
        cb: Callback,
    ) {
        self.handler(point, Mode::Gate, order, id, HandlerFn::Ordinary(cb));
    }
    pub fn on_around(
        &mut self,
        point: &'static str,
        order: i32,
        id: impl Into<String>,
        cb: AroundCallback,
    ) {
        self.handler(point, Mode::Around, order, id, HandlerFn::Around(cb));
    }
    fn rollback(&mut self) {
        while let Some(cleanup) = self.cleanups.pop() {
            cleanup();
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardDecision {
    Abstain,
    Allow,
    Ask,
    Deny,
}
pub struct GuardContext<'a> {
    pub tool: &'a ToolInfo,
    pub arguments: &'a serde_json::Value,
    pub call_id: &'a ToolCallId,
    pub session_id: &'a SessionId,
    pub run_id: &'a RunId,
}
pub trait ToolGuard: Send + Sync {
    fn id(&self) -> &str;
    fn check(&self, name: &str, arguments: &serde_json::Value) -> GuardDecision;
    fn check_with_context(&self, context: GuardContext<'_>) -> GuardDecision {
        self.check(&context.tool.name, context.arguments)
    }
}
struct Mount {
    id: u64,
    seq: u64,
    scope: Scope,
    extension: Arc<dyn Extension>,
    registrar: Mutex<Registrar>,
    active: Mutex<bool>,
    closing: Mutex<bool>,
    closed: Mutex<bool>,
    leases: AtomicUsize,
    released: Notify,
    closed_notify: Notify,
}
#[derive(Default)]
struct RegistryInner {
    mounts: Vec<Arc<Mount>>,
    next_id: u64,
}
#[cfg(test)]
type AcquireHook = Arc<dyn Fn() + Send + Sync>;
#[derive(Clone, Default)]
pub struct Registry {
    inner: Arc<Mutex<RegistryInner>>,
    #[cfg(test)]
    acquire_hook: Arc<Mutex<Option<AcquireHook>>>,
}
impl Registry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    pub async fn mount(
        &self,
        extension: Arc<dyn Extension>,
        scope: Scope,
    ) -> Result<MountHandle, ExtensionError> {
        let mut registrar = Registrar::new();
        if let Err(error) = extension.install(&mut registrar).await {
            registrar.rollback();
            return Err(error);
        }
        let mut inner = self.inner.lock().unwrap();
        let mut seen = HashSet::new();
        let collision = registrar.tools.iter().find_map(|tool| {
            (!seen.insert(tool.info.name.clone())
                || inner.mounts.iter().any(|m| {
                    *m.active.lock().unwrap()
                        && scopes_overlap(&scope, &m.scope)
                        && m.registrar
                            .lock()
                            .unwrap()
                            .tools
                            .iter()
                            .any(|t| t.info.name == tool.info.name)
                }))
            .then(|| tool.info.name.clone())
        });
        if let Some(name) = collision {
            drop(inner);
            registrar.rollback();
            return Err(ExtensionError::ToolCollision(name));
        }
        inner.next_id += 1;
        let id = inner.next_id;
        for handler in &mut registrar.handlers {
            handler.mount_id = id;
            handler.mount_seq = id;
            handler.scope_rank = scope.rank();
        }
        let mount = Arc::new(Mount {
            id,
            seq: id,
            scope,
            extension,
            registrar: Mutex::new(registrar),
            active: Mutex::new(true),
            closing: Mutex::new(false),
            closed: Mutex::new(false),
            leases: AtomicUsize::new(0),
            released: Notify::new(),
            closed_notify: Notify::new(),
        });
        inner.mounts.push(Arc::clone(&mount));
        Ok(MountHandle { mount })
    }
    pub async fn close_all(&self) -> Result<(), ExtensionError> {
        let handles = self
            .inner
            .lock()
            .unwrap()
            .mounts
            .iter()
            .map(|mount| MountHandle {
                mount: Arc::clone(mount),
            })
            .collect::<Vec<_>>();
        for handle in handles.into_iter().rev() {
            handle.close().await?;
        }
        Ok(())
    }
    #[must_use]
    pub fn acquire(&self, session: &SessionId) -> RunPlan {
        let inner = self.inner.lock().unwrap();
        let mut mounts = Vec::new();
        for mount in &inner.mounts {
            let active = mount.active.lock().unwrap();
            if *active && mount.scope.applies(session) {
                #[cfg(test)]
                if let Some(hook) = self.acquire_hook.lock().unwrap().as_ref() {
                    hook();
                }
                mount.leases.fetch_add(1, Ordering::SeqCst);
                mounts.push(Arc::clone(mount));
            }
        }
        mounts.sort_by_key(|m| (m.scope.rank(), m.seq));
        let mut tools = vec![];
        let mut prompts = vec![];
        let mut guards = vec![];
        let mut restrictions = vec![];
        let mut handlers = vec![];
        let mut providers = vec![];
        let mut components = vec![];
        for mount in &mounts {
            let r = mount.registrar.lock().unwrap();
            tools.extend(r.tools.iter().cloned());
            for prompt in &r.prompts {
                prompts.retain(|old: &Arc<PromptSection>| old.name != prompt.name);
                prompts.push(Arc::clone(prompt));
            }
            guards.extend(r.guards.iter().cloned());
            restrictions.extend(r.restrictions.iter().cloned());
            handlers.extend(r.handlers.iter().cloned());
            providers.extend(r.providers.iter().cloned());
            components.push(ComponentIdentity {
                id: format!("extension:{}", mount.extension.id()),
                version: format!(
                    "{}:{}",
                    mount.extension.version(),
                    canonical_hash(&mount.extension.config_hash())
                ),
            });
            for t in &r.tools {
                components.push(ComponentIdentity {
                    id: format!("tool:{}", t.info.name),
                    version: canonical_json(&serde_json::to_value(&t.info).unwrap()).to_string(),
                });
            }
            for p in &r.prompts {
                components.push(ComponentIdentity {
                    id: format!("prompt:{}", p.name),
                    version: format!("{}:{}", p.order, p.text),
                });
            }
            for g in &r.guards {
                components.push(ComponentIdentity {
                    id: format!("guard:{}", g.id()),
                    version: String::new(),
                });
            }
            for h in &r.handlers {
                components.push(ComponentIdentity {
                    id: format!("handler:{}:{}", h.point, h.id),
                    version: format!("{}:{}", h.order, mount.seq),
                });
            }
            for provider in &r.providers {
                components.push(ComponentIdentity {
                    id: format!("provider:{}", provider.info().id),
                    version: provider.info().name,
                });
            }
            for restriction in &r.restrictions {
                components.push(ComponentIdentity {
                    id: format!("restriction:{}", restriction.join(",")),
                    version: String::new(),
                });
            }
        }
        prompts.sort_by(|a, b| (a.order, &a.name).cmp(&(b.order, &b.name)));
        let fingerprint = compute_fingerprint(&components);
        RunPlan::from_registry(
            fingerprint,
            tools,
            prompts,
            guards,
            restrictions,
            Dispatcher::new(handlers),
            providers,
            components,
            move || {
                for mount in &mounts {
                    if mount.leases.fetch_sub(1, Ordering::SeqCst) == 1 {
                        mount.released.notify_waiters();
                    }
                }
            },
        )
    }
}
fn scopes_overlap(a: &Scope, b: &Scope) -> bool {
    matches!(a, Scope::Global) || matches!(b, Scope::Global) || a == b
}
fn canonical_hash(hash: &str) -> String {
    serde_json::from_str::<serde_json::Value>(hash)
        .map_or_else(|_| hash.to_owned(), |v| canonical_json(&v).to_string())
}
fn canonical_json(value: &serde_json::Value) -> serde_json::Value {
    let mut canonical = value.clone();
    canonical.sort_all_objects();
    canonical
}
#[async_trait]
impl RunPlanProvider for Registry {
    async fn acquire_plan(&self, session: &SessionId) -> Result<RunPlan, ExtensionError> {
        Ok(self.acquire(session))
    }
}
#[derive(Clone)]
pub struct MountHandle {
    mount: Arc<Mount>,
}
impl MountHandle {
    pub fn deactivate(&self) {
        *self.mount.active.lock().unwrap() = false;
    }
    pub async fn close(&self) -> Result<(), ExtensionError> {
        if dispatch::is_active_mount(self.mount.id) {
            return Err(ExtensionError::SelfClose);
        }
        let leader = {
            let mut closing = self.mount.closing.lock().unwrap();
            let leader = !*closing;
            *closing = true;
            leader
        };
        self.deactivate();
        if leader {
            let mount = Arc::clone(&self.mount);
            tokio::spawn(async move {
                while mount.leases.load(Ordering::SeqCst) > 0 {
                    let notified = mount.released.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    if mount.leases.load(Ordering::SeqCst) == 0 {
                        break;
                    }
                    notified.await;
                }
                mount.registrar.lock().unwrap().rollback();
                mount.extension.shutdown().await;
                *mount.closed.lock().unwrap() = true;
                mount.closed_notify.notify_waiters();
            });
        }
        loop {
            let notified = self.mount.closed_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if *self.mount.closed.lock().unwrap() {
                return Ok(());
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Point, ToolExecutor};
    use crabber_core::ToolInfo;
    use serde_json::{Value, json};
    struct Echo;
    #[async_trait]
    impl ToolExecutor for Echo {
        async fn execute(&self, value: Value) -> Result<Value, ExtensionError> {
            Ok(value)
        }
    }
    struct TestExtension {
        id: &'static str,
        tool: bool,
        fail: bool,
        cleanup: Arc<AtomicUsize>,
    }
    #[async_trait]
    impl Extension for TestExtension {
        fn id(&self) -> &'static str {
            self.id
        }
        fn version(&self) -> &'static str {
            "1"
        }
        fn config_hash(&self) -> String {
            "{}".into()
        }
        async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
            let c = Arc::clone(&self.cleanup);
            r.defer(move || {
                c.fetch_add(1, Ordering::SeqCst);
            });
            if self.tool {
                r.tool(Arc::new(ToolDefinition { info:ToolInfo{name:"echo".into(),description:String::new(),parameters:json!({"type":"object","properties":{"b":{"type":"string"},"a":{"type":"string"}}}),retry_safe:true,required_permissions:vec![]},executor:Arc::new(Echo)}));
            }
            if self.fail {
                Err(ExtensionError::Plan("install".into()))
            } else {
                Ok(())
            }
        }
    }
    fn ext(
        id: &'static str,
        tool: bool,
        fail: bool,
        cleanup: Arc<AtomicUsize>,
    ) -> Arc<dyn Extension> {
        Arc::new(TestExtension {
            id,
            tool,
            fail,
            cleanup,
        })
    }
    #[tokio::test]
    async fn rollback_and_collision() {
        let registry = Registry::new();
        let count = Arc::new(AtomicUsize::new(0));
        assert!(
            registry
                .mount(ext("bad", true, true, Arc::clone(&count)), Scope::Global)
                .await
                .is_err()
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
        let _first = registry
            .mount(ext("first", true, false, Arc::clone(&count)), Scope::Global)
            .await
            .unwrap();
        assert!(matches!(
            registry
                .mount(
                    ext("second", true, false, Arc::clone(&count)),
                    Scope::Global
                )
                .await,
            Err(ExtensionError::ToolCollision(_))
        ));
        assert_eq!(count.load(Ordering::SeqCst), 2);
        assert_eq!(registry.acquire(&SessionId::new()).tools.len(), 1);
    }
    #[tokio::test]
    async fn close_waits_for_release() {
        let registry = Registry::new();
        let count = Arc::new(AtomicUsize::new(0));
        let handle = registry
            .mount(ext("one", false, false, Arc::clone(&count)), Scope::Global)
            .await
            .unwrap();
        let plan = registry.acquire(&SessionId::new());
        let task = tokio::spawn({
            let handle = handle.clone();
            async move {
                handle.close().await.unwrap();
            }
        });
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        plan.release();
        task.await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn acquire_lease_is_atomic_with_close_deactivation() {
        let registry = Registry::new();
        let shutdown = Arc::new(AtomicUsize::new(0));
        let handle = registry
            .mount(
                Arc::new(ShutdownExtension(Arc::clone(&shutdown))),
                Scope::Global,
            )
            .await
            .unwrap();
        let (observed_tx, observed_rx) = std::sync::mpsc::channel();
        let resume = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        *registry.acquire_hook.lock().unwrap() = Some(Arc::new({
            let resume = Arc::clone(&resume);
            move || {
                observed_tx.send(()).unwrap();
                let (lock, ready) = &*resume;
                let mut resumed = lock.lock().unwrap();
                while !*resumed {
                    resumed = ready.wait(resumed).unwrap();
                }
            }
        }));
        let acquiring = tokio::task::spawn_blocking({
            let registry = registry.clone();
            move || registry.acquire(&SessionId::new())
        });
        observed_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap();
        let closing = tokio::task::spawn_blocking({
            let handle = handle.clone();
            let runtime = tokio::runtime::Handle::current();
            move || runtime.block_on(handle.close())
        });
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !*handle.mount.closing.lock().unwrap() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let (lock, ready) = &*resume;
        *lock.lock().unwrap() = true;
        ready.notify_one();
        let plan = acquiring.await.unwrap();
        assert!(!closing.is_finished());
        assert_eq!(shutdown.load(Ordering::SeqCst), 0);
        plan.release();
        tokio::time::timeout(std::time::Duration::from_secs(1), closing)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(shutdown.load(Ordering::SeqCst), 1);
    }
    struct ConfigExtension(&'static str);
    #[async_trait]
    impl Extension for ConfigExtension {
        fn id(&self) -> &'static str {
            "config"
        }
        fn version(&self) -> &'static str {
            "1"
        }
        fn config_hash(&self) -> String {
            self.0.into()
        }
        async fn install(&self, _r: &mut Registrar) -> Result<(), ExtensionError> {
            Ok(())
        }
    }
    #[tokio::test]
    async fn stable_fingerprint_across_key_order() {
        let first = Registry::new();
        let second = Registry::new();
        let _a = first
            .mount(
                Arc::new(ConfigExtension(r#"{"a":1,"b":{"x":2,"y":3}}"#)),
                Scope::Global,
            )
            .await
            .unwrap();
        let _b = second
            .mount(
                Arc::new(ConfigExtension(r#"{"b":{"y":3,"x":2},"a":1}"#)),
                Scope::Global,
            )
            .await
            .unwrap();
        assert_eq!(
            first.acquire(&SessionId::new()).fingerprint,
            second.acquire(&SessionId::new()).fingerprint
        );
    }
    struct SelfClosing {
        handle: Arc<std::sync::OnceLock<MountHandle>>,
    }
    #[async_trait]
    impl Extension for SelfClosing {
        fn id(&self) -> &'static str {
            "self"
        }
        fn version(&self) -> &'static str {
            "1"
        }
        fn config_hash(&self) -> String {
            String::new()
        }
        async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
            let slot = Arc::clone(&self.handle);
            r.on_hook(
                crate::TurnPrepare::ID,
                0,
                "self-close",
                Arc::new(move |_| {
                    let handle = slot.get().unwrap().clone();
                    Box::pin(async move {
                        assert_eq!(handle.close().await, Err(ExtensionError::SelfClose));
                        Ok(Value::Null)
                    })
                }),
            );
            Ok(())
        }
    }
    #[tokio::test]
    async fn self_close_is_rejected() {
        let registry = Registry::new();
        let slot = Arc::new(std::sync::OnceLock::new());
        let handle = registry
            .mount(
                Arc::new(SelfClosing {
                    handle: Arc::clone(&slot),
                }),
                Scope::Global,
            )
            .await
            .unwrap();
        slot.set(handle.clone()).ok();
        let plan = registry.acquire(&SessionId::new());
        plan.dispatcher
            .hook::<crate::TurnPrepare>(Value::Null)
            .await
            .unwrap();
        plan.release();
        handle.close().await.unwrap();
    }
    struct ShutdownExtension(Arc<AtomicUsize>);
    #[async_trait]
    impl Extension for ShutdownExtension {
        fn id(&self) -> &'static str {
            "shutdown"
        }
        fn version(&self) -> &'static str {
            "1"
        }
        fn config_hash(&self) -> String {
            String::new()
        }
        async fn install(&self, _: &mut Registrar) -> Result<(), ExtensionError> {
            Ok(())
        }
        async fn shutdown(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct OrderedClose(Arc<Mutex<Vec<&'static str>>>);
    #[async_trait]
    impl Extension for OrderedClose {
        fn id(&self) -> &'static str {
            "ordered"
        }
        fn version(&self) -> &'static str {
            "1"
        }
        fn config_hash(&self) -> String {
            String::new()
        }
        async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
            let log = Arc::clone(&self.0);
            r.defer(move || log.lock().unwrap().push("cleanup"));
            Ok(())
        }
        async fn shutdown(&self) {
            self.0.lock().unwrap().push("shutdown");
        }
    }
    // Expected to change with crabber-b4zy (D7: close waits on the cleanup tracker under a bound).
    #[tokio::test]
    async fn characterize_close_orders_lease_wait_cleanup_shutdown() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let registry = Registry::new();
        let handle = registry
            .mount(Arc::new(OrderedClose(Arc::clone(&log))), Scope::Global)
            .await
            .unwrap();
        let plan = registry.acquire(&SessionId::new());
        let task = tokio::spawn({
            let handle = handle.clone();
            async move { handle.close().await }
        });
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        assert!(log.lock().unwrap().is_empty());
        log.lock().unwrap().push("release");
        plan.release();
        task.await.unwrap().unwrap();
        assert_eq!(*log.lock().unwrap(), ["release", "cleanup", "shutdown"]);
    }

    #[tokio::test]
    async fn canceled_first_close_still_finishes_once() {
        let count = Arc::new(AtomicUsize::new(0));
        let registry = Registry::new();
        let handle = registry
            .mount(
                Arc::new(ShutdownExtension(Arc::clone(&count))),
                Scope::Global,
            )
            .await
            .unwrap();
        let plan = registry.acquire(&SessionId::new());
        let first = tokio::spawn({
            let handle = handle.clone();
            async move { handle.close().await }
        });
        tokio::task::yield_now().await;
        first.abort();
        let _ = first.await;
        plan.release();
        tokio::time::timeout(std::time::Duration::from_secs(1), handle.close())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
    struct ReentrantCollision(Registry);
    #[async_trait]
    impl Extension for ReentrantCollision {
        fn id(&self) -> &'static str {
            "reentrant"
        }
        fn version(&self) -> &'static str {
            "1"
        }
        fn config_hash(&self) -> String {
            String::new()
        }
        async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
            let registry = self.0.clone();
            r.defer(move || {
                let _ = registry.acquire(&SessionId::new());
            });
            r.tool(Arc::new(ToolDefinition {
                info: ToolInfo {
                    name: "echo".into(),
                    description: String::new(),
                    parameters: json!({"type":"object"}),
                    retry_safe: true,
                    required_permissions: vec![],
                },
                executor: Arc::new(Echo),
            }));
            Ok(())
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn collision_cleanup_can_reenter_registry() {
        let registry = Registry::new();
        let count = Arc::new(AtomicUsize::new(0));
        let _first = registry
            .mount(ext("first", true, false, count), Scope::Global)
            .await
            .unwrap();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            registry.mount(
                Arc::new(ReentrantCollision(registry.clone())),
                Scope::Global,
            ),
        )
        .await
        .unwrap();
        assert!(matches!(result, Err(ExtensionError::ToolCollision(_))));
    }
    struct Outer;
    #[async_trait]
    impl Extension for Outer {
        fn id(&self) -> &'static str {
            "outer"
        }
        fn version(&self) -> &'static str {
            "1"
        }
        fn config_hash(&self) -> String {
            String::new()
        }
        async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
            r.on_around(
                crate::ToolExecute::ID,
                0,
                "outer",
                Arc::new(|value, next| Box::pin(async move { next.call(value).await })),
            );
            Ok(())
        }
    }
    struct Inner(Arc<std::sync::OnceLock<MountHandle>>);
    #[async_trait]
    impl Extension for Inner {
        fn id(&self) -> &'static str {
            "inner"
        }
        fn version(&self) -> &'static str {
            "1"
        }
        fn config_hash(&self) -> String {
            String::new()
        }
        async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
            let slot = Arc::clone(&self.0);
            r.on_around(
                crate::ToolExecute::ID,
                1,
                "inner",
                Arc::new(move |value, next| {
                    let handle = slot.get().unwrap().clone();
                    Box::pin(async move {
                        assert_eq!(handle.close().await, Err(ExtensionError::SelfClose));
                        next.call(value).await
                    })
                }),
            );
            Ok(())
        }
    }
    #[tokio::test]
    async fn nested_mount_stack_rejects_outer_close() {
        let registry = Registry::new();
        let slot = Arc::new(std::sync::OnceLock::new());
        let outer = registry
            .mount(Arc::new(Outer), Scope::Global)
            .await
            .unwrap();
        slot.set(outer.clone()).ok();
        let inner = registry
            .mount(Arc::new(Inner(slot)), Scope::Global)
            .await
            .unwrap();
        let plan = registry.acquire(&SessionId::new());
        let terminal: crate::Callback = Arc::new(|value| Box::pin(async move { Ok(value) }));
        let output = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            plan.dispatcher
                .around::<crate::ToolExecute>(Value::Null, terminal),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(output, Value::Null);
        plan.release();
        outer.close().await.unwrap();
        inner.close().await.unwrap();
    }
    type DeferredNextResult =
        Arc<Mutex<Option<tokio::sync::oneshot::Sender<Result<Value, ExtensionError>>>>>;
    struct DetachedExtension {
        gate: Arc<tokio::sync::Notify>,
        result: DeferredNextResult,
    }
    #[async_trait]
    impl Extension for DetachedExtension {
        fn id(&self) -> &'static str {
            "detached"
        }
        fn version(&self) -> &'static str {
            "1"
        }
        fn config_hash(&self) -> String {
            String::new()
        }
        async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
            let gate = Arc::clone(&self.gate);
            let result = Arc::clone(&self.result);
            r.on_around(
                crate::ToolExecute::ID,
                0,
                "detach",
                Arc::new(move |_, next| {
                    let gate = Arc::clone(&gate);
                    let result = Arc::clone(&result);
                    Box::pin(async move {
                        tokio::spawn(async move {
                            gate.notified().await;
                            let output = next.call(Value::Null).await;
                            let _ = result.lock().unwrap().take().unwrap().send(output);
                        });
                        Ok(Value::Null)
                    })
                }),
            );
            Ok(())
        }
    }
    #[tokio::test]
    async fn delayed_next_cannot_run_after_release_and_close() {
        let registry = Registry::new();
        let gate = Arc::new(tokio::sync::Notify::new());
        let (send, receive) = tokio::sync::oneshot::channel();
        let handle = registry
            .mount(
                Arc::new(DetachedExtension {
                    gate: Arc::clone(&gate),
                    result: Arc::new(Mutex::new(Some(send))),
                }),
                Scope::Global,
            )
            .await
            .unwrap();
        let plan = registry.acquire(&SessionId::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let terminal: crate::Callback = {
            let calls = Arc::clone(&calls);
            Arc::new(move |value| {
                let calls = Arc::clone(&calls);
                Box::pin(async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(value)
                })
            })
        };
        assert_eq!(
            plan.dispatcher
                .around::<crate::ToolExecute>(Value::Null, terminal)
                .await,
            Err(ExtensionError::NextNotCalled)
        );
        plan.release();
        handle.close().await.unwrap();
        gate.notify_one();
        assert_eq!(receive.await.unwrap(), Err(ExtensionError::NextExpired));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}
