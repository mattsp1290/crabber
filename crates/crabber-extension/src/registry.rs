//! Atomic native extension mounting and immutable plan acquisition.
#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
use crate::dispatch::{
    self, AroundCallback, Callback, Dispatcher, Handler, HandlerFn, Mode, Point,
    ToolResultTransform,
};
use crate::{
    CleanupTracker, ComponentIdentity, ExtensionError, PromptSection, ResultTransformCallback,
    RunPlan, RunPlanProvider, ToolDefinition, TransformPhase, compute_fingerprint,
    json_result_transform,
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
    time::Duration,
};
use tokio::sync::Notify;
use tokio_util::{sync::CancellationToken, task::TaskTracker};

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
        self.handler_in_phase(point, mode, order, id, TransformPhase::Ordinary, callback);
    }
    fn handler_in_phase(
        &mut self,
        point: &'static str,
        mode: Mode,
        order: i32,
        id: impl Into<String>,
        phase: TransformPhase,
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
            phase,
            cleanup: CleanupTracker::detached(),
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
    /// Typed result-transform handler in the ordinary phase.
    pub fn on_result_transform(
        &mut self,
        order: i32,
        id: impl Into<String>,
        cb: ResultTransformCallback,
    ) {
        self.handler_in_phase(
            ToolResultTransform::ID,
            Mode::Transform,
            order,
            id,
            TransformPhase::Ordinary,
            HandlerFn::ResultTransform(cb),
        );
    }
    /// Typed result-transform handler in the final-redaction phase.
    pub fn on_final_redaction(
        &mut self,
        order: i32,
        id: impl Into<String>,
        cb: ResultTransformCallback,
    ) {
        self.handler_in_phase(
            ToolResultTransform::ID,
            Mode::Transform,
            order,
            id,
            TransformPhase::FinalRedaction,
            HandlerFn::ResultTransform(cb),
        );
    }
    /// JSON envelope handler in the final-redaction phase.
    pub fn on_final_redaction_json(&mut self, order: i32, id: impl Into<String>, cb: Callback) {
        self.on_final_redaction(order, id, json_result_transform(cb));
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
fn handler_version(h: &Handler, mount_seq: u64) -> String {
    if h.point != ToolResultTransform::ID {
        return format!("{}:{mount_seq}", h.order);
    }
    let phase = match h.phase {
        TransformPhase::Ordinary => "ordinary",
        TransformPhase::FinalRedaction => "final_redaction",
    };
    format!("{}:{mount_seq}:{phase}", h.order)
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
    /// Close and join belong to crabber-b4zy's bounded close.
    #[allow(dead_code)]
    cleanup: CleanupOwner,
}
/// Owner side of a [`CleanupTracker`]. A mount holds one; a host with a
/// `ToolPipeline` may hold one.
#[derive(Debug, Default)]
pub struct CleanupOwner {
    tasks: TaskTracker,
    closing: CancellationToken,
}
impl CleanupOwner {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    /// A handle on this owner's tasks and close signal. Every call returns a
    /// handle to the same tracker.
    #[must_use]
    pub fn tracker(&self) -> CleanupTracker {
        CleanupTracker::from_parts(self.tasks.clone(), self.closing.clone())
    }
    /// Sends the close signal. Idempotent. Never aborts a task.
    pub fn close(&self) {
        self.closing.cancel();
    }
    /// `close()`, then waits for every tracked task, at most `bound`. Tasks
    /// keep running after a timeout and a later call can still succeed.
    pub async fn join(&self, bound: Duration) -> Result<(), CleanupJoinTimeout> {
        self.close();
        // `wait` resolves only once the tracker is closed and empty. Closing
        // the tracker does not stop `spawn`: late cleanup still runs and counts.
        self.tasks.close();
        tokio::time::timeout(bound, self.tasks.wait())
            .await
            .map_err(|_| CleanupJoinTimeout {
                pending: self.tasks.len(),
            })
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CleanupJoinTimeout {
    pub pending: usize,
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
        let cleanup = CleanupOwner::new();
        for handler in &mut registrar.handlers {
            handler.cleanup = cleanup.tracker();
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
            cleanup,
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
                    version: handler_version(h, mount.seq),
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
    struct RedactorExtension {
        phase: TransformPhase,
        json: bool,
    }
    #[async_trait]
    impl Extension for RedactorExtension {
        fn id(&self) -> &'static str {
            "redactor"
        }
        fn version(&self) -> &'static str {
            "1"
        }
        fn config_hash(&self) -> String {
            String::new()
        }
        async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
            let typed: ResultTransformCallback = Arc::new(|_, result| {
                Box::pin(std::future::ready(Ok(crate::TransformOutput::new(result))))
            });
            let json_cb: Callback = Arc::new(|v| Box::pin(std::future::ready(Ok(v))));
            match (self.phase, self.json) {
                (TransformPhase::Ordinary, false) => r.on_result_transform(3, "red", typed),
                (TransformPhase::FinalRedaction, false) => r.on_final_redaction(3, "red", typed),
                (TransformPhase::Ordinary, true) => {
                    r.on_transform(ToolResultTransform::ID, 3, "red", json_cb);
                }
                (TransformPhase::FinalRedaction, true) => {
                    r.on_final_redaction_json(3, "red", json_cb);
                }
            }
            Ok(())
        }
    }
    async fn redactor_registry(phase: TransformPhase, json: bool) -> (Registry, MountHandle) {
        let registry = Registry::new();
        let handle = registry
            .mount(Arc::new(RedactorExtension { phase, json }), Scope::Global)
            .await
            .unwrap();
        (registry, handle)
    }
    #[tokio::test]
    async fn result_transform_registrations_carry_phase_and_callback_kind() {
        for (phase, json) in [
            (TransformPhase::Ordinary, false),
            (TransformPhase::FinalRedaction, false),
            (TransformPhase::Ordinary, true),
            (TransformPhase::FinalRedaction, true),
        ] {
            let (_registry, handle) = redactor_registry(phase, json).await;
            let r = handle.mount.registrar.lock().unwrap();
            let h = &r.handlers[0];
            assert_eq!(h.point, ToolResultTransform::ID);
            assert_eq!(h.mode, Mode::Transform);
            assert_eq!(h.order, 3);
            assert_eq!(h.phase, phase);
            // on_transform stays an untyped callback; every other path is typed.
            assert_eq!(
                matches!(h.callback, HandlerFn::Ordinary(_)),
                json && phase == TransformPhase::Ordinary
            );
            assert_eq!(
                matches!(h.callback, HandlerFn::ResultTransform(_)),
                !(json && phase == TransformPhase::Ordinary)
            );
        }
    }
    #[tokio::test]
    async fn phase_participates_in_the_plan_fingerprint() {
        let fp = |phase, json| async move {
            let (registry, _handle) = redactor_registry(phase, json).await;
            registry.acquire(&SessionId::new()).fingerprint
        };
        let ordinary = fp(TransformPhase::Ordinary, false).await;
        let final_phase = fp(TransformPhase::FinalRedaction, false).await;
        assert_ne!(ordinary, final_phase);
        assert_eq!(final_phase, fp(TransformPhase::FinalRedaction, false).await);
        // The callback kind is not identity; the phase is.
        assert_eq!(final_phase, fp(TransformPhase::FinalRedaction, true).await);
        assert_eq!(ordinary, fp(TransformPhase::Ordinary, true).await);
    }
    #[test]
    fn only_the_result_transform_handler_version_carries_a_phase() {
        let mut registrar = Registrar::new();
        let cb: Callback = Arc::new(|v| Box::pin(std::future::ready(Ok(v))));
        registrar.on_hook(crate::TurnPrepare::ID, 4, "h", Arc::clone(&cb));
        registrar.on_transform(ToolResultTransform::ID, 4, "t", cb);
        assert_eq!(handler_version(&registrar.handlers[0], 7), "4:7");
        assert_eq!(handler_version(&registrar.handlers[1], 7), "4:7:ordinary");
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

    mod cleanup {
        use super::*;
        use crate::{
            ToolInput, ToolOutcomeClass, ToolResultContext, ToolResultOutcome, TransformOutput,
        };
        use tokio::sync::oneshot;

        const BOUND: Duration = Duration::from_secs(5);

        fn context() -> ToolResultContext {
            ToolResultContext::new(
                "echo".into(),
                true,
                ToolInput::Normalized(json!({})),
                ToolCallId::from("call-1"),
                SessionId::from("session-1"),
                RunId::from("run-1"),
                ToolOutcomeClass::Succeeded,
            )
        }
        fn seed() -> TransformOutput {
            TransformOutput::new(json!("s"))
        }

        /// Registers two ordinary result handlers that record the tracker they see.
        struct Capture {
            id: &'static str,
            seen: Arc<Mutex<Vec<CleanupTracker>>>,
        }
        #[async_trait]
        impl Extension for Capture {
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
                for (order, name) in [(0, "first"), (1, "second")] {
                    let seen = Arc::clone(&self.seen);
                    r.on_result_transform(
                        order,
                        format!("{}-{name}", self.id),
                        Arc::new(move |context, value| {
                            let seen = Arc::clone(&seen);
                            Box::pin(async move {
                                seen.lock().unwrap().push(context.cleanup().clone());
                                Ok(TransformOutput::new(value))
                            })
                        }),
                    );
                }
                Ok(())
            }
        }
        struct Spawner {
            registered: Arc<tokio::sync::Notify>,
            gate: Arc<Mutex<Option<oneshot::Receiver<()>>>>,
            finished: Arc<AtomicUsize>,
        }
        #[async_trait]
        impl Extension for Spawner {
            fn id(&self) -> &'static str {
                "spawner"
            }
            fn version(&self) -> &'static str {
                "1"
            }
            fn config_hash(&self) -> String {
                "{}".into()
            }
            async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
                let (registered, gate, finished) = (
                    Arc::clone(&self.registered),
                    Arc::clone(&self.gate),
                    Arc::clone(&self.finished),
                );
                r.on_result_transform(
                    0,
                    "spawner",
                    Arc::new(move |context, _| {
                        let (registered, gate, finished) = (
                            Arc::clone(&registered),
                            Arc::clone(&gate),
                            Arc::clone(&finished),
                        );
                        Box::pin(async move {
                            let gate = gate.lock().unwrap().take().unwrap();
                            context.cleanup().spawn(async move {
                                gate.await.ok();
                                finished.fetch_add(1, Ordering::SeqCst);
                            });
                            registered.notify_one();
                            std::future::pending().await
                        })
                    }),
                );
                Ok(())
            }
        }

        async fn mount_capture(
            registry: &Registry,
            id: &'static str,
        ) -> (MountHandle, Arc<Mutex<Vec<CleanupTracker>>>) {
            let seen = Arc::new(Mutex::new(vec![]));
            let handle = registry
                .mount(
                    Arc::new(Capture {
                        id,
                        seen: Arc::clone(&seen),
                    }),
                    Scope::Global,
                )
                .await
                .unwrap();
            (handle, seen)
        }

        #[tokio::test]
        async fn spawned_task_is_tracked_and_survives_a_dropped_handle() {
            let owner = CleanupOwner::new();
            let tracker = owner.tracker();
            let (release, gate) = oneshot::channel::<()>();
            let (done_tx, done_rx) = oneshot::channel::<()>();
            drop(tracker.spawn(async move {
                gate.await.ok();
                done_tx.send(()).ok();
            }));
            assert_eq!(tracker.pending(), 1);
            release.send(()).unwrap();
            done_rx.await.unwrap();
            assert_eq!(owner.join(BOUND).await, Ok(()));
            assert_eq!(tracker.pending(), 0);
        }

        #[tokio::test]
        async fn handler_cleanup_completes_after_its_future_is_dropped() {
            let registry = Registry::new();
            let registered = Arc::new(tokio::sync::Notify::new());
            let (release, gate) = oneshot::channel::<()>();
            let gate = Arc::new(Mutex::new(Some(gate)));
            let finished = Arc::new(AtomicUsize::new(0));
            let handle = registry
                .mount(
                    Arc::new(Spawner {
                        registered: Arc::clone(&registered),
                        gate,
                        finished: Arc::clone(&finished),
                    }),
                    Scope::Global,
                )
                .await
                .unwrap();
            let plan = registry.acquire(&SessionId::new());
            tokio::select! {
                _ = plan.dispatcher.transform_tool_result(context(), seed()) => unreachable!(),
                () = registered.notified() => {}
            }
            // The driver future, and with it the callback future, is dropped.
            assert_eq!(handle.mount.cleanup.tracker().pending(), 1);
            assert_eq!(finished.load(Ordering::SeqCst), 0);
            release.send(()).unwrap();
            assert_eq!(handle.mount.cleanup.join(BOUND).await, Ok(()));
            assert_eq!(finished.load(Ordering::SeqCst), 1);
            plan.release();
            handle.close().await.unwrap();
        }

        #[tokio::test]
        async fn mounts_get_distinct_trackers_and_handlers_of_a_mount_share_one() {
            let registry = Registry::new();
            let (a, seen_a) = mount_capture(&registry, "a").await;
            let (b, seen_b) = mount_capture(&registry, "b").await;
            let plan = registry.acquire(&SessionId::new());
            let outcome = plan
                .dispatcher
                .transform_tool_result(context(), seed())
                .await;
            assert!(matches!(outcome, ToolResultOutcome::Completed { .. }));
            let (seen_a, seen_b) = (
                seen_a.lock().unwrap().clone(),
                seen_b.lock().unwrap().clone(),
            );
            assert_eq!((seen_a.len(), seen_b.len()), (2, 2));

            // A task spawned through A's handler context is pending on A only.
            let (release, gate) = oneshot::channel::<()>();
            let task = seen_a[0].spawn(async move {
                gate.await.ok();
            });
            assert_eq!(a.mount.cleanup.tracker().pending(), 1);
            assert_eq!(b.mount.cleanup.tracker().pending(), 0);
            assert_eq!(seen_a[1].pending(), 1, "same mount shares one tracker");
            assert_eq!(seen_b[0].pending(), 0);
            assert_eq!(seen_b[1].pending(), 0);

            // Closing A's owner signals A's handlers' closing() and not B's.
            assert!(!seen_a[1].is_closing());
            a.mount.cleanup.close();
            assert!(seen_a.iter().all(CleanupTracker::is_closing));
            assert!(seen_b.iter().all(|t| !t.is_closing()));
            tokio::time::timeout(BOUND, seen_a[0].closing())
                .await
                .unwrap();

            release.send(()).unwrap();
            task.await.unwrap();
            plan.release();
            a.close().await.unwrap();
            b.close().await.unwrap();
        }

        #[tokio::test]
        async fn closing_a_mount_owner_fails_only_that_mounts_handlers() {
            let registry = Registry::new();
            let (a, seen_a) = mount_capture(&registry, "a").await;
            let (b, seen_b) = mount_capture(&registry, "b").await;
            let plan = registry.acquire(&SessionId::new());
            // Order is a-first, b-first, a-second, b-second: B's owner closed
            // fails B's first handler after A's first one ran.
            b.mount.cleanup.close();
            let outcome = plan
                .dispatcher
                .transform_tool_result(context(), seed())
                .await;
            assert!(
                matches!(&outcome, ToolResultOutcome::Failed { handler } if handler.contains("b-first")),
                "{outcome:?}"
            );
            assert_eq!(seen_a.lock().unwrap().len(), 1);
            assert!(seen_b.lock().unwrap().is_empty());
            seen_a.lock().unwrap().clear();
            a.mount.cleanup.close();
            let outcome = plan
                .dispatcher
                .transform_tool_result(context(), seed())
                .await;
            assert!(
                matches!(&outcome, ToolResultOutcome::Failed { handler } if handler.contains("a-first")),
                "{outcome:?}"
            );
            assert!(seen_a.lock().unwrap().is_empty());
            plan.release();
            a.close().await.unwrap();
            b.close().await.unwrap();
        }

        #[tokio::test(start_paused = true)]
        async fn join_times_out_then_succeeds_once_the_task_finishes() {
            let owner = CleanupOwner::new();
            let (release, gate) = oneshot::channel::<()>();
            let finished = Arc::new(AtomicUsize::new(0));
            let task = owner.tracker().spawn({
                let finished = Arc::clone(&finished);
                async move {
                    gate.await.ok();
                    finished.fetch_add(1, Ordering::SeqCst);
                }
            });
            assert_eq!(
                owner.join(Duration::from_secs(1)).await,
                Err(CleanupJoinTimeout { pending: 1 })
            );
            assert!(owner.tracker().is_closing());
            assert!(!task.is_finished());
            assert_eq!(finished.load(Ordering::SeqCst), 0);
            release.send(()).unwrap();
            task.await.unwrap();
            assert_eq!(finished.load(Ordering::SeqCst), 1);
            assert_eq!(owner.join(Duration::from_secs(1)).await, Ok(()));
        }

        #[tokio::test]
        async fn join_with_no_tasks_is_ok_and_close_is_idempotent() {
            let owner = CleanupOwner::default();
            let tracker = owner.tracker();
            assert!(!tracker.is_closing());
            owner.close();
            owner.close();
            assert!(tracker.is_closing());
            assert_eq!(owner.join(BOUND).await, Ok(()));
            assert_eq!(owner.join(BOUND).await, Ok(()));
        }

        #[tokio::test]
        async fn spawn_after_close_still_runs_and_is_tracked() {
            let owner = CleanupOwner::new();
            let tracker = owner.tracker();
            assert_eq!(owner.join(BOUND).await, Ok(()));
            let (release, gate) = oneshot::channel::<()>();
            let ran = Arc::new(AtomicUsize::new(0));
            let task = tracker.spawn({
                let ran = Arc::clone(&ran);
                async move {
                    gate.await.ok();
                    ran.fetch_add(1, Ordering::SeqCst);
                }
            });
            assert_eq!(tracker.pending(), 1);
            release.send(()).unwrap();
            assert_eq!(owner.join(BOUND).await, Ok(()));
            task.await.unwrap();
            assert_eq!(ran.load(Ordering::SeqCst), 1);
        }

        #[tokio::test]
        async fn existing_close_path_does_not_touch_the_owner() {
            let registry = Registry::new();
            let (a, seen) = mount_capture(&registry, "a").await;
            let tracker = a.mount.cleanup.tracker();
            a.close().await.unwrap();
            assert!(!tracker.is_closing());
            assert!(seen.lock().unwrap().is_empty());
        }
    }
}
