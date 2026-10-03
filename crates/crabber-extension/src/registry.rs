//! Atomic native extension mounting and immutable plan acquisition.
#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
use crate::dispatch::{
    self, AroundCallback, Callback, Dispatcher, Handler, HandlerFn, Mode, Point,
    ToolResultTransform,
};
use crate::{
    CleanupTracker, ComponentIdentity, DEFAULT_MOUNT_CLOSE_TIMEOUT, ExtensionError, PromptSection,
    ResultTransformCallback, RunPlan, RunPlanProvider, ToolDefinition, TransformPhase,
    compute_fingerprint, json_result_transform,
};
use async_trait::async_trait;
use crabber_core::{RunId, SessionId, ToolCallId, ToolInfo};
use crabber_providers::ProviderAdapter;
use futures::FutureExt;
use std::{
    collections::HashSet,
    panic::AssertUnwindSafe,
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
    /// Set by the close task once leases and the tracker are drained, before
    /// rollback and `shutdown`. The bound on `close` covers this wait only.
    drained: Mutex<bool>,
    drained_notify: Notify,
    cleanup: CleanupOwner,
    close_timeout: Duration,
    close_observer: Option<MountCloseObserver>,
    /// Runs once, right after the tracker first drains, before the re-check.
    #[cfg(test)]
    drain_hook: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}
impl Mount {
    /// Waits until no plan lease is held.
    async fn leases_released(&self) {
        while self.leases.load(Ordering::SeqCst) > 0 {
            let notified = self.released.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.leases.load(Ordering::SeqCst) == 0 {
                break;
            }
            notified.await;
        }
    }
    /// The mount is deactivated and signalled: waits for leases, then joins the
    /// tracker, and re-checks both, because a callback that was still in flight
    /// when the tracker drained can spawn late cleanup (`join` alone would miss
    /// it). Never bounded and never aborts a task; the caller bounds its wait.
    /// Runs the deferred cleanups in reverse order with no lock held, so a
    /// panicking one neither poisons the registrar nor skips the rest.
    fn rollback_contained(&self) {
        loop {
            let next = self
                .registrar
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .cleanups
                .pop();
            let Some(cleanup) = next else { break };
            let _ = std::panic::catch_unwind(AssertUnwindSafe(cleanup));
        }
    }
    async fn drain(&self) {
        loop {
            self.leases_released().await;
            self.cleanup.wait_drained().await;
            #[cfg(test)]
            if let Some(hook) = self.drain_hook.lock().unwrap().take() {
                hook();
            }
            if self.leases.load(Ordering::SeqCst) == 0 && self.cleanup.tracker().pending() == 0 {
                return;
            }
        }
    }
}
async fn wait_flag(flag: &Mutex<bool>, notify: &Notify) {
    loop {
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if *flag.lock().unwrap() {
            return;
        }
        notified.await;
    }
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
    /// Closes the tracker and waits, unbounded, until it is empty.
    async fn wait_drained(&self) {
        self.tasks.close();
        self.tasks.wait().await;
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
/// A `MountHandle::close` that did not finish within its bound. The close
/// task keeps running and finishes the close when the work it waits for ends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MountCloseTimeout {
    /// `Extension::id()`.
    pub extension: String,
    pub bound: Duration,
    /// Plan leases still held when the bound expired.
    pub leases: usize,
    /// Tracked cleanup tasks still running when the bound expired.
    pub pending_tasks: usize,
}
pub type MountCloseObserver = Arc<dyn Fn(&MountCloseTimeout) + Send + Sync>;
struct RegistryInner {
    mounts: Vec<Arc<Mount>>,
    next_id: u64,
    /// Terminal: set by `close_all` under this mutex, never cleared.
    closed: bool,
    close_timeout: Duration,
    close_observer: Option<MountCloseObserver>,
}
impl Default for RegistryInner {
    fn default() -> Self {
        Self {
            mounts: Vec::new(),
            next_id: 0,
            closed: false,
            close_timeout: DEFAULT_MOUNT_CLOSE_TIMEOUT,
            close_observer: None,
        }
    }
}
#[cfg(test)]
type AcquireHook = Arc<dyn Fn() + Send + Sync>;
/// Runs inside `close_all`'s critical section, after the deactivation loop.
#[cfg(test)]
type CloseHook = Arc<dyn Fn(&RegistryInner) + Send + Sync>;
#[derive(Clone, Default)]
pub struct Registry {
    inner: Arc<Mutex<RegistryInner>>,
    #[cfg(test)]
    acquire_hook: Arc<Mutex<Option<AcquireHook>>>,
    #[cfg(test)]
    close_hook: Arc<Mutex<Option<CloseHook>>>,
    /// Runs at the start of `close_all`, before it takes the registry mutex.
    #[cfg(test)]
    close_start_hook: Arc<Mutex<Option<AcquireHook>>>,
}
impl Registry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    /// Bound on each mount's close, shared by every clone. Set it before the
    /// first mount: mounts take the setting when they are mounted.
    #[must_use]
    pub fn with_close_timeout(self, bound: Duration) -> Self {
        self.inner.lock().unwrap().close_timeout = bound;
        self
    }
    /// Called once per `close` that times out, shared by every clone. Set it
    /// before the first mount.
    #[must_use]
    pub fn with_close_observer(self, observer: MountCloseObserver) -> Self {
        self.inner.lock().unwrap().close_observer = Some(observer);
        self
    }
    pub async fn mount(
        &self,
        extension: Arc<dyn Extension>,
        scope: Scope,
    ) -> Result<MountHandle, ExtensionError> {
        if self.inner.lock().unwrap().closed {
            return Err(ExtensionError::RegistryClosed);
        }
        let mut registrar = Registrar::new();
        if let Err(error) = extension.install(&mut registrar).await {
            registrar.rollback();
            return Err(error);
        }
        let mut inner = self.inner.lock().unwrap();
        if inner.closed {
            drop(inner);
            registrar.rollback();
            return Err(ExtensionError::RegistryClosed);
        }
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
            drained: Mutex::new(false),
            drained_notify: Notify::new(),
            cleanup,
            close_timeout: inner.close_timeout,
            close_observer: inner.close_observer.clone(),
            #[cfg(test)]
            drain_hook: Mutex::new(None),
        });
        inner.mounts.push(Arc::clone(&mount));
        Ok(MountHandle { mount })
    }
    /// Closes every mount and makes the registry terminal. Await it to
    /// completion.
    ///
    /// The registry is closed for every clone before any mount is deactivated,
    /// in one critical section under the mutex `acquire` takes, so no plan can
    /// hold some mounts but not a closed one. Afterwards `try_acquire`,
    /// `acquire_plan` and `mount` return `RegistryClosed` and `acquire` panics;
    /// there is no reopen, also when this returns an error. Then every mount is
    /// signalled, before any is joined.
    ///
    /// The joins run in one detached task, so dropping this future after the
    /// terminal state is set never leaves a signalled mount without a close
    /// task: the task keeps going and every mount still gets closed. Mounts are
    /// closed sequentially in reverse mount order, each under its own bound
    /// (see [`MountHandle::close`]); the next mount's close starts once the
    /// previous one finished or timed out. When no close times out, rollbacks
    /// and shutdowns therefore happen in reverse mount order. A mount whose
    /// close timed out finishes later in its reaper, possibly after an earlier
    /// mount. The first error is returned after every mount was attempted.
    ///
    /// Returns `SelfClose`, changing nothing, when called from a callback of
    /// one of this registry's mounts.
    pub async fn close_all(&self) -> Result<(), ExtensionError> {
        #[cfg(test)]
        if let Some(hook) = self.close_start_hook.lock().unwrap().clone() {
            hook();
        }
        let mounts = {
            let mut inner = self.inner.lock().unwrap();
            if inner.mounts.iter().any(|m| dispatch::is_active_mount(m.id)) {
                return Err(ExtensionError::SelfClose);
            }
            inner.closed = true;
            for mount in &inner.mounts {
                *mount.active.lock().unwrap() = false;
            }
            #[cfg(test)]
            if let Some(hook) = self.close_hook.lock().unwrap().clone() {
                hook(&inner);
            }
            inner.mounts.clone()
        };
        for mount in &mounts {
            mount.cleanup.close();
        }
        let driver = tokio::spawn(async move {
            let mut first = Ok(());
            for mount in mounts.into_iter().rev() {
                let result = MountHandle { mount }.close().await;
                if first.is_ok() {
                    first = result;
                }
            }
            first
        });
        match driver.await {
            Ok(result) => result,
            Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
            // The runtime is shutting down: nothing is left to wait for.
            Err(_) => Ok(()),
        }
    }
    /// `Err(ExtensionError::RegistryClosed)` once `close_all` has started.
    pub fn try_acquire(&self, session: &SessionId) -> Result<RunPlan, ExtensionError> {
        let inner = self.inner.lock().unwrap();
        if inner.closed {
            return Err(ExtensionError::RegistryClosed);
        }
        Ok(self.plan_for(&inner, session))
    }
    /// `try_acquire` unwrapped.
    ///
    /// # Panics
    /// When the registry is closed.
    #[must_use]
    pub fn acquire(&self, session: &SessionId) -> RunPlan {
        self.try_acquire(session)
            .expect("Registry::acquire on a closed registry")
    }
    // `self` is read only by the test acquire hook.
    #[cfg_attr(not(test), allow(clippy::unused_self))]
    fn plan_for(&self, inner: &RegistryInner, session: &SessionId) -> RunPlan {
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
        self.try_acquire(session)
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
    /// Closes this mount: deactivates it, signals its cleanup tracker (which
    /// also fails its result-transform invocations), waits for plan leases and
    /// the tracker, then runs deferred cleanups and `Extension::shutdown`.
    ///
    /// This is NOT terminal for the registry: new plans simply omit this mount.
    /// Do not close a redactor's mount this way while runs are still admitted;
    /// use `Registry::close_all`, which makes the registry terminal first.
    ///
    /// The registry's close bound covers only the drain of plan leases and the
    /// tracker. If it expires this returns `MountCloseTimeout`, reports it to
    /// the close observer and aborts nothing: the detached close task is the
    /// reaper that keeps waiting and then finishes the close, and a later
    /// `close` waits again under the bound. Once drained, rollback and
    /// `shutdown` run unbounded, and a panic in either is contained: the mount
    /// still ends closed and `close` returns `Ok`.
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
        let mount = &self.mount;
        if leader {
            mount.cleanup.close();
            // The detached task is the process-lifetime reaper: it outlives a
            // cancelled or timed-out `close` and never aborts anything.
            let mount = Arc::clone(mount);
            tokio::spawn(async move {
                mount.drain().await;
                *mount.drained.lock().unwrap() = true;
                mount.drained_notify.notify_waiters();
                mount.rollback_contained();
                // A panicking shutdown must not leave the mount unclosed.
                let _ = AssertUnwindSafe(mount.extension.shutdown())
                    .catch_unwind()
                    .await;
                *mount.closed.lock().unwrap() = true;
                mount.closed_notify.notify_waiters();
            });
        }
        if tokio::time::timeout(
            mount.close_timeout,
            wait_flag(&mount.drained, &mount.drained_notify),
        )
        .await
        .is_err()
            // Drained in the instant after expiry: not a timeout.
            && !*mount.drained.lock().unwrap()
        {
            if let Some(observer) = &mount.close_observer {
                let timeout = MountCloseTimeout {
                    extension: mount.extension.id().to_owned(),
                    bound: mount.close_timeout,
                    leases: mount.leases.load(Ordering::SeqCst),
                    pending_tasks: mount.cleanup.tracker().pending(),
                };
                // A panicking observer must not turn a timeout into an unwind.
                let _ = std::panic::catch_unwind(AssertUnwindSafe(|| observer(&timeout)));
            }
            return Err(ExtensionError::MountCloseTimeout {
                extension: mount.extension.id().to_owned(),
            });
        }
        wait_flag(&mount.closed, &mount.closed_notify).await;
        Ok(())
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
    // Was `characterize_close_orders_lease_wait_cleanup_shutdown` (crabber-flx7).
    // crabber-b4zy changed it: close now sends the close signal first and joins
    // the cleanup tracker after the leases, under one bound, so the order is
    // signal -> lease wait -> tracker join -> deferred cleanups -> shutdown.
    #[tokio::test]
    async fn close_orders_signal_leases_tracker_cleanup_shutdown() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let registry = Registry::new();
        let handle = registry
            .mount(Arc::new(OrderedClose(Arc::clone(&log))), Scope::Global)
            .await
            .unwrap();
        let tracker = handle.mount.cleanup.tracker();
        let (finish, gate) = tokio::sync::oneshot::channel::<()>();
        let task = tracker.spawn({
            let log = Arc::clone(&log);
            async move {
                gate.await.ok();
                log.lock().unwrap().push("tracker");
            }
        });
        let plan = registry.acquire(&SessionId::new());
        let closing = tokio::spawn({
            let handle = handle.clone();
            async move { handle.close().await }
        });
        settle().await;
        assert!(!closing.is_finished());
        assert!(tracker.is_closing(), "signal is sent before the waits");
        assert!(log.lock().unwrap().is_empty());
        log.lock().unwrap().push("release");
        plan.release();
        settle().await;
        assert!(!closing.is_finished(), "tracker still pending");
        finish.send(()).unwrap();
        task.await.unwrap();
        closing.await.unwrap().unwrap();
        assert_eq!(
            *log.lock().unwrap(),
            ["release", "tracker", "cleanup", "shutdown"]
        );
    }
    async fn settle() {
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
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
    }

    mod close {
        use super::*;
        use crate::{ToolInput, ToolOutcomeClass, ToolResultContext, ToolResultOutcome};
        use tokio::sync::oneshot;
        use tokio::time::Instant;

        type Log = Arc<Mutex<Vec<String>>>;

        /// Logs `cleanup:<id>` on rollback and `shutdown:<id>` on shutdown;
        /// optionally registers one ordinary result handler.
        struct Logged {
            id: &'static str,
            log: Log,
            handler: bool,
        }
        #[async_trait]
        impl Extension for Logged {
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
                let (log, id) = (Arc::clone(&self.log), self.id);
                r.defer(move || log.lock().unwrap().push(format!("cleanup:{id}")));
                if self.handler {
                    r.on_result_transform(
                        0,
                        format!("{}-handler", self.id),
                        Arc::new(|_, value| {
                            Box::pin(async move { Ok(crate::TransformOutput::new(value)) })
                        }),
                    );
                }
                Ok(())
            }
            async fn shutdown(&self) {
                self.log
                    .lock()
                    .unwrap()
                    .push(format!("shutdown:{}", self.id));
            }
        }
        fn logged(id: &'static str, log: &Log, handler: bool) -> Arc<dyn Extension> {
            Arc::new(Logged {
                id,
                log: Arc::clone(log),
                handler,
            })
        }
        fn new_log() -> Log {
            Arc::new(Mutex::new(Vec::new()))
        }
        fn entries(log: &Log) -> Vec<String> {
            log.lock().unwrap().clone()
        }
        fn assert_log(log: &Log, expected: &[&str]) {
            assert_eq!(entries(log), expected);
        }
        fn observed() -> (MountCloseObserver, Arc<Mutex<Vec<MountCloseTimeout>>>) {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let sink = Arc::clone(&seen);
            (
                Arc::new(move |timeout: &MountCloseTimeout| {
                    sink.lock().unwrap().push(timeout.clone());
                }),
                seen,
            )
        }
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
        /// A tracked task parked on `gate` that logs `task` when it ends.
        fn stuck_task(
            handle: &MountHandle,
            log: &Log,
        ) -> (oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
            let (release, gate) = oneshot::channel::<()>();
            let log = Arc::clone(log);
            let task = handle.mount.cleanup.tracker().spawn(async move {
                gate.await.ok();
                log.lock().unwrap().push("task".into());
            });
            (release, task)
        }
        fn timeout_for(extension: &str) -> ExtensionError {
            ExtensionError::MountCloseTimeout {
                extension: extension.into(),
            }
        }

        #[test]
        fn new_error_variants_have_the_designed_texts() {
            assert_eq!(timeout_for("x").to_string(), "mount close timed out: x");
            assert_eq!(
                ExtensionError::RegistryClosed.to_string(),
                "extension registry is closed"
            );
        }

        #[tokio::test]
        async fn close_deactivates_signals_and_rejects_result_invocations() {
            let log = new_log();
            let registry = Registry::new();
            let handle = registry
                .mount(logged("one", &log, true), Scope::Global)
                .await
                .unwrap();
            let plan = registry.acquire(&SessionId::new());
            let tracker = handle.mount.cleanup.tracker();
            let (release, task) = stuck_task(&handle, &log);
            let closing = tokio::spawn({
                let handle = handle.clone();
                async move { handle.close().await }
            });
            settle().await;
            // (1) deactivated: new plans skip the mount.
            assert_eq!(registry.acquire(&SessionId::new()).components.len(), 0);
            // (2) signalled: result invocations through the held plan fail.
            assert!(tracker.is_closing());
            assert_eq!(
                plan.dispatcher
                    .transform_tool_result(context(), crate::TransformOutput::new(json!("s")))
                    .await,
                ToolResultOutcome::Failed {
                    handler: "one-handler".into()
                }
            );
            // (3) leases and the tracker are still being waited on.
            assert!(!closing.is_finished());
            plan.release();
            settle().await;
            assert!(!closing.is_finished());
            assert_log(&log, &[]);
            release.send(()).unwrap();
            task.await.unwrap();
            closing.await.unwrap().unwrap();
            assert_eq!(entries(&log), ["task", "cleanup:one", "shutdown:one"]);
        }

        #[tokio::test(start_paused = true)]
        async fn tracker_task_finishing_within_the_bound_closes_ok() {
            let log = new_log();
            let registry = Registry::new();
            let handle = registry
                .mount(logged("one", &log, false), Scope::Global)
                .await
                .unwrap();
            let (release, task) = stuck_task(&handle, &log);
            let closing = tokio::spawn({
                let handle = handle.clone();
                async move { handle.close().await }
            });
            tokio::time::advance(DEFAULT_MOUNT_CLOSE_TIMEOUT / 2).await;
            release.send(()).unwrap();
            task.await.unwrap();
            assert_eq!(closing.await.unwrap(), Ok(()));
            assert_eq!(entries(&log), ["task", "cleanup:one", "shutdown:one"]);
        }

        #[tokio::test(start_paused = true)]
        async fn tracker_task_past_the_bound_times_out_and_the_reaper_finishes_the_close() {
            let log = new_log();
            let (observer, seen) = observed();
            let registry = Registry::new().with_close_observer(observer);
            assert_eq!(
                registry.inner.lock().unwrap().close_timeout,
                DEFAULT_MOUNT_CLOSE_TIMEOUT
            );
            let handle = registry
                .mount(logged("one", &log, false), Scope::Global)
                .await
                .unwrap();
            let (release, task) = stuck_task(&handle, &log);
            let start = Instant::now();
            assert_eq!(handle.close().await, Err(timeout_for("one")));
            assert!(start.elapsed() >= DEFAULT_MOUNT_CLOSE_TIMEOUT);
            assert_eq!(
                *seen.lock().unwrap(),
                [MountCloseTimeout {
                    extension: "one".into(),
                    bound: DEFAULT_MOUNT_CLOSE_TIMEOUT,
                    leases: 0,
                    pending_tasks: 1,
                }]
            );
            // Not aborted, and neither rollback nor shutdown ran yet.
            settle().await;
            assert!(!task.is_finished());
            assert_log(&log, &[]);
            // A second close while still stuck waits again and times out again.
            assert_eq!(handle.close().await, Err(timeout_for("one")));
            assert_eq!(seen.lock().unwrap().len(), 2);
            assert_log(&log, &[]);
            // The reaper runs rollback then shutdown only once the task ends.
            release.send(()).unwrap();
            task.await.unwrap();
            assert_eq!(handle.close().await, Ok(()));
            assert_eq!(entries(&log), ["task", "cleanup:one", "shutdown:one"]);
            assert_eq!(seen.lock().unwrap().len(), 2, "no observation on success");
        }

        #[tokio::test(start_paused = true)]
        async fn held_plan_lease_past_the_bound_times_out_with_the_lease_count() {
            let log = new_log();
            let (observer, seen) = observed();
            let registry = Registry::new().with_close_observer(observer);
            let handle = registry
                .mount(logged("one", &log, false), Scope::Global)
                .await
                .unwrap();
            let plan = registry.acquire(&SessionId::new());
            assert_eq!(handle.close().await, Err(timeout_for("one")));
            assert_eq!(
                *seen.lock().unwrap(),
                [MountCloseTimeout {
                    extension: "one".into(),
                    bound: DEFAULT_MOUNT_CLOSE_TIMEOUT,
                    leases: 1,
                    pending_tasks: 0,
                }]
            );
            assert_log(&log, &[]);
            plan.release();
            assert_eq!(handle.close().await, Ok(()));
            assert_eq!(entries(&log), ["cleanup:one", "shutdown:one"]);
        }

        #[tokio::test(start_paused = true)]
        async fn with_close_timeout_changes_the_bound_for_every_clone() {
            let log = new_log();
            let (observer, seen) = observed();
            let bound = Duration::from_millis(250);
            assert_ne!(bound, DEFAULT_MOUNT_CLOSE_TIMEOUT);
            let registry = Registry::new()
                .with_close_timeout(bound)
                .with_close_observer(observer);
            let handle = registry
                .clone()
                .mount(logged("one", &log, false), Scope::Global)
                .await
                .unwrap();
            let _plan = registry.acquire(&SessionId::new());
            let start = Instant::now();
            assert_eq!(handle.close().await, Err(timeout_for("one")));
            assert_eq!(start.elapsed(), bound);
            assert_eq!(seen.lock().unwrap()[0].bound, bound);
        }

        // Coordinator note 1: a spawn that lands after the close signal while a
        // lease is still held must be awaited before rollback and shutdown, not
        // missed because the tracker was joined while it was still empty.
        #[tokio::test(start_paused = true)]
        async fn late_spawn_while_a_lease_is_held_is_joined_before_rollback() {
            let log = new_log();
            let registry = Registry::new();
            let handle = registry
                .mount(logged("one", &log, false), Scope::Global)
                .await
                .unwrap();
            let plan = registry.acquire(&SessionId::new());
            let closing = tokio::spawn({
                let handle = handle.clone();
                async move { handle.close().await }
            });
            settle().await;
            assert_eq!(handle.mount.cleanup.tracker().pending(), 0);
            let (release, late) = stuck_task(&handle, &log);
            plan.release();
            assert_eq!(closing.await.unwrap(), Err(timeout_for("one")));
            assert_log(&log, &[]);
            release.send(()).unwrap();
            late.await.unwrap();
            assert_eq!(handle.close().await, Ok(()));
            assert_eq!(entries(&log), ["task", "cleanup:one", "shutdown:one"]);
        }

        #[tokio::test]
        async fn closing_a_mount_signals_its_owner_and_is_not_terminal_for_the_registry() {
            let log = new_log();
            let registry = Registry::new();
            let a = registry
                .mount(logged("a", &log, false), Scope::Global)
                .await
                .unwrap();
            let b = registry
                .mount(logged("b", &log, false), Scope::Global)
                .await
                .unwrap();
            let (tracker_a, tracker_b) = (a.mount.cleanup.tracker(), b.mount.cleanup.tracker());
            a.close().await.unwrap();
            assert!(tracker_a.is_closing());
            assert!(!tracker_b.is_closing());
            let plan = registry.try_acquire(&SessionId::new()).unwrap();
            assert_eq!(plan.components.len(), 1);
            plan.release();
            registry
                .mount(logged("c", &log, false), Scope::Global)
                .await
                .unwrap();
        }

        #[tokio::test]
        async fn close_all_closes_in_reverse_mount_order_and_is_terminal() {
            let log = new_log();
            let registry = Registry::new();
            for id in ["a", "b", "c"] {
                registry
                    .mount(logged(id, &log, false), Scope::Global)
                    .await
                    .unwrap();
            }
            registry.close_all().await.unwrap();
            assert_eq!(
                entries(&log),
                [
                    "cleanup:c",
                    "shutdown:c",
                    "cleanup:b",
                    "shutdown:b",
                    "cleanup:a",
                    "shutdown:a"
                ]
            );
            assert_eq!(
                registry.try_acquire(&SessionId::new()).err(),
                Some(ExtensionError::RegistryClosed)
            );
            // A second close_all is harmless.
            registry.close_all().await.unwrap();
        }

        #[tokio::test(start_paused = true)]
        async fn close_all_attempts_every_mount_returns_the_first_error_and_stays_terminal() {
            let log = new_log();
            let (observer, seen) = observed();
            let registry = Registry::new().with_close_observer(observer);
            let mut handles = vec![];
            for id in ["a", "b", "c"] {
                handles.push(
                    registry
                        .mount(logged(id, &log, false), Scope::Global)
                        .await
                        .unwrap(),
                );
            }
            // One global plan leases all three mounts, so every close times out
            // and the first error is the first mount attempted: the last mounted.
            let session = SessionId::new();
            let plan = registry.acquire(&session);
            assert_eq!(registry.close_all().await, Err(timeout_for("c")));
            let names: Vec<_> = seen
                .lock()
                .unwrap()
                .iter()
                .map(|t| t.extension.clone())
                .collect();
            assert_eq!(
                names,
                ["c", "b", "a"],
                "every mount attempted, reverse order"
            );
            assert_log(&log, &[]);
            // Terminal even though close_all returned an error.
            assert_eq!(
                registry.try_acquire(&session).err(),
                Some(ExtensionError::RegistryClosed)
            );
            assert!(matches!(
                registry
                    .mount(logged("d", &log, false), Scope::Global)
                    .await,
                Err(ExtensionError::RegistryClosed)
            ));
            plan.release();
            registry.close_all().await.unwrap();
            let mut finished = entries(&log);
            finished.sort();
            assert_eq!(finished.len(), 6);
        }

        #[tokio::test(start_paused = true)]
        async fn close_all_attempts_remaining_mounts_after_a_timeout() {
            let log = new_log();
            let registry = Registry::new().with_close_timeout(Duration::from_secs(1));
            let a = registry
                .mount(logged("a", &log, false), Scope::Global)
                .await
                .unwrap();
            let b = registry
                .mount(logged("b", &log, false), Scope::Global)
                .await
                .unwrap();
            let (release, task) = stuck_task(&b, &log);
            assert_eq!(registry.close_all().await, Err(timeout_for("b")));
            // a was still closed after b timed out.
            assert_eq!(entries(&log), ["cleanup:a", "shutdown:a"]);
            a.close().await.unwrap();
            release.send(()).unwrap();
            task.await.unwrap();
            b.close().await.unwrap();
            assert_eq!(
                entries(&log),
                ["cleanup:a", "shutdown:a", "task", "cleanup:b", "shutdown:b"]
            );
        }

        #[tokio::test(start_paused = true)]
        async fn a_closing_registry_never_hands_out_a_plan_missing_a_mount() {
            let log = new_log();
            let registry = Registry::new();
            for id in ["one", "two"] {
                registry
                    .mount(logged(id, &log, true), Scope::Global)
                    .await
                    .unwrap();
            }
            let session = SessionId::new();
            let held = registry.acquire(&session);
            assert_eq!(
                held.components
                    .iter()
                    .filter(|c| c.id.starts_with("extension:"))
                    .count(),
                2
            );
            let closing = tokio::spawn({
                let registry = registry.clone();
                async move { registry.close_all().await }
            });
            settle().await;
            // close_all is parked on the held lease; neither mount is acquirable.
            assert!(!closing.is_finished());
            let clone = registry.clone();
            assert_eq!(
                clone.try_acquire(&session).err(),
                Some(ExtensionError::RegistryClosed)
            );
            assert_eq!(
                clone.acquire_plan(&session).await.err(),
                Some(ExtensionError::RegistryClosed)
            );
            assert!(matches!(
                clone
                    .mount(logged("three", &log, false), Scope::Global)
                    .await,
                Err(ExtensionError::RegistryClosed)
            ));
            held.release();
            closing.await.unwrap().unwrap();
        }

        // A plan acquired before close_all started is complete: close_all waits
        // for the registry mutex that acquire holds across all mounts.
        #[tokio::test]
        async fn acquire_in_progress_when_close_all_starts_keeps_every_mount() {
            let log = new_log();
            let registry = Registry::new();
            for id in ["one", "two"] {
                registry
                    .mount(logged(id, &log, false), Scope::Global)
                    .await
                    .unwrap();
            }
            let (observed_tx, observed_rx) = std::sync::mpsc::channel();
            let resume = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
            *registry.acquire_hook.lock().unwrap() = Some(Arc::new({
                let resume = Arc::clone(&resume);
                let first = std::sync::Once::new();
                move || {
                    first.call_once(|| {
                        observed_tx.send(()).unwrap();
                        let (lock, ready) = &*resume;
                        let mut resumed = lock.lock().unwrap();
                        while !*resumed {
                            resumed = ready.wait(resumed).unwrap();
                        }
                    });
                }
            }));
            let acquiring = tokio::task::spawn_blocking({
                let registry = registry.clone();
                move || registry.try_acquire(&SessionId::new())
            });
            observed_rx.recv().unwrap();
            // close_all is about to take the mutex the acquire still holds, so
            // whichever way they interleave from here the plan is complete.
            let (about_tx, about_rx) = std::sync::mpsc::channel();
            *registry.close_start_hook.lock().unwrap() = Some(Arc::new(move || {
                about_tx.send(()).unwrap();
            }));
            let closing = tokio::task::spawn_blocking({
                let registry = registry.clone();
                let runtime = tokio::runtime::Handle::current();
                move || runtime.block_on(registry.close_all())
            });
            about_rx.recv().unwrap();
            let (lock, ready) = &*resume;
            *lock.lock().unwrap() = true;
            ready.notify_one();
            let plan = acquiring.await.unwrap().unwrap();
            let extensions = plan
                .components
                .iter()
                .filter(|c| c.id.starts_with("extension:"))
                .count();
            assert_eq!(extensions, 2);
            plan.release();
            closing.await.unwrap().unwrap();
            assert_eq!(
                registry.try_acquire(&SessionId::new()).err(),
                Some(ExtensionError::RegistryClosed)
            );
        }

        struct SlowInstall {
            gate: Mutex<Option<oneshot::Receiver<()>>>,
            rolled_back: Arc<AtomicUsize>,
        }
        #[async_trait]
        impl Extension for SlowInstall {
            fn id(&self) -> &'static str {
                "slow"
            }
            fn version(&self) -> &'static str {
                "1"
            }
            fn config_hash(&self) -> String {
                "{}".into()
            }
            async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
                let rolled_back = Arc::clone(&self.rolled_back);
                r.defer(move || {
                    rolled_back.fetch_add(1, Ordering::SeqCst);
                });
                let gate = self.gate.lock().unwrap().take().unwrap();
                gate.await.ok();
                Ok(())
            }
        }

        #[tokio::test]
        async fn mount_after_close_all_started_returns_registry_closed() {
            let registry = Registry::new();
            let (release, gate) = oneshot::channel::<()>();
            let rolled_back = Arc::new(AtomicUsize::new(0));
            let mounting = tokio::spawn({
                let registry = registry.clone();
                let rolled_back = Arc::clone(&rolled_back);
                async move {
                    registry
                        .mount(
                            Arc::new(SlowInstall {
                                gate: Mutex::new(Some(gate)),
                                rolled_back,
                            }),
                            Scope::Global,
                        )
                        .await
                        .map(|_| ())
                }
            });
            settle().await;
            registry.close_all().await.unwrap();
            release.send(()).unwrap();
            // Installed before the flag, committed after it: rejected and rolled back.
            assert_eq!(mounting.await.unwrap(), Err(ExtensionError::RegistryClosed));
            assert_eq!(rolled_back.load(Ordering::SeqCst), 1);
            // Mounting once closed does not even install.
            let count = Arc::new(AtomicUsize::new(0));
            assert!(matches!(
                registry
                    .mount(ext("late", false, false, Arc::clone(&count)), Scope::Global)
                    .await,
                Err(ExtensionError::RegistryClosed)
            ));
            assert_eq!(count.load(Ordering::SeqCst), 0);
        }

        #[tokio::test]
        #[should_panic(expected = "closed registry")]
        async fn acquire_panics_on_a_closed_registry() {
            let registry = Registry::new();
            registry.close_all().await.unwrap();
            let _ = registry.acquire(&SessionId::new());
        }

        // A-1: only the re-check after the first tracker wait catches a spawn
        // that lands once `wait_drained` has returned (a single pass would run
        // rollback and shutdown with the task still pending).
        #[tokio::test(start_paused = true)]
        async fn a_spawn_after_the_first_tracker_wait_is_joined_before_rollback() {
            let log = new_log();
            let registry = Registry::new();
            let handle = registry
                .mount(logged("one", &log, false), Scope::Global)
                .await
                .unwrap();
            let (release_tx, gate) = oneshot::channel::<()>();
            let tracker = handle.mount.cleanup.tracker();
            let late_log = Arc::clone(&log);
            *handle.mount.drain_hook.lock().unwrap() = Some(Box::new(move || {
                drop(tracker.spawn(async move {
                    gate.await.ok();
                    late_log.lock().unwrap().push("late".into());
                }));
            }));
            assert_eq!(handle.close().await, Err(timeout_for("one")));
            assert_log(&log, &[]);
            release_tx.send(()).unwrap();
            assert_eq!(handle.close().await, Ok(()));
            assert_log(&log, &["late", "cleanup:one", "shutdown:one"]);
        }

        // A-2: close_all signals every mount before it joins any.
        #[tokio::test(start_paused = true)]
        async fn close_all_signals_every_mount_before_joining_the_last_one() {
            let log = new_log();
            let registry = Registry::new();
            let mut handles = vec![];
            for id in ["a", "b", "c"] {
                handles.push(
                    registry
                        .mount(logged(id, &log, true), Scope::Global)
                        .await
                        .unwrap(),
                );
            }
            // The dispatcher outlives the released plan; no lease is held.
            let plan = registry.acquire(&SessionId::new());
            let dispatcher = plan.dispatcher.clone();
            plan.release();
            // c is joined first and is stuck on a tracker task.
            let (release, task) = stuck_task(&handles[2], &log);
            let closing = registry.close_all();
            tokio::pin!(closing);
            for _ in 0..8 {
                assert!(futures::poll!(&mut closing).is_pending());
                tokio::task::yield_now().await;
            }
            // c's bound has not expired, yet a and b are already signalled and
            // their handlers already fail through the driver.
            assert!(handles[0].mount.cleanup.tracker().is_closing());
            assert!(handles[1].mount.cleanup.tracker().is_closing());
            assert_eq!(
                dispatcher
                    .transform_tool_result(context(), crate::TransformOutput::new(json!("s")))
                    .await,
                ToolResultOutcome::Failed {
                    handler: "a-handler".into()
                }
            );
            assert_log(&log, &[]);
            assert_eq!(closing.await, Err(timeout_for("c")));
            release.send(()).unwrap();
            task.await.unwrap();
        }

        // A-3: closed is set in the same critical section as the deactivation.
        #[tokio::test]
        async fn close_all_sets_closed_in_the_deactivation_critical_section() {
            let log = new_log();
            let registry = Registry::new();
            for id in ["a", "b"] {
                registry
                    .mount(logged(id, &log, true), Scope::Global)
                    .await
                    .unwrap();
            }
            let ran = Arc::new(AtomicUsize::new(0));
            let mutex = Arc::clone(&registry.inner);
            *registry.close_hook.lock().unwrap() = Some(Arc::new({
                let ran = Arc::clone(&ran);
                move |inner: &RegistryInner| {
                    ran.fetch_add(1, Ordering::SeqCst);
                    // Every mount is deactivated, so the flag must already be
                    // set, and the mutex `try_acquire` takes is still held:
                    // no acquire can have observed the state in between.
                    assert!(inner.mounts.iter().all(|m| !*m.active.lock().unwrap()));
                    assert!(inner.closed, "closed must be set with the deactivation");
                    assert!(mutex.try_lock().is_err(), "critical section was left");
                }
            }));
            registry.close_all().await.unwrap();
            assert_eq!(ran.load(Ordering::SeqCst), 1);
        }

        // A-3 (stress): while close_all runs, every try_acquire is either a
        // complete plan (every mount's handler) or RegistryClosed, never a
        // plan missing mounts.
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn concurrent_acquires_see_a_complete_plan_or_registry_closed() {
            for _ in 0..200 {
                let log = new_log();
                let registry = Registry::new();
                for id in ["a", "b", "c"] {
                    registry
                        .mount(logged(id, &log, true), Scope::Global)
                        .await
                        .unwrap();
                }
                let spinners: Vec<_> = (0..2)
                    .map(|_| {
                        let registry = registry.clone();
                        tokio::task::spawn_blocking(move || {
                            let session = SessionId::new();
                            let mut complete = 0;
                            loop {
                                match registry.try_acquire(&session) {
                                    Ok(plan) => {
                                        let handlers = plan
                                            .components
                                            .iter()
                                            .filter(|c| c.id.starts_with("handler:"));
                                        assert_eq!(handlers.count(), 3, "partial plan");
                                        plan.release();
                                        complete += 1;
                                    }
                                    Err(error) => {
                                        assert_eq!(error, ExtensionError::RegistryClosed);
                                        return complete;
                                    }
                                }
                            }
                        })
                    })
                    .collect();
                tokio::task::yield_now().await;
                registry.close_all().await.unwrap();
                for spinner in spinners {
                    spinner.await.unwrap();
                }
            }
        }

        // B-1: dropping the close_all future must not strand a signalled mount.
        #[tokio::test(start_paused = true)]
        async fn dropped_close_all_still_closes_every_mount() {
            let log = new_log();
            let registry = Registry::new();
            for id in ["a", "b"] {
                registry
                    .mount(logged(id, &log, false), Scope::Global)
                    .await
                    .unwrap();
            }
            let plan = registry.acquire(&SessionId::new());
            assert!(
                tokio::time::timeout(Duration::from_secs(1), registry.close_all())
                    .await
                    .is_err()
            );
            assert_log(&log, &[]);
            plan.release();
            // No second close_all: the detached driver finishes both mounts, in
            // reverse mount order.
            tokio::time::sleep(DEFAULT_MOUNT_CLOSE_TIMEOUT * 2).await;
            assert_log(
                &log,
                &["cleanup:b", "shutdown:b", "cleanup:a", "shutdown:a"],
            );
        }

        // B-2
        struct CloseAllFromHandler {
            registry: Registry,
            result: Arc<Mutex<Option<Result<(), ExtensionError>>>>,
        }
        #[async_trait]
        impl Extension for CloseAllFromHandler {
            fn id(&self) -> &'static str {
                "reentrant-close-all"
            }
            fn version(&self) -> &'static str {
                "1"
            }
            fn config_hash(&self) -> String {
                "{}".into()
            }
            async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
                let (registry, result) = (self.registry.clone(), Arc::clone(&self.result));
                r.on_hook(
                    crate::TurnPrepare::ID,
                    0,
                    "close-all",
                    Arc::new(move |_| {
                        let (registry, result) = (registry.clone(), Arc::clone(&result));
                        Box::pin(async move {
                            *result.lock().unwrap() = Some(registry.close_all().await);
                            Ok(Value::Null)
                        })
                    }),
                );
                Ok(())
            }
        }

        #[tokio::test]
        async fn close_all_from_a_handler_is_rejected_before_anything_changes() {
            let log = new_log();
            let registry = Registry::new();
            let other = registry
                .mount(logged("other", &log, false), Scope::Global)
                .await
                .unwrap();
            let result = Arc::new(Mutex::new(None));
            registry
                .mount(
                    Arc::new(CloseAllFromHandler {
                        registry: registry.clone(),
                        result: Arc::clone(&result),
                    }),
                    Scope::Global,
                )
                .await
                .unwrap();
            let plan = registry.acquire(&SessionId::new());
            plan.dispatcher
                .hook::<crate::TurnPrepare>(Value::Null)
                .await
                .unwrap();
            assert_eq!(
                result.lock().unwrap().take(),
                Some(Err(ExtensionError::SelfClose))
            );
            plan.release();
            // Not terminal, nothing deactivated or signalled.
            assert!(!registry.inner.lock().unwrap().closed);
            assert!(!other.mount.cleanup.tracker().is_closing());
            let plan = registry.try_acquire(&SessionId::new()).unwrap();
            assert_eq!(
                plan.components
                    .iter()
                    .filter(|c| c.id.starts_with("extension:"))
                    .count(),
                2
            );
            plan.release();
            registry.close_all().await.unwrap();
        }

        // B-4
        struct Panicking {
            log: Log,
            panic_shutdown: bool,
        }
        #[async_trait]
        impl Extension for Panicking {
            fn id(&self) -> &'static str {
                "panicking"
            }
            fn version(&self) -> &'static str {
                "1"
            }
            fn config_hash(&self) -> String {
                "{}".into()
            }
            async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
                let log = Arc::clone(&self.log);
                r.defer(move || log.lock().unwrap().push("cleanup:first".into()));
                r.defer(|| panic!("deferred cleanup panics"));
                Ok(())
            }
            async fn shutdown(&self) {
                self.log.lock().unwrap().push("shutdown".into());
                assert!(!self.panic_shutdown, "shutdown panics");
            }
        }

        #[tokio::test]
        async fn a_panicking_cleanup_or_shutdown_still_ends_the_mount_closed() {
            for panic_shutdown in [false, true] {
                let log = new_log();
                let registry = Registry::new();
                let handle = registry
                    .mount(
                        Arc::new(Panicking {
                            log: Arc::clone(&log),
                            panic_shutdown,
                        }),
                        Scope::Global,
                    )
                    .await
                    .unwrap();
                // The panicking cleanup runs first (reverse order); the earlier
                // cleanup and shutdown still run and the mount ends closed.
                assert_eq!(handle.close().await, Ok(()));
                assert_log(&log, &["cleanup:first", "shutdown"]);
                assert_eq!(handle.close().await, Ok(()));
                assert_log(&log, &["cleanup:first", "shutdown"]);
            }
        }
    }
}
