//! Actual original destructor handle, retained before its gate can open.
use super::ContextualProductRoot;
use crate::{
    ExtensionError,
    contextual::{INITIALIZING, Owner},
};
use std::{
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
};
use tokio::{
    sync::{Mutex as AsyncMutex, Notify, oneshot},
    task::JoinHandle,
};

#[derive(Default, PartialEq, Eq)]
enum RootStage {
    #[default]
    Empty,
    Installed,
    Dispatched,
}
#[derive(Default)]
struct Value {
    root: Option<Box<dyn ContextualProductRoot>>,
    stage: RootStage,
    constructor_done: bool,
    closing: bool,
    failed: bool,
}
#[derive(Default)]
struct Task {
    handle: Option<JoinHandle<bool>>,
    observed: Option<bool>,
}
pub(in crate::contextual) struct Cell {
    owner: Owner,
    drop_context: Mutex<Option<crate::dispatch::CallbackTaskContext>>,
    drop_context_missing: std::sync::atomic::AtomicBool,
    runtime: tokio::runtime::Handle,
    published: Mutex<Option<JoinHandle<bool>>>,
    value: Mutex<Value>,
    changed: Notify,
    actor: AsyncMutex<Task>,
    destructor: AsyncMutex<Task>,
}
/// An abnormal actor exit must retain the actual cell/Box/handles rather than
/// run a user destructor while unwinding its last ordinary Arc.
struct Retainer(Option<Arc<Cell>>);
impl Drop for Retainer {
    fn drop(&mut self) {
        if let Some(cell) = self.0.take() {
            std::mem::forget(cell);
        }
    }
}
impl Cell {
    pub(in crate::contextual) fn new(owner: Owner) -> Arc<Self> {
        let cell = Arc::new(Self {
            owner,
            drop_context: Mutex::new(None),
            drop_context_missing: std::sync::atomic::AtomicBool::new(false),
            runtime: tokio::runtime::Handle::current(),
            published: Mutex::new(None),
            value: Mutex::new(Value::default()),
            changed: Notify::new(),
            actor: AsyncMutex::new(Task::default()),
            destructor: AsyncMutex::new(Task::default()),
        });
        let (release, ready) = oneshot::channel();
        let mut retained = Retainer(Some(cell.clone()));
        let task = tokio::spawn(async move {
            if ready.await.is_err() {
                return false;
            }
            let cell = retained.0.as_ref().expect("retained cell");
            let success = cell.run().await;
            if cell.value.lock().unwrap().root.is_none() {
                retained.0.take();
            }
            success
        });
        cell.actor
            .try_lock()
            .expect("unpublished root actor")
            .handle = Some(task);
        let _ = release.send(());
        cell
    }
    pub(in crate::contextual) fn with_callback_drop_context(&self, require_owner: bool) -> bool {
        let present = !require_owner || crate::registry::current_callback_invocations().is_some();
        if !present {
            self.drop_context_missing
                .store(true, std::sync::atomic::Ordering::SeqCst);
            return false;
        }
        *self.drop_context.lock().unwrap() =
            Some(crate::dispatch::CallbackTaskContext::capture_existing());
        true
    }
    pub(in crate::contextual) fn install(self: &Arc<Self>, root: Box<dyn ContextualProductRoot>) {
        let mut value = self.value.lock().unwrap();
        assert!(
            value.stage == RootStage::Empty,
            "original root installed once"
        );
        value.stage = RootStage::Installed;
        value.root = Some(root);
        self.launch(&mut value);
        drop(value);
        self.changed.notify_waiters();
    }
    pub(in crate::contextual) fn constructor_done(self: &Arc<Self>, failed: bool) {
        let mut value = self.value.lock().unwrap();
        value.constructor_done = true;
        value.failed |= failed;
        drop(value);
        self.changed.notify_waiters();
    }
    pub(in crate::contextual) fn close(self: &Arc<Self>) {
        let mut value = self.value.lock().unwrap();
        value.closing = true;
        self.launch(&mut value);
        drop(value);
        self.changed.notify_waiters();
    }
    async fn run(self: &Arc<Self>) -> bool {
        loop {
            let changed = self.changed.notified();
            let (ready, empty) = {
                let value = self.value.lock().unwrap();
                (
                    value.stage == RootStage::Dispatched,
                    value.closing && value.constructor_done && value.stage == RootStage::Empty,
                )
            };
            if ready {
                break;
            }
            if empty {
                return !self.value.lock().unwrap().failed;
            }
            changed.await;
        }
        self.observe_destructor().await
    }
    // Synchronous ingress launches the actual destructor before returning to
    // untrusted create-future disposal; an async actor may not have polled yet.
    fn launch(self: &Arc<Self>, value: &mut Value) {
        if self
            .drop_context_missing
            .load(std::sync::atomic::Ordering::SeqCst)
            || !value.closing
            || value.stage != RootStage::Installed
        {
            return;
        }
        let (release, ready) = oneshot::channel();
        let cell = self.clone();
        let owner = self.owner.clone();
        let context = self.drop_context.lock().unwrap().take();
        let runtime = self.runtime.clone();
        let handle = self.runtime.spawn_blocking(move || {
            INITIALIZING.sync_scope(owner, || {
                if ready.blocking_recv().is_err() {
                    return false;
                }
                let root = cell
                    .value
                    .lock()
                    .unwrap()
                    .root
                    .take()
                    .expect("installed original root");
                let dispose = || match std::panic::catch_unwind(AssertUnwindSafe(|| drop(root))) {
                    Ok(()) => true,
                    Err(payload) => {
                        crate::dispatch::discard_panic_payload(payload);
                        false
                    }
                };
                match context {
                    Some(context) => runtime.block_on(context.scope(async move { dispose() })),
                    None => dispose(),
                }
            })
        });
        *self.published.lock().unwrap() = Some(handle);
        value.stage = RootStage::Dispatched;
        let _ = release.send(());
    }
    async fn observe_destructor(&self) -> bool {
        {
            let mut task = self.destructor.lock().await;
            if task.handle.is_none() && task.observed.is_none() {
                task.handle = self.published.lock().unwrap().take();
            }
        }
        observe(&self.destructor, &self.owner).await
    }
    pub(in crate::contextual) async fn join(&self) -> Result<(), ExtensionError> {
        let actor = observe(&self.actor, &self.owner).await;
        // A failed coordinator never substitutes for the actual original Drop.
        let has_destructor = {
            let task = self.destructor.lock().await;
            task.handle.is_some()
                || task.observed.is_some()
                || self.published.lock().unwrap().is_some()
        };
        let destructor = !has_destructor || self.observe_destructor().await;
        let value = self.value.lock().unwrap();
        if actor && destructor && !value.failed {
            Ok(())
        } else {
            Err(ExtensionError::ContextCleanupIncomplete)
        }
    }
    pub(super) async fn disposed(&self) -> Result<(), ExtensionError> {
        self.join().await?;
        let installed = {
            let value = self.value.lock().unwrap();
            value.stage != RootStage::Empty && value.root.is_none()
        };
        if !installed {
            return Err(ExtensionError::ContextInitializationIncomplete);
        }
        if self.destructor.lock().await.observed != Some(true) {
            return Err(ExtensionError::ContextCleanupIncomplete);
        }
        Ok(())
    }
}
async fn observe(task: &AsyncMutex<Task>, owner: &Owner) -> bool {
    INITIALIZING
        .scope(owner.clone(), async {
            let mut task = task.lock().await;
            if let Some(observed) = task.observed {
                return observed;
            }
            let Some(handle) = task.handle.as_mut() else {
                return false;
            };
            let observed = match handle.await {
                Ok(success) => success,
                Err(error) => {
                    if error.is_panic() {
                        crate::dispatch::discard_panic_payload(error.into_panic());
                    }
                    false
                }
            };
            task.observed = Some(observed);
            task.handle.take();
            observed
        })
        .await
}
