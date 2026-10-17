//! Actual unique Box storage: reference/borrow admissions precede exposure.
use crate::contextual::{Owner, product::Cell};
use crate::{ExtensionError, ResourceRegistrationError};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};
use tokio::{
    sync::{Mutex as AsyncMutex, Notify, oneshot},
    task::JoinHandle,
};

struct Value<T: ?Sized>(Box<T>);
#[derive(PartialEq, Eq)]
enum ReturnState {
    Pending,
    Installed,
}
struct State<T: ?Sized> {
    value: Option<Arc<Value<T>>>,
    returned: ReturnState,
    constructor_done: bool,
    closed: bool,
    failed: bool,
    next: u64,
    references: BTreeSet<u64>,
    borrows: BTreeSet<u64>,
    uses: BTreeSet<u64>,
    completions: BTreeSet<u64>,
    holds: Vec<crabber_providers::ProviderReleaseHolds>,
}
struct Task {
    handle: Option<JoinHandle<bool>>,
    result: Option<bool>,
}
pub(super) struct Owned<T: ?Sized + Send + Sync + 'static> {
    state: Mutex<State<T>>,
    root: Arc<Cell>,
    require_owner: bool,
    close_on_last_reference: bool,
    changed: Notify,
    task: AsyncMutex<Task>,
}
struct Retain<T: ?Sized + Send + Sync + 'static>(Option<Arc<Owned<T>>>);
impl<T: ?Sized + Send + Sync + 'static> Drop for Retain<T> {
    fn drop(&mut self) {
        if let Some(original) = self.0.take() {
            std::mem::forget(original);
        }
    }
}
pub(super) struct Reference<T: ?Sized + Send + Sync + 'static> {
    original: Arc<Owned<T>>,
    id: u64,
}
impl<T: ?Sized + Send + Sync + 'static> Drop for Reference<T> {
    fn drop(&mut self) {
        let holds = crabber_providers::ProviderReleaseHolds::capture_current();
        let mut state = self.original.state.lock().unwrap();
        assert!(state.references.remove(&self.id));
        if state.references.is_empty() && self.original.close_on_last_reference {
            state.holds.push(holds);
            state.closed = true;
        }
        self.original.progress(&mut state);
        self.original.changed.notify_waiters();
    }
}
pub(super) struct Borrow<T: ?Sized + Send + Sync + 'static> {
    original: Arc<Owned<T>>,
    id: u64,
    value: Option<Arc<Value<T>>>,
}
impl<T: ?Sized + Send + Sync + 'static> Borrow<T> {
    pub(super) fn get(&self) -> &T {
        self.value.as_ref().unwrap().0.as_ref()
    }
}
impl<T: ?Sized + Send + Sync + 'static> Drop for Borrow<T> {
    fn drop(&mut self) {
        // The actual object reference disappears before retiring its admission.
        self.value.take();
        let mut state = self.original.state.lock().unwrap();
        assert!(state.borrows.remove(&self.id));
        self.original.progress(&mut state);
        self.original.changed.notify_waiters();
    }
}
impl<T: ?Sized + Send + Sync + 'static> Owned<T> {
    pub(super) fn new(owner: Owner, close_on_last_reference: bool) -> Arc<Self> {
        let original = Arc::new(Self {
            state: Mutex::new(State {
                value: None,
                returned: ReturnState::Pending,
                constructor_done: false,
                closed: false,
                failed: false,
                next: 0,
                references: BTreeSet::new(),
                borrows: BTreeSet::new(),
                uses: BTreeSet::new(),
                completions: BTreeSet::new(),
                holds: Vec::new(),
            }),
            root: Cell::new(owner),
            require_owner: close_on_last_reference,
            close_on_last_reference,
            changed: Notify::new(),
            task: AsyncMutex::new(Task {
                handle: None,
                result: None,
            }),
        });
        let (release, ready) = oneshot::channel();
        let mut retained = Retain(Some(original.clone()));
        let context = crate::dispatch::CallbackTaskContext::capture_existing();
        let handle = tokio::spawn(context.scope(async move {
            if ready.await.is_err() {
                return false;
            }
            let original = retained.0.as_ref().unwrap();
            let success = original.drive().await;
            if original.state.lock().unwrap().value.is_none() {
                retained.0.take();
            }
            success
        }));
        original.task.try_lock().unwrap().handle = Some(handle);
        let _ = release.send(());
        original
    }
    pub(super) fn install(self: &Arc<Self>, value: Box<T>) {
        // Capture the real install frame; no arbitrary host context is accepted.
        let context = self.root.with_callback_drop_context(self.require_owner);
        let mut state = self.state.lock().unwrap();
        assert!(state.returned == ReturnState::Pending);
        state.failed |= !context;
        state.returned = ReturnState::Installed;
        state.value = Some(Arc::new(Value(value)));
        self.progress(&mut state);
        drop(state);
        // The returned Box is already owned if capture itself refuses/panics.
        // Constructor completion stays false until this install has returned.
        let holds = crabber_providers::ProviderReleaseHolds::capture_current();
        self.state.lock().unwrap().holds.push(holds);
        self.changed.notify_waiters();
    }
    pub(super) fn constructor_done(self: &Arc<Self>, failed: bool) {
        let mut state = self.state.lock().unwrap();
        state.constructor_done = true;
        state.failed |= failed;
        self.root.constructor_done(failed);
        self.progress(&mut state);
        self.changed.notify_waiters();
    }
    pub(super) fn close(self: &Arc<Self>) {
        let mut state = self.state.lock().unwrap();
        state.closed = true;
        self.progress(&mut state);
        self.changed.notify_waiters();
    }
    fn next(state: &mut State<T>) -> Result<u64, ExtensionError> {
        state.next = state
            .next
            .checked_add(1)
            .ok_or(ResourceRegistrationError::Capacity)?;
        Ok(state.next)
    }
    pub(super) fn reference(self: &Arc<Self>) -> Result<Reference<T>, ExtensionError> {
        let mut state = self.state.lock().unwrap();
        if state.closed || state.value.is_none() {
            return Err(ExtensionError::ContextOwnerClosed);
        }
        if state.references.len() >= 64 {
            return Err(ResourceRegistrationError::Capacity.into());
        }
        let id = Self::next(&mut state)?;
        state.references.insert(id);
        Ok(Reference {
            original: self.clone(),
            id,
        })
    }
    pub(super) fn borrow(self: &Arc<Self>) -> Result<Borrow<T>, ExtensionError> {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(ExtensionError::ContextOwnerClosed);
        }
        let value = state
            .value
            .clone()
            .ok_or(ExtensionError::ContextOwnerClosed)?;
        let id = Self::next(&mut state)?;
        state.borrows.insert(id);
        Ok(Borrow {
            original: self.clone(),
            id,
            value: Some(value),
        })
    }
    /// Actual task-use admission, retired only by that original observed join.
    pub(super) fn admit_use(self: &Arc<Self>) -> Result<u64, ExtensionError> {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(ExtensionError::ContextOwnerClosed);
        }
        if state.uses.len() >= 64 {
            return Err(ResourceRegistrationError::Capacity.into());
        }
        let id = Self::next(&mut state)?;
        state.uses.insert(id);
        Ok(id)
    }
    /// Explicit original reservation refused before any task was published.
    pub(super) fn refused_use(self: &Arc<Self>, id: u64) {
        self.observed_use(id, true);
    }
    pub(super) fn observed_use(self: &Arc<Self>, id: u64, success: bool) {
        let mut state = self.state.lock().unwrap();
        state.failed |= !success;
        if success {
            assert!(state.uses.remove(&id));
        }
        self.progress(&mut state);
        self.changed.notify_waiters();
    }
    // Constructor/stream-main joins gate final release, not starting the child
    // destructor that may be needed to unblock that same constructor/main.
    pub(super) fn admit_completion(self: &Arc<Self>) -> Result<u64, ExtensionError> {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(ExtensionError::ContextOwnerClosed);
        }
        if state.completions.len() >= 64 {
            return Err(ResourceRegistrationError::Capacity.into());
        }
        let id = Self::next(&mut state)?;
        state.completions.insert(id);
        Ok(id)
    }
    pub(super) fn refused_completion(&self, id: u64) {
        self.observed_completion(id, true);
    }
    pub(super) fn observed_completion(&self, id: u64, success: bool) {
        let mut state = self.state.lock().unwrap();
        state.failed |= !success;
        if success {
            assert!(state.completions.remove(&id));
        }
        self.changed.notify_waiters();
    }
    fn progress(self: &Arc<Self>, state: &mut State<T>) {
        if state.closed
            && state.references.is_empty()
            && state.borrows.is_empty()
            && state.uses.is_empty()
            && let Some(value) = state.value.take()
        {
            // The explicit lifetime roster, not an Arc count, authorizes transfer.
            match Arc::try_unwrap(value) {
                Ok(value) => {
                    self.root.install(Box::new(value));
                    self.root.close();
                }
                Err(value) => {
                    state.value = Some(value);
                    state.failed = true;
                }
            }
        }
        if state.closed && state.constructor_done && state.value.is_none() {
            self.root.close();
        }
    }
    async fn drive(self: &Arc<Self>) -> bool {
        loop {
            let changed = self.changed.notified();
            let ready = {
                let state = self.state.lock().unwrap();
                state.closed && state.constructor_done && state.value.is_none()
            };
            if ready {
                break;
            }
            changed.await;
        }
        let joined = self.root.join().await.is_ok();
        loop {
            let changed = self.changed.notified();
            let finished = {
                let state = self.state.lock().unwrap();
                state.completions.is_empty() || state.failed
            };
            if finished {
                break;
            }
            changed.await;
        }
        if !joined || self.state.lock().unwrap().failed {
            return false;
        }
        let holds = std::mem::take(&mut self.state.lock().unwrap().holds);
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(holds))) {
            Ok(()) => true,
            Err(payload) => {
                crate::dispatch::discard_panic_payload(payload);
                false
            }
        }
    }
    pub(super) async fn body_join(&self) -> Result<(), ExtensionError> {
        self.root.join().await
    }
    pub(super) fn is_closed(&self) -> bool {
        self.state.lock().unwrap().closed
    }
    pub(super) fn observed(&self) -> Option<bool> {
        self.task.try_lock().ok().and_then(|task| task.result)
    }
    pub(super) async fn reap_join(&self) {
        let finished = self
            .task
            .try_lock()
            .is_ok_and(|task| task.handle.as_ref().is_some_and(JoinHandle::is_finished));
        if finished {
            let _ = self.join().await;
        }
    }
    pub(super) async fn join(&self) -> Result<(), ExtensionError> {
        let mut task = self.task.lock().await;
        if let Some(handle) = task.handle.as_mut() {
            let joined = match handle.await {
                Ok(success) => success,
                Err(error) => {
                    if error.is_panic() {
                        crate::dispatch::discard_panic_payload(error.into_panic());
                    }
                    false
                }
            };
            task.handle.take();
            task.result = Some(joined);
        }
        if task.result == Some(true) {
            Ok(())
        } else {
            Err(ExtensionError::ContextCleanupIncomplete)
        }
    }
}
