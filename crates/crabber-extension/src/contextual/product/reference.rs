//! Shared original Box/reference/borrow/invocation lifetime, never policy evidence.
use crate::contextual::{Owner, product::Cell};
use crate::registry::OriginalRoster;
use crate::{ExtensionError, ResourceRegistrationError};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};
use tokio::{
    sync::{Mutex as AsyncMutex, Notify, oneshot},
    task::JoinHandle,
};

#[derive(Default, PartialEq, Eq)]
enum Metadata {
    #[default]
    Uncaptured,
    Captured,
    Bound,
}
#[derive(PartialEq, Eq)]
enum Installation {
    Empty,
    Installed,
}
struct State<T> {
    body: Option<Arc<T>>,
    next: u64,
    references: BTreeSet<u64>,
    borrows: BTreeSet<u64>,
    installation: Installation,
    closed: bool,
    done: bool,
    failed: bool,
    roster: Option<OriginalRoster>,
    metadata: Metadata,
}
struct Task {
    handle: Option<JoinHandle<bool>>,
    result: Option<bool>,
}
pub(crate) struct Node<T: Send + Sync + 'static> {
    state: Mutex<State<T>>,
    changed: Notify,
    root: Arc<Cell>,
    task: AsyncMutex<Task>,
}
struct Retain<T: Send + Sync + 'static>(Option<Arc<Node<T>>>);
impl<T: Send + Sync + 'static> Drop for Retain<T> {
    fn drop(&mut self) {
        if let Some(node) = self.0.take() {
            std::mem::forget(node);
        }
    }
}
pub(crate) struct Borrow<T: Send + Sync + 'static> {
    node: Arc<Node<T>>,
    id: u64,
    value: Option<Arc<T>>,
}
impl<T: Send + Sync + 'static> Borrow<T> {
    pub(crate) fn value(&self) -> &T {
        self.value.as_ref().unwrap()
    }
}
impl<T: Send + Sync + 'static> Drop for Borrow<T> {
    fn drop(&mut self) {
        self.value.take();
        let mut state = self.node.state.lock().unwrap();
        assert!(state.borrows.remove(&self.id));
        drop(state);
        self.node.progress();
        self.node.changed.notify_waiters();
    }
}
pub(crate) struct Reference<T: Send + Sync + 'static> {
    node: Arc<Node<T>>,
    id: u64,
}
impl<T: Send + Sync + 'static> Drop for Reference<T> {
    fn drop(&mut self) {
        let mut state = self.node.state.lock().unwrap();
        assert!(state.references.remove(&self.id));
        drop(state);
        self.node.progress();
        self.node.changed.notify_waiters();
    }
}
impl<T: Send + Sync + 'static> Node<T> {
    pub(in crate::contextual) fn new(owner: Owner) -> Arc<Self> {
        let node = Arc::new(Self {
            state: Mutex::new(State {
                body: None,
                next: 0,
                references: BTreeSet::new(),
                borrows: BTreeSet::new(),
                installation: Installation::Empty,
                closed: false,
                done: false,
                failed: false,
                roster: None,
                metadata: Metadata::Uncaptured,
            }),
            changed: Notify::new(),
            root: Cell::new(owner),
            task: AsyncMutex::new(Task {
                handle: None,
                result: None,
            }),
        });
        let (release, ready) = oneshot::channel();
        let mut retain = Retain(Some(node.clone()));
        let task = tokio::spawn(async move {
            if ready.await.is_err() {
                return false;
            }
            let node = retain.0.as_ref().unwrap();
            let success = node.drive().await;
            if node.state.lock().unwrap().body.is_none() {
                retain.0.take();
            }
            success
        });
        node.task.try_lock().unwrap().handle = Some(task);
        let _ = release.send(());
        node
    }
    pub(crate) fn install(self: &Arc<Self>, body: T) {
        let mut state = self.state.lock().unwrap();
        assert!(
            state.installation == Installation::Empty,
            "original Box installed once"
        );
        state.installation = Installation::Installed;
        state.body = Some(Arc::new(body));
        self.launch(&mut state);
        drop(state);
        self.changed.notify_waiters();
    }
    pub(in crate::contextual) fn done(&self, failed: bool) {
        let mut state = self.state.lock().unwrap();
        state.done = true;
        state.failed |= failed;
        self.root.constructor_done(failed);
        if state.closed && state.body.is_none() {
            self.root.close();
        }
        drop(state);
        self.changed.notify_waiters();
    }
    pub(in crate::contextual) fn close(&self) {
        let roster = {
            let mut state = self.state.lock().unwrap();
            state.closed = true;
            state.roster.clone()
        };
        if let Some(roster) = roster {
            roster.seal();
        }
        self.progress();
        self.changed.notify_waiters();
    }
    fn progress(&self) {
        let roster = self.state.lock().unwrap().roster.clone();
        let observed = roster.and_then(|roster| roster.cached_join());
        let mut state = self.state.lock().unwrap();
        if let Some(joined) = observed {
            state.failed |= !joined;
            state.roster = None;
        }
        self.launch(&mut state);
        if state.closed && state.done && state.body.is_none() {
            self.root.close();
        }
    }
    // Only the actual sealed original roster can authorize the source-bound case.
    fn launch(&self, state: &mut State<T>) {
        if state.closed
            && state.references.is_empty()
            && state.borrows.is_empty()
            && state.roster.is_none()
            && let Some(body) = state.body.take()
        {
            match Arc::try_unwrap(body) {
                Ok(value) => {
                    self.root.install(Box::new(value));
                    self.root.close();
                }
                Err(body) => {
                    state.body = Some(body);
                    state.failed = true;
                }
            }
        }
    }
    pub(crate) fn bind(&self, roster: OriginalRoster) -> Result<(), ExtensionError> {
        let mut state = self.state.lock().unwrap();
        if state.closed || state.metadata != Metadata::Captured {
            return Err(ExtensionError::ContextOwnerClosed);
        }
        state.metadata = Metadata::Bound;
        state.roster = Some(roster);
        Ok(())
    }
    pub(crate) fn borrow(self: &Arc<Self>) -> Result<Borrow<T>, ExtensionError> {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(ExtensionError::ContextOwnerClosed);
        }
        let value = state
            .body
            .clone()
            .ok_or(ExtensionError::ContextOwnerClosed)?;
        state.next = state
            .next
            .checked_add(1)
            .ok_or(ResourceRegistrationError::Capacity)?;
        let id = state.next;
        state.borrows.insert(id);
        Ok(Borrow {
            node: self.clone(),
            id,
            value: Some(value),
        })
    }
    pub(crate) fn captured(&self) -> Result<(), ExtensionError> {
        let mut state = self.state.lock().unwrap();
        if state.closed || state.metadata != Metadata::Uncaptured {
            return Err(ExtensionError::ContextOwnerClosed);
        }
        state.metadata = Metadata::Captured;
        Ok(())
    }
    pub(crate) fn check_captured(&self) -> Result<(), ExtensionError> {
        let state = self.state.lock().unwrap();
        if state.closed || state.metadata != Metadata::Captured {
            return Err(ExtensionError::ContextOwnerClosed);
        }
        Ok(())
    }
    pub(crate) fn reference(self: &Arc<Self>) -> Result<Reference<T>, ExtensionError> {
        let mut state = self.state.lock().unwrap();
        if state.closed || state.metadata != Metadata::Bound {
            return Err(ExtensionError::ContextOwnerClosed);
        }
        if state.references.len() >= 64 {
            return Err(ResourceRegistrationError::Capacity.into());
        }
        state.next = state
            .next
            .checked_add(1)
            .ok_or(ResourceRegistrationError::Capacity)?;
        let id = state.next;
        state.references.insert(id);
        Ok(Reference {
            node: self.clone(),
            id,
        })
    }
    async fn drive(&self) -> bool {
        let roster = loop {
            let changed = self.changed.notified();
            let closed = {
                let state = self.state.lock().unwrap();
                state.closed.then(|| state.roster.clone())
            };
            if let Some(roster) = closed {
                break roster;
            }
            changed.await;
        };
        if let Some(roster) = roster {
            let joined = roster.sealed_join().await.is_ok();
            let mut state = self.state.lock().unwrap();
            state.failed |= !joined;
            state.roster = None;
            self.launch(&mut state);
        }
        loop {
            let changed = self.changed.notified();
            let ready = {
                let mut state = self.state.lock().unwrap();
                self.launch(&mut state);
                state.done && state.body.is_none()
            };
            if ready {
                break;
            }
            changed.await;
        }
        self.root.close();
        self.root.join().await.is_ok() && !self.state.lock().unwrap().failed
    }
    pub(in crate::contextual) async fn join(&self) -> Result<(), ExtensionError> {
        let mut task = self.task.lock().await;
        if let Some(handle) = task.handle.as_mut() {
            let result = match handle.await {
                Ok(success) => success,
                Err(error) => {
                    if error.is_panic() {
                        crate::dispatch::discard_panic_payload(error.into_panic());
                    }
                    false
                }
            };
            task.result = Some(result);
            task.handle.take();
        }
        if task.result == Some(true) {
            Ok(())
        } else {
            Err(ExtensionError::ContextCleanupIncomplete)
        }
    }
    pub(crate) async fn disposed(&self) -> Result<(), ExtensionError> {
        self.join().await?;
        self.root.disposed().await
    }
}
