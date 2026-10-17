use super::{ContextualToolProduct, facade};
use crate::contextual::{Owner, product::Cell};
use crate::registry::{InvocationSource, OriginalRoster};
use crate::{ExtensionError, ResourceRegistrationError, ToolDefinition, ToolExecutor};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};
use tokio::{
    sync::{Mutex as AsyncMutex, Notify, oneshot},
    task::JoinHandle,
};

pub(super) struct Value(pub(super) Box<dyn ToolExecutor>);
#[derive(Default, PartialEq, Eq)]
enum Metadata {
    #[default]
    Uncaptured,
    Captured,
}
#[derive(Default)]
struct State {
    body: Option<Arc<Value>>,
    info: Option<crabber_core::ToolInfo>,
    next: u64,
    references: BTreeSet<u64>,
    borrows: BTreeSet<u64>,
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
pub(crate) struct Node {
    state: Mutex<State>,
    changed: Notify,
    root: Arc<Cell>,
    task: AsyncMutex<Task>,
}
struct Retain(Option<Arc<Node>>);
impl Drop for Retain {
    fn drop(&mut self) {
        if let Some(node) = self.0.take() {
            std::mem::forget(node);
        }
    }
}
pub(super) struct Borrow {
    node: Arc<Node>,
    id: u64,
    value: Option<Arc<Value>>,
}
impl Borrow {
    pub(super) fn tool(&self) -> &dyn ToolExecutor {
        self.value.as_ref().unwrap().0.as_ref()
    }
}
impl Drop for Borrow {
    fn drop(&mut self) {
        self.value.take();
        let mut state = self.node.state.lock().unwrap();
        assert!(state.borrows.remove(&self.id));
        self.node.launch(&mut state);
        drop(state);
        self.node.changed.notify_waiters();
    }
}
pub(super) struct Reference {
    node: Arc<Node>,
    id: u64,
}
impl Drop for Reference {
    fn drop(&mut self) {
        let mut state = self.node.state.lock().unwrap();
        assert!(state.references.remove(&self.id));
        self.node.launch(&mut state);
        drop(state);
        self.node.changed.notify_waiters();
    }
}
impl Node {
    pub(in crate::contextual) fn new(owner: Owner) -> Arc<Self> {
        let node = Arc::new(Self {
            state: Mutex::new(State::default()),
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
    pub(in crate::contextual) fn install(self: &Arc<Self>, product: ContextualToolProduct) {
        let mut state = self.state.lock().unwrap();
        assert!(state.info.is_none());
        state.info = Some(product.info);
        state.body = Some(Arc::new(Value(product.executor)));
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
            self.launch(&mut state);
            if state.done && state.body.is_none() {
                self.root.close();
            }
            state.roster.clone()
        };
        if let Some(roster) = roster {
            roster.seal();
        }
        self.changed.notify_waiters();
    }
    // Only the actual sealed original roster can authorize the source-bound case.
    fn launch(&self, state: &mut State) {
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
        if state.closed || state.roster.is_some() {
            return Err(ExtensionError::ContextOwnerClosed);
        }
        state.roster = Some(roster);
        Ok(())
    }
    pub(super) fn borrow(self: &Arc<Self>) -> Result<Borrow, ExtensionError> {
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
    pub(in crate::contextual) fn freeze(self: &Arc<Self>) -> Result<(), ExtensionError> {
        let value = self.borrow()?;
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if value.tool().resumable_identity().is_some() || value.tool().resumable().is_some() {
                Err(ExtensionError::Tool(
                    "Box product requires an ordinary Tool".into(),
                ))
            } else {
                Ok(())
            }
        }));
        let result = match result {
            Ok(result) => result,
            Err(payload) => {
                crate::dispatch::discard_panic_payload(payload);
                Err(ExtensionError::RegistrationDescriptorUnavailable)
            }
        };
        drop(value);
        result?;
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(ExtensionError::ContextOwnerClosed);
        }
        state.metadata = Metadata::Captured;
        Ok(())
    }
    pub(crate) fn template(self: &Arc<Self>) -> Result<Arc<ToolDefinition>, ExtensionError> {
        let state = self.state.lock().unwrap();
        if state.closed || state.metadata != Metadata::Captured {
            return Err(ExtensionError::ContextOwnerClosed);
        }
        Ok(Arc::new(ToolDefinition {
            info: state.info.clone().unwrap(),
            executor: Arc::new(facade::Template),
        }))
    }
    pub(crate) fn project(
        self: &Arc<Self>,
        source: InvocationSource,
    ) -> Result<Arc<ToolDefinition>, ExtensionError> {
        let mut state = self.state.lock().unwrap();
        if state.closed {
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
        Ok(Arc::new(ToolDefinition {
            info: state.info.clone().unwrap(),
            executor: Arc::new(facade::Executor {
                node: Arc::downgrade(self),
                source,
                _reference: Reference {
                    node: self.clone(),
                    id,
                },
            }),
        }))
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
    pub(super) async fn disposed(&self) -> Result<(), ExtensionError> {
        self.join().await?;
        self.root.disposed().await
    }
}
