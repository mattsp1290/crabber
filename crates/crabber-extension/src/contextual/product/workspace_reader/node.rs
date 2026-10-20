use super::{ContextualWorkspaceReaderResolverProduct, child::Child, facade};
use crate::contextual::{Owner, product::reference};
use crate::registry::{InvocationSource, OriginalRoster};
use crate::{ExtensionError, WorkspaceReaderResolver};
use futures::FutureExt;
use std::sync::{Arc, Mutex};
pub(super) struct Value(pub(super) Box<dyn super::ContextualWorkspaceReaderResolverBody>);
pub(super) type Reference = reference::Reference<Value>;
struct Children {
    admitted: u64,
    retired_joined: u64,
    closed: bool,
    nodes: Vec<Arc<Child>>,
}
pub(crate) struct Node {
    pub(super) original: Arc<reference::Node<Value>>,
    owner: Owner,
    children: Mutex<Children>,
}
impl Node {
    pub(in crate::contextual) fn new(owner: Owner) -> Arc<Self> {
        Arc::new(Self {
            original: reference::Node::new(owner.clone()),
            owner,
            children: Mutex::new(Children {
                admitted: 0,
                retired_joined: 0,
                closed: false,
                nodes: Vec::new(),
            }),
        })
    }
    pub(super) fn install(self: &Arc<Self>, product: ContextualWorkspaceReaderResolverProduct) {
        self.original.install(Value(product.body));
    }
    pub(super) fn done(&self, failed: bool) {
        self.original.done(failed);
    }
    pub(in crate::contextual) fn close(&self) {
        let children = {
            let mut state = self.children.lock().unwrap();
            state.closed = true;
            state.nodes.clone()
        };
        for child in children {
            child.close();
        }
        self.original.close();
    }
    pub(crate) fn bind(&self, roster: OriginalRoster) -> Result<(), ExtensionError> {
        self.original.bind(roster)
    }
    pub(super) fn freeze(&self) -> Result<(), ExtensionError> {
        self.original.captured()
    }
    pub(crate) fn check_captured(&self) -> Result<(), ExtensionError> {
        self.original.check_captured()
    }
    pub(crate) fn project(
        self: &Arc<Self>,
        source: InvocationSource,
    ) -> Result<Arc<dyn WorkspaceReaderResolver>, ExtensionError> {
        Ok(Arc::new(facade::Resolver {
            node: Arc::downgrade(self),
            source,
            _reference: self.original.reference()?,
        }))
    }
    pub(super) fn child(&self) -> Result<Arc<Child>, ExtensionError> {
        // Poll original retained handles outside the roster lock. Only actual
        // successful disposal joins permit reaping; failures keep their slot.
        let candidates = self.children.lock().unwrap().nodes.clone();
        let mut joined = Vec::new();
        for child in candidates {
            if child
                .disposed()
                .now_or_never()
                .is_some_and(|result| result.is_ok())
            {
                joined.push(child);
            }
        }
        let mut state = self.children.lock().unwrap();
        let retired = state
            .nodes
            .iter()
            .filter(|child| joined.iter().any(|old| Arc::ptr_eq(child, old)))
            .count();
        let retired_joined = state
            .retired_joined
            .checked_add(
                u64::try_from(retired).map_err(|_| crate::ResourceRegistrationError::Capacity)?,
            )
            .ok_or(crate::ResourceRegistrationError::Capacity)?;
        state
            .nodes
            .retain(|child| !joined.iter().any(|old| Arc::ptr_eq(child, old)));
        state.retired_joined = retired_joined;
        if state.closed {
            return Err(ExtensionError::ContextOwnerClosed);
        }
        if state.nodes.len() >= 64 {
            return Err(crate::ResourceRegistrationError::Capacity.into());
        }
        let admitted = state
            .admitted
            .checked_add(1)
            .ok_or(crate::ResourceRegistrationError::Capacity)?;
        let child = Child::new(self.owner.clone())?;
        state.nodes.push(child.clone());
        state.admitted = admitted;
        Ok(child)
    }
    pub(in crate::contextual) async fn join(&self) -> Result<(), ExtensionError> {
        {
            let state = self.children.lock().unwrap();
            let retained = u64::try_from(state.nodes.len())
                .map_err(|_| ExtensionError::ContextCleanupIncomplete)?;
            if !state.closed || state.retired_joined.checked_add(retained) != Some(state.admitted) {
                return Err(ExtensionError::ContextCleanupIncomplete);
            }
        }
        self.original.join().await?;
        let children = self.children.lock().unwrap().nodes.clone();
        for child in children {
            child.disposed().await?;
        }
        Ok(())
    }
    pub(super) async fn disposed(&self) -> Result<(), ExtensionError> {
        self.join().await?;
        self.original.disposed().await
    }
}
