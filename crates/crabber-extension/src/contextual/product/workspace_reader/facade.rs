use super::{child, node};
use crate::registry::{InvocationSource, current_callback_invocations};
use crate::{
    CallbackInvocationCapability, WorkspaceContext, WorkspaceReadError, WorkspaceReadErrorKind,
    WorkspaceReader, WorkspaceReaderResolver,
};
use async_trait::async_trait;
use std::sync::{Arc, Weak};
use tokio::sync::oneshot;
fn denied() -> WorkspaceReadError {
    WorkspaceReadError::new(WorkspaceReadErrorKind::Denied)
}
fn io() -> WorkspaceReadError {
    WorkspaceReadError::new(WorkspaceReadErrorKind::Io)
}
pub(super) struct Resolver {
    pub(super) node: Weak<node::Node>,
    pub(super) source: InvocationSource,
    pub(super) _reference: node::Reference,
}
#[async_trait]
impl WorkspaceReaderResolver for Resolver {
    async fn resolve(
        &self,
        workspace: &WorkspaceContext,
    ) -> Result<Arc<dyn WorkspaceReader>, WorkspaceReadError> {
        let capability = current_callback_invocations().ok_or_else(denied)?;
        let node = self.node.upgrade().ok_or_else(denied)?;
        let child = node.child().map_err(|_| denied())?;
        let mut pending = child::Pending(Some(child.clone()));
        let workspace = workspace.clone();
        let original_child = child.clone();
        let creator = capability.clone();
        let (send, receive) = oneshot::channel();
        let actor = self
            .source
            .register_actor(child.clone(), move |cancel| {
                Box::pin(async move {
                    if !creator.matches_current() {
                        let _ = send.send(Err(denied()));
                        return true;
                    }
                    let Ok(original) = node.original.borrow() else {
                        return false;
                    };
                    let result = super::super::provider::original::original(
                        || original.value().0.resolve_box(&workspace),
                        |result| result.and_then(|product| original_child.install(product)),
                        &cancel,
                    )
                    .await;
                    match result {
                        Ok(Some(result)) => {
                            let _ = send.send(result);
                            true
                        }
                        Ok(None) => true,
                        Err(()) => false,
                    }
                })
            })
            .await
            .map_err(|_| denied())?;
        let actor = Arc::new(actor);
        child.registered(actor.clone());
        let mut waiter = actor.waiter();
        child.constructor_join().await.map_err(|_| io())?;
        receive.await.map_err(|_| denied())??;
        if !capability.matches_current() {
            return Err(denied());
        }
        let reference = child.original.reference().map_err(|_| denied())?;
        let reader: Arc<dyn WorkspaceReader> = Arc::new(Reader {
            child: child.clone(),
            source: self.source.clone(),
            capability,
            _reference: reference,
        });
        waiter.disarm();
        pending.0.take();
        Ok(reader)
    }
}
struct Reader {
    child: Arc<child::Child>,
    source: InvocationSource,
    capability: CallbackInvocationCapability,
    _reference: child::Reference,
}
impl Drop for Reader {
    fn drop(&mut self) {
        self.child.close();
    }
}
#[async_trait]
impl WorkspaceReader for Reader {
    async fn read_limited(
        &self,
        path: &str,
        maximum: usize,
    ) -> Result<Vec<u8>, WorkspaceReadError> {
        if !self.capability.matches_current() {
            return Err(denied());
        }
        let child = self.child.clone();
        let path = path.to_owned();
        let creator = self.capability.clone();
        let result = self
            .source
            .invoke_in(&self.child.roster, move || {
                Box::pin(async move {
                    if !creator.matches_current() {
                        return Err(denied());
                    }
                    let original = child.original.borrow().map_err(|_| denied())?;
                    let result = super::super::provider::original::original(
                        || original.value().0.read_limited(&path, maximum),
                        std::convert::identity,
                        &tokio_util::sync::CancellationToken::new(),
                    )
                    .await;
                    let bytes = match result {
                        Ok(Some(bytes)) => bytes?,
                        // Original panic/disposal failure must fail the retained
                        // task, rather than become a successful semantic Io result.
                        _ => panic!("original workspace read observation failed"),
                    };
                    if bytes.len() > maximum {
                        return Err(WorkspaceReadError::new(WorkspaceReadErrorKind::TooLarge));
                    }
                    Ok(bytes)
                })
            })
            .await
            .map_err(|_| io())?;
        if !self.capability.matches_current() {
            return Err(denied());
        }
        result
    }
}
