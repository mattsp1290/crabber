use super::{
    ContextualProviderBindingBody,
    node::{Metadata, Node},
    value::{Owned, Reference},
};
use crate::registry::{ActorClose, InvocationSource};
use async_trait::async_trait;
use crabber_core::ProviderBindingDescriptor;
use crabber_providers::{
    BoundProvider, ModelDescriptor, ProviderAdapter, ProviderBindingError, ProviderError,
    ProviderErrorKind, ProviderInfo, Selection, Streamer,
};
use std::sync::{
    Arc, Weak,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::oneshot;

pub(super) struct Template(pub(super) Arc<Metadata>);
pub(super) struct Adapter {
    pub(super) node: Weak<Node>,
    pub(super) metadata: Arc<Metadata>,
    pub(super) source: InvocationSource,
    pub(super) _reference: Reference<dyn ContextualProviderBindingBody>,
}
fn descriptor(
    metadata: &Metadata,
    selection: &Selection,
) -> Result<ProviderBindingDescriptor, ProviderBindingError> {
    metadata
        .routes
        .iter()
        .find(|route| {
            route.provider_id() == selection.provider_id && route.model_id() == selection.model_id
        })
        .cloned()
        .ok_or(ProviderBindingError::Unavailable)
}
fn models(metadata: &Metadata) -> Vec<ModelDescriptor> {
    metadata
        .routes
        .iter()
        .map(|route| ModelDescriptor {
            provider_id: route.provider_id().into(),
            id: route.model_id().into(),
            context_limit: route.limits().context_tokens.unwrap_or(0),
        })
        .collect()
}
pub(super) fn unavailable(error: impl std::fmt::Display) -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::Canceled,
        message: error.to_string(),
        retryable: false,
    }
}
#[async_trait]
impl ProviderAdapter for Template {
    fn info(&self) -> ProviderInfo {
        self.0.info.clone()
    }
    async fn models(&self) -> Result<Vec<ModelDescriptor>, ProviderError> {
        Ok(models(&self.0))
    }
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<ProviderBindingDescriptor, ProviderBindingError> {
        descriptor(&self.0, selection)
    }
    async fn build(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        Err(unavailable("provider template is not an acquisition"))
    }
}
#[async_trait]
impl ProviderAdapter for Adapter {
    fn info(&self) -> ProviderInfo {
        self.metadata.info.clone()
    }
    async fn models(&self) -> Result<Vec<ModelDescriptor>, ProviderError> {
        Ok(models(&self.metadata))
    }
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<ProviderBindingDescriptor, ProviderBindingError> {
        descriptor(&self.metadata, selection)
    }
    async fn build(&self, selection: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        self.bind(selection)
            .await
            .map(|bound| bound.streamer())
            .map_err(unavailable)
    }
    async fn bind(&self, selection: &Selection) -> Result<BoundProvider, ProviderBindingError> {
        if crate::registry::current_callback_invocations().is_none() {
            return Err(ProviderBindingError::ResourcesUnsupported);
        }
        let expected = descriptor(&self.metadata, selection)?;
        let node = self
            .node
            .upgrade()
            .ok_or(ProviderBindingError::Unavailable)?;
        let child = node
            .child()
            .await
            .map_err(|error| ProviderBindingError::Acquisition(unavailable(error)))?;
        let using = match node.body.admit_use() {
            Ok(using) => using,
            Err(error) => {
                child.constructor_done(false);
                child.close();
                return Err(ProviderBindingError::Acquisition(unavailable(error)));
            }
        };
        let completion = match child.admit_completion() {
            Ok(completion) => completion,
            Err(error) => {
                node.body.refused_use(using);
                child.constructor_done(false);
                child.close();
                return Err(ProviderBindingError::Acquisition(unavailable(error)));
            }
        };
        let close = Arc::new(BindingClose {
            parent: node.body.clone(),
            using,
            child: child.clone(),
            completion,
            entered: AtomicBool::new(false),
        });
        let actual = close.clone();
        let requested = selection.clone();
        let wanted = expected.clone();
        let (send, receive) = oneshot::channel();
        let actor = match self
            .source
            .register_actor(close.clone(), move |cancel| {
                Box::pin(async move {
                    actual.entered.store(true, Ordering::SeqCst);
                    let mut done = Constructed {
                        child: actual.child.clone(),
                        done: false,
                    };
                    let result =
                        bind_original(&actual.parent, &actual.child, requested, &wanted, &cancel)
                            .await;
                    actual.child.constructor_done(result.is_err());
                    done.done = true;
                    if let Ok(result) = result {
                        if result.is_err() {
                            actual.child.close();
                        }
                        let _ = send.send(result);
                        true
                    } else {
                        actual.child.close();
                        false
                    }
                })
            })
            .await
        {
            Ok(actor) => actor,
            Err(error) => {
                // register_actor errors only before publishing any actual task.
                close.parent.refused_use(close.using);
                close.child.refused_completion(close.completion);
                close.child.constructor_done(false);
                close.close();
                return Err(ProviderBindingError::Acquisition(unavailable(error)));
            }
        };
        let mut waiter = actor.waiter();
        let result = receive.await;
        let joined = actor.join().await;
        waiter.disarm();
        if let Err(error) = joined {
            return Err(ProviderBindingError::Acquisition(unavailable(error)));
        }
        let descriptor = result.map_err(|_| ProviderBindingError::Unavailable)??;
        let reference = child
            .reference()
            .map_err(|error| ProviderBindingError::Acquisition(unavailable(error)))?;
        let streamer = Arc::new(super::stream::Binding {
            owner: node.owner.clone(),
            body: child,
            source: self.source.clone(),
            _reference: reference,
            streams: std::sync::Mutex::new(Vec::new()),
        });
        BoundProvider::new(selection, descriptor, streamer)
    }
}
struct Constructed {
    child: Arc<Owned<dyn Streamer>>,
    done: bool,
}
impl Drop for Constructed {
    fn drop(&mut self) {
        if !self.done {
            self.child.constructor_done(true);
            self.child.close();
        }
    }
}
async fn bind_original(
    parent: &Arc<Owned<dyn ContextualProviderBindingBody>>,
    child: &Arc<Owned<dyn Streamer>>,
    selection: Selection,
    expected: &ProviderBindingDescriptor,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<Result<ProviderBindingDescriptor, ProviderBindingError>, ()> {
    let borrowed = match parent.borrow() {
        Ok(borrowed) => borrowed,
        Err(error) => return Ok(Err(ProviderBindingError::Acquisition(unavailable(error)))),
    };
    let result = super::original::original(
        || borrowed.get().bind_box(selection),
        |result| {
            result.and_then(|product| {
                child.install(product.body);
                if &product.descriptor == expected {
                    Ok(product.descriptor)
                } else {
                    child.close();
                    Err(ProviderBindingError::VersionConflict)
                }
            })
        },
        cancel,
    )
    .await;
    drop(borrowed);
    result.map(|result| result.unwrap_or(Err(ProviderBindingError::Unavailable)))
}

struct BindingClose {
    parent: Arc<Owned<dyn ContextualProviderBindingBody>>,
    using: u64,
    child: Arc<Owned<dyn Streamer>>,
    completion: u64,
    entered: AtomicBool,
}
impl ActorClose for BindingClose {
    fn close(&self) {
        self.child.close();
    }
    fn observed(&self, success: bool) {
        if !success {
            self.child.close();
        }
        if !self.entered.load(Ordering::SeqCst) {
            self.child.constructor_done(false);
            self.child.close();
        }
        self.parent.observed_use(self.using, success);
    }
    fn released(&self, success: bool) -> futures::future::BoxFuture<'static, bool> {
        self.child.observed_completion(self.completion, success);
        let child = self.child.clone();
        Box::pin(async move { success && (!child.is_closed() || child.join().await.is_ok()) })
    }
}
