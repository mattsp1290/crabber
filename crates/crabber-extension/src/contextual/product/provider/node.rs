use super::{ContextualProviderBindingBody, ContextualProviderProduct, facade, value::Owned};
use crate::{
    ExtensionError, ResourceRegistrationError, contextual::Owner, registry::InvocationSource,
};
use crabber_core::ProviderBindingDescriptor;
use crabber_providers::{ProviderAdapter, ProviderInfo, Streamer};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub(super) struct Metadata {
    pub(super) info: ProviderInfo,
    pub(super) routes: Vec<ProviderBindingDescriptor>,
}
struct Children {
    entries: Vec<Arc<Owned<dyn Streamer>>>,
    closed: bool,
}
pub(crate) struct Node {
    pub(super) owner: Owner,
    pub(super) body: Arc<Owned<dyn ContextualProviderBindingBody>>,
    metadata: Mutex<Option<Arc<Metadata>>>,
    children: Mutex<Children>,
    fingerprint: Mutex<Option<String>>,
}
impl Node {
    pub(in crate::contextual) fn new(owner: Owner) -> Arc<Self> {
        Arc::new(Self {
            body: Owned::new(owner.clone(), false),
            owner,
            metadata: Mutex::new(None),
            children: Mutex::new(Children {
                entries: Vec::new(),
                closed: false,
            }),
            fingerprint: Mutex::new(None),
        })
    }
    pub(in crate::contextual) fn install(&self, product: ContextualProviderProduct) {
        // Both metadata and original body move before validation/future disposal.
        self.body.install(product.body);
        let mut metadata = self.metadata.lock().unwrap();
        assert!(metadata.is_none());
        *metadata = Some(Arc::new(Metadata {
            info: product.info,
            routes: product.routes,
        }));
    }
    pub(in crate::contextual) fn done(&self, failed: bool) {
        self.body.constructor_done(failed);
    }
    pub(in crate::contextual) fn freeze(&self) -> Result<(), ExtensionError> {
        let metadata = self.metadata.lock().unwrap();
        let metadata = metadata
            .as_ref()
            .ok_or(ExtensionError::ContextInitializationIncomplete)?;
        if metadata.info.id.is_empty()
            || metadata.info.id.len() > 512
            || metadata.info.name.len() > 4096
            || metadata.info.id.chars().any(char::is_control)
            || metadata.routes.is_empty()
            || metadata.routes.len() > 64
        {
            return Err(ExtensionError::RegistrationDescriptorUnavailable);
        }
        let mut routes = std::collections::BTreeSet::new();
        if metadata.routes.iter().any(|route| {
            route.provider_id() != metadata.info.id
                || !routes.insert((route.provider_id(), route.model_id()))
        }) {
            return Err(ExtensionError::RegistrationDescriptorUnavailable);
        }
        *self.fingerprint.lock().unwrap() = Some(
            crate::plan::registration_hash(serde_json::json!([
                "crabber.contextual-provider-product-cell.v1",
                metadata.info.id,
                metadata.info.name,
                metadata.routes
            ]))?
            .as_str()
            .to_owned(),
        );
        Ok(())
    }
    pub(crate) fn fingerprint(&self) -> String {
        self.fingerprint
            .lock()
            .unwrap()
            .as_ref()
            .expect("frozen provider metadata")
            .clone()
    }
    pub(crate) fn template(self: &Arc<Self>) -> Result<Arc<dyn ProviderAdapter>, ExtensionError> {
        let metadata = self
            .metadata
            .lock()
            .unwrap()
            .clone()
            .ok_or(ExtensionError::ContextInitializationIncomplete)?;
        Ok(Arc::new(facade::Template(metadata)))
    }
    pub(crate) fn project(
        self: &Arc<Self>,
        source: InvocationSource,
    ) -> Result<Arc<dyn ProviderAdapter>, ExtensionError> {
        let reference = self.body.reference()?;
        let metadata = self
            .metadata
            .lock()
            .unwrap()
            .clone()
            .ok_or(ExtensionError::ContextInitializationIncomplete)?;
        Ok(Arc::new(facade::Adapter {
            node: Arc::downgrade(self),
            metadata,
            source,
            _reference: reference,
        }))
    }
    pub(super) async fn child(&self) -> Result<Arc<Owned<dyn Streamer>>, ExtensionError> {
        let children = self.children.lock().unwrap().entries.clone();
        for child in children {
            child.reap_join().await;
        }
        let mut children = self.children.lock().unwrap();
        if children.closed {
            return Err(ExtensionError::ContextOwnerClosed);
        }
        children
            .entries
            .retain(|child| child.observed() != Some(true));
        if children.entries.len() >= 64 {
            return Err(ResourceRegistrationError::Capacity.into());
        }
        let child = Owned::new(self.owner.clone(), true);
        children.entries.push(child.clone());
        Ok(child)
    }
    pub(in crate::contextual) fn close(&self) {
        let children = {
            let mut children = self.children.lock().unwrap();
            children.closed = true;
            children.entries.clone()
        };
        for child in children {
            child.close();
        }
        self.body.close();
    }
    pub(in crate::contextual) async fn join(&self) -> Result<(), ExtensionError> {
        let children = self.children.lock().unwrap().entries.clone();
        let (body, children) = tokio::join!(
            self.body.join(),
            futures::future::join_all(children.iter().map(|child| child.join()))
        );
        body?;
        for child in children {
            child?;
        }
        Ok(())
    }
}
