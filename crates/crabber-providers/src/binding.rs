//! Binding acquisition and exact restoration without selecting a substitute.
mod release;
mod resources;
use crate::{ProviderError, Selection, Streamer};
use crabber_core::ProviderBindingDescriptor;
pub use release::ProviderReleaseObserver;
pub use resources::{
    ProviderResourceContext, ProviderResourceContract, ProviderResourceFailure,
    ProviderResourceHost, ProviderResourceRegistrationError, ProviderResourceTask,
};
use std::sync::Arc;

/// Finite binding failures separate configuration availability from transport.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProviderBindingError {
    #[error("provider binding is unavailable")]
    Unavailable,
    #[error("provider binding version or configuration changed")]
    VersionConflict,
    #[error("provider binding does not match the selection")]
    InvalidDescriptor,
    #[error(transparent)]
    Acquisition(#[from] ProviderError),
    #[error("owned provider binding is unsupported")]
    ResourcesUnsupported,
    #[error(transparent)]
    ResourceRegistration(#[from] ProviderResourceRegistrationError),
}

/// One frozen implementation, held throughout an active attempt. Credentials
/// may refresh inside the implementation without changing this descriptor.
#[derive(Clone)]
pub struct BoundProvider {
    descriptor: ProviderBindingDescriptor,
    streamer: Arc<dyn Streamer>,
}
impl BoundProvider {
    /// Bind an implementation after independently checking the requested route.
    /// # Errors
    /// Rejects a descriptor that names a different provider or model.
    pub fn new(
        selection: &Selection,
        descriptor: ProviderBindingDescriptor,
        streamer: Arc<dyn Streamer>,
    ) -> Result<Self, ProviderBindingError> {
        validate_selection(selection, &descriptor)?;
        let streamer = Arc::new(BindingStream {
            descriptor: descriptor.clone(),
            inner: streamer,
        });
        Ok(Self {
            descriptor,
            streamer,
        })
    }
    #[must_use]
    pub fn descriptor(&self) -> &ProviderBindingDescriptor {
        &self.descriptor
    }
    #[must_use]
    pub fn streamer(&self) -> Arc<dyn Streamer> {
        self.streamer.clone()
    }
}

struct BindingStream {
    descriptor: ProviderBindingDescriptor,
    inner: Arc<dyn Streamer>,
}
#[async_trait::async_trait]
impl Streamer for BindingStream {
    async fn stream(
        &self,
        request: crate::ModelRequest,
    ) -> Result<crate::DeltaStream, ProviderError> {
        if validate_selection(&request.selection, &self.descriptor).is_err()
            || request.max_tokens == Some(0)
            || matches!((request.max_tokens, self.descriptor.limits().output_tokens), (Some(requested), Some(limit)) if requested > limit)
        {
            return Err(ProviderError {
                kind: crate::ProviderErrorKind::Invalid,
                message: "request does not match the frozen provider binding".into(),
                retryable: false,
            });
        }
        self.inner.stream(request).await
    }
}

/// Explicit host declarations for a resolver's routes. The host must keep the
/// wrapped resolver's behavior consistent with these identities, including any
/// opaque callbacks or gateway routing. Credential refresh is excluded.
pub struct VersionedResolver {
    inner: Arc<dyn crate::Resolver>,
    descriptors: std::collections::BTreeMap<(String, String), ProviderBindingDescriptor>,
}
impl VersionedResolver {
    /// Freeze an allowlist of model routes and their backend/configuration identity.
    /// # Errors
    /// Rejects duplicate routes rather than silently replacing a declaration.
    pub fn new(
        inner: Arc<dyn crate::Resolver>,
        descriptors: Vec<ProviderBindingDescriptor>,
    ) -> Result<Self, ProviderBindingError> {
        Ok(Self {
            inner,
            descriptors: collect_descriptors(descriptors)?,
        })
    }
}
#[async_trait::async_trait]
impl crate::Resolver for VersionedResolver {
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<ProviderBindingDescriptor, ProviderBindingError> {
        self.descriptors
            .get(&(selection.provider_id.clone(), selection.model_id.clone()))
            .cloned()
            .ok_or(ProviderBindingError::Unavailable)
    }
    async fn resolve(&self, selection: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        if !self
            .descriptors
            .contains_key(&(selection.provider_id.clone(), selection.model_id.clone()))
        {
            return Err(ProviderError {
                kind: crate::ProviderErrorKind::Invalid,
                message: "provider binding is unavailable".into(),
                retryable: false,
            });
        }
        self.inner.resolve(selection).await
    }
}

pub(crate) fn validate_selection(
    selection: &Selection,
    descriptor: &ProviderBindingDescriptor,
) -> Result<(), ProviderBindingError> {
    if descriptor.provider_id() != selection.provider_id
        || descriptor.model_id() != selection.model_id
    {
        Err(ProviderBindingError::InvalidDescriptor)
    } else {
        Ok(())
    }
}

type Descriptors = std::collections::BTreeMap<(String, String), ProviderBindingDescriptor>;
fn collect_descriptors(
    descriptors: Vec<ProviderBindingDescriptor>,
) -> Result<Descriptors, ProviderBindingError> {
    let mut routes = Descriptors::new();
    for descriptor in descriptors {
        let route = (
            descriptor.provider_id().into(),
            descriptor.model_id().into(),
        );
        if routes.insert(route, descriptor).is_some() {
            return Err(ProviderBindingError::InvalidDescriptor);
        }
    }
    Ok(routes)
}

/// Versioned routes for an extension-provided adapter. The extension's own
/// configuration identity must also cover its provider registration behavior.
pub struct VersionedAdapter {
    inner: Arc<dyn crate::ProviderAdapter>,
    info: crate::ProviderInfo,
    descriptors: Descriptors,
}
impl VersionedAdapter {
    /// Freeze declared model routes for exactly this adapter's provider ID.
    /// # Errors
    /// Rejects duplicate routes and descriptors naming another provider.
    pub fn new(
        inner: Arc<dyn crate::ProviderAdapter>,
        descriptors: Vec<ProviderBindingDescriptor>,
    ) -> Result<Self, ProviderBindingError> {
        let info = inner.info();
        let provider_id = &info.id;
        if descriptors
            .iter()
            .any(|descriptor| descriptor.provider_id() != provider_id)
        {
            return Err(ProviderBindingError::InvalidDescriptor);
        }
        Ok(Self {
            inner,
            info,
            descriptors: collect_descriptors(descriptors)?,
        })
    }
}
#[async_trait::async_trait]
impl crate::ProviderAdapter for VersionedAdapter {
    fn info(&self) -> crate::ProviderInfo {
        self.info.clone()
    }
    async fn models(&self) -> Result<Vec<crate::ModelDescriptor>, ProviderError> {
        Ok(self
            .descriptors
            .values()
            .map(|descriptor| crate::ModelDescriptor {
                provider_id: descriptor.provider_id().into(),
                id: descriptor.model_id().into(),
                context_limit: descriptor.limits().context_tokens.unwrap_or(0),
            })
            .collect())
    }
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<ProviderBindingDescriptor, ProviderBindingError> {
        self.descriptors
            .get(&(selection.provider_id.clone(), selection.model_id.clone()))
            .cloned()
            .ok_or(ProviderBindingError::Unavailable)
    }
    async fn build(&self, selection: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        if !self
            .descriptors
            .contains_key(&(selection.provider_id.clone(), selection.model_id.clone()))
        {
            return Err(ProviderError {
                kind: crate::ProviderErrorKind::Invalid,
                message: "provider binding is unavailable".into(),
                retryable: false,
            });
        }
        self.inner.build(selection).await
    }
}
