//! Bounded host registration, independent of extension and Store dependencies.
mod acquisition;
use crabber_core::{InputFingerprint, SessionContext};
use futures::future::BoxFuture;
use sha2::{Digest, Sha256};
use std::{future::Future, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ProviderResourceRegistrationError {
    #[error("provider resource ownership is unavailable")]
    Unsupported,
    #[error("provider resource scope is sealed")]
    Sealed,
    #[error("provider resource task capacity reached")]
    Capacity,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderResourceFailure {
    CleanupFailed,
    Cancelled,
}
pub type ProviderResourceTask = BoxFuture<'static, Result<(), ProviderResourceFailure>>;

/// The host must retain the actual task join before returning success, seal
/// registration at close, and retain failures after attempted cleanup.
pub trait ProviderResourceHost: Send + Sync {
    /// # Errors
    /// Refuses unsupported, sealed or exhausted registration before polling work.
    fn register(&self, task: ProviderResourceTask)
    -> Result<(), ProviderResourceRegistrationError>;
    fn closing(&self) -> BoxFuture<'static, ()>;
}
#[derive(Clone)]
pub struct ProviderResourceContext {
    session: SessionContext,
    host: Arc<dyn ProviderResourceHost>,
}
impl ProviderResourceContext {
    /// Trusted host capability bound to immutable routing before acquisition.
    #[must_use]
    pub fn from_host(session: SessionContext, host: Arc<dyn ProviderResourceHost>) -> Self {
        Self { session, host }
    }
    #[must_use]
    pub fn session(&self) -> &SessionContext {
        &self.session
    }
    pub async fn closing(&self) {
        self.host.closing().await;
    }
    /// Register retained work before polling it. No task abort handle escapes.
    /// # Errors
    /// Refuses registration when unavailable, sealed or full.
    pub fn spawn<W>(&self, work: W) -> Result<(), ProviderResourceRegistrationError>
    where
        W: Future<Output = Result<(), ProviderResourceFailure>> + Send + 'static,
    {
        self.host.register(Box::pin(work))
    }
    /// Register cleanup before its associated acquisition/effect. The action
    /// starts after closing and must include every actual nested resource join.
    /// # Errors
    /// Refuses registration before invoking the action when sealed or full.
    pub fn defer<F, W>(&self, action: F) -> Result<(), ProviderResourceRegistrationError>
    where
        F: FnOnce() -> W + Send + 'static,
        W: Future<Output = Result<(), ProviderResourceFailure>> + Send + 'static,
    {
        let closing = self.host.closing();
        self.spawn(async move {
            closing.await;
            action().await
        })
    }
}

/// A pure binding-resource policy declaration. It grants no ownership or joins.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderResourceContract {
    component: String,
    version: String,
    config: InputFingerprint,
}
impl ProviderResourceContract {
    /// All behavior/configuration that changes ownership belongs in this value.
    /// # Errors
    /// Refuses empty, oversized or control-bearing identities.
    pub fn new(
        component: String,
        version: String,
        config: InputFingerprint,
    ) -> Result<Self, super::ProviderBindingError> {
        if component.is_empty()
            || component.len() > 512
            || version.is_empty()
            || version.len() > 128
            || component
                .chars()
                .chain(version.chars())
                .any(char::is_control)
        {
            return Err(super::ProviderBindingError::InvalidDescriptor);
        }
        Ok(Self {
            component,
            version,
            config,
        })
    }
    /// # Panics
    /// Only if the SHA256 digest is not a valid fingerprint.
    #[must_use]
    pub fn fingerprint(&self) -> InputFingerprint {
        let value = serde_json::json!([
            "crabber.provider-binding.resources.v1",
            self.component,
            self.version,
            self.config.as_str()
        ]);
        InputFingerprint::new(format!(
            "{:x}",
            Sha256::digest(value.to_string().as_bytes())
        ))
        .expect("SHA256 fingerprint")
    }
}
