use super::ErrorClassifier;
use super::{
    CredentialPlacement, HttpAdapter, HttpClientConfig, Protocol, invalid,
    is_protected_static_header,
};
use crate::{
    DeltaStream, ModelDescriptor, ModelRequest, ProviderAdapter, ProviderError, ProviderInfo,
    Selection, Streamer,
};
use async_trait::async_trait;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Placement of the one Crabber-owned credential header.
///
/// `Bearer` sends exactly one `Authorization` header whose value is
/// `Bearer <credential>`, and no `x-api-key`; `XApiKey` sends exactly one
/// `x-api-key` and no `Authorization`.
pub enum AuthScheme {
    Bearer,
    XApiKey,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Token-limit field used by the Chat Completions request encoder.
pub enum ChatTokenField {
    MaxTokens,
    MaxCompletionTokens,
}

#[async_trait]
/// Supplies a custom adapter's credential for each HTTP attempt.
///
/// After a 401, Crabber calls `invalidate` with the exact stale credential and
/// retries once. Implementations own compare-and-invalidate and single-flight
/// refresh across concurrent requests; stale invalidation must not clear a
/// newer credential generation.
pub trait CredentialSource: Send + Sync {
    async fn credential(&self, request: &ModelRequest) -> Result<String, ProviderError>;
    async fn invalidate(&self, stale: &str);
}

/// Produces host headers for each attempt, after static headers are copied.
///
/// Hook values replace all static values of the same name. Authorization,
/// x-api-key, content-type, user-agent, and anthropic-version are protected and
/// cause the attempt to fail before it is sent.
pub trait RequestHeaderHook: Send + Sync {
    /// Produces additional headers for one request.
    ///
    /// # Errors
    ///
    /// Returns a provider error when the hook cannot produce valid headers.
    fn headers(&self, request: &ModelRequest) -> Result<HeaderMap, ProviderError>;
}

/// Observes every received HTTP response's raw status and headers.
///
/// The callback runs exactly once per received response, including a received
/// 401, after headers arrive and before retry handling, status classification,
/// or body consumption. It does not run for an attempt that fails before
/// response headers arrive.
///
/// `headers` is raw and unredacted. It may contain cookies, authentication
/// challenges, or vendor-specific secrets. Hosts must not indiscriminately log
/// or export the [`HeaderMap`] and are responsible for their own redaction.
pub trait ResponseObserver: Send + Sync {
    fn observe(&self, status: reqwest::StatusCode, headers: &HeaderMap);
}

#[derive(Clone)]
/// A host-configured HTTP adapter with a type-separated configuration surface.
///
/// Unlike [`HttpAdapter`], this type exposes custom-only settings. It converts
/// into the internal adapter only when registered with [`HttpResolver`].
pub struct CustomHttpAdapter {
    pub(super) inner: HttpAdapter,
}

impl CustomHttpAdapter {
    /// Prepends one validated host product, keeping Crabber's product last.
    /// # Errors
    /// Returns a sanitized invalid-provider error for an invalid product.
    pub fn try_with_user_agent_product(
        mut self,
        product: impl AsRef<str>,
    ) -> Result<Self, ProviderError> {
        self.inner = self.inner.try_with_user_agent_product(product)?;
        Ok(self)
    }

    #[must_use]
    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.inner.base_url = url.into();
        self
    }

    #[must_use]
    pub fn with_api_key(mut self, key: impl Into<String>) -> Self {
        self.inner.key_override = Some(key.into());
        self.inner.credential_source = None;
        self
    }

    #[must_use]
    pub fn with_auth_scheme(mut self, scheme: AuthScheme) -> Self {
        self.inner.credential_placement = match scheme {
            AuthScheme::Bearer => CredentialPlacement::Bearer,
            AuthScheme::XApiKey => CredentialPlacement::XApiKey,
        };
        self
    }

    #[must_use]
    pub fn with_credential_source(mut self, source: Arc<dyn CredentialSource>) -> Self {
        self.inner.credential_source = Some(source);
        self.inner.key_override = None;
        self
    }

    /// # Errors
    /// Returns an invalid-provider error when a protected header is present.
    pub fn try_with_static_headers(mut self, headers: HeaderMap) -> Result<Self, ProviderError> {
        if headers.keys().any(is_protected_static_header) {
            return Err(invalid());
        }
        self.inner.static_headers = headers;
        Ok(self)
    }

    #[must_use]
    pub fn with_request_header_hook(mut self, hook: Arc<dyn RequestHeaderHook>) -> Self {
        self.inner.request_header_hook = Some(hook);
        self
    }

    /// # Errors
    /// Returns an invalid-provider error when reqwest rejects the configuration.
    pub fn try_with_client_config(
        mut self,
        config: HttpClientConfig,
    ) -> Result<Self, ProviderError> {
        self.inner.client = config.build()?;
        Ok(self)
    }

    #[must_use]
    pub fn with_response_observer(mut self, observer: Arc<dyn ResponseObserver>) -> Self {
        self.inner.response_observer = Some(observer);
        self
    }

    #[must_use]
    pub fn with_error_classifier(mut self, classifier: Arc<dyn ErrorClassifier>) -> Self {
        self.inner.error_classifier = Some(classifier);
        self
    }

    #[must_use]
    pub fn with_chat_token_field(mut self, field: ChatTokenField) -> Self {
        self.inner.chat_token_mode = match field {
            ChatTokenField::MaxTokens => crate::chat::TokenMode::MaxTokens,
            ChatTokenField::MaxCompletionTokens => crate::chat::TokenMode::MaxCompletionTokens,
        };
        self
    }
}

impl From<CustomHttpAdapter> for HttpAdapter {
    fn from(adapter: CustomHttpAdapter) -> Self {
        adapter.inner
    }
}

impl HttpAdapter {
    pub(super) async fn custom_headers(
        &self,
        request: &ModelRequest,
    ) -> Result<(HeaderMap, String), ProviderError> {
        let credential = if let Some(source) = &self.credential_source {
            source.credential(request).await?
        } else {
            self.api_key()?
        };
        if credential.is_empty() {
            return Err(invalid());
        }

        let mut headers = self.static_headers.clone();
        if let Some(hook) = &self.request_header_hook {
            let hook_headers = hook.headers(request)?;
            if hook_headers.keys().any(is_protected_static_header) {
                return Err(invalid());
            }
            for name in hook_headers.keys() {
                headers.remove(name);
                for value in hook_headers.get_all(name) {
                    headers.append(name, value.clone());
                }
            }
        }

        headers.insert("user-agent", self.user_agent.clone());
        headers.insert("content-type", HeaderValue::from_static("application/json"));
        if self.protocol == Protocol::Messages {
            headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        }
        let (name, value) = match self.credential_placement {
            CredentialPlacement::Bearer => ("authorization", format!("Bearer {credential}")),
            CredentialPlacement::XApiKey => ("x-api-key", credential.clone()),
            #[cfg(feature = "opencode-go")]
            CredentialPlacement::ProtocolDefault => unreachable!(),
        };
        let mut value = HeaderValue::from_str(&value).map_err(|_| invalid())?;
        value.set_sensitive(true);
        headers.insert(HeaderName::from_static(name), value);
        Ok((headers, credential))
    }
}

#[async_trait]
impl Streamer for CustomHttpAdapter {
    async fn stream(&self, request: ModelRequest) -> Result<DeltaStream, ProviderError> {
        self.inner.send(request).await
    }
}

#[async_trait]
impl ProviderAdapter for CustomHttpAdapter {
    fn info(&self) -> ProviderInfo {
        self.inner.info()
    }

    async fn models(&self) -> Result<Vec<ModelDescriptor>, ProviderError> {
        self.inner.models().await
    }

    async fn build(&self, selection: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        self.inner.build(selection).await
    }
}
