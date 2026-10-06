#![allow(clippy::too_many_lines, clippy::items_after_statements)]
use crate::{
    DeltaStream, ModelDescriptor, ModelRequest, ProviderAdapter, ProviderError, ProviderInfo,
    Resolver, Selection, Streamer,
};
use async_trait::async_trait;
use reqwest::{
    Client, Response,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use serde_json::Value;
use std::{collections::HashMap, sync::Arc};

#[cfg(feature = "custom-http")]
mod client;
#[cfg(feature = "custom-http")]
mod custom;
mod errors;
mod streaming;
#[cfg(feature = "custom-http")]
pub use client::{HttpClientConfig, HttpProxyConfig};
#[cfg(feature = "custom-http")]
pub use custom::{
    AuthScheme, ChatTokenField, CredentialSource, CustomHttpAdapter, RequestHeaderHook,
    ResponseObserver,
};
#[cfg(feature = "custom-http")]
pub use errors::ErrorClassifier;
use errors::{auth, credentials_from_headers, invalid, response_error, transport_from_reqwest};

const CRABBER_USER_AGENT: &str = "crabber/0.1";

fn user_agent_product_is_valid(product: &str) -> bool {
    fn part_is_valid(part: &str) -> bool {
        (1..=64).contains(&part.len())
            && part
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
    }
    let mut parts = product.split('/');
    let name = parts.next().unwrap_or_default();
    part_is_valid(name)
        && !name.eq_ignore_ascii_case("crabber")
        && parts.next().is_none_or(part_is_valid)
        && parts.next().is_none()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Wire protocol implemented by an HTTP adapter.
pub enum Protocol {
    Responses,
    Messages,
    ChatCompletions,
}
impl Protocol {
    fn path(self) -> &'static str {
        match self {
            Self::Responses => "/responses",
            Self::Messages => "/messages",
            Self::ChatCompletions => "/chat/completions",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CredentialPlacement {
    #[cfg_attr(
        not(any(
            feature = "custom-http",
            feature = "openai",
            feature = "codex",
            feature = "opencode-go"
        )),
        allow(dead_code)
    )]
    Bearer,
    #[cfg_attr(
        not(any(
            feature = "custom-http",
            feature = "anthropic",
            feature = "opencode-go"
        )),
        allow(dead_code)
    )]
    XApiKey,
    #[cfg(feature = "opencode-go")]
    ProtocolDefault,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AdapterKind {
    #[cfg(feature = "anthropic")]
    Anthropic,
    #[cfg(feature = "openai")]
    OpenAi,
    #[cfg(feature = "codex")]
    Codex,
    #[cfg(feature = "opencode-go")]
    OpenCode,
    #[cfg(feature = "custom-http")]
    Custom,
}

#[derive(Clone)]
/// HTTP implementation of Crabber's public provider contracts.
pub struct HttpAdapter {
    id: String,
    base_url: String,
    protocol: Protocol,
    kind: AdapterKind,
    credential_placement: CredentialPlacement,
    #[allow(dead_code)]
    chat_token_mode: crate::chat::TokenMode,
    key_env: Option<&'static str>,
    key_override: Option<String>,
    client: Client,
    user_agent: HeaderValue,
    #[cfg(feature = "codex")]
    tokens: Option<Arc<crabber_auth::TokenManager>>,
    #[cfg(feature = "custom-http")]
    credential_source: Option<Arc<dyn CredentialSource>>,
    #[cfg(feature = "custom-http")]
    static_headers: HeaderMap,
    #[cfg(feature = "custom-http")]
    request_header_hook: Option<Arc<dyn RequestHeaderHook>>,
    #[cfg(feature = "custom-http")]
    response_observer: Option<Arc<dyn ResponseObserver>>,
    #[cfg(feature = "custom-http")]
    error_classifier: Option<Arc<dyn ErrorClassifier>>,
}

impl HttpAdapter {
    fn new(
        id: impl Into<String>,
        base_url: impl Into<String>,
        protocol: Protocol,
        kind: AdapterKind,
        key_env: Option<&'static str>,
        credential_placement: CredentialPlacement,
    ) -> Self {
        Self {
            id: id.into(),
            base_url: base_url.into(),
            protocol,
            kind,
            credential_placement,
            chat_token_mode: crate::chat::TokenMode::MaxTokens,
            key_env,
            key_override: None,
            user_agent: HeaderValue::from_static(CRABBER_USER_AGENT),
            client: Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("default HTTP client configuration must be valid"),
            #[cfg(feature = "codex")]
            tokens: None,
            #[cfg(feature = "custom-http")]
            credential_source: None,
            #[cfg(feature = "custom-http")]
            static_headers: HeaderMap::new(),
            #[cfg(feature = "custom-http")]
            request_header_hook: None,
            #[cfg(feature = "custom-http")]
            response_observer: None,
            #[cfg(feature = "custom-http")]
            error_classifier: None,
        }
    }
    #[cfg(feature = "anthropic")]
    #[must_use]
    pub fn anthropic() -> Self {
        Self::new(
            "anthropic",
            "https://api.anthropic.com/v1",
            Protocol::Messages,
            AdapterKind::Anthropic,
            Some("ANTHROPIC_API_KEY"),
            CredentialPlacement::XApiKey,
        )
    }
    #[cfg(feature = "openai")]
    #[must_use]
    pub fn openai() -> Self {
        Self::new(
            "openai",
            "https://api.openai.com/v1",
            Protocol::Responses,
            AdapterKind::OpenAi,
            Some("OPENAI_API_KEY"),
            CredentialPlacement::Bearer,
        )
    }
    #[cfg(feature = "opencode-go")]
    #[must_use]
    pub fn opencode_go(protocol: Protocol) -> Self {
        Self::new(
            "opencode-go",
            "https://opencode.ai/zen/go/v1",
            protocol,
            AdapterKind::OpenCode,
            Some("OPENCODE_GO_API_KEY"),
            CredentialPlacement::ProtocolDefault,
        )
    }
    #[cfg(feature = "codex")]
    pub fn codex(store: Arc<dyn crabber_auth::CredentialStore>) -> Self {
        let mut adapter = Self::new(
            "codex",
            "https://api.openai.com/v1",
            Protocol::Responses,
            AdapterKind::Codex,
            None,
            CredentialPlacement::Bearer,
        );
        adapter.tokens = Some(Arc::new(crabber_auth::TokenManager::new(store)));
        adapter
    }
    #[cfg(feature = "custom-http")]
    #[must_use]
    /// Creates a host-configured adapter without fetching credentials or
    /// contacting the network.
    ///
    /// Custom adapters return an empty model catalog and do not mint gateway
    /// tokens; discovery and token minting remain host-owned.
    pub fn custom(
        id: impl Into<String>,
        base_url: impl Into<String>,
        protocol: Protocol,
    ) -> CustomHttpAdapter {
        CustomHttpAdapter {
            inner: Self::new(
                id,
                base_url,
                protocol,
                AdapterKind::Custom,
                None,
                CredentialPlacement::Bearer,
            ),
        }
    }
    /// Override the base URL for a compatible proxy or local fixture server.
    #[must_use]
    pub fn with_base_url(mut self, url: impl Into<String>) -> Self {
        self.base_url = url.into();
        self
    }
    #[must_use]
    pub fn with_api_key(mut self, key: impl Into<String>) -> Self {
        self.key_override = Some(key.into());
        #[cfg(feature = "custom-http")]
        {
            self.credential_source = None;
        }
        self
    }
    /// Prepends one host product (`name` or `name/version`) to `crabber/0.1`.
    /// Each part must contain 1–64 ASCII HTTP token bytes. Replaces any earlier
    /// host product; the product name `crabber` is reserved (case insensitive).
    /// # Errors
    /// Returns a sanitized invalid-provider error when the product is invalid.
    pub fn try_with_user_agent_product(
        mut self,
        product: impl AsRef<str>,
    ) -> Result<Self, ProviderError> {
        let product = product.as_ref();
        if !user_agent_product_is_valid(product) {
            return Err(invalid());
        }
        self.user_agent = HeaderValue::from_str(&format!("{product} {CRABBER_USER_AGENT}"))
            .map_err(|_| invalid())?;
        Ok(self)
    }
    #[cfg(feature = "codex")]
    fn uses_codex_responses_mode(&self) -> bool {
        self.kind == AdapterKind::Codex
    }
    #[cfg(not(feature = "codex"))]
    fn uses_codex_responses_mode(&self) -> bool {
        let _ = self.kind;
        false
    }
    #[cfg(feature = "opencode-go")]
    fn uses_opencode_models(&self) -> bool {
        self.kind == AdapterKind::OpenCode
    }
    #[cfg(not(feature = "opencode-go"))]
    fn uses_opencode_models(&self) -> bool {
        let _ = self.kind;
        false
    }
    fn uses_opencode_session(&self) -> bool {
        self.uses_opencode_models()
    }
    fn api_key(&self) -> Result<String, ProviderError> {
        if let Some(key) = &self.key_override {
            return Ok(key.clone());
        }
        let Some(env) = self.key_env else {
            return Err(auth());
        };
        std::env::var(env)
            .ok()
            .filter(|v| !v.is_empty())
            .ok_or_else(auth)
    }
    #[allow(
        clippy::redundant_else,
        clippy::unused_async,
        clippy::unused_async_trait_impl
    )]
    async fn headers(&self, request: &ModelRequest) -> Result<HeaderMap, ProviderError> {
        #[cfg(feature = "custom-http")]
        if self.kind == AdapterKind::Custom {
            return self
                .custom_headers(request)
                .await
                .map(|(headers, _)| headers);
        }
        let mut headers = HeaderMap::new();
        headers.insert("user-agent", self.user_agent.clone());
        if self.uses_codex_responses_mode() {
            #[cfg(feature = "codex")]
            {
                let tokens = self.tokens.as_ref().ok_or_else(auth)?;
                let creds = tokens.credentials().await.map_err(|_| auth())?;
                insert(
                    &mut headers,
                    "authorization",
                    &format!("Bearer {}", creds.access_token),
                )?;
                headers.insert("originator", HeaderValue::from_static("advisor"));
                insert(&mut headers, "session_id", &request.identity.session_id.0)?;
            }
            #[cfg(not(feature = "codex"))]
            {
                return Err(auth());
            }
        } else {
            let key = self.api_key()?;
            let placement = match self.credential_placement {
                #[cfg(feature = "opencode-go")]
                CredentialPlacement::ProtocolDefault if self.protocol == Protocol::Messages => {
                    CredentialPlacement::XApiKey
                }
                #[cfg(feature = "opencode-go")]
                CredentialPlacement::ProtocolDefault => CredentialPlacement::Bearer,
                placement => placement,
            };
            match placement {
                CredentialPlacement::XApiKey => insert(&mut headers, "x-api-key", &key)?,
                CredentialPlacement::Bearer => {
                    insert(&mut headers, "authorization", &format!("Bearer {key}"))?;
                }
                #[cfg(feature = "opencode-go")]
                CredentialPlacement::ProtocolDefault => unreachable!(),
            }
        }
        if self.protocol == Protocol::Messages {
            headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        }
        if self.uses_opencode_session() {
            let session = &request.identity.session_id.0;
            if session.len() <= 256 && session.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
                insert(&mut headers, "x-opencode-session", session)?;
            }
        }
        Ok(headers)
    }
    async fn post(
        &self,
        url: &reqwest::Url,
        headers: HeaderMap,
        body: &Value,
    ) -> Result<Response, ProviderError> {
        self.client
            .post(url.clone())
            .headers(headers)
            .json(body)
            .send()
            .await
            .map_err(|error| transport_from_reqwest(&error))
    }

    async fn send(&self, request: ModelRequest) -> Result<DeltaStream, ProviderError> {
        let url = endpoint_url(&self.base_url, self.protocol)?;
        #[cfg(feature = "custom-http")]
        let (headers, custom_credential) = if self.kind == AdapterKind::Custom {
            let (headers, credential) = self.custom_headers(&request).await?;
            (headers, Some(credential))
        } else {
            (self.headers(&request).await?, None)
        };
        #[cfg(not(feature = "custom-http"))]
        let headers = self.headers(&request).await?;
        #[allow(unused_mut)]
        let mut attempt_credentials = credentials_from_headers(&headers);
        #[cfg(feature = "custom-http")]
        if let Some(credential) = &custom_credential {
            attempt_credentials.push(credential.clone());
        }
        let body = match self.protocol {
            Protocol::Responses => {
                crate::responses::body(&request, self.uses_codex_responses_mode())
            }
            Protocol::Messages => crate::messages::body(&request),
            Protocol::ChatCompletions => crate::chat::body(&request, self.chat_token_mode),
        };
        #[allow(unused_mut)]
        let mut response = self.post(&url, headers.clone(), &body).await?;
        #[cfg(feature = "custom-http")]
        if self.kind == AdapterKind::Custom {
            if let Some(observer) = &self.response_observer {
                observer.observe(response.status(), response.headers());
            }
            if response.status() == reqwest::StatusCode::UNAUTHORIZED
                && self.credential_source.is_some()
            {
                let stale = custom_credential.as_deref().ok_or_else(auth)?;
                let source = self.credential_source.as_ref().ok_or_else(auth)?;
                drop(response);
                source.invalidate(stale).await;
                let (retry_headers, retry_credential) = self.custom_headers(&request).await?;
                attempt_credentials.extend(credentials_from_headers(&retry_headers));
                attempt_credentials.push(retry_credential);
                response = self.post(&url, retry_headers, &body).await?;
                if let Some(observer) = &self.response_observer {
                    observer.observe(response.status(), response.headers());
                }
                if response.status() == reqwest::StatusCode::UNAUTHORIZED {
                    return Err(response_error(response, None, true, &attempt_credentials).await);
                }
            }
        }
        #[cfg(feature = "codex")]
        if self.uses_codex_responses_mode()
            && response.status() == reqwest::StatusCode::UNAUTHORIZED
        {
            let previous = headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("Bearer "))
                .ok_or_else(auth)?;
            self.tokens
                .as_ref()
                .ok_or_else(auth)?
                .force_refresh(previous)
                .await
                .map_err(|_| auth())?;
            let retry_headers = self.headers(&request).await?;
            attempt_credentials.extend(credentials_from_headers(&retry_headers));
            response = self.post(&url, retry_headers, &body).await?;
        }
        if !response.status().is_success() {
            #[cfg(feature = "custom-http")]
            let classifier = (self.kind == AdapterKind::Custom)
                .then_some(self.error_classifier.as_deref())
                .flatten();
            #[cfg(not(feature = "custom-http"))]
            let classifier = None;
            return Err(response_error(response, classifier, false, &attempt_credentials).await);
        }
        Ok(self.response_stream(response))
    }
}
fn insert(headers: &mut HeaderMap, name: &'static str, value: &str) -> Result<(), ProviderError> {
    headers.insert(
        HeaderName::from_static(name),
        HeaderValue::from_str(value).map_err(|_| invalid())?,
    );
    Ok(())
}
#[cfg(feature = "custom-http")]
fn is_protected_static_header(name: &HeaderName) -> bool {
    [
        HeaderName::from_static("authorization"),
        HeaderName::from_static("x-api-key"),
        HeaderName::from_static("content-type"),
        HeaderName::from_static("user-agent"),
        HeaderName::from_static("anthropic-version"),
    ]
    .contains(name)
}
fn endpoint_url(base_url: &str, protocol: Protocol) -> Result<reqwest::Url, ProviderError> {
    fn validate(url: &str) -> Result<reqwest::Url, ProviderError> {
        let url = reqwest::Url::parse(url).map_err(|_| invalid())?;
        if !matches!(url.scheme(), "http" | "https") || !url.has_host() {
            return Err(invalid());
        }
        Ok(url)
    }

    let base = validate(base_url)?;
    if base.query().is_some() || base.fragment().is_some() {
        return Err(invalid());
    }
    let assembled = format!(
        "{}{path}",
        base_url.trim_end_matches('/'),
        path = protocol.path()
    );
    validate(&assembled)
}
#[async_trait]
impl Streamer for HttpAdapter {
    async fn stream(&self, request: ModelRequest) -> Result<DeltaStream, ProviderError> {
        self.send(request).await
    }
}
#[async_trait]
impl ProviderAdapter for HttpAdapter {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            id: self.id.clone(),
            name: self.id.clone(),
        }
    }
    async fn models(&self) -> Result<Vec<ModelDescriptor>, ProviderError> {
        if !self.uses_opencode_models() {
            return Ok(Vec::new());
        }
        let key = self.api_key()?;
        let credentials = vec![key.clone(), format!("Bearer {key}")];
        let response = self
            .client
            .get(format!("{}/models", self.base_url.trim_end_matches('/')))
            .bearer_auth(key)
            .header("user-agent", self.user_agent.clone())
            .send()
            .await
            .map_err(|error| transport_from_reqwest(&error))?;
        if !response.status().is_success() {
            return Err(response_error(response, None, false, &credentials).await);
        }
        let bytes = response
            .bytes()
            .await
            .map_err(|error| transport_from_reqwest(&error))?;
        let envelope: Value = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        Ok(envelope["data"]
            .as_array()
            .ok_or_else(invalid)?
            .iter()
            .filter_map(|m| m["id"].as_str())
            .map(|id| ModelDescriptor {
                provider_id: self.id.clone(),
                id: id.into(),
                context_limit: 0,
            })
            .collect())
    }
    async fn build(&self, selection: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        if selection.provider_id != self.id {
            return Err(invalid());
        }
        Ok(Arc::new(self.clone()))
    }
}

#[derive(Default)]
/// Provider resolver populated with HTTP adapters by provider id.
pub struct HttpResolver {
    adapters: HashMap<String, HttpAdapter>,
}
impl HttpResolver {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    #[must_use]
    /// Registers an adapter. Registration performs no credential or network I/O.
    pub fn with_adapter(mut self, adapter: impl Into<HttpAdapter>) -> Self {
        let adapter = adapter.into();
        self.adapters.insert(adapter.id.clone(), adapter);
        self
    }
    #[must_use]
    pub fn from_env() -> Self {
        #[allow(unused_mut)]
        let mut resolver = Self::new();
        #[cfg(feature = "anthropic")]
        {
            resolver = resolver.with_adapter(HttpAdapter::anthropic());
        }
        #[cfg(feature = "openai")]
        {
            resolver = resolver.with_adapter(HttpAdapter::openai());
        }
        #[cfg(feature = "opencode-go")]
        {
            resolver = resolver.with_adapter(HttpAdapter::opencode_go(Protocol::Responses));
        }
        #[cfg(feature = "codex")]
        {
            resolver = resolver.with_adapter(HttpAdapter::codex(Arc::new(
                crabber_auth::FileCredentialStore::default_crabber(),
            )));
        }
        resolver
    }
}
#[async_trait]
impl Resolver for HttpResolver {
    async fn resolve(&self, selection: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        let adapter = self
            .adapters
            .get(&selection.provider_id)
            .ok_or_else(invalid)?;
        if selection.model_id.is_empty() {
            return Err(invalid());
        }
        adapter.build(selection).await
    }
}

#[cfg(all(
    test,
    any(feature = "custom-http", feature = "codex", feature = "opencode-go")
))]
mod tests;
