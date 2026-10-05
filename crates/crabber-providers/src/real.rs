#![allow(clippy::too_many_lines, clippy::items_after_statements)]
use crate::{
    DeltaStream, ModelDescriptor, ModelRequest, ProviderAdapter, ProviderError, ProviderErrorKind,
    ProviderInfo, Resolver, Selection, StreamDelta, Streamer, sse,
};
use async_trait::async_trait;
use futures::{StreamExt, stream};
use reqwest::{
    Client,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use serde_json::Value;
#[cfg(feature = "custom-http")]
use std::{any::Any, time::Duration};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[cfg(feature = "custom-http")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthScheme {
    Bearer,
    XApiKey,
}

#[cfg(feature = "custom-http")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatTokenField {
    MaxTokens,
    MaxCompletionTokens,
}

#[cfg(feature = "custom-http")]
#[async_trait]
pub trait CredentialSource: Send + Sync {
    async fn credential(&self, request: &ModelRequest) -> Result<String, ProviderError>;
    async fn invalidate(&self, stale: &str);
}

#[cfg(feature = "custom-http")]
pub trait RequestHeaderHook: Send + Sync {
    /// Produces additional headers for one request.
    ///
    /// # Errors
    ///
    /// Returns a provider error when the hook cannot produce valid headers.
    fn headers(&self, request: &ModelRequest) -> Result<HeaderMap, ProviderError>;
}

#[cfg(feature = "custom-http")]
pub trait ResponseObserver: Send + Sync {
    fn observe(&self, status: reqwest::StatusCode, headers: &HeaderMap);
}

#[cfg(feature = "custom-http")]
pub trait ErrorClassifier: Send + Sync {
    fn classify(&self, status: reqwest::StatusCode, excerpt: &str) -> (ProviderErrorKind, bool);
}

#[cfg(feature = "custom-http")]
pub struct HttpProxyConfig {
    proxy: reqwest::Proxy,
}

#[cfg(feature = "custom-http")]
impl HttpProxyConfig {
    /// Applies one proxy URL to every protocol supported by the HTTP client.
    ///
    /// # Errors
    ///
    /// Returns an invalid-provider error when `url` is not a valid proxy URL.
    pub fn all(url: impl AsRef<str>) -> Result<Self, ProviderError> {
        reqwest::Proxy::all(url.as_ref())
            .map(|proxy| Self { proxy })
            .map_err(|_| invalid())
    }

    #[must_use]
    pub fn with_basic_auth(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        let username = username.into();
        let password = password.into();
        self.proxy = self.proxy.basic_auth(&username, &password);
        self
    }
}

#[cfg(feature = "custom-http")]
pub struct HttpClientConfig {
    builder: reqwest::ClientBuilder,
}

#[cfg(feature = "custom-http")]
impl HttpClientConfig {
    #[must_use]
    pub fn new() -> Self {
        Self {
            builder: Client::builder().redirect(reqwest::redirect::Policy::none()),
        }
    }

    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.builder = self.builder.timeout(timeout);
        self
    }

    #[must_use]
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.builder = self.builder.connect_timeout(timeout);
        self
    }

    #[must_use]
    pub fn read_timeout(mut self, timeout: Duration) -> Self {
        self.builder = self.builder.read_timeout(timeout);
        self
    }

    #[must_use]
    pub fn pool_idle_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.builder = self.builder.pool_idle_timeout(timeout);
        self
    }

    #[must_use]
    pub fn pool_max_idle_per_host(mut self, max: usize) -> Self {
        self.builder = self.builder.pool_max_idle_per_host(max);
        self
    }

    #[must_use]
    pub fn with_proxy(mut self, proxy: HttpProxyConfig) -> Self {
        self.builder = self.builder.proxy(proxy.proxy);
        self
    }

    #[must_use]
    pub fn without_proxy(mut self) -> Self {
        self.builder = self.builder.no_proxy();
        self
    }

    #[must_use]
    pub fn add_root_certificate(mut self, certificate: reqwest::Certificate) -> Self {
        self.builder = self.builder.add_root_certificate(certificate);
        self
    }

    #[must_use]
    pub fn with_identity(mut self, identity: reqwest::Identity) -> Self {
        self.builder = self.builder.identity(identity);
        self
    }

    #[must_use]
    pub fn min_tls_version(mut self, version: reqwest::tls::Version) -> Self {
        self.builder = self.builder.min_tls_version(version);
        self
    }

    #[must_use]
    pub fn max_tls_version(mut self, version: reqwest::tls::Version) -> Self {
        self.builder = self.builder.max_tls_version(version);
        self
    }

    #[must_use]
    pub fn use_preconfigured_tls<T: Any + Send + Sync + 'static>(mut self, tls: T) -> Self {
        self.builder = self.builder.use_preconfigured_tls(tls);
        self
    }

    fn build(self) -> Result<Client, ProviderError> {
        self.builder.build().map_err(|_| invalid())
    }
}

#[cfg(feature = "custom-http")]
impl Default for HttpClientConfig {
    fn default() -> Self {
        Self::new()
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
enum ChatTokenMode {
    MaxTokens,
    #[cfg(feature = "custom-http")]
    MaxCompletionTokens,
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
pub struct HttpAdapter {
    id: String,
    base_url: String,
    protocol: Protocol,
    kind: AdapterKind,
    credential_placement: CredentialPlacement,
    #[allow(dead_code)]
    chat_token_mode: ChatTokenMode,
    key_env: Option<&'static str>,
    key_override: Option<String>,
    client: Client,
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
            chat_token_mode: ChatTokenMode::MaxTokens,
            key_env,
            key_override: None,
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
    pub fn custom(id: impl Into<String>, base_url: impl Into<String>, protocol: Protocol) -> Self {
        Self::new(
            id,
            base_url,
            protocol,
            AdapterKind::Custom,
            None,
            CredentialPlacement::Bearer,
        )
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
    #[cfg(feature = "custom-http")]
    #[must_use]
    pub fn with_auth_scheme(mut self, scheme: AuthScheme) -> Self {
        self.credential_placement = match scheme {
            AuthScheme::Bearer => CredentialPlacement::Bearer,
            AuthScheme::XApiKey => CredentialPlacement::XApiKey,
        };
        self
    }
    #[cfg(feature = "custom-http")]
    #[must_use]
    pub fn with_credential_source(mut self, source: Arc<dyn CredentialSource>) -> Self {
        self.credential_source = Some(source);
        self.key_override = None;
        self
    }
    /// Installs validated static request headers.
    ///
    /// # Errors
    ///
    /// Returns a non-retryable invalid-provider error when a protected header name is present.
    #[cfg(feature = "custom-http")]
    pub fn try_with_static_headers(mut self, headers: HeaderMap) -> Result<Self, ProviderError> {
        if headers.keys().any(is_protected_static_header) {
            return Err(invalid());
        }
        self.static_headers = headers;
        Ok(self)
    }
    #[cfg(feature = "custom-http")]
    #[must_use]
    pub fn with_request_header_hook(mut self, hook: Arc<dyn RequestHeaderHook>) -> Self {
        self.request_header_hook = Some(hook);
        self
    }
    /// Builds and installs the configured HTTP client.
    ///
    /// # Errors
    ///
    /// Returns an invalid-provider error when reqwest rejects the final client configuration.
    #[cfg(feature = "custom-http")]
    pub fn try_with_client_config(
        mut self,
        config: HttpClientConfig,
    ) -> Result<Self, ProviderError> {
        self.client = config.build()?;
        Ok(self)
    }
    #[cfg(feature = "custom-http")]
    #[must_use]
    pub fn with_response_observer(mut self, observer: Arc<dyn ResponseObserver>) -> Self {
        self.response_observer = Some(observer);
        self
    }
    #[cfg(feature = "custom-http")]
    #[must_use]
    pub fn with_error_classifier(mut self, classifier: Arc<dyn ErrorClassifier>) -> Self {
        self.error_classifier = Some(classifier);
        self
    }
    #[cfg(feature = "custom-http")]
    #[must_use]
    pub fn with_chat_token_field(mut self, field: ChatTokenField) -> Self {
        self.chat_token_mode = match field {
            ChatTokenField::MaxTokens => ChatTokenMode::MaxTokens,
            ChatTokenField::MaxCompletionTokens => ChatTokenMode::MaxCompletionTokens,
        };
        self
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
        let mut headers = HeaderMap::new();
        headers.insert("user-agent", HeaderValue::from_static("crabber/0.1"));
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
    async fn send(&self, request: ModelRequest) -> Result<DeltaStream, ProviderError> {
        let headers = self.headers(&request).await?;
        let body = match self.protocol {
            Protocol::Responses => {
                crate::responses::body(&request, self.uses_codex_responses_mode())
            }
            Protocol::Messages => crate::messages::body(&request),
            Protocol::ChatCompletions => crate::chat::body(&request),
        };
        let url = format!(
            "{}{}",
            self.base_url.trim_end_matches('/'),
            self.protocol.path()
        );
        let response = self
            .client
            .post(&url)
            .headers(headers.clone())
            .json(&body)
            .send()
            .await
            .map_err(|_| transport())?;
        #[cfg(feature = "codex")]
        let response = {
            let mut response = response;
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
                response = self
                    .client
                    .post(&url)
                    .headers(self.headers(&request).await?)
                    .json(&body)
                    .send()
                    .await
                    .map_err(|_| transport())?;
            }
            response
        };
        if !response.status().is_success() {
            return Err(status_error(response.status()));
        }
        let codec = match self.protocol {
            Protocol::Responses => Codec::Responses(crate::responses::Codec::new(
                self.uses_codex_responses_mode(),
            )),
            Protocol::Messages => Codec::Messages(crate::messages::Codec::default()),
            Protocol::ChatCompletions => Codec::Chat(crate::chat::Codec::default()),
        };
        struct State {
            bytes: futures::stream::BoxStream<'static, Result<bytes::Bytes, reqwest::Error>>,
            parser: sse::Parser,
            codec: Codec,
            queue: VecDeque<StreamDelta>,
            complete: bool,
            ended: bool,
        }
        let state = State {
            bytes: Box::pin(response.bytes_stream()),
            parser: sse::Parser::default(),
            codec,
            queue: VecDeque::new(),
            complete: false,
            ended: false,
        };
        Ok(Box::pin(stream::unfold(state, |mut state| async move {
            loop {
                if let Some(item) = state.queue.pop_front() {
                    return Some((item, state));
                }
                if state.ended {
                    return None;
                }
                match state.bytes.next().await {
                    Some(Ok(chunk)) => {
                        let result = state
                            .parser
                            .push(&chunk)
                            .and_then(|events| decode_events(&mut state.codec, events));
                        match result {
                            Ok(items) => {
                                for item in items {
                                    if matches!(item, StreamDelta::Completed) {
                                        state.complete = true;
                                    }
                                    state.queue.push_back(item);
                                }
                            }
                            Err(error) => {
                                state.queue.push_back(StreamDelta::Error(error));
                                state.ended = true;
                            }
                        }
                    }
                    Some(Err(_)) => {
                        state.queue.push_back(StreamDelta::Error(transport()));
                        state.ended = true;
                    }
                    None => {
                        let result = state
                            .parser
                            .finish()
                            .and_then(|events| decode_events(&mut state.codec, events));
                        if let Ok(items) = result {
                            for item in items {
                                if matches!(item, StreamDelta::Completed) {
                                    state.complete = true;
                                }
                                state.queue.push_back(item);
                            }
                        } else {
                            state.queue.push_back(StreamDelta::Error(transport()));
                        }
                        if !state.complete && state.queue.is_empty() {
                            state.queue.push_back(StreamDelta::Error(transport()));
                        }
                        state.ended = true;
                    }
                }
            }
        })))
    }
}
fn decode_events(
    codec: &mut Codec,
    events: Vec<sse::Event>,
) -> Result<Vec<StreamDelta>, ProviderError> {
    let mut out = Vec::new();
    for event in events {
        out.extend(codec.event(&event)?);
    }
    Ok(out)
}
enum Codec {
    Responses(crate::responses::Codec),
    Messages(crate::messages::Codec),
    Chat(crate::chat::Codec),
}
impl Codec {
    fn event(&mut self, event: &sse::Event) -> Result<Vec<StreamDelta>, ProviderError> {
        match self {
            Self::Responses(c) => c.event(event),
            Self::Messages(c) => c.event(event),
            Self::Chat(c) => c.event(event),
        }
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
fn auth() -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::Auth,
        message: "credentials unavailable".into(),
        retryable: false,
    }
}
fn invalid() -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::Invalid,
        message: "invalid provider setting".into(),
        retryable: false,
    }
}
fn transport() -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::Transport,
        message: "provider stream interrupted".into(),
        retryable: true,
    }
}
fn status_error(status: reqwest::StatusCode) -> ProviderError {
    let kind = match status.as_u16() {
        401 | 403 => ProviderErrorKind::Auth,
        429 => ProviderErrorKind::RateLimited,
        500..=599 => ProviderErrorKind::Server,
        _ => ProviderErrorKind::Invalid,
    };
    ProviderError {
        retryable: matches!(
            kind,
            ProviderErrorKind::RateLimited | ProviderErrorKind::Server
        ),
        kind,
        message: format!("provider HTTP {}", status.as_u16()),
    }
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
        let response = self
            .client
            .get(format!("{}/models", self.base_url.trim_end_matches('/')))
            .bearer_auth(key)
            .send()
            .await
            .map_err(|_| transport())?;
        if !response.status().is_success() {
            return Err(status_error(response.status()));
        }
        let envelope: Value = response.json().await.map_err(|_| invalid())?;
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
pub struct HttpResolver {
    adapters: HashMap<String, HttpAdapter>,
}
impl HttpResolver {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    #[must_use]
    pub fn with_adapter(mut self, adapter: HttpAdapter) -> Self {
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
mod tests {
    use super::*;
    #[cfg(any(feature = "codex", feature = "opencode-go"))]
    use crate::RequestIdentity;
    #[cfg(any(feature = "codex", feature = "opencode-go"))]
    use crabber_core::{RunId, SessionId, TurnId};
    #[cfg(any(feature = "codex", feature = "opencode-go"))]
    fn request(provider: &str) -> ModelRequest {
        ModelRequest {
            identity: RequestIdentity {
                session_id: SessionId::from("session-1"),
                run_id: RunId::from("run-1"),
                turn_id: TurnId::from("turn-1"),
            },
            selection: Selection {
                provider_id: provider.into(),
                model_id: "model".into(),
            },
            system: None,
            messages: vec![],
            tools: vec![],
            temperature: None,
            max_tokens: None,
            tool_choice: None,
        }
    }
    #[cfg(feature = "codex")]
    #[test]
    fn real_codex_selects_privileged_responses_mode() {
        use crabber_auth::MemoryCredentialStore;

        let adapter = HttpAdapter::codex(Arc::new(MemoryCredentialStore::default()));
        assert!(adapter.uses_codex_responses_mode());
    }
    #[cfg(feature = "opencode-go")]
    #[test]
    fn real_opencode_selects_special_models_and_session_behavior() {
        let adapter = HttpAdapter::opencode_go(Protocol::Responses);
        assert!(adapter.uses_opencode_models());
        assert!(adapter.uses_opencode_session());
    }
    #[cfg(all(feature = "custom-http", feature = "codex"))]
    #[test]
    fn real_and_custom_codex_ids_have_distinct_privilege() {
        use crabber_auth::MemoryCredentialStore;

        let real = HttpAdapter::codex(Arc::new(MemoryCredentialStore::default()));
        let impostor = HttpAdapter::custom("codex", "https://example.test", Protocol::Responses);
        assert!(real.uses_codex_responses_mode());
        assert!(!impostor.uses_codex_responses_mode());
    }
    #[cfg(all(feature = "custom-http", feature = "opencode-go"))]
    #[test]
    fn real_and_custom_opencode_ids_have_distinct_privilege() {
        let real = HttpAdapter::opencode_go(Protocol::Responses);
        let impostor =
            HttpAdapter::custom("opencode-go", "https://example.test", Protocol::Responses);
        assert!(real.uses_opencode_models());
        assert!(!impostor.uses_opencode_models());
        assert!(real.uses_opencode_session());
        assert!(!impostor.uses_opencode_session());
    }
    #[cfg(feature = "custom-http")]
    #[test]
    fn static_headers_accept_unprotected_names() {
        let mut headers = HeaderMap::new();
        headers.insert("x-request-label", HeaderValue::from_static("safe-value"));

        let adapter = HttpAdapter::custom("custom", "https://example.test", Protocol::Responses)
            .try_with_static_headers(headers)
            .expect("unprotected static header should be accepted");

        assert_eq!(adapter.static_headers["x-request-label"], "safe-value");
    }
    #[cfg(feature = "custom-http")]
    #[test]
    fn static_headers_reject_protected_names_without_disclosure() {
        for name in [
            "authorization",
            "x-api-key",
            "content-type",
            "user-agent",
            "anthropic-version",
        ] {
            let secret = format!("secret-for-{name}");
            let mut headers = HeaderMap::new();
            headers.insert(
                HeaderName::from_bytes(name.as_bytes()).unwrap(),
                HeaderValue::from_str(&secret).unwrap(),
            );

            let error = HttpAdapter::custom(
                "custom",
                "https://credential@example.test",
                Protocol::Responses,
            )
            .try_with_static_headers(headers)
            .err()
            .unwrap_or_else(|| panic!("protected header {name} should be rejected"));

            assert_eq!(error.kind, ProviderErrorKind::Invalid);
            assert!(!error.retryable);
            let rendered = error.to_string();
            assert!(!rendered.contains(&secret));
            assert!(!rendered.contains("credential"));
        }
    }
    #[cfg(feature = "custom-http")]
    #[test]
    fn custom_builder_precedence_and_defaults_are_stable() {
        struct Source;
        #[async_trait]
        impl CredentialSource for Source {
            async fn credential(&self, _request: &ModelRequest) -> Result<String, ProviderError> {
                Ok("dynamic".into())
            }

            async fn invalidate(&self, _stale: &str) {}
        }

        let adapter = HttpAdapter::custom("custom", "https://example.test", Protocol::Responses);
        assert!(matches!(
            adapter.credential_placement,
            CredentialPlacement::Bearer
        ));
        assert!(matches!(adapter.chat_token_mode, ChatTokenMode::MaxTokens));

        let dynamic = adapter
            .with_api_key("static")
            .with_credential_source(Arc::new(Source));
        assert!(dynamic.key_override.is_none());
        assert!(dynamic.credential_source.is_some());

        let static_key = dynamic.with_api_key("replacement");
        assert_eq!(static_key.key_override.as_deref(), Some("replacement"));
        assert!(static_key.credential_source.is_none());
    }
    #[cfg(feature = "custom-http")]
    #[test]
    fn invalid_proxy_error_does_not_disclose_url_credentials() {
        let error = HttpProxyConfig::all("http://proxy-user:super-secret@[invalid")
            .err()
            .expect("proxy URL should be rejected");
        let rendered = error.to_string();
        assert!(!rendered.contains("proxy-user"));
        assert!(!rendered.contains("super-secret"));
    }
    #[cfg(feature = "opencode-go")]
    #[tokio::test]
    async fn opencode_headers_follow_protocol() {
        let req = request("opencode-go");
        for protocol in [
            Protocol::Responses,
            Protocol::ChatCompletions,
            Protocol::Messages,
        ] {
            let headers = HttpAdapter::opencode_go(protocol)
                .with_api_key("test-key")
                .headers(&req)
                .await
                .unwrap();
            assert_eq!(headers["x-opencode-session"], "session-1");
            if protocol == Protocol::Messages {
                assert_eq!(headers["x-api-key"], "test-key");
                assert_eq!(headers["anthropic-version"], "2023-06-01");
                assert!(!headers.contains_key("authorization"));
            } else {
                assert_eq!(headers["authorization"], "Bearer test-key");
                assert!(!headers.contains_key("x-api-key"));
            }
        }
        let mut invalid = req.clone();
        invalid.identity.session_id = SessionId::from("session with spaces");
        let headers = HttpAdapter::opencode_go(Protocol::Responses)
            .with_api_key("test-key")
            .headers(&invalid)
            .await
            .unwrap();
        assert!(!headers.contains_key("x-opencode-session"));
    }
    #[cfg(feature = "codex")]
    #[tokio::test]
    async fn codex_headers_have_exact_session_and_originator() {
        use crabber_auth::{CredentialStore, MemoryCredentialStore, OAuthCredentials, now_ms};
        let store: Arc<dyn CredentialStore> = Arc::new(MemoryCredentialStore::default());
        store
            .save(&OAuthCredentials {
                client_id: "client".into(),
                host_id: "host".into(),
                account_id: "account".into(),
                access_token: "test-token".into(),
                refresh_token: "refresh".into(),
                id_token: "id".into(),
                scopes: vec!["chatgpt.tokens.use.direct".into()],
                expires_unix_ms: now_ms() + 3_600_000,
            })
            .unwrap();
        let headers = HttpAdapter::codex(store)
            .headers(&request("codex"))
            .await
            .unwrap();
        assert_eq!(headers["authorization"], "Bearer test-token");
        assert_eq!(headers["originator"], "advisor");
        assert_eq!(headers["session_id"], "session-1");
    }
}
