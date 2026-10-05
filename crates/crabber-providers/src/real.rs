#![allow(clippy::too_many_lines, clippy::items_after_statements)]
use crate::{
    DeltaStream, ModelDescriptor, ModelRequest, ProviderAdapter, ProviderError, ProviderErrorKind,
    ProviderInfo, Resolver, Selection, StreamDelta, Streamer, sse,
};
use async_trait::async_trait;
use futures::{StreamExt, stream};
use reqwest::{
    Client, Response,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use serde_json::Value;
#[cfg(feature = "custom-http")]
use std::{any::Any, time::Duration};
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

const ERROR_EXCERPT_MAX_BYTES: usize = 4096;
/// `"provider HTTP " + 3 status digits + ": "`.
const ERROR_MESSAGE_PREFIX_BYTES: usize = 19;

#[derive(Clone, Copy)]
enum TransportCause {
    Connect,
    Timeout,
    Body,
}

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
    chat_token_mode: crate::chat::TokenMode,
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
            chat_token_mode: crate::chat::TokenMode::MaxTokens,
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
            ChatTokenField::MaxTokens => crate::chat::TokenMode::MaxTokens,
            ChatTokenField::MaxCompletionTokens => crate::chat::TokenMode::MaxCompletionTokens,
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
        #[cfg(feature = "custom-http")]
        if self.kind == AdapterKind::Custom {
            return self
                .custom_headers(request)
                .await
                .map(|(headers, _)| headers);
        }
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
    #[cfg(feature = "custom-http")]
    async fn custom_headers(
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

        headers.insert("user-agent", HeaderValue::from_static("crabber/0.1"));
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
        let mut response = self
            .client
            .post(url.clone())
            .headers(headers.clone())
            .json(&body)
            .send()
            .await
            .map_err(|error| transport_from_reqwest(&error))?;
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
                response = self
                    .client
                    .post(url.clone())
                    .headers(retry_headers)
                    .json(&body)
                    .send()
                    .await
                    .map_err(|error| transport_from_reqwest(&error))?;
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
            response = self
                .client
                .post(url.clone())
                .headers(retry_headers)
                .json(&body)
                .send()
                .await
                .map_err(|error| transport_from_reqwest(&error))?;
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
            protocol_complete: bool,
            ended: bool,
        }
        let state = State {
            bytes: Box::pin(response.bytes_stream()),
            parser: sse::Parser::default(),
            codec,
            queue: VecDeque::new(),
            protocol_complete: false,
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
                        for segment in chunk.split_inclusive(|byte| *byte == b'\n') {
                            let result = state
                                .parser
                                .push(segment)
                                .and_then(|events| decode_events(&mut state.codec, events));
                            match result {
                                Ok(items) => {
                                    if queue_items(
                                        &mut state.queue,
                                        &mut state.protocol_complete,
                                        items,
                                    ) {
                                        state.ended = true;
                                    }
                                }
                                Err(error) => {
                                    state.protocol_complete = false;
                                    state.queue.push_back(StreamDelta::Error(error));
                                    state.ended = true;
                                }
                            }
                            if state.ended {
                                break;
                            }
                        }
                    }
                    Some(Err(error)) => {
                        state
                            .queue
                            .push_back(StreamDelta::Error(transport_from_reqwest(&error)));
                        state.ended = true;
                    }
                    None => {
                        match state.parser.finish() {
                            Ok(events) => match decode_events(&mut state.codec, events) {
                                Ok(items) => {
                                    let protocol_error = queue_items(
                                        &mut state.queue,
                                        &mut state.protocol_complete,
                                        items,
                                    );
                                    if !protocol_error {
                                        if state.protocol_complete {
                                            state.queue.push_back(StreamDelta::Completed);
                                        } else {
                                            state.queue.push_back(StreamDelta::Error(transport(
                                                TransportCause::Body,
                                            )));
                                        }
                                    }
                                }
                                Err(error) => state.queue.push_back(StreamDelta::Error(error)),
                            },
                            Err(_) => state
                                .queue
                                .push_back(StreamDelta::Error(transport(TransportCause::Body))),
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
        for item in codec.event(&event)? {
            let terminal = matches!(item, StreamDelta::Error(_));
            out.push(item);
            if terminal {
                return Ok(out);
            }
        }
    }
    Ok(out)
}
fn queue_items(
    queue: &mut VecDeque<StreamDelta>,
    protocol_complete: &mut bool,
    items: Vec<StreamDelta>,
) -> bool {
    for item in items {
        if matches!(item, StreamDelta::Completed) {
            *protocol_complete = true;
            continue;
        }
        if matches!(item, StreamDelta::Error(_)) {
            *protocol_complete = false;
            queue.push_back(item);
            return true;
        }
        if !*protocol_complete {
            queue.push_back(item);
        }
    }
    false
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
fn transport(cause: TransportCause) -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::Transport,
        message: match cause {
            TransportCause::Connect => "provider transport connect",
            TransportCause::Timeout => "provider transport timeout",
            TransportCause::Body => "provider transport body",
        }
        .into(),
        retryable: true,
    }
}
fn transport_from_reqwest(error: &reqwest::Error) -> ProviderError {
    if error.is_timeout() {
        transport(TransportCause::Timeout)
    } else if error.is_connect() {
        transport(TransportCause::Connect)
    } else {
        transport(TransportCause::Body)
    }
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
fn credentials_from_headers(headers: &HeaderMap) -> Vec<String> {
    let mut credentials = Vec::new();
    for name in headers.keys() {
        let canonical = name == "authorization" || name == "x-api-key";
        for value in headers.get_all(name) {
            if !canonical && !value.is_sensitive() {
                continue;
            }
            let value = String::from_utf8_lossy(value.as_bytes()).into_owned();
            if value.is_empty() {
                continue;
            }
            if let Some(raw) = value.strip_prefix("Bearer ").filter(|raw| !raw.is_empty()) {
                credentials.push(raw.to_owned());
            }
            credentials.push(value);
        }
    }
    credentials
}
fn truncate_utf8(value: &mut String, max_bytes: usize) {
    if value.len() <= max_bytes {
        return;
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value.truncate(end);
}
fn sanitize_excerpt(source: &[u8], credentials: &[String], may_be_truncated: bool) -> String {
    let mut representations = credentials
        .iter()
        .filter(|value| !value.is_empty())
        .flat_map(|credential| {
            let serialized = serde_json::to_string(credential).unwrap_or_default();
            let escaped_payload = serialized
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
                .unwrap_or_default()
                .to_owned();
            [credential.clone(), serialized, escaped_payload]
        })
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    representations.sort_unstable();
    representations.dedup();
    representations.sort_unstable_by_key(|value| std::cmp::Reverse(value.len()));
    let terminal_prefix_start = may_be_truncated
        .then(|| {
            let terminal_full_match_start = representations
                .iter()
                .map(String::as_bytes)
                .filter(|representation| source.ends_with(representation))
                .map(|representation| source.len() - representation.len())
                .min()
                .unwrap_or(source.len());
            representations
                .iter()
                .map(String::as_bytes)
                .filter_map(|representation| {
                    let max_prefix = source.len().min(representation.len().saturating_sub(1));
                    (1..=max_prefix).rev().find_map(|prefix_len| {
                        source
                            .ends_with(&representation[..prefix_len])
                            .then_some(source.len() - prefix_len)
                    })
                })
                .filter(|start| *start < terminal_full_match_start)
                .min()
        })
        .flatten();
    let source = terminal_prefix_start.map_or(source, |start| &source[..start]);
    let mut excerpt = String::from_utf8_lossy(source).into_owned();
    for representation in &representations {
        excerpt = excerpt.replace(representation, "[REDACTED]");
    }
    if terminal_prefix_start.is_some() {
        excerpt.push_str("[REDACTED]");
    }
    truncate_utf8(&mut excerpt, ERROR_EXCERPT_MAX_BYTES);
    excerpt
}
async fn response_error(
    response: Response,
    classifier: Option<&dyn ErrorClassifier>,
    force_auth: bool,
    credentials: &[String],
) -> ProviderError {
    let status = response.status();
    let mut source = Vec::with_capacity(ERROR_EXCERPT_MAX_BYTES);
    let mut stream = response.bytes_stream();
    while source.len() < ERROR_EXCERPT_MAX_BYTES {
        match stream.next().await {
            Some(Ok(chunk)) => {
                let remaining = ERROR_EXCERPT_MAX_BYTES - source.len();
                source.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
            }
            Some(Err(error)) => return transport_from_reqwest(&error),
            None => break,
        }
    }
    let excerpt = sanitize_excerpt(
        &source,
        credentials,
        source.len() == ERROR_EXCERPT_MAX_BYTES,
    );
    let (kind, retryable) = if force_auth {
        (ProviderErrorKind::Auth, false)
    } else if let Some(classifier) = classifier {
        classifier.classify(status, &excerpt)
    } else {
        status_classification(status)
    };
    let message = format!("provider HTTP {}: {excerpt}", status.as_u16());
    debug_assert!(message.len() <= ERROR_MESSAGE_PREFIX_BYTES + ERROR_EXCERPT_MAX_BYTES);
    ProviderError {
        kind,
        message,
        retryable,
    }
}
fn status_classification(status: reqwest::StatusCode) -> (ProviderErrorKind, bool) {
    let kind = match status.as_u16() {
        401 | 403 => ProviderErrorKind::Auth,
        429 => ProviderErrorKind::RateLimited,
        500..=599 => ProviderErrorKind::Server,
        _ => ProviderErrorKind::Invalid,
    };
    let retryable = matches!(
        kind,
        ProviderErrorKind::RateLimited | ProviderErrorKind::Server
    );
    (kind, retryable)
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
    #[cfg(any(feature = "custom-http", feature = "codex", feature = "opencode-go"))]
    use crate::RequestIdentity;
    #[cfg(any(feature = "custom-http", feature = "codex", feature = "opencode-go"))]
    use crabber_core::{RunId, SessionId, TurnId};
    #[cfg(any(feature = "codex", feature = "opencode-go"))]
    use std::time::Duration;
    #[cfg(any(feature = "codex", feature = "opencode-go"))]
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        time::timeout,
    };

    #[cfg(any(feature = "codex", feature = "opencode-go"))]
    const LOOPBACK_TIMEOUT: Duration = Duration::from_secs(5);

    #[cfg(any(feature = "codex", feature = "opencode-go"))]
    async fn accept_loopback_request(listener: &TcpListener) -> (TcpStream, String) {
        let (mut socket, _) = timeout(LOOPBACK_TIMEOUT, listener.accept())
            .await
            .expect("timed out waiting for provider request")
            .expect("failed to accept provider request");
        let mut raw = Vec::new();
        let mut buffer = [0_u8; 4096];
        let header_end = loop {
            let n = timeout(LOOPBACK_TIMEOUT, socket.read(&mut buffer))
                .await
                .expect("timed out reading provider request")
                .expect("failed to read provider request");
            assert_ne!(n, 0, "request ended before its headers");
            raw.extend_from_slice(&buffer[..n]);
            if let Some(position) = raw.windows(4).position(|window| window == b"\r\n\r\n") {
                break position + 4;
            }
        };
        let headers = String::from_utf8(raw[..header_end].to_vec())
            .expect("provider request headers were not UTF-8");
        let content_length = headers
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .map_or(0, |(_, value)| {
                value
                    .trim()
                    .parse::<usize>()
                    .expect("invalid request Content-Length")
            });
        let request_end = header_end
            .checked_add(content_length)
            .expect("request length overflowed");
        while raw.len() < request_end {
            let n = timeout(LOOPBACK_TIMEOUT, socket.read(&mut buffer))
                .await
                .expect("timed out reading provider request body")
                .expect("failed to read provider request body");
            assert_ne!(n, 0, "request ended before its declared body");
            raw.extend_from_slice(&buffer[..n]);
        }
        (socket, headers)
    }

    #[cfg(any(feature = "codex", feature = "opencode-go"))]
    async fn write_loopback_response(socket: &mut TcpStream, response: &[u8]) {
        timeout(LOOPBACK_TIMEOUT, socket.write_all(response))
            .await
            .expect("timed out writing provider response")
            .expect("failed to write provider response");
    }
    #[cfg(feature = "custom-http")]
    #[test]
    fn error_excerpts_are_lossy_utf8_redacted_and_byte_bounded() {
        let below = sanitize_excerpt(b"short error", &[], false);
        assert_eq!(below, "short error");

        let above = sanitize_excerpt(&vec![b'x'; ERROR_EXCERPT_MAX_BYTES + 100], &[], true);
        assert_eq!(above.len(), ERROR_EXCERPT_MAX_BYTES);

        let invalid = sanitize_excerpt(&vec![0xff; ERROR_EXCERPT_MAX_BYTES], &[], true);
        assert!(invalid.len() <= ERROR_EXCERPT_MAX_BYTES);
        assert!(invalid.is_char_boundary(invalid.len()));
        assert!(invalid.contains('\u{fffd}'));

        let mut split = vec![b'a'; ERROR_EXCERPT_MAX_BYTES - 1];
        split.push(0xf0);
        let split = sanitize_excerpt(&split, &[], true);
        assert!(split.len() <= ERROR_EXCERPT_MAX_BYTES);
        assert!(split.is_char_boundary(split.len()));

        let expanded = sanitize_excerpt(
            &vec![b'k'; ERROR_EXCERPT_MAX_BYTES],
            &["k".to_owned()],
            true,
        );
        assert!(expanded.len() <= ERROR_EXCERPT_MAX_BYTES);
        assert!(!expanded.contains('k'));
        assert!(
            ERROR_MESSAGE_PREFIX_BYTES + expanded.len()
                <= ERROR_MESSAGE_PREFIX_BYTES + ERROR_EXCERPT_MAX_BYTES
        );

        let credential = "sec\"ret\\token";
        let serialized = serde_json::to_string(credential).unwrap();
        let redacted = sanitize_excerpt(serialized.as_bytes(), &[credential.to_owned()], false);
        assert_eq!(redacted, "[REDACTED]");
        assert!(!redacted.contains(credential));
        assert!(!redacted.contains(&serialized));
    }

    #[cfg(feature = "custom-http")]
    #[test]
    fn default_status_taxonomy_is_stable() {
        assert_eq!(
            status_classification(reqwest::StatusCode::UNAUTHORIZED),
            (ProviderErrorKind::Auth, false)
        );
        assert_eq!(
            status_classification(reqwest::StatusCode::FORBIDDEN),
            (ProviderErrorKind::Auth, false)
        );
        assert_eq!(
            status_classification(reqwest::StatusCode::TOO_MANY_REQUESTS),
            (ProviderErrorKind::RateLimited, true)
        );
        assert_eq!(
            status_classification(reqwest::StatusCode::INTERNAL_SERVER_ERROR),
            (ProviderErrorKind::Server, true)
        );
        assert_eq!(
            status_classification(reqwest::StatusCode::BAD_REQUEST),
            (ProviderErrorKind::Invalid, false)
        );
    }

    #[cfg(any(feature = "custom-http", feature = "codex", feature = "opencode-go"))]
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
        assert!(matches!(
            adapter.chat_token_mode,
            crate::chat::TokenMode::MaxTokens
        ));

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
    #[tokio::test]
    async fn custom_dynamic_authorization_is_sensitive_before_encoding() {
        struct Source;
        #[async_trait]
        impl CredentialSource for Source {
            async fn credential(&self, _request: &ModelRequest) -> Result<String, ProviderError> {
                Ok("raw-secret".into())
            }
            async fn invalidate(&self, _stale: &str) {}
        }

        let headers = HttpAdapter::custom("custom", "https://example.test", Protocol::Responses)
            .with_credential_source(Arc::new(Source))
            .headers(&request("custom"))
            .await
            .unwrap();
        let authorization = headers.get("authorization").unwrap();
        assert_eq!(authorization, "Bearer raw-secret");
        assert!(authorization.is_sensitive());
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
    #[cfg(feature = "codex")]
    #[tokio::test]
    async fn real_codex_401_refreshes_once_against_loopback() {
        use crabber_auth::{
            CredentialStore, MemoryCredentialStore, OAuthCredentials, TokenManager, now_ms,
        };
        use std::sync::Mutex;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let server_seen = seen.clone();
        let server = tokio::spawn(async move {
            for _ in 0..3 {
                let (mut socket, headers) = accept_loopback_request(&listener).await;
                let first = headers.lines().next().unwrap().to_owned();
                let authorization = headers
                    .lines()
                    .find(|line| line.to_ascii_lowercase().starts_with("authorization:"))
                    .unwrap_or_default()
                    .to_owned();
                server_seen
                    .lock()
                    .unwrap()
                    .push((first.clone(), authorization));
                let (status, content_type, body) = if first.contains("/token") {
                    (
                        "200 OK",
                        "application/json",
                        r#"{"access_token":"fresh","refresh_token":"rotated","expires_in":3600}"#,
                    )
                } else if server_seen
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(line, _)| line.contains("/responses"))
                    .count()
                    == 1
                {
                    ("401 Unauthorized", "text/plain", "no")
                } else {
                    ("500 Server Error", "text/plain", "stale fresh codex-error")
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                write_loopback_response(&mut socket, response.as_bytes()).await;
            }
        });

        let store: Arc<dyn CredentialStore> = Arc::new(MemoryCredentialStore::default());
        store
            .save(&OAuthCredentials {
                client_id: "client".into(),
                host_id: "host".into(),
                account_id: "account".into(),
                access_token: "stale".into(),
                refresh_token: "refresh".into(),
                id_token: "id".into(),
                scopes: vec!["chatgpt.tokens.use.direct".into()],
                expires_unix_ms: now_ms() + 3_600_000,
            })
            .unwrap();
        let mut adapter = HttpAdapter::codex(store.clone()).with_base_url(&base_url);
        adapter.tokens = Some(Arc::new(
            TokenManager::new(store).with_token_url(format!("{base_url}/token")),
        ));
        let error = adapter
            .stream(request("codex"))
            .await
            .err()
            .expect("post-refresh 500 should fail");
        assert_eq!(error.kind, ProviderErrorKind::Server);
        assert!(!error.message.contains("stale"));
        assert!(!error.message.contains("fresh"));
        assert!(error.message.ends_with("[REDACTED] [REDACTED] codex-error"));
        timeout(LOOPBACK_TIMEOUT, server)
            .await
            .expect("loopback server did not finish")
            .unwrap();
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 3);
        assert!(seen[0].1.ends_with("Bearer stale"));
        assert!(seen[1].0.contains("/token"));
        assert!(seen[2].1.ends_with("Bearer fresh"));
    }
    #[cfg(feature = "opencode-go")]
    #[tokio::test]
    async fn real_opencode_models_use_endpoint_but_custom_same_id_does_not() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, request) = accept_loopback_request(&listener).await;
            assert!(request.starts_with("GET /models HTTP/1.1"));
            assert!(
                request
                    .to_ascii_lowercase()
                    .contains("authorization: bearer key")
            );
            let body = r#"{"data":[{"id":"real-model"}]}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            write_loopback_response(&mut socket, response.as_bytes()).await;
        });
        let real = HttpAdapter::opencode_go(Protocol::Responses)
            .with_api_key("key")
            .with_base_url(&base_url);
        assert_eq!(real.models().await.unwrap()[0].id, "real-model");
        timeout(LOOPBACK_TIMEOUT, server)
            .await
            .expect("loopback server did not finish")
            .unwrap();

        #[cfg(feature = "custom-http")]
        {
            let custom =
                HttpAdapter::custom("opencode-go", "http://127.0.0.1:1", Protocol::Responses)
                    .with_api_key("key");
            assert_eq!(
                custom.models().await.unwrap(),
                Vec::<ModelDescriptor>::new()
            );
            let custom_headers = custom.headers(&request("opencode-go")).await.unwrap();
            assert!(!custom_headers.contains_key("x-opencode-session"));
        }
    }
    #[cfg(feature = "opencode-go")]
    #[tokio::test]
    async fn opencode_models_error_redacts_bearer_credential() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, request) = accept_loopback_request(&listener).await;
            assert!(request.starts_with("GET /models HTTP/1.1"));
            let body = "model-secret";
            let response = format!(
                "HTTP/1.1 500 Server Error\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            write_loopback_response(&mut socket, response.as_bytes()).await;
        });
        let error = HttpAdapter::opencode_go(Protocol::Responses)
            .with_api_key("model-secret")
            .with_base_url(&base_url)
            .models()
            .await
            .expect_err("model endpoint 500 should fail");
        assert_eq!(error.kind, ProviderErrorKind::Server);
        assert!(!error.message.contains("model-secret"));
        assert!(error.message.ends_with("[REDACTED]"));
        timeout(LOOPBACK_TIMEOUT, server)
            .await
            .expect("loopback server did not finish")
            .unwrap();
    }
    #[cfg(all(feature = "custom-http", feature = "opencode-go"))]
    #[tokio::test]
    async fn real_opencode_session_header_is_wire_privileged() {
        use std::sync::Mutex;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let server_seen = seen.clone();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut socket, request) = accept_loopback_request(&listener).await;
                server_seen.lock().unwrap().push(request);
                let body = "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n";
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                write_loopback_response(&mut socket, response.as_bytes()).await;
            }
        });

        let real = HttpAdapter::opencode_go(Protocol::Responses)
            .with_api_key("real-key")
            .with_base_url(&base_url);
        let _real_events: Vec<_> = real
            .stream(request("opencode-go"))
            .await
            .unwrap()
            .collect()
            .await;

        let custom = HttpAdapter::custom("opencode-go", &base_url, Protocol::Responses)
            .with_api_key("custom-key");
        let _custom_events: Vec<_> = custom
            .stream(request("opencode-go"))
            .await
            .unwrap()
            .collect()
            .await;

        timeout(LOOPBACK_TIMEOUT, server)
            .await
            .expect("loopback server did not finish")
            .unwrap();
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert!(
            seen[0]
                .to_ascii_lowercase()
                .contains("x-opencode-session: session-1")
        );
        assert!(!seen[1].to_ascii_lowercase().contains("x-opencode-session:"));
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
