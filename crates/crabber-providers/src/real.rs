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
#[derive(Clone)]
pub struct HttpAdapter {
    id: &'static str,
    base_url: String,
    protocol: Protocol,
    key_env: Option<&'static str>,
    key_override: Option<String>,
    client: Client,
    #[cfg(feature = "codex")]
    tokens: Option<Arc<crabber_auth::TokenManager>>,
}
impl HttpAdapter {
    fn new(
        id: &'static str,
        base_url: &str,
        protocol: Protocol,
        key_env: Option<&'static str>,
    ) -> Self {
        Self {
            id,
            base_url: base_url.into(),
            protocol,
            key_env,
            key_override: None,
            client: Client::new(),
            #[cfg(feature = "codex")]
            tokens: None,
        }
    }
    #[cfg(feature = "anthropic")]
    #[must_use]
    pub fn anthropic() -> Self {
        Self::new(
            "anthropic",
            "https://api.anthropic.com/v1",
            Protocol::Messages,
            Some("ANTHROPIC_API_KEY"),
        )
    }
    #[cfg(feature = "openai")]
    #[must_use]
    pub fn openai() -> Self {
        Self::new(
            "openai",
            "https://api.openai.com/v1",
            Protocol::Responses,
            Some("OPENAI_API_KEY"),
        )
    }
    #[cfg(feature = "opencode-go")]
    #[must_use]
    pub fn opencode_go(protocol: Protocol) -> Self {
        Self::new(
            "opencode-go",
            "https://opencode.ai/zen/go/v1",
            protocol,
            Some("OPENCODE_GO_API_KEY"),
        )
    }
    #[cfg(feature = "codex")]
    pub fn codex(store: Arc<dyn crabber_auth::CredentialStore>) -> Self {
        let mut adapter = Self::new(
            "codex",
            "https://api.openai.com/v1",
            Protocol::Responses,
            None,
        );
        adapter.tokens = Some(Arc::new(crabber_auth::TokenManager::new(store)));
        adapter
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
        self
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
    async fn headers(&self, request: &ModelRequest) -> Result<HeaderMap, ProviderError> {
        let mut headers = HeaderMap::new();
        headers.insert("user-agent", HeaderValue::from_static("crabber/0.1"));
        if self.id == "codex" {
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
            if self.protocol == Protocol::Messages {
                insert(&mut headers, "x-api-key", &key)?;
            } else {
                insert(&mut headers, "authorization", &format!("Bearer {key}"))?;
            }
        }
        if self.protocol == Protocol::Messages {
            headers.insert("anthropic-version", HeaderValue::from_static("2023-06-01"));
        }
        if self.id == "opencode-go" {
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
            Protocol::Responses => crate::responses::body(&request, self.id == "codex"),
            Protocol::Messages => crate::messages::body(&request),
            Protocol::ChatCompletions => crate::chat::body(&request),
        };
        let url = format!(
            "{}{}",
            self.base_url.trim_end_matches('/'),
            self.protocol.path()
        );
        let mut response = self
            .client
            .post(&url)
            .headers(headers.clone())
            .json(&body)
            .send()
            .await
            .map_err(|_| transport())?;
        #[cfg(feature = "codex")]
        if self.id == "codex" && response.status() == reqwest::StatusCode::UNAUTHORIZED {
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
        if !response.status().is_success() {
            return Err(status_error(response.status()));
        }
        let codec = match self.protocol {
            Protocol::Responses => {
                Codec::Responses(crate::responses::Codec::new(self.id == "codex"))
            }
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
            id: self.id.into(),
            name: self.id.into(),
        }
    }
    async fn models(&self) -> Result<Vec<ModelDescriptor>, ProviderError> {
        if self.id != "opencode-go" {
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
                provider_id: self.id.into(),
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
        self.adapters.insert(adapter.id.into(), adapter);
        self
    }
    #[must_use]
    pub fn from_env() -> Self {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RequestIdentity;
    use crabber_core::{RunId, SessionId, TurnId};
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
