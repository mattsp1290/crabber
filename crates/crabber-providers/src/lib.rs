//! Provider contracts and deterministic or HTTP-backed providers.
//!
//! Enable `custom-http` independently of the built-in provider features to use
//! `HttpAdapter::custom` and register it with `HttpResolver::with_adapter`.
//! The host supplies credentials, per-attempt headers, observation,
//! classification, transport policy, model discovery, and any gateway token
//! minting. See the crate README's custom HTTP section for security and retry
//! guidance. Public header, status, certificate, identity, and TLS
//! types come from `reqwest` 0.12.

use async_trait::async_trait;
use crabber_core::{Message, RunId, SessionId, ToolCallId, ToolInfo, TurnId, Usage};
use futures::{Stream, stream};
use serde_json::Value;
use std::{
    collections::VecDeque,
    pin::Pin,
    sync::{Arc, Mutex},
};

#[cfg(any(
    feature = "custom-http",
    feature = "anthropic",
    feature = "openai",
    feature = "codex",
    feature = "opencode-go"
))]
mod chat;
#[cfg(any(
    feature = "custom-http",
    feature = "anthropic",
    feature = "openai",
    feature = "codex",
    feature = "opencode-go"
))]
mod messages;
#[cfg(any(
    feature = "custom-http",
    feature = "anthropic",
    feature = "openai",
    feature = "codex",
    feature = "opencode-go"
))]
mod real;
#[cfg(any(
    feature = "custom-http",
    feature = "anthropic",
    feature = "openai",
    feature = "codex",
    feature = "opencode-go"
))]
mod responses;
pub mod sse;
#[cfg(feature = "custom-http")]
pub use real::{
    AuthScheme, ChatTokenField, CredentialSource, CustomHttpAdapter, ErrorClassifier,
    HttpClientConfig, HttpProxyConfig, RequestHeaderHook, ResponseObserver,
};
#[cfg(any(
    feature = "custom-http",
    feature = "anthropic",
    feature = "openai",
    feature = "codex",
    feature = "opencode-go"
))]
pub use real::{HttpAdapter, HttpResolver, Protocol};
#[cfg(all(test, feature = "all-providers"))]
mod codec_tests;

/// Retained for the workspace scaffold's smoke test.
pub const CRATE_NAME: &str = "crabber-providers";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Selection {
    pub provider_id: String,
    pub model_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProviderInfo {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelDescriptor {
    pub provider_id: String,
    pub id: String,
    pub context_limit: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestIdentity {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub turn_id: TurnId,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ModelRequest {
    pub identity: RequestIdentity,
    pub selection: Selection,
    pub system: Option<String>,
    pub messages: Vec<Message>,
    pub tools: Vec<ToolInfo>,
    pub temperature: Option<f64>,
    pub max_tokens: Option<u32>,
    pub tool_choice: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProviderErrorKind {
    Auth,
    RateLimited,
    ContextOverflow,
    Transport,
    Invalid,
    Server,
    Canceled,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("provider {kind:?}: {message}")]
pub struct ProviderError {
    pub kind: ProviderErrorKind,
    pub message: String,
    pub retryable: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StreamDelta {
    TextDelta(String),
    ReasoningDelta(String),
    ToolCallStart { call_id: ToolCallId, name: String },
    ToolCallArgsDelta { call_id: ToolCallId, text: String },
    ToolCallDone { call_id: ToolCallId },
    ProviderState { codec_id: String, payload: Value },
    Usage(Usage),
    Completed,
    Error(ProviderError),
}

pub type DeltaStream = Pin<Box<dyn Stream<Item = StreamDelta> + Send>>;

#[async_trait]
pub trait Streamer: Send + Sync {
    async fn stream(&self, request: ModelRequest) -> Result<DeltaStream, ProviderError>;
}

#[async_trait]
pub trait ProviderAdapter: Send + Sync {
    fn info(&self) -> ProviderInfo;
    async fn models(&self) -> Result<Vec<ModelDescriptor>, ProviderError>;
    async fn build(&self, selection: &Selection) -> Result<Arc<dyn Streamer>, ProviderError>;
}

#[async_trait]
pub trait Resolver: Send + Sync {
    async fn resolve(&self, selection: &Selection) -> Result<Arc<dyn Streamer>, ProviderError>;
}

#[derive(Clone, Default)]
pub struct FakeProvider {
    scripts: Arc<Mutex<VecDeque<Vec<StreamDelta>>>>,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
}

impl FakeProvider {
    #[must_use]
    pub fn scripted(scripts: Vec<Vec<StreamDelta>>) -> Self {
        Self {
            scripts: Arc::new(Mutex::new(scripts.into())),
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    #[must_use]
    pub fn requests(&self) -> Vec<ModelRequest> {
        self.requests
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

#[async_trait]
impl Streamer for FakeProvider {
    async fn stream(&self, request: ModelRequest) -> Result<DeltaStream, ProviderError> {
        self.requests
            .lock()
            .expect("fake provider poisoned")
            .push(request);
        let script = self
            .scripts
            .lock()
            .expect("fake provider poisoned")
            .pop_front()
            .ok_or_else(|| ProviderError {
                kind: ProviderErrorKind::Invalid,
                message: "script exhausted".into(),
                retryable: false,
            })?;
        Ok(Box::pin(stream::iter(script)))
    }
}

#[async_trait]
impl Resolver for FakeProvider {
    async fn resolve(&self, selection: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        if selection.provider_id != "fake" {
            return Err(ProviderError {
                kind: ProviderErrorKind::Invalid,
                message: "unknown provider".into(),
                retryable: false,
            });
        }
        Ok(Arc::new(self.clone()))
    }
}

#[async_trait]
impl ProviderAdapter for FakeProvider {
    fn info(&self) -> ProviderInfo {
        ProviderInfo {
            id: "fake".into(),
            name: "Scripted fake".into(),
        }
    }

    async fn models(&self) -> Result<Vec<ModelDescriptor>, ProviderError> {
        Ok(vec![ModelDescriptor {
            provider_id: "fake".into(),
            id: "scripted".into(),
            context_limit: 8192,
        }])
    }

    async fn build(&self, selection: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        self.resolve(selection).await
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn crate_name_is_stable() {
        assert_eq!(crate::CRATE_NAME, "crabber-providers");
    }
}
