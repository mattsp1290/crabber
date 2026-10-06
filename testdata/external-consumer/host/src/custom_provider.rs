use async_trait::async_trait;
use crabber::providers::{
    AuthScheme, ChatTokenField, CredentialSource, ErrorClassifier, HttpAdapter, HttpClientConfig,
    HttpProxyConfig, HttpResolver, ModelRequest, Protocol, ProviderError, ProviderErrorKind,
    RequestHeaderHook, Resolver, ResponseObserver,
};
use reqwest::{
    StatusCode,
    header::{HeaderMap, HeaderValue},
};
use std::{sync::Arc, time::Duration};

struct SyntheticCredentialSource;

#[async_trait]
impl CredentialSource for SyntheticCredentialSource {
    async fn credential(&self, _request: &ModelRequest) -> Result<String, ProviderError> {
        Err(ProviderError {
            kind: ProviderErrorKind::Auth,
            message: "synthetic credential source is construction-only".into(),
            retryable: false,
        })
    }

    async fn invalidate(&self, _stale: &str) {}
}

struct SyntheticHeaderHook;

impl RequestHeaderHook for SyntheticHeaderHook {
    fn headers(&self, _request: &ModelRequest) -> Result<HeaderMap, ProviderError> {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-host-request",
            HeaderValue::from_static("construction-proof"),
        );
        Ok(headers)
    }
}

struct SyntheticResponseObserver;

impl ResponseObserver for SyntheticResponseObserver {
    fn observe(&self, _status: StatusCode, _headers: &HeaderMap) {}
}

struct SyntheticErrorClassifier;

impl ErrorClassifier for SyntheticErrorClassifier {
    fn classify(&self, status: StatusCode, _excerpt: &str) -> (ProviderErrorKind, bool) {
        if status == StatusCode::BAD_REQUEST {
            (ProviderErrorKind::ContextOverflow, false)
        } else {
            (ProviderErrorKind::Server, status.is_server_error())
        }
    }
}

fn accepts_public_resolver<R: Resolver>(_resolver: &R) {}

/// Exercises a public custom-HTTP construction and registration path without
/// resolving a model, fetching a credential, minting a token, opening a stream, or contacting
/// a network.
pub fn run() -> Result<(), ProviderError> {
    let mut static_headers = HeaderMap::new();
    static_headers.insert(
        "x-host-static",
        HeaderValue::from_static("external-consumer"),
    );
    let mut sensitive = HeaderValue::from_static("opaque-host-metadata");
    sensitive.set_sensitive(true);
    static_headers.insert("x-host-sensitive", sensitive);

    let client = HttpClientConfig::new()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(5))
        .read_timeout(Duration::from_secs(20))
        .pool_idle_timeout(Some(Duration::from_secs(60)))
        .pool_max_idle_per_host(4)
        .without_proxy();

    let _proxy_config =
        HttpProxyConfig::all("http://proxy.invalid")?.with_basic_auth("synthetic", "synthetic");
    let _alternate_auth_and_token_fields = HttpAdapter::custom(
        "external-custom-chat-alternate",
        "https://example.invalid/v1",
        Protocol::ChatCompletions,
    )
    .with_auth_scheme(AuthScheme::XApiKey)
    .with_chat_token_field(ChatTokenField::MaxTokens);

    let adapter = HttpAdapter::custom(
        "external-custom-chat",
        "https://example.invalid/v1",
        Protocol::ChatCompletions,
    )
    .try_with_user_agent_product("external-consumer/0.1")?
    .with_credential_source(Arc::new(SyntheticCredentialSource))
    .try_with_static_headers(static_headers)?
    .with_request_header_hook(Arc::new(SyntheticHeaderHook))
    .try_with_client_config(client)?
    .with_auth_scheme(AuthScheme::Bearer)
    .with_response_observer(Arc::new(SyntheticResponseObserver))
    .with_error_classifier(Arc::new(SyntheticErrorClassifier))
    .with_chat_token_field(ChatTokenField::MaxCompletionTokens);

    let resolver = HttpResolver::new().with_adapter(adapter);
    accepts_public_resolver(&resolver);
    Ok(())
}
