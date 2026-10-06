use async_trait::async_trait;
use crabber_providers::{
    AuthScheme, ChatTokenField, CredentialSource, ErrorClassifier, HttpAdapter, HttpClientConfig,
    HttpProxyConfig, ModelRequest, ProviderError, ProviderErrorKind, Protocol, RequestHeaderHook,
    ResponseObserver,
};
use reqwest::header::HeaderMap;
use std::{sync::Arc, time::Duration};

struct Hooks;

#[async_trait]
impl CredentialSource for Hooks {
    async fn credential(&self, _request: &ModelRequest) -> Result<String, ProviderError> {
        Ok("credential".into())
    }

    async fn invalidate(&self, _stale: &str) {}
}

impl RequestHeaderHook for Hooks {
    fn headers(&self, _request: &ModelRequest) -> Result<HeaderMap, ProviderError> {
        Ok(HeaderMap::new())
    }
}

impl ResponseObserver for Hooks {
    fn observe(&self, _status: reqwest::StatusCode, _headers: &HeaderMap) {}
}

impl ErrorClassifier for Hooks {
    fn classify(
        &self,
        _status: reqwest::StatusCode,
        _excerpt: &str,
    ) -> (ProviderErrorKind, bool) {
        (ProviderErrorKind::Invalid, false)
    }
}

fn finite_tls_surface(
    config: HttpClientConfig,
    certificate: reqwest::Certificate,
    identity: reqwest::Identity,
) -> HttpClientConfig {
    config
        .add_root_certificate(certificate)
        .with_identity(identity)
}

fn main() {
    let proxy = HttpProxyConfig::all("http://localhost:8080")
        .unwrap()
        .with_basic_auth("user", "secret");
    let config = HttpClientConfig::default()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(5))
        .read_timeout(Duration::from_secs(10))
        .pool_idle_timeout(Some(Duration::from_secs(90)))
        .pool_max_idle_per_host(4)
        .with_proxy(proxy)
        .without_proxy()
        .min_tls_version(reqwest::tls::Version::TLS_1_2)
        .max_tls_version(reqwest::tls::Version::TLS_1_3)
        .use_preconfigured_tls(());
    let hooks = Arc::new(Hooks);
    let _ = HttpAdapter::custom("custom", "https://example.com/v1", Protocol::Responses)
        .with_auth_scheme(AuthScheme::XApiKey)
        .with_credential_source(hooks.clone())
        .with_api_key("static")
        .with_credential_source(hooks.clone())
        .try_with_static_headers(HeaderMap::new())
        .unwrap()
        .with_request_header_hook(hooks.clone())
        .with_response_observer(hooks.clone())
        .with_error_classifier(hooks)
        .with_chat_token_field(ChatTokenField::MaxCompletionTokens)
        .try_with_client_config(config)
        .unwrap();
    let _ = HttpClientConfig::new;
    let _ = finite_tls_surface;
}
