use super::invalid;
use crate::ProviderError;
use reqwest::Client;
use std::{any::Any, time::Duration};

/// An all-protocol proxy accepted by [`HttpClientConfig`].
///
/// Raw `reqwest::Proxy` conversion is intentionally unavailable.
pub struct HttpProxyConfig {
    proxy: reqwest::Proxy,
}

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

/// Finite transport configuration for a custom HTTP adapter.
///
/// The allowlist comprises total/connect/read timeouts, idle-pool controls,
/// [`HttpProxyConfig`] or proxy disabling, root certificates, client identity,
/// TLS version bounds, and preconfigured TLS state. Redirects are always
/// disabled. Raw clients/builders/proxies, default headers, redirect policy,
/// and generic builder callbacks are intentionally unavailable. Public HTTP
/// and TLS types are from reqwest 0.12.
pub struct HttpClientConfig {
    builder: reqwest::ClientBuilder,
}

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

    pub(super) fn build(self) -> Result<Client, ProviderError> {
        self.builder.build().map_err(|_| invalid())
    }
}

impl Default for HttpClientConfig {
    fn default() -> Self {
        Self::new()
    }
}
