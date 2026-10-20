//! Host-owned tracing configuration. Construction never installs a subscriber.
use std::{env::VarError, fmt, time::Duration};
use tracing::Level;
use tracing_subscriber::{Layer, layer::Identity};

use crate::{DatadogConfig, DatadogObserver, ExportError};

/// Signals enabled by a host, independently of environment configuration.
#[derive(Debug, Clone, Copy)]
pub struct Signals {
    pub traces: bool,
    pub metrics: bool,
    pub logs: bool,
}
impl Default for Signals {
    fn default() -> Self {
        Self {
            traces: true,
            metrics: true,
            logs: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LogsPolicy {
    pub min_level: Level,
    pub guest_text: bool,
}
impl Default for LogsPolicy {
    fn default() -> Self {
        Self {
            min_level: Level::INFO,
            guest_text: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtlpProtocol {
    HttpProtobuf,
    HttpJson,
    Grpc,
}

/// Plain HTTP OTLP settings; header values and endpoints are hidden in Debug.
#[derive(Clone)]
pub struct OtlpConfig {
    pub traces_url: String,
    pub protocol: OtlpProtocol,
    pub timeout: Duration,
    pub headers: Vec<(String, String)>,
}
impl fmt::Debug for OtlpConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OtlpConfig")
            .field("traces_url", &"[REDACTED]")
            .field("protocol", &self.protocol)
            .field("timeout", &self.timeout)
            .field("headers", &"[REDACTED]")
            .finish()
    }
}
impl OtlpConfig {
    /// Reads standard OTLP HTTP environment settings.
    /// # Errors
    /// Rejects unsupported protocols and malformed or non-HTTP endpoints.
    pub fn from_lookup(
        get: &dyn Fn(&str) -> Result<String, VarError>,
    ) -> Result<Option<Self>, TelemetryError> {
        let nonempty = |name| get(name).ok().filter(|v| !v.is_empty());
        let Some(traces_url) = nonempty("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT").or_else(|| {
            nonempty("OTEL_EXPORTER_OTLP_ENDPOINT")
                .map(|v| format!("{}/v1/traces", v.trim_end_matches('/')))
        }) else {
            return Ok(None);
        };
        let endpoint =
            reqwest::Url::parse(&traces_url).map_err(|_| TelemetryError::InvalidEndpoint)?;
        if endpoint.scheme() != "http"
            || endpoint.host_str().is_none()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
        {
            return Err(TelemetryError::InvalidEndpoint);
        }
        let protocol = match get("OTEL_EXPORTER_OTLP_PROTOCOL")
            .as_deref()
            .unwrap_or("http/protobuf")
        {
            "http/protobuf" => OtlpProtocol::HttpProtobuf,
            "http/json" => OtlpProtocol::HttpJson,
            _ => return Err(TelemetryError::UnsupportedProtocol),
        };
        let timeout = get("OTEL_EXPORTER_OTLP_TIMEOUT")
            .ok()
            .map(|v| v.parse::<u64>())
            .transpose()
            .map_err(|_| TelemetryError::Otlp)?
            .unwrap_or(10_000);
        let headers = get("OTEL_EXPORTER_OTLP_HEADERS")
            .ok()
            .map(|v| {
                v.split(',')
                    .filter(|p| !p.trim().is_empty())
                    .map(|p| {
                        p.split_once('=')
                            .map(|(k, v)| (k.trim().to_owned(), v.trim().to_owned()))
                            .ok_or(TelemetryError::Otlp)
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?
            .unwrap_or_default();
        Ok(Some(Self {
            traces_url,
            protocol,
            timeout: Duration::from_millis(timeout),
            headers,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct TelemetryConfig {
    pub datadog: Option<DatadogConfig>,
    pub otlp: Option<OtlpConfig>,
    pub service: String,
    pub env: Option<String>,
    pub version: Option<String>,
    pub resource_tags: Vec<(String, String)>,
    pub logs: LogsPolicy,
    pub traces_level: Level,
    pub signals: Signals,
}
impl TelemetryConfig {
    /// # Errors
    /// Rejects malformed OTLP configuration without exposing its values.
    pub fn from_env() -> Result<Self, TelemetryError> {
        Self::from_lookup(&|name| std::env::var(name))
    }
    /// # Errors
    /// Rejects malformed OTLP configuration without exposing its values.
    pub fn from_lookup(
        get: &dyn Fn(&str) -> Result<String, VarError>,
    ) -> Result<Self, TelemetryError> {
        Ok(Self {
            datadog: DatadogConfig::from_lookup(get),
            otlp: OtlpConfig::from_lookup(get)?,
            service: get("DD_SERVICE").unwrap_or_else(|_| "crabber".into()),
            env: get("DD_ENV").ok(),
            version: get("DD_VERSION").ok(),
            resource_tags: Vec::new(),
            logs: LogsPolicy::default(),
            traces_level: Level::INFO,
            signals: Signals::default(),
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TelemetryError {
    #[error("OTLP operation failed")]
    Otlp,
    #[error(transparent)]
    Export(#[from] ExportError),
    #[error("unsupported OTLP protocol")]
    UnsupportedProtocol,
    #[error("invalid OTLP endpoint")]
    InvalidEndpoint,
    #[error("telemetry requires a Tokio runtime")]
    Runtime,
}

/// Host-local counts for tracing layers; exporter queue health is separate.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TelemetryHealth {
    pub spans_seen: u64,
    pub events_seen: u64,
    pub metrics_enqueued: u64,
    pub metrics_dropped: u64,
    pub logs_enqueued: u64,
    pub logs_dropped: u64,
    pub messages_unlisted: u64,
    pub attributes_scrubbed: u64,
}

/// Host-owned telemetry bundle. Construct inside a Tokio runtime, including
/// hosts with a synchronous `main`; this never installs a global subscriber.
pub struct Telemetry;
impl Telemetry {
    /// # Errors
    /// Returns configuration errors or `Runtime` outside a Tokio runtime.
    pub fn from_env() -> Result<Self, TelemetryError> {
        Self::new(TelemetryConfig::from_env()?)
    }
    /// # Errors
    /// Returns `Runtime` outside a Tokio runtime.
    pub fn new(_config: TelemetryConfig) -> Result<Self, TelemetryError> {
        tokio::runtime::Handle::try_current().map_err(|_| TelemetryError::Runtime)?;
        Ok(Self)
    }
    #[must_use]
    pub fn layer<S: tracing::Subscriber>(&self) -> impl Layer<S> {
        Identity::new()
    }
    #[must_use]
    pub fn exporter(&self) -> Option<DatadogObserver> {
        None
    }
    #[must_use]
    pub fn current_trace_context() -> Option<crabber_core::TraceContext> {
        None
    }
    #[must_use]
    pub fn health(&self) -> TelemetryHealth {
        TelemetryHealth::default()
    }
    /// # Errors
    /// Reserved for exporter failures when a signal pipeline is configured.
    pub async fn flush(&self) -> Result<(), TelemetryError> {
        std::future::ready(Ok(())).await
    }
    /// # Errors
    /// Reserved for exporter failures when a signal pipeline is configured.
    pub async fn shutdown(&self) -> Result<(), TelemetryError> {
        std::future::ready(Ok(())).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config(vars: &[(&str, &str)]) -> Result<TelemetryConfig, TelemetryError> {
        TelemetryConfig::from_lookup(&|name| {
            vars.iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| (*value).to_owned())
                .ok_or(VarError::NotPresent)
        })
    }
    #[test]
    fn environment_defaults_and_endpoints() {
        let empty = config(&[]).unwrap();
        assert!(empty.datadog.is_none() && empty.otlp.is_none());
        let base = config(&[("OTEL_EXPORTER_OTLP_ENDPOINT", "http://h:4318/")]).unwrap();
        assert_eq!(base.otlp.unwrap().traces_url, "http://h:4318/v1/traces");
        let explicit = config(&[
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://base"),
            ("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://trace/custom"),
        ])
        .unwrap();
        assert_eq!(explicit.otlp.unwrap().traces_url, "http://trace/custom");
        for value in ["https://h", "ftp://h", "not an endpoint"] {
            assert!(matches!(
                config(&[("OTEL_EXPORTER_OTLP_ENDPOINT", value)]),
                Err(TelemetryError::InvalidEndpoint)
            ));
        }
        assert!(matches!(
            config(&[
                ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://h"),
                ("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
            ]),
            Err(TelemetryError::UnsupportedProtocol)
        ));
    }
    #[test]
    fn secrets_are_redacted() {
        let dd = config(&[("DD_API_KEY", "PRIVATE_KEY")]).unwrap();
        assert!(dd.datadog.is_some() && dd.otlp.is_none());
        let full = config(&[
            ("DD_API_KEY", "PRIVATE_KEY"),
            ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://h"),
            ("OTEL_EXPORTER_OTLP_HEADERS", "authorization=PRIVATE_HEADER"),
        ])
        .unwrap();
        let debug = format!("{full:?}");
        assert!(!debug.contains("PRIVATE_KEY") && !debug.contains("PRIVATE_HEADER"));
    }
    #[tokio::test]
    async fn empty_telemetry_is_inert() {
        use tracing_subscriber::prelude::*;
        let mut config = config(&[]).unwrap();
        config.signals.metrics = false;
        let telemetry = Telemetry::new(config).unwrap();
        let subscriber = tracing_subscriber::registry().with(telemetry.layer());
        tracing::subscriber::with_default(subscriber, || {
            tracing::info!("host event");
        });
        assert!(telemetry.exporter().is_none());
        assert_eq!(telemetry.health(), TelemetryHealth::default());
        telemetry.flush().await.unwrap();
        telemetry.shutdown().await.unwrap();
        telemetry.shutdown().await.unwrap();
    }
    #[test]
    fn construction_requires_runtime() {
        assert!(matches!(
            Telemetry::new(config(&[]).unwrap()),
            Err(TelemetryError::Runtime)
        ));
    }
}
