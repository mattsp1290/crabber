//! Redacted host-owned telemetry configuration.
use std::{env::VarError, fmt, time::Duration};
use tracing::Level;

use crate::{DatadogConfig, ExportError};

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

use crate::layers::LogsPolicy;

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
            service: get("DD_SERVICE")
                .or_else(|_| get("OTEL_SERVICE_NAME"))
                .unwrap_or_else(|_| "crabber".into()),
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
