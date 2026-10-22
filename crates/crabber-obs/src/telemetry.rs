//! Host-owned exporters and tracing layers. Never installs a subscriber.
mod config;
mod export;
mod scrub;

pub use crate::layers::{LayerHealthSnapshot as TelemetryHealth, LogsPolicy};
pub use config::{OtlpConfig, OtlpProtocol, Signals, TelemetryConfig, TelemetryError};
pub use scrub::ALLOWED_ATTRIBUTES;

use crate::{
    DatadogObserver, ExportError,
    layers::{LayerHealth, LogsLayer, SpanMetricsLayer},
};
use opentelemetry::{
    KeyValue,
    trace::{TraceContextExt as _, TracerProvider as _},
};
use opentelemetry_otlp::{WithExportConfig as _, WithHttpConfig as _};
use opentelemetry_sdk::{
    Resource,
    trace::{BatchSpanProcessor, SdkTracerProvider},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tracing::Subscriber;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;
use tracing_subscriber::{
    Layer,
    filter::{LevelFilter, Targets},
    registry::LookupSpan,
};

/// Construct inside Tokio. Install `layer()` globally in multithreaded hosts,
/// share `exporter()` with agents, and call `shutdown()` before runtime exit.
pub struct Telemetry {
    exporter: Option<DatadogObserver>,
    provider: Option<SdkTracerProvider>,
    health: Arc<LayerHealth>,
    config: TelemetryConfig,
    stopped: AtomicBool,
    otlp_failed: Arc<AtomicBool>,
    control: tokio::sync::Mutex<Option<TerminalFailure>>,
}
#[derive(Clone, Copy)]
enum TerminalFailure {
    Otlp,
    Intake,
}
impl Telemetry {
    /// # Errors
    /// Returns a finite configuration, runtime or exporter error.
    pub fn from_env() -> Result<Self, TelemetryError> {
        Self::new(TelemetryConfig::from_env()?)
    }

    /// Never installs a subscriber. OTLP transport uses a background thread.
    /// # Errors
    /// Returns finite errors without endpoints, headers or credential values.
    pub fn new(config: TelemetryConfig) -> Result<Self, TelemetryError> {
        tokio::runtime::Handle::try_current().map_err(|_| TelemetryError::Runtime)?;
        let health = Arc::new(LayerHealth::default());
        let otlp_failed = Arc::new(AtomicBool::new(false));
        let provider = if config.signals.traces {
            config
                .otlp
                .as_ref()
                .map(|otlp| build_provider(&config, otlp, health.clone(), otlp_failed.clone()))
                .transpose()?
        } else {
            None
        };
        let exporter = config.datadog.as_ref().map(DatadogObserver::new);
        Ok(Self {
            exporter,
            provider,
            health,
            config,
            stopped: AtomicBool::new(false),
            otlp_failed,
            control: tokio::sync::Mutex::new(None),
        })
    }

    /// Each pipeline uses its own filter; host formatting must also use
    /// per-layer filtering. Host spans of any target participate in APM traces.
    #[must_use]
    pub fn layer<S>(&self) -> impl Layer<S> + Send + Sync + 'static
    where
        S: Subscriber + for<'a> LookupSpan<'a> + Send + Sync + 'static,
    {
        let mut layers: Vec<Box<dyn Layer<S> + Send + Sync>> = Vec::new();
        if let Some(provider) = &self.provider {
            let filter = Targets::new()
                .with_default(self.config.traces_level)
                .with_target("crabber::wasm::guest", LevelFilter::OFF);
            layers.push(
                tracing_opentelemetry::layer()
                    .with_tracer(provider.tracer("crabber"))
                    .with_location(false)
                    .with_threads(false)
                    .with_level(false)
                    .with_target(false)
                    .with_tracked_inactivity(false)
                    .with_filter(filter)
                    .boxed(),
            );
        }
        if let Some(observer) = &self.exporter {
            if self.config.signals.metrics {
                layers.push(
                    SpanMetricsLayer::new(observer.clone(), self.health.clone())
                        .with_filter(SpanMetricsLayer::filter())
                        .boxed(),
                );
            }
            if self.config.signals.logs {
                layers.push(
                    LogsLayer::new(
                        observer.clone(),
                        self.config.logs.clone(),
                        self.health.clone(),
                    )
                    .with_filter(LogsLayer::filter(&self.config.logs))
                    .boxed(),
                );
            }
        }
        // An empty Vec<Layer> reports Interest::never(), which would globally
        // suppress a host's other layers. Option::None is a neutral layer.
        (!layers.is_empty()).then_some(layers)
    }

    #[must_use]
    pub fn exporter(&self) -> Option<DatadogObserver> {
        self.exporter.clone()
    }
    /// Call inside the host request span, then supply to `prompt_with_context`.
    #[must_use]
    pub fn current_trace_context() -> Option<crabber_core::TraceContext> {
        let context = tracing::Span::current().context();
        let span = context.span();
        let context = span.span_context();
        if !context.is_valid() {
            return None;
        }
        crabber_core::TraceContext::new(
            &context.trace_id().to_string(),
            &context.span_id().to_string(),
        )
        .ok()
    }
    #[must_use]
    pub fn health(&self) -> TelemetryHealth {
        self.health.snapshot()
    }

    fn otlp_result(&self) -> Result<(), TelemetryError> {
        if self.otlp_failed.load(Ordering::Acquire) {
            Err(TelemetryError::Otlp)
        } else {
            Ok(())
        }
    }

    fn terminal_result(&self, failure: Option<TerminalFailure>) -> Result<(), TelemetryError> {
        self.otlp_result()?;
        match failure {
            Some(TerminalFailure::Otlp) => Err(TelemetryError::Otlp),
            Some(TerminalFailure::Intake) => Err(ExportError::WorkerStopped.into()),
            None => Ok(()),
        }
    }

    /// # Errors
    /// Reports an OTLP or agentless intake failure.
    pub async fn flush(&self) -> Result<(), TelemetryError> {
        let terminal = self.control.lock().await;
        if self.stopped.load(Ordering::Acquire) {
            return self.terminal_result(*terminal);
        }
        let otlp = if let Some(provider) = &self.provider {
            let provider = provider.clone();
            tokio::task::spawn_blocking(move || provider.force_flush())
                .await
                .map_err(|_| TelemetryError::Otlp)
                .and_then(|result| result.map_err(|_| TelemetryError::Otlp))
        } else {
            Ok(())
        };
        let intake = if let Some(exporter) = &self.exporter {
            exporter.flush().await.map_err(TelemetryError::from)
        } else {
            Ok(())
        };
        self.otlp_result()?;
        otlp.and(intake)
    }

    /// Idempotent, including when a shared agent observer was stopped first.
    /// # Errors
    /// Reports finite OTLP or agentless intake failure categories.
    pub async fn shutdown(&self) -> Result<(), TelemetryError> {
        let mut terminal = self.control.lock().await;
        if self.stopped.load(Ordering::Acquire) {
            return self.terminal_result(*terminal);
        }
        let timeout = self
            .config
            .otlp
            .as_ref()
            .map_or(Duration::from_secs(10), |o| o.timeout);
        let otlp = if let Some(provider) = &self.provider {
            let provider = provider.clone();
            tokio::task::spawn_blocking(move || provider.shutdown_with_timeout(timeout))
                .await
                .map_err(|_| TelemetryError::Otlp)
                .and_then(|result| result.map_err(|_| TelemetryError::Otlp))
        } else {
            Ok(())
        };
        let intake = if let Some(exporter) = &self.exporter {
            match exporter.shutdown().await {
                Ok(()) => Ok(()),
                Err(ExportError::WorkerStopped) if exporter.dropped() == 0 => Ok(()),
                Err(error) => Err(error.into()),
            }
        } else {
            Ok(())
        };
        let result = self.otlp_result().and(otlp).and(intake);
        *terminal = result.as_ref().err().map(|error| match error {
            TelemetryError::Otlp => TerminalFailure::Otlp,
            _ => TerminalFailure::Intake,
        });
        self.stopped.store(true, Ordering::Release);
        result
    }
}
impl Drop for Telemetry {
    fn drop(&mut self) {
        if self.stopped.load(Ordering::Acquire) {
            return;
        }
        if let Some(exporter) = &self.exporter {
            exporter.stop_now();
        }
        if let Some(provider) = self.provider.take() {
            let timeout = self
                .config
                .otlp
                .as_ref()
                .map_or(Duration::from_secs(10), |o| o.timeout);
            // Detach: Drop must work after a current-thread runtime stops driving.
            // Thread creation failure must also avoid dropping the provider here.
            let provider = Arc::new(provider);
            let cleanup = provider.clone();
            if std::thread::Builder::new()
                .name("crabber-otlp-shutdown".into())
                .spawn(move || {
                    let _ = cleanup.shutdown_with_timeout(timeout);
                })
                .is_err()
            {
                std::mem::forget(provider);
            }
        }
    }
}

fn build_provider(
    config: &TelemetryConfig,
    otlp: &OtlpConfig,
    health: Arc<LayerHealth>,
    failed: Arc<AtomicBool>,
) -> Result<SdkTracerProvider, TelemetryError> {
    // Validate programmatic configurations as strictly as environment input.
    let endpoint =
        reqwest::Url::parse(&otlp.traces_url).map_err(|_| TelemetryError::InvalidEndpoint)?;
    if endpoint.scheme() != "http"
        || endpoint.host_str().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
    {
        return Err(TelemetryError::InvalidEndpoint);
    }
    let protocol = match otlp.protocol {
        OtlpProtocol::HttpProtobuf => opentelemetry_otlp::Protocol::HttpBinary,
        OtlpProtocol::HttpJson => opentelemetry_otlp::Protocol::HttpJson,
        OtlpProtocol::Grpc => return Err(TelemetryError::UnsupportedProtocol),
    };
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(otlp.traces_url.clone())
        .with_protocol(protocol)
        .with_timeout(otlp.timeout)
        .with_headers(otlp.headers.iter().cloned().collect())
        .build()
        .map_err(|_| TelemetryError::Otlp)?;
    let mut attributes = vec![KeyValue::new("service.name", config.service.clone())];
    if let Some(env) = &config.env {
        attributes.push(KeyValue::new("deployment.environment.name", env.clone()));
        attributes.push(KeyValue::new("deployment.environment", env.clone()));
    }
    if let Some(version) = &config.version {
        attributes.push(KeyValue::new("service.version", version.clone()));
    }
    attributes.extend(
        config.resource_tags.iter().map(|(key, value)| {
            KeyValue::new(crate::sanitize_tag(key), crate::sanitize_tag(value))
        }),
    );
    let resource = Resource::builder_empty()
        .with_attributes(attributes)
        .build();
    Ok(SdkTracerProvider::builder()
        .with_resource(resource)
        .with_span_processor(scrub::ScrubbingProcessor::new(
            BatchSpanProcessor::builder(export::TrackedExporter::new(exporter, failed)).build(),
            health,
        ))
        .build())
}

#[cfg(test)]
mod config_tests;
#[cfg(test)]
mod tests;
