//! Final attribute boundary for spans owned by Crabber.
use crate::layers::LayerHealth;
use opentelemetry::Context;
use opentelemetry_sdk::{
    Resource,
    error::OTelSdkResult,
    trace::{Span, SpanData, SpanProcessor},
};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

pub const ALLOWED_ATTRIBUTES: &[&str] = &[
    "admission.limit_kind",
    "admission.limit_bytes",
    "session.id",
    "run.id",
    "attempt.id",
    "outcome",
    "reason",
    "error.kind",
    "host.trace_id",
    "host.span_id",
    "host.predecessor_trace_id",
    "keyed",
    "provider.id",
    "model.id",
    "purpose",
    "retry.attempt",
    "tokens.input",
    "tokens.output",
    "tool.name",
    "tool.call_id",
    "permission",
    "calls",
    "db.system",
    "crabber.store.scope",
    "schema.version",
    "protocol",
    "http.response.status_code",
    "extension.id",
    "scope",
    "leases",
    "pending_tasks",
    "handler.count",
    "otel.kind",
    "otel.status_code",
    "otel.name",
];

#[derive(Debug)]
pub(super) struct ScrubbingProcessor<P> {
    inner: P,
    health: Arc<LayerHealth>,
}
impl<P> ScrubbingProcessor<P> {
    pub(super) fn new(inner: P, health: Arc<LayerHealth>) -> Self {
        Self { inner, health }
    }
}
impl<P: SpanProcessor> SpanProcessor for ScrubbingProcessor<P> {
    fn on_start(&self, span: &mut Span, context: &Context) {
        self.inner.on_start(span, context);
    }
    fn on_end(&self, mut span: SpanData) {
        if span.name.starts_with("crabber.") {
            span.attributes.retain(|attribute| {
                let allowed = ALLOWED_ATTRIBUTES.contains(&attribute.key.as_str());
                if !allowed {
                    self.health
                        .attributes_scrubbed
                        .fetch_add(1, Ordering::Relaxed);
                }
                allowed
            });
            // tracing-opentelemetry can turn arbitrary events into exception payloads.
            // Library diagnostics use the separately redacted logs path, so Crabber
            // spans export no events or links carrying unknown attributes.
            span.events.events.clear();
            if matches!(span.status, opentelemetry::trace::Status::Error { .. }) {
                span.status = opentelemetry::trace::Status::error("");
            }
            for link in &mut span.links.links {
                link.attributes
                    .retain(|attribute| ALLOWED_ATTRIBUTES.contains(&attribute.key.as_str()));
            }
        }
        self.inner.on_end(span);
    }
    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }
    fn shutdown(&self) -> OTelSdkResult {
        self.inner.shutdown()
    }
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }
    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}
