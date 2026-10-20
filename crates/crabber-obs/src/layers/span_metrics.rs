use super::{
    Fields, LayerHealth, library_target,
    metric_specs::{describe, known_span},
    metric_values::{error_tag, tag},
};
use crate::{DatadogObserver, MetricDimensions};
use serde_json::{Value, json};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Instant,
};
use tracing::{
    Event, Subscriber,
    span::{Attributes, Id, Record},
};
use tracing_subscriber::{
    Layer,
    filter::{LevelFilter, Targets},
    layer::Context,
    registry::LookupSpan,
};

/// Span-derived I/O metrics; runtime run/model/tool timing stays observer-owned.
pub struct SpanMetricsLayer {
    observer: DatadogObserver,
    dimensions: MetricDimensions,
    health: Arc<LayerHealth>,
}
struct TimedFields {
    started: Instant,
    fields: Fields,
}
impl SpanMetricsLayer {
    #[must_use]
    pub fn new(observer: DatadogObserver, health: Arc<LayerHealth>) -> Self {
        Self {
            dimensions: observer.dimensions.clone(),
            observer,
            health,
        }
    }
    /// Apply this as a per-layer filter using `Layer::with_filter`.
    #[must_use]
    pub fn filter() -> Targets {
        Targets::new()
            .with_target("crabber::runtime", LevelFilter::INFO)
            .with_target("crabber::session", LevelFilter::DEBUG)
            .with_target("crabber::providers", LevelFilter::INFO)
            .with_target("crabber::extension", LevelFilter::INFO)
            .with_target("crabber::wasm", LevelFilter::INFO)
            .with_target("crabber::wasm::guest", LevelFilter::OFF)
    }
    fn account(&self, accepted: bool) {
        let counter = if accepted {
            &self.health.metrics_enqueued
        } else {
            &self.health.metrics_dropped
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
    fn count(&self, name: &str, value: u64, tags: &[String]) {
        self.account(
            self.observer
                .enqueue_series(vec![counter(name, value, tags)])
                .is_ok(),
        );
    }
}
impl<S> Layer<S> for SpanMetricsLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        if !metric_enabled(attrs.metadata()) || !known_span(attrs.metadata().name()) {
            return;
        }
        if let Some(span) = ctx.span(id) {
            let mut fields = Fields::default();
            attrs.record(&mut fields);
            span.extensions_mut().insert(TimedFields {
                started: Instant::now(),
                fields,
            });
            self.health.spans_seen.fetch_add(1, Ordering::Relaxed);
        }
    }
    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id)
            && let Some(fields) = span.extensions_mut().get_mut::<TimedFields>()
        {
            values.record(&mut fields.fields);
        }
    }
    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else {
            return;
        };
        let Some(timed) = span.extensions_mut().remove::<TimedFields>() else {
            return;
        };
        let metric = describe(span.name(), &timed.fields, &self.dimensions);
        if let Some(name) = metric.distribution {
            let sample = json!({"metric":name,"points":[[crate::time_now(),[timed.started.elapsed().as_secs_f64()*1000.0]]],"tags":metric.tags});
            self.account(self.observer.enqueue_measurement(vec![sample]).is_ok());
        }
        if let Some(name) = metric.counter {
            self.count(name, 1, &metric.tags);
        }
    }
    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        if !metric_enabled(event.metadata()) {
            return;
        }
        let mut fields = Fields::default();
        event.record(&mut fields);
        let target = event.metadata().target();
        let message = fields.message.as_deref();
        if target == "crabber::runtime::heartbeat" {
            if fields.get("error.kind").as_deref() == Some("lease_lost")
                && fields.get("source").as_deref() == Some("heartbeat")
            {
                self.count("crabber.lease.lost", 1, &[]);
            } else if message == Some("lease renew failed") {
                self.count("crabber.lease.renew_failures", 1, &[error_tag(&fields)]);
            }
        } else if target == "crabber::runtime" {
            if let Some(lagged) = fields.get("lagged").and_then(|v| v.parse::<u64>().ok()) {
                self.count("crabber.events.lagged", lagged, &[]);
            }
            if message == Some("model request failed, retry scheduled") {
                self.count(
                    "crabber.model.retries",
                    1,
                    &[
                        error_tag(&fields),
                        tag(
                            &fields,
                            "compaction_requested",
                            "compaction_requested",
                            &["true", "false"],
                        ),
                    ],
                );
            }
        }
    }
}
fn metric_enabled(metadata: &tracing::Metadata<'_>) -> bool {
    let max_level = if metadata.target() == "crabber::session"
        || metadata.target().starts_with("crabber::session::")
    {
        tracing::Level::DEBUG
    } else {
        tracing::Level::INFO
    };
    metric_target(metadata.target()) && *metadata.level() <= max_level
}
fn metric_target(target: &str) -> bool {
    library_target(target) && !target.starts_with("crabber::wasm::guest")
}
fn counter(name: &str, value: u64, tags: &[String]) -> Value {
    json!({"metric":name,"type":1,"interval":1,"points":[{"timestamp":crate::time_now(),"value":value}],"tags":tags})
}
