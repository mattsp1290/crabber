use super::{
    Fields, LayerHealth, LogTrace, LogsPolicy, SafeLog, allowlisted_message, library_target,
};
use crate::DatadogObserver;
use std::sync::{Arc, atomic::Ordering};
use tracing::{
    Event, Level, Subscriber,
    span::{Attributes, Id, Record},
};
use tracing_subscriber::{
    Layer,
    filter::{LevelFilter, Targets},
    layer::Context,
    registry::LookupSpan,
};

/// Exports bounded structured diagnostics while preserving observer lifecycle logs.
pub struct LogsLayer {
    observer: DatadogObserver,
    policy: LogsPolicy,
    health: Arc<LayerHealth>,
}
struct LogFields(Fields);
const LOG_KEYS: &[&str] = &[
    "session.id",
    "run.id",
    "attempt.id",
    "tool.name",
    "tool.call_id",
    "provider.id",
    "model.id",
    "extension.id",
    "db.system",
    "crabber.store.scope",
    "outcome",
    "error.kind",
    "retry.attempt",
    "retry.delay_ms",
    "skip.reason",
    "http.response.status_code",
    "lagged",
    "calls",
    "leases",
    "pending_tasks",
    "schema.version",
    "source",
    "guest.text",
];
impl LogsLayer {
    #[must_use]
    pub fn new(observer: DatadogObserver, policy: LogsPolicy, health: Arc<LayerHealth>) -> Self {
        Self {
            observer,
            policy,
            health,
        }
    }
    /// Apply this as a per-layer filter using `Layer::with_filter`.
    #[must_use]
    pub fn filter(policy: &LogsPolicy) -> Targets {
        Targets::new()
            .with_target("crabber::runtime", policy.min_level)
            .with_target("crabber::runtime::lifecycle", LevelFilter::OFF)
            .with_target("crabber::session", policy.min_level)
            .with_target("crabber::providers", policy.min_level)
            .with_target("crabber::extension", policy.min_level)
            .with_target("crabber::wasm", policy.min_level)
            .with_target(
                "crabber::wasm::guest",
                if policy.guest_text {
                    LevelFilter::WARN
                } else {
                    LevelFilter::OFF
                },
            )
    }
    fn enabled(&self, target: &str, level: Level) -> bool {
        if target == "crabber::wasm::guest" || target.starts_with("crabber::wasm::guest::") {
            return self.policy.guest_text && level <= Level::WARN;
        }
        library_target(target)
            && !target.starts_with("crabber::runtime::lifecycle")
            && level <= self.policy.min_level
    }
}
impl<S> Layer<S> for LogsLayer
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        if !self.enabled(attrs.metadata().target(), *attrs.metadata().level()) {
            return;
        }
        if let Some(span) = ctx.span(id) {
            let mut fields = Fields::default();
            attrs.record(&mut fields);
            span.extensions_mut().insert(LogFields(fields));
            self.health.spans_seen.fetch_add(1, Ordering::Relaxed);
        }
    }
    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        if let Some(span) = ctx.span(id)
            && let Some(fields) = span.extensions_mut().get_mut::<LogFields>()
        {
            values.record(&mut fields.0);
        }
    }
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        if !self.enabled(event.metadata().target(), *event.metadata().level()) {
            return;
        }
        self.health.events_seen.fetch_add(1, Ordering::Relaxed);
        let mut event_fields = Fields::default();
        event.record(&mut event_fields);
        if matches!(
            event_fields.message.as_deref(),
            Some("run admitted" | "run settled" | "tool settled" | "permission decided")
        ) {
            return;
        }
        let message = event_fields
            .message
            .as_deref()
            .and_then(allowlisted_message)
            .unwrap_or_else(|| {
                event_fields.redacted = event_fields.redacted.saturating_add(1);
                self.health
                    .messages_unlisted
                    .fetch_add(1, Ordering::Relaxed);
                "unlisted message"
            });
        let mut merged = Fields::default();
        let mut trace = None;
        if let Some(scope) = ctx.event_scope(event) {
            for span in scope.from_root() {
                if let Some(fields) = span.extensions().get::<LogFields>() {
                    for (key, value) in &fields.0.values {
                        merged.put(key, value.clone());
                    }
                    merged.redacted = merged.redacted.saturating_add(fields.0.redacted);
                    if matches!(span.name(), "crabber.run" | "crabber.resume") {
                        trace = correlation(&fields.0);
                    }
                }
            }
        }
        for (key, value) in event_fields.values {
            merged.put(key, value);
        }
        merged.redacted = merged.redacted.saturating_add(event_fields.redacted);
        merged.values.retain(|(key, _)| {
            let keep = LOG_KEYS.contains(key) && (*key != "guest.text" || self.policy.guest_text);
            if !keep {
                merged.redacted = merged.redacted.saturating_add(1);
            }
            keep
        });
        let log = SafeLog {
            message,
            status: match *event.metadata().level() {
                Level::ERROR => "error",
                Level::WARN => "warn",
                _ => "info",
            },
            target: event.metadata().target(),
            time_ns: time::OffsetDateTime::now_utc().unix_timestamp_nanos(),
            fields: merged.values,
            trace,
            redacted_count: merged.redacted,
        };
        let counter = if self.observer.enqueue_log(log).is_ok() {
            &self.health.logs_enqueued
        } else {
            &self.health.logs_dropped
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}
fn correlation(fields: &Fields) -> Option<LogTrace> {
    let trace = fields.get("host.trace_id")?;
    let span = fields.get("host.span_id")?;
    if !matches!(trace.len(), 16 | 32)
        || span.len() != 16
        || !trace
            .bytes()
            .chain(span.bytes())
            .all(|b| b.is_ascii_hexdigit())
    {
        return None;
    }
    let trace_number = u128::from_str_radix(&trace, 16).ok()?;
    let span_number = u64::from_str_radix(&span, 16).ok()?;
    if trace_number == 0 || span_number == 0 {
        return None;
    }
    Some(LogTrace {
        trace_id_hex: format!("{trace_number:032x}"),
        span_id_hex: format!("{span_number:016x}"),
        dd_trace_id: crate::apm_trace(&trace),
        dd_span_id: span_number.to_string(),
    })
}
