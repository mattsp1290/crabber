//! Nonblocking span metrics and redacted event logs for the existing exporter.
mod fields;
mod logs;
mod metric_specs;
mod metric_values;
mod span_metrics;

pub(crate) use fields::{Fields, SafeValue};
pub use logs::LogsLayer;
pub use span_metrics::SpanMetricsLayer;

use serde_json::{Value, json};
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::Level;

/// Closed set of library diagnostic and lifecycle messages.
pub const MESSAGES: &[&str] = &[
    "run started",
    "run settled",
    "tool settled",
    "tool call failed",
    "run admitted",
    "permission decided",
    "run attempt lost its lease",
    "lease ownership lost",
    "recovery skipped run",
    "lease lost",
    "lease renew failed",
    "model request failed, retry scheduled",
    "event receiver lagged",
    "recovery skipped",
    "approval suspended",
    "durable cancel accepted",
    "run suspended for approval",
    "schema migrated",
    "credential refreshed, retrying",
    "provider request rejected",
    "extension install failed",
    "extension close timed out",
    "guest log",
    "store commit failed",
    "unlisted message",
];
fn allowlisted_message(formatted: &str) -> Option<&'static str> {
    MESSAGES
        .iter()
        .copied()
        .find(|message| *message == formatted)
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

/// Shared cumulative layer counters, separate from export queue health.
#[derive(Debug, Default)]
pub struct LayerHealth {
    pub spans_seen: AtomicU64,
    pub events_seen: AtomicU64,
    pub metrics_enqueued: AtomicU64,
    pub metrics_dropped: AtomicU64,
    pub logs_enqueued: AtomicU64,
    pub logs_dropped: AtomicU64,
    pub messages_unlisted: AtomicU64,
    pub attributes_scrubbed: AtomicU64,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LayerHealthSnapshot {
    pub spans_seen: u64,
    pub events_seen: u64,
    pub metrics_enqueued: u64,
    pub metrics_dropped: u64,
    pub logs_enqueued: u64,
    pub logs_dropped: u64,
    pub messages_unlisted: u64,
    pub attributes_scrubbed: u64,
}
impl LayerHealth {
    #[must_use]
    pub fn snapshot(&self) -> LayerHealthSnapshot {
        LayerHealthSnapshot {
            spans_seen: self.spans_seen.load(Ordering::Relaxed),
            events_seen: self.events_seen.load(Ordering::Relaxed),
            metrics_enqueued: self.metrics_enqueued.load(Ordering::Relaxed),
            metrics_dropped: self.metrics_dropped.load(Ordering::Relaxed),
            logs_enqueued: self.logs_enqueued.load(Ordering::Relaxed),
            logs_dropped: self.logs_dropped.load(Ordering::Relaxed),
            messages_unlisted: self.messages_unlisted.load(Ordering::Relaxed),
            attributes_scrubbed: self.attributes_scrubbed.load(Ordering::Relaxed),
        }
    }
}

pub(crate) struct SafeLog {
    message: &'static str,
    status: &'static str,
    target: &'static str,
    time_ns: i128,
    fields: Vec<(&'static str, SafeValue)>,
    trace: Option<LogTrace>,
    redacted_count: u32,
}
struct LogTrace {
    trace_id_hex: String,
    span_id_hex: String,
    dd_trace_id: String,
    dd_span_id: String,
}
impl SafeLog {
    pub(crate) fn to_value(&self, config: &crate::DatadogConfig) -> Value {
        let mut tags = vec![
            format!("service:{}", config.service),
            format!("env:{}", config.env),
            format!("version:{}", config.version),
        ];
        tags.extend(crate::sanitized_host_tags(config));
        let mut value = json!({"message":self.message,"status":self.status,"service":config.service,"ddsource":"crabber","ddtags":tags.join(","),"crabber.source":"layer","target":self.target,"timestamp":self.time_ns / 1_000_000,"redacted_count":self.redacted_count});
        for (key, field) in &self.fields {
            value[*key] = field.to_value();
        }
        if let Some(trace) = &self.trace {
            value["trace_id"] = json!(trace.trace_id_hex);
            value["span_id"] = json!(trace.span_id_hex);
            value["dd.trace_id"] = json!(trace.dd_trace_id);
            value["dd.span_id"] = json!(trace.dd_span_id);
        }
        value
    }
}

fn library_target(target: &str) -> bool {
    [
        "crabber::runtime",
        "crabber::session",
        "crabber::providers",
        "crabber::extension",
        "crabber::wasm",
    ]
    .iter()
    .any(|prefix| {
        target == *prefix
            || target
                .strip_prefix(prefix)
                .is_some_and(|suffix| suffix.starts_with("::"))
    })
}

#[cfg(test)]
mod tests;
