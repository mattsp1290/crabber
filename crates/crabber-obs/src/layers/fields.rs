use std::fmt::{self, Write};
use tracing::field::{Field, Visit};

const KEYS: &[&str] = &[
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
    "reason",
    "purpose",
    "permission",
    "protocol",
    "scope",
    "keyed",
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
    "host.trace_id",
    "host.span_id",
    "host.predecessor_trace_id",
    "otel.status_code",
    "otel.kind",
    "tokens.input",
    "tokens.output",
    "compaction_requested",
];

#[derive(Clone)]
pub(crate) enum SafeValue {
    String(String),
    Bool(bool),
    U64(u64),
    I64(i64),
    F64(f64),
}
impl SafeValue {
    pub(crate) fn to_value(&self) -> serde_json::Value {
        match self {
            Self::String(v) => serde_json::json!(v),
            Self::Bool(v) => serde_json::json!(v),
            Self::U64(v) => serde_json::json!(v),
            Self::I64(v) => serde_json::json!(v),
            Self::F64(v) => serde_json::json!(v),
        }
    }
    pub(crate) fn text(&self) -> String {
        match self {
            Self::String(v) => v.clone(),
            _ => self.to_value().to_string(),
        }
    }
}

/// Bounded allowlisted span/event fields, shared by the two layers.
#[derive(Default, Clone)]
pub(crate) struct Fields {
    pub values: Vec<(&'static str, SafeValue)>,
    pub message: Option<String>,
    pub redacted: u32,
}
impl Fields {
    pub fn get(&self, name: &str) -> Option<String> {
        self.values
            .iter()
            .find(|(key, _)| *key == name)
            .map(|(_, value)| value.text())
    }
    pub fn put(&mut self, key: &'static str, value: SafeValue) {
        if let Some((_, old)) = self.values.iter_mut().find(|(name, _)| *name == key) {
            *old = value;
        } else if self.values.len() < 32 {
            self.values.push((key, value));
        } else {
            self.redacted = self.redacted.saturating_add(1);
        }
    }
    fn record(&mut self, field: &Field, value: SafeValue) {
        if field.name() == "message" {
            self.message = Some(value.text());
        } else if let Some(key) = KEYS.iter().copied().find(|key| *key == field.name()) {
            self.put(key, value);
        } else {
            self.redacted = self.redacted.saturating_add(1);
        }
    }
}
impl Visit for Fields {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.record(field, SafeValue::String(bounded(format_args!("{value}"))));
    }
    fn record_bool(&mut self, field: &Field, value: bool) {
        self.record(field, SafeValue::Bool(value));
    }
    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record(field, SafeValue::U64(value));
    }
    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record(field, SafeValue::I64(value));
    }
    fn record_f64(&mut self, field: &Field, value: f64) {
        self.record(field, SafeValue::F64(value));
    }
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        if field.name() != "message" && !KEYS.contains(&field.name()) {
            self.redacted = self.redacted.saturating_add(1);
            return;
        }
        // tracing uses this callback for both %Display fields and message arguments.
        self.record(field, SafeValue::String(bounded(format_args!("{value:?}"))));
    }
}
struct Bounded(String);
impl Write for Bounded {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        let remaining = 256 - self.0.len();
        let mut end = remaining.min(value.len());
        while !value.is_char_boundary(end) {
            end -= 1;
        }
        self.0.push_str(&value[..end]);
        Ok(())
    }
}
pub(crate) fn bounded(arguments: fmt::Arguments<'_>) -> String {
    let mut output = Bounded(String::new());
    let _ = output.write_fmt(arguments);
    output.0
}
