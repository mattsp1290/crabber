//! Model-facing result envelopes. Cancellation is an executor error, never an envelope.
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Eino-compatible orchestration outcomes; tools emit Succeeded or Failed only.
#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    /// Completed tool work (including a shell command with nonzero status).
    Succeeded,
    /// Tool-level failure.
    Failed,
    /// Reserved for caller timeouts.
    TimedOut,
    /// Reserved for orchestration rejection.
    Rejected,
}
/// A sanitized error: messages must not reveal host paths or contents.
#[derive(Serialize, Clone, PartialEq, Eq, Debug)]
pub struct ToolError {
    /// Stable model-facing category.
    pub category: &'static str,
    /// Sanitized explanation.
    pub message: String,
}
impl ToolError {
    /// Construct an error without any operating-system error text.
    pub fn new(category: &'static str, message: impl Into<String>) -> Self {
        Self {
            category,
            message: message.into(),
        }
    }
    /// Convert to the failure envelope.
    #[must_use]
    pub fn value(&self) -> Value {
        failed(self.category, &self.message)
    }
}
/// Stable error category vocabulary.
pub mod category {
    /// Recorded host denial.
    pub const DENIED: &str = "denied";
    /// Recorded host cancellation.
    pub const CANCELLED: &str = "cancelled";
    /// The `conflict` failure category.
    pub const CONFLICT: &str = "conflict";
    /// The `validation` failure category.
    pub const VALIDATION: &str = "validation";
    /// The `path_escape` failure category.
    pub const PATH_ESCAPE: &str = "path_escape";
    /// The `not_found` failure category.
    pub const NOT_FOUND: &str = "not_found";
    /// The `is_directory` failure category.
    pub const IS_DIRECTORY: &str = "is_directory";
    /// The `not_directory` failure category.
    pub const NOT_DIRECTORY: &str = "not_directory";
    /// The `anchor_not_found` failure category.
    pub const ANCHOR_NOT_FOUND: &str = "anchor_not_found";
    /// The `anchor_ambiguous` failure category.
    pub const ANCHOR_AMBIGUOUS: &str = "anchor_ambiguous";
    /// The `too_large` failure category.
    pub const TOO_LARGE: &str = "too_large";
    /// The `binary` failure category.
    pub const BINARY: &str = "binary";
    /// The `io` failure category.
    pub const IO: &str = "io";
    /// The `timeout` failure category.
    pub const TIMEOUT: &str = "timeout";
    /// The `exec_failed` failure category.
    pub const EXEC_FAILED: &str = "exec_failed";
    /// The `invalid_pattern` failure category.
    pub const INVALID_PATTERN: &str = "invalid_pattern";
    /// The `workspace_mismatch` failure category.
    pub const WORKSPACE_MISMATCH: &str = "workspace_mismatch";
    /// The `unavailable` failure category.
    pub const UNAVAILABLE: &str = "unavailable";
    /// The `unknown` failure category.
    pub const UNKNOWN: &str = "unknown";
}
/// Build an envelope containing exactly outcome and error.
pub fn failed(category: &'static str, message: impl Into<String>) -> Value {
    json!({"outcome":"failed","error": {"category":category,"message":message.into()}})
}

/// Convert an envelope to bounded typed output, preserving the failure status.
/// Named text fields may be truncated to fit Crabber's serialized output bound.
///
/// # Panics
/// Panics if an internal selected-string or bounded-envelope invariant is violated.
pub fn to_output(mut value: Value, truncatable: &[&str]) -> crabber::core::ToolOutput {
    loop {
        if let Ok(output) = crabber::core::ToolOutput::from_json(&value) {
            return if value.get("outcome").and_then(Value::as_str) == Some("failed") {
                output.with_error()
            } else {
                output
            };
        }
        let longest = truncatable
            .iter()
            .filter_map(|key| {
                value
                    .get(*key)
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                    .map(|s| (*key, s.len()))
            })
            .max_by_key(|(_, len)| *len);
        if let Some((key, len)) = longest {
            let text = value[key].as_str().expect("selected string");
            let mut end = len / 2;
            while !text.is_char_boundary(end) {
                end -= 1;
            }
            value[key] = Value::String(text[..end].into());
            value[format!("{key}_truncated")] = Value::Bool(true);
        } else {
            return crabber::core::ToolOutput::from_json(&failed(
                category::TOO_LARGE,
                "tool result exceeds the 4 MiB output bound",
            ))
            .expect("bounded failure envelope")
            .with_error();
        }
    }
}

/// Fail closed when an executor is called without its host context.
///
/// # Errors
/// This helper returns a failed tool output rather than an executor error.
pub fn context_required() -> Result<crabber::core::ToolOutput, crabber::ExtensionError> {
    Ok(to_output(
        failed(category::VALIDATION, "context required"),
        &[],
    ))
}
