//! Allow-listed host correlation identity. This is transport metadata, not execution authority.
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;

/// Validated host trace and parent span identity, independent of any tracing SDK.
/// Hex trace IDs have exactly 16 (64-bit) or 32 (128-bit) digits; span IDs have
/// exactly 16 digits. Both must be nonzero. No baggage or payload fields are accepted.
#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct TraceContext {
    trace_id: String,
    span_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("invalid host trace context identity")]
pub struct TraceContextError;

impl TraceContext {
    /// Validates hexadecimal identity before it can reach admission.
    /// # Errors
    /// Rejects unsupported widths, nonhex characters and zero IDs without echoing input.
    pub fn new(trace_id: &str, span_id: &str) -> Result<Self, TraceContextError> {
        let valid = |value: &str| {
            value.bytes().all(|b| b.is_ascii_hexdigit()) && value.bytes().any(|b| b != b'0')
        };
        if !matches!(trace_id.len(), 16 | 32)
            || span_id.len() != 16
            || !valid(trace_id)
            || !valid(span_id)
        {
            return Err(TraceContextError);
        }
        Ok(Self {
            trace_id: trace_id.to_ascii_lowercase(),
            span_id: span_id.to_ascii_lowercase(),
        })
    }
    #[must_use]
    pub fn trace_id(&self) -> &str {
        &self.trace_id
    }
    #[must_use]
    pub fn span_id(&self) -> &str {
        &self.span_id
    }
}
impl fmt::Debug for TraceContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TraceContext { identity: [redacted] }")
    }
}
impl<'de> Deserialize<'de> for TraceContext {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Identity {
            trace_id: String,
            span_id: String,
        }
        let identity = Identity::deserialize(deserializer)
            .map_err(|_| serde::de::Error::custom(TraceContextError))?;
        Self::new(&identity.trace_id, &identity.span_id).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn validates_identity_without_truncation_or_diagnostics() {
        for trace in ["1234567890ABCDEF", "1234567890ABCDEF1234567890ABCDEF"] {
            let context = TraceContext::new(trace, "FEDCBA0987654321").unwrap();
            assert_eq!(context.trace_id(), trace.to_ascii_lowercase());
            let serialized = serde_json::to_string(&context).unwrap();
            assert_eq!(
                serde_json::from_str::<TraceContext>(&serialized).unwrap(),
                context
            );
            assert!(!format!("{context:?}").contains(context.trace_id()));
        }
        for trace in [
            "",
            "0000000000000000",
            "00000000000000000000000000000000",
            "123",
            "123456789012345678901234567890123",
            "12345678901234567890123456789012345",
            "secret-token-xxxx",
        ] {
            let error = TraceContext::new(trace, "1234567890123456").unwrap_err();
            assert_eq!(error.to_string(), "invalid host trace context identity");
        }
        assert!(TraceContext::new("1234567890123456", "0000000000000000").is_err());
        for value in [
            serde_json::json!({"trace_id":"1234567890123456", "span_id":"1234567890123456", "baggage":"secret-sentinel"}),
            serde_json::json!({"trace_id":"secret-sentinel", "span_id":"1234567890123456"}),
            serde_json::json!({"trace_id":["secret-sentinel"], "span_id":"1234567890123456"}),
        ] {
            let error = serde_json::from_value::<TraceContext>(value).unwrap_err();
            assert!(!error.to_string().contains("secret"));
        }
    }
}
