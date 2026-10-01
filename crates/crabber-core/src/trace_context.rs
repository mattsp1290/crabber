//! Allow-listed host correlation identity. This is transport metadata, not execution authority.
use crate::RunId;
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt;

/// Validated host trace and parent span identity, independent of any tracing SDK.
/// Hex trace IDs have exactly 16 (64-bit) or 32 (128-bit) digits; span IDs have
/// exactly 16 digits. Both must be nonzero. No baggage or payload fields are accepted.
#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct TraceContext {
    trace_id: String,
    span_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    predecessor: Option<TraceLink>,
}

/// One bounded prior attempt identity; contains no nested links or execution authority.
#[derive(Clone, PartialEq, Eq, Serialize)]
pub struct TraceLink {
    trace_id: String,
    span_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    observation_attempt: Option<RunId>,
}
impl TraceLink {
    #[must_use]
    pub fn observation_attempt(&self) -> Option<&RunId> {
        self.observation_attempt.as_ref()
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
impl<'de> Deserialize<'de> for TraceLink {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Identity {
            trace_id: String,
            span_id: String,
            #[serde(default)]
            observation_attempt: Option<RunId>,
        }
        let identity = Identity::deserialize(deserializer)
            .map_err(|_| serde::de::Error::custom(TraceContextError))?;
        let context = TraceContext::new(&identity.trace_id, &identity.span_id)
            .map_err(serde::de::Error::custom)?;
        if let Some(attempt) = &identity.observation_attempt {
            validate_attempt(attempt).map_err(serde::de::Error::custom)?;
        }
        Ok(Self {
            trace_id: context.trace_id,
            span_id: context.span_id,
            observation_attempt: identity.observation_attempt,
        })
    }
}
impl fmt::Debug for TraceLink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TraceLink { identity: [redacted] }")
    }
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
            predecessor: None,
        })
    }
    /// Starts a new trace linked to one prior attempt, discarding its older link.
    /// # Errors
    /// Rejects reuse of the prior numeric trace identity (including 64/128-bit aliases).
    pub fn linked_to(mut self, prior: &Self) -> Result<Self, TraceContextError> {
        if self.trace_id.trim_start_matches('0') == prior.trace_id.trim_start_matches('0') {
            return Err(TraceContextError);
        }
        self.predecessor = Some(TraceLink {
            trace_id: prior.trace_id.clone(),
            span_id: prior.span_id.clone(),
            observation_attempt: None,
        });
        Ok(self)
    }
    /// Links one specifically observed prior execution attempt for native LLM lineage.
    /// The attempt is correlation only; it conveys no admission or lease authority.
    /// # Errors
    /// Rejects same numeric trace or noncanonical/zero attempt UUID.
    pub fn linked_to_attempt(
        self,
        prior: &Self,
        attempt: &RunId,
    ) -> Result<Self, TraceContextError> {
        validate_attempt(attempt)?;
        let mut context = self.linked_to(prior)?;
        context
            .predecessor
            .as_mut()
            .ok_or(TraceContextError)?
            .observation_attempt = Some(attempt.clone());
        Ok(context)
    }
    #[must_use]
    pub fn predecessor(&self) -> Option<&TraceLink> {
        self.predecessor.as_ref()
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
            #[serde(default)]
            predecessor: Option<TraceLink>,
        }
        let identity = Identity::deserialize(deserializer)
            .map_err(|_| serde::de::Error::custom(TraceContextError))?;
        let context =
            Self::new(&identity.trace_id, &identity.span_id).map_err(serde::de::Error::custom)?;
        match identity.predecessor {
            Some(link) => {
                let prior =
                    Self::new(&link.trace_id, &link.span_id).map_err(serde::de::Error::custom)?;
                match link.observation_attempt {
                    Some(attempt) => context.linked_to_attempt(&prior, &attempt),
                    None => context.linked_to(&prior),
                }
                .map_err(serde::de::Error::custom)
            }
            None => Ok(context),
        }
    }
}

fn validate_attempt(attempt: &RunId) -> Result<(), TraceContextError> {
    let parsed = uuid::Uuid::parse_str(&attempt.0).map_err(|_| TraceContextError)?;
    if parsed.is_nil() || parsed.to_string() != attempt.0 {
        return Err(TraceContextError);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn observation_links_are_bounded_validated_and_old_links_remain_usable() {
        let prior = TraceContext::new("0000000000000001", "1234567890abcdef").unwrap();
        let current = TraceContext::new("0000000000000002", "1234567890abcdef").unwrap();
        let legacy = current.clone().linked_to(&prior).unwrap();
        assert!(
            legacy
                .predecessor()
                .unwrap()
                .observation_attempt()
                .is_none()
        );
        let attempt = RunId::new();
        let linked = current.clone().linked_to_attempt(&prior, &attempt).unwrap();
        assert_eq!(
            linked.predecessor().unwrap().observation_attempt(),
            Some(&attempt)
        );
        assert_eq!(
            serde_json::from_str::<TraceContext>(&serde_json::to_string(&linked).unwrap()).unwrap(),
            linked
        );
        assert!(!format!("{linked:?} {:?}", linked.predecessor()).contains(&attempt.to_string()));
        for invalid in [
            "SECRET_SENTINEL",
            "00000000-0000-0000-0000-000000000000",
            "123456781234123412341234567890ab",
            "12345678-1234-1234-1234-1234567890AB",
        ] {
            let error = current
                .clone()
                .linked_to_attempt(&prior, &RunId(invalid.into()))
                .unwrap_err();
            assert!(!format!("{error} {error:?}").contains(invalid));
            let json = serde_json::json!({"trace_id":current.trace_id(),"span_id":current.span_id(),"predecessor":{"trace_id":prior.trace_id(),"span_id":prior.span_id(),"observation_attempt":invalid}});
            assert!(serde_json::from_value::<TraceContext>(json).is_err());
        }
    }
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
