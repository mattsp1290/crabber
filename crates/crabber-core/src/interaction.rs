//! Tool-authored interaction descriptions, without runtime correlation authority.
//! Unstable until the durable-host contract is qualified.
use crate::{CoreError, input};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InteractionKind {
    Approval,
    UserInput,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InteractionState {
    Pending,
    Answered,
    Denied,
    Cancelled,
    Expired,
}

/// Nonsecret host policy identity. This declares a requirement; it is not a
/// credential and cannot establish that any answer was authorized.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "WireAuthority", into = "WireAuthority")]
pub struct AuthorityRequirement(WireAuthority);
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireAuthority {
    policy_id: String,
    policy_version: String,
}
impl AuthorityRequirement {
    /// # Errors
    /// Rejects empty, unbounded or control-bearing policy identities.
    pub fn new(
        policy_id: impl Into<String>,
        policy_version: impl Into<String>,
    ) -> Result<Self, CoreError> {
        WireAuthority {
            policy_id: policy_id.into(),
            policy_version: policy_version.into(),
        }
        .try_into()
    }
    #[must_use]
    pub fn policy_id(&self) -> &str {
        &self.0.policy_id
    }
    #[must_use]
    pub fn policy_version(&self) -> &str {
        &self.0.policy_version
    }
}
impl TryFrom<WireAuthority> for AuthorityRequirement {
    type Error = CoreError;
    fn try_from(value: WireAuthority) -> Result<Self, Self::Error> {
        if !identity(&value.policy_id) || !identity(&value.policy_version) {
            return Err(invalid());
        }
        Ok(Self(value))
    }
}
impl From<AuthorityRequirement> for WireAuthority {
    fn from(value: AuthorityRequirement) -> Self {
        value.0
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireDescription {
    name: String,
    kind: InteractionKind,
    prompt: String,
    response_schema: Value,
    authority: AuthorityRequirement,
    expires_after_ms: Option<u64>,
}
/// A local named question/approval request. Runtime assigns session/run/call/
/// phase/interaction identity and persists it before exposing it as pending.
/// Runtime must compile the schema and enforce the host authority requirement.
#[derive(Clone, PartialEq, Serialize)]
#[serde(transparent)]
pub struct InteractionDescription(WireDescription);
impl InteractionDescription {
    /// # Errors
    /// Rejects invalid identity, prompt/schema bounds and unrepresentable expiry.
    pub fn new(
        name: impl Into<String>,
        kind: InteractionKind,
        prompt: impl Into<String>,
        response_schema: Value,
        authority: AuthorityRequirement,
        expires_after_ms: Option<u64>,
    ) -> Result<Self, CoreError> {
        let value = WireDescription {
            name: name.into(),
            kind,
            prompt: prompt.into(),
            response_schema,
            authority,
            expires_after_ms,
        };
        if !identity(&value.name)
            || value.prompt.len() > 8192
            || value.prompt.contains('\0')
            || !matches!(value.response_schema, Value::Object(_) | Value::Bool(_))
            || value
                .expires_after_ms
                .is_some_and(|ms| ms > i64::MAX as u64)
        {
            return Err(invalid());
        }
        input::validate_json(&value.response_schema, 0).map_err(|_| invalid())?;
        input::validate_serialized_limit(&value.response_schema, 16384).map_err(|_| invalid())?;
        Ok(Self(value))
    }
    #[must_use]
    pub fn name(&self) -> &str {
        &self.0.name
    }
    #[must_use]
    pub fn kind(&self) -> InteractionKind {
        self.0.kind
    }
    #[must_use]
    pub fn prompt(&self) -> &str {
        &self.0.prompt
    }
    #[must_use]
    pub fn response_schema(&self) -> &Value {
        &self.0.response_schema
    }
    #[must_use]
    pub fn authority(&self) -> &AuthorityRequirement {
        &self.0.authority
    }
    /// Relative expiry is evaluated against the store clock in the transaction.
    /// Zero expires immediately; expiration never implies an affirmative answer.
    #[must_use]
    pub fn expires_after_ms(&self) -> Option<u64> {
        self.0.expires_after_ms
    }
}
impl<'de> Deserialize<'de> for InteractionDescription {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let invalid = || serde::de::Error::custom("invalid interaction description");
        let wire = WireDescription::deserialize(deserializer).map_err(|_| invalid())?;
        Self::new(
            wire.name,
            wire.kind,
            wire.prompt,
            wire.response_schema,
            wire.authority,
            wire.expires_after_ms,
        )
        .map_err(|_| invalid())
    }
}
impl std::fmt::Debug for InteractionDescription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("InteractionDescription([redacted])")
    }
}
pub(crate) fn identity(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128 && !value.chars().any(char::is_control)
}
fn invalid() -> CoreError {
    CoreError::Validation("invalid interaction description".into())
}
