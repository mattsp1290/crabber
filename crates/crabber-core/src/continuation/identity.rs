use crate::{CoreError, InputFingerprint, InteractionState};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Host-declared native behavior and private checkpoint contract. No credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResumableToolIdentity {
    pub component_id: String,
    pub component_version: String,
    pub config_fingerprint: InputFingerprint,
    pub checkpoint_schema_id: String,
    pub checkpoint_schema_version: String,
}
impl ResumableToolIdentity {
    /// # Errors
    /// Rejects missing, oversized or control-bearing declarations.
    pub fn validate(&self) -> Result<(), CoreError> {
        for text in [
            &self.component_id,
            &self.component_version,
            &self.checkpoint_schema_id,
            &self.checkpoint_schema_version,
        ] {
            if !crate::interaction::identity(text) {
                return Err(CoreError::Validation(
                    "invalid resumable tool identity".into(),
                ));
            }
        }
        Ok(())
    }
    #[must_use]
    pub fn accepts(&self, checkpoint: &super::ToolCheckpoint) -> bool {
        self.checkpoint_schema_id == checkpoint.schema_id()
            && self.checkpoint_schema_version == checkpoint.schema_version()
    }
}
/// Accepted, exactly correlated tool-local response. Host authority is verified
/// and recorded by Store before the runtime supplies this private resume input.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptedToolResponse {
    pub name: String,
    pub state: InteractionState,
    pub value: Option<Value>,
}
impl std::fmt::Debug for AcceptedToolResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AcceptedToolResponse([redacted])")
    }
}

impl AcceptedToolResponse {
    /// Validate bounded private response JSON before recording host acceptance.
    /// # Errors
    /// Rejects deep JSON and values exceeding 64 KiB.
    pub fn validate_value(value: &Value) -> Result<(), CoreError> {
        crate::input::validate_json(value, 0)?;
        crate::input::validate_serialized_limit(value, super::MAX_TOOL_CHECKPOINT_BYTES)
    }
}
