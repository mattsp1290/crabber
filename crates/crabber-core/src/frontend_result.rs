//! External frontend data has typed content and explicit failure, never authority.
use crate::{CoreError, ToolCallId, ToolOutput};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FrontendResultStatus {
    Completed,
    Failed,
    Declined,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireResult {
    call_id: ToolCallId,
    status: FrontendResultStatus,
    output: ToolOutput,
}
/// One externally supplied result. The enclosing continuation binds its exact
/// call, original descriptor and generation; this value cannot authorize work.
#[derive(Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct FrontendToolResult(WireResult);
impl FrontendToolResult {
    /// # Errors
    /// Rejects malformed call identity or contradictory result/error state.
    pub fn new(
        call_id: ToolCallId,
        status: FrontendResultStatus,
        output: ToolOutput,
    ) -> Result<Self, CoreError> {
        if call_id.0.is_empty()
            || call_id.0.len() > 512
            || call_id.0.chars().any(char::is_control)
            || output.is_error() != (status != FrontendResultStatus::Completed)
        {
            return Err(invalid());
        }
        Ok(Self(WireResult {
            call_id,
            status,
            output,
        }))
    }
    #[must_use]
    pub fn call_id(&self) -> &ToolCallId {
        &self.0.call_id
    }
    #[must_use]
    pub fn status(&self) -> FrontendResultStatus {
        self.0.status
    }
    #[must_use]
    pub fn output(&self) -> &ToolOutput {
        &self.0.output
    }
}
fn invalid() -> CoreError {
    CoreError::Validation("invalid frontend tool result".into())
}
impl<'de> Deserialize<'de> for FrontendToolResult {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let invalid = || serde::de::Error::custom("invalid frontend tool result");
        let wire = WireResult::deserialize(deserializer).map_err(|_| invalid())?;
        Self::new(wire.call_id, wire.status, wire.output).map_err(|_| invalid())
    }
}
impl std::fmt::Debug for FrontendToolResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FrontendToolResult")
            .field("call_id", self.call_id())
            .field("status", &self.status())
            .field("output", &"[redacted]")
            .finish()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MediaKind, MediaSource, MessageContent};
    #[test]
    fn external_results_preserve_typed_partial_failure_and_reject_authority_fields() {
        let output = ToolOutput::new(
            vec![
                MessageContent::Text {
                    text: "PRIVATE_PARTIAL".into(),
                },
                MessageContent::Media {
                    kind: MediaKind::Image,
                    mime_type: "image/png".into(),
                    source: MediaSource::File {
                        file_id: "PRIVATE_FILE".into(),
                    },
                },
            ],
            true,
        )
        .unwrap();
        let result = FrontendToolResult::new(
            ToolCallId::new(),
            FrontendResultStatus::Failed,
            output.clone(),
        )
        .unwrap();
        let value = serde_json::to_value(&result).unwrap();
        let restored: FrontendToolResult = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(restored, result);
        assert_eq!(restored.output(), &output);
        assert!(!format!("{result:?}").contains("PRIVATE"));
        let mut forged = value.clone();
        forged["authority"] = "model text".into();
        assert!(serde_json::from_value::<FrontendToolResult>(forged).is_err());
        let mut forged = value;
        forged["status"] = "completed".into();
        assert!(serde_json::from_value::<FrontendToolResult>(forged).is_err());
        assert!(
            FrontendToolResult::new(
                ToolCallId::from("bad\0id"),
                FrontendResultStatus::Failed,
                output
            )
            .is_err()
        );
    }
}
