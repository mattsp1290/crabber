//! Typed ordinary/resumable tool completion; runtime supplies result authority.
use crate::{CoreError, MessageContent, input};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireOutput {
    content: Vec<MessageContent>,
    is_error: bool,
}

/// Ordered typed content and a tool-declared error bit. Unstable until durable
/// host qualification. Runtime denial/interruption/failure cannot be cleared by
/// this bit. A suspension is a separate control outcome, never a `ToolOutput`.
#[derive(Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct ToolOutput(WireOutput);
impl ToolOutput {
    /// Validate a completion without attaching call/session/workspace identity.
    /// # Errors
    /// Rejects unsupported, malformed or oversized content with fixed diagnostics.
    pub fn new(content: Vec<MessageContent>, is_error: bool) -> Result<Self, CoreError> {
        let wire = WireOutput { content, is_error };
        if wire.content.len() > input::MAX_INPUT_PARTS {
            return Err(invalid());
        }
        for block in &wire.content {
            input::validate_content(block).map_err(|_| invalid())?;
        }
        input::validate_serialized_bound(&wire).map_err(|_| invalid())?;
        Ok(Self(wire))
    }
    /// Deliberate JSON-to-text convenience. It never interprets arbitrary JSON
    /// as media, interaction, result identity, reasoning or execution authority.
    /// # Errors
    /// Rejects oversized/deep JSON before allocating its serialized text.
    pub fn from_json(value: &Value) -> Result<Self, CoreError> {
        input::validate_json(value, 0).map_err(|_| invalid())?;
        input::validate_serialized_bound(value).map_err(|_| invalid())?;
        let text = serde_json::to_string(value).map_err(|_| invalid())?;
        Self::new(vec![MessageContent::Text { text }], false)
    }
    /// Project validated content into durable `ToolResult` blocks without flattening
    /// media or attaching call/session identity or a runtime outcome class.
    #[must_use]
    pub fn to_content_blocks(&self) -> Vec<crate::ContentBlock> {
        self.content().iter().map(input::record_content).collect()
    }
    /// Recover only typed data blocks from a stored result or a protected transform.
    /// Legacy URI media and control/provider/reasoning blocks are not interpreted.
    /// # Errors
    /// Rejects untyped media, authority-bearing blocks, malformed or oversized data.
    pub fn from_content_blocks(
        content: Vec<crate::ContentBlock>,
        is_error: bool,
    ) -> Result<Self, CoreError> {
        if content.len() > input::MAX_INPUT_PARTS {
            return Err(invalid());
        }
        let content = content
            .into_iter()
            .map(|block| match block {
                crate::ContentBlock::Text { text } => Ok(MessageContent::Text { text }),
                crate::ContentBlock::MediaContent {
                    kind,
                    mime_type,
                    source,
                } => Ok(MessageContent::Media {
                    kind,
                    mime_type,
                    source,
                }),
                _ => Err(invalid()),
            })
            .collect::<Result<Vec<_>, CoreError>>()?;
        Self::new(content, is_error)
    }
    #[must_use]
    pub fn content(&self) -> &[MessageContent] {
        &self.0.content
    }
    #[must_use]
    pub fn is_error(&self) -> bool {
        self.0.is_error
    }
    /// Monotonic escalation; tool-authored transforms cannot use this to clear
    /// an error that an earlier step declared.
    #[must_use]
    pub fn with_error(mut self) -> Self {
        self.0.is_error = true;
        self
    }
}
fn invalid() -> CoreError {
    CoreError::Validation("invalid typed tool output".into())
}
impl<'de> Deserialize<'de> for ToolOutput {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let invalid = || serde::de::Error::custom("invalid typed tool output");
        let wire = WireOutput::deserialize(deserializer).map_err(|_| invalid())?;
        Self::new(wire.content, wire.is_error).map_err(|_| invalid())
    }
}
impl std::fmt::Debug for ToolOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ToolOutput([redacted])")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MediaKind, MediaSource};
    #[test]
    fn media_order_and_error_are_preserved_and_json_stays_literal_text() {
        let content = vec![
            MessageContent::Text {
                text: "caption".into(),
            },
            MessageContent::Media {
                kind: MediaKind::Image,
                mime_type: "image/png".into(),
                source: MediaSource::File {
                    file_id: "opaque-file".into(),
                },
            },
        ];
        let output = ToolOutput::new(content.clone(), false).unwrap();
        assert_eq!(output.content(), content);
        let records = output.to_content_blocks();
        assert!(
            matches!(&records[1], crate::ContentBlock::MediaContent { source: MediaSource::File { file_id }, .. } if file_id == "opaque-file")
        );
        assert_eq!(
            ToolOutput::from_content_blocks(records, false).unwrap(),
            output
        );
        for forbidden in [
            crate::ContentBlock::Media {
                uri: "https://example.invalid/image".into(),
                mime_type: "image/png".into(),
            },
            crate::ContentBlock::ToolCall {
                call_id: crate::ToolCallId::new(),
                name: "authority".into(),
                arguments: serde_json::json!({}),
            },
            crate::ContentBlock::ProviderState {
                codec_id: "authority".into(),
                payload: serde_json::Value::Null,
            },
            crate::ContentBlock::Reasoning {
                text: "private".into(),
                provider_state: None,
            },
        ] {
            assert!(ToolOutput::from_content_blocks(vec![forbidden], false).is_err());
        }
        let restored: ToolOutput =
            serde_json::from_value(serde_json::to_value(output.clone().with_error()).unwrap())
                .unwrap();
        assert!(restored.is_error());
        assert_eq!(restored.content(), output.content());
        let json = serde_json::json!({"type":"media","source":"PRIVATE_SENTINEL","is_error":false});
        let literal = ToolOutput::from_json(&json).unwrap();
        assert_eq!(
            literal.content(),
            &[MessageContent::Text {
                text: json.to_string()
            }]
        );
        assert!(!format!("{literal:?}").contains("PRIVATE_SENTINEL"));
        assert!(ToolOutput::new(vec![], true).unwrap().is_error());
        assert!(
            serde_json::from_value::<ToolOutput>(
                serde_json::json!({"content":[{"type":"PRIVATE_SENTINEL"}],"is_error":false})
            )
            .unwrap_err()
            .to_string()
            .contains("invalid typed tool output")
        );
    }
}
