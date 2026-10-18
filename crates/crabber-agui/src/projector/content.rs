//! Typed wire content. Source interpretation never follows a string heuristic.
use ag_ui_core::types::{ContentPart, MessageContent, PartSource};
use crabber_core::{ContentBlock, MediaKind, MediaSource, ReasoningProvenance};
use serde_json::{Value, json};

use super::ProjectionError;

pub(super) fn tool_content(
    blocks: &[ContentBlock],
    reasoning: bool,
) -> Result<MessageContent, ProjectionError> {
    let mut parts = Vec::new();
    append(blocks, None, reasoning, &mut parts)?;
    Ok(MessageContent::Parts(parts))
}

fn append(
    blocks: &[ContentBlock],
    metadata: Option<&Value>,
    reasoning: bool,
    parts: &mut Vec<ContentPart>,
) -> Result<(), ProjectionError> {
    for block in blocks {
        match block {
            ContentBlock::Text { text } => parts.push(ContentPart::Text {
                text: text.clone(),
                id: None,
                metadata: metadata.cloned(),
            }),
            ContentBlock::MediaContent {
                kind,
                mime_type,
                source,
            } => {
                let source = match source {
                    MediaSource::Data { base64 } => PartSource::Data {
                        value: base64.clone(),
                        mime_type: mime_type.clone(),
                    },
                    MediaSource::Url { url } => PartSource::Url {
                        value: url.clone(),
                        mime_type: Some(mime_type.clone()),
                    },
                    MediaSource::File { file_id } => PartSource::File {
                        value: file_id.clone(),
                        provider: None,
                        mime_type: Some(mime_type.clone()),
                    },
                };
                let id = None;
                let metadata = metadata.cloned();
                parts.push(match kind {
                    MediaKind::Image => ContentPart::Image {
                        source,
                        id,
                        metadata,
                    },
                    MediaKind::Audio => ContentPart::Audio {
                        source,
                        id,
                        metadata,
                    },
                    MediaKind::Video => ContentPart::Video {
                        source,
                        id,
                        metadata,
                    },
                    MediaKind::Document => ContentPart::Document {
                        source,
                        id,
                        metadata,
                    },
                });
            }
            ContentBlock::ToolResult {
                call_id,
                content,
                is_error,
            } => {
                let nested = json!({"crabber":{"toolCallId":call_id,"isError":is_error}});
                append(content, Some(&nested), reasoning, parts)?;
            }
            ContentBlock::Reasoning {
                text,
                provenance: ReasoningProvenance::PublicSummary,
                item_id: Some(item_id),
                ..
            } if reasoning
                && !item_id.is_empty()
                && item_id.len() <= 1024
                && !item_id.chars().any(char::is_control) =>
            {
                parts.push(ContentPart::Text {
                    text: text.clone(), id: None,
                    metadata: Some(json!({"crabber":{"reasoningProvenance":"public_summary","itemId":item_id}})),
                });
            }
            // Unspecified/private reasoning and provider continuation never
            // become tool output, even with the generic reasoning display flag.
            ContentBlock::Reasoning { .. } | ContentBlock::ProviderState { .. } => {}
            ContentBlock::Media { .. } | ContentBlock::ToolCall { .. } => {
                return Err(ProjectionError::Malformed);
            }
        }
    }
    Ok(())
}
