//! Lossless projection of validated input messages into persistence records.
use super::{InputContent, InputEnvelope, MessageContent, validation};
use crate::{ContentBlock, CoreError, Message, MessageId, Part, PartId, PartKind, Role, SessionId};
use std::collections::BTreeSet;
use time::OffsetDateTime;

pub(crate) fn record_content(content: &MessageContent) -> ContentBlock {
    match content {
        MessageContent::Text { text } => ContentBlock::Text { text: text.clone() },
        MessageContent::Media {
            kind,
            mime_type,
            source,
        } => ContentBlock::MediaContent {
            kind: *kind,
            mime_type: mime_type.clone(),
            source: source.clone(),
        },
    }
}
fn content(input: &InputContent) -> ContentBlock {
    match input {
        InputContent::Content { content } => record_content(content),
        InputContent::ToolCall {
            call_id,
            name,
            arguments,
        } => ContentBlock::ToolCall {
            call_id: call_id.clone(),
            name: name.clone(),
            arguments: arguments.clone(),
        },
        InputContent::ToolResult {
            call_id,
            content,
            is_error,
        } => ContentBlock::ToolResult {
            call_id: call_id.clone(),
            content: content.iter().map(record_content).collect(),
            is_error: *is_error,
        },
    }
}
fn kind(role: Role, input: &InputContent) -> PartKind {
    match input {
        InputContent::Content {
            content: MessageContent::Text { .. },
        } => match role {
            Role::User => PartKind::UserInputText,
            Role::Assistant => PartKind::AssistantText,
            Role::System | Role::Tool => PartKind::InputText,
        },
        InputContent::Content {
            content: MessageContent::Media { .. },
        } => PartKind::Media,
        InputContent::ToolCall { .. } => PartKind::FunctionToolCall,
        InputContent::ToolResult { .. } => PartKind::FunctionToolResult,
    }
}
impl InputEnvelope {
    /// Materialize message/part IDs once for an admission transaction. Supplied
    /// IDs and ordering are preserved. Generated IDs never change this envelope.
    /// Context, state and frontend declarations stay on the envelope separately.
    /// This does not authorize roles, tool results, artifacts or execution.
    /// # Errors
    /// Rejects invalid session identity or projection inconsistency.
    pub fn materialize_messages(
        &self,
        session: &SessionId,
        now: OffsetDateTime,
    ) -> Result<Vec<Message>, CoreError> {
        if !validation::identity(&session.0, 512) {
            return Err(validation::invalid());
        }
        let messages = self
            .messages()
            .iter()
            .map(|input| {
                let id = input.id.clone().map_or_else(MessageId::new, MessageId);
                let role = input.role;
                let parts = input
                    .parts
                    .iter()
                    .enumerate()
                    .map(|(ordinal, input)| {
                        Ok(Part {
                            id: input.id.clone().map_or_else(PartId::new, PartId),
                            message_id: id.clone(),
                            ordinal: u32::try_from(ordinal).map_err(|_| validation::invalid())?,
                            kind: kind(role, &input.content),
                            content: content(&input.content),
                        })
                    })
                    .collect::<Result<Vec<_>, CoreError>>()?;
                Ok(Message {
                    id,
                    session_id: session.clone(),
                    run_id: None,
                    role: input.role,
                    parent_id: input.parent_id.clone().map(MessageId),
                    parts,
                    created_at: now,
                })
            })
            .collect::<Result<Vec<_>, CoreError>>()?;
        self.validate_materialized_messages(session, &messages)?;
        Ok(messages)
    }

    /// Independently verify that records preserve every supplied ID, role,
    /// parent, part and content block before granting admission. Generated IDs
    /// may differ on a retry but must be bounded, unique and correctly linked.
    /// # Errors
    /// Rejects altered content/scope/identity without echoing private input.
    pub fn validate_materialized_messages(
        &self,
        session: &SessionId,
        messages: &[Message],
    ) -> Result<(), CoreError> {
        self.validate_message_projection(session, None, messages)
    }

    /// Validate original input against either prepared or committed records.
    /// A run ID is correlation only; this check never grants execution authority.
    /// # Errors
    /// Rejects a different scope, provenance, ordering, or payload.
    pub fn validate_message_projection<'a>(
        &self,
        session: &SessionId,
        run: Option<&crate::RunId>,
        messages: impl IntoIterator<Item = &'a Message>,
    ) -> Result<(), CoreError> {
        let messages: Vec<_> = messages
            .into_iter()
            .take(super::MAX_INPUT_MESSAGES + 1)
            .collect();
        if !validation::identity(&session.0, 512) || messages.len() != self.messages().len() {
            return Err(validation::invalid());
        }
        let mut message_ids = BTreeSet::new();
        let mut part_ids = BTreeSet::new();
        for (input, message) in self.messages().iter().zip(messages) {
            if message.session_id != *session
                || message.run_id.as_ref() != run
                || !validation::identity(&message.id.0, 512)
                || !message_ids.insert(&message.id.0)
                || input.id.as_ref().is_some_and(|id| *id != message.id.0)
                || input.parent_id.as_deref() != message.parent_id.as_ref().map(|id| id.0.as_str())
                || input.role != message.role
                || input.parts.len() != message.parts.len()
            {
                return Err(validation::invalid());
            }
            for (ordinal, (input, part)) in input.parts.iter().zip(&message.parts).enumerate() {
                if part.message_id != message.id
                    || usize::try_from(part.ordinal).ok() != Some(ordinal)
                    || !validation::identity(&part.id.0, 512)
                    || !part_ids.insert(&part.id.0)
                    || input.id.as_ref().is_some_and(|id| *id != part.id.0)
                    || part.kind != kind(message.role, &input.content)
                    || part.content != content(&input.content)
                {
                    return Err(validation::invalid());
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AdmissionKind, InputMessage, InputPart, InputState, MediaKind, MediaSource,
        SessionRevision, ToolCallId,
    };
    use serde_json::json;

    fn fixture() -> InputEnvelope {
        InputEnvelope::new(
            vec![
                InputMessage {
                    id: Some("supplied-message".into()),
                    parent_id: Some("supplied-parent".into()),
                    role: Role::User,
                    parts: vec![
                        InputPart {
                            id: Some("supplied-text".into()),
                            content: InputContent::Content {
                                content: MessageContent::Text {
                                    text: "caption".into(),
                                },
                            },
                        },
                        InputPart {
                            id: None,
                            content: InputContent::Content {
                                content: MessageContent::Media {
                                    kind: MediaKind::Image,
                                    mime_type: "image/png".into(),
                                    source: MediaSource::File {
                                        file_id: "opaque-file".into(),
                                    },
                                },
                            },
                        },
                    ],
                },
                InputMessage {
                    id: None,
                    parent_id: Some("supplied-message".into()),
                    role: Role::Assistant,
                    parts: vec![InputPart {
                        id: None,
                        content: InputContent::ToolCall {
                            call_id: ToolCallId::from("call"),
                            name: "frontend".into(),
                            arguments: json!({"name":"value"}),
                        },
                    }],
                },
                InputMessage {
                    id: None,
                    parent_id: None,
                    role: Role::Tool,
                    parts: vec![InputPart {
                        id: None,
                        content: InputContent::ToolResult {
                            call_id: ToolCallId::from("call"),
                            is_error: true,
                            content: vec![
                                MessageContent::Text {
                                    text: "partial".into(),
                                },
                                MessageContent::Media {
                                    kind: MediaKind::Document,
                                    mime_type: "application/pdf".into(),
                                    source: MediaSource::Url {
                                        url: "https://example.invalid/private.pdf".into(),
                                    },
                                },
                            ],
                        },
                    }],
                },
            ],
            vec![],
            vec![],
            InputState::Absent,
            SessionRevision::INITIAL,
            AdmissionKind::Ordinary,
        )
        .unwrap()
    }
    #[test]
    fn materialization_preserves_order_supplied_provenance_and_typed_media() {
        let envelope = fixture();
        let original = serde_json::to_value(&envelope).unwrap();
        let session = SessionId::new();
        let first = envelope
            .materialize_messages(&session, OffsetDateTime::UNIX_EPOCH)
            .unwrap();
        let second = envelope
            .materialize_messages(&session, OffsetDateTime::UNIX_EPOCH)
            .unwrap();
        assert_eq!(first[0].id.0, "supplied-message");
        assert_eq!(first[0].parts[0].id.0, "supplied-text");
        assert_ne!(first[0].parts[1].id, second[0].parts[1].id);
        assert_ne!(first[1].id, second[1].id);
        assert_eq!(first[0].parent_id.as_ref().unwrap().0, "supplied-parent");
        assert_eq!(first[1].parent_id.as_ref().unwrap().0, "supplied-message");
        assert_eq!(
            first[0].parts[1].content,
            ContentBlock::MediaContent {
                kind: MediaKind::Image,
                mime_type: "image/png".into(),
                source: MediaSource::File {
                    file_id: "opaque-file".into()
                }
            }
        );
        assert!(
            matches!(&first[2].parts[0].content, ContentBlock::ToolResult { is_error: true, content, .. } if matches!(&content[1], ContentBlock::MediaContent { kind: MediaKind::Document, source: MediaSource::Url { url }, .. } if url == "https://example.invalid/private.pdf"))
        );
        assert_eq!(serde_json::to_value(&envelope).unwrap(), original);
        envelope
            .validate_materialized_messages(&session, &second)
            .unwrap();
        assert_eq!(
            serde_json::from_value::<Vec<Message>>(serde_json::to_value(&first).unwrap()).unwrap(),
            first
        );
    }
    #[test]
    fn projection_validation_rejects_record_tampering_before_admission() {
        let envelope = fixture();
        let session = SessionId::new();
        let original = envelope
            .materialize_messages(&session, OffsetDateTime::UNIX_EPOCH)
            .unwrap();
        for field in 0..9 {
            let mut records = original.clone();
            match field {
                0 => records[0].id = MessageId::new(),
                1 => records[0].role = Role::Assistant,
                2 => records[0].parent_id = None,
                3 => records[0].parts[0].ordinal = 1,
                4 => records[0].parts[0].message_id = MessageId::new(),
                5 => {
                    records[0].parts[0].content = ContentBlock::Text {
                        text: "changed".into(),
                    }
                }
                6 => records[0].session_id = SessionId::new(),
                7 => records[0].run_id = Some(crate::RunId::new()),
                8 => records[2].parts[0].id = records[1].parts[0].id.clone(),
                _ => unreachable!(),
            }
            assert_eq!(
                envelope
                    .validate_materialized_messages(&session, &records)
                    .unwrap_err(),
                validation::invalid()
            );
        }
    }
}
