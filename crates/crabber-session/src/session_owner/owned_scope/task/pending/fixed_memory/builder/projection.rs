//! Borrowed wire projection before Core copies any input body. Generated UUIDs
//! have exactly this 36-byte unescaped spelling length; supplied IDs stay exact.
use super::{PreparationCommitError, Proof};
use crabber_core::{
    InputContent, InputEnvelope, InputMessage, InputPart, Message, MessageContent, PartKind, Role,
    SessionId,
};
use serde::{Serialize, Serializer, ser::SerializeSeq};
use time::OffsetDateTime;
const GENERATED_ID: &str = "00000000-0000-0000-0000-000000000000";

pub(super) fn materialize(
    input: &InputEnvelope,
    session: &SessionId,
    now: OffsetDateTime,
    proof: &mut Proof,
) -> Result<Vec<Message>, PreparationCommitError> {
    precharge(input, session, now, proof)?;
    Ok(input.materialize_messages(session, now)?)
}
fn precharge(
    input: &InputEnvelope,
    session: &SessionId,
    now: OffsetDateTime,
    proof: &mut Proof,
) -> Result<(), PreparationCommitError> {
    proof
        .measure(&Messages {
            input,
            session,
            now,
        })
        .map_err(|_| crate::session_owner::SessionOwnerError::Capacity)?;
    // Core's validate_materialized_messages independently constructs each
    // ContentBlock once more for equality. Charge that temporary carrier too.
    for message in input.messages() {
        for part in &message.parts {
            proof
                .measure(&Content::from(&part.content))
                .map_err(|_| crate::session_owner::SessionOwnerError::Capacity)?;
        }
    }
    Ok(())
}
struct Messages<'a> {
    input: &'a InputEnvelope,
    session: &'a SessionId,
    now: OffsetDateTime,
}
impl Serialize for Messages<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.input.messages().len()))?;
        for input in self.input.messages() {
            let id = input.id.as_deref().unwrap_or(GENERATED_ID);
            sequence.serialize_element(&MessageView {
                id,
                session_id: self.session,
                run_id: None,
                role: input.role,
                parent_id: &input.parent_id,
                parts: Parts { input, id },
                created_at: self.now,
            })?;
        }
        sequence.end()
    }
}
#[derive(Serialize)]
struct MessageView<'a> {
    id: &'a str,
    session_id: &'a SessionId,
    run_id: Option<()>,
    role: Role,
    parent_id: &'a Option<String>,
    parts: Parts<'a>,
    created_at: OffsetDateTime,
}
struct Parts<'a> {
    input: &'a InputMessage,
    id: &'a str,
}
impl Serialize for Parts<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.input.parts.len()))?;
        for (ordinal, input) in self.input.parts.iter().enumerate() {
            sequence.serialize_element(&PartView {
                id: input.id.as_deref().unwrap_or(GENERATED_ID),
                message_id: self.id,
                ordinal,
                kind: kind(self.input.role, input),
                content: Content::from(&input.content),
            })?;
        }
        sequence.end()
    }
}
#[derive(Serialize)]
struct PartView<'a> {
    id: &'a str,
    message_id: &'a str,
    ordinal: usize,
    kind: PartKind,
    content: Content<'a>,
}
fn kind(role: Role, input: &InputPart) -> PartKind {
    match &input.content {
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
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Content<'a> {
    Text {
        text: &'a str,
    },
    #[serde(rename = "media_content")]
    Media {
        kind: crabber_core::MediaKind,
        mime_type: &'a str,
        source: &'a crabber_core::MediaSource,
    },
    ToolCall {
        call_id: &'a crabber_core::ToolCallId,
        name: &'a str,
        arguments: &'a serde_json::Value,
    },
    ToolResult {
        call_id: &'a crabber_core::ToolCallId,
        content: Contents<'a>,
        is_error: bool,
    },
}
impl<'a> From<&'a MessageContent> for Content<'a> {
    fn from(content: &'a MessageContent) -> Self {
        match content {
            MessageContent::Text { text } => Self::Text { text },
            MessageContent::Media {
                kind,
                mime_type,
                source,
            } => Self::Media {
                kind: *kind,
                mime_type,
                source,
            },
        }
    }
}
impl<'a> From<&'a InputContent> for Content<'a> {
    fn from(input: &'a InputContent) -> Self {
        match input {
            InputContent::Content { content } => Self::from(content),
            InputContent::ToolCall {
                call_id,
                name,
                arguments,
            } => Self::ToolCall {
                call_id,
                name,
                arguments,
            },
            InputContent::ToolResult {
                call_id,
                content,
                is_error,
            } => Self::ToolResult {
                call_id,
                content: Contents(content),
                is_error: *is_error,
            },
        }
    }
}
struct Contents<'a>(&'a [MessageContent]);
impl Serialize for Contents<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for content in self.0 {
            sequence.serialize_element(&Content::from(content))?;
        }
        sequence.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_projection_charge_precedes_original_materialization() {
        let input = InputEnvelope::text(
            "private projection body",
            crabber_core::SessionRevision::INITIAL,
        )
        .unwrap();
        let session = SessionId::new();
        let now = OffsetDateTime::UNIX_EPOCH;
        let mut measured = Proof::bounded(32 << 20);
        precharge(&input, &session, now, &mut measured).unwrap();
        let n = measured.used();
        let result = materialize(&input, &session, now, &mut Proof::bounded(n)).unwrap();
        assert_eq!(
            serde_json::to_vec(&Messages {
                input: &input,
                session: &session,
                now
            })
            .unwrap()
            .len(),
            serde_json::to_vec(&result).unwrap().len()
        );
        let mut short = Proof::bounded(n - 1);
        assert!(materialize(&input, &session, now, &mut short).is_err());
        assert!(
            short.charge(0).is_err(),
            "refusal remains sticky before any native I/O"
        );
    }
}
