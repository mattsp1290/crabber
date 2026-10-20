//! Text and explicitly public reasoning deltas share bounded message accounting.
use super::{ProjectionError, Projector};
use crate::sse::json_bytes;
use ag_ui_core::event::{
    BaseEvent, Event, ReasoningMessageContentEvent, ReasoningMessageStartEvent,
    ReasoningStartEvent, TextMessageContentEvent,
};
use ag_ui_core::types::Role;
use crabber_core::event_payload::MessageDelta;
use crabber_core::{EventKind, EventRecord, MessageId, ReasoningProvenance};
use serde_json::Value;

impl Projector {
    pub(super) fn project_text_delta(
        &mut self,
        record: &EventRecord,
        base: BaseEvent,
    ) -> Result<Vec<Event>, ProjectionError> {
        let mut output = Vec::new();
        // Check field size before deserializing/copying it.
        let text = record
            .payload
            .get("text")
            .and_then(Value::as_str)
            .ok_or(ProjectionError::Malformed)?;
        let reasoning = record.kind == EventKind::ReasoningDelta;
        if text.len() > self.config.max_text_bytes {
            return Err(ProjectionError::Limit);
        }
        let (payload, item_id) = if reasoning {
            json_bytes(&record.payload, self.config.max_event_bytes)?;
            let message_id = record
                .payload
                .get("message_id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty() && id.len() <= 512 && !id.chars().any(char::is_control))
                .ok_or(ProjectionError::Malformed)?;
            match record.payload.get("provenance").and_then(Value::as_str) {
                Some("public_summary") => {
                    let (payload, item_id) = self.read_public_reasoning(record)?;
                    (payload, Some(item_id))
                }
                None | Some("private" | "unspecified") => {
                    let limit = self.config.max_text_bytes;
                    let message = self.active_message(&MessageId::from(message_id))?;
                    message.reasoning_bytes = message
                        .reasoning_bytes
                        .checked_add(text.len())
                        .filter(|bytes| *bytes <= limit)
                        .ok_or(ProjectionError::Limit)?;
                    return Ok(output);
                }
                Some(_) => return Err(ProjectionError::Malformed),
            }
        } else {
            (self.payload::<MessageDelta>(record)?, None)
        };
        let limit = self.config.max_text_bytes;
        let show_reasoning = self.config.reasoning;
        let reasoning_limit = self.config.max_messages.saturating_sub(
            self.messages
                .values()
                .map(|message| message.reasoning_items.len())
                .sum(),
        );
        let message = self.active_message(&payload.message_id)?;
        let bytes = if reasoning {
            &mut message.reasoning_bytes
        } else {
            &mut message.text_bytes
        };
        *bytes = bytes
            .checked_add(text.len())
            .filter(|n| *n <= limit)
            .ok_or(ProjectionError::Limit)?;
        if text.is_empty() || (reasoning && !show_reasoning) {
            return Ok(output);
        }
        if reasoning {
            let item_id = item_id.ok_or(ProjectionError::Malformed)?;
            let id = format!(
                "crabber:reasoning:{}:{}:{item_id}",
                payload.message_id.0.len(),
                payload.message_id
            );
            if !message.reasoning_items.contains(&id) {
                if reasoning_limit == 0 {
                    return Err(ProjectionError::Limit);
                }
                message.reasoning_items.insert(id.clone());
                message.reasoning_public = true;
                message.public = true;
                output.push(Event::ReasoningStart(ReasoningStartEvent {
                    base: base.clone(),
                    message_id: id.clone().into(),
                }));
                output.push(Event::ReasoningMessageStart(ReasoningMessageStartEvent {
                    base: base.clone(),
                    message_id: id.clone().into(),
                    role: Role::Reasoning,
                }));
            }
            output.push(Event::ReasoningMessageContent(
                ReasoningMessageContentEvent {
                    base,
                    message_id: id.into(),
                    delta: payload.text,
                },
            ));
        } else {
            self.open_text(&payload.message_id, &base, &mut output)?;
            output.push(Event::TextMessageContent(TextMessageContentEvent {
                base,
                message_id: payload.message_id.to_string().into(),
                delta: payload.text,
            }));
        }
        Ok(output)
    }

    fn read_public_reasoning(
        &self,
        record: &EventRecord,
    ) -> Result<(MessageDelta, String), ProjectionError> {
        let public: crabber_core::event_payload::ReasoningDelta = self.payload(record)?;
        if public.provenance != ReasoningProvenance::PublicSummary
            || public.item_id.is_empty()
            || public.item_id.len() > 1024
            || public.item_id.chars().any(char::is_control)
        {
            return Err(ProjectionError::Malformed);
        }
        Ok((
            MessageDelta {
                message_id: public.message_id,
                text: public.text,
            },
            public.item_id,
        ))
    }
}
