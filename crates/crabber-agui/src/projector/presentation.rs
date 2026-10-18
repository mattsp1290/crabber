//! Source-attributed public presentation has independent bounded message slots.
use ag_ui_core::event::{
    BaseEvent, CustomEvent, Event, MessagesSnapshotEvent, RawEvent, TextMessageContentEvent,
    TextMessageEndEvent, TextMessageStartEvent,
};
use ag_ui_core::types::{Message, Role};
use crabber_core::{
    ContentBlock, EventKind, EventRecord, MessageId, PublicPresentationChange,
    PublicSourceIdentity, PublicSourceMessage,
};

use super::{ProjectionError, Projector, json_bytes};

#[derive(Clone)]
pub(super) struct UiMessage {
    owner: String,
    bytes: usize,
    pub invocation: Option<String>,
    open: bool,
}

pub(super) struct PresentationUndo {
    owner: String,
    active: Option<MessageId>,
    messages: Vec<(MessageId, Option<UiMessage>)>,
}

fn owner(source: &PublicSourceIdentity) -> String {
    serde_json::to_string(&(&source.invocation, &source.call_id))
        .expect("source identities serialize")
}

impl Projector {
    pub(super) fn presentation_undo(
        &mut self,
        record: &EventRecord,
    ) -> Result<Option<PresentationUndo>, ProjectionError> {
        if record.kind != EventKind::PublicPresentation {
            return Ok(None);
        }
        if let Err(error) = json_bytes(&record.payload, self.config.max_event_bytes) {
            self.fail(error);
            return Err(error);
        }
        let source: PublicSourceIdentity = serde_json::from_value(
            record
                .payload
                .get("source")
                .cloned()
                .ok_or(ProjectionError::Malformed)?,
        )
        .map_err(|_| ProjectionError::Malformed)?;
        let owner = owner(&source);
        let mut messages = Vec::new();
        if let Some(id) = record
            .payload
            .pointer("/change/presentation/message_id")
            .and_then(serde_json::Value::as_str)
            .filter(|id| valid(id))
        {
            let id = MessageId::from(id);
            messages.push((id.clone(), self.ui_messages.get(&id).cloned()));
        }
        if let Some(items) = record
            .payload
            .pointer("/change/presentation/messages")
            .and_then(serde_json::Value::as_array)
        {
            if items.len() > self.config.max_messages {
                self.fail(ProjectionError::Limit);
                return Err(ProjectionError::Limit);
            }
            for item in items {
                if let Some(id) = item
                    .get("message_id")
                    .and_then(serde_json::Value::as_str)
                    .filter(|id| valid(id))
                {
                    let id = MessageId::from(id);
                    messages.push((id.clone(), self.ui_messages.get(&id).cloned()));
                }
            }
        }
        Ok(Some(PresentationUndo {
            active: self.ui_active.get(&owner).cloned(),
            owner,
            messages,
        }))
    }

    pub(super) fn restore_presentation(&mut self, undo: Option<PresentationUndo>) {
        let Some(undo) = undo else {
            return;
        };
        if let Some(active) = undo.active {
            self.ui_active.insert(undo.owner, active);
        } else {
            self.ui_active.remove(&undo.owner);
        }
        for (id, old) in undo.messages {
            if let Some(old) = old {
                self.ui_messages.insert(id, old);
            } else {
                self.ui_messages.remove(&id);
            }
        }
    }

    pub(super) fn project_presentation(
        &mut self,
        source: &PublicSourceIdentity,
        change: &PublicPresentationChange,
        base: BaseEvent,
    ) -> Result<Vec<Event>, ProjectionError> {
        let key = owner(source);
        let mut events = Vec::new();
        match change {
            PublicPresentationChange::MessageStarted { message_id, role } => {
                events.push(self.start_presentation(key, message_id, *role, base)?);
            }
            PublicPresentationChange::TextDelta { message_id, text } => {
                if self.ui_active.get(&key) != Some(message_id) {
                    return Err(ProjectionError::Order);
                }
                let message = self
                    .ui_messages
                    .get_mut(message_id)
                    .filter(|message| message.open && message.owner == key)
                    .ok_or(ProjectionError::Order)?;
                message.bytes = message
                    .bytes
                    .checked_add(text.len())
                    .filter(|bytes| *bytes <= self.config.max_text_bytes)
                    .ok_or(ProjectionError::Limit)?;
                if !text.is_empty() {
                    events.push(Event::TextMessageContent(TextMessageContentEvent {
                        base,
                        message_id: message_id.0.clone().into(),
                        delta: text.clone(),
                    }));
                }
            }
            PublicPresentationChange::MessageEnded { message_id } => {
                if self.ui_active.get(&key) != Some(message_id) {
                    return Err(ProjectionError::Order);
                }
                self.ui_messages
                    .get_mut(message_id)
                    .filter(|message| message.open && message.owner == key)
                    .ok_or(ProjectionError::Order)?
                    .open = false;
                self.ui_active.remove(&key);
                events.push(Event::TextMessageEnd(TextMessageEndEvent {
                    base,
                    message_id: message_id.0.clone().into(),
                }));
            }
            PublicPresentationChange::MessagesSnapshot { messages } => {
                events.push(self.messages_snapshot(source, messages, base)?);
            }
            PublicPresentationChange::ToolProgress { content } => {
                let call = source.call_id.as_ref().ok_or(ProjectionError::Identity)?;
                events.extend(self.progress_event(call, content, base)?);
            }
            PublicPresentationChange::Raw { source, event } => {
                if !self.config.raw_sources.contains(source) {
                    return Err(ProjectionError::Malformed);
                }
                events.push(Event::Raw(RawEvent {
                    base,
                    source: Some(source.clone()),
                    event: event.clone(),
                }));
            }
            PublicPresentationChange::Custom { name, value } => {
                if !self.config.custom_events.contains(name) {
                    return Err(ProjectionError::Malformed);
                }
                events.push(Event::Custom(CustomEvent {
                    base,
                    name: name.clone(),
                    value: value.clone(),
                }));
            }
        }
        Ok(events)
    }

    fn start_presentation(
        &mut self,
        key: String,
        message_id: &MessageId,
        role: crabber_core::Role,
        base: BaseEvent,
    ) -> Result<Event, ProjectionError> {
        if !(self.lifecycle.source_started
            || self.resume.ready
            || self.child_exchange()
                && self.binding.child_started
                && self.binding.child_terminal.is_none())
            || self.ui_active.contains_key(&key)
            || self.message_known(message_id)
        {
            return Err(ProjectionError::Order);
        }
        if !valid(&message_id.0) {
            return Err(ProjectionError::Identity);
        }
        if self.ui_messages.len() + self.messages.len() >= self.config.max_messages {
            return Err(ProjectionError::Limit);
        }
        let role = wire_role(role)?;
        self.ui_active.insert(key.clone(), message_id.clone());
        self.ui_messages.insert(
            message_id.clone(),
            UiMessage {
                owner: key,
                bytes: 0,
                invocation: base.subagent_run_id.clone(),
                open: true,
            },
        );
        Ok(Event::TextMessageStart(TextMessageStartEvent {
            base,
            message_id: message_id.0.clone().into(),
            role: Some(role),
            name: None,
        }))
    }

    pub(super) fn local_progress(
        &self,
        record: &EventRecord,
        base: BaseEvent,
    ) -> Result<Vec<Event>, ProjectionError> {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Progress {
            version: u32,
            call_id: crabber_core::ToolCallId,
            content: ContentBlock,
        }
        let payload: Progress = self.payload(record)?;
        if payload.version != 1
            || !record.live_only
            || record.correlation.as_deref() != Some(&payload.call_id.0)
        {
            return Err(ProjectionError::Malformed);
        }
        self.progress_event(&payload.call_id, &payload.content, base)
    }

    fn progress_event(
        &self,
        call: &crabber_core::ToolCallId,
        content: &ContentBlock,
        base: BaseEvent,
    ) -> Result<Vec<Event>, ProjectionError> {
        if self.calls.get(call).is_none_or(|call| call.settled) {
            return Err(ProjectionError::Identity);
        }
        let content =
            super::content::tool_content(std::slice::from_ref(content), self.config.reasoning)?;
        if matches!(&content, ag_ui_core::types::MessageContent::Parts(parts) if parts.is_empty()) {
            return Ok(Vec::new());
        }
        Ok(vec![Event::Custom(CustomEvent {
            base,
            name: "crabber.tool_progress".into(),
            value: serde_json::json!({"callId":call,"content":content}),
        })])
    }

    fn message_known(&self, id: &MessageId) -> bool {
        self.messages.contains_key(id)
            || self.ui_messages.contains_key(id)
            || self.result_ids.contains(id)
            || self.activities.contains(&id.0)
            || self.public_state.activities.contains_key(&id.0)
    }

    pub(super) fn close_selected_presentations(&mut self, scopes: &[String]) -> Vec<Event> {
        let selected: Vec<_> = self
            .ui_active
            .iter()
            .filter(|(_, id)| {
                self.ui_messages.get(*id).is_some_and(|message| {
                    message
                        .invocation
                        .as_ref()
                        .is_some_and(|scope| scopes.contains(scope))
                })
            })
            .map(|(owner, id)| (owner.clone(), id.clone()))
            .collect();
        let mut events = Vec::new();
        for (owner, id) in selected {
            self.ui_active.remove(&owner);
            let message = self
                .ui_messages
                .get_mut(&id)
                .expect("selected presentation");
            message.open = false;
            let mut base = super::base(None);
            base.subagent_run_id.clone_from(&message.invocation);
            events.push(Event::TextMessageEnd(TextMessageEndEvent {
                base,
                message_id: id.0.into(),
            }));
        }
        events
    }

    pub(super) fn close_presentations(&mut self) -> Vec<Event> {
        let mut events = Vec::new();
        for (_, id) in std::mem::take(&mut self.ui_active) {
            if let Some(message) = self.ui_messages.get_mut(&id) {
                message.open = false;
                let mut base = super::base(None);
                base.subagent_run_id.clone_from(&message.invocation);
                events.push(Event::TextMessageEnd(TextMessageEndEvent {
                    base,
                    message_id: id.0.into(),
                }));
            }
        }
        events
    }

    fn messages_snapshot(
        &mut self,
        source: &PublicSourceIdentity,
        messages: &[PublicSourceMessage],
        base: BaseEvent,
    ) -> Result<Event, ProjectionError> {
        let bound = self.binding.explicit
            && self.binding.source.as_ref().is_some_and(|bound| {
                bound.call_id == source.call_id && bound.invocation == source.invocation
            });
        if (source.call_id.is_some() || source.invocation.is_some()) && !bound {
            return Err(ProjectionError::Identity);
        }
        if self.active.is_some()
            || !self.ui_active.is_empty()
            || self.step.is_some()
            || self.invocations_open()
            || !self.calls.is_empty()
            || !self.result_ids.is_empty()
            || self
                .messages
                .values()
                .any(|message| message.reasoning_public)
            || self
                .ui_messages
                .values()
                .any(|message| message.invocation.is_some())
        {
            return Err(ProjectionError::Order);
        }
        if messages.len() + self.activities.len() + self.public_state.activities.len()
            > self.config.max_messages
        {
            return Err(ProjectionError::Limit);
        }
        let mut ids = std::collections::BTreeSet::new();
        let mut output = Vec::new();
        for message in messages {
            if !valid(&message.message_id.0)
                || !ids.insert(&message.message_id)
                || self.activities.contains(&message.message_id.0)
                || self
                    .public_state
                    .activities
                    .contains_key(&message.message_id.0)
            {
                return Err(ProjectionError::Identity);
            }
            if message.text.len() > self.config.max_text_bytes {
                return Err(ProjectionError::Limit);
            }
            output.push(Message::new(
                wire_role(message.role)?,
                message.message_id.0.clone(),
                &message.text,
            ));
        }
        self.preserve_snapshot_activities(&mut output)?;
        let new_ids = messages
            .iter()
            .filter(|message| {
                !self.ui_messages.contains_key(&message.message_id)
                    && !self.messages.contains_key(&message.message_id)
            })
            .count();
        if self.ui_messages.len() + self.messages.len() + new_ids > self.config.max_messages {
            return Err(ProjectionError::Limit);
        }
        let key = owner(source);
        for message in messages {
            if !self.messages.contains_key(&message.message_id) {
                self.ui_messages.insert(
                    message.message_id.clone(),
                    UiMessage {
                        owner: key.clone(),
                        bytes: message.text.len(),
                        invocation: None,
                        open: false,
                    },
                );
            }
        }
        Ok(Event::MessagesSnapshot(MessagesSnapshotEvent {
            base,
            messages: output,
        }))
    }
    // SDK snapshots replace the global message set. Retain current activity
    // messages so a later exact activity delta still has its real base/owner.
    fn preserve_snapshot_activities(
        &self,
        output: &mut Vec<Message>,
    ) -> Result<(), ProjectionError> {
        for activity in self.native_activities.values() {
            output.push(Message::Activity {
                id: activity.message_id.clone(),
                activity_type: activity.activity_type.clone(),
                content: activity.content.clone(),
                metadata: None,
                subagent_run_id: activity.base.subagent_run_id.clone(),
            });
        }
        if let Some(contents) = &self.public_state.contents {
            for activity in contents.activities() {
                output.push(Message::Activity {
                    id: activity.id().into(),
                    activity_type: activity.activity_type().into(),
                    content: activity
                        .content()
                        .as_object()
                        .ok_or(ProjectionError::Malformed)?
                        .clone(),
                    metadata: self
                        .public_state
                        .activity_metadata
                        .get(activity.id())
                        .cloned(),
                    subagent_run_id: None,
                });
            }
        }
        Ok(())
    }
}

fn wire_role(role: crabber_core::Role) -> Result<Role, ProjectionError> {
    match role {
        crabber_core::Role::System => Ok(Role::System),
        crabber_core::Role::User => Ok(Role::User),
        crabber_core::Role::Assistant => Ok(Role::Assistant),
        crabber_core::Role::Tool => Err(ProjectionError::Malformed),
    }
}
fn valid(id: &str) -> bool {
    !id.is_empty() && id.len() <= 512 && !id.chars().any(char::is_control)
}
