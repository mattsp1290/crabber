//! Invocation ownership comes from the source writer, never an ambient child slot.
use ag_ui_core::event::{
    BaseEvent, Event, SubagentErrorEvent, SubagentFinishedEvent, SubagentStartedEvent,
};
use crabber_core::{
    EventKind, EventRecord, PublicInvocationEvent, PublicInvocationIdentity,
    PublicInvocationStatus, PublicSourceIdentity,
};

use super::{ProjectionError, Projector, base, json_bytes};

#[derive(Clone)]
pub(super) struct Invocation {
    identity: PublicInvocationIdentity,
    wire_id: String,
    closed: bool,
    depth: usize,
}

pub(super) struct InvocationUndo {
    invocations: std::collections::BTreeMap<String, Invocation>,
    messages: std::collections::BTreeMap<crabber_core::MessageId, super::presentation::UiMessage>,
    active: std::collections::BTreeMap<String, crabber_core::MessageId>,
}

fn is_invocation(kind: &EventKind) -> bool {
    matches!(
        kind,
        EventKind::InvocationStarted | EventKind::InvocationFinished | EventKind::InvocationError
    )
}

impl Projector {
    pub(super) fn child_exchange(&self) -> bool {
        self.binding.explicit
            && self
                .binding
                .source
                .as_ref()
                .is_some_and(|source| source.invocation.is_some())
    }
    pub(super) fn invocation_undo(
        &mut self,
        record: &EventRecord,
    ) -> Result<Option<InvocationUndo>, ProjectionError> {
        if !is_invocation(&record.kind) {
            return Ok(None);
        }
        if let Err(error) = json_bytes(&record.payload, self.config.max_event_bytes) {
            self.fail(error);
            return Err(error);
        }
        // Lifecycle closure can end several independently emitted presentation
        // slots. Retain bounded identity/counter metadata, never transcript text.
        Ok(Some(InvocationUndo {
            invocations: self.invocations.clone(),
            messages: self.ui_messages.clone(),
            active: self.ui_active.clone(),
        }))
    }

    pub(super) fn restore_invocation(&mut self, undo: Option<InvocationUndo>) {
        let Some(undo) = undo else {
            return;
        };
        self.invocations = undo.invocations;
        self.ui_messages = undo.messages;
        self.ui_active = undo.active;
    }

    pub(super) fn attributed_base(
        &self,
        source: &PublicSourceIdentity,
        mut base: BaseEvent,
    ) -> Result<BaseEvent, ProjectionError> {
        self.check_source_call(source)?;
        if let Some(identity) = &source.invocation {
            if self.binding.explicit
                && self
                    .binding
                    .source
                    .as_ref()
                    .and_then(|source| source.invocation.as_ref())
                    == Some(identity)
            {
                // A separately selected child exchange has its own root envelope.
                return Ok(base);
            }
            let actual = self
                .invocations
                .get(&identity.id.0)
                .filter(|actual| !actual.closed && actual.identity == *identity)
                .ok_or(ProjectionError::Identity)?;
            base.subagent_run_id = Some(actual.wire_id.clone());
        }
        Ok(base)
    }

    pub(super) fn check_source_call(
        &self,
        source: &PublicSourceIdentity,
    ) -> Result<(), ProjectionError> {
        if let Some(call) = &source.call_id
            && !self.calls.contains_key(call)
            && !(self.binding.explicit
                && self
                    .binding
                    .source
                    .as_ref()
                    .and_then(|source| source.call_id.as_ref())
                    == Some(call))
        {
            return Err(ProjectionError::Identity);
        }
        Ok(())
    }

    pub(super) fn project_invocation(
        &mut self,
        record: &EventRecord,
    ) -> Result<Option<Vec<Event>>, ProjectionError> {
        if !is_invocation(&record.kind) {
            return Ok(None);
        }
        let payload: PublicInvocationEvent = self.payload(record)?;
        if payload.version != 1 {
            return Err(ProjectionError::Malformed);
        }
        self.check_public_source(&payload.source)?;
        self.check_source_call(&payload.source)?;
        if self
            .public_state
            .revision
            .is_some_and(|previous| payload.revision.get() <= previous)
            || record
                .cursor
                .zip(self.public_state.cursor)
                .is_some_and(|(next, old)| next <= old)
        {
            return Err(ProjectionError::Order);
        }
        let identity = payload
            .source
            .invocation
            .as_ref()
            .ok_or(ProjectionError::Identity)?;
        if !valid(&identity.id.0) {
            return Err(ProjectionError::Identity);
        }
        let event_base = base(Some(record));
        let mut events = Vec::new();
        let bound_root = self
            .binding
            .source
            .as_ref()
            .and_then(|source| source.invocation.as_ref())
            .cloned();
        if self.child_exchange()
            && bound_root
                .as_ref()
                .is_some_and(|bound| bound.id == identity.id)
        {
            self.project_bound_invocation(record, &payload, &event_base, &mut events)?;
            return Ok(Some(events));
        }
        if self.child_exchange()
            && (!self.binding.child_started || self.binding.child_terminal.is_some())
        {
            return Err(ProjectionError::Order);
        }
        self.open_exchange(&event_base, &mut events);
        match (&record.kind, payload.status) {
            (EventKind::InvocationStarted, PublicInvocationStatus::Started) => {
                events.push(self.start_invocation(identity, bound_root.as_ref(), event_base)?);
            }
            (EventKind::InvocationFinished, PublicInvocationStatus::Finished)
            | (EventKind::InvocationError, PublicInvocationStatus::Error) => {
                self.finish_invocation(identity, payload.status, event_base, &mut events)?;
            }
            _ => return Err(ProjectionError::Malformed),
        }
        self.binding.source.get_or_insert(payload.source);
        self.public_state.revision = Some(payload.revision.get());
        self.public_state.cursor = record.cursor;
        self.public_state.snapshot = false;
        Ok(Some(events))
    }

    fn project_bound_invocation(
        &mut self,
        record: &EventRecord,
        payload: &PublicInvocationEvent,
        event_base: &BaseEvent,
        events: &mut Vec<Event>,
    ) -> Result<(), ProjectionError> {
        if self.binding.source.as_ref().is_none_or(|bound| {
            bound.invocation != payload.source.invocation || bound.call_id != payload.source.call_id
        }) {
            return Err(ProjectionError::Identity);
        }
        match (&record.kind, payload.status) {
            (EventKind::InvocationStarted, PublicInvocationStatus::Started)
                if !self.binding.child_started && self.binding.child_terminal.is_none() =>
            {
                self.binding.child_started = true;
                self.open_exchange(event_base, events);
            }
            (EventKind::InvocationFinished, PublicInvocationStatus::Finished)
                if self.binding.child_started && self.binding.child_terminal.is_none() =>
            {
                events.extend(self.close_presentations());
                events.extend(self.close_invocations());
                self.binding.child_terminal = Some(super::Completion::Completed);
            }
            (EventKind::InvocationError, PublicInvocationStatus::Error)
                if self.binding.child_started && self.binding.child_terminal.is_none() =>
            {
                events.extend(self.close_presentations());
                events.extend(self.close_invocations());
                self.binding.child_terminal = Some(super::Completion::Failed);
            }
            _ => return Err(ProjectionError::Order),
        }
        self.public_state.revision = Some(payload.revision.get());
        self.public_state.cursor = record.cursor;
        self.public_state.snapshot = false;
        Ok(())
    }

    fn start_invocation(
        &mut self,
        identity: &PublicInvocationIdentity,
        bound_root: Option<&PublicInvocationIdentity>,
        event_base: BaseEvent,
    ) -> Result<Event, ProjectionError> {
        if self.invocations.contains_key(&identity.id.0) {
            return Err(ProjectionError::Order);
        }
        if self.invocations.len() >= self.config.max_messages {
            return Err(ProjectionError::Limit);
        }
        let root_parent = self.child_exchange()
            && identity.parent_invocation.as_ref() == bound_root.map(|root| &root.id);
        if root_parent && identity.parent_generation != bound_root.map(|root| root.generation) {
            return Err(ProjectionError::Identity);
        }
        let parent = identity
            .parent_invocation
            .as_ref()
            .filter(|_| !root_parent)
            .map(|id| {
                self.invocations
                    .get(&id.0)
                    .filter(|parent| {
                        !parent.closed
                            && Some(parent.identity.generation) == identity.parent_generation
                    })
                    .ok_or(ProjectionError::Identity)
            })
            .transpose()?;
        let depth = parent.map_or(0, |parent| parent.depth + 1);
        let parent_wire = parent.map(|parent| parent.wire_id.clone());
        if let Some(call) = &identity.parent_call_id
            && !self.calls.contains_key(call)
        {
            return Err(ProjectionError::Identity);
        }
        let wire_id = format!(
            "crabber:invocation:{}:{}",
            identity.id,
            identity.generation.get()
        );
        let event = Event::SubagentStarted(SubagentStartedEvent {
            base: event_base,
            subagent_run_id: wire_id.clone(),
            name: "crabber.invocation".into(),
            description: None,
            parent_subagent_run_id: parent_wire,
            parent_message_id: None,
            parent_tool_call_id: identity
                .parent_call_id
                .as_ref()
                .map(|id| id.0.clone().into()),
        });
        self.invocations.insert(
            identity.id.0.clone(),
            Invocation {
                identity: identity.clone(),
                wire_id,
                closed: false,
                depth,
            },
        );
        Ok(event)
    }

    fn finish_invocation(
        &mut self,
        identity: &PublicInvocationIdentity,
        status: PublicInvocationStatus,
        event_base: BaseEvent,
        events: &mut Vec<Event>,
    ) -> Result<(), ProjectionError> {
        let actual = self
            .invocations
            .get(&identity.id.0)
            .filter(|actual| !actual.closed && actual.identity == *identity)
            .ok_or(ProjectionError::Order)?;
        let wire_id = actual.wire_id.clone();
        let descendants = self.descendant_presentations(&identity.id.0);
        events.extend(self.close_selected_presentations(&descendants));
        events.extend(self.close_descendant_invocations(&identity.id.0));
        events.extend(self.close_selected_presentations(std::slice::from_ref(&wire_id)));
        self.invocations
            .get_mut(&identity.id.0)
            .ok_or(ProjectionError::Order)?
            .closed = true;
        events.push(if status == PublicInvocationStatus::Finished {
            Event::SubagentFinished(SubagentFinishedEvent {
                base: event_base,
                subagent_run_id: wire_id,
                outcome: Some(ag_ui_core::types::SubagentFinishedOutcome::Success),
                result: None,
            })
        } else {
            Event::SubagentError(SubagentErrorEvent {
                base: event_base,
                subagent_run_id: wire_id,
                message: "The invocation failed.".into(),
                code: Some("crabber_invocation_failed".into()),
            })
        });
        Ok(())
    }

    fn descendant_presentations(&self, parent: &str) -> Vec<String> {
        self.invocations
            .values()
            .filter(|candidate| {
                !candidate.closed && self.is_descendant(&candidate.identity.id.0, parent)
            })
            .map(|candidate| candidate.wire_id.clone())
            .collect()
    }

    fn is_descendant(&self, id: &str, parent: &str) -> bool {
        let mut next = self
            .invocations
            .get(id)
            .and_then(|item| item.identity.parent_invocation.as_ref());
        for _ in 0..self.invocations.len() {
            let Some(current) = next else {
                return false;
            };
            if current.0 == parent {
                return true;
            }
            next = self
                .invocations
                .get(&current.0)
                .and_then(|item| item.identity.parent_invocation.as_ref());
        }
        false
    }

    fn close_descendant_invocations(&mut self, parent: &str) -> Vec<Event> {
        let mut selected: Vec<_> = self
            .invocations
            .iter()
            .filter(|(_, item)| !item.closed && self.is_descendant(&item.identity.id.0, parent))
            .map(|(id, item)| (item.depth, id.clone()))
            .collect();
        selected.sort_by_key(|item| std::cmp::Reverse(item.0));
        selected
            .into_iter()
            .map(|(_, id)| {
                let invocation = self.invocations.get_mut(&id).expect("selected descendant");
                invocation.closed = true;
                scope_closed(invocation.wire_id.clone())
            })
            .collect()
    }

    pub(super) fn invocations_open(&self) -> bool {
        self.invocations
            .values()
            .any(|invocation| !invocation.closed)
    }

    pub(super) fn close_invocations(&mut self) -> Vec<Event> {
        let mut active: Vec<_> = self
            .invocations
            .iter()
            .filter(|(_, invocation)| !invocation.closed)
            .map(|(id, invocation)| (invocation.depth, id.clone()))
            .collect();
        active.sort_by_key(|item| std::cmp::Reverse(item.0));
        let mut events = Vec::new();
        for (_, id) in active {
            let invocation = self.invocations.get_mut(&id).expect("selected invocation");
            invocation.closed = true;
            events.push(Event::SubagentError(SubagentErrorEvent {
                base: base(None),
                subagent_run_id: invocation.wire_id.clone(),
                message: "Invocation presentation closed with its parent stream.".into(),
                code: Some("crabber_projection_scope_closed".into()),
            }));
        }
        events
    }
}

fn valid(id: &str) -> bool {
    !id.is_empty() && id.len() <= 128 && !id.chars().any(char::is_control)
}

fn scope_closed(wire_id: String) -> Event {
    Event::SubagentError(SubagentErrorEvent { base: base(None), subagent_run_id: wire_id, message: "Invocation presentation closed with its parent scope; execution is independently observed.".into(), code: Some("crabber_projection_scope_closed".into()) })
}
