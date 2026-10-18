//! Native committed continuation facts, never browser or metadata permissions.
use std::sync::Arc;

use ag_ui_core::{
    event::{BaseEvent, Event, StateSnapshotEvent, StepStartedEvent},
    types::{Interrupt, RunFinishedOutcome},
};
use crabber_core::{
    EventKind, EventRecord, MessageId, ToolCallId,
    continuation_source::{AttemptIdentity, ResumeMode, ResumeSource, WaitingSource},
};

use super::{CallState, MessagePhase, MessageState, ProjectionError, Projector, json_bytes};

pub(super) struct ContinuationUndo {
    ready: bool,
    seen: bool,
    attempt: Option<AttemptIdentity>,
    waiting: Option<Arc<WaitingSource>>,
    calls: Vec<(ToolCallId, Option<CallState>)>,
    parents: Vec<(MessageId, Option<MessageState>)>,
}

impl Projector {
    pub(super) fn continuation_undo(
        &mut self,
        record: &EventRecord,
    ) -> Result<Option<ContinuationUndo>, ProjectionError> {
        if !matches!(
            record.kind,
            EventKind::RunResumed | EventKind::ContinuationWaiting
        ) {
            return Ok(None);
        }
        if let Err(error) = json_bytes(&record.payload, self.config.max_event_bytes) {
            self.fail(error);
            return Err(error);
        }
        let mut calls = Vec::new();
        let mut parents = Vec::new();
        if let Some(items) = record
            .payload
            .get("calls")
            .and_then(serde_json::Value::as_array)
        {
            if items.len() > self.config.max_open_calls {
                return Err(ProjectionError::Limit);
            }
            for item in items {
                if let Some(id) = item
                    .get("call_id")
                    .and_then(serde_json::Value::as_str)
                    .filter(|id| id.len() <= 512)
                {
                    let id = ToolCallId::from(id);
                    calls.push((id.clone(), self.calls.get(&id).cloned()));
                }
                if let Some(id) = item
                    .get("message_id")
                    .and_then(serde_json::Value::as_str)
                    .filter(|id| id.len() <= 512)
                {
                    let id = MessageId::from(id);
                    parents.push((id.clone(), self.messages.get(&id).cloned()));
                }
            }
        }
        Ok(Some(ContinuationUndo {
            ready: self.resume.ready,
            seen: self.resume.seen,
            attempt: self.resume.attempt.clone(),
            waiting: self.resume.waiting.clone(),
            calls,
            parents,
        }))
    }

    pub(super) fn restore_continuation(&mut self, undo: Option<ContinuationUndo>) {
        let Some(undo) = undo else {
            return;
        };
        self.resume.ready = undo.ready;
        self.resume.seen = undo.seen;
        self.resume.attempt = undo.attempt;
        self.resume.waiting = undo.waiting;
        for (id, old) in undo.calls {
            if let Some(old) = old {
                self.calls.insert(id, old);
            } else {
                self.calls.remove(&id);
            }
        }
        for (id, old) in undo.parents {
            if let Some(old) = old {
                self.messages.insert(id, old);
            } else {
                self.messages.remove(&id);
            }
        }
    }

    fn continuation_scope(
        &self,
        session: &crabber_core::SessionId,
        run: &crabber_core::RunId,
        attempt: &AttemptIdentity,
    ) -> Result<(), ProjectionError> {
        if session != &self.session
            || run != &self.run
            || self
                .resume
                .attempt
                .as_ref()
                .is_some_and(|expected| expected != attempt)
            || self.binding.source.as_ref().is_some_and(|expected| {
                expected.attempt.id != attempt.id
                    || expected.attempt.generation != attempt.generation
            })
        {
            return Err(ProjectionError::Identity);
        }
        Ok(())
    }

    pub(super) fn project_resume(
        &mut self,
        record: &EventRecord,
        base: BaseEvent,
    ) -> Result<Vec<Event>, ProjectionError> {
        let mut events = Vec::new();
        if record.payload.is_null() {
            return Ok(events);
        }
        let source: ResumeSource = self.payload(record)?;
        source.validate().map_err(|_| ProjectionError::Malformed)?;
        self.continuation_scope(&source.session_id, &source.run_id, &source.attempt)?;
        if self.resume.seen
            || self.lifecycle.source_started
            || self.active.is_some()
            || !self.calls.is_empty()
            || !self.ui_active.is_empty()
        {
            return Err(ProjectionError::Order);
        }
        if source.calls.len() > self.config.max_open_calls
            || source.calls.len() > self.config.max_messages
        {
            return Err(ProjectionError::Limit);
        }
        if source.mode == ResumeMode::NativeUnstarted
            && (!source.calls.is_empty()
                || source.current_turn.is_some()
                || source.continuation.is_some())
        {
            return Err(ProjectionError::Malformed);
        }
        self.open_exchange(&base, &mut events);
        let mut state_base = base.clone();
        state_base.metadata = Some(serde_json::Map::from_iter([(
            "crabber".into(),
            serde_json::json!({"source":{"runId":source.run_id,"attempt":source.attempt},"continuation":source.continuation}),
        )]));
        events.push(Event::StateSnapshot(StateSnapshotEvent {
            base: state_base.clone(),
            snapshot: source.contents.state().clone(),
        }));
        self.complete_activities(&state_base, &source.contents, &mut events)?;
        for call in &source.calls {
            if self.messages.contains_key(&call.message_id)
                || self.ui_messages.contains_key(&call.message_id)
                || self.activities.contains(&call.message_id.0)
                || self
                    .public_state
                    .activities
                    .contains_key(&call.message_id.0)
                || self.result_ids.contains(&call.message_id)
            {
                // Several frozen calls may share one actual committed parent.
                if !self
                    .messages
                    .get(&call.message_id)
                    .is_some_and(|parent| parent.phase == MessagePhase::Committed)
                {
                    return Err(ProjectionError::Identity);
                }
            }
            let bytes = json_bytes(&call.arguments, self.config.max_argument_bytes)?;
            self.messages
                .entry(call.message_id.clone())
                .or_insert_with(|| MessageState {
                    phase: MessagePhase::Committed,
                    ..MessageState::default()
                });
            self.calls.insert(
                call.call_id.clone(),
                CallState {
                    parent: call.message_id.clone(),
                    ended: true,
                    settled: false,
                    bytes,
                },
            );
        }
        self.resume.seen = true;
        self.resume.ready = source.mode == ResumeMode::Continued;
        if self.resume.ready
            && let Some(turn) = &source.current_turn
        {
            if self.step.is_some() {
                return Err(ProjectionError::Order);
            }
            let step_name = format!("turn:{turn}");
            self.step = Some(step_name.clone());
            events.push(Event::StepStarted(StepStartedEvent { base, step_name }));
        }
        self.resume.attempt = Some(source.attempt);
        self.public_state.revision = Some(source.revision.get());
        self.public_state.cursor = record.cursor;
        self.public_state.snapshot = false;
        self.public_state.contents = Some(Arc::new(source.contents));
        self.public_state.seen = true;
        Ok(events)
    }

    pub(super) fn project_waiting(
        &mut self,
        record: &EventRecord,
        base: &BaseEvent,
    ) -> Result<Vec<Event>, ProjectionError> {
        let source: WaitingSource = self.payload(record)?;
        source.validate().map_err(|_| ProjectionError::Malformed)?;
        self.continuation_scope(&source.session_id, &source.run_id, &source.attempt)?;
        if record.correlation.as_deref() != Some(&source.operation_id.0)
            || record.turn_id != source.current_turn
            || record.live_only
            || record
                .cursor
                .is_some_and(|cursor| cursor <= source.source_event_cursor)
            || self
                .public_state
                .revision
                .is_some_and(|revision| source.revision.get() < revision)
        {
            return Err(ProjectionError::Identity);
        }
        if source.source_event_cursor.0 == 0 || self.resume.waiting.is_some() {
            return Err(ProjectionError::Order);
        }
        if source.frontend_pending.len() + source.backend_pending.len() > self.config.max_open_calls
        {
            return Err(ProjectionError::Limit);
        }
        for id in source
            .frontend_pending
            .iter()
            .chain(&source.backend_pending)
        {
            if !self
                .calls
                .get(id)
                .is_some_and(|call| call.ended && !call.settled)
            {
                return Err(ProjectionError::Identity);
            }
        }
        let remaining: std::collections::BTreeSet<_> = self
            .calls
            .iter()
            .filter(|(_, call)| !call.settled)
            .map(|(id, _)| id)
            .collect();
        let selected: std::collections::BTreeSet<_> = source
            .frontend_pending
            .iter()
            .chain(&source.backend_pending)
            .collect();
        if remaining != selected {
            return Err(ProjectionError::Identity);
        }
        if let (Some(turn), Some(step)) = (&source.current_turn, &self.step)
            && step != &format!("turn:{turn}")
        {
            return Err(ProjectionError::Identity);
        }
        let mut events = Vec::new();
        self.open_exchange(base, &mut events);
        self.resume.attempt = Some(source.attempt.clone());
        self.resume.waiting = Some(Arc::new(source));
        self.paused = true;
        Ok(events)
    }

    pub(super) fn waiting_outcome(&self) -> Result<RunFinishedOutcome, ProjectionError> {
        let source = self.resume.waiting.as_ref().ok_or(ProjectionError::Order)?;
        if !source.interrupts.is_empty() {
            let interrupts = source.interrupts.iter().map(|interrupt| {
                let response_schema = response_schema(&interrupt.response_schema)?;
                Ok(Interrupt {
                    id: interrupt.interaction_id.0.clone(),
                    reason: match interrupt.kind { crabber_core::InteractionKind::Approval => "approval", crabber_core::InteractionKind::UserInput => "user_input" }.into(),
                    message: Some(interrupt.prompt.clone()), tool_call_id: Some(interrupt.call_id.0.clone().into()),
                    response_schema: Some(response_schema), expires_at: None,
                    metadata: Some(serde_json::Map::from_iter([("crabber".into(), serde_json::json!({"name":interrupt.name,"continuation":source.continuation}))])), subagent_run_id: None,
                })
            }).collect::<Result<Vec<_>, ProjectionError>>()?;
            return Ok(RunFinishedOutcome::Interrupt { interrupts });
        }
        if !source.backend_pending.is_empty() || source.frontend_pending.is_empty() {
            return Err(ProjectionError::Order);
        }
        let remaining: std::collections::BTreeSet<_> = self
            .calls
            .iter()
            .filter(|(_, call)| !call.settled)
            .map(|(id, _)| id)
            .collect();
        let frontend: std::collections::BTreeSet<_> = source.frontend_pending.iter().collect();
        if remaining != frontend {
            return Err(ProjectionError::Order);
        }
        Ok(RunFinishedOutcome::Success {
            pending_tool_call_ids: Some(
                source
                    .frontend_pending
                    .iter()
                    .map(|id| id.0.clone().into())
                    .collect(),
            ),
        })
    }
}

// The pinned SDK represents schemas as objects. A JSON Schema conjunction
// preserves the exact semantics of a native boolean schema without discarding it.
fn response_schema(
    schema: &serde_json::Value,
) -> Result<serde_json::Map<String, serde_json::Value>, ProjectionError> {
    match schema {
        serde_json::Value::Object(object) => Ok(object.clone()),
        serde_json::Value::Bool(value) => Ok(serde_json::Map::from_iter([(
            "allOf".into(),
            serde_json::json!([value]),
        )])),
        _ => Err(ProjectionError::Malformed),
    }
}
