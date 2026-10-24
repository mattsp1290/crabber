use std::collections::{BTreeMap, BTreeSet};

use ag_ui_core::event::{
    ActivitySnapshotEvent, BaseEvent, Event, ReasoningEndEvent, ReasoningMessageEndEvent,
    StepFinishedEvent, StepStartedEvent, TextMessageEndEvent, TextMessageStartEvent,
    ToolCallArgsEvent, ToolCallEndEvent, ToolCallResultEvent, ToolCallStartEvent,
};
use ag_ui_core::types::Role;
use crabber_core::event_payload::{
    CallDelta, CallIdentity, CallSettled, CallStarted, MessageCommitted, MessageEnded,
    MessageIdentity, StreamOutcome,
};
use crabber_core::{
    ContentBlock, EventKind, EventRecord, MessageId, ReasoningProvenance, RunId, SessionId,
    ToolCallId,
};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::sse::{frame_budget, json_bytes};

mod content;
mod continuation;
mod invocations;
mod native;
mod presentation;
mod sources;
mod terminal;
mod text;

/// All limits are independently reducible from these defaults. No transcript is retained.
#[derive(Debug, Clone)]
pub struct ProjectionConfig {
    pub reasoning: bool,
    /// Explicit trusted host hook sources; empty by default.
    pub raw_sources: BTreeSet<String>,
    /// Explicit trusted host CUSTOM names; legacy custom records never pass through.
    pub custom_events: BTreeSet<String>,
    pub max_event_bytes: usize,
    pub max_text_bytes: usize,
    pub max_argument_bytes: usize,
    pub max_open_calls: usize,
    pub max_messages: usize,
    pub max_batch: usize,
}
impl Default for ProjectionConfig {
    fn default() -> Self {
        Self {
            reasoning: false,
            raw_sources: BTreeSet::new(),
            custom_events: BTreeSet::new(),
            max_event_bytes: 1_048_576,
            max_text_bytes: 1_048_576,
            max_argument_bytes: 1_048_576,
            max_open_calls: 64,
            max_messages: 1024,
            max_batch: 256,
        }
    }
}

/// Finite public classifications; never pass backend error strings to the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Completion {
    Completed,
    Cancelled,
    Paused,
    Failed,
    LeaseLost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ProjectionError {
    #[error("invalid projection configuration")]
    Configuration,
    #[error("malformed source event")]
    Malformed,
    #[error("source identity mismatch")]
    Identity,
    #[error("invalid source event order")]
    Order,
    #[error("projection limit exceeded")]
    Limit,
    #[error("public source attempt failed")]
    SourceAttemptFailed,
    #[error("source events were lost")]
    Lagged,
    #[error("transport deadline or capacity exceeded")]
    Transport,
    #[error("projection already finished")]
    Terminal,
}
impl ProjectionError {
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Configuration => "crabber_configuration",
            Self::Malformed => "crabber_source_malformed",
            Self::Identity => "crabber_source_identity",
            Self::Order => "crabber_source_order",
            Self::Limit => "crabber_projection_limit",
            Self::SourceAttemptFailed => "crabber_source_attempt_failed",
            Self::Lagged => "crabber_source_lagged",
            Self::Transport => "crabber_transport",
            Self::Terminal => "crabber_terminal",
        }
    }
}

#[derive(Default, Clone, PartialEq, Eq)]
enum MessagePhase {
    #[default]
    Streaming,
    Ended,
    Committed,
}

#[derive(Default, Clone)]
struct MessageState {
    text_open: bool,
    reasoning_items: BTreeSet<String>,
    reasoning_public: bool,
    public: bool,
    phase: MessagePhase,
    text_bytes: usize,
    reasoning_bytes: usize,
}
#[derive(Clone)]
struct CallState {
    parent: MessageId,
    ended: bool,
    settled: bool,
    bytes: usize,
}

// Wire lifecycle is distinct from whether the native source actually started.
#[derive(Clone, Default)]
struct WireLifecycle {
    wire_open: bool,
    source_started: bool,
    terminal: bool,
}

// Explicit child selection and its actual lifecycle are one source binding.
#[derive(Clone, Default)]
struct SourceBinding {
    source: Option<crabber_core::PublicSourceIdentity>,
    explicit: bool,
    child_started: bool,
    child_terminal: Option<Completion>,
}

#[derive(Clone, Default)]
struct ResumeProjection {
    ready: bool,
    seen: bool,
    attempt: Option<crabber_core::continuation_source::AttemptIdentity>,
    waiting: Option<std::sync::Arc<crabber_core::continuation_source::WaitingSource>>,
}

// One current public view, not an accumulated event or message transcript.
#[derive(Clone, Default)]
struct PublicStateProjection {
    revision: Option<u64>,
    cursor: Option<crabber_core::EventCursor>,
    snapshot: bool,
    seen: bool,
    contents: Option<std::sync::Arc<crabber_core::PublicStateContents>>,
    activities: BTreeMap<String, String>,
    activity_metadata: BTreeMap<String, serde_json::Map<String, Value>>,
}

/// Projects exactly one source run. `finish` is required after drainage and task completion.
#[derive(Clone)]
pub struct Projector {
    session: SessionId,
    run: RunId,
    thread_alias: String,
    run_alias: String,
    config: ProjectionConfig,
    lifecycle: WireLifecycle,
    binding: SourceBinding,
    resume: ResumeProjection,
    public_state: PublicStateProjection,
    fault: Option<ProjectionError>,
    active: Option<MessageId>,
    messages: BTreeMap<MessageId, MessageState>,
    calls: BTreeMap<ToolCallId, CallState>,
    result_ids: BTreeSet<MessageId>,
    paused: bool,
    settled: Option<Completion>,
    activities: BTreeSet<String>,
    native_activities: BTreeMap<String, ActivitySnapshotEvent>,
    step: Option<String>,
    ui_messages: BTreeMap<MessageId, presentation::UiMessage>,
    ui_active: BTreeMap<String, MessageId>,
    invocations: BTreeMap<String, invocations::Invocation>,
}

fn base(record: Option<&EventRecord>) -> BaseEvent {
    let timestamp = record
        .and_then(|e| i64::try_from(e.created_at.unix_timestamp_nanos().div_euclid(1_000_000)).ok())
        .filter(|n| (-9_007_199_254_740_991..=9_007_199_254_740_991).contains(n));
    BaseEvent {
        timestamp,
        raw_event: None,
        metadata: None,
        subagent_run_id: None,
    }
}

/// Public tool result representation: JSON `{content: [...], is_error: bool}`.
/// Provider state is omitted recursively; reasoning obeys the same display opt-in.
#[must_use]
pub fn public_tool_content(content: &[ContentBlock], is_error: bool, reasoning: bool) -> Value {
    fn public(block: &ContentBlock, reasoning: bool) -> Option<Value> {
        match block {
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
                Some(json!({"type":"reasoning", "text":text, "item_id":item_id}))
            }
            ContentBlock::ProviderState { .. } | ContentBlock::Reasoning { .. } => None,
            ContentBlock::ToolResult {
                call_id,
                content,
                is_error,
            } => Some(json!({"type":"tool_result", "call_id":call_id,
                "content":content.iter().filter_map(|c| public(c, reasoning)).collect::<Vec<_>>(), "is_error":is_error})),
            _ => Some(serde_json::to_value(block).expect("content serializes")),
        }
    }
    json!({"content":content.iter().filter_map(|c| public(c, reasoning)).collect::<Vec<_>>(), "is_error":is_error})
}

impl Projector {
    /// Bind source IDs and wire aliases. Limits cannot exceed the default ceilings.
    ///
    /// # Errors
    /// Returns `Configuration` for zero/excessive limits or aliases that cannot fit.
    pub fn new(
        session: SessionId,
        run: RunId,
        thread_alias: String,
        run_alias: String,
        config: ProjectionConfig,
    ) -> Result<Self, ProjectionError> {
        let defaults = ProjectionConfig::default();
        for names in [&config.raw_sources, &config.custom_events] {
            if names.len() > 64
                || names.iter().any(|name| {
                    name.is_empty() || name.len() > 128 || name.chars().any(char::is_control)
                })
            {
                return Err(ProjectionError::Configuration);
            }
        }
        for (value, ceiling) in [
            (config.max_event_bytes, defaults.max_event_bytes),
            (config.max_text_bytes, defaults.max_text_bytes),
            (config.max_argument_bytes, defaults.max_argument_bytes),
            (config.max_open_calls, defaults.max_open_calls),
            (config.max_messages, defaults.max_messages),
            (config.max_batch, defaults.max_batch),
        ] {
            if value == 0 || value > ceiling {
                return Err(ProjectionError::Configuration);
            }
        }
        if thread_alias.is_empty()
            || run_alias.is_empty()
            || thread_alias.len() > 256
            || run_alias.len() > 256
            || thread_alias
                .chars()
                .chain(run_alias.chars())
                .any(char::is_control)
        {
            return Err(ProjectionError::Configuration);
        }
        let projector = Self {
            session,
            run,
            thread_alias,
            run_alias,
            config,
            lifecycle: WireLifecycle::default(),
            binding: SourceBinding::default(),
            resume: ResumeProjection::default(),
            public_state: PublicStateProjection::default(),
            fault: None,
            active: None,
            messages: BTreeMap::new(),
            calls: BTreeMap::new(),
            result_ids: BTreeSet::new(),
            paused: false,
            settled: None,
            activities: BTreeSet::new(),
            native_activities: BTreeMap::new(),
            step: None,
            ui_messages: BTreeMap::new(),
            ui_active: BTreeMap::new(),
            invocations: BTreeMap::new(),
        };
        // Guarantee the configured start and every finite terminal fit, even on startup failure.
        frame_budget(
            &projector.start_event(base(None)),
            projector.config.max_event_bytes,
        )
        .map_err(|_| ProjectionError::Configuration)?;
        frame_budget(
            &projector.finished_event(Completion::Cancelled),
            projector.config.max_event_bytes,
        )
        .map_err(|_| ProjectionError::Configuration)?;
        frame_budget(
            &Self::error_event(ProjectionError::SourceAttemptFailed.code()),
            projector.config.max_event_bytes,
        )
        .map_err(|_| ProjectionError::Configuration)?;
        Ok(projector)
    }

    /// Permanently fail a stream after lag or a transport fault, preserving the first fault.
    pub fn fail(&mut self, error: ProjectionError) {
        if !self.lifecycle.terminal {
            self.fault.get_or_insert(error);
        }
    }

    /// Bind the trusted source scope before observing this exchange. This is
    /// identity validation only; source metadata never grants execution rights.
    /// # Errors
    /// Rejects invalid/mismatched scope or rebinding after wire startup.
    pub fn bind_source(
        mut self,
        source: crabber_core::PublicSourceIdentity,
    ) -> Result<Self, ProjectionError> {
        if self.lifecycle.wire_open {
            return Err(ProjectionError::Order);
        }
        self.check_public_source(&source)?;
        self.binding.source = Some(source);
        self.binding.explicit = true;
        Ok(self)
    }

    /// Validate and project a record. Any error permanently faults this stream.
    ///
    /// # Errors
    /// Rejects cross-run, malformed, out-of-order, oversized or post-terminal records.
    pub fn push(&mut self, record: &EventRecord) -> Result<Vec<Event>, ProjectionError> {
        self.push_with_delivery(record, |_| Ok(()))
    }

    /// Project and atomically deliver a batch with bounded presentation rollback.
    /// The callback must reserve the entire batch before sending any event. On
    /// rejection presentation boundaries roll back and the stream permanently faults.
    ///
    /// # Errors
    /// Returns source validation errors or the delivery callback's error.
    pub fn push_with_delivery(
        &mut self,
        record: &EventRecord,
        deliver: impl FnOnce(&[Event]) -> Result<(), ProjectionError>,
    ) -> Result<Vec<Event>, ProjectionError> {
        if self.lifecycle.terminal {
            return Err(ProjectionError::Terminal);
        }
        if let Some(error) = self.fault {
            return Err(error);
        }
        if record.session_id != self.session || record.run_id != self.run {
            self.fail(ProjectionError::Identity);
            return Err(ProjectionError::Identity);
        }
        // Save only the presentation state this record can change. On rejection,
        // finish must never close a boundary whose start was not returned to the host.
        let started = self.lifecycle.wire_open;
        let source_started = self.lifecycle.source_started;
        let child_started = self.binding.child_started;
        let child_terminal = self.binding.child_terminal;
        let settled = self.settled;
        let paused = self.paused;
        let step = self.step.clone();
        let public_undo = self
            .public_undo(record)
            .inspect_err(|error| self.fail(*error))?;
        let presentation_undo = self
            .presentation_undo(record)
            .inspect_err(|error| self.fail(*error))?;
        let invocation_undo = self
            .invocation_undo(record)
            .inspect_err(|error| self.fail(*error))?;
        let continuation_undo = self
            .continuation_undo(record)
            .inspect_err(|error| self.fail(*error))?;
        let activity_id = native::activity_id(record);
        let activity_present = activity_id
            .as_ref()
            .is_some_and(|id| self.activities.contains(id));
        let activity_before = activity_id
            .as_ref()
            .and_then(|id| self.native_activities.get(id))
            .cloned();
        let active = self.active.clone();
        let message = active
            .as_ref()
            .and_then(|id| self.messages.get(id))
            .cloned();
        let call_id = record
            .payload
            .get("call_id")
            .and_then(Value::as_str)
            .filter(|id| id.len() <= self.config.max_event_bytes)
            .map(ToolCallId::from);
        let call = call_id.as_ref().and_then(|id| self.calls.get(id)).cloned();
        let result = self.project(record).and_then(|events| {
            self.validate_batch(&events)?;
            deliver(&events)?;
            Ok(events)
        });
        if let Err(error) = &result {
            self.lifecycle.wire_open = started;
            self.lifecycle.source_started = source_started;
            self.binding.child_started = child_started;
            self.binding.child_terminal = child_terminal;
            self.settled = settled;
            self.paused = paused;
            self.step = step;
            self.restore_public(public_undo);
            self.restore_presentation(presentation_undo);
            self.restore_invocation(invocation_undo);
            self.restore_continuation(continuation_undo);
            if let Some(id) = activity_id {
                if !activity_present {
                    self.activities.remove(&id);
                }
                if let Some(before) = activity_before {
                    self.native_activities.insert(id, before);
                } else {
                    self.native_activities.remove(&id);
                }
            }
            self.active.clone_from(&active);
            if let (Some(id), Some(message)) = (active, message) {
                self.messages.insert(id, message);
            }
            if let Some(id) = call_id {
                if let Some(call) = call {
                    self.calls.insert(id, call);
                } else {
                    self.calls.remove(&id);
                }
            }
            self.fail(*error);
        }
        result
    }
    fn validate_batch(&self, events: &[Event]) -> Result<(), ProjectionError> {
        if events.len() > self.config.max_batch {
            return Err(ProjectionError::Limit);
        }
        for event in events {
            frame_budget(event, self.config.max_event_bytes)?;
        }
        Ok(())
    }
    fn payload<T: DeserializeOwned>(&self, record: &EventRecord) -> Result<T, ProjectionError> {
        json_bytes(&record.payload, self.config.max_event_bytes)?;
        serde_json::from_value(record.payload.clone()).map_err(|_| ProjectionError::Malformed)
    }
    fn active_message(&mut self, id: &MessageId) -> Result<&mut MessageState, ProjectionError> {
        if self.active.as_ref() != Some(id) {
            return Err(ProjectionError::Order);
        }
        self.messages.get_mut(id).ok_or(ProjectionError::Order)
    }
    fn open_text(
        &mut self,
        id: &MessageId,
        base: &BaseEvent,
        output: &mut Vec<Event>,
    ) -> Result<(), ProjectionError> {
        let message = self.active_message(id)?;
        if !message.text_open {
            message.text_open = true;
            message.public = true;
            output.push(Event::TextMessageStart(TextMessageStartEvent {
                base: base.clone(),
                message_id: id.to_string().into(),
                role: Some(Role::Assistant),
                name: None,
            }));
        }
        Ok(())
    }
    fn close_message(&mut self, id: &MessageId, base: &BaseEvent) -> Vec<Event> {
        let mut output = Vec::new();
        for (call_id, call) in &mut self.calls {
            if &call.parent == id && !call.ended {
                call.ended = true;
                output.push(Event::ToolCallEnd(ToolCallEndEvent {
                    base: base.clone(),
                    tool_call_id: call_id.to_string().into(),
                }));
            }
        }
        if let Some(message) = self.messages.get_mut(id) {
            if message.text_open {
                message.text_open = false;
                output.push(Event::TextMessageEnd(TextMessageEndEvent {
                    base: base.clone(),
                    message_id: id.to_string().into(),
                }));
            }
            for reasoning_id in std::mem::take(&mut message.reasoning_items) {
                output.push(Event::ReasoningMessageEnd(ReasoningMessageEndEvent {
                    base: base.clone(),
                    message_id: reasoning_id.clone().into(),
                }));
                output.push(Event::ReasoningEnd(ReasoningEndEvent {
                    base: base.clone(),
                    message_id: reasoning_id.into(),
                }));
            }
        }
        output
    }

    #[allow(clippy::too_many_lines)]
    fn project(&mut self, record: &EventRecord) -> Result<Vec<Event>, ProjectionError> {
        if record.session_id != self.session || record.run_id != self.run {
            return Err(ProjectionError::Identity);
        }
        let base = base(Some(record));
        let mut output = Vec::new();
        if self.child_exchange()
            && !matches!(
                record.kind,
                EventKind::InvocationStarted
                    | EventKind::InvocationFinished
                    | EventKind::InvocationError
                    | EventKind::PublicStateSnapshot
                    | EventKind::PublicStateDelta
                    | EventKind::ActivitySnapshot
                    | EventKind::ActivityDelta
                    | EventKind::PublicPresentation
            )
        {
            return Err(ProjectionError::Identity);
        }
        if let Some(events) = self.project_invocation(record)? {
            return Ok(events);
        }
        if let Some(events) = self.project_public(record)? {
            return Ok(events);
        }
        if record.kind == EventKind::RunStarted {
            if self.lifecycle.source_started || self.resume.ready {
                return Err(ProjectionError::Order);
            }
            self.lifecycle.source_started = true;
            self.open_exchange(&base, &mut output);
            return Ok(output);
        }
        if record.kind == EventKind::RunAdmitted {
            return Ok(output);
        }
        if matches!(
            record.kind,
            EventKind::ExecutionAttemptChanged
                | EventKind::ToolPhaseChanged
                | EventKind::RunControlChanged
                | EventKind::InteractionChanged
                | EventKind::ContinuationChanged
        ) {
            return self.project_native_activity(record, base);
        }
        if record.kind == EventKind::RunResumed {
            return self.project_resume(record, base);
        }
        if record.kind == EventKind::ContinuationWaiting {
            return self.project_waiting(record, &base);
        }
        if record.kind == EventKind::RunSettled {
            json_bytes(&record.payload, self.config.max_event_bytes)?;
            self.settled = Some(match record.payload.get("status").and_then(Value::as_str) {
                Some("completed") => Completion::Completed,
                Some("interrupted") => Completion::Cancelled,
                Some("failed") => Completion::Failed,
                _ => return Err(ProjectionError::Malformed),
            });
            return Ok(output);
        }
        if !self.lifecycle.source_started && !self.resume.ready {
            return Err(ProjectionError::Order);
        }
        match record.kind {
            EventKind::ToolProgress => return self.local_progress(record, base),
            EventKind::MessageStarted => {
                let payload: MessageIdentity = self.payload(record)?;
                if self.active.is_some()
                    || self.messages.contains_key(&payload.message_id)
                    || self.activities.contains(&payload.message_id.0)
                    || self
                        .public_state
                        .activities
                        .contains_key(&payload.message_id.0)
                    || self.result_ids.contains(&payload.message_id)
                    || self.ui_messages.contains_key(&payload.message_id)
                {
                    return Err(ProjectionError::Order);
                }
                if self.messages.len() + self.ui_messages.len() >= self.config.max_messages {
                    return Err(ProjectionError::Limit);
                }
                self.active = Some(payload.message_id.clone());
                self.messages
                    .insert(payload.message_id, MessageState::default());
            }
            EventKind::TextDelta | EventKind::ReasoningDelta => {
                return self.project_text_delta(record, base);
            }
            EventKind::ToolCallStarted => {
                let payload: CallStarted = self.payload(record)?;
                self.active_message(&payload.message_id)?;
                if self.calls.contains_key(&payload.call_id) || payload.name.is_empty() {
                    return Err(ProjectionError::Order);
                }
                if self.calls.values().filter(|c| !c.settled).count() >= self.config.max_open_calls
                    || self.calls.len() >= self.config.max_messages * self.config.max_open_calls
                {
                    return Err(ProjectionError::Limit);
                }
                self.open_text(&payload.message_id, &base, &mut output)?;
                output.push(Event::ToolCallStart(ToolCallStartEvent {
                    base,
                    tool_call_id: payload.call_id.to_string().into(),
                    tool_call_name: payload.name,
                    parent_message_id: Some(payload.message_id.to_string().into()),
                }));
                self.calls.insert(
                    payload.call_id,
                    CallState {
                        parent: payload.message_id,
                        ended: false,
                        settled: false,
                        bytes: 0,
                    },
                );
            }
            EventKind::ToolCallArgsDelta => {
                let text = record
                    .payload
                    .get("text")
                    .and_then(Value::as_str)
                    .ok_or(ProjectionError::Malformed)?;
                if text.len() > self.config.max_argument_bytes {
                    return Err(ProjectionError::Limit);
                }
                let payload: CallDelta = self.payload(record)?;
                self.active_message(&payload.message_id)?;
                let call = self
                    .calls
                    .get_mut(&payload.call_id)
                    .ok_or(ProjectionError::Order)?;
                if call.parent != payload.message_id || call.ended {
                    return Err(ProjectionError::Order);
                }
                call.bytes = call
                    .bytes
                    .checked_add(text.len())
                    .filter(|n| *n <= self.config.max_argument_bytes)
                    .ok_or(ProjectionError::Limit)?;
                if !text.is_empty() {
                    output.push(Event::ToolCallArgs(ToolCallArgsEvent {
                        base,
                        tool_call_id: payload.call_id.to_string().into(),
                        delta: payload.text,
                    }));
                }
            }
            EventKind::ToolCallArgsCompleted => {
                let payload: CallIdentity = self.payload(record)?;
                self.active_message(&payload.message_id)?;
                let call = self
                    .calls
                    .get_mut(&payload.call_id)
                    .ok_or(ProjectionError::Order)?;
                if call.parent != payload.message_id || call.ended {
                    return Err(ProjectionError::Order);
                }
                call.ended = true;
                output.push(Event::ToolCallEnd(ToolCallEndEvent {
                    base,
                    tool_call_id: payload.call_id.to_string().into(),
                }));
            }
            EventKind::MessageStreamEnded => {
                let payload: MessageEnded = self.payload(record)?;
                let public = self.active_message(&payload.message_id)?.public;
                // Source cleanup must have delivered every argument end.
                if self
                    .calls
                    .values()
                    .any(|c| c.parent == payload.message_id && !c.ended)
                {
                    return Err(ProjectionError::Order);
                }
                output.extend(self.close_message(&payload.message_id, &base));
                self.messages
                    .get_mut(&payload.message_id)
                    .ok_or(ProjectionError::Order)?
                    .phase = MessagePhase::Ended;
                self.active = None;
                if public && payload.outcome == StreamOutcome::Failed {
                    // Deliver valid closures now, but permanently suppress retry content.
                    self.fail(ProjectionError::SourceAttemptFailed);
                }
            }
            EventKind::MessageCommitted => {
                let payload: MessageCommitted = self.payload(record)?;
                let message = self
                    .messages
                    .get_mut(&payload.message_id)
                    .ok_or(ProjectionError::Order)?;
                if payload.role != crabber_core::Role::Assistant
                    || message.phase != MessagePhase::Ended
                {
                    return Err(ProjectionError::Order);
                }
                message.phase = MessagePhase::Committed;
            }
            EventKind::ToolCallSettled => {
                let payload: CallSettled<'_> = self.payload(record)?;
                let call = self
                    .calls
                    .get_mut(&payload.call_id)
                    .ok_or(ProjectionError::Order)?;
                if !call.ended
                    || call.settled
                    || !self
                        .messages
                        .get(&call.parent)
                        .is_some_and(|m| m.phase == MessagePhase::Committed)
                {
                    return Err(ProjectionError::Order);
                }
                if self.messages.contains_key(&payload.message_id)
                    || self.activities.contains(&payload.message_id.0)
                    || self
                        .public_state
                        .activities
                        .contains_key(&payload.message_id.0)
                    || self.ui_messages.contains_key(&payload.message_id)
                    || !self.result_ids.insert(payload.message_id.clone())
                {
                    return Err(ProjectionError::Order);
                }
                call.settled = true;
                let content = content::tool_content(&payload.content, self.config.reasoning)?;
                let mut base = base;
                base.metadata = Some(serde_json::Map::from_iter([(
                    "crabber".into(),
                    json!({"isError":payload.is_error}),
                )]));
                output.push(Event::ToolCallResult(ToolCallResultEvent {
                    base,
                    message_id: payload.message_id.to_string().into(),
                    tool_call_id: payload.call_id.to_string().into(),
                    content,
                    role: Some(Role::Tool),
                }));
            }
            EventKind::RunPaused => self.paused = true,
            EventKind::TurnStarted => {
                let turn = record
                    .turn_id
                    .as_ref()
                    .filter(|turn| {
                        !turn.0.is_empty()
                            && turn.0.len() <= 128
                            && !turn.0.chars().any(char::is_control)
                    })
                    .ok_or(ProjectionError::Identity)?;
                if self.step.is_some() {
                    return Err(ProjectionError::Order);
                }
                let step_name = format!("turn:{turn}");
                self.step = Some(step_name.clone());
                output.push(Event::StepStarted(StepStartedEvent { base, step_name }));
            }
            EventKind::TurnCompleted => {
                let turn = record.turn_id.as_ref().ok_or(ProjectionError::Identity)?;
                let step_name = format!("turn:{turn}");
                if self.step.as_ref() != Some(&step_name) {
                    return Err(ProjectionError::Order);
                }
                self.step = None;
                output.push(Event::StepFinished(StepFinishedEvent { base, step_name }));
            }
            // Explicit source allowlist: these control/operational records have no public projection.
            EventKind::ToolCallPending
            | EventKind::ModelResponseCompleted
            | EventKind::ToolCallRunning
            | EventKind::PermissionRequested
            | EventKind::PermissionDecided
            | EventKind::ContextEpochStarted
            | EventKind::ContextEpochFinished
            | EventKind::ExtensionNotice
            | EventKind::Custom { .. } => {}
            EventKind::RunAdmitted
            | EventKind::RunStarted
            | EventKind::RunResumed
            | EventKind::RunSettled
            | EventKind::ExecutionAttemptChanged
            | EventKind::ContinuationWaiting
            | EventKind::ToolPhaseChanged
            | EventKind::RunControlChanged
            | EventKind::InteractionChanged
            | EventKind::ContinuationChanged
            | EventKind::PublicStateDelta
            | EventKind::ActivitySnapshot
            | EventKind::ActivityDelta
            | EventKind::PublicPresentation
            | EventKind::InvocationStarted
            | EventKind::InvocationFinished
            | EventKind::InvocationError => unreachable!(),
            EventKind::PublicStateSnapshot => {
                return Err(ProjectionError::Malformed);
            }
        }
        Ok(output)
    }
}
