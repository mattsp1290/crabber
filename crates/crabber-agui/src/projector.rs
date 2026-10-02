use std::collections::{BTreeMap, BTreeSet};

use ag_ui_core::event::{
    BaseEvent, Event, ReasoningEndEvent, ReasoningMessageContentEvent, ReasoningMessageEndEvent,
    ReasoningMessageStartEvent, ReasoningStartEvent, RunErrorEvent, RunFinishedEvent,
    RunStartedEvent, TextMessageContentEvent, TextMessageEndEvent, TextMessageStartEvent,
    ToolCallArgsEvent, ToolCallEndEvent, ToolCallResultEvent, ToolCallStartEvent,
};
use ag_ui_core::types::{Interrupt, Role, RunFinishedOutcome};
use crabber_core::event_payload::{
    CallDelta, CallIdentity, CallStarted, MessageCommitted, MessageDelta, MessageEnded,
    MessageIdentity, StreamOutcome,
};
use crabber_core::{ContentBlock, EventKind, EventRecord, MessageId, RunId, SessionId, ToolCallId};
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::sse::json_bytes;

/// All limits are independently reducible from these defaults. No transcript is retained.
#[derive(Debug, Clone)]
pub struct ProjectionConfig {
    pub reasoning: bool,
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
    reasoning_open: bool,
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

/// Projects exactly one source run. `finish` is required after drainage and task completion.
#[derive(Clone)]
pub struct Projector {
    session: SessionId,
    run: RunId,
    thread_alias: String,
    run_alias: String,
    config: ProjectionConfig,
    started: bool,
    terminal: bool,
    fault: Option<ProjectionError>,
    active: Option<MessageId>,
    messages: BTreeMap<MessageId, MessageState>,
    calls: BTreeMap<ToolCallId, CallState>,
    result_ids: BTreeSet<MessageId>,
    paused: bool,
    settled: Option<Completion>,
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
            ContentBlock::Reasoning { text, .. } if reasoning => {
                Some(json!({"type":"reasoning", "text":text}))
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
            started: false,
            terminal: false,
            fault: None,
            active: None,
            messages: BTreeMap::new(),
            calls: BTreeMap::new(),
            result_ids: BTreeSet::new(),
            paused: false,
            settled: None,
        };
        // Guarantee the configured start and every finite terminal fit, even on startup failure.
        json_bytes(
            &projector.start_event(base(None)),
            projector.config.max_event_bytes,
        )
        .map_err(|_| ProjectionError::Configuration)?;
        json_bytes(
            &projector.finished_event(Completion::Paused),
            projector.config.max_event_bytes,
        )
        .map_err(|_| ProjectionError::Configuration)?;
        json_bytes(
            &Self::error_event(ProjectionError::SourceAttemptFailed.code()),
            projector.config.max_event_bytes,
        )
        .map_err(|_| ProjectionError::Configuration)?;
        Ok(projector)
    }

    /// Permanently fail a stream after lag or a transport fault, preserving the first fault.
    pub fn fail(&mut self, error: ProjectionError) {
        if !self.terminal {
            self.fault.get_or_insert(error);
        }
    }

    fn start_event(&self, base: BaseEvent) -> Event {
        Event::RunStarted(Box::new(RunStartedEvent {
            base,
            thread_id: self.thread_alias.clone().into(),
            run_id: self.run_alias.clone().into(),
            protocol_version: Some("1.0".into()),
            parent_run_id: None,
            input: None,
        }))
    }
    fn error_event(code: &str) -> Event {
        Event::RunError(RunErrorEvent {
            base: base(None),
            message: "The live run could not be projected.".into(),
            code: Some(code.into()),
            usage: None,
        })
    }
    fn finished_event(&self, completion: Completion) -> Event {
        let outcome = match completion {
            Completion::Cancelled => RunFinishedOutcome::Cancelled,
            Completion::Paused => RunFinishedOutcome::Interrupt {
                interrupts: vec![Interrupt {
                    id: format!("{}:pause", self.run_alias),
                    reason: "crabber_paused".into(),
                    message: None,
                    tool_call_id: None,
                    response_schema: None,
                    expires_at: None,
                    metadata: None,
                    subagent_run_id: None,
                }],
            },
            _ => RunFinishedOutcome::Success {
                pending_tool_call_ids: None,
            },
        };
        Event::RunFinished(RunFinishedEvent {
            base: base(None),
            thread_id: self.thread_alias.clone().into(),
            run_id: self.run_alias.clone().into(),
            result: None,
            outcome: Some(outcome),
            usage: None,
        })
    }

    /// Validate and project a record. Any error permanently faults this stream.
    ///
    /// # Errors
    /// Rejects cross-run, malformed, out-of-order, oversized or post-terminal records.
    pub fn push(&mut self, record: &EventRecord) -> Result<Vec<Event>, ProjectionError> {
        if self.terminal {
            return Err(ProjectionError::Terminal);
        }
        if let Some(error) = self.fault {
            return Err(error);
        }
        // Save only the presentation state this record can change. On rejection,
        // finish must never close a boundary whose start was not returned to the host.
        let started = self.started;
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
            Ok(events)
        });
        if let Err(error) = &result {
            self.started = started;
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
            json_bytes(event, self.config.max_event_bytes)?;
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
                role: Role::Assistant,
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
            if message.reasoning_open {
                message.reasoning_open = false;
                let reasoning_id = format!("{id}:reasoning");
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
        if record.kind == EventKind::RunStarted {
            if self.started {
                return Err(ProjectionError::Order);
            }
            self.started = true;
            output.push(self.start_event(base));
            return Ok(output);
        }
        if record.kind == EventKind::RunAdmitted {
            return Ok(output);
        }
        if !self.started {
            return Err(ProjectionError::Order);
        }
        match record.kind {
            EventKind::MessageStarted => {
                let payload: MessageIdentity = self.payload(record)?;
                if self.active.is_some() || self.messages.contains_key(&payload.message_id) {
                    return Err(ProjectionError::Order);
                }
                if self.messages.len() >= self.config.max_messages {
                    return Err(ProjectionError::Limit);
                }
                self.active = Some(payload.message_id.clone());
                self.messages
                    .insert(payload.message_id, MessageState::default());
            }
            EventKind::TextDelta | EventKind::ReasoningDelta => {
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
                let payload: MessageDelta = self.payload(record)?;
                let limit = self.config.max_text_bytes;
                let show_reasoning = self.config.reasoning;
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
                    let id = format!("{}:reasoning", payload.message_id);
                    if !message.reasoning_open {
                        message.reasoning_open = true;
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
                #[derive(serde::Deserialize)]
                struct Settled {
                    call_id: ToolCallId,
                    message_id: MessageId,
                    content: Vec<ContentBlock>,
                    is_error: bool,
                }
                let payload: Settled = self.payload(record)?;
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
                    || !self.result_ids.insert(payload.message_id.clone())
                {
                    return Err(ProjectionError::Order);
                }
                call.settled = true;
                let content =
                    public_tool_content(&payload.content, payload.is_error, self.config.reasoning);
                json_bytes(&content, self.config.max_event_bytes)?;
                output.push(Event::ToolCallResult(ToolCallResultEvent {
                    base,
                    message_id: payload.message_id.to_string().into(),
                    tool_call_id: payload.call_id.to_string().into(),
                    content: serde_json::to_string(&content)
                        .map_err(|_| ProjectionError::Malformed)?
                        .into(),
                    role: Some(Role::Tool),
                }));
            }
            EventKind::RunPaused => self.paused = true,
            EventKind::RunSettled => {
                self.settled = Some(match record.payload.get("status").and_then(Value::as_str) {
                    Some("completed") => Completion::Completed,
                    Some("interrupted") => Completion::Cancelled,
                    Some("failed") => Completion::Failed,
                    _ => return Err(ProjectionError::Malformed),
                });
            }
            // Explicit source allowlist: these control/operational records have no public projection.
            EventKind::TurnStarted
            | EventKind::TurnCompleted
            | EventKind::ToolCallPending
            | EventKind::ToolCallRunning
            | EventKind::PermissionRequested
            | EventKind::PermissionDecided
            | EventKind::RunResumed
            | EventKind::ContextEpochStarted
            | EventKind::ContextEpochFinished
            | EventKind::ExtensionNotice
            | EventKind::Custom { .. } => {}
            EventKind::RunAdmitted | EventKind::RunStarted => unreachable!(),
        }
        Ok(output)
    }

    /// Emit the sole terminal after the receiver drains and `RunHandle::done` resolves.
    /// Repeated calls return an empty batch. Task failures outrank durable status hints.
    ///
    /// # Errors
    /// Returns `Limit` if valid presentation closures cannot fit the configured batch.
    pub fn finish(&mut self, completion: Completion) -> Result<Vec<Event>, ProjectionError> {
        if self.terminal {
            return Ok(Vec::new());
        }
        let mut output = Vec::new();
        if !self.started {
            self.started = true;
            output.push(self.start_event(base(None)));
        }
        if self.active.is_some() {
            self.fail(ProjectionError::Order);
        }
        if completion == Completion::Completed
            && (self.settled != Some(Completion::Completed)
                || self
                    .messages
                    .values()
                    .any(|m| m.public && m.phase != MessagePhase::Committed)
                || self.calls.values().any(|c| !c.settled))
        {
            self.fail(ProjectionError::Order);
        }
        if completion == Completion::Cancelled && self.settled != Some(Completion::Cancelled) {
            self.fail(ProjectionError::Order);
        }
        if completion == Completion::Paused && !self.paused {
            self.fail(ProjectionError::Order);
        }
        if let Some(id) = self.active.take() {
            output.extend(self.close_message(&id, &base(None)));
        }
        let terminal = if let Some(error) = self.fault {
            Self::error_event(error.code())
        } else if completion == Completion::LeaseLost {
            Self::error_event("crabber_lease_lost")
        } else if completion == Completion::Failed {
            Self::error_event("crabber_runtime_failed")
        } else {
            self.finished_event(completion)
        };
        output.push(terminal);
        if output.len() > self.config.max_batch {
            // An error terminal is valid without promising successful stream closure.
            // Never exceed the configured batch or silently drop a success terminal.
            output.clear();
            output.push(Self::error_event(ProjectionError::Limit.code()));
        }
        self.terminal = true;
        self.validate_batch(&output)?;
        Ok(output)
    }
}
