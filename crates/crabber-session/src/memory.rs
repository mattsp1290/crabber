#[path = "memory/abandon.rs"]
mod abandon;
#[path = "memory/admission_execution.rs"]
mod admission_execution;

use crate::{
    AdmitOutcome, AdmitRequest, ExecutionStore, InboxKind, KeyedAdmitOutcome, KeyedAdmitRequest,
    Store, StoreError,
};
use async_trait::async_trait;
use crabber_core::{
    AdmissionKey, AdmissionReceipt, ByteLimits, Clock, ContextEpoch, EpochId, EventCursor,
    EventRecord, Message, MessageId, Part, PartKind, Run, RunFence, RunId, RunStatus, Session,
    SessionId, SystemClock, ToolCallId, ToolCallRecord, ToolCallStatus, ToolResult,
    ToolResultStatus, Usage,
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use time::OffsetDateTime;

/// Volatile persistence. Snapshot continuations work across clones of this Store.
/// Appends preserve their frozen cutoffs; any message-part or tool mutation in
/// the session conservatively invalidates continuations, even for newer records.
#[derive(Clone)]
pub struct MemoryStore {
    state: Arc<Mutex<State>>,
    clock: Arc<dyn Clock>,
    limits: ByteLimits,
    snapshot_key: String,
}

#[derive(Clone, Default)]
struct State {
    #[cfg(test)]
    abandon_fail_after_tools: bool,
    #[cfg(test)]
    abandon_fault_boundary: u8,
    abandonments: BTreeMap<RunId, crate::abandonment::AbandonCommit>,
    admission_executions: BTreeMap<RunId, crate::AdmissionExecutionRecord>,
    receipts: BTreeMap<(SessionId, AdmissionKey), AdmissionReceipt>,
    sessions: BTreeMap<SessionId, Session>,
    runs: BTreeMap<RunId, Run>,
    run_order: Vec<RunId>,
    messages: Vec<Message>,
    events: Vec<EventRecord>,
    calls: BTreeMap<ToolCallId, ToolCallRecord>,
    call_order: Vec<ToolCallId>,
    snapshot_revisions: BTreeMap<SessionId, u64>,
    epochs: BTreeMap<EpochId, ContextEpoch>,
    inbox: Vec<InboxRow>,
    extension_values: BTreeMap<(SessionId, String), BTreeMap<String, String>>,
}

#[derive(Clone)]
struct InboxRow {
    session_id: SessionId,
    kind: InboxKind,
    message: Message,
    consumed_by_run: Option<RunId>,
}

#[derive(Clone)]
struct MemoryExecution {
    store: MemoryStore,
    fence: RunFence,
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStore {
    #[must_use]
    pub fn new() -> Self {
        Self::with_clock(Arc::new(SystemClock))
    }

    #[must_use]
    pub fn with_clock(clock: Arc<dyn Clock>) -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
            clock,
            limits: ByteLimits::default(),
            snapshot_key: uuid::Uuid::new_v4().to_string(),
        }
    }

    #[must_use]
    pub fn with_limits(mut self, limits: ByteLimits) -> Self {
        self.limits = limits;
        self
    }

    fn transact<T, E>(&self, operation: impl FnOnce(&mut State) -> Result<T, E>) -> Result<T, E> {
        let mut guard = self.state.lock().expect("memory store poisoned");
        let mut next = guard.clone();
        let output = operation(&mut next)?;
        *guard = next;
        Ok(output)
    }

    fn ownership_fenced<T>(
        &self,
        fence: &RunFence,
        operation: impl FnOnce(&mut State, &Run) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        self.transact(|state| {
            let now = self.clock.now();
            let run = state
                .runs
                .get(&fence.run_id)
                .ok_or(StoreError::NotFound)?
                .clone();
            if run.claim_token != fence.claim_token
                || run.status.is_terminal()
                || run.lease_until <= now
            {
                return Err(StoreError::Conflict);
            }
            operation(state, &run)
        })
    }
    fn fenced<T>(
        &self,
        fence: &RunFence,
        operation: impl FnOnce(&mut State, &Run) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        self.ownership_fenced(fence, |state, run| {
            if state
                .admission_executions
                .get(&run.id)
                .is_some_and(|r| r.state == crate::AdmissionExecutionState::Unstarted)
            {
                return Err(StoreError::AdmissionRecoveryRequired);
            }
            operation(state, run)
        })
    }
}

fn insert_message(state: &mut State, run: &Run, message: Message) -> Result<(), StoreError> {
    if message.session_id != run.session_id || message.run_id.as_ref() != Some(&run.id) {
        return Err(StoreError::Validation(
            "message belongs to another run".into(),
        ));
    }
    if state.messages.iter().any(|old| old.id == message.id) {
        return Err(StoreError::Conflict);
    }
    for part in &message.parts {
        if part.message_id != message.id {
            return Err(StoreError::Validation(
                "part belongs to another message".into(),
            ));
        }
    }
    state.messages.push(message);
    Ok(())
}

fn insert_event(state: &mut State, run: &Run, mut event: EventRecord) -> Result<(), StoreError> {
    if event.live_only || event.kind.is_live_only() {
        return Err(StoreError::Validation(
            "live-only events cannot be persisted".into(),
        ));
    }
    if event.session_id != run.session_id || event.run_id != run.id {
        return Err(StoreError::Validation(
            "event belongs to another run".into(),
        ));
    }
    event.cursor = Some(EventCursor(state.events.len() as u64 + 1));
    state.events.push(event);
    Ok(())
}

fn project_messages(
    state: &State,
    id: &SessionId,
    epoch: Option<EpochId>,
) -> Result<Vec<Message>, StoreError> {
    if !state.sessions.contains_key(id) {
        return Err(StoreError::NotFound);
    }
    let selected = epoch.or_else(|| {
        state.run_order.iter().rev().find_map(|run_id| {
            state
                .runs
                .get(run_id)
                .filter(|run| &run.session_id == id)
                .map(|run| run.epoch_id.clone())
        })
    });
    let selected_epoch = selected
        .as_ref()
        .map(|key| state.epochs.get(key).ok_or(StoreError::NotFound))
        .transpose()?;
    if selected_epoch.is_some_and(|value| &value.session_id != id) {
        return Err(StoreError::Validation(
            "epoch belongs to another session".into(),
        ));
    }
    let all: Vec<_> = state
        .messages
        .iter()
        .filter(|message| &message.session_id == id)
        .cloned()
        .collect();
    let selected_messages =
        if let Some(epoch) = selected_epoch.filter(|epoch| epoch.summary_message_id.is_some()) {
            let summary_id = epoch.summary_message_id.as_ref().expect("filtered above");
            let summary = all
                .iter()
                .find(|message| &message.id == summary_id)
                .ok_or(StoreError::NotFound)?
                .clone();
            let tail = epoch
                .tail_start_message_id
                .as_ref()
                .map(|tail_id| {
                    all.iter()
                        .position(|message| &message.id == tail_id)
                        .ok_or(StoreError::NotFound)
                })
                .transpose()?;
            let mut projected = vec![summary];
            if let Some(start) = tail {
                projected.extend(
                    all[start..]
                        .iter()
                        .filter(|message| &message.id != summary_id)
                        .cloned(),
                );
            }
            projected
        } else {
            all
        };
    Ok(selected_messages
        .into_iter()
        .filter_map(|mut message| {
            message
                .parts
                .retain(|part| !matches!(part.kind, PartKind::Custom { .. }));
            (!message.parts.is_empty()).then_some(message)
        })
        .collect())
}

fn initial_epoch(
    id: EpochId,
    session_id: SessionId,
    run_id: RunId,
    previous: Option<ContextEpoch>,
) -> ContextEpoch {
    ContextEpoch {
        id,
        session_id,
        run_id,
        parent: previous.as_ref().map(|epoch| epoch.id.clone()),
        summarized_range: previous
            .as_ref()
            .and_then(|epoch| epoch.summarized_range.clone()),
        summary_message_id: previous
            .as_ref()
            .and_then(|epoch| epoch.summary_message_id.clone()),
        tail_start_message_id: previous
            .as_ref()
            .and_then(|epoch| epoch.tail_start_message_id.clone()),
        provider_id: previous
            .as_ref()
            .map_or_else(String::new, |epoch| epoch.provider_id.clone()),
        model_id: previous
            .as_ref()
            .map_or_else(String::new, |epoch| epoch.model_id.clone()),
        reason: "initial".into(),
        next_policy: previous.and_then(|epoch| epoch.next_policy),
    }
}

fn admit_transaction(
    state: &mut State,
    request: &AdmitRequest,
    now: OffsetDateTime,
    allow_create: bool,
) -> Result<AdmitOutcome, StoreError> {
    let lease = time::Duration::try_from(request.lease)
        .map_err(|_| StoreError::Validation("lease is too large".into()))?;
    if lease <= time::Duration::ZERO {
        return Err(StoreError::Validation("lease must be positive".into()));
    }
    let session_id = request
        .session_id
        .clone()
        .unwrap_or_else(|| request.user_message.session_id.clone());
    let session = if let Some(existing) = state.sessions.get(&session_id) {
        // Workspace identity is immutable: exact comparison, empty is a value.
        if existing.workspace_id != request.workspace_id || existing.directory != request.directory
        {
            return Err(StoreError::SessionIdentityMismatch);
        }
        existing.clone()
    } else if request.session_id.is_some() && !allow_create {
        return Err(StoreError::NotFound);
    } else {
        Session {
            id: session_id.clone(),
            workspace_id: request.workspace_id.clone(),
            directory: request.directory.clone(),
            title: request.title.clone(),
            created_at: now,
            updated_at: now,
        }
    };
    if state
        .runs
        .values()
        .any(|run| run.session_id == session_id && !run.status.is_terminal())
    {
        return Err(StoreError::Busy);
    }
    if request.user_message.session_id != session_id {
        return Err(StoreError::Validation(
            "user message has wrong session".into(),
        ));
    }
    let prior_history = if state.sessions.contains_key(&session_id) {
        project_messages(state, &session_id, None)?
    } else {
        Vec::new()
    };
    let previous_epoch = state
        .run_order
        .iter()
        .rev()
        .find_map(|id| {
            state
                .runs
                .get(id)
                .filter(|run| run.session_id == session_id)
        })
        .map(|run| state.epochs.get(&run.epoch_id).ok_or(StoreError::NotFound))
        .transpose()?
        .cloned();
    let run_id = RunId::new();
    let epoch_id = EpochId::new();
    let token = uuid::Uuid::new_v4().to_string();
    let run = Run {
        id: run_id.clone(),
        session_id: session_id.clone(),
        status: RunStatus::Running,
        owner: request.owner.clone(),
        claim_token: token.clone(),
        lease_until: now + lease,
        epoch_id: epoch_id.clone(),
        config_hash: request.config_hash.clone(),
        plan_fingerprint: request.plan_fingerprint.clone(),
        checkpoint: None,
        error: None,
        usage: Usage::default(),
        created_at: now,
        updated_at: now,
    };
    let mut user_message = request.user_message.clone();
    user_message.run_id = Some(run_id.clone());
    insert_message(state, &run, user_message)?;
    state.sessions.insert(session_id.clone(), session.clone());
    state.runs.insert(run_id.clone(), run.clone());
    state.run_order.push(run_id.clone());
    state.epochs.insert(
        epoch_id.clone(),
        initial_epoch(epoch_id.clone(), session_id, run_id.clone(), previous_epoch),
    );
    Ok(AdmitOutcome {
        session,
        run,
        fence: RunFence {
            run_id,
            claim_token: token,
        },
        assistant_placeholder: MessageId::new(),
        epoch: epoch_id,
        prior_history,
    })
}

#[async_trait]
impl Store for MemoryStore {
    async fn admit_run(&self, request: AdmitRequest) -> Result<AdmitOutcome, StoreError> {
        self.transact(|state| admit_transaction(state, &request, self.clock.now(), false))
    }

    async fn admit_keyed_run(
        &self,
        keyed: KeyedAdmitRequest,
    ) -> Result<KeyedAdmitOutcome, StoreError> {
        let request = &keyed.request;
        let session_id = request.session_id.as_ref().ok_or_else(|| {
            StoreError::Validation("keyed admission requires a stable session ID".into())
        })?;
        if request.user_message.session_id != *session_id
            || request.user_message.run_id.is_some()
            || request
                .user_message
                .parts
                .iter()
                .any(|part| part.message_id != request.user_message.id)
        {
            return Err(StoreError::Validation(
                "invalid admission message identity".into(),
            ));
        }
        let digest = keyed.semantic_digest()?;
        if let Some(capsule) = &keyed.execution {
            capsule.validate(&keyed)?;
        }
        self.transact(|state| {
            if let Some(session) = state.sessions.get(session_id)
                && (session.workspace_id != request.workspace_id
                    || session.directory != request.directory)
            {
                return Err(StoreError::SessionIdentityMismatch);
            }
            let key = (session_id.clone(), keyed.options.key.clone());
            if let Some(receipt) = state.receipts.get(&key) {
                if receipt.fingerprint != keyed.options.fingerprint
                    || receipt.semantic_digest != digest
                {
                    return Err(StoreError::AdmissionConflict);
                }
                return Ok(KeyedAdmitOutcome::Replayed(receipt.clone()));
            }
            let admitted = admit_transaction(state, request, self.clock.now(), true)?;
            let receipt = AdmissionReceipt {
                session_id: session_id.clone(),
                run_id: admitted.run.id.clone(),
                user_message_id: request.user_message.id.clone(),
                fingerprint: keyed.options.fingerprint.clone(),
                semantic_digest_version: 1,
                semantic_digest: digest,
            };
            if let Some(capsule) = keyed.execution.clone() {
                state.admission_executions.insert(
                    admitted.run.id.clone(),
                    crate::AdmissionExecutionRecord {
                        receipt: receipt.clone(),
                        key: keyed.options.key.clone(),
                        capsule,
                        state: crate::AdmissionExecutionState::Unstarted,
                    },
                );
            }
            state.receipts.insert(key, receipt.clone());
            Ok(KeyedAdmitOutcome::Started {
                receipt,
                admitted: Box::new(admitted),
            })
        })
    }

    async fn lookup_admission(
        &self,
        session: &SessionId,
        key: &AdmissionKey,
    ) -> Result<Option<AdmissionReceipt>, StoreError> {
        Ok(self
            .state
            .lock()
            .expect("memory store poisoned")
            .receipts
            .get(&(session.clone(), key.clone()))
            .cloned())
    }

    async fn load_admission_execution(
        &self,
        session: &SessionId,
        key: &AdmissionKey,
    ) -> Result<Option<crate::AdmissionExecutionRecord>, crate::AdmissionExecutionError> {
        Ok(self.load_execution_record(session, key))
    }
    async fn claim_unstarted_admission(
        &self,
        request: crate::ClaimUnstartedAdmissionRequest,
    ) -> Result<crate::ClaimedAdmission, crate::AdmissionExecutionError> {
        self.claim_admission(request)
    }
    async fn abandon_run(
        &self,
        request: crabber_core::AbandonRequest,
    ) -> Result<crabber_core::AbandonOutcome, crabber_core::AbandonError> {
        // The operation and store-clock read share the owner-write lock.
        self.transact(|state| abandon::abandon_transaction(state, request, self.clock.now()))
    }

    async fn execution(&self, fence: RunFence) -> Result<Box<dyn ExecutionStore>, StoreError> {
        self.ownership_fenced(&fence, |_, _| Ok(()))?;
        Ok(Box::new(MemoryExecution {
            store: self.clone(),
            fence,
        }))
    }

    async fn get_session(&self, id: &SessionId) -> Result<Option<Session>, StoreError> {
        Ok(self
            .state
            .lock()
            .expect("memory store poisoned")
            .sessions
            .get(id)
            .cloned())
    }

    async fn get_run(&self, id: &RunId) -> Result<Option<Run>, StoreError> {
        Ok(self
            .state
            .lock()
            .expect("memory store poisoned")
            .runs
            .get(id)
            .cloned())
    }

    async fn snapshot(
        &self,
        request: crate::SnapshotRequest,
    ) -> Result<crate::SnapshotOutcome, StoreError> {
        let state = self.state.lock().expect("memory store poisoned");
        self.read_snapshot(&state, &request)
    }

    async fn list_messages(
        &self,
        id: &SessionId,
        epoch: Option<EpochId>,
    ) -> Result<Vec<Message>, StoreError> {
        let state = self.state.lock().expect("memory store poisoned");
        project_messages(&state, id, epoch)
    }

    async fn list_all_messages(&self, id: &SessionId) -> Result<Vec<Message>, StoreError> {
        let state = self.state.lock().expect("memory store poisoned");
        Ok(state
            .messages
            .iter()
            .filter(|message| &message.session_id == id)
            .cloned()
            .collect())
    }

    async fn list_events(
        &self,
        id: &SessionId,
        after: Option<EventCursor>,
        limit: usize,
    ) -> Result<Vec<EventRecord>, StoreError> {
        let state = self.state.lock().expect("memory store poisoned");
        Ok(state
            .events
            .iter()
            .filter(|event| &event.session_id == id && event.cursor > after)
            .take(limit)
            .cloned()
            .collect())
    }

    async fn list_unfinished_runs(&self) -> Result<Vec<Run>, StoreError> {
        Ok(self
            .state
            .lock()
            .expect("memory store poisoned")
            .runs
            .values()
            .filter(|run| !run.status.is_terminal())
            .cloned()
            .collect())
    }

    async fn list_unfinished_tool_calls(
        &self,
        run: &RunId,
    ) -> Result<Vec<ToolCallRecord>, StoreError> {
        Ok(self
            .state
            .lock()
            .expect("memory store poisoned")
            .calls
            .values()
            .filter(|call| {
                &call.run_id == run
                    && matches!(
                        call.status,
                        ToolCallStatus::Pending | ToolCallStatus::Running
                    )
            })
            .cloned()
            .collect())
    }

    async fn admission_execution_state(
        &self,
        run: &RunId,
    ) -> Result<Option<crate::AdmissionExecutionState>, StoreError> {
        Ok(self
            .state
            .lock()
            .expect("memory store poisoned")
            .admission_executions
            .get(run)
            .map(|record| record.state))
    }

    async fn claim_expired_run(&self, id: &RunId, owner: &str) -> Result<RunFence, StoreError> {
        self.transact(|state| {
            let now = self.clock.now();
            if state
                .admission_executions
                .get(id)
                .is_some_and(|r| r.state == crate::AdmissionExecutionState::Unstarted)
            {
                return Err(StoreError::AdmissionRecoveryRequired);
            }
            let run = state.runs.get_mut(id).ok_or(StoreError::NotFound)?;
            if run.status.is_terminal() || run.lease_until > now {
                return Err(StoreError::Conflict);
            }
            owner.clone_into(&mut run.owner);
            run.claim_token = uuid::Uuid::new_v4().to_string();
            run.lease_until = now + time::Duration::seconds(30);
            run.updated_at = now;
            Ok(RunFence {
                run_id: id.clone(),
                claim_token: run.claim_token.clone(),
            })
        })
    }

    async fn get_extension_state(
        &self,
        extension_id: &str,
        session: &SessionId,
    ) -> Result<BTreeMap<String, String>, StoreError> {
        Ok(self
            .state
            .lock()
            .expect("memory store poisoned")
            .extension_values
            .get(&(session.clone(), extension_id.to_owned()))
            .cloned()
            .unwrap_or_default())
    }

    async fn enqueue_inbox(
        &self,
        session: &SessionId,
        kind: InboxKind,
        message: Message,
    ) -> Result<(), StoreError> {
        self.transact(|state| {
            if !state.sessions.contains_key(session) {
                return Err(StoreError::NotFound);
            }
            if &message.session_id != session {
                return Err(StoreError::Validation(
                    "inbox message has wrong session".into(),
                ));
            }
            state.inbox.push(InboxRow {
                session_id: session.clone(),
                kind,
                message,
                consumed_by_run: None,
            });
            Ok(())
        })
    }
}

#[async_trait]
impl ExecutionStore for MemoryExecution {
    async fn begin_admission_execution(&self) -> Result<(), crate::AdmissionExecutionError> {
        self.begin_admission()
    }
    async fn renew_lease(&self, until: OffsetDateTime) -> Result<(), StoreError> {
        let now = self.store.clock.now();
        self.store.ownership_fenced(&self.fence, |state, run| {
            if until <= now {
                return Err(StoreError::Validation(
                    "lease must extend into the future".into(),
                ));
            }
            let entry = state.runs.get_mut(&run.id).expect("fenced run exists");
            entry.lease_until = until;
            entry.updated_at = now;
            Ok(())
        })
    }

    async fn append_message(&self, message: Message) -> Result<(), StoreError> {
        self.store.fenced(&self.fence, |state, run| {
            insert_message(state, run, message)
        })
    }

    async fn append_part(&self, part: Part) -> Result<(), StoreError> {
        self.store.fenced(&self.fence, |state, run| {
            let message = state
                .messages
                .iter_mut()
                .find(|message| message.id == part.message_id)
                .ok_or(StoreError::NotFound)?;
            if message.session_id != run.session_id || message.run_id.as_ref() != Some(&run.id) {
                return Err(StoreError::Validation("part belongs to another run".into()));
            }
            if message
                .parts
                .iter()
                .any(|existing| existing.id == part.id || existing.ordinal == part.ordinal)
            {
                return Err(StoreError::Conflict);
            }
            message.parts.push(part);
            message.parts.sort_by_key(|entry| entry.ordinal);
            *state
                .snapshot_revisions
                .entry(run.session_id.clone())
                .or_default() += 1;
            Ok(())
        })
    }

    async fn append_event(&self, event: EventRecord) -> Result<(), StoreError> {
        self.store
            .fenced(&self.fence, |state, run| insert_event(state, run, event))
    }

    async fn create_tool_call(
        &self,
        call: ToolCallRecord,
        pending_event: EventRecord,
    ) -> Result<(), StoreError> {
        self.store.fenced(&self.fence, |state, run| {
            if call.run_id != run.id
                || call.status != ToolCallStatus::Pending
                || state.calls.contains_key(&call.id)
            {
                return Err(StoreError::Validation("invalid new tool call".into()));
            }
            insert_event(state, run, pending_event)?;
            state.call_order.push(call.id.clone());
            state.calls.insert(call.id.clone(), call);
            Ok(())
        })
    }

    async fn claim_tool_call(
        &self,
        id: &ToolCallId,
        running_event: EventRecord,
    ) -> Result<(), StoreError> {
        self.store.fenced(&self.fence, |state, run| {
            let call = state.calls.get(id).ok_or(StoreError::NotFound)?;
            if call.run_id != run.id || call.status != ToolCallStatus::Pending {
                return Err(StoreError::Conflict);
            }
            insert_event(state, run, running_event)?;
            state
                .calls
                .get_mut(id)
                .expect("validated call exists")
                .status = ToolCallStatus::Running;
            *state
                .snapshot_revisions
                .entry(run.session_id.clone())
                .or_default() += 1;
            Ok(())
        })
    }

    async fn settle_tool_call(
        &self,
        id: &ToolCallId,
        result: ToolResult,
        result_message: Message,
        terminal_event: EventRecord,
    ) -> Result<(), StoreError> {
        self.store.fenced(&self.fence, |state, run| {
            let call = state.calls.get(id).ok_or(StoreError::NotFound)?;
            if call.run_id != run.id || call.status != ToolCallStatus::Running { return Err(StoreError::Conflict); }
            let status = match result.status {
                ToolResultStatus::Completed => ToolCallStatus::Completed,
                ToolResultStatus::Failed => ToolCallStatus::Failed,
                ToolResultStatus::Interrupted => ToolCallStatus::Interrupted,
            };
            if !result_message.parts.iter().any(|part| matches!(&part.content, crabber_core::ContentBlock::ToolResult { call_id, .. } if call_id == id)) {
                return Err(StoreError::Validation("tool result message lacks matching result part".into()));
            }
            insert_message(state, run, result_message)?;
            insert_event(state, run, terminal_event)?;
            let call = state.calls.get_mut(id).expect("validated call exists");
            call.status = status;
            call.result = Some(result);
            *state.snapshot_revisions.entry(run.session_id.clone()).or_default() += 1;
            Ok(())
        })
    }

    async fn start_epoch(&self, epoch: ContextEpoch) -> Result<(), StoreError> {
        self.store.fenced(&self.fence, |state, run| {
            if epoch.run_id != run.id
                || epoch.session_id != run.session_id
                || epoch.parent.as_ref() != Some(&run.epoch_id)
                || state.epochs.contains_key(&epoch.id)
            {
                return Err(StoreError::Validation("invalid context epoch".into()));
            }
            state
                .runs
                .get_mut(&run.id)
                .expect("fenced run exists")
                .epoch_id = epoch.id.clone();
            state.epochs.insert(epoch.id.clone(), epoch);
            Ok(())
        })
    }

    async fn finish_epoch(&self, id: &EpochId, summary: Message) -> Result<(), StoreError> {
        self.store.fenced(&self.fence, |state, run| {
            let epoch = state.epochs.get(id).ok_or(StoreError::NotFound)?;
            if epoch.run_id != run.id || epoch.summary_message_id.is_some() || run.epoch_id != *id {
                return Err(StoreError::Conflict);
            }
            if !summary
                .parts
                .iter()
                .any(|part| part.kind == PartKind::CompactionSummary)
            {
                return Err(StoreError::Validation(
                    "summary message lacks compaction part".into(),
                ));
            }
            let summary_id = summary.id.clone();
            insert_message(state, run, summary)?;
            state
                .epochs
                .get_mut(id)
                .expect("validated epoch exists")
                .summary_message_id = Some(summary_id);
            Ok(())
        })
    }

    async fn pause_run(
        &self,
        checkpoint: serde_json::Value,
        event: EventRecord,
    ) -> Result<(), StoreError> {
        let now = self.store.clock.now();
        self.store.fenced(&self.fence, |state, run| {
            insert_event(state, run, event)?;
            let entry = state.runs.get_mut(&run.id).expect("fenced run exists");
            entry.status = RunStatus::Paused;
            entry.checkpoint = Some(checkpoint);
            entry.lease_until = now;
            entry.updated_at = now;
            Ok(())
        })
    }

    async fn settle_run(
        &self,
        status: RunStatus,
        error: Option<String>,
        usage: Usage,
        event: EventRecord,
    ) -> Result<(), StoreError> {
        let now = self.store.clock.now();
        self.store.fenced(&self.fence, |state, run| {
            if !status.is_terminal() {
                return Err(StoreError::Validation(
                    "run settlement must be terminal".into(),
                ));
            }
            if status == RunStatus::Completed
                && state
                    .inbox
                    .iter()
                    .any(|row| row.session_id == run.session_id && row.consumed_by_run.is_none())
            {
                return Err(StoreError::PendingInput);
            }
            insert_event(state, run, event)?;
            let entry = state.runs.get_mut(&run.id).expect("fenced run exists");
            entry.status = status;
            entry.error = error;
            entry.usage = usage;
            entry.updated_at = now;
            Ok(())
        })
    }

    async fn put_extension_state(
        &self,
        extension_id: &str,
        entries: Vec<(String, Option<String>)>,
    ) -> Result<(), StoreError> {
        let limits = self.store.limits.clone();
        self.store.fenced(&self.fence, |state, run| {
            let values = state
                .extension_values
                .entry((run.session_id.clone(), extension_id.to_owned()))
                .or_default();
            for (key, value) in entries {
                if let Some(value) = value {
                    values.insert(key, value);
                } else {
                    values.remove(&key);
                }
            }
            if values.len() > limits.max_state_entries
                || values
                    .iter()
                    .map(|(key, value)| key.len() + value.len())
                    .sum::<usize>()
                    > limits.max_state_bytes
            {
                return Err(StoreError::Limit("extension state".into()));
            }
            Ok(())
        })
    }

    async fn claim_inbox(&self, kind: InboxKind) -> Result<Vec<Message>, StoreError> {
        self.store.fenced(&self.fence, |state, run| {
            let mut claimed = Vec::new();
            for row in &mut state.inbox {
                if row.session_id == run.session_id
                    && row.kind == kind
                    && row.consumed_by_run.is_none()
                {
                    row.consumed_by_run = Some(run.id.clone());
                    claimed.push(row.message.clone());
                }
            }
            Ok(claimed)
        })
    }

    async fn claim_inbox_into_history(&self, kind: InboxKind) -> Result<Vec<Message>, StoreError> {
        self.store.fenced(&self.fence, |state, run| {
            let positions = state
                .inbox
                .iter()
                .enumerate()
                .filter(|(_, row)| {
                    row.session_id == run.session_id
                        && row.kind == kind
                        && row.consumed_by_run.is_none()
                })
                .map(|(index, _)| index)
                .collect::<Vec<_>>();
            let mut claimed = Vec::with_capacity(positions.len());
            for index in positions {
                let mut message = state.inbox[index].message.clone();
                message.run_id = Some(run.id.clone());
                insert_message(state, run, message.clone())?;
                state.inbox[index].consumed_by_run = Some(run.id.clone());
                claimed.push(message);
            }
            Ok(claimed)
        })
    }
}

#[cfg(test)]
mod atomic_claim_tests {
    use super::*;
    use crate::AdmitRequest;
    use crabber_core::{ContentBlock, EventKind, ManualClock, MessageId, PartId, Role};
    use std::time::Duration;

    fn input(session_id: &SessionId, text: &str, now: OffsetDateTime) -> Message {
        let id = MessageId::new();
        Message {
            id: id.clone(),
            session_id: session_id.clone(),
            run_id: None,
            role: Role::User,
            parent_id: None,
            parts: vec![Part {
                id: PartId::new(),
                message_id: id,
                ordinal: 0,
                kind: PartKind::UserInputText,
                content: ContentBlock::Text { text: text.into() },
            }],
            created_at: now,
        }
    }

    #[tokio::test]
    async fn failed_claim_append_rolls_back_and_reclaimed_owner_can_retry() {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let clock = Arc::new(ManualClock::new(now));
        let store = MemoryStore::with_clock(clock.clone());
        let session_id = SessionId::new();
        let user = input(&session_id, "first", now);
        let admitted = store
            .admit_run(AdmitRequest {
                session_id: None,
                workspace_id: "test".into(),
                directory: "/tmp".into(),
                title: "test".into(),
                user_message: user.clone(),
                config_hash: "config".into(),
                plan_fingerprint: "plan".into(),
                owner: "first owner".into(),
                lease: Duration::from_secs(30),
            })
            .await
            .unwrap();
        let first = input(&session_id, "one", now);
        let mut second = input(&session_id, "two", now);
        second.id = user.id.clone();
        second.parts[0].message_id = second.id.clone();
        store
            .enqueue_inbox(&session_id, InboxKind::Steer, first.clone())
            .await
            .unwrap();
        store
            .enqueue_inbox(&session_id, InboxKind::Steer, second)
            .await
            .unwrap();
        let execution = store.execution(admitted.fence.clone()).await.unwrap();
        assert_eq!(
            execution
                .claim_inbox_into_history(InboxKind::Steer)
                .await
                .unwrap_err(),
            StoreError::Conflict
        );
        assert_eq!(
            store.list_messages(&session_id, None).await.unwrap().len(),
            1
        );
        let settled = EventRecord {
            cursor: None,
            session_id: session_id.clone(),
            run_id: admitted.run.id.clone(),
            turn_id: None,
            kind: EventKind::RunSettled,
            payload: serde_json::Value::Null,
            correlation: None,
            live_only: false,
            created_at: now,
        };
        assert_eq!(
            execution
                .settle_run(
                    RunStatus::Completed,
                    None,
                    Usage::default(),
                    settled.clone()
                )
                .await
                .unwrap_err(),
            StoreError::PendingInput
        );

        clock.set(now + time::Duration::seconds(31));
        let reclaimed = store
            .claim_expired_run(&admitted.run.id, "second owner")
            .await
            .unwrap();
        assert_eq!(
            execution
                .claim_inbox_into_history(InboxKind::Steer)
                .await
                .unwrap_err(),
            StoreError::Conflict
        );
        // Repair the injected duplicate ID, then prove both rows are still available.
        {
            let mut state = store.state.lock().unwrap();
            let replacement = MessageId::new();
            state.inbox[1].message.id = replacement.clone();
            state.inbox[1].message.parts[0].message_id = replacement;
        }
        let recovered = store.execution(reclaimed).await.unwrap();
        let claimed = recovered
            .claim_inbox_into_history(InboxKind::Steer)
            .await
            .unwrap();
        assert_eq!(claimed.len(), 2);
        assert_eq!(claimed[0].id, first.id);
        assert_eq!(
            store.list_messages(&session_id, None).await.unwrap().len(),
            3
        );
        recovered
            .settle_run(RunStatus::Completed, None, Usage::default(), settled)
            .await
            .unwrap();
    }
}

#[cfg(test)]
#[path = "memory/abandon_tests.rs"]
mod abandon_tests;
#[path = "memory_snapshot.rs"]
mod snapshot_impl;

#[cfg(test)]
#[async_trait]
impl crate::abandonment_contract::FixtureStore for MemoryStore {
    async fn seed_run(&self, run: Run) {
        self.state.lock().unwrap().runs.insert(run.id.clone(), run);
    }
    async fn calls(&self, run: &RunId) -> Vec<ToolCallRecord> {
        self.state
            .lock()
            .unwrap()
            .calls
            .values()
            .filter(|c| c.run_id == *run)
            .cloned()
            .collect()
    }
    async fn unconsumed_inbox(&self, session: &SessionId) -> usize {
        self.state
            .lock()
            .unwrap()
            .inbox
            .iter()
            .filter(|r| r.session_id == *session && r.consumed_by_run.is_none())
            .count()
    }
}
#[cfg(test)]
#[tokio::test]
async fn shared_abandonment_contract() {
    let clock = Arc::new(crabber_core::ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    crate::abandonment_contract::run_contract(MemoryStore::with_clock(clock.clone()), clock).await;
}

#[cfg(test)]
#[tokio::test]
async fn untrusted_terminal_markers_are_never_replay_authority() {
    let clock = Arc::new(crabber_core::ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    crate::abandonment_contract::untrusted_terminal_contract(
        &MemoryStore::with_clock(clock.clone()),
        &clock,
    )
    .await;
}
