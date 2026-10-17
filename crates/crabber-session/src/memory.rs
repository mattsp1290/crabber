#[path = "memory/admission.rs"]
mod admission;
#[path = "memory/approval.rs"]
mod approval;
#[path = "memory/continuation_handoff.rs"]
mod continuation_handoff;
#[path = "memory/continuation_input.rs"]
mod continuation_input;
#[path = "memory/deletion.rs"]
mod deletion;
#[path = "memory/fork.rs"]
mod fork;
#[path = "memory/frontend.rs"]
mod frontend;
#[path = "memory/tool_continuation.rs"]
mod tool_continuation;
use admission::{admit_keyed_transaction, admit_transaction};
#[path = "memory/events.rs"]
mod events;
use events::EventLog;
#[path = "memory/history.rs"]
mod history;
use history::SessionSequence;
#[path = "memory/abandon.rs"]
mod abandon;
#[path = "memory/admission_execution.rs"]
mod admission_execution;
#[path = "memory/admission_journal.rs"]
mod admission_journal;
#[path = "memory/attempt.rs"]
mod attempt;
#[path = "memory/execution_binding.rs"]
mod execution_binding;
#[path = "memory/expired.rs"]
mod expired;
#[path = "memory/management.rs"]
mod management;
#[path = "memory/native_enrollment.rs"]
mod native_enrollment;
#[path = "memory/native_parent.rs"]
mod native_parent;
#[path = "memory/phase.rs"]
mod phase;
#[path = "memory/public_header.rs"]
mod public_header;
#[path = "memory/public_run.rs"]
mod public_run;
#[path = "memory/public_state.rs"]
mod public_state;
#[path = "memory/session_owner.rs"]
mod session_owner;
#[path = "memory/tool_result.rs"]
mod tool_result;

#[path = "memory/queue.rs"]
mod queue;
#[path = "memory/run_control.rs"]
mod run_control;

use crate::{
    AdmitOutcome, AdmitRequest, ExecutionStore, InboxKind, KeyedAdmitOutcome, KeyedAdmitRequest,
    Store, StoreError,
};
use async_trait::async_trait;
use crabber_core::{
    AdmissionKey, AdmissionReceipt, ByteLimits, Clock, ContextEpoch, EpochId, EventCursor,
    EventRecord, Message, Part, PartKind, Run, RunFence, RunId, RunStatus, Session, SessionId,
    SystemClock, ToolCallId, ToolCallRecord, ToolCallStatus, ToolResult, ToolResultStatus, Usage,
};
use std::{
    collections::{BTreeMap, BTreeSet},
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
    #[cfg(test)]
    public_state_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    management_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    session_owner_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    attempt_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    phase_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    approval_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    binding_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    queue_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    deletion_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    tool_continuation_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    control_fault: Arc<std::sync::atomic::AtomicU8>,
}

#[derive(Clone, Default)]
struct State {
    tombstones: BTreeMap<SessionId, crate::deletion::Tombstone>,
    ownership_origins: BTreeMap<SessionId, crate::deletion::origin::OwnershipOrigin>,
    preparation_admissions: BTreeMap<RunId, crate::session_owner::preparation::admission::Witness>,
    preparation_admission_joins:
        BTreeMap<(SessionId, crabber_core::SessionOwnerOperationId), RunId>,
    ownership_protocol_sessions: BTreeSet<SessionId>,
    native_run_issuances: BTreeMap<(SessionId, RunId, u64), crate::native_parent::RunIssuance>,
    native_session_protocols: BTreeSet<SessionId>,
    native_run_protocols: BTreeSet<(SessionId, RunId)>,
    native_run_sources:
        BTreeMap<(SessionId, RunId, u64), crate::native_parent::source_protocol::JournalSource>,
    native_run_source_heads:
        BTreeMap<(SessionId, RunId), crate::native_parent::source_protocol::JournalHead>,
    execution_enrollment_protocols: BTreeSet<(SessionId, RunId)>,
    execution_enrollment_origins: BTreeMap<(SessionId, RunId), crate::native_enrollment::Origin>,
    execution_enrollment_unclassified:
        BTreeMap<(SessionId, RunId), crate::native_enrollment::Unclassified>,
    native_parent_witnesses: BTreeMap<crabber_core::EventCursor, crate::native_parent::Witness>,
    suspension_proofs: BTreeMap<RunId, crate::run_control::SuspensionProof>,
    run_controls: BTreeMap<(SessionId, crabber_core::ControlId), crate::run_control::Record>,
    run_control_audits:
        BTreeMap<(SessionId, crabber_core::ControlId, u64), crate::RunControlSummary>,
    frontend_origins: BTreeMap<ToolCallId, crate::frontend_continuation::FrontendOriginRecord>,
    queue: BTreeMap<(SessionId, crabber_core::QueueId), crate::queue::Record>,
    queue_audits: BTreeMap<(SessionId, crabber_core::ControlId), crate::queue::Audit>,
    continuation_inputs: crate::continuation_input::Ledger,
    queue_publications: crate::queue::source::Ledger,
    tool_continuations:
        BTreeMap<crabber_core::ContinuationId, crate::tool_continuation::ToolContinuationRecord>,
    tool_barriers: BTreeMap<SessionId, crabber_core::ContinuationId>,
    #[cfg(test)]
    abandon_fail_after_tools: bool,
    #[cfg(test)]
    abandon_fault_boundary: u8,
    approvals: BTreeMap<crabber_core::ContinuationId, crate::approval::ApprovalRecord>,
    approval_barriers: BTreeMap<SessionId, crabber_core::ContinuationId>,
    abandonments: BTreeMap<RunId, crate::abandonment::AbandonCommit>,
    admission_executions: BTreeMap<RunId, crate::AdmissionExecutionRecord>,
    admission_execution_protocols: std::collections::BTreeSet<RunId>,
    admission_execution_sources:
        BTreeMap<(RunId, crate::admission_journal::Kind), crate::admission_journal::Source>,
    execution_bindings: BTreeMap<RunId, String>,
    receipts: BTreeMap<(SessionId, AdmissionKey), AdmissionReceipt>,
    sessions: BTreeMap<SessionId, Session>,
    session_owners: crate::session_owner::Ledger,
    management: BTreeMap<SessionId, crate::management::Metadata>,
    management_audits: BTreeMap<(SessionId, crabber_core::ControlId), crate::management::Audit>,
    fork_audits: BTreeMap<(SessionId, crabber_core::ControlId), crate::fork::Audit>,
    imported_calls: SessionSequence<crabber_core::ImportedToolCall>,
    imported_call_ids: std::collections::BTreeSet<ToolCallId>,
    imported_provenance: SessionSequence<crabber_core::ImportedMessageProvenance>,
    runs: BTreeMap<RunId, Run>,
    run_order: Vec<RunId>,
    messages: SessionSequence<Message>,
    events: EventLog,
    calls: BTreeMap<ToolCallId, ToolCallRecord>,
    call_order: SessionSequence<ToolCallId>,
    snapshot_revisions: BTreeMap<SessionId, u64>,
    public_revisions: BTreeMap<SessionId, crabber_core::SessionRevision>,
    public_contents: BTreeMap<SessionId, crabber_core::PublicStateContents>,
    operation_receipts: BTreeMap<(SessionId, crabber_core::ControlId), OperationReceipt>,
    attempts: BTreeMap<crabber_core::AttemptId, crate::attempt::AttemptRecord>,
    phases: BTreeMap<crabber_core::PhaseId, crate::phase::PhaseRecord>,
    epochs: BTreeMap<EpochId, ContextEpoch>,
    inbox: Vec<InboxRow>,
    extension_values: BTreeMap<(SessionId, String), BTreeMap<String, String>>,
}

#[derive(Clone, serde::Serialize)]
enum OperationReceipt {
    Fork(crate::ForkReceipt),
    RunControl(crate::RunControlReceipt),
    FrontendStaging(crate::FrontendStagingReceipt),
    Queue(crate::QueueReceipt),
    ToolContinuation(crate::ToolContinuationReceipt),
    Management(crate::ManagementReceipt),
    Approval(crate::ApprovalReceipt),
    PublicState(crate::PublicStateReceipt),
    Attempt(crate::AttemptReceipt),
    Phase(crate::PhaseReceipt),
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
            #[cfg(test)]
            public_state_fault: Arc::default(),
            #[cfg(test)]
            management_fault: Arc::default(),
            #[cfg(test)]
            session_owner_fault: Arc::default(),
            #[cfg(test)]
            attempt_fault: Arc::default(),
            #[cfg(test)]
            phase_fault: Arc::default(),
            #[cfg(test)]
            approval_fault: Arc::default(),
            #[cfg(test)]
            binding_fault: Arc::default(),
            #[cfg(test)]
            queue_fault: Arc::default(),
            #[cfg(test)]
            deletion_fault: Arc::default(),
            #[cfg(test)]
            tool_continuation_fault: Arc::default(),
            #[cfg(test)]
            control_fault: Arc::default(),
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

    fn transact_or_replay<T, E>(
        &self,
        before: impl FnOnce(&State) -> Result<Option<T>, E>,
        operation: impl FnOnce(&mut State) -> Result<T, E>,
    ) -> Result<T, E> {
        let mut guard = self.state.lock().expect("memory store poisoned");
        if let Some(replayed) = before(&guard)? {
            return Ok(replayed);
        }
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
            let previous = crate::native_parent::RunState::from_run(&run).map_err(|_| {
                StoreError::Validation("native run fence evidence unavailable".into())
            })?;
            let output = operation(state, &run)?;
            let updated = state.runs.get(&run.id).ok_or(StoreError::NotFound)?;
            let updated = crate::native_parent::RunState::from_run(updated).map_err(|_| {
                StoreError::Validation("native run fence evidence unavailable".into())
            })?;
            native_parent::record_run_states(state, Some(&previous), updated).map_err(|_| {
                StoreError::Validation("native run fence evidence unavailable".into())
            })?;
            Ok(output)
        })
    }
    fn fenced<T>(
        &self,
        fence: &RunFence,
        operation: impl FnOnce(&mut State, &Run) -> Result<T, StoreError>,
    ) -> Result<T, StoreError> {
        self.ownership_fenced(fence, |state, run| {
            admission_journal::verify(state, &run.id)?;
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

fn has_pending_continuation(state: &State, session: &SessionId) -> Result<bool, StoreError> {
    approval::verify_session(state, session)
        .map_err(|_| StoreError::Validation("approval integrity failure".into()))?;
    let pending = tool_continuation::pending_in_session(state, session)
        .map_err(|_| StoreError::Validation("invalid tool continuation evidence".into()))?;
    Ok(state.approval_barriers.contains_key(session) || pending)
}
fn bump_public_revision(state: &mut State, session: &SessionId) -> Result<(), StoreError> {
    let current = state
        .public_revisions
        .get(session)
        .copied()
        .unwrap_or(crabber_core::SessionRevision::INITIAL);
    state
        .public_revisions
        .insert(session.clone(), current.next()?);
    Ok(())
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
    if message.parts.iter().any(|part| {
        state
            .messages
            .iter()
            .any(|stored| stored.parts.iter().any(|old| old.id == part.id))
    }) {
        return Err(StoreError::Conflict);
    }
    crate::ensure_storable(&message)?;
    bump_public_revision(state, &run.session_id)?;
    state.messages.push(message.session_id.clone(), message);
    Ok(())
}

fn insert_event(state: &mut State, run: &Run, event: EventRecord) -> Result<(), StoreError> {
    insert_event_captured(state, run, event).map(|_| ())
}
fn insert_event_captured(
    state: &mut State,
    run: &Run,
    mut event: EventRecord,
) -> Result<EventCursor, StoreError> {
    if event.kind == crabber_core::EventKind::ToolCallPending
        && event.payload.get("frontend_origin").is_some()
    {
        return Err(StoreError::Validation(
            "frontend origin requires atomic staging".into(),
        ));
    }
    if matches!(
        event.kind,
        crabber_core::EventKind::RunControlChanged
            | crabber_core::EventKind::PublicStateSnapshot
            | crabber_core::EventKind::ExecutionAttemptChanged
            | crabber_core::EventKind::InteractionChanged
            | crabber_core::EventKind::ContinuationChanged
            | crabber_core::EventKind::ToolPhaseChanged
    ) {
        return Err(StoreError::Validation(
            "public state events require atomic state mutation".into(),
        ));
    }
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
    let cursor = state.events.next_cursor()?;
    event.cursor = Some(cursor);
    crate::ensure_storable(&event)?;
    bump_public_revision(state, &run.session_id)?;
    state.events.append(event)?;
    Ok(cursor)
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

#[async_trait]
impl Store for MemoryStore {
    async fn frontend_call_origin(
        &self,
        session: &SessionId,
        run: &RunId,
        call: &ToolCallId,
    ) -> Result<Option<crate::FrontendCallOrigin>, crate::ToolContinuationError> {
        crate::tool_continuation::canonical::identity(&session.0, 512)?;
        crate::tool_continuation::canonical::identity(&run.0, 128)?;
        crate::tool_continuation::canonical::identity(&call.0, 512)?;
        let state = self.state.lock().expect("memory store poisoned");
        let run = state
            .runs
            .get(run)
            .filter(|run| run.session_id == *session)
            .ok_or(crate::ToolContinuationError::NotFound)?;
        frontend::origin(&state, run, call)?
            .map(|record| {
                Ok(crate::FrontendCallOrigin {
                    call_id: record.request.call.id.clone(),
                    descriptor: record.request.descriptor.clone(),
                    descriptor_fingerprint: record.request.descriptor_fingerprint.clone(),
                    call_binding: crate::phase::call_binding(&record.request.call)
                        .map_err(|_| crate::ToolContinuationError::UnknownStoreOutcome)?,
                })
            })
            .transpose()
    }
    async fn lookup_frontend_staging(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crate::FrontendStagingReceipt>, crate::ToolContinuationError> {
        crate::tool_continuation::canonical::identity(&session.0, 512)?;
        crate::tool_continuation::canonical::identity(&id.0, 128)?;
        frontend::receipt(
            &self.state.lock().expect("memory store poisoned"),
            session,
            id,
        )
    }

    async fn public_history_v2(
        &self,
        request: crate::SnapshotRequest,
    ) -> Result<crate::PublicHistoryOutcome, StoreError> {
        Ok(self
            .read_snapshot_profile(
                &self.state.lock().unwrap(),
                &request,
                crate::snapshot_profile::Profile::AggregateV2,
            )?
            .into())
    }
    async fn public_control_header_v2(
        &self,
        request: crate::PublicHeaderRequestV2,
    ) -> Result<crate::PublicHeaderOutcomeV2, crate::PublicSessionError> {
        self.read_public_header_v2(&request)
    }
    async fn public_control_header(
        &self,
        request: crate::PublicHeaderRequest,
    ) -> Result<crate::PublicHeaderOutcome, crate::PublicSessionError> {
        self.read_public_header(&request)
    }

    async fn settle_waiting_cancellation(
        &self,
        request: crate::WaitingCancellation,
    ) -> Result<crate::RunControlSummary, crate::RunControlError> {
        self.control_settle_waiting(&request)
    }

    async fn run_control_binding(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<crate::RunControlBinding, crate::RunControlError> {
        self.control_binding(session, run)
    }

    fn supports_run_controls(&self) -> bool {
        true
    }
    async fn control_run(
        &self,
        request: crate::RunControlRequest,
    ) -> Result<crate::RunControlOutcome, crate::RunControlError> {
        self.control_accept(request)
    }
    async fn lookup_run_control(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crate::RunControlReceipt>, crate::RunControlError> {
        self.control_lookup(session, id)
    }
    async fn run_controls(
        &self,
        target: &crate::RunControlTarget,
    ) -> Result<crate::RunControlPage, crate::RunControlError> {
        self.control_page(target)
    }

    async fn mark_attempt_uncertain(
        &self,
        request: crate::AttemptUncertainty,
    ) -> Result<crate::AttemptControlOutcome, crate::AttemptError> {
        self.host_attempt(&crate::attempt_control::Control::Uncertain(request))
    }
    async fn retire_attempt(
        &self,
        request: crate::AttemptRetirement,
    ) -> Result<crate::AttemptControlOutcome, crate::AttemptError> {
        self.host_attempt(&crate::attempt_control::Control::Retire(request))
    }

    async fn queue_preparation_snapshot(
        &self,
        session: &SessionId,
    ) -> Result<crate::SessionSummary, StoreError> {
        self.queue_session_snapshot(session)
    }
    fn supports_future_queue(&self) -> bool {
        true
    }
    fn supports_owned_queue_preparation_inspection(&self) -> bool {
        true
    }
    async fn inspect_owned_queue_preparation(
        &self,
        context: &crabber_core::SessionContext,
        authority: &crate::QueueAuthority,
        expected_revision: crabber_core::SessionRevision,
    ) -> Result<crate::RetainedQueue, StoreError> {
        self.queue_owned_inspection(context, authority, expected_revision)
    }
    async fn mutate_queue(
        &self,
        request: crate::QueueMutation,
    ) -> Result<crate::QueueOutcome, StoreError> {
        self.queue_mutate(&request)
    }
    async fn lookup_queue(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crate::QueueReceipt>, StoreError> {
        self.queue_lookup(session, id)
    }
    async fn list_queue(
        &self,
        request: crate::QueuePageRequest,
    ) -> Result<crate::QueuePage, StoreError> {
        self.queue_list(&request)
    }
    async fn get_queue(
        &self,
        session: &SessionId,
        id: &crabber_core::QueueId,
    ) -> Result<crate::QueueSummary, StoreError> {
        self.queue_get(session, id)
    }
    async fn retained_queue(
        &self,
        session: &SessionId,
        id: &crabber_core::QueueId,
    ) -> Result<crate::RetainedQueue, StoreError> {
        self.queue_private(session, id)
    }

    fn supports_session_owners(&self) -> bool {
        true
    }
    async fn reserve_session_owner(
        &self,
        request: crate::session_owner::ReserveSessionOwner,
        parent: Option<crate::session_owner::SessionOwnerParentAuthority>,
    ) -> Result<crate::session_owner::SessionOwnerOutcome, crate::session_owner::SessionOwnerError>
    {
        self.reserve_owner(&request, parent.as_ref())
    }
    fn supports_queue_preparation_admission_inspection(&self) -> bool {
        true
    }
    async fn inspect_queue_preparation_admission(
        &self,
        original: &crate::session_owner::preparation::JoinedPreparation,
    ) -> Result<
        Option<crate::QueueReceipt>,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        self.inspect_queue_admission(original)
    }
    fn supports_initial_preparation_admission_inspection(&self) -> bool {
        true
    }
    async fn inspect_initial_preparation_admission(
        &self,
        original: &crate::session_owner::preparation::JoinedPreparation,
    ) -> Result<
        Option<crate::session_owner::preparation::PreparationAdmissionReceipt>,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        self.inspect_initial_admission(original)
    }
    fn supports_preparation_protocol(&self) -> bool {
        true
    }
    async fn reserve_preparation(
        &self,
        request: crate::session_owner::preparation::ReservePreparation,
    ) -> Result<crate::session_owner::SessionOwnerOutcome, crate::session_owner::SessionOwnerError>
    {
        self.reserve_preparation(&request)
    }
    async fn lookup_preparation(
        &self,
        session: &SessionId,
        key: &crate::session_owner::preparation::PreparationKey,
    ) -> Result<
        Option<crate::session_owner::preparation::PreparationReconciliation>,
        crate::session_owner::SessionOwnerError,
    > {
        let state = self.state.lock().unwrap();
        deletion::ensure_live(&state, session).map_err(crate::deletion::owner_error)?;
        state.session_owners.preparation_metadata(session, key)
    }
    fn supports_successor_execution_drivers(&self) -> bool {
        true
    }
    fn supports_successor_execution_driver_origin(
        &self,
        origin: crate::session_owner::driver::SuccessorExecutionDriverOrigin,
    ) -> bool {
        use crate::session_owner::driver::SuccessorExecutionDriverOrigin as Origin;
        matches!(
            origin,
            Origin::AdmissionRecovery
                | Origin::ExpiredRun
                | Origin::ApprovalClaim
                | Origin::ApprovalRecovery
        )
    }
    async fn register_successor_execution_driver(
        &self,
        request: crate::session_owner::driver::RegisterSuccessorExecutionDriver,
    ) -> Result<
        crate::session_owner::driver::SuccessorExecutionDriverOutcome,
        crate::session_owner::SessionOwnerError,
    > {
        self.register_successor_driver(&request)
    }
    fn supports_execution_drivers(&self) -> bool {
        true
    }
    async fn register_execution_driver(
        &self,
        request: crate::session_owner::driver::RegisterExecutionDriver,
    ) -> Result<
        crate::session_owner::driver::ExecutionDriverOutcome,
        crate::session_owner::SessionOwnerError,
    > {
        self.register_driver(&request)
    }
    fn supports_preparation_resources(&self) -> bool {
        true
    }
    fn supports_session_service_dependencies(&self) -> bool {
        true
    }
    async fn register_session_service_dependency(
        &self,
        request: crate::session_owner::service::RegisterSessionServiceDependency,
    ) -> Result<
        crate::session_owner::service::SessionServiceDependencyOutcome,
        crate::session_owner::SessionOwnerError,
    > {
        self.register_service(&request)
    }
    async fn check_session_service_dependency_access(
        &self,
        request: crate::session_owner::service::SessionServiceDependencyAccess,
    ) -> Result<(), crate::session_owner::SessionOwnerError> {
        self.check_service_access(&request)
    }
    async fn retain_preparation_resources(
        &self,
        grant: crate::session_owner::preparation::PreparationGrant,
    ) -> Result<
        crate::session_owner::preparation::PreparationResourceOutcome,
        crate::session_owner::SessionOwnerError,
    > {
        self.retain_preparation_resources(&grant)
    }
    async fn complete_preparation(
        &self,
        joined: crate::session_owner::preparation::JoinedPreparation,
    ) -> Result<
        crate::session_owner::preparation::PreparationCompletion,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        self.complete_preparation(&joined)
    }
    async fn complete_preparation_with_driver_source(
        &self,
        joined: crate::session_owner::preparation::JoinedPreparation,
    ) -> Result<
        crate::session_owner::preparation::PreparedDriverCompletion,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        crate::session_owner::preparation::driver_completion::validate_original(&joined)?;
        self.complete_preparation_captured(&joined, true)
    }
    async fn inspect_pending_approval_preparation(
        &self,
        request: crate::session_owner::preparation::InspectPendingApprovalPreparation,
    ) -> Result<
        crate::session_owner::preparation::PendingApprovalPreparationMetadata,
        crate::session_owner::SessionOwnerError,
    > {
        self.inspect_pending_approval(&request)
    }
    fn supports_pending_execution_driver_origin(
        &self,
        origin: crate::session_owner::driver::SuccessorExecutionDriverOrigin,
    ) -> bool {
        self.supports_successor_execution_driver_origin(origin)
    }
    async fn inspect_prepared_approval_driver(
        &self,
        request: crate::session_owner::preparation::InspectPreparedApprovalDriver,
    ) -> Result<
        crate::session_owner::preparation::PreparedApprovalDriverMetadata,
        crate::session_owner::SessionOwnerError,
    > {
        self.inspect_approval_driver(&request)
    }

    fn supports_session_owner_cancellation(&self) -> bool {
        true
    }
    async fn request_session_owner_cancel(
        &self,
        request: crate::session_owner::RequestSessionOwnerCancel,
    ) -> Result<crate::session_owner::SessionOwnerOutcome, crate::session_owner::SessionOwnerError>
    {
        self.request_owner_cancel(&request)
    }
    async fn observe_session_owner_cancel(
        &self,
        request: crate::session_owner::ObserveSessionOwnerCancel,
        authority: crate::session_owner::SessionOwnerAuthority,
    ) -> Result<crate::session_owner::SessionOwnerOutcome, crate::session_owner::SessionOwnerError>
    {
        self.observe_owner_cancel(&request, &authority)
    }
    async fn pending_session_owner_cancel(
        &self,
        authority: &crate::session_owner::SessionOwnerAuthority,
    ) -> Result<
        Option<crate::session_owner::SessionOwnerCancellation>,
        crate::session_owner::SessionOwnerError,
    > {
        self.pending_owner_cancel(authority)
    }
    fn supports_owned_scopes(&self) -> bool {
        true
    }
    /// Activate only an already registered exact local gate. Fresh Applied is
    /// the sole source of a dispatch permit; replay and unknown ACK never grant.
    async fn publish_owned_scope_activation(
        &self,
        request: crate::session_owner::owned_scope::OwnedScopeActivation,
        authority: crate::session_owner::SessionOwnerAuthority,
        parent: Option<crate::session_owner::owned_scope::OwnedScopeParentAuthority>,
    ) -> Result<crate::session_owner::SessionOwnerOutcome, crate::session_owner::SessionOwnerError>
    {
        self.publish_owned_scope_activation(&request, &authority, parent.as_ref())
    }
    /// Report exact observed main and sealed resource joins. Receipt replay
    /// never grants work and no lease expiry, timeout or Drop supplies proof.
    async fn join_owned_scope(
        &self,
        proof: crate::session_owner::owned_scope::JoinedOwnedScope,
        authority: crate::session_owner::SessionOwnerAuthority,
    ) -> Result<crate::session_owner::SessionOwnerOutcome, crate::session_owner::SessionOwnerError>
    {
        self.join_owned_scope(&proof, &authority)
    }
    async fn mutate_session_owner(
        &self,
        request: crate::session_owner::MutateSessionOwner,
        authority: Option<crate::session_owner::SessionOwnerAuthority>,
    ) -> Result<crate::session_owner::SessionOwnerOutcome, crate::session_owner::SessionOwnerError>
    {
        self.mutate_owner(&request, authority.as_ref())
    }
    async fn get_session_owner(
        &self,
        session: &SessionId,
        id: &crabber_core::SessionOwnerId,
    ) -> Result<crate::session_owner::SessionOwnerSnapshot, crate::session_owner::SessionOwnerError>
    {
        let state = self.state.lock().unwrap();
        deletion::ensure_live(&state, session).map_err(crate::deletion::owner_error)?;
        state.session_owners.snapshot(session, id)
    }
    async fn lookup_session_owner(
        &self,
        session: &SessionId,
        id: &crabber_core::SessionOwnerOperationId,
    ) -> Result<
        Option<crate::session_owner::SessionOwnerReceipt>,
        crate::session_owner::SessionOwnerError,
    > {
        let state = self.state.lock().unwrap();
        deletion::ensure_live(&state, session).map_err(crate::deletion::owner_error)?;
        state.session_owners.receipt(session, id)
    }

    async fn delete_session(
        &self,
        request: crate::DeleteSession,
    ) -> Result<crate::DeletionOutcome, crate::DeletionError> {
        self.guarded_delete(request)
    }
    async fn lookup_session_deletion(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crate::DeletionReceipt>, crate::DeletionError> {
        self.deletion_receipt(session, id)
    }
    async fn session_disposition(
        &self,
        session: &SessionId,
    ) -> Result<crate::SessionDisposition, crate::DeletionError> {
        self.disposition(session)
    }
    async fn fork_session(
        &self,
        request: crate::ForkSession,
    ) -> Result<crate::ForkOutcome, crate::ForkError> {
        self.fork(request)
    }
    async fn lookup_session_fork(
        &self,
        target: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crate::ForkReceipt>, crate::ForkError> {
        self.fork_receipt(target, id)
    }
    async fn manage_session(
        &self,
        request: crate::ManageSession,
    ) -> Result<crate::ManagementReceipt, crate::ManagementError> {
        self.manage(request)
    }
    async fn lookup_session_management(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crate::ManagementReceipt>, crate::ManagementError> {
        self.management_receipt(session, id)
    }
    async fn public_run_state(
        &self,
        session: &SessionId,
        max_bytes: usize,
    ) -> Result<crate::PublicRunOutcome, StoreError> {
        self.read_public_run(session, max_bytes)
    }
    async fn session_summary(
        &self,
        session: &SessionId,
    ) -> Result<crate::SessionSummary, crate::ManagementError> {
        self.read_summary(session)
    }
    async fn list_sessions(
        &self,
        request: crate::ListSessions,
    ) -> Result<crate::SessionListPage, crate::ManagementError> {
        self.summaries(&request)
    }

    async fn approval_snapshot(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<Option<crate::ApprovalSnapshot>, crate::ApprovalError> {
        self.read_approval(session, run)
    }
    async fn original_approval_plan(
        &self,
        session: &SessionId,
        run: &RunId,
        target: &crate::ContinuationTarget,
    ) -> Result<crabber_core::InputFingerprint, crate::ApprovalError> {
        self.read_original_approval_plan(session, run, target)
    }
    async fn lookup_approval(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crate::ApprovalReceipt>, crate::ApprovalError> {
        self.read_approval_receipt(session, id)
    }
    async fn answer_approval(
        &self,
        answer: crate::ApprovalAnswer,
    ) -> Result<crate::ApprovalReceipt, crate::ApprovalError> {
        self.answer_approval_inner(&answer)
    }
    async fn terminate_approval(
        &self,
        request: crate::ApprovalTermination,
    ) -> Result<crate::ApprovalReceipt, crate::ApprovalError> {
        self.end_approval(&request)
    }
    async fn reclaim_approval_continuation(
        &self,
        request: crate::ApprovalContinuationRecovery,
    ) -> Result<crate::ApprovalClaimOutcome, crate::ApprovalError> {
        self.recover_approval(&request)
    }
    async fn claim_approval_continuation(
        &self,
        claim: crate::ApprovalContinuationClaim,
    ) -> Result<crate::ApprovalClaimOutcome, crate::ApprovalError> {
        self.claim_approval(&claim)
    }

    fn supports_owned_attempts(&self) -> bool {
        true
    }
    fn supports_owned_tool_phases(&self) -> bool {
        true
    }
    fn supports_tool_continuations(&self) -> bool {
        true
    }
    async fn tool_continuation_snapshot(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<Option<crate::ToolContinuationSnapshot>, crate::ToolContinuationError> {
        self.read_tools(session, run)
    }
    async fn original_tool_continuation_plan(
        &self,
        session: &SessionId,
        run: &RunId,
        target: &crate::ContinuationTarget,
    ) -> Result<crabber_core::InputFingerprint, crate::ToolContinuationError> {
        self.read_original_tool_plan(session, run, target)
    }
    async fn lookup_tool_continuation(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crate::ToolContinuationReceipt>, crate::ToolContinuationError> {
        self.tool_receipt(session, id)
    }
    async fn answer_tool_interaction(
        &self,
        answer: crate::ToolInteractionAnswer,
    ) -> Result<crate::ToolContinuationReceipt, crate::ToolContinuationError> {
        self.answer_tools(&answer)
    }
    async fn recover_tool_continuation(
        &self,
        request: crate::ToolContinuationRecovery,
    ) -> Result<crate::ToolContinuationClaimOutcome, crate::ToolContinuationError> {
        self.recover_tools(&request)
    }
    async fn claim_tool_continuation(
        &self,
        claim: crate::ToolContinuationClaim,
    ) -> Result<crate::ToolContinuationClaimOutcome, crate::ToolContinuationError> {
        self.claim_tools(&claim)
    }
    async fn claim_frontend_continuation(
        &self,
        claim: crate::FrontendContinuationClaim,
    ) -> Result<crate::ToolContinuationClaimOutcome, crate::ToolContinuationError> {
        self.claim_frontend(&claim)
    }
    fn supports_frontend_tools(&self) -> bool {
        true
    }
    async fn recover_frontend_continuation(
        &self,
        request: crate::FrontendContinuationRecovery,
    ) -> Result<crate::ToolContinuationClaimOutcome, crate::ToolContinuationError> {
        self.recover_frontend(&request)
    }
    fn supports_pre_effect_approvals(&self) -> bool {
        true
    }

    async fn tool_phase(
        &self,
        run: &RunId,
        id: &crabber_core::PhaseId,
    ) -> Result<crate::VersionedPhase, crate::PhaseError> {
        self.read_phase(run, id)
    }
    async fn mark_phase_uncertain(
        &self,
        request: crate::PhaseUncertainty,
    ) -> Result<crate::PhaseControlOutcome, crate::PhaseError> {
        request.fingerprint()?;
        self.host_phase(
            &request.run_id,
            request.mutation(),
            crate::phase::PhaseEffect::Uncertainty(&request),
        )
    }
    async fn record_phase_cleanup(
        &self,
        request: crate::PhaseCleanup,
    ) -> Result<crate::PhaseControlOutcome, crate::PhaseError> {
        request.fingerprint()?;
        self.host_phase(
            &request.run_id,
            request.mutation(),
            crate::phase::PhaseEffect::Cleanup(&request),
        )
    }
    async fn list_tool_phases(
        &self,
        request: crate::PhasePageRequest,
    ) -> Result<crate::PhasePage, crate::PhaseError> {
        self.phase_page(&request)
    }
    async fn resolve_uncertain_phase(
        &self,
        resolution: crate::PhaseResolution,
    ) -> Result<crate::PhaseResolutionOutcome, crate::PhaseError> {
        self.resolve_phase(&resolution)
    }
    async fn lookup_phase_mutation(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crate::PhaseReceipt>, crate::PhaseError> {
        self.phase_receipt(session, id)
    }
    async fn execution_attempt(
        &self,
        run: &RunId,
        id: &crabber_core::AttemptId,
    ) -> Result<crate::VersionedAttempt, crate::AttemptError> {
        self.read_attempt(run, Some(id))?
            .ok_or(crate::AttemptError::NotFound)
    }
    async fn latest_execution_attempt(
        &self,
        run: &RunId,
    ) -> Result<Option<crate::VersionedAttempt>, crate::AttemptError> {
        self.read_attempt(run, None)
    }
    async fn lookup_attempt_mutation(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crate::AttemptReceipt>, crate::AttemptError> {
        self.attempt_receipt(session, id)
    }

    async fn public_state(
        &self,
        session: &SessionId,
    ) -> Result<crate::PublicStateSnapshot, crate::PublicStateError> {
        self.read_public_state(session)
    }
    async fn lookup_public_state_mutation(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crate::PublicStateReceipt>, crate::PublicStateError> {
        self.public_receipt(session, id)
    }

    async fn session_revision(
        &self,
        session: &SessionId,
    ) -> Result<crabber_core::SessionRevision, StoreError> {
        let state = self.state.lock().expect("memory store poisoned");
        deletion::ensure_live(&state, session)?;
        if !state.sessions.contains_key(session) {
            return Err(StoreError::NotFound);
        }
        Ok(state
            .public_revisions
            .get(session)
            .copied()
            .unwrap_or(crabber_core::SessionRevision::INITIAL))
    }
    async fn admit_run(&self, request: AdmitRequest) -> Result<AdmitOutcome, StoreError> {
        self.transact(|state| admit_transaction(state, &request, self.clock.now(), false))
    }

    async fn admit_keyed_run(
        &self,
        keyed: KeyedAdmitRequest,
    ) -> Result<KeyedAdmitOutcome, StoreError> {
        keyed.validated_digest()?;
        self.transact(|state| admit_keyed_transaction(state, &keyed, self.clock.now()))
    }
    async fn admit_bound_run(
        &self,
        request: crate::BoundAdmitRequest,
    ) -> Result<AdmitOutcome, StoreError> {
        self.admit_with_binding(request)
    }
    async fn admit_bound_keyed_run(
        &self,
        request: crate::BoundKeyedAdmitRequest,
    ) -> Result<KeyedAdmitOutcome, StoreError> {
        self.admit_keyed_with_binding(request)
    }
    async fn execution_binding(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<Option<crate::RetainedExecutionBinding>, StoreError> {
        self.read_execution_binding(session, run)
    }

    async fn lookup_admission(
        &self,
        session: &SessionId,
        key: &AdmissionKey,
    ) -> Result<Option<AdmissionReceipt>, StoreError> {
        let state = self.state.lock().expect("memory store poisoned");
        deletion::ensure_live(&state, session)?;
        Ok(state.receipts.get(&(session.clone(), key.clone())).cloned())
    }

    async fn load_admission_execution(
        &self,
        session: &SessionId,
        key: &AdmissionKey,
    ) -> Result<Option<crate::AdmissionExecutionRecord>, crate::AdmissionExecutionError> {
        self.load_execution_record(session, key)
            .map_err(|_| crate::AdmissionExecutionError::UnknownStoreFailure)
    }
    async fn lookup_expired_run_preparation(
        &self,
        session: &SessionId,
        run: &RunId,
        issuance: u64,
    ) -> Result<
        Option<crate::session_owner::preparation::ExpiredRunReceipt>,
        crate::session_owner::SessionOwnerError,
    > {
        self.lookup_expired_source(session, run, issuance)
    }
    async fn inspect_expired_run_preparation(
        &self,
        request: crate::session_owner::preparation::InspectExpiredRunPreparation,
    ) -> Result<
        crate::session_owner::preparation::ExpiredRunPreparation,
        crate::session_owner::SessionOwnerError,
    > {
        self.inspect_expired_source(request)
    }
    async fn inspect_admission_recovery(
        &self,
        request: crate::ClaimUnstartedAdmissionRequest,
    ) -> Result<
        crate::session_owner::preparation::AdmissionRecoveryPreparation,
        crate::AdmissionExecutionError,
    > {
        self.inspect_admission_source(request)
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
                        ToolCallStatus::Pending | ToolCallStatus::Running | ToolCallStatus::Waiting
                    )
            })
            .cloned()
            .collect())
    }

    async fn admission_execution_state(
        &self,
        run: &RunId,
    ) -> Result<Option<crate::AdmissionExecutionState>, StoreError> {
        let state = self.state.lock().expect("memory store poisoned");
        admission_journal::verify(&state, run)?;
        Ok(state
            .admission_executions
            .get(run)
            .map(|record| record.state))
    }

    async fn claim_expired_run(&self, id: &RunId, owner: &str) -> Result<RunFence, StoreError> {
        self.transact(|state| {
            let scope = &state.runs.get(id).ok_or(StoreError::NotFound)?.session_id;
            if state.session_owners.has_preparation(scope) {
                return Err(StoreError::AdmissionRecoveryRequired);
            }

            let now = self.clock.now();
            admission_journal::verify(state, id)?;
            if state
                .admission_executions
                .get(id)
                .is_some_and(|r| r.state == crate::AdmissionExecutionState::Unstarted)
            {
                return Err(StoreError::AdmissionRecoveryRequired);
            }
            let scoped = state.runs.get(id).ok_or(StoreError::NotFound)?;
            if tool_continuation::pending_in_session(state, &scoped.session_id)
                .map_err(|_| StoreError::Validation("invalid tool continuation evidence".into()))?
            {
                return Err(StoreError::Busy);
            }
            let previous = crate::native_parent::RunState::from_run(
                state.runs.get(id).ok_or(StoreError::NotFound)?,
            )
            .map_err(|_| StoreError::Validation("native run fence evidence unavailable".into()))?;
            let run = state.runs.get_mut(id).ok_or(StoreError::NotFound)?;
            if run.status.is_terminal() || run.lease_until > now {
                return Err(StoreError::Conflict);
            }
            owner.clone_into(&mut run.owner);
            run.claim_token = uuid::Uuid::new_v4().to_string();
            run.lease_until = now + time::Duration::seconds(30);
            run.updated_at = now;
            crate::ensure_storable(run)?;
            let fence = RunFence {
                run_id: id.clone(),
                claim_token: run.claim_token.clone(),
            };
            let updated = crate::native_parent::RunState::from_run(run).map_err(|_| {
                StoreError::Validation("native run fence evidence unavailable".into())
            })?;
            native_parent::record_run_states(state, Some(&previous), updated).map_err(|_| {
                StoreError::Validation("native run fence evidence unavailable".into())
            })?;
            Ok(fence)
        })
    }

    async fn get_extension_state(
        &self,
        extension_id: &str,
        session: &SessionId,
    ) -> Result<BTreeMap<String, String>, StoreError> {
        let state = self.state.lock().expect("memory store poisoned");
        deletion::ensure_live(&state, session)?;
        Ok(state
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
            deletion::ensure_live(state, session)?;
            if !state.sessions.contains_key(session) {
                return Err(StoreError::NotFound);
            }
            if &message.session_id != session {
                return Err(StoreError::Validation(
                    "inbox message has wrong session".into(),
                ));
            }
            crate::ensure_storable(&message)?;
            state.inbox.push(InboxRow {
                session_id: session.clone(),
                kind,
                message,
                consumed_by_run: None,
            });
            bump_public_revision(state, session)?;
            Ok(())
        })
    }
}

#[async_trait]
impl ExecutionStore for MemoryExecution {
    async fn stage_frontend_call(
        &self,
        request: crate::FrontendCallStaging,
        authority: crate::AttemptAuthority,
    ) -> Result<crate::FrontendStagingReceipt, crate::ToolContinuationError> {
        self.stage_frontend(&request, &authority)
    }

    async fn suspend_tool_continuation(
        &self,
        batch: crate::ToolContinuationSuspension,
        authority: crate::ToolSuspensionAuthority,
    ) -> Result<crate::ToolContinuationReceipt, crate::ToolContinuationError> {
        self.suspend_tools(&batch, &authority)
    }

    async fn observe_run_controls(
        &self,
        turn: Option<&crabber_core::TurnId>,
    ) -> Result<crate::RunControlBoundary, crate::RunControlError> {
        self.control_boundary(turn)
    }

    async fn start_tool_phase(
        &self,
        start: crate::PhaseStart,
        authority: Option<crate::PhaseAuthority>,
    ) -> Result<crate::PhaseMutationOutcome, crate::PhaseError> {
        self.start_phase(&start, authority)
    }

    async fn suspend_for_approval(
        &self,
        batch: crate::ApprovalSuspension,
        authority: crate::AttemptAuthority,
    ) -> Result<crate::ApprovalReceipt, crate::ApprovalError> {
        self.suspend_approval(&batch, &authority)
    }

    async fn conclude_attempt(
        &self,
        conclusion: crate::AttemptConclusion,
        authority: Option<crate::AttemptAuthority>,
    ) -> Result<crate::AttemptMutationOutcome, crate::AttemptError> {
        self.conclude_owned_attempt(&conclusion, authority.as_ref())
    }

    async fn settle_tool_phase(
        &self,
        settlement: crate::PhaseSettlement,
        authority: Option<crate::PhaseAuthority>,
    ) -> Result<crate::PhaseMutationOutcome, crate::PhaseError> {
        self.settle_phase(&settlement, authority)
    }

    async fn mutate_phase(
        &self,
        request: crate::PhaseMutation,
        authority: Option<crate::PhaseWriteAuthority>,
    ) -> Result<crate::PhaseMutationOutcome, crate::PhaseError> {
        self.change_phase(request, authority.as_ref())
    }
    async fn mutate_attempt(
        &self,
        request: crate::AttemptMutation,
        authority: Option<crate::AttemptAuthority>,
    ) -> Result<crate::AttemptMutationOutcome, crate::AttemptError> {
        self.change_attempt(&request, authority.as_ref())
    }

    async fn mutate_public_state(
        &self,
        mutation: crate::PublicStateMutation,
    ) -> Result<crate::PublicStateReceipt, crate::PublicStateError> {
        self.change_public_state(mutation)
    }

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
            for attempt in state
                .attempts
                .values_mut()
                .filter(|a| a.snapshot.run_id == run.id && a.run_token == run.claim_token)
            {
                attempt.lease_until = until;
            }
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
            crate::ensure_storable(message)?;
            *state
                .snapshot_revisions
                .entry(run.session_id.clone())
                .or_default() += 1;
            bump_public_revision(state, &run.session_id)?;
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
                || state.imported_call_ids.contains(&call.id)
            {
                return Err(StoreError::Validation("invalid new tool call".into()));
            }
            if frontend::declared(state, run, &call.name)
                .map_err(|_| StoreError::Validation("invalid frontend origin evidence".into()))?
            {
                return Err(StoreError::Validation(
                    "frontend calls require atomic staging".into(),
                ));
            }
            crate::ensure_storable(&call)?;
            insert_event(state, run, pending_event)?;
            state
                .call_order
                .push(run.session_id.clone(), call.id.clone());
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
            if frontend::origin(state, run, id)
                .map_err(|_| StoreError::Validation("frontend origin integrity failure".into()))?
                .is_some()
            {
                return Err(StoreError::Conflict);
            }
            if state
                .phases
                .values()
                .any(|p| p.snapshot.run_id == run.id && p.snapshot.target.call_id == *id)
            {
                return Err(StoreError::Conflict);
            }
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
            if frontend::origin(state, run, id).map_err(|_| StoreError::Validation("frontend origin integrity failure".into()))?.is_some() {
                return Err(StoreError::Conflict);
            }
            if state.phases.values().any(|p| p.snapshot.run_id == run.id && p.snapshot.target.call_id == *id) {
                return Err(StoreError::Conflict);
            }
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
            crate::ensure_storable(call)?;
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
            crate::ensure_storable(&epoch)?;
            state.epochs.insert(epoch.id.clone(), epoch);
            bump_public_revision(state, &run.session_id)?;
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
            if state
                .attempts
                .values()
                .any(|record| record.snapshot.run_id == run.id)
                || state
                    .phases
                    .values()
                    .any(|record| record.snapshot.run_id == run.id)
                || state.approval_barriers.contains_key(&run.session_id)
                || state.tool_barriers.contains_key(&run.session_id)
            {
                return Err(StoreError::Conflict);
            }
            insert_event(state, run, event)?;
            let entry = state.runs.get_mut(&run.id).expect("fenced run exists");
            entry.status = RunStatus::Paused;
            entry.checkpoint = Some(checkpoint);
            entry.lease_until = now;
            entry.updated_at = now;
            crate::ensure_storable(entry)?;
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
            if state
                .attempts
                .values()
                .any(|record| record.snapshot.run_id == run.id)
                || state
                    .phases
                    .values()
                    .any(|record| record.snapshot.run_id == run.id)
                || state.approval_barriers.contains_key(&run.session_id)
                || state.tool_barriers.contains_key(&run.session_id)
            {
                return Err(StoreError::Conflict);
            }
            if !status.is_terminal() {
                return Err(StoreError::Validation(
                    "run settlement must be terminal".into(),
                ));
            }
            if status == RunStatus::Completed
                && (run_control::pending(state, run).map_err(crate::run_control::store_error)?
                    || state.inbox.iter().any(|row| {
                        row.session_id == run.session_id && row.consumed_by_run.is_none()
                    }))
            {
                return Err(StoreError::PendingInput);
            }
            insert_event(state, run, event)?;
            let entry = state.runs.get_mut(&run.id).expect("fenced run exists");
            entry.status = status;
            entry.error = error;
            entry.usage = usage;
            entry.updated_at = now;
            crate::ensure_storable(entry)?;
            let terminal = entry.clone();
            run_control::finalize(state, &terminal, now)?;
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
            crate::ensure_storable_text(extension_id)?;
            for (key, value) in &entries {
                crate::ensure_storable_text(key)?;
                if let Some(value) = value {
                    crate::ensure_storable_text(value)?;
                }
            }
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
            if !claimed.is_empty() {
                bump_public_revision(state, &run.session_id)?;
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
    #[allow(clippy::too_many_lines)] // Full rollback and reclaimed-owner fixture.
    async fn failed_claim_append_rolls_back_and_reclaimed_owner_can_retry() {
        let now = OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let clock = Arc::new(ManualClock::new(now));
        let store = MemoryStore::with_clock(clock.clone());
        let session_id = SessionId::new();
        let user = input(&session_id, "first", now);
        let admitted = store
            .admit_run(AdmitRequest {
                input_envelope: None,
                additional_messages: Vec::new(),
                expected_revision: None,
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
        let mut state = self.state.lock().unwrap();
        let previous = state.runs.get(&run.id).cloned();
        // This helper establishes an intentional fixture state, unlike the
        // direct private-row corruption probes that must leave audits intact.
        native_parent::record_run(&mut state, previous.as_ref(), &run).unwrap();
        state.runs.insert(run.id.clone(), run);
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

#[cfg(test)]
#[path = "memory/run_control_tests.rs"]
mod run_control_tests;
