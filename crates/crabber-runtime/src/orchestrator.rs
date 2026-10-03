use crate::observation::{Measurement, result_reason};
use crate::policy::{
    ApprovalRequester, DefaultDenyApprover, IdentityToolPipeline, InterruptPolicy,
    PermissionDecision, PermissionPolicy, StaticPolicy, ToolPipeline,
};
use crate::{
    ModelPurpose, MonotonicClock, OperationKind, OperationalObservation, SystemMonotonicClock,
    TerminalReason,
};
use async_trait::async_trait;
use crabber_core::TraceContext;
use crabber_core::event_payload::{self, StreamOutcome};
use crabber_core::{
    AdmissionKey, AdmissionOptions, AdmissionReceipt, Clock, ContentBlock, ContextEpoch, EpochId,
    EventKind, EventRecord, Message, MessageId, Part, PartId, PartKind, Role, RunFence, RunId,
    RunStatus, Session, SessionId, SystemClock, ToolCallId, ToolCallRecord, ToolCallStatus,
    ToolInfo, ToolResult, ToolResultStatus, TurnId, Usage,
};
use crabber_extension::{
    ApprovalFacade, Callback, ContextAssemble, EventPublished, GuardContext, GuardDecision,
    HostServices, ModelCompleted, ModelRequestError, ModelRequested,
    ModelStream as ExtensionModelStream, RunAdmitted, RunBeforeExecute, RunPlan, RunPlanProvider,
    RunSettled, RunStarted, StateSink, ToolContext, ToolDefinition, ToolExecute, ToolPrepare,
    ToolResultTransform, TurnCompleted, TurnPrepare, TurnStarted, WorkspaceContext,
};
use crabber_providers::{
    DeltaStream, ModelRequest, ProviderError, RequestIdentity, Resolver, Selection, StreamDelta,
    Streamer,
};
use crabber_session::{
    AdmissionExecutionCapsule, AdmissionExecutionError, AdmissionExecutionState,
    AdmissionRequestData, AdmitRequest, ClaimUnstartedAdmissionRequest, ExecutionStore, InboxKind,
    KeyedAdmitOutcome, KeyedAdmitRequest, Store, StoreError, admission_config_hash,
};
use futures::{StreamExt, TryStreamExt};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use time::OffsetDateTime;
use tokio::sync::{oneshot, watch};
use tokio_util::sync::CancellationToken;

fn admission_error(error: StoreError) -> RuntimeError {
    if error == StoreError::Busy {
        RuntimeError::SessionBusy
    } else {
        RuntimeError::Store(error)
    }
}

#[derive(Debug, Clone)]
pub struct Request {
    pub session_id: Option<SessionId>,
    pub workspace_id: String,
    pub directory: String,
    pub title: String,
    pub text: String,
    pub selection: Selection,
    pub system_prompt: Option<String>,
}

#[derive(Clone, Copy)]
enum InitialEntry {
    Unkeyed,
    Keyed,
}
struct InitialRequest {
    request: Request,
    entry: InitialEntry,
}

impl Request {
    fn admission_data(&self) -> Result<AdmissionRequestData, StoreError> {
        Ok(AdmissionRequestData {
            session_id: self.session_id.clone().ok_or(StoreError::NotFound)?,
            workspace_id: self.workspace_id.clone(),
            directory: self.directory.clone(),
            title: self.title.clone(),
            text: self.text.clone(),
            provider_id: self.selection.provider_id.clone(),
            model_id: self.selection.model_id.clone(),
            system_prompt: self.system_prompt.clone(),
        })
    }
    fn from_admission(data: AdmissionRequestData) -> Self {
        Self {
            session_id: Some(data.session_id),
            workspace_id: data.workspace_id,
            directory: data.directory,
            title: data.title,
            text: data.text,
            selection: Selection {
                provider_id: data.provider_id,
                model_id: data.model_id,
            },
            system_prompt: data.system_prompt,
        }
    }
}

/// A replay has no execution handle and grants no lease authority.
pub enum Admission {
    Started {
        receipt: AdmissionReceipt,
        handle: RunHandle,
    },
    Replayed(AdmissionReceipt),
}

impl Admission {
    #[must_use]
    pub fn receipt(&self) -> &AdmissionReceipt {
        match self {
            Self::Started { receipt, .. } | Self::Replayed(receipt) => receipt,
        }
    }
}

impl std::fmt::Debug for Admission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Started { receipt, .. } => f.debug_tuple("Started").field(receipt).finish(),
            Self::Replayed(receipt) => f.debug_tuple("Replayed").field(receipt).finish(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunResult {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub status: RunStatus,
    pub usage: Usage,
}

/// Outcome of a recovery sweep: the runs it recovered and the expired runs it
/// left unfinished. A skipped run keeps its status and fence; the host can
/// inspect it with `resume` and dispose of it with fenced abandonment.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RecoverReport {
    pub recovered: Vec<RunResult>,
    pub skipped: Vec<SkippedRun>,
}

/// An expired run a recovery sweep did not resume, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedRun {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub reason: StoreError,
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("session is busy")]
    SessionBusy,
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("admission execution: {0}")]
    AdmissionExecution(#[from] AdmissionExecutionError),
    #[error("provider: {0}")]
    Provider(#[from] ProviderError),
    #[error("extension: {0}")]
    Extension(String),
    #[error("turn limit reached")]
    TurnLimit,
    #[error("run task stopped")]
    TaskStopped,
    #[error("orchestrator is missing {0}")]
    Missing(&'static str),
    #[error("invalid orchestrator configuration: {0}")]
    InvalidConfiguration(&'static str),
    #[error("run lease ownership was lost")]
    LeaseLost,
    #[error("run interrupted")]
    Interrupted,
    #[error("run paused")]
    Paused,
    #[error("run plan changed since checkpoint")]
    PlanChanged,
}

#[derive(Debug, Clone)]
pub struct TurnSnapshot {
    pub identity: RequestIdentity,
    pub selection: Selection,
    pub messages: Vec<Message>,
    pub system: Option<String>,
    pub tools: Vec<ToolInfo>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExecutionMode {
    #[default]
    Sequential,
    Parallel {
        max: usize,
    },
}

#[derive(Debug, Clone, Copy)]
pub struct CompactionPolicy {
    pub trigger_ratio: f64,
    pub keep_tail_messages: usize,
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            trigger_ratio: 0.85,
            keep_tail_messages: 8,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ConfigSnapshot {
    pub execution_mode: ExecutionMode,
    pub compaction: CompactionPolicy,
    pub max_turns: usize,
}

impl Default for ConfigSnapshot {
    fn default() -> Self {
        Self {
            execution_mode: ExecutionMode::Sequential,
            compaction: CompactionPolicy::default(),
            max_turns: 64,
        }
    }
}

/// Callbacks execute inline: implementations must remain bounded and nonblocking.
pub trait Observer: Send + Sync {
    /// One local execution measurement; does not assert durable settlement.
    fn operational_completed(&self, _observation: &OperationalObservation) {}
    fn operational_completed_in_attempt(
        &self,
        observation: &OperationalObservation,
        _context: Option<&TraceContext>,
        _attempt: &RunId,
    ) {
        self.operational_completed(observation);
    }
    fn emit(&self, event: &EventRecord);
    fn model_completed(&self, _event: &EventRecord) {}
    /// Contextual callbacks default to the legacy callbacks for existing implementors.
    fn emit_with_context(&self, event: &EventRecord, _context: Option<&TraceContext>) {
        self.emit(event);
    }
    fn model_completed_with_context(&self, event: &EventRecord, _context: Option<&TraceContext>) {
        self.model_completed(event);
    }
    /// Execution identity is fresh even when the host supplies no trace context.
    fn emit_in_attempt(
        &self,
        event: &EventRecord,
        context: Option<&TraceContext>,
        _attempt: &RunId,
    ) {
        self.emit_with_context(event, context);
    }
    fn model_completed_in_attempt(
        &self,
        event: &EventRecord,
        context: Option<&TraceContext>,
        _attempt: &RunId,
    ) {
        self.model_completed_with_context(event, context);
    }
}

// Owned by one execution attempt; cloning the runtime into tasks keeps the identity
// explicit and immutable, without depending on task-local or global tracing state.
struct ContextObserver {
    inner: Arc<dyn Observer>,
    context: Option<TraceContext>,
    attempt: RunId,
}
impl Observer for ContextObserver {
    fn operational_completed(&self, observation: &OperationalObservation) {
        self.inner.operational_completed_in_attempt(
            observation,
            self.context.as_ref(),
            &self.attempt,
        );
    }
    fn emit(&self, event: &EventRecord) {
        self.inner
            .emit_in_attempt(event, self.context.as_ref(), &self.attempt);
    }
    fn model_completed(&self, event: &EventRecord) {
        self.inner
            .model_completed_in_attempt(event, self.context.as_ref(), &self.attempt);
    }
}

pub struct NoopObserver;
impl Observer for NoopObserver {
    fn emit(&self, _event: &EventRecord) {}
}

#[async_trait]
pub trait ModelStream: Send + Sync {
    async fn stream(
        &self,
        request: ModelRequest,
        next: Arc<dyn Streamer>,
    ) -> Result<DeltaStream, ProviderError>;
}

pub struct DirectModelStream;
#[async_trait]
impl ModelStream for DirectModelStream {
    async fn stream(
        &self,
        request: ModelRequest,
        next: Arc<dyn Streamer>,
    ) -> Result<DeltaStream, ProviderError> {
        next.stream(request).await
    }
}

struct SingleUseStreamer {
    inner: Arc<dyn Streamer>,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Streamer for SingleUseStreamer {
    async fn stream(&self, request: ModelRequest) -> Result<DeltaStream, ProviderError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) != 0 {
            return Err(invalid_provider(
                "model stream wrapper called provider more than once",
            ));
        }
        self.inner.stream(request).await
    }
}

#[derive(Clone)]
pub struct Orchestrator {
    store: Arc<dyn Store>,
    resolver: Arc<dyn Resolver>,
    plan_provider: Arc<dyn RunPlanProvider>,
    clock: Arc<dyn Clock>,
    monotonic_clock: Arc<dyn MonotonicClock>,
    observer: Arc<dyn Observer>,
    model_stream: Arc<dyn ModelStream>,
    policy: Arc<dyn PermissionPolicy>,
    approver: Arc<dyn ApprovalRequester>,
    tool_pipeline: Arc<dyn ToolPipeline>,
    host_services: HostServices,
    max_turns: usize,
    heartbeat_interval: Duration,
    execution_mode: ExecutionMode,
    compaction: CompactionPolicy,
    /// Workspace identity of the run this clone executes, read from the
    /// persisted session. `None` outside a run: nothing else may fill it.
    workspace: Option<WorkspaceContext>,
}

#[derive(Default)]
pub struct OrchestratorBuilder {
    store: Option<Arc<dyn Store>>,
    resolver: Option<Arc<dyn Resolver>>,
    plan_provider: Option<Arc<dyn RunPlanProvider>>,
    clock: Option<Arc<dyn Clock>>,
    monotonic_clock: Option<Arc<dyn MonotonicClock>>,
    observer: Option<Arc<dyn Observer>>,
    model_stream: Option<Arc<dyn ModelStream>>,
    policy: Option<Arc<dyn PermissionPolicy>>,
    approver: Option<Arc<dyn ApprovalRequester>>,
    tool_pipeline: Option<Arc<dyn ToolPipeline>>,
    host_services: Option<HostServices>,
    max_turns: Option<usize>,
    heartbeat_interval: Option<Duration>,
    execution_mode: Option<ExecutionMode>,
    compaction: Option<CompactionPolicy>,
}

impl OrchestratorBuilder {
    #[must_use]
    pub fn store(mut self, value: Arc<dyn Store>) -> Self {
        self.store = Some(value);
        self
    }
    #[must_use]
    pub fn resolver(mut self, value: Arc<dyn Resolver>) -> Self {
        self.resolver = Some(value);
        self
    }
    #[must_use]
    pub fn plan_provider(mut self, value: Arc<dyn RunPlanProvider>) -> Self {
        self.plan_provider = Some(value);
        self
    }
    #[must_use]
    pub fn clock(mut self, value: Arc<dyn Clock>) -> Self {
        self.clock = Some(value);
        self
    }
    /// Injects execution timing independently of event/lease wall time.
    #[must_use]
    pub fn monotonic_clock(mut self, value: Arc<dyn MonotonicClock>) -> Self {
        self.monotonic_clock = Some(value);
        self
    }
    #[must_use]
    pub fn observer(mut self, value: Arc<dyn Observer>) -> Self {
        self.observer = Some(value);
        self
    }
    #[must_use]
    pub fn model_stream(mut self, value: Arc<dyn ModelStream>) -> Self {
        self.model_stream = Some(value);
        self
    }
    #[must_use]
    pub fn policy(mut self, value: Arc<dyn PermissionPolicy>) -> Self {
        self.policy = Some(value);
        self
    }
    #[must_use]
    pub fn approver(mut self, value: Arc<dyn ApprovalRequester>) -> Self {
        self.approver = Some(value);
        self
    }
    #[must_use]
    pub fn tool_pipeline(mut self, value: Arc<dyn ToolPipeline>) -> Self {
        self.tool_pipeline = Some(value);
        self
    }
    #[must_use]
    pub fn host_services(mut self, value: HostServices) -> Self {
        self.host_services = Some(value);
        self
    }
    #[must_use]
    pub fn max_turns(mut self, value: usize) -> Self {
        self.max_turns = Some(value);
        self
    }
    #[must_use]
    pub fn heartbeat_interval(mut self, value: Duration) -> Self {
        self.heartbeat_interval = Some(value);
        self
    }
    #[must_use]
    pub fn execution_mode(mut self, value: ExecutionMode) -> Self {
        self.execution_mode = Some(value);
        self
    }
    #[must_use]
    pub fn compaction(mut self, value: CompactionPolicy) -> Self {
        self.compaction = Some(value);
        self
    }
    #[must_use]
    pub fn config_snapshot(mut self, value: ConfigSnapshot) -> Self {
        self.execution_mode = Some(value.execution_mode);
        self.compaction = Some(value.compaction);
        self.max_turns = Some(value.max_turns);
        self
    }

    /// Builds an orchestrator from its required dependencies.
    ///
    /// # Errors
    ///
    /// Returns `Missing` if the store, resolver, or plan provider is absent.
    pub fn build(self) -> Result<Orchestrator, RuntimeError> {
        let heartbeat_interval = self.heartbeat_interval.unwrap_or(Duration::from_secs(5));
        if heartbeat_interval.is_zero() || heartbeat_interval >= Duration::from_secs(15) {
            return Err(RuntimeError::InvalidConfiguration(
                "heartbeat interval must be positive and below 15 seconds",
            ));
        }
        if matches!(
            self.execution_mode,
            Some(ExecutionMode::Parallel { max: 0 })
        ) {
            return Err(RuntimeError::InvalidConfiguration(
                "parallel max must be positive",
            ));
        }
        if self.max_turns == Some(0) {
            return Err(RuntimeError::InvalidConfiguration(
                "max turns must be positive",
            ));
        }
        let compaction = self.compaction.unwrap_or_default();
        if !(0.0..=1.0).contains(&compaction.trigger_ratio) || compaction.trigger_ratio == 0.0 {
            return Err(RuntimeError::InvalidConfiguration(
                "compaction trigger ratio must be in (0,1]",
            ));
        }
        Ok(Orchestrator {
            store: self.store.ok_or(RuntimeError::Missing("store"))?,
            resolver: self.resolver.ok_or(RuntimeError::Missing("resolver"))?,
            plan_provider: self
                .plan_provider
                .ok_or(RuntimeError::Missing("plan provider"))?,
            clock: self.clock.unwrap_or_else(|| Arc::new(SystemClock)),
            monotonic_clock: self
                .monotonic_clock
                .unwrap_or_else(|| Arc::new(SystemMonotonicClock::default())),
            observer: self.observer.unwrap_or_else(|| Arc::new(NoopObserver)),
            model_stream: self
                .model_stream
                .unwrap_or_else(|| Arc::new(DirectModelStream)),
            policy: self
                .policy
                .unwrap_or_else(|| Arc::new(StaticPolicy::default())),
            approver: self
                .approver
                .unwrap_or_else(|| Arc::new(DefaultDenyApprover)),
            tool_pipeline: self
                .tool_pipeline
                .unwrap_or_else(|| Arc::new(IdentityToolPipeline)),
            host_services: self.host_services.unwrap_or_default(),
            max_turns: self.max_turns.unwrap_or(64),
            heartbeat_interval,
            execution_mode: self.execution_mode.unwrap_or_default(),
            compaction,
            workspace: None,
        })
    }
}

pub struct RunHandle {
    session_id: SessionId,
    run_id: RunId,
    store: Arc<dyn Store>,
    clock: Arc<dyn Clock>,
    done: oneshot::Receiver<Result<RunResult, RuntimeError>>,
    completion: watch::Receiver<bool>,
    cancellation: CancellationToken,
}

struct RunStateSink {
    store: Arc<dyn Store>,
    execution: Arc<dyn ExecutionStore>,
    session_id: SessionId,
    lost: Arc<AtomicBool>,
}

#[async_trait]
impl StateSink for RunStateSink {
    fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    async fn snapshot(&self, extension_id: &str) -> Result<BTreeMap<String, String>, String> {
        if self.lost.load(Ordering::Acquire) {
            return Err("run lease lost".into());
        }
        self.store
            .get_extension_state(extension_id, &self.session_id)
            .await
            .map_err(|error| error.to_string())
    }

    async fn apply(
        &self,
        extension_id: &str,
        changes: Vec<(String, Option<String>)>,
    ) -> Result<(), String> {
        if self.lost.load(Ordering::Acquire) {
            return Err("run lease lost".into());
        }
        self.execution
            .put_extension_state(extension_id, changes)
            .await
            .map_err(|error| error.to_string())
    }
}

struct HeartbeatGuard {
    task: tokio::task::JoinHandle<()>,
    lost: Arc<AtomicBool>,
    signal: watch::Receiver<bool>,
    _signal_sender: watch::Sender<bool>,
}

impl HeartbeatGuard {
    fn start(
        execution: Arc<dyn ExecutionStore>,
        store: Arc<dyn Store>,
        fence: RunFence,
        clock: Arc<dyn Clock>,
        period: Duration,
    ) -> Self {
        let lost = Arc::new(AtomicBool::new(false));
        let lost_in_task = Arc::clone(&lost);
        let (sender, signal) = watch::channel(false);
        let task_sender = sender.clone();
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                if execution
                    .renew_lease(clock.now() + time::Duration::seconds(30))
                    .await
                    .is_err()
                {
                    let settled_by_this_owner = store
                        .get_run(&fence.run_id)
                        .await
                        .ok()
                        .flatten()
                        .is_some_and(|run| {
                            run.status.is_terminal() && run.claim_token == fence.claim_token
                        });
                    if settled_by_this_owner {
                        break;
                    }
                    lost_in_task.store(true, Ordering::SeqCst);
                    let _ = task_sender.send(true);
                    break;
                }
            }
        });
        Self {
            task,
            lost,
            signal,
            _signal_sender: sender,
        }
    }
}

impl Drop for HeartbeatGuard {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl RunHandle {
    /// Cancels the active provider stream or tool body. The run task performs
    /// durable settlement before `done` resolves.
    pub fn interrupt(&self) {
        self.cancellation.cancel();
    }
    /// Signals when the run task has returned, whether it succeeded or failed.
    #[must_use]
    pub fn completion_signal(&self) -> watch::Receiver<bool> {
        self.completion.clone()
    }
    #[must_use]
    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }
    #[must_use]
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    /// Waits for the run's terminal result.
    ///
    /// # Errors
    ///
    /// Returns the run failure or `TaskStopped` if its task ended unexpectedly.
    pub async fn done(self) -> Result<RunResult, RuntimeError> {
        self.done.await.map_err(|_| RuntimeError::TaskStopped)?
    }
    /// Queues input for the next model request.
    ///
    /// # Errors
    ///
    /// Returns a store error if the inbox write fails.
    pub async fn steer(&self, text: impl Into<String>) -> Result<(), RuntimeError> {
        self.enqueue(InboxKind::Steer, text.into()).await
    }
    /// Queues input after pending tool work finishes.
    ///
    /// # Errors
    ///
    /// Returns a store error if the inbox write fails.
    pub async fn follow_up(&self, text: impl Into<String>) -> Result<(), RuntimeError> {
        self.enqueue(InboxKind::FollowUp, text.into()).await
    }
    async fn enqueue(&self, kind: InboxKind, text: String) -> Result<(), RuntimeError> {
        let message = user_message(self.session_id.clone(), text, self.clock.now());
        self.store
            .enqueue_inbox(&self.session_id, kind, message)
            .await?;
        Ok(())
    }
}

impl Orchestrator {
    #[must_use]
    pub fn builder() -> OrchestratorBuilder {
        OrchestratorBuilder::default()
    }

    /// Rebuilds the latest assistant batch when a resumed process died after
    /// committing its message but before staging every call.
    async fn reconcile_committed_assistant(
        &self,
        execution: &dyn ExecutionStore,
        run: &crabber_core::Run,
        plan: &RunPlan,
        lease_lost: &AtomicBool,
    ) -> Result<Option<bool>, RuntimeError> {
        if run.status != RunStatus::Paused {
            return Ok(None);
        }
        let Some(checkpoint) = &run.checkpoint else {
            return Ok(None);
        };
        let paused_calls: HashSet<&str> = checkpoint
            .get("pending_calls")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect();
        if paused_calls.is_empty() {
            return Ok(None);
        }
        let history = self.store.list_all_messages(&run.session_id).await?;
        let Some(anchor) = history.iter().rposition(|message| message.parts.iter().any(|part|
            matches!(&part.content, ContentBlock::ToolCall { call_id, .. } if paused_calls.contains(call_id.0.as_str())))) else {
            return Ok(None);
        };
        let latest = history[anchor + 1..].iter().rev().find(|message| {
            message.run_id.as_ref() == Some(&run.id)
                && message.role == Role::Assistant
                && message
                    .parts
                    .iter()
                    .any(|part| part.kind != PartKind::CompactionSummary)
        });
        let Some(latest) = latest else {
            return Ok(None);
        };
        let settled: HashSet<ToolCallId> = history
            .iter()
            .flat_map(|message| &message.parts)
            .filter_map(|part| match &part.content {
                ContentBlock::ToolResult { call_id, .. } => Some(call_id.clone()),
                _ => None,
            })
            .collect();
        let unfinished: HashSet<ToolCallId> = self
            .store
            .list_unfinished_tool_calls(&run.id)
            .await?
            .into_iter()
            .map(|call| call.id)
            .collect();
        let mut has_calls = false;
        for part in &latest.parts {
            if let ContentBlock::ToolCall {
                call_id,
                name,
                arguments,
            } = &part.content
            {
                has_calls = true;
                if !settled.contains(call_id) && !unfinished.contains(call_id) {
                    ensure_lease(lease_lost)?;
                    self.stage_tool(
                        execution,
                        &run.session_id,
                        &run.id,
                        plan,
                        PendingCall {
                            message_id: Some(latest.id.clone()),
                            turn_id: None,
                            args_completed: true,
                            id: call_id.clone(),
                            name: name.clone(),
                            raw: String::new(),
                            arguments: Some(arguments.clone()),
                        },
                        lease_lost,
                    )
                    .await?;
                }
            }
        }
        Ok(Some(has_calls))
    }

    /// Reclaims a paused or expired run and finishes its persisted tool calls.
    ///
    /// # Errors
    ///
    /// Returns a conflict for a live lease, or `PlanChanged` if registered
    /// tools and handlers no longer match the admitted plan.
    pub async fn resume(&self, run_id: &RunId) -> Result<RunResult, RuntimeError> {
        self.resume_with_context(run_id, None).await
    }

    /// Reclaims one run with explicit current attempt correlation identity.
    /// The host persists predecessor identity separately from execution authority.
    /// # Errors
    /// Returns the same fencing and plan errors as `resume`.
    pub async fn resume_with_context(
        &self,
        run_id: &RunId,
        context: Option<TraceContext>,
    ) -> Result<RunResult, RuntimeError> {
        Box::pin(self.with_context(context).resume_attempt(run_id)).await
    }

    async fn resume_attempt(&self, run_id: &RunId) -> Result<RunResult, RuntimeError> {
        let run = self
            .store
            .get_run(run_id)
            .await?
            .ok_or(StoreError::NotFound)?;
        // Stored-record identity check, before plan acquisition, the claim and
        // any pending tool execution: a rejection leaves the run untouched.
        let session = self.validated_session(&run).await?;
        Box::pin(self.with_workspace(&session).resume_validated(run_id, run)).await
    }

    /// Loads the run's persisted session and compares it with the request saved
    /// in the run checkpoint, when there is one.
    async fn validated_session(&self, run: &crabber_core::Run) -> Result<Session, StoreError> {
        let session = self
            .store
            .get_session(&run.session_id)
            .await?
            .ok_or(StoreError::NotFound)?;
        ensure_checkpoint_identity(&session, run.checkpoint.as_ref())?;
        Ok(session)
    }

    #[allow(clippy::too_many_lines)]
    async fn resume_validated(
        &self,
        run_id: &RunId,
        run: crabber_core::Run,
    ) -> Result<RunResult, RuntimeError> {
        if self.store.admission_execution_state(run_id).await?
            == Some(crabber_session::AdmissionExecutionState::Unstarted)
        {
            return Err(StoreError::AdmissionRecoveryRequired.into());
        }
        let plan = self
            .plan_provider
            .acquire_plan(&run.session_id)
            .await
            .map_err(|error| RuntimeError::Extension(error.to_string()))?;
        if plan.fingerprint().to_string() != run.plan_fingerprint {
            plan.release();
            return Err(RuntimeError::PlanChanged);
        }
        let fence = self
            .store
            .claim_expired_run(run_id, &RunId::new().to_string())
            .await?;
        let measurement_lost = Arc::new(AtomicBool::new(false));
        let measurement_cancellation = CancellationToken::new();
        let mut measurement = Measurement::new(
            self.observer.as_ref(),
            self.monotonic_clock.as_ref(),
            &run.session_id,
            run_id,
            OperationKind::Run,
            &measurement_cancellation,
            &measurement_lost,
        );
        let execution = match self.store.execution(fence.clone()).await {
            Ok(execution) => execution,
            Err(error) => {
                if self.verified_lease_loss(&fence).await {
                    measurement_lost.store(true, Ordering::SeqCst);
                }
                return Err(error.into());
            }
        };
        let execution: Arc<dyn ExecutionStore> = execution.into();
        let heartbeat = HeartbeatGuard::start(
            Arc::clone(&execution),
            Arc::clone(&self.store),
            fence.clone(),
            Arc::clone(&self.clock),
            self.heartbeat_interval,
        );
        let mut signal = heartbeat.signal.clone();
        let lost = Arc::clone(&heartbeat.lost);
        let cancellation = CancellationToken::new();
        let work_cancellation = cancellation.clone();
        let state_sink: Arc<dyn StateSink> = Arc::new(RunStateSink {
            store: Arc::clone(&self.store),
            execution: Arc::clone(&execution),
            session_id: run.session_id.clone(),
            lost: Arc::clone(&lost),
        });
        let work_fence = fence.clone();
        let observed_lost = Arc::clone(&measurement_lost);
        let resumed_work = async move {
            let resumed = self.event(&run.session_id, run_id, EventKind::RunResumed);
            execution.append_event(resumed.clone()).await?;
            self.observer.emit(&resumed);
            let committed_turn = self
                .reconcile_committed_assistant(execution.as_ref(), &run, &plan, lost.as_ref())
                .await?;
            let mut interrupted = false;
            for call in self.store.list_unfinished_tool_calls(run_id).await? {
                if call.status == ToolCallStatus::Running
                    || (run.status != RunStatus::Paused && !call.retry_safe)
                {
                    if call.status == ToolCallStatus::Pending {
                        let event = self.event(&run.session_id, run_id, EventKind::ToolCallRunning);
                        execution.claim_tool_call(&call.id, event).await?;
                    }
                    self.settle_interrupted_call(
                        execution.as_ref(),
                        &run.session_id,
                        run_id,
                        &plan,
                        &call,
                        None,
                    )
                    .await?;
                    interrupted = true;
                    continue;
                }
                let pending = PendingCall {
                    message_id: None,
                    turn_id: None,
                    args_completed: true,
                    id: call.id.clone(),
                    name: call.name.clone(),
                    raw: String::new(),
                    arguments: Some(call.arguments.clone()),
                };
                self.execute_tool(
                    execution.as_ref(),
                    &run.session_id,
                    run_id,
                    &plan,
                    pending,
                    Some(call),
                    None,
                    &work_cancellation,
                    lost.as_ref(),
                )
                .await?;
            }
            if run.status == RunStatus::Paused
                && let Some(checkpoint) = &run.checkpoint
                && let Some(request) = request_from_checkpoint(&run.session_id, checkpoint)
            {
                let usage = checkpoint
                    .get("usage")
                    .cloned()
                    .and_then(|value| serde_json::from_value(value).ok())
                    .unwrap_or_default();
                if committed_turn == Some(false) {
                    let event = self.run_settled_event(
                        &run.session_id,
                        run_id,
                        RunStatus::Completed,
                        &usage,
                        run.created_at,
                    );
                    match execution
                        .settle_run(RunStatus::Completed, None, usage.clone(), event.clone())
                        .await
                    {
                        Ok(()) => {
                            self.observer.emit(&event);
                            plan.release();
                            return Ok(RunResult {
                                session_id: run.session_id,
                                run_id: run_id.clone(),
                                status: RunStatus::Completed,
                                usage,
                            });
                        }
                        Err(StoreError::PendingInput) => {
                            self.claim_input(execution.as_ref(), InboxKind::Steer)
                                .await?;
                            self.claim_input(execution.as_ref(), InboxKind::FollowUp)
                                .await?;
                        }
                        Err(error) => return Err(error.into()),
                    }
                }
                self.emit_durable(
                    execution.as_ref(),
                    &run.session_id,
                    run_id,
                    &plan,
                    EventKind::TurnCompleted,
                )
                .await?;
                let outcome = self
                    .run_loop(
                        execution.as_ref(),
                        run_id,
                        &run.session_id,
                        &request,
                        &plan,
                        &work_cancellation,
                        false,
                        usage,
                        lost.as_ref(),
                        &std::sync::Mutex::new(None),
                    )
                    .await;
                let result = match outcome {
                    Ok(usage) => Ok(RunResult {
                        session_id: run.session_id.clone(),
                        run_id: run_id.clone(),
                        status: RunStatus::Completed,
                        usage,
                    }),
                    Err(RuntimeError::Paused) => Ok(RunResult {
                        session_id: run.session_id.clone(),
                        run_id: run_id.clone(),
                        status: RunStatus::Paused,
                        usage: Usage::default(),
                    }),
                    Err(error) => {
                        if lost.load(Ordering::SeqCst)
                            || matches!(error, RuntimeError::LeaseLost)
                            || self.verified_lease_loss(&work_fence).await
                        {
                            observed_lost.store(true, Ordering::SeqCst);
                            plan.release();
                            return Err(error);
                        }
                        self.settle_unfinished_calls(
                            execution.as_ref(),
                            &run.session_id,
                            run_id,
                            &plan,
                            true,
                            None,
                        )
                        .await?;
                        let event = self.run_settled_event(
                            &run.session_id,
                            run_id,
                            RunStatus::Failed,
                            &Usage::default(),
                            run.created_at,
                        );
                        execution
                            .settle_run(
                                RunStatus::Failed,
                                Some(error.to_string()),
                                Usage::default(),
                                event.clone(),
                            )
                            .await?;
                        self.observer.emit(&event);
                        Err(error)
                    }
                };
                plan.release();
                return result;
            }
            let status = if interrupted || run.status != RunStatus::Paused {
                RunStatus::Interrupted
            } else {
                RunStatus::Completed
            };
            let event =
                self.run_settled_event(&run.session_id, run_id, status, &run.usage, run.created_at);
            execution
                .settle_run(status, None, run.usage.clone(), event.clone())
                .await?;
            self.observer.emit(&event);
            plan.release();
            Ok(RunResult {
                session_id: run.session_id,
                run_id: run_id.clone(),
                status,
                usage: run.usage,
            })
        };
        let resumed_work = crabber_extension::with_state_sink(state_sink, resumed_work);
        tokio::pin!(resumed_work);
        let result = tokio::select! {
            biased;
            update = signal.changed() => match update {
                Ok(()) if *signal.borrow_and_update() => {
                    cancellation.cancel();
                    Err(RuntimeError::LeaseLost)
                }
                Ok(()) | Err(_) => resumed_work.as_mut().await,
            },
            result = &mut resumed_work => result,
        };
        drop(heartbeat);
        if result.is_err() && self.verified_lease_loss(&fence).await {
            measurement_lost.store(true, Ordering::SeqCst);
        }
        measurement.observation.reason = result_reason(&result);
        result
    }

    /// Reclaims expired unfinished runs. Live leases are left to their owners.
    /// Use `recover_report` to also learn which expired runs were skipped.
    ///
    /// # Errors
    ///
    /// Returns a store or execution error for a run it successfully claims.
    pub async fn recover(&self) -> Result<Vec<RunResult>, RuntimeError> {
        self.recover_with_context(|_| None).await
    }

    /// Like `recover`, and also reports each expired run it left unfinished.
    ///
    /// # Errors
    ///
    /// Returns the same errors as `recover`.
    pub async fn recover_report(&self) -> Result<RecoverReport, RuntimeError> {
        self.recover_report_with_context(|_| None).await
    }

    /// Selects fresh correlation independently for each eligible run.
    /// The selector cannot grant a lease; live leases and competing claims stay fenced.
    /// # Errors
    /// Returns the same store and execution errors as `recover`.
    pub async fn recover_with_context<F>(
        &self,
        context_for: F,
    ) -> Result<Vec<RunResult>, RuntimeError>
    where
        F: FnMut(&crabber_core::Run) -> Option<TraceContext> + Send,
    {
        Ok(self
            .recover_report_with_context(context_for)
            .await?
            .recovered)
    }

    /// Recovery sweep with per-run correlation that reports skipped runs.
    /// # Errors
    /// Returns the same store and execution errors as `recover`.
    pub async fn recover_report_with_context<F>(
        &self,
        mut context_for: F,
    ) -> Result<RecoverReport, RuntimeError>
    where
        F: FnMut(&crabber_core::Run) -> Option<TraceContext> + Send,
    {
        let mut report = RecoverReport::default();
        for run in self.store.list_unfinished_runs().await? {
            if run.lease_until > self.clock.now() {
                continue;
            }
            let mut skip = |reason: StoreError| {
                report.skipped.push(SkippedRun {
                    session_id: run.session_id.clone(),
                    run_id: run.id.clone(),
                    reason,
                });
            };
            // One run whose session is missing, undecodable or drifted must not
            // stop the sweep; it stays unfinished and is reported instead.
            match self.validated_session(&run).await {
                Err(
                    reason @ (StoreError::NotFound
                    | StoreError::Validation(_)
                    | StoreError::SessionIdentityMismatch),
                ) => {
                    skip(reason);
                    continue;
                }
                Err(error) => return Err(error.into()),
                Ok(_) => {}
            }
            match self.resume_with_context(&run.id, context_for(&run)).await {
                Ok(result) => report.recovered.push(result),
                Err(RuntimeError::Store(
                    reason @ (StoreError::Conflict
                    | StoreError::AdmissionRecoveryRequired
                    | StoreError::SessionIdentityMismatch),
                )) => skip(reason),
                Err(error) => return Err(error),
            }
        }
        Ok(report)
    }

    async fn settle_unfinished_calls(
        &self,
        execution: &dyn ExecutionStore,
        session_id: &SessionId,
        run_id: &RunId,
        plan: &RunPlan,
        include_pending: bool,
        turn_id: Option<&TurnId>,
    ) -> Result<(), RuntimeError> {
        for call in self.store.list_unfinished_tool_calls(run_id).await? {
            if call.status == ToolCallStatus::Pending && !include_pending {
                continue;
            }
            if call.status == ToolCallStatus::Pending {
                let mut running = self.event(session_id, run_id, EventKind::ToolCallRunning);
                running.turn_id = turn_id.cloned();
                execution.claim_tool_call(&call.id, running).await?;
            }
            self.settle_interrupted_call(execution, session_id, run_id, plan, &call, turn_id)
                .await?;
        }
        Ok(())
    }

    async fn settle_interrupted_call(
        &self,
        execution: &dyn ExecutionStore,
        session_id: &SessionId,
        run_id: &RunId,
        plan: &RunPlan,
        call: &ToolCallRecord,
        turn_id: Option<&TurnId>,
    ) -> Result<(), RuntimeError> {
        let content = vec![ContentBlock::Text {
            text: "interrupted".into(),
        }];
        let message_id = MessageId::new();
        let message = Message {
            id: message_id.clone(),
            session_id: session_id.clone(),
            run_id: Some(run_id.clone()),
            role: Role::Tool,
            parent_id: None,
            parts: vec![Part {
                id: PartId::new(),
                message_id,
                ordinal: 0,
                kind: PartKind::FunctionToolResult,
                content: ContentBlock::ToolResult {
                    call_id: call.id.clone(),
                    content: content.clone(),
                    is_error: true,
                },
            }],
            created_at: self.clock.now(),
        };
        let mut event = self.event(session_id, run_id, EventKind::ToolCallSettled);
        event.turn_id = turn_id.cloned();
        event.payload = json!(crate::event_payload::Settlement {
            result: event_payload::CallSettled {
                call_id: call.id.clone(),
                message_id: message.id.clone(),
                content: std::borrow::Cow::Borrowed(&content),
                is_error: true,
            },
            diagnostics: json!({"name": call.name, "status": "interrupted"}),
        });
        event.correlation = Some(call.id.to_string());
        execution
            .settle_tool_call(
                &call.id,
                ToolResult {
                    status: ToolResultStatus::Interrupted,
                    content,
                },
                message,
                event.clone(),
            )
            .await?;
        self.observer.emit(&event);
        plan.dispatcher
            .notify::<EventPublished>(serde_json::to_value(&event).unwrap_or(Value::Null))
            .await;
        Ok(())
    }

    /// Admits a run and starts its turn loop in the background.
    ///
    /// # Errors
    ///
    /// Returns `SessionBusy` for a session with an active run, or a plan/store error.
    pub async fn start(&self, request: Request) -> Result<RunHandle, RuntimeError> {
        self.start_with_context(request, None).await
    }

    /// Starts a run with validated transport metadata, excluded from its semantic fingerprint.
    /// # Errors
    /// Returns the same admission errors as `start`.
    pub async fn start_with_context(
        &self,
        request: Request,
        context: Option<TraceContext>,
    ) -> Result<RunHandle, RuntimeError> {
        let runtime = self.with_context(context);
        let (plan, admission, _) = runtime.prepare_admission(&request).await?;
        let admitted = self
            .store
            .admit_run(admission)
            .await
            .map_err(admission_error)?;
        Ok(runtime.spawn_admitted(
            request,
            plan,
            admitted.fence,
            &admitted.session,
            InitialEntry::Unkeyed,
        ))
    }

    /// Admit with a host-selected stable session ID, or reconcile its original receipt.
    /// # Errors
    /// Rejects missing session ID, semantic conflicts, immutable identity changes,
    /// busy sessions, unsupported stores, or plan/store failures.
    pub async fn start_keyed(
        &self,
        request: Request,
        options: AdmissionOptions,
    ) -> Result<Admission, RuntimeError> {
        self.start_keyed_with_context(request, options, None).await
    }

    /// Keyed admission with optional host identity. Replay never attaches replacement
    /// context or executes the retained receipt; identity is not semantic input.
    /// # Errors
    /// Returns the same admission errors as `start_keyed`.
    pub async fn start_keyed_with_context(
        &self,
        request: Request,
        options: AdmissionOptions,
        context: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        if request.session_id.is_none() {
            return Err(StoreError::Validation(
                "keyed admission requires a stable session ID".into(),
            )
            .into());
        }
        let (plan, keyed) = self.prepare_keyed_admission(&request, options).await?;
        match self
            .store
            .admit_keyed_run(keyed)
            .await
            .map_err(admission_error)?
        {
            KeyedAdmitOutcome::Started { receipt, admitted } => Ok(Admission::Started {
                receipt,
                handle: self.with_context(context).spawn_admitted(
                    request,
                    plan,
                    admitted.fence,
                    &admitted.session,
                    InitialEntry::Keyed,
                ),
            }),
            KeyedAdmitOutcome::Replayed(receipt) => Ok(Admission::Replayed(receipt)),
        }
    }

    /// Completes an expired, durably Unstarted keyed admission with the original
    /// semantics. Exact retries and Started/terminal evidence remain metadata-only.
    /// # Errors
    /// Missing evidence, semantic drift, live/stale owners, unsupported adapters,
    /// and unknown claim responses grant no execution authority.
    pub async fn recover_admission(
        &self,
        request: Request,
        options: AdmissionOptions,
    ) -> Result<Admission, RuntimeError> {
        self.recover_admission_with_context(request, options, None)
            .await
    }

    /// Recovers with fresh transport metadata, excluded from retained authority.
    /// # Errors
    /// Returns the same failures as `recover_admission`.
    pub async fn recover_admission_with_context(
        &self,
        request: Request,
        options: AdmissionOptions,
        context: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        let session = request.session_id.as_ref().ok_or(StoreError::NotFound)?;
        // Host-presented identity check, before the receipt lookup and before
        // any replay: a drifted request never receives a replayed result.
        let persisted = self
            .store
            .get_session(session)
            .await?
            .ok_or(StoreError::NotFound)?;
        ensure_session_identity(&persisted, &request.workspace_id, &request.directory)?;
        let receipt = self
            .store
            .lookup_admission(session, &options.key)
            .await?
            .ok_or(StoreError::NotFound)?;
        let record = self
            .store
            .load_admission_execution(session, &options.key)
            .await?
            .ok_or(AdmissionExecutionError::MissingEvidence)?;
        let (plan, keyed) = self.prepare_keyed_admission(&request, options).await?;
        let capsule = keyed
            .execution
            .as_ref()
            .ok_or(AdmissionExecutionError::MissingEvidence)?;
        // Compare the entire capsule, including independently recomputed semantics,
        // before terminal/Started replay or any ownership operation.
        record.capsule.validate(&keyed)?;
        if receipt.semantic_digest_version != 1
            || record.receipt != receipt
            || record.key != keyed.options.key
            || record.capsule != *capsule
        {
            return Err(StoreError::AdmissionConflict.into());
        }
        let run = self
            .store
            .get_run(&receipt.run_id)
            .await?
            .ok_or(StoreError::NotFound)?;
        if run.session_id != *session
            || run.config_hash != capsule.config_hash
            || run.plan_fingerprint != capsule.plan_fingerprint
        {
            return Err(StoreError::AdmissionConflict.into());
        }
        if run.status.is_terminal() || record.state == AdmissionExecutionState::Started {
            return Ok(Admission::Replayed(receipt));
        }
        let claimed = self
            .store
            .claim_unstarted_admission(ClaimUnstartedAdmissionRequest {
                session_id: session.clone(),
                key: keyed.options.key,
                expected_fence: RunFence {
                    run_id: run.id,
                    claim_token: run.claim_token,
                },
                expected_owner: run.owner,
                fingerprint: capsule.fingerprint.clone(),
                behavior_fingerprint: capsule.behavior_fingerprint.clone(),
                semantic_digest: capsule.semantic_digest.clone(),
                capsule_digest: capsule.digest()?,
                owner: keyed.request.owner,
                lease: keyed.request.lease,
            })
            .await?;
        Ok(Admission::Started {
            receipt: claimed.record.receipt,
            handle: self.with_context(context).spawn_admitted(
                Request::from_admission(claimed.record.capsule.request),
                plan,
                claimed.fence,
                &persisted,
                InitialEntry::Keyed,
            ),
        })
    }

    async fn prepare_keyed_admission(
        &self,
        request: &Request,
        options: AdmissionOptions,
    ) -> Result<(RunPlan, KeyedAdmitRequest), RuntimeError> {
        let (plan, admission, runtime_semantics) = self.prepare_admission(request).await?;
        let mut keyed = KeyedAdmitRequest {
            request: admission,
            options,
            execution: None,
        };
        let capsule = AdmissionExecutionCapsule {
            version: 1,
            request: request.admission_data()?,
            runtime_semantics,
            config_hash: keyed.request.config_hash.clone(),
            plan_fingerprint: keyed.request.plan_fingerprint.clone(),
            fingerprint: keyed.options.fingerprint.clone(),
            behavior_fingerprint: keyed.options.behavior_fingerprint.clone(),
            semantic_digest: keyed.semantic_digest()?,
        };
        capsule.validate(&keyed)?;
        keyed.execution = Some(capsule);
        Ok((plan, keyed))
    }

    // A generic store Conflict is not evidence of lease loss. Verify retained
    // authority before classifying it or skipping settlement, and keep the original error.
    async fn verified_lease_loss(&self, fence: &RunFence) -> bool {
        self.store
            .get_run(&fence.run_id)
            .await
            .ok()
            .flatten()
            .is_some_and(|run| {
                run.claim_token != fence.claim_token
                    || (run.status != RunStatus::Paused
                        && !run.status.is_terminal()
                        && run.lease_until <= self.clock.now())
            })
    }

    /// Binds a run to the workspace identity of its persisted session, the
    /// only source extension context is ever populated from.
    fn with_workspace(&self, session: &Session) -> Self {
        let mut runtime = self.clone();
        runtime.workspace = Some(WorkspaceContext::from_persisted(
            &session.workspace_id,
            &session.directory,
        ));
        runtime
    }

    fn workspace(&self) -> Result<&WorkspaceContext, RuntimeError> {
        self.workspace
            .as_ref()
            .ok_or(RuntimeError::Missing("workspace context"))
    }

    fn with_context(&self, context: Option<TraceContext>) -> Self {
        let mut runtime = self.clone();
        runtime.observer = Arc::new(ContextObserver {
            inner: Arc::clone(&self.observer),
            context,
            attempt: RunId::new(),
        });
        runtime
    }

    /// Settles orphaned work without resolving providers, preparing plans or
    /// executing extension hooks. Retry the same request after unknown responses.
    /// # Errors
    /// Returns typed eligibility, ownership or durable settlement failures.
    pub async fn abandon(
        &self,
        request: crabber_core::AbandonRequest,
    ) -> Result<crabber_core::AbandonOutcome, crabber_core::AbandonError> {
        self.store.abandon_run(request).await
    }

    /// Reads retained metadata without acquiring a plan or execution lease.
    /// # Errors
    /// Returns store errors; None does not rule out a concurrent commit.
    pub async fn lookup_admission(
        &self,
        session: &SessionId,
        key: &AdmissionKey,
    ) -> Result<Option<AdmissionReceipt>, RuntimeError> {
        Ok(self.store.lookup_admission(session, key).await?)
    }

    async fn prepare_admission(
        &self,
        request: &Request,
    ) -> Result<(RunPlan, AdmitRequest, Value), RuntimeError> {
        let now = self.clock.now();
        let session_id = request.session_id.clone().unwrap_or_default();
        let plan = self
            .plan_provider
            .acquire_plan(&session_id)
            .await
            .map_err(|error| RuntimeError::Extension(error.to_string()))?;
        // Hash structured, independently observed semantics, never a caller's claim.
        let mut config = json!([
            "crabber.runtime.admission.v1",
            request.selection.provider_id,
            request.selection.model_id,
            request.system_prompt,
            format!("{:?}", self.execution_mode),
            self.compaction.trigger_ratio,
            self.compaction.keep_tail_messages,
            self.max_turns,
            plan.tools.iter().map(|tool| &tool.info).collect::<Vec<_>>(),
            plan.prompts
                .iter()
                .map(|p| (&p.name, p.order, &p.text))
                .collect::<Vec<_>>(),
            plan.restrictions,
            plan.components,
            plan.guards
                .iter()
                .map(|guard| guard.id())
                .collect::<Vec<_>>(),
            plan.providers
                .iter()
                .map(|provider| {
                    let info = provider.info();
                    (info.id, info.name)
                })
                .collect::<Vec<_>>()
        ]);
        config.sort_all_objects();
        let config_hash = admission_config_hash(&config);
        let admission = AdmitRequest {
            session_id: request.session_id.clone(),
            workspace_id: request.workspace_id.clone(),
            directory: request.directory.clone(),
            title: request.title.clone(),
            user_message: user_message(session_id, request.text.clone(), now),
            config_hash,
            plan_fingerprint: plan.fingerprint().to_string(),
            owner: RunId::new().to_string(),
            lease: Duration::from_secs(30),
        };
        Ok((plan, admission, config))
    }

    /// Every admitted run is bound to the workspace identity of the persisted
    /// `session` it was admitted into; there is no other way to start one.
    fn spawn_admitted(
        &self,
        request: Request,
        plan: RunPlan,
        fence: RunFence,
        session: &Session,
        entry: InitialEntry,
    ) -> RunHandle {
        let session_id = session.id.clone();
        let (sender, done) = oneshot::channel();
        let (completion_sender, completion) = watch::channel(false);
        let handle = RunHandle {
            session_id: session_id.clone(),
            run_id: fence.run_id.clone(),
            store: Arc::clone(&self.store),
            clock: Arc::clone(&self.clock),
            done,
            completion,
            cancellation: CancellationToken::new(),
        };
        let cancellation = handle.cancellation.clone();
        let runtime = self.with_workspace(session);
        tokio::spawn(async move {
            let result = runtime
                .run(fence, session_id, request, plan, cancellation, entry)
                .await;
            let _ = sender.send(result);
            let _ = completion_sender.send(true);
        });
        handle
    }

    async fn run(
        &self,
        fence: RunFence,
        session_id: SessionId,
        request: Request,
        plan: RunPlan,
        cancellation: CancellationToken,
        entry: InitialEntry,
    ) -> Result<RunResult, RuntimeError> {
        let lost = AtomicBool::new(false);
        let mut measurement = Measurement::new(
            self.observer.as_ref(),
            self.monotonic_clock.as_ref(),
            &session_id,
            &fence.run_id,
            OperationKind::Run,
            &cancellation,
            &lost,
        );
        let result = self
            .run_inner(
                fence.clone(),
                session_id,
                InitialRequest { request, entry },
                plan,
                cancellation.clone(),
                &lost,
            )
            .await;
        if result.is_err() && self.verified_lease_loss(&fence).await {
            lost.store(true, Ordering::SeqCst);
        }
        measurement.observation.reason = result_reason(&result);
        result
    }

    #[allow(clippy::too_many_lines)]
    async fn run_inner(
        &self,
        fence: RunFence,
        session_id: SessionId,
        initial: InitialRequest,
        plan: RunPlan,
        cancellation: CancellationToken,
        observed_lost: &AtomicBool,
    ) -> Result<RunResult, RuntimeError> {
        let InitialRequest { request, entry } = initial;
        let execution: Arc<dyn ExecutionStore> = self.store.execution(fence.clone()).await?.into();
        if matches!(entry, InitialEntry::Keyed) {
            execution.begin_admission_execution().await?;
        }
        execution
            .renew_lease(self.clock.now() + time::Duration::seconds(30))
            .await?;
        let heartbeat = HeartbeatGuard::start(
            Arc::clone(&execution),
            Arc::clone(&self.store),
            fence.clone(),
            Arc::clone(&self.clock),
            self.heartbeat_interval,
        );
        let run_id = fence.run_id.clone();
        let run_started_at = self.clock.now();
        let mut signal = heartbeat.signal.clone();
        let lost = Arc::clone(&heartbeat.lost);
        let state_sink: Arc<dyn StateSink> = Arc::new(RunStateSink {
            store: Arc::clone(&self.store),
            execution: Arc::clone(&execution),
            session_id: session_id.clone(),
            lost: Arc::clone(&lost),
        });
        let current_turn = std::sync::Mutex::new(None);
        let outcome = {
            let run_future = crabber_extension::with_state_sink(
                state_sink,
                self.run_loop(
                    execution.as_ref(),
                    &run_id,
                    &session_id,
                    &request,
                    &plan,
                    &cancellation,
                    true,
                    Usage::default(),
                    lost.as_ref(),
                    &current_turn,
                ),
            );
            tokio::pin!(run_future);
            loop {
                tokio::select! {
                    biased;
                    update = signal.changed() => match update {
                        Ok(()) if *signal.borrow_and_update() => break Err(RuntimeError::LeaseLost),
                        Ok(()) => (),
                        Err(_) => break run_future.as_mut().await,
                    },
                    result = &mut run_future => break result,
                }
            }
        };
        drop(heartbeat);
        let result = match outcome {
            Ok(usage) => Ok(RunResult {
                session_id,
                run_id,
                status: RunStatus::Completed,
                usage,
            }),
            Err(error) => {
                if matches!(error, RuntimeError::Paused) {
                    plan.release();
                    return Ok(RunResult {
                        session_id,
                        run_id,
                        status: RunStatus::Paused,
                        usage: Usage::default(),
                    });
                }
                if lost.load(Ordering::SeqCst)
                    || matches!(error, RuntimeError::LeaseLost)
                    || self.verified_lease_loss(&fence).await
                {
                    observed_lost.store(true, Ordering::SeqCst);
                    plan.release();
                    return Err(error);
                }
                if !matches!(error, RuntimeError::LeaseLost) {
                    let turn_id = current_turn.lock().expect("turn context poisoned").clone();
                    self.settle_unfinished_calls(
                        execution.as_ref(),
                        &session_id,
                        &run_id,
                        &plan,
                        true,
                        turn_id.as_ref(),
                    )
                    .await?;
                }
                let status = if matches!(error, RuntimeError::Interrupted) {
                    RunStatus::Interrupted
                } else {
                    RunStatus::Failed
                };
                let settled = self.run_settled_event(
                    &session_id,
                    &run_id,
                    status,
                    &Usage::default(),
                    run_started_at,
                );
                if !matches!(error, RuntimeError::LeaseLost)
                    && execution
                        .settle_run(
                            status,
                            Some(error.to_string()),
                            Usage::default(),
                            settled.clone(),
                        )
                        .await
                        .is_ok()
                {
                    self.observer.emit(&settled);
                    let projection = serde_json::to_value(&settled).unwrap_or(Value::Null);
                    plan.dispatcher
                        .notify::<EventPublished>(projection.clone())
                        .await;
                    plan.dispatcher.notify::<RunSettled>(projection).await;
                }
                if status == RunStatus::Interrupted {
                    Ok(RunResult {
                        session_id,
                        run_id,
                        status,
                        usage: Usage::default(),
                    })
                } else {
                    Err(error)
                }
            }
        };
        plan.release();
        result
    }

    #[allow(clippy::too_many_lines, clippy::too_many_arguments)]
    async fn run_loop(
        &self,
        execution: &dyn ExecutionStore,
        run_id: &RunId,
        session_id: &SessionId,
        request: &Request,
        plan: &RunPlan,
        cancellation: &CancellationToken,
        initial: bool,
        mut usage: Usage,
        lease_lost: &AtomicBool,
        current_turn: &std::sync::Mutex<Option<TurnId>>,
    ) -> Result<Usage, RuntimeError> {
        let run_started_at = self.clock.now();
        if initial {
            plan.dispatcher
                .gate::<RunBeforeExecute>(json!({"run_id": run_id.to_string()}))
                .await
                .map_err(|e| RuntimeError::Extension(e.to_string()))?;
            self.emit_durable(execution, session_id, run_id, plan, EventKind::RunAdmitted)
                .await?;
            self.emit_durable(execution, session_id, run_id, plan, EventKind::RunStarted)
                .await?;
        }
        let streamer = if let Some(provider) = plan
            .providers
            .iter()
            .find(|p| p.info().id == request.selection.provider_id)
        {
            provider.build(&request.selection).await?
        } else {
            self.resolver.resolve(&request.selection).await?
        };
        for _ in 0..self.max_turns {
            if cancellation.is_cancelled() {
                return Err(RuntimeError::Interrupted);
            }
            let turn_id = TurnId::new();
            *current_turn.lock().expect("turn context poisoned") = Some(turn_id.clone());
            self.publish_event(
                execution,
                plan,
                self.turn_event(session_id, run_id, &turn_id, EventKind::TurnStarted),
            )
            .await?;
            let mut snapshot = self.snapshot(run_id, session_id, request, plan).await?;
            snapshot.identity.turn_id = turn_id.clone();
            let mut compacted = false;
            if let Some(provider) = plan
                .providers
                .iter()
                .find(|provider| provider.info().id == request.selection.provider_id)
            {
                let models = provider.models().await?;
                if let Some(model) = models
                    .iter()
                    .find(|model| model.id == request.selection.model_id)
                {
                    let estimated =
                        serde_json::to_vec(&snapshot.messages).map_or(0, |bytes| bytes.len() / 4);
                    #[allow(clippy::cast_precision_loss)]
                    if snapshot.messages.len() > self.compaction.keep_tail_messages
                        && model.context_limit > 0
                        && (estimated as f64)
                            >= (model.context_limit as f64 * self.compaction.trigger_ratio)
                    {
                        self.compact(
                            execution,
                            run_id,
                            session_id,
                            &snapshot,
                            plan,
                            Arc::clone(&streamer),
                            false,
                            cancellation,
                            lease_lost,
                        )
                        .await?;
                        snapshot.messages = self.store.list_messages(session_id, None).await?;
                        compacted = true;
                    }
                }
            }
            plan.dispatcher
                .hook::<TurnPrepare>(json!({"run_id":run_id.to_string()}))
                .await
                .map_err(|e| RuntimeError::Extension(e.to_string()))?;
            let (calls, turn_usage) = Box::pin(self.model_turn_with_retry(
                execution,
                run_id,
                session_id,
                &snapshot,
                plan,
                Arc::clone(&streamer),
                compacted,
                cancellation,
                lease_lost,
            ))
            .await?;
            if cancellation.is_cancelled() {
                return Err(RuntimeError::Interrupted);
            }
            usage.input_tokens += turn_usage.input_tokens;
            usage.output_tokens += turn_usage.output_tokens;
            let had_tools = !calls.is_empty();
            let mut staged = Vec::with_capacity(calls.len());
            for call in calls {
                staged.push(
                    self.stage_tool(execution, session_id, run_id, plan, call, lease_lost)
                        .await?,
                );
            }
            if staged.iter().any(|call| {
                plan.tools
                    .iter()
                    .find(|tool| tool.info.name == call.name)
                    .is_some_and(|tool| {
                        self.policy.interrupt_policy(&tool.info, &call.arguments)
                            == InterruptPolicy::Pause
                    })
            }) {
                let event = self.turn_event(session_id, run_id, &turn_id, EventKind::RunPaused);
                execution.pause_run(json!({"pending_calls": staged.iter().map(|call| call.id.to_string()).collect::<Vec<_>>(),
                    "request": {"workspace_id":request.workspace_id,"directory":request.directory,"title":request.title,
                        "text":request.text,"provider_id":request.selection.provider_id,"model_id":request.selection.model_id,
                        "system_prompt":request.system_prompt}, "usage":usage}), event.clone()).await?;
                self.observer.emit(&event);
                return Err(RuntimeError::Paused);
            }
            match self.execution_mode {
                ExecutionMode::Sequential => {
                    for record in staged {
                        let mut call = PendingCall::from_record(&record);
                        call.turn_id = Some(turn_id.clone());
                        self.execute_tool(
                            execution,
                            session_id,
                            run_id,
                            plan,
                            call,
                            Some(record),
                            None,
                            cancellation,
                            lease_lost,
                        )
                        .await?;
                    }
                }
                ExecutionMode::Parallel { max } => {
                    let (sender, receiver) = watch::channel(0usize);
                    futures::stream::iter(staged.into_iter().enumerate().map(|(index, record)| {
                        let receiver = receiver.clone();
                        let sender = sender.clone();
                        let turn_id = turn_id.clone();
                        async move {
                            let mut call = PendingCall::from_record(&record);
                            call.turn_id = Some(turn_id);
                            self.execute_tool(
                                execution,
                                session_id,
                                run_id,
                                plan,
                                call,
                                Some(record),
                                Some((index, receiver, sender)),
                                cancellation,
                                lease_lost,
                            )
                            .await
                        }
                    }))
                    .buffer_unordered(max)
                    .try_collect::<Vec<_>>()
                    .await?;
                }
            }
            let mut claimed = self.claim_input(execution, InboxKind::Steer).await?;
            if !had_tools && claimed == 0 {
                claimed += self.claim_input(execution, InboxKind::FollowUp).await?;
            }
            self.publish_event(
                execution,
                plan,
                self.turn_event(session_id, run_id, &turn_id, EventKind::TurnCompleted),
            )
            .await?;
            if !had_tools && claimed == 0 {
                ensure_lease(lease_lost)?;
                let settled = self.run_settled_event(
                    session_id,
                    run_id,
                    RunStatus::Completed,
                    &usage,
                    run_started_at,
                );
                match execution
                    .settle_run(RunStatus::Completed, None, usage.clone(), settled.clone())
                    .await
                {
                    Ok(()) => {
                        self.observer.emit(&settled);
                        let projection = serde_json::to_value(&settled).unwrap_or(Value::Null);
                        plan.dispatcher
                            .notify::<EventPublished>(projection.clone())
                            .await;
                        plan.dispatcher.notify::<RunSettled>(projection).await;
                        return Ok(usage);
                    }
                    Err(StoreError::PendingInput) => {
                        if self.claim_input(execution, InboxKind::Steer).await?
                            + self.claim_input(execution, InboxKind::FollowUp).await?
                            == 0
                        {
                            return Err(RuntimeError::Store(StoreError::PendingInput));
                        }
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }
        Err(RuntimeError::TurnLimit)
    }

    async fn snapshot(
        &self,
        run_id: &RunId,
        session_id: &SessionId,
        request: &Request,
        plan: &RunPlan,
    ) -> Result<TurnSnapshot, RuntimeError> {
        let mut system = request.system_prompt.clone().unwrap_or_default();
        for prompt in &plan.prompts {
            if !system.is_empty() {
                system.push('\n');
            }
            system.push_str(&prompt.text);
        }
        let mut messages = self.store.list_messages(session_id, None).await?;
        let turn_index = messages
            .iter()
            .filter(|message| message.role == Role::Assistant)
            .count();
        // Workspace keys are runtime-owned: pinned so no handler can change
        // them for a later handler. An unavailable field is JSON null.
        let workspace = self.workspace()?;
        let contributions = plan
            .dispatcher
            .transform_pinned::<ContextAssemble>(
                json!({
                    "system_prelude":[], "user_suffix":[], "prompt_sections":[],
                    "run_id":run_id.to_string(), "session_id":session_id.to_string(),
                    "provider_id":request.selection.provider_id, "model_id":request.selection.model_id,
                    "turn_index":turn_index, "message_count":messages.len(),
                    "has_system_prompt":request.system_prompt.is_some()
                }),
                &[
                    (
                        WorkspaceContext::WORKSPACE_ID_KEY,
                        json!(workspace.workspace_id()),
                    ),
                    (
                        WorkspaceContext::DIRECTORY_KEY,
                        json!(workspace.directory()),
                    ),
                ],
            )
            .await
            .map_err(|e| RuntimeError::Extension(e.to_string()))?;
        if let Some(sections) = contributions
            .get("prompt_sections")
            .and_then(Value::as_array)
        {
            for section in sections.iter().filter_map(Value::as_str) {
                if !system.is_empty() {
                    system.push('\n');
                }
                system.push_str(section);
            }
        }
        if let Some(prelude) = contributions
            .get("system_prelude")
            .and_then(Value::as_array)
        {
            for line in prelude.iter().filter_map(Value::as_str) {
                system = format!("{line}\n{system}");
            }
        }
        if let Some(suffixes) = contributions.get("user_suffix").and_then(Value::as_array)
            && let Some(message) = messages
                .iter_mut()
                .rev()
                .find(|message| message.role == Role::User)
            && let Some(text) =
                message
                    .parts
                    .iter_mut()
                    .rev()
                    .find_map(|part| match &mut part.content {
                        ContentBlock::Text { text } => Some(text),
                        _ => None,
                    })
        {
            for suffix in suffixes.iter().filter_map(Value::as_str) {
                text.push('\n');
                text.push_str(suffix);
            }
        }
        Ok(TurnSnapshot {
            identity: RequestIdentity {
                session_id: session_id.clone(),
                run_id: run_id.clone(),
                turn_id: TurnId::new(),
            },
            selection: request.selection.clone(),
            messages,
            system: (!system.is_empty()).then_some(system),
            tools: plan.tools.iter().map(|tool| tool.info.clone()).collect(),
        })
    }

    async fn claim_input(
        &self,
        execution: &dyn ExecutionStore,
        kind: InboxKind,
    ) -> Result<usize, RuntimeError> {
        Ok(execution.claim_inbox_into_history(kind).await?.len())
    }

    #[allow(clippy::too_many_arguments)]
    async fn model_turn_with_retry(
        &self,
        execution: &dyn ExecutionStore,
        run_id: &RunId,
        session_id: &SessionId,
        snapshot: &TurnSnapshot,
        plan: &RunPlan,
        streamer: Arc<dyn Streamer>,
        mut compacted: bool,
        cancellation: &CancellationToken,
        lease_lost: &AtomicBool,
    ) -> Result<(Vec<PendingCall>, Usage), RuntimeError> {
        let mut snapshot = snapshot.clone();
        let mut retries = 0u32;
        loop {
            match self
                .model_turn(
                    execution,
                    run_id,
                    session_id,
                    &snapshot,
                    plan,
                    Arc::clone(&streamer),
                    cancellation,
                    lease_lost,
                )
                .await
            {
                Ok(result) => return Ok(result),
                Err(RuntimeError::Provider(error)) => {
                    let overflow =
                        error.kind == crabber_providers::ProviderErrorKind::ContextOverflow;
                    let retry = !overflow && error.retryable && retries < 2;
                    let delay = if retry {
                        100u64.saturating_mul(1u64 << retries)
                    } else {
                        0
                    };
                    let decision = request_error(plan, &error, retries + 1, retry, delay).await?;
                    if overflow && !compacted && decision.compaction_requested {
                        self.compact(
                            execution,
                            run_id,
                            session_id,
                            &snapshot,
                            plan,
                            Arc::clone(&streamer),
                            true,
                            cancellation,
                            lease_lost,
                        )
                        .await?;
                        snapshot.messages = self.store.list_messages(session_id, None).await?;
                        compacted = true;
                        continue;
                    }
                    if retry && decision.retry {
                        retries += 1;
                        tokio::time::sleep(Duration::from_millis(decision.delay_ms)).await;
                        continue;
                    }
                    return Err(RuntimeError::Provider(error));
                }
                Err(error) => return Err(error),
            }
        }
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn compact(
        &self,
        execution: &dyn ExecutionStore,
        run_id: &RunId,
        session_id: &SessionId,
        snapshot: &TurnSnapshot,
        plan: &RunPlan,
        streamer: Arc<dyn Streamer>,
        overflow: bool,
        cancellation: &CancellationToken,
        lease_lost: &AtomicBool,
    ) -> Result<(), RuntimeError> {
        let run = self
            .store
            .get_run(run_id)
            .await?
            .ok_or(StoreError::NotFound)?;
        let messages = &snapshot.messages;
        let context_limit = if let Some(provider) = plan
            .providers
            .iter()
            .find(|provider| provider.info().id == snapshot.selection.provider_id)
        {
            provider
                .models()
                .await?
                .into_iter()
                .find(|model| model.id == snapshot.selection.model_id)
                .map_or(0, |model| model.context_limit)
        } else {
            0
        };
        let budget_chars = usize::try_from(context_limit)
            .ok()
            .filter(|limit| *limit > 0)
            .unwrap_or(2048)
            .min(4096);
        let mut tail_start = messages
            .len()
            .saturating_sub(self.compaction.keep_tail_messages);
        if overflow && !messages.is_empty() {
            tail_start = tail_start.max(1);
            while tail_start < messages.len()
                && serde_json::to_vec(&messages[tail_start..])
                    .map_or(usize::MAX, |bytes| bytes.len())
                    > budget_chars
            {
                tail_start += 1;
            }
        }
        let summary_range = if tail_start == 0 {
            None
        } else {
            messages
                .first()
                .zip(messages.get(tail_start - 1))
                .map(|(first, last)| (first.id.clone(), last.id.clone()))
        };
        let epoch = ContextEpoch {
            id: EpochId::new(),
            session_id: session_id.clone(),
            run_id: run_id.clone(),
            parent: Some(run.epoch_id),
            summarized_range: summary_range,
            summary_message_id: None,
            tail_start_message_id: messages.get(tail_start).map(|message| message.id.clone()),
            provider_id: snapshot.selection.provider_id.clone(),
            model_id: snapshot.selection.model_id.clone(),
            reason: if overflow {
                "context_overflow"
            } else {
                "proactive"
            }
            .into(),
            next_policy: None,
        };
        ensure_lease(lease_lost)?;
        execution.start_epoch(epoch.clone()).await?;
        self.emit_durable(
            execution,
            session_id,
            run_id,
            plan,
            EventKind::ContextEpochStarted,
        )
        .await?;
        let summary_prompt = "Summarize this context for continuation. Preserve every standing instruction and unresolved task from the previous summary and new context.";
        let raw: Vec<char> = serde_json::to_string(&messages[..tail_start])
            .map_err(|error| invalid_provider(&format!("cannot serialize context: {error}")))?
            .chars()
            .collect();
        let mut offset = 0;
        let mut text = String::new();
        while offset < raw.len() {
            // Every character in the replaced range reaches a summary request.
            // Reserve room for the prior summary instead of silently taking a suffix.
            let capacity = budget_chars.saturating_sub(text.chars().count());
            if capacity == 0 {
                return Err(invalid_provider("compaction summary exceeded input budget").into());
            }
            let end = (offset + capacity).min(raw.len());
            let chunk: String = raw[offset..end].iter().collect();
            let input = format!("Previous summary:\n{text}\nNew context:\n{chunk}");
            let request = ModelRequest {
                identity: RequestIdentity {
                    session_id: session_id.clone(),
                    run_id: run_id.clone(),
                    turn_id: TurnId::new(),
                },
                selection: snapshot.selection.clone(),
                system: Some(summary_prompt.into()),
                messages: vec![user_message(session_id.clone(), input, self.clock.now())],
                tools: Vec::new(),
                temperature: None,
                max_tokens: Some(1024),
                tool_choice: None,
            };
            let mut measurement = Measurement::new(
                self.observer.as_ref(),
                self.monotonic_clock.as_ref(),
                session_id,
                run_id,
                OperationKind::Model {
                    purpose: ModelPurpose::Compaction,
                    provider: snapshot.selection.provider_id.clone(),
                    model: snapshot.selection.model_id.clone(),
                },
                cancellation,
                lease_lost,
            );
            let summary_started = self.monotonic_clock.now();
            let observe_summary = |status: &str| {
                let mut observed = self.event(
                    session_id,
                    run_id,
                    EventKind::Custom {
                        name: "model_call".into(),
                    },
                );
                observed.payload = json!({"provider":snapshot.selection.provider_id,"model":snapshot.selection.model_id,
                    "status":status,"input_tokens":0,"output_tokens":0,
                    "latency_ms":self.monotonic_clock.now().saturating_sub(summary_started).as_millis(),"purpose":"compaction"});
                self.observer.model_completed(&observed);
            };
            let mut stream = tokio::select! {
                () = cancellation.cancelled() => { observe_summary("error"); return Err(RuntimeError::Interrupted); },
                result = streamer.stream(request) => match result {
                    Ok(stream) => stream,
                    Err(error) => { observe_summary("error"); return Err(error.into()); }
                },
            };
            let mut next_text = String::new();
            let mut completed = false;
            loop {
                let delta = tokio::select! {
                    () = cancellation.cancelled() => { observe_summary("error"); return Err(RuntimeError::Interrupted); },
                    result = stream.next() => result,
                };
                let Some(delta) = delta else {
                    break;
                };
                match delta {
                    StreamDelta::TextDelta(fragment) => {
                        measurement.first_text(&fragment);
                        next_text.push_str(&fragment);
                    }
                    StreamDelta::Completed => {
                        completed = true;
                        break;
                    }
                    StreamDelta::Error(error) => {
                        observe_summary("error");
                        return Err(error.into());
                    }
                    _ => {}
                }
            }
            if cancellation.is_cancelled() {
                observe_summary("error");
                return Err(RuntimeError::Interrupted);
            }
            observe_summary(if completed { "ok" } else { "error" });
            if !completed || next_text.is_empty() {
                return Err(invalid_provider("compaction summary was empty").into());
            }
            measurement.observation.reason = TerminalReason::Success;
            drop(measurement);
            text = next_text;
            offset = end;
        }
        let message_id = MessageId::new();
        let summary = Message {
            id: message_id.clone(),
            session_id: session_id.clone(),
            run_id: Some(run_id.clone()),
            role: Role::Assistant,
            parent_id: None,
            created_at: self.clock.now(),
            parts: vec![Part {
                id: PartId::new(),
                message_id,
                ordinal: 0,
                kind: PartKind::CompactionSummary,
                content: ContentBlock::Text { text },
            }],
        };
        if cancellation.is_cancelled() {
            return Err(RuntimeError::Interrupted);
        }
        ensure_lease(lease_lost)?;
        execution.finish_epoch(&epoch.id, summary).await?;
        self.emit_durable(
            execution,
            session_id,
            run_id,
            plan,
            EventKind::ContextEpochFinished,
        )
        .await?;
        Ok(())
    }

    fn event(&self, session_id: &SessionId, run_id: &RunId, kind: EventKind) -> EventRecord {
        EventRecord {
            cursor: None,
            session_id: session_id.clone(),
            run_id: run_id.clone(),
            turn_id: None,
            kind,
            payload: Value::Null,
            correlation: None,
            live_only: false,
            created_at: self.clock.now(),
        }
    }

    fn turn_event(
        &self,
        session_id: &SessionId,
        run_id: &RunId,
        turn_id: &TurnId,
        kind: EventKind,
    ) -> EventRecord {
        let mut event = self.event(session_id, run_id, kind);
        event.turn_id = Some(turn_id.clone());
        event
    }

    pub(crate) fn live_payload(
        &self,
        snapshot: &TurnSnapshot,
        kind: EventKind,
        payload: impl serde::Serialize,
    ) {
        let mut event = self.turn_event(
            &snapshot.identity.session_id,
            &snapshot.identity.run_id,
            &snapshot.identity.turn_id,
            kind,
        );
        event.live_only = true;
        event.payload = serde_json::to_value(payload).expect("event payload serializes");
        self.observer.emit(&event);
    }

    async fn committed_message(
        &self,
        execution: &dyn ExecutionStore,
        plan: &RunPlan,
        snapshot: &TurnSnapshot,
        id: &MessageId,
    ) -> Result<(), RuntimeError> {
        let mut event = self.turn_event(
            &snapshot.identity.session_id,
            &snapshot.identity.run_id,
            &snapshot.identity.turn_id,
            EventKind::MessageCommitted,
        );
        event.payload = serde_json::to_value(event_payload::MessageCommitted {
            message_id: id.clone(),
            role: Role::Assistant,
        })
        .expect("event payload serializes");
        self.publish_event(execution, plan, event).await
    }

    fn run_settled_event(
        &self,
        session_id: &SessionId,
        run_id: &RunId,
        status: RunStatus,
        usage: &Usage,
        started_at: OffsetDateTime,
    ) -> EventRecord {
        let mut event = self.event(session_id, run_id, EventKind::RunSettled);
        let duration = event.created_at - started_at;
        event.payload = json!({"status": status, "usage": usage,
            "input_tokens": usage.input_tokens, "output_tokens": usage.output_tokens,
            "duration_ms": duration.whole_milliseconds(),
            "duration_ns": duration.whole_nanoseconds()});
        event.correlation = Some(run_id.to_string());
        event
    }

    async fn emit_durable(
        &self,
        execution: &dyn ExecutionStore,
        session_id: &SessionId,
        run_id: &RunId,
        plan: &RunPlan,
        kind: EventKind,
    ) -> Result<(), RuntimeError> {
        self.publish_event(execution, plan, self.event(session_id, run_id, kind))
            .await
    }

    async fn publish_event(
        &self,
        execution: &dyn ExecutionStore,
        plan: &RunPlan,
        event: EventRecord,
    ) -> Result<(), RuntimeError> {
        execution.append_event(event.clone()).await?;
        self.observer.emit(&event);
        let projection = serde_json::to_value(&event).unwrap_or(Value::Null);
        plan.dispatcher
            .notify::<EventPublished>(projection.clone())
            .await;
        match event.kind {
            EventKind::RunAdmitted => plan.dispatcher.notify::<RunAdmitted>(projection).await,
            EventKind::RunStarted => plan.dispatcher.notify::<RunStarted>(projection).await,
            EventKind::TurnStarted => plan.dispatcher.notify::<TurnStarted>(projection).await,
            EventKind::TurnCompleted => plan.dispatcher.notify::<TurnCompleted>(projection).await,
            EventKind::RunSettled => plan.dispatcher.notify::<RunSettled>(projection).await,
            _ => {}
        }
        Ok(())
    }
}

fn user_message(session_id: SessionId, text: String, now: OffsetDateTime) -> Message {
    let id = MessageId::new();
    Message {
        id: id.clone(),
        session_id,
        run_id: None,
        role: Role::User,
        parent_id: None,
        parts: vec![Part {
            id: PartId::new(),
            message_id: id,
            ordinal: 0,
            kind: PartKind::UserInputText,
            content: ContentBlock::Text { text },
        }],
        created_at: now,
    }
}

/// Exact, per-field comparison of presented workspace identity with the
/// persisted session. An empty string is a value like any other; nothing is
/// normalized or resolved against the filesystem.
fn ensure_session_identity(
    session: &Session,
    workspace_id: &str,
    directory: &str,
) -> Result<(), StoreError> {
    if session.identity_matches(workspace_id, directory) {
        Ok(())
    } else {
        Err(StoreError::SessionIdentityMismatch)
    }
}

/// Compares the request saved in a run checkpoint, when there is one, with the
/// persisted session. A checkpoint without a request has nothing to compare; a
/// request without string identity fields is corrupt, not drift.
fn ensure_checkpoint_identity(
    session: &Session,
    checkpoint: Option<&Value>,
) -> Result<(), StoreError> {
    let Some(request) = checkpoint.and_then(|checkpoint| checkpoint.get("request")) else {
        return Ok(());
    };
    match (
        request.get("workspace_id").and_then(Value::as_str),
        request.get("directory").and_then(Value::as_str),
    ) {
        (Some(workspace_id), Some(directory)) => {
            ensure_session_identity(session, workspace_id, directory)
        }
        _ => Err(StoreError::Validation(
            "checkpoint request has no workspace identity".into(),
        )),
    }
}

fn request_from_checkpoint(session_id: &SessionId, checkpoint: &Value) -> Option<Request> {
    let value = checkpoint.get("request")?;
    Some(Request {
        session_id: Some(session_id.clone()),
        workspace_id: value.get("workspace_id")?.as_str()?.to_owned(),
        directory: value.get("directory")?.as_str()?.to_owned(),
        title: value.get("title")?.as_str()?.to_owned(),
        text: value.get("text")?.as_str()?.to_owned(),
        selection: Selection {
            provider_id: value.get("provider_id")?.as_str()?.to_owned(),
            model_id: value.get("model_id")?.as_str()?.to_owned(),
        },
        system_prompt: value
            .get("system_prompt")
            .and_then(Value::as_str)
            .map(str::to_owned),
    })
}

struct PendingCall {
    message_id: Option<MessageId>,
    turn_id: Option<TurnId>,
    args_completed: bool,
    id: ToolCallId,
    name: String,
    raw: String,
    arguments: Option<Value>,
}

impl PendingCall {
    fn from_record(record: &ToolCallRecord) -> Self {
        Self {
            message_id: None,
            turn_id: None,
            args_completed: true,
            id: record.id.clone(),
            name: record.name.clone(),
            raw: String::new(),
            arguments: Some(record.arguments.clone()),
        }
    }
}

impl Orchestrator {
    #[allow(clippy::too_many_lines)] // Streaming assembly and durable assistant commit form one turn boundary.
    #[allow(clippy::too_many_arguments)]
    async fn model_turn(
        &self,
        execution: &dyn ExecutionStore,
        run_id: &RunId,
        session_id: &SessionId,
        snapshot: &TurnSnapshot,
        plan: &RunPlan,
        streamer: Arc<dyn Streamer>,
        cancellation: &CancellationToken,
        lease_lost: &AtomicBool,
    ) -> Result<(Vec<PendingCall>, Usage), RuntimeError> {
        let mut measurement = Measurement::new(
            self.observer.as_ref(),
            self.monotonic_clock.as_ref(),
            session_id,
            run_id,
            OperationKind::Model {
                purpose: ModelPurpose::Turn,
                provider: snapshot.selection.provider_id.clone(),
                model: snapshot.selection.model_id.clone(),
            },
            cancellation,
            lease_lost,
        );
        let model_started = self.monotonic_clock.now();
        let request = ModelRequest {
            identity: snapshot.identity.clone(),
            selection: snapshot.selection.clone(),
            system: snapshot.system.clone(),
            messages: snapshot.messages.clone(),
            tools: snapshot.tools.clone(),
            temperature: None,
            max_tokens: None,
            tool_choice: None,
        };
        let calls_to_next = Arc::new(AtomicUsize::new(0));
        let next: Arc<dyn Streamer> = Arc::new(SingleUseStreamer {
            inner: streamer,
            calls: Arc::clone(&calls_to_next),
        });
        plan.dispatcher.notify::<ModelRequested>(json!({"provider":snapshot.selection.provider_id,"model":snapshot.selection.model_id,"message_count":snapshot.messages.len()})).await;
        let stream_slot = Arc::new(std::sync::Mutex::new(None));
        let error_slot = Arc::new(std::sync::Mutex::new(None));
        let terminal: Callback = {
            let model_stream = Arc::clone(&self.model_stream);
            let stream_slot = Arc::clone(&stream_slot);
            let error_slot = Arc::clone(&error_slot);
            Arc::new(move |input| {
                let model_stream = Arc::clone(&model_stream);
                let next = Arc::clone(&next);
                let request = request.clone();
                let stream_slot = Arc::clone(&stream_slot);
                let error_slot = Arc::clone(&error_slot);
                Box::pin(async move {
                    let mut request = request;
                    if let Some(value) = input.get("temperature") {
                        request.temperature = if value.is_null() {
                            None
                        } else {
                            Some(
                                value
                                    .as_f64()
                                    .filter(|v| v.is_finite() && (0.0..=2.0).contains(v))
                                    .ok_or_else(|| {
                                        crabber_extension::ExtensionError::Plan(
                                            "invalid model temperature".into(),
                                        )
                                    })?,
                            )
                        };
                    }
                    if let Some(value) = input.get("max_tokens") {
                        request.max_tokens = if value.is_null() {
                            None
                        } else {
                            Some(
                                value
                                    .as_u64()
                                    .and_then(|v| u32::try_from(v).ok())
                                    .filter(|v| *v > 0)
                                    .ok_or_else(|| {
                                        crabber_extension::ExtensionError::Plan(
                                            "invalid model max_tokens".into(),
                                        )
                                    })?,
                            )
                        };
                    }
                    if let Some(value) = input.get("tool_choice") {
                        request.tool_choice = if value.is_null() {
                            None
                        } else {
                            Some(
                                value
                                    .as_str()
                                    .ok_or_else(|| {
                                        crabber_extension::ExtensionError::Plan(
                                            "invalid model tool_choice".into(),
                                        )
                                    })?
                                    .to_owned(),
                            )
                        };
                    }
                    match model_stream.stream(request, next).await {
                        Ok(stream) => {
                            *stream_slot.lock().unwrap() = Some(stream);
                            Ok(Value::Null)
                        }
                        Err(error) => {
                            *error_slot.lock().unwrap() = Some(error);
                            Err(crabber_extension::ExtensionError::Plan(
                                "model stream failed".into(),
                            ))
                        }
                    }
                })
            })
        };
        let dispatched = tokio::select! {
            () = cancellation.cancelled() => return Err(RuntimeError::Interrupted),
            dispatched = plan.dispatcher.around::<ExtensionModelStream>(json!({"provider":snapshot.selection.provider_id,"model":snapshot.selection.model_id,"session_id":snapshot.identity.session_id,"run_id":snapshot.identity.run_id,"turn_id":snapshot.identity.turn_id,"message_count":snapshot.messages.len(),"temperature":null,"max_tokens":null,"tool_choice":null}),terminal) => dispatched,
        };
        let provider_error = error_slot.lock().unwrap().take();
        if let Some(error) = provider_error {
            return Err(error.into());
        }
        dispatched.map_err(|e| {
            measurement.observation.reason = TerminalReason::RuntimeError;
            RuntimeError::Extension(e.to_string())
        })?;
        let mut stream = stream_slot.lock().unwrap().take().ok_or_else(|| {
            measurement.observation.reason = TerminalReason::RuntimeError;
            RuntimeError::Extension("model stream did not produce a stream".into())
        })?;
        if calls_to_next.load(Ordering::SeqCst) != 1 {
            return Err(invalid_provider(
                "model stream wrapper did not call provider exactly once",
            )
            .into());
        }
        let id = MessageId::new();
        self.live_payload(
            snapshot,
            EventKind::MessageStarted,
            event_payload::MessageIdentity {
                message_id: id.clone(),
            },
        );
        let mut presentation = crate::event_payload::Presentation::new(self, snapshot, id.clone());
        let mut text = String::new();
        let mut reasoning = String::new();
        let mut provider_state = Vec::new();
        let mut calls: Vec<PendingCall> = Vec::new();
        let mut usage = Usage::default();
        let observe_model = |status: &str, usage: &Usage| {
            let mut event = self.event(
                session_id,
                run_id,
                EventKind::Custom {
                    name: "model_call".into(),
                },
            );
            event.payload = json!({"provider": snapshot.selection.provider_id,
                "model": snapshot.selection.model_id, "status": status,
                "input_tokens": usage.input_tokens, "output_tokens": usage.output_tokens,
                "latency_ms": self.monotonic_clock.now().saturating_sub(model_started).as_millis()});
            self.observer.model_completed(&event);
        };
        let stream_result: Result<(), RuntimeError> = async {
            let mut completed = false;
            loop {
                let delta = tokio::select! {
                    () = cancellation.cancelled() => return Err(RuntimeError::Interrupted),
                    delta = stream.next() => delta,
                };
                let Some(delta) = delta else {
                    break;
                };
                match delta {
                    StreamDelta::TextDelta(fragment) => {
                        measurement.first_text(&fragment);
                        text.push_str(&fragment);
                        self.live_payload(
                            snapshot,
                            EventKind::TextDelta,
                            event_payload::MessageDelta {
                                message_id: id.clone(),
                                text: fragment,
                            },
                        );
                    }
                    StreamDelta::ReasoningDelta(fragment) => {
                        reasoning.push_str(&fragment);
                        self.live_payload(
                            snapshot,
                            EventKind::ReasoningDelta,
                            event_payload::MessageDelta {
                                message_id: id.clone(),
                                text: fragment,
                            },
                        );
                    }
                    StreamDelta::ToolCallStart { call_id, name } => {
                        if calls.iter().any(|call| call.id == call_id) {
                            return Err(invalid_provider("duplicate tool call start").into());
                        }
                        self.live_payload(
                            snapshot,
                            EventKind::ToolCallStarted,
                            event_payload::CallStarted {
                                message_id: id.clone(),
                                call_id: call_id.clone(),
                                name: name.clone(),
                            },
                        );
                        presentation.start_call(call_id.clone());
                        calls.push(PendingCall {
                            message_id: Some(id.clone()),
                            turn_id: Some(snapshot.identity.turn_id.clone()),
                            args_completed: false,
                            id: call_id,
                            name,
                            raw: String::new(),
                            arguments: None,
                        });
                    }
                    StreamDelta::ToolCallArgsDelta { call_id, text } => {
                        let Some(call) = calls
                            .iter_mut()
                            .find(|call| call.id == call_id && !call.args_completed)
                        else {
                            return Err(invalid_provider("tool arguments without open call").into());
                        };
                        call.raw.push_str(&text);
                        self.live_payload(
                            snapshot,
                            EventKind::ToolCallArgsDelta,
                            event_payload::CallDelta {
                                message_id: id.clone(),
                                call_id,
                                text,
                            },
                        );
                    }
                    StreamDelta::ToolCallDone { call_id } => {
                        let Some(call) = calls
                            .iter_mut()
                            .find(|call| call.id == call_id && !call.args_completed)
                        else {
                            return Err(
                                invalid_provider("tool completion without open call").into()
                            );
                        };
                        call.args_completed = true;
                        call.arguments = Some(
                            serde_json::from_str(&call.raw)
                                .unwrap_or_else(|_| Value::String(call.raw.clone())),
                        );
                        presentation.complete_call(&call_id);
                    }
                    StreamDelta::ProviderState { codec_id, payload } => {
                        provider_state.push((codec_id, payload));
                    }
                    StreamDelta::Usage(delta) => {
                        usage.input_tokens += delta.input_tokens;
                        usage.output_tokens += delta.output_tokens;
                    }
                    StreamDelta::Completed => {
                        completed = true;
                        break;
                    }
                    StreamDelta::Error(error) => {
                        return Err(error.into());
                    }
                }
            }
            if !completed {
                return Err(invalid_provider("model stream ended without completion").into());
            }
            if text.is_empty()
                && reasoning.is_empty()
                && calls.is_empty()
                && provider_state.is_empty()
            {
                return Err(invalid_provider("empty model response").into());
            }
            Ok(())
        }
        .await;
        let outcome = match &stream_result {
            Ok(()) => StreamOutcome::Completed,
            Err(RuntimeError::Interrupted) => StreamOutcome::Interrupted,
            Err(_) => StreamOutcome::Failed,
        };
        presentation.finish(outcome);
        if matches!(&stream_result, Err(RuntimeError::Interrupted))
            && (!text.is_empty() || !reasoning.is_empty())
        {
            let mut parts = Vec::new();
            if !text.is_empty() {
                push_part(
                    &mut parts,
                    &id,
                    PartKind::AssistantText,
                    ContentBlock::Text { text: text.clone() },
                );
            }
            if !reasoning.is_empty() {
                push_part(
                    &mut parts,
                    &id,
                    PartKind::Reasoning,
                    ContentBlock::Reasoning {
                        text: reasoning.clone(),
                        provider_state: None,
                    },
                );
            }
            ensure_lease(lease_lost)?;
            execution
                .append_message(Message {
                    id: id.clone(),
                    session_id: session_id.clone(),
                    run_id: Some(run_id.clone()),
                    role: Role::Assistant,
                    parent_id: None,
                    parts,
                    created_at: self.clock.now(),
                })
                .await?;
            self.committed_message(execution, plan, snapshot, &id)
                .await?;
        }
        if stream_result.is_err() {
            observe_model("error", &usage);
        }
        stream_result?;
        measurement.observation.reason = TerminalReason::Success;
        drop(measurement);
        observe_model("ok", &usage);
        let mut parts = Vec::new();
        if !text.is_empty() {
            push_part(
                &mut parts,
                &id,
                PartKind::AssistantText,
                ContentBlock::Text { text },
            );
        }
        if !reasoning.is_empty() {
            push_part(
                &mut parts,
                &id,
                PartKind::Reasoning,
                ContentBlock::Reasoning {
                    text: reasoning,
                    provider_state: None,
                },
            );
        }
        for (codec_id, payload) in provider_state {
            push_part(
                &mut parts,
                &id,
                PartKind::ProviderState,
                ContentBlock::ProviderState { codec_id, payload },
            );
        }
        for call in &calls {
            push_part(
                &mut parts,
                &id,
                PartKind::FunctionToolCall,
                ContentBlock::ToolCall {
                    call_id: call.id.clone(),
                    name: call.name.clone(),
                    arguments: call
                        .arguments
                        .clone()
                        .unwrap_or_else(|| Value::String(call.raw.clone())),
                },
            );
        }
        let message = Message {
            id: id.clone(),
            session_id: session_id.clone(),
            run_id: Some(run_id.clone()),
            role: Role::Assistant,
            parent_id: None,
            parts,
            created_at: self.clock.now(),
        };
        ensure_lease(lease_lost)?;
        execution.append_message(message).await?;
        self.committed_message(execution, plan, snapshot, &id)
            .await?;
        plan.dispatcher
            .notify::<ModelCompleted>(json!({"usage":usage}))
            .await;
        Ok((calls, usage))
    }

    #[allow(clippy::too_many_arguments)]
    async fn stage_tool(
        &self,
        execution: &dyn ExecutionStore,
        session_id: &SessionId,
        run_id: &RunId,
        plan: &RunPlan,
        call: PendingCall,
        lease_lost: &AtomicBool,
    ) -> Result<ToolCallRecord, RuntimeError> {
        let definition = plan
            .tools
            .iter()
            .find(|tool| tool.info.name == call.name)
            .cloned();
        let raw = call.arguments.unwrap_or(Value::String(call.raw));
        let prepared = if let Some(tool) = &definition {
            match validate_arguments(&tool.info, &raw) {
                Ok(()) => match self.tool_pipeline.prepare(&tool.info, raw.clone()).await {
                    Ok(value) => match plan.dispatcher.transform::<ToolPrepare>(json!({"name":tool.info.name,"call_id":call.id.to_string(),"input":value})).await {
                        Ok(output) => {
                            let input = output.get("input").cloned().unwrap_or(output);
                            validate_arguments(&tool.info, &input).map(|()| input)
                        }
                        Err(error) => Err(error.to_string()),
                    },
                    Err(error) => Err(error),
                },
                Err(error) => Err(error),
            }
        } else {
            Err(format!("unknown tool: {}", call.name))
        };
        ensure_lease(lease_lost)?;
        let mut event = self.event(session_id, run_id, EventKind::ToolCallPending);
        event.turn_id = call.turn_id.clone();
        event.payload = json!({"call_id":call.id,"name":call.name,"status":"pending", "message_id":call.message_id});
        event.correlation = Some(call.id.to_string());
        let record = ToolCallRecord {
            id: call.id,
            run_id: run_id.clone(),
            name: call.name,
            arguments: prepared.unwrap_or_else(|error| json!({"$crabber_prepare_error":error})),
            status: ToolCallStatus::Pending,
            retry_safe: definition.as_ref().is_some_and(|tool| tool.info.retry_safe),
            result: None,
        };
        execution
            .create_tool_call(record.clone(), event.clone())
            .await?;
        self.observer.emit(&event);
        plan.dispatcher
            .notify::<EventPublished>(serde_json::to_value(&event).unwrap_or(Value::Null))
            .await;
        Ok(record)
    }

    #[allow(clippy::too_many_lines, clippy::too_many_arguments)]
    async fn execute_tool(
        &self,
        execution: &dyn ExecutionStore,
        session_id: &SessionId,
        run_id: &RunId,
        plan: &RunPlan,
        call: PendingCall,
        existing: Option<ToolCallRecord>,
        settlement: Option<(usize, watch::Receiver<usize>, watch::Sender<usize>)>,
        cancellation: &CancellationToken,
        lease_lost: &AtomicBool,
    ) -> Result<(), RuntimeError> {
        let definition = plan
            .tools
            .iter()
            .find(|tool| tool.info.name == call.name)
            .cloned();
        let record = existing.expect("tool calls are staged before execution");
        let prepared = if let Some(error) = record
            .arguments
            .get("$crabber_prepare_error")
            .and_then(Value::as_str)
        {
            Err(error.to_owned())
        } else {
            Ok(record.arguments)
        };
        ensure_lease(lease_lost)?;
        let tool_name = call.name.clone();
        let mut measurement = Measurement::new(
            self.observer.as_ref(),
            self.monotonic_clock.as_ref(),
            session_id,
            run_id,
            OperationKind::Tool {
                name: tool_name.clone(),
            },
            cancellation,
            lease_lost,
        );
        let tool_started_at = self.monotonic_clock.now();
        let mut running = self.event(session_id, run_id, EventKind::ToolCallRunning);
        running.turn_id = call.turn_id.clone();
        running.payload = json!({"call_id": call.id, "name": tool_name, "status": "running"});
        running.correlation = Some(call.id.to_string());
        execution
            .claim_tool_call(&call.id, running.clone())
            .await
            .inspect_err(|_| {
                measurement.observation.reason = TerminalReason::RuntimeError;
            })?;
        self.observer.emit(&running);
        let running_projection = serde_json::to_value(&running).unwrap_or(Value::Null);
        plan.dispatcher
            .notify::<EventPublished>(running_projection.clone())
            .await;
        plan.dispatcher
            .notify::<crabber_extension::ToolStarted>(running_projection)
            .await;
        execution
            .renew_lease(self.clock.now() + time::Duration::seconds(30))
            .await
            .inspect_err(|_| {
                measurement.observation.reason = TerminalReason::RuntimeError;
            })?;
        ensure_lease(lease_lost)?;
        let outcome: Result<Value, String> = match (definition, prepared) {
            (Some(tool), Ok(arguments)) => {
                tokio::select! {
                    () = cancellation.cancelled() => return Err(RuntimeError::Interrupted),
                    result = self.permit_and_execute(
                    execution,
                    session_id,
                    run_id,
                    &call.id,
                    plan,
                    &tool,
                    arguments,
                    cancellation,
                    lease_lost,
                    ) => result.inspect_err(|error| {
                        measurement.observation.reason = match error {
                            RuntimeError::LeaseLost => TerminalReason::LeaseLost,
                            RuntimeError::Interrupted => TerminalReason::Cancelled,
                            RuntimeError::Paused => TerminalReason::Paused,
                            _ => TerminalReason::RuntimeError,
                        };
                    })?,
                }
            }
            (_, Err(error)) => Err(error),
            (None, Ok(_)) => Err("unknown tool".into()),
        };
        ensure_lease(lease_lost)?;
        let original_error = outcome.is_err();
        let seed = match outcome {
            Ok(value) => value,
            Err(error) => Value::String(error),
        };
        let outcome = match plan
            .dispatcher
            .transform::<ToolResultTransform>(json!({"result":seed,"is_error":original_error}))
            .await
        {
            Ok(output) => {
                let result = output
                    .get("result")
                    .cloned()
                    .unwrap_or_else(|| output.clone());
                if original_error || output.get("is_error").and_then(Value::as_bool) == Some(true) {
                    Err(result
                        .as_str()
                        .map_or_else(|| result.to_string(), str::to_owned))
                } else {
                    Ok(result)
                }
            }
            Err(error) => Err(error.to_string()),
        };
        let (status, output, is_error) = match outcome {
            Ok(value) => (ToolResultStatus::Completed, value, false),
            Err(error) => (ToolResultStatus::Failed, Value::String(error), true),
        };
        measurement.observation.reason = if is_error {
            TerminalReason::ToolError
        } else {
            TerminalReason::Success
        };
        drop(measurement);
        let text = serde_json::to_string(&output).expect("JSON value serializes");
        let content = vec![ContentBlock::Text { text }];
        let result = ToolResult {
            status,
            content: content.clone(),
        };
        let message_id = MessageId::new();
        let message = Message {
            id: message_id.clone(),
            session_id: session_id.clone(),
            run_id: Some(run_id.clone()),
            role: Role::Tool,
            parent_id: None,
            parts: vec![Part {
                id: PartId::new(),
                message_id,
                ordinal: 0,
                kind: PartKind::FunctionToolResult,
                content: ContentBlock::ToolResult {
                    call_id: call.id.clone(),
                    content,
                    is_error,
                },
            }],
            created_at: self.clock.now(),
        };
        let mut settled = self.event(session_id, run_id, EventKind::ToolCallSettled);
        settled.turn_id = call.turn_id.clone();
        settled.payload = json!(crate::event_payload::Settlement {
            result: event_payload::CallSettled {
                call_id: call.id.clone(),
                message_id: message.id.clone(),
                content: std::borrow::Cow::Borrowed(&result.content),
                is_error,
            },
            diagnostics: json!({"name": tool_name, "status": status,
                "tool": tool_name, "tool_id": call.id.to_string(),
                "duration_ms": self.monotonic_clock.now().saturating_sub(tool_started_at).as_millis()}),
        });
        settled.correlation = Some(call.id.to_string());
        if let Some((index, receiver, _)) = &settlement {
            let mut receiver = receiver.clone();
            while *receiver.borrow_and_update() != *index {
                receiver
                    .changed()
                    .await
                    .map_err(|_| RuntimeError::TaskStopped)?;
            }
        }
        execution
            .settle_tool_call(&call.id, result, message, settled.clone())
            .await?;
        self.observer.emit(&settled);
        let settled_projection = serde_json::to_value(&settled).unwrap_or(Value::Null);
        plan.dispatcher
            .notify::<EventPublished>(settled_projection.clone())
            .await;
        plan.dispatcher
            .notify::<crabber_extension::ToolSettled>(settled_projection)
            .await;
        if let Some((index, _, sender)) = settlement {
            let _ = sender.send(index + 1);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn permit_and_execute(
        &self,
        execution: &dyn ExecutionStore,
        session_id: &SessionId,
        run_id: &RunId,
        call_id: &ToolCallId,
        plan: &RunPlan,
        tool: &ToolDefinition,
        arguments: Value,
        cancellation: &CancellationToken,
        lease_lost: &AtomicBool,
    ) -> Result<Result<Value, String>, RuntimeError> {
        let workspace = self.workspace()?.clone();
        let guard_decisions: Vec<_> = plan
            .guards
            .iter()
            .map(|guard| {
                guard.check_with_context(GuardContext {
                    tool: &tool.info,
                    arguments: &arguments,
                    call_id,
                    session_id,
                    run_id,
                })
            })
            .collect();
        let guard_denied = guard_decisions.contains(&GuardDecision::Deny);
        let restricted = plan
            .restrictions
            .iter()
            .any(|set| !set.iter().any(|name| name == &tool.info.name));
        let allowed = if guard_denied || restricted {
            false
        } else {
            let decision = if guard_decisions.contains(&GuardDecision::Ask) {
                PermissionDecision::Ask
            } else if guard_decisions.contains(&GuardDecision::Allow) {
                PermissionDecision::Allow
            } else {
                self.policy.decide(&tool.info, &arguments)
            };
            match decision {
                PermissionDecision::Allow => true,
                PermissionDecision::Deny => false,
                PermissionDecision::Ask => {
                    self.emit_durable(
                        execution,
                        session_id,
                        run_id,
                        plan,
                        EventKind::PermissionRequested,
                    )
                    .await?;
                    let approved = self.approver.approve(&tool.info, &arguments).await;
                    self.emit_durable(
                        execution,
                        session_id,
                        run_id,
                        plan,
                        EventKind::PermissionDecided,
                    )
                    .await?;
                    approved
                }
            }
        };
        if !allowed {
            return Ok(Err("permission denied".into()));
        }
        ensure_lease(lease_lost)?;
        execution
            .renew_lease(self.clock.now() + time::Duration::seconds(30))
            .await
            .map_err(|_| RuntimeError::LeaseLost)?;
        ensure_lease(lease_lost)?;
        let observer = Arc::clone(&self.observer);
        let session_for_progress = session_id.clone();
        let run_for_progress = run_id.clone();
        let clock = Arc::clone(&self.clock);
        let progress = Arc::new(move |content: ContentBlock| {
            let event = EventRecord {
                cursor: None,
                session_id: session_for_progress.clone(),
                run_id: run_for_progress.clone(),
                turn_id: None,
                kind: EventKind::Custom {
                    name: "tool_progress".into(),
                },
                payload: json!({"content":content}),
                correlation: None,
                live_only: true,
                created_at: clock.now(),
            };
            observer.emit(&event);
        });
        let context = ToolContext::new(
            session_id.clone(),
            run_id.clone(),
            call_id.clone(),
            cancellation.clone(),
            self.host_services.clone(),
            workspace,
            progress,
            Some(Arc::new(HostApproval {
                approver: Arc::clone(&self.approver),
                tool: tool.info.clone(),
                arguments: arguments.clone(),
            })),
        );
        let executor = Arc::clone(&tool.executor);
        let authorized_arguments = arguments.clone();
        let terminal: Callback = Arc::new(move |input| {
            let executor = Arc::clone(&executor);
            let context = context.clone();
            let authorized_arguments = authorized_arguments.clone();
            Box::pin(async move {
                if input != authorized_arguments {
                    return Err(crabber_extension::ExtensionError::Tool(
                        "around handler changed immutable tool input".into(),
                    ));
                }
                executor
                    .execute_with_context(context, authorized_arguments)
                    .await
            })
        });
        let output = match plan
            .dispatcher
            .around::<ToolExecute>(arguments, terminal)
            .await
        {
            Ok(output) => output,
            Err(error) => return Ok(Err(error.to_string())),
        };
        ensure_lease(lease_lost)?;
        Ok(self
            .tool_pipeline
            .transform_result(&tool.info, output)
            .await)
    }
}

fn push_part(parts: &mut Vec<Part>, message_id: &MessageId, kind: PartKind, content: ContentBlock) {
    parts.push(Part {
        id: PartId::new(),
        message_id: message_id.clone(),
        ordinal: u32::try_from(parts.len()).expect("assistant part count fits u32"),
        kind,
        content,
    });
}

fn invalid_provider(message: &str) -> ProviderError {
    ProviderError {
        kind: crabber_providers::ProviderErrorKind::Invalid,
        message: message.into(),
        retryable: false,
    }
}

fn ensure_lease(lost: &AtomicBool) -> Result<(), RuntimeError> {
    if lost.load(Ordering::SeqCst) {
        Err(RuntimeError::LeaseLost)
    } else {
        Ok(())
    }
}

fn validate_arguments(tool: &ToolInfo, arguments: &Value) -> Result<(), String> {
    let validator = jsonschema::validator_for(&tool.parameters)
        .map_err(|error| format!("invalid tool schema: {error}"))?;
    validator
        .validate(arguments)
        .map_err(|error| format!("invalid tool arguments: {error}"))
}

struct HostApproval {
    approver: Arc<dyn ApprovalRequester>,
    tool: ToolInfo,
    arguments: Value,
}
#[async_trait]
impl ApprovalFacade for HostApproval {
    async fn request(&self, _reason: &str) -> bool {
        self.approver.approve(&self.tool, &self.arguments).await
    }
}

struct RetryDecision {
    retry: bool,
    delay_ms: u64,
    compaction_requested: bool,
}

async fn request_error(
    plan: &RunPlan,
    error: &ProviderError,
    attempt: u32,
    retry: bool,
    delay_ms: u64,
) -> Result<RetryDecision, RuntimeError> {
    let class = format!("{:?}", error.kind);
    let seed = json!({"class":class,"attempt":attempt,"retry":retry,"delay_ms":delay_ms,"compaction_requested":error.kind == crabber_providers::ProviderErrorKind::ContextOverflow});
    match plan.dispatcher.transform::<ModelRequestError>(seed).await {
        Ok(decision)
            if decision.get("class").and_then(Value::as_str) != Some(class.as_str())
                || decision.get("attempt").and_then(Value::as_u64) != Some(u64::from(attempt)) =>
        {
            Err(RuntimeError::Extension(
                "request-error handler changed immutable metadata".into(),
            ))
        }
        Ok(decision) if decision.get("retry").and_then(Value::as_bool) == Some(true) && !retry => {
            Err(RuntimeError::Extension(
                "request-error handler cannot enable retry".into(),
            ))
        }
        Ok(decision)
            if decision
                .get("delay_ms")
                .and_then(Value::as_u64)
                .unwrap_or(0)
                > delay_ms =>
        {
            Err(RuntimeError::Extension(
                "request-error handler cannot lengthen delay".into(),
            ))
        }
        Ok(decision)
            if decision
                .get("compaction_requested")
                .and_then(Value::as_bool)
                == Some(true)
                && error.kind != crabber_providers::ProviderErrorKind::ContextOverflow =>
        {
            Err(RuntimeError::Extension(
                "compaction is only valid for context overflow".into(),
            ))
        }
        Ok(decision) => Ok(RetryDecision {
            retry: decision["retry"].as_bool().unwrap_or(false),
            delay_ms: decision["delay_ms"].as_u64().unwrap_or(0),
            compaction_requested: decision["compaction_requested"].as_bool().unwrap_or(false),
        }),
        Err(handler) => Err(RuntimeError::Extension(handler.to_string())),
    }
}
