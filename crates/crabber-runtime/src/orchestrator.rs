#[path = "orchestrator/admission.rs"]
mod admission;
#[path = "orchestrator/approval.rs"]
mod approval;
#[path = "orchestrator/approval_recovery.rs"]
mod approval_recovery;
pub use approval_recovery::RecoverApproval;
#[path = "orchestrator/continuation.rs"]
mod continuation;
mod management;
#[path = "orchestrator/phase_control.rs"]
mod phase_control;
pub use continuation::{Continuation, ContinueApproval};
#[path = "orchestrator/attempt.rs"]
mod attempt;
#[path = "orchestrator/binding.rs"]
mod binding;
#[path = "orchestrator/input.rs"]
mod input;
#[path = "orchestrator/terminal.rs"]
mod terminal;
#[path = "orchestrator/tool_batch.rs"]
mod tool_batch;
#[path = "orchestrator/tool_execution.rs"]
mod tool_execution;
#[path = "orchestrator/tool_invoke.rs"]
mod tool_invoke;
#[path = "orchestrator/tool_phase.rs"]
mod tool_phase;
use crate::observation::{Measurement, result_reason};
use crate::policy::{
    IdentityToolPipeline, InterruptPolicy, PermissionDecision, PermissionPolicy, StaticPolicy,
    ToolPipeline,
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
    RunStatus, Session, SessionContext, SessionId, SystemClock, ToolCallId, ToolCallRecord,
    ToolCallStatus, ToolInfo, ToolResult, ToolResultStatus, TurnId, Usage,
};
use crabber_extension::{
    Callback, ContextAssemble, EventPublished, GuardContext, GuardDecision, HostServices,
    InputUnavailable, ModelCompleted, ModelRequestError, ModelRequested,
    ModelStream as ExtensionModelStream, PromptAttemptContext, PromptContributionOutcome,
    RunAdmitted, RunBeforeExecute, RunPlan, RunPlanProvider, RunSettled, RunStarted, StateSink,
    ToolContext, ToolDefinition, ToolExecute, ToolInput, ToolOutcomeClass, ToolPrepare,
    ToolResultContext, ToolResultOutcome, TransformOutput, TurnCompleted, TurnPrepare, TurnStarted,
    WorkspaceContext, WorkspaceReadError, WorkspaceReadErrorKind, WorkspaceReader,
    WorkspaceReaderResolver, collect_prompt_contributions_with_resolver,
    prompt_contribution_failed_message, result_transform_failed_message,
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
use futures::StreamExt;
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

struct AttemptWorkspaceReaderResolver {
    admitted: WorkspaceContext,
    host: Arc<dyn WorkspaceReaderResolver>,
}

#[async_trait]
impl WorkspaceReaderResolver for AttemptWorkspaceReaderResolver {
    async fn resolve(
        &self,
        workspace: &WorkspaceContext,
    ) -> Result<Arc<dyn WorkspaceReader>, WorkspaceReadError> {
        if workspace != &self.admitted {
            return Err(WorkspaceReadError::new(WorkspaceReadErrorKind::Denied));
        }
        self.host.resolve(workspace).await
    }
}

fn admission_error(error: StoreError) -> RuntimeError {
    if error == StoreError::Busy {
        RuntimeError::SessionBusy
    } else {
        RuntimeError::Store(error)
    }
}

#[derive(Debug, Clone)]
pub struct Request {
    /// Explicit selection or frozen host fallback for stored session defaults.
    pub selection_policy: crabber_session::AdmissionSelectionPolicy,
    /// Defaults and input share this captured session revision at admission.
    pub captured_revision: Option<crabber_core::SessionRevision>,
    /// Host-validated semantic input. None adapts the text convenience API.
    /// With Some, text must be empty or the exact plain-text adapter shape.
    pub input_envelope: Option<crabber_core::InputEnvelope>,
    pub session_id: Option<SessionId>,
    pub workspace_id: String,
    pub directory: String,
    pub title: String,
    pub text: String,
    pub selection: Selection,
    pub system_prompt: Option<String>,
    pub max_output_tokens: Option<u32>,
}

#[derive(Clone, Copy)]
enum InitialEntry {
    Unkeyed,
    Keyed,
}
struct InitialRequest {
    request: Request,
    entry: InitialEntry,
    bound: Option<crabber_providers::BoundProvider>,
}

impl Request {
    fn admission_data(&self) -> Result<AdmissionRequestData, StoreError> {
        Ok(AdmissionRequestData {
            selection_policy: self.selection_policy.clone(),
            session_id: self.session_id.clone().ok_or(StoreError::NotFound)?,
            workspace_id: self.workspace_id.clone(),
            directory: self.directory.clone(),
            title: self.title.clone(),
            text: self.text.clone(),
            provider_id: self.selection.provider_id.clone(),
            model_id: self.selection.model_id.clone(),
            system_prompt: self.system_prompt.clone(),
            max_output_tokens: self.max_output_tokens,
        })
    }
    fn from_admission(data: AdmissionRequestData) -> Self {
        Self {
            selection_policy: data.selection_policy,
            captured_revision: None,
            input_envelope: None,
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
            max_output_tokens: data.max_output_tokens,
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
/// left unfinished. Reconciliation never dispatches a Started/unknown attempt.
/// A failed atomic first-reservation check may already have rotated an expired
/// run fence; that ownership change does not establish an execution outcome.
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
    pub reason: RecoverySkipReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoverySkipReason {
    Store(StoreError),
    Attempt(crabber_session::AttemptError),
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("agent host is closed")]
    HostClosed,
    #[error("session is busy")]
    SessionBusy,
    #[error("session management: {0}")]
    Management(#[from] crabber_session::ManagementError),
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("admission execution: {0}")]
    AdmissionExecution(#[from] AdmissionExecutionError),
    #[error("execution attempt: {0}")]
    Attempt(#[from] crabber_session::AttemptError),
    #[error("durable approval: {0}")]
    Approval(#[from] crabber_session::ApprovalError),
    #[error("approval boundary persistence outcome is unknown")]
    ApprovalBoundaryUnknown {
        run_id: RunId,
        operation_id: crabber_core::ControlId,
        reason: crabber_session::ApprovalError,
    },
    #[error("provider: {0}")]
    Provider(#[from] ProviderError),
    #[error("provider binding: {0}")]
    ProviderBinding(#[from] crabber_providers::ProviderBindingError),
    #[error("extension: {0}")]
    Extension(String),
    #[error(transparent)]
    ExtensionPlan(#[from] crabber_extension::ExtensionError),
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
    /// The task stopped without acknowledged terminal persistence. Reconcile
    /// the retained run and events; this error proves neither rollback nor commit.
    #[error("run terminal persistence outcome is unknown")]
    TerminalOutcomeUnknown {
        run_id: RunId,
        #[source]
        reason: StoreError,
    },
    #[error("attempt boundary persistence outcome is unknown")]
    AttemptBoundaryUnknown {
        run_id: RunId,
        reason: crabber_session::AttemptError,
    },
    #[error("attempt resource cleanup is incomplete")]
    AttemptCleanupIncomplete { run_id: RunId },
    #[error("native tool phase outcome requires reconciliation")]
    PhaseOutcomeUnknown {
        run_id: RunId,
        phase: Box<crabber_session::PhaseTarget>,
        operation_id: Option<crabber_core::ControlId>,
        reason: crabber_session::PhaseError,
    },
    #[error("tool execution outcome requires reconciliation")]
    ToolOutcomeUnknown { run_id: RunId, call_id: ToolCallId },
    #[error("run requires its durable continuation claim")]
    DurableContinuationRequired { run_id: RunId },
    #[error("run paused")]
    Paused,
    #[error("run plan changed since checkpoint")]
    PlanChanged,
}

#[derive(Debug, Clone)]
pub struct TurnSnapshot {
    pub context: Vec<crabber_core::InputContext>,
    pub identity: RequestIdentity,
    pub selection: Selection,
    pub messages: Vec<Message>,
    pub system: Option<String>,
    pub tools: Vec<ToolInfo>,
    pub max_output_tokens: Option<u32>,
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
    /// Local mount-close timeout; a registry close has no session or run identity.
    fn mount_close_timed_out(&self, _timeout: &crabber_extension::MountCloseTimeout) {}
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
    behavior_fingerprint: crabber_core::InputFingerprint,
    execution_tasks: crate::execution_tasks::ExecutionTasks,
    owned_attempt: Option<Arc<attempt::OwnedAttempt>>,
    resolver: Arc<dyn Resolver>,
    workspace_reader_resolver: Option<Arc<dyn WorkspaceReaderResolver>>,
    plan_provider: Arc<dyn RunPlanProvider>,
    clock: Arc<dyn Clock>,
    monotonic_clock: Arc<dyn MonotonicClock>,
    observer: Arc<dyn Observer>,
    model_stream: Arc<dyn ModelStream>,
    policy: Arc<dyn PermissionPolicy>,
    tool_pipeline: Arc<dyn ToolPipeline>,
    host_services: HostServices,
    max_turns: usize,
    heartbeat_interval: Duration,
    execution_mode: ExecutionMode,
    compaction: CompactionPolicy,
    /// Test seam: sees every authoritative result context `execute_tool` builds.
    #[cfg(test)]
    context_observer: Option<ResultContextObserver>,
}

#[cfg(test)]
pub(crate) type ResultContextObserver = Arc<dyn Fn(&ToolResultContext) + Send + Sync>;

/// The persisted session a run executes in, carried along the run path so a
/// run cannot execute tools or assemble context without its workspace.
#[derive(Clone)]
struct RunSession {
    id: SessionId,
    workspace: WorkspaceContext,
}
impl From<&Session> for RunSession {
    fn from(session: &Session) -> Self {
        Self {
            id: session.id.clone(),
            workspace: WorkspaceContext::from_persisted(&session.workspace_id, &session.directory),
        }
    }
}

/// The request saved in a paused run's checkpoint, written and read only here.
#[derive(serde::Serialize, serde::Deserialize)]
struct CheckpointRequest {
    #[serde(
        default,
        skip_serializing_if = "crabber_session::AdmissionSelectionPolicy::is_explicit"
    )]
    selection_policy: crabber_session::AdmissionSelectionPolicy,
    #[serde(default)]
    version: u32,
    #[serde(default)]
    input_digest: Option<String>,
    workspace_id: String,
    directory: String,
    title: String,
    text: String,
    provider_id: String,
    model_id: String,
    system_prompt: Option<String>,
    max_output_tokens: Option<u32>,
}
impl CheckpointRequest {
    fn new(request: &Request) -> Self {
        Self {
            selection_policy: request.selection_policy.clone(),
            version: 2,
            input_digest: request
                .input_envelope
                .as_ref()
                .map(|input| admission_config_hash(&serde_json::json!(input))),
            workspace_id: request.workspace_id.clone(),
            directory: request.directory.clone(),
            title: request.title.clone(),
            text: request.text.clone(),
            provider_id: request.selection.provider_id.clone(),
            model_id: request.selection.model_id.clone(),
            system_prompt: request.system_prompt.clone(),
            max_output_tokens: request.max_output_tokens,
        }
    }
    /// `None` when the checkpoint saved no request; a request that does not
    /// decode is corrupt, not drift.
    fn from_checkpoint(checkpoint: Option<&Value>) -> Result<Option<Self>, StoreError> {
        checkpoint
            .and_then(|checkpoint| checkpoint.get("request"))
            .map(|request| {
                let invalid = || StoreError::Validation("checkpoint request is invalid".into());
                if request.get("max_output_tokens").is_none() {
                    return Err(invalid());
                }
                let saved =
                    <Self as serde::Deserialize>::deserialize(request).map_err(|_| invalid())?;
                if saved.max_output_tokens == Some(0) {
                    return Err(invalid());
                }
                Ok(saved)
            })
            .transpose()
    }
    fn into_request(self, session_id: &SessionId) -> Request {
        Request {
            selection_policy: self.selection_policy,
            captured_revision: None,
            input_envelope: None,
            session_id: Some(session_id.clone()),
            workspace_id: self.workspace_id,
            directory: self.directory,
            title: self.title,
            text: self.text,
            selection: Selection {
                provider_id: self.provider_id,
                model_id: self.model_id,
            },
            system_prompt: self.system_prompt,
            max_output_tokens: self.max_output_tokens,
        }
    }
}

#[derive(Default)]
pub struct OrchestratorBuilder {
    behavior_fingerprint: Option<crabber_core::InputFingerprint>,
    store: Option<Arc<dyn Store>>,
    resolver: Option<Arc<dyn Resolver>>,
    workspace_reader_resolver: Option<Arc<dyn WorkspaceReaderResolver>>,
    plan_provider: Option<Arc<dyn RunPlanProvider>>,
    clock: Option<Arc<dyn Clock>>,
    monotonic_clock: Option<Arc<dyn MonotonicClock>>,
    observer: Option<Arc<dyn Observer>>,
    model_stream: Option<Arc<dyn ModelStream>>,
    policy: Option<Arc<dyn PermissionPolicy>>,
    tool_pipeline: Option<Arc<dyn ToolPipeline>>,
    host_services: Option<HostServices>,
    max_turns: Option<usize>,
    heartbeat_interval: Option<Duration>,
    execution_mode: Option<ExecutionMode>,
    compaction: Option<CompactionPolicy>,
    #[cfg(test)]
    context_observer: Option<ResultContextObserver>,
}

impl OrchestratorBuilder {
    #[cfg(test)]
    pub(crate) fn context_observer(mut self, observer: ResultContextObserver) -> Self {
        self.context_observer = Some(observer);
        self
    }

    /// Declare the nonsecret version of opaque native policy/callback behavior.
    /// Change it whenever relevant behavior changes; credentials are excluded.
    #[must_use]
    pub fn behavior_fingerprint(mut self, value: crabber_core::InputFingerprint) -> Self {
        self.behavior_fingerprint = Some(value);
        self
    }
    /// Hash an explicit host behavior version in the library's versioned domain.
    /// # Panics
    /// Panics only if a library SHA-256 digest ceases to be a valid fingerprint.
    #[must_use]
    pub fn behavior_version(self, version: &str) -> Self {
        let fingerprint = crabber_core::InputFingerprint::new(admission_config_hash(&json!([
            "crabber.host-behavior.v1",
            version
        ])))
        .expect("SHA-256 is a valid fingerprint");
        self.behavior_fingerprint(fingerprint)
    }

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
    /// Grants typed model middleware host-authorized, per-workspace readers.
    #[must_use]
    pub fn workspace_reader_resolver(mut self, value: Arc<dyn WorkspaceReaderResolver>) -> Self {
        self.workspace_reader_resolver = Some(value);
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
            execution_tasks: crate::execution_tasks::ExecutionTasks::default(),
            owned_attempt: None,
            store: self.store.ok_or(RuntimeError::Missing("store"))?,
            resolver: self.resolver.ok_or(RuntimeError::Missing("resolver"))?,
            workspace_reader_resolver: self.workspace_reader_resolver,
            plan_provider: self
                .plan_provider
                .ok_or(RuntimeError::Missing("plan provider"))?,
            behavior_fingerprint: self
                .behavior_fingerprint
                .ok_or(RuntimeError::Missing("host behavior fingerprint"))?,
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
            tool_pipeline: self
                .tool_pipeline
                .unwrap_or_else(|| Arc::new(IdentityToolPipeline)),
            host_services: self.host_services.unwrap_or_default(),
            max_turns: self.max_turns.unwrap_or(64),
            heartbeat_interval,
            execution_mode: self.execution_mode.unwrap_or_default(),
            compaction,
            #[cfg(test)]
            context_observer: self.context_observer,
        })
    }
}

pub struct RunHandle {
    session_id: SessionId,
    run_id: RunId,
    store: Arc<dyn Store>,
    clock: Arc<dyn Clock>,
    done: oneshot::Receiver<Result<RunResult, RuntimeError>>,
    task: Arc<crate::execution_tasks::ExecutionTask>,
    tasks: crate::execution_tasks::ExecutionTasks,
    completion: watch::Receiver<bool>,
    cancellation: CancellationToken,
    events: Option<crate::RunEvents>,
}

/// Cloneable local control/observation of one worker. It carries no fence and
/// grants no durable cancellation authority. Dropping it does not cancel work.
#[derive(Clone)]
pub struct RunControl {
    session_id: SessionId,
    run_id: RunId,
    cancellation: CancellationToken,
    completion: watch::Receiver<bool>,
}
impl RunControl {
    /// Signal local cancellation. This is not a persisted request/acknowledgement.
    pub fn interrupt(&self) {
        self.cancellation.cancel();
    }
    #[must_use]
    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }
    #[must_use]
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    /// Publication of the worker result; never a join or proof of cleanup.
    #[must_use]
    pub fn completion_signal(&self) -> watch::Receiver<bool> {
        self.completion.clone()
    }
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
    /// Clone local control while retaining sole ownership of the event receiver.
    #[must_use]
    pub fn control(&self) -> RunControl {
        RunControl {
            session_id: self.session_id.clone(),
            run_id: self.run_id.clone(),
            cancellation: self.cancellation.clone(),
            completion: self.completion.clone(),
        }
    }

    /// Take the sole bounded event receiver for this worker.
    /// # Panics
    /// Panics if the receiver was already taken.
    #[must_use]
    pub fn events(&mut self) -> crate::RunEvents {
        self.events.take().expect("events receiver already taken")
    }

    /// Requests cancellation of the active provider stream or tool body.
    /// `done` reports settlement failures, including unknown durable outcomes.
    pub fn interrupt(&self) {
        self.cancellation.cancel();
    }
    /// Signals publication of the worker result, whether success or failure.
    /// This signal is not a join or proof of durable outcome/cleanup.
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
    /// Waits for the worker result and its actual task join. This joins the
    /// worker only; extension-owned resources require separate cleanup.
    ///
    /// # Errors
    ///
    /// Returns the run failure or `TaskStopped` if its task ended unexpectedly.
    pub async fn done(self) -> Result<RunResult, RuntimeError> {
        let result = self.done.await;
        if self.tasks.join(&self.task).await != crate::TaskTermination::Returned {
            return Err(RuntimeError::TaskStopped);
        }
        result.map_err(|_| RuntimeError::TaskStopped)?
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

    /// Declared nonsecret native behavior identity, required for exact recovery.
    #[must_use]
    pub fn behavior_fingerprint(&self) -> &crabber_core::InputFingerprint {
        &self.behavior_fingerprint
    }

    /// Joins retained initial, keyed-recovery and resume workers within a shared
    /// deadline. A dropped run handle leaves its worker retained here.
    /// This does not stop ingress, cancel work, or prove durable settlement or
    /// external resource cleanup. Inspect `pending`, `pending_preparations` and
    /// `resources` (sealed state, pending joins and failures). Active tool scopes
    /// remain open; the host must stop its ingress before a final drain.
    pub async fn drain_execution_tasks(&self, bound: Duration) -> crate::ExecutionTaskDrain {
        self.execution_tasks.drain(bound).await
    }

    async fn own_preparation<T: Send + 'static>(
        &self,
        work: impl std::future::Future<Output = Result<T, RuntimeError>> + Send + 'static,
    ) -> Result<T, RuntimeError> {
        let (sender, receiver) = oneshot::channel();
        // This task owns the entire prepare/admit/claim future before its first
        // storage await. Dropping the caller detaches only this result waiter.
        let task = self.execution_tasks.spawn_preparation(async move {
            let _ = sender.send(work.await);
        });
        let outcome = receiver.await;
        if self.execution_tasks.join(&task).await != crate::TaskTermination::Returned {
            return Err(RuntimeError::TaskStopped);
        }
        outcome.map_err(|_| RuntimeError::TaskStopped)?
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
        self.resume_execution_with_context(run_id, context)
            .await?
            .done()
            .await
    }

    /// Observe/control a resumed worker using the same handle as initial admission.
    /// Eligibility, frozen-plan restoration and claim execute under retained task
    /// ownership. A handle is observation, never a grant of execution authority.
    /// # Errors
    /// Returns missing/invalid identity; `done` reports subsequent eligibility,
    /// binding, claim, execution and terminal-persistence failures.
    pub async fn resume_execution(&self, run_id: &RunId) -> Result<RunHandle, RuntimeError> {
        self.resume_execution_with_context(run_id, None).await
    }
    /// Resume with explicit correlation, excluded from persisted execution authority.
    /// # Errors
    /// Returns the same failures as `resume_execution`.
    pub async fn resume_execution_with_context(
        &self,
        run_id: &RunId,
        context: Option<TraceContext>,
    ) -> Result<RunHandle, RuntimeError> {
        let runtime = self.with_context(context);
        let run_id = run_id.clone();
        self.own_preparation(async move {
            let run = runtime
                .store
                .get_run(&run_id)
                .await?
                .ok_or(StoreError::NotFound)?;
            let validated = runtime.validated_session(&run).await?;
            let session_id = validated.0.id.clone();
            Ok(runtime.spawn_worker(
                session_id,
                run_id,
                move |runtime, cancellation| async move {
                    Box::pin(runtime.resume_loaded(run, validated, cancellation, None)).await
                },
            ))
        })
        .await
    }

    /// Loads the run's persisted session and compares it with the request saved
    /// in the run checkpoint, when there is one.
    async fn validated_session(
        &self,
        run: &crabber_core::Run,
    ) -> Result<(RunSession, Option<Request>), StoreError> {
        let session = self
            .store
            .get_session(&run.session_id)
            .await?
            .ok_or(StoreError::NotFound)?;
        let saved = CheckpointRequest::from_checkpoint(run.checkpoint.as_ref())?;
        if let Some(saved) = &saved {
            session.ensure_identity(&saved.workspace_id, &saved.directory)?;
        }
        let saved = if let Some(saved) = saved {
            let binding = self
                .store
                .execution_binding(&session.id, &run.id)
                .await?
                .ok_or(StoreError::HostUnsupported)?
                .capsule;
            let input = binding
                .input_envelope
                .as_ref()
                .ok_or(StoreError::HostUnsupported)?;
            if saved.version != 2
                || saved.input_digest.as_deref()
                    != Some(admission_config_hash(&serde_json::json!(input)).as_str())
            {
                return Err(StoreError::HostUnsupported);
            }
            let mut request = saved.into_request(&session.id);
            request.input_envelope = Some(input.clone());
            Some(request)
        } else {
            None
        };
        Ok((RunSession::from(&session), saved))
    }

    #[allow(clippy::too_many_lines)]
    async fn resume_loaded(
        &self,
        run: crabber_core::Run,
        (session, saved): (RunSession, Option<Request>),
        cancellation: CancellationToken,
        approval: Option<continuation::ApprovalResume>,
    ) -> Result<RunResult, RuntimeError> {
        let run_id = &run.id.clone();
        if self.store.admission_execution_state(run_id).await?
            == Some(crabber_session::AdmissionExecutionState::Unstarted)
        {
            return Err(StoreError::AdmissionRecoveryRequired.into());
        }
        if approval.is_none()
            && (run.status.is_terminal()
                || (run.status != RunStatus::Paused && run.lease_until > self.clock.now()))
        {
            return Err(StoreError::Conflict.into());
        }
        if approval.is_none()
            && run
                .checkpoint
                .as_ref()
                .is_some_and(|checkpoint| checkpoint.get("approval").is_some())
        {
            return Err(RuntimeError::DurableContinuationRequired {
                run_id: run.id.clone(),
            });
        }
        let binding = self.stored_binding(&session.id, run_id).await?;
        if let Some(saved) = &saved {
            self.verify_binding_request(saved, &session.id, &binding)?;
        }
        let original = Request::from_binding(&binding);
        let (plan, prepared, _) = self.prepare_admission(&original).await?;
        if prepared.binding.provider != binding.provider {
            return Err(crabber_providers::ProviderBindingError::VersionConflict.into());
        }
        if prepared.binding != binding || plan.fingerprint().to_string() != run.plan_fingerprint {
            plan.release();
            return Err(RuntimeError::PlanChanged);
        }
        let bound = self.restore_provider(&plan, &binding.provider).await?;
        if !self.store.supports_owned_attempts()
            || !self.store.supports_owned_tool_phases()
            || !self.store.supports_pre_effect_approvals()
        {
            return Err(StoreError::HostUnsupported.into());
        }
        let resume_turns_used = match run.checkpoint.as_ref() {
            Some(checkpoint) => checkpoint
                .get("turns_used")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .filter(|count| *count > 0 && *count <= self.max_turns)
                .ok_or(StoreError::HostUnsupported)?,
            None if run.status == RunStatus::Running => {
                match self.store.latest_execution_attempt(run_id).await? {
                    None => 0,
                    Some(previous)
                        if previous.attempt.state == crabber_session::AttemptState::Unstarted =>
                    {
                        0
                    }
                    Some(_) => return Err(crabber_session::AttemptError::Busy.into()),
                }
            }
            None => return Err(StoreError::HostUnsupported.into()),
        };
        if approval.is_none() && run.checkpoint.is_some() {
            tool_batch::restore_paused_calls(
                &run,
                self.store.list_unfinished_tool_calls(run_id).await?,
            )?;
        }
        let Some(acquisition) = self.acquire_resume(&run, approval).await? else {
            plan.release();
            return Ok(RunResult {
                session_id: run.session_id,
                run_id: run.id,
                status: run.status,
                usage: run.usage,
            });
        };
        let continuation::ResumeAuthority {
            fence,
            native_unstarted,
            granted,
            approved,
        } = acquisition;
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
        let mut worker = self.clone();
        worker.owned_attempt = Some(Arc::new(match granted {
            Some(owner) => {
                self.begin_granted_attempt(execution.as_ref(), &run.session_id, owner)
                    .await?
            }
            None => {
                self.begin_owned_attempt(execution.as_ref(), &run.session_id, run_id, true)
                    .await?
            }
        }));
        let self_ = &worker;
        let heartbeat = HeartbeatGuard::start(
            Arc::clone(&execution),
            Arc::clone(&self.store),
            fence.clone(),
            Arc::clone(&self.clock),
            self.heartbeat_interval,
        );
        let mut signal = heartbeat.signal.clone();
        let lost = Arc::clone(&heartbeat.lost);
        let work_cancellation = cancellation.clone();
        let state_sink: Arc<dyn StateSink> = Arc::new(RunStateSink {
            store: Arc::clone(&self.store),
            execution: Arc::clone(&execution),
            session_id: run.session_id.clone(),
            lost: Arc::clone(&lost),
        });
        let failure_execution = execution.clone();
        let failure_plan = plan.clone();
        let failure_session = run.session_id.clone();
        let failure_started_at = run.created_at;
        let failure_lost = lost.clone();
        let current_turn = Arc::new(std::sync::Mutex::new(None));
        let work_turn = current_turn.clone();
        let approval_turn = approved.as_ref().and_then(|batch| batch.turn.clone());
        let resumed_turn = approval_turn.clone().or_else(|| {
            run.checkpoint
                .as_ref()
                .and_then(|checkpoint| checkpoint.get("source_turn_id"))
                .and_then(Value::as_str)
                .map(|id| TurnId(id.to_owned()))
        });
        let approval_usage = approved.as_ref().map(|batch| batch.usage.clone());
        if approved
            .as_ref()
            .is_some_and(|batch| batch.turns_used != resume_turns_used)
        {
            return Err(crabber_session::ApprovalError::UnknownStoreOutcome.into());
        }
        let was_approval = approved.is_some();
        let resumed_work = async move {
            let resumed = self.event(&run.session_id, run_id, EventKind::RunResumed);
            execution.append_event(resumed.clone()).await?;
            self.observer.emit(&resumed);
            if native_unstarted && run.checkpoint.is_none() {
                let usage = self_
                    .run_loop(
                        execution.as_ref(),
                        run_id,
                        &session,
                        &original,
                        &plan,
                        &bound,
                        &work_cancellation,
                        true,
                        run.usage.clone(),
                        resume_turns_used,
                        lost.as_ref(),
                        &work_turn,
                    )
                    .await?;
                return Ok(RunResult {
                    session_id: run.session_id.clone(),
                    run_id: run_id.clone(),
                    status: RunStatus::Completed,
                    usage,
                });
            }
            let committed_turn = if run
                .checkpoint
                .as_ref()
                .and_then(|checkpoint| checkpoint.get("version"))
                .and_then(Value::as_u64)
                == Some(4)
            {
                // Native v4 boundaries already committed the complete prepared
                // batch. Later model history cannot add or reorder its calls.
                None
            } else {
                self.reconcile_committed_assistant(execution.as_ref(), &run, &plan, lost.as_ref())
                    .await?
            };
            // A stopped attempt cannot manufacture an outcome for a call whose
            // body may already have caused an external effect.
            let pending_calls = self.store.list_unfinished_tool_calls(run_id).await?;
            let pending_calls = if approved.is_none() && run.checkpoint.is_some() {
                tool_batch::restore_paused_calls(&run, pending_calls)?
            } else {
                pending_calls
            };
            let permissions = if let Some(batch) = approved {
                batch.calls
            } else {
                self_
                    .authorize_tools(
                        execution.as_ref(),
                        &session,
                        run_id,
                        &plan,
                        &pending_calls,
                        &run.usage,
                        resume_turns_used,
                        resumed_turn.clone(),
                        lost.as_ref(),
                    )
                    .await?
            };
            if permissions.len() != pending_calls.len() {
                return Err(crabber_session::ApprovalError::SemanticConflict.into());
            }
            self_
                .execute_prepared_tools(
                    tool_batch::Batch {
                        execution: execution.as_ref(),
                        session: &session,
                        run: run_id,
                        plan: &plan,
                        turn: resumed_turn.clone(),
                        cancellation: &work_cancellation,
                        lease_lost: lost.as_ref(),
                    },
                    pending_calls,
                    permissions,
                )
                .await?;
            if was_approval {
                if let Some(turn) = &approval_turn {
                    self_
                        .publish_event(
                            execution.as_ref(),
                            &plan,
                            self_.turn_event(
                                &run.session_id,
                                run_id,
                                turn,
                                EventKind::TurnCompleted,
                            ),
                        )
                        .await?;
                }
                let usage = self_
                    .run_loop(
                        execution.as_ref(),
                        run_id,
                        &session,
                        &original,
                        &plan,
                        &bound,
                        &work_cancellation,
                        false,
                        approval_usage.unwrap_or_else(|| run.usage.clone()),
                        resume_turns_used,
                        lost.as_ref(),
                        &work_turn,
                    )
                    .await?;
                plan.release();
                return Ok(RunResult {
                    session_id: run.session_id.clone(),
                    run_id: run_id.clone(),
                    status: RunStatus::Completed,
                    usage,
                });
            }
            if let Some(checkpoint) = &run.checkpoint
                && let Some(request) = saved
            {
                let usage: Usage = checkpoint
                    .get("usage")
                    .cloned()
                    .and_then(|value| serde_json::from_value(value).ok())
                    .unwrap_or_default();
                if committed_turn == Some(false) {
                    match self_
                        .conclude_owned_attempt(
                            execution.as_ref(),
                            run_id,
                            crabber_session::RunBoundary::Terminal {
                                status: RunStatus::Completed,
                                error: None,
                                usage: usage.clone(),
                            },
                            None,
                            run.created_at,
                        )
                        .await
                    {
                        Ok(event) => {
                            self.observer.emit(&event);
                            plan.release();
                            return Ok(RunResult {
                                session_id: run.session_id,
                                run_id: run_id.clone(),
                                status: RunStatus::Completed,
                                usage,
                            });
                        }
                        Err(RuntimeError::Attempt(crabber_session::AttemptError::PendingInput)) => {
                            self.claim_input(execution.as_ref(), InboxKind::Steer)
                                .await?;
                            self.claim_input(execution.as_ref(), InboxKind::FollowUp)
                                .await?;
                        }
                        Err(error) => return Err(error),
                    }
                }
                if let Some(turn) = checkpoint.get("source_turn_id").and_then(Value::as_str) {
                    self_
                        .publish_event(
                            execution.as_ref(),
                            &plan,
                            self_.turn_event(
                                &run.session_id,
                                run_id,
                                &TurnId(turn.to_owned()),
                                EventKind::TurnCompleted,
                            ),
                        )
                        .await?;
                } else {
                    self.emit_durable(
                        execution.as_ref(),
                        &run.session_id,
                        run_id,
                        &plan,
                        EventKind::TurnCompleted,
                    )
                    .await?;
                }
                let outcome = self_
                    .run_loop(
                        execution.as_ref(),
                        run_id,
                        &session,
                        &request,
                        &plan,
                        &bound,
                        &work_cancellation,
                        false,
                        usage,
                        resume_turns_used,
                        lost.as_ref(),
                        &work_turn,
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
                    Err(error) => Err(error),
                };
                plan.release();
                return result;
            }
            let status = RunStatus::Completed;
            let event = self_
                .conclude_owned_attempt(
                    execution.as_ref(),
                    run_id,
                    crabber_session::RunBoundary::Terminal {
                        status,
                        error: None,
                        usage: run.usage.clone(),
                    },
                    None,
                    run.created_at,
                )
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
        let result = match result {
            Ok(result) => Ok(result),
            Err(error) => {
                let turn = current_turn.lock().expect("turn context poisoned").clone();
                self_
                    .finish_failed_run(
                        terminal::FailureContext {
                            execution: failure_execution.as_ref(),
                            fence: &fence,
                            session: &failure_session,
                            plan: &failure_plan,
                            started_at: failure_started_at,
                            lost: failure_lost.as_ref(),
                            observed_lost: measurement_lost.as_ref(),
                        },
                        error,
                        turn.as_ref(),
                    )
                    .await
            }
        };
        failure_plan.release();
        if result.is_err() && self.verified_lease_loss(&fence).await {
            measurement_lost.store(true, Ordering::SeqCst);
        }
        measurement.observation.reason = result_reason(&result);
        result
    }

    /// Reclaims expired unfinished runs. Live leases are left to their owners.
    /// The report also lists each expired run left unfinished, and why.
    ///
    /// # Errors
    ///
    /// Returns a store or execution error for a run it successfully claims.
    pub async fn recover(&self) -> Result<RecoverReport, RuntimeError> {
        self.recover_with_context(|_| None).await
    }

    /// Selects fresh correlation independently for each eligible run.
    /// The selector cannot grant a lease; live leases and competing claims stay fenced.
    /// Only an expired Unstarted reservation, a pristine bound admission or a
    /// fully quiescent suspended attempt can enter the driver. Started, unknown
    /// and incomplete attempts stay unfinished and are reported for reconciliation.
    /// # Errors
    /// Returns the same store and execution errors as `recover`.
    pub async fn recover_with_context<F>(
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
            let (session_id, run_id) = (run.session_id.clone(), run.id.clone());
            let context = context_for(&run);
            // A run whose session is missing, undecodable or drifted, or that is
            // claimed elsewhere or awaits admission recovery, stays unfinished
            // and is reported; it must not stop the sweep.
            let outcome = self.resume_with_context(&run_id, context).await;
            match outcome {
                Ok(result) => report.recovered.push(result),
                Err(RuntimeError::Store(
                    reason @ (StoreError::NotFound
                    | StoreError::Validation(_)
                    | StoreError::SessionIdentityMismatch
                    | StoreError::Conflict
                    | StoreError::AdmissionRecoveryRequired),
                )) => {
                    report.skipped.push(SkippedRun {
                        session_id,
                        run_id,
                        reason: RecoverySkipReason::Store(reason),
                    });
                }
                Err(RuntimeError::Attempt(
                    reason @ (crabber_session::AttemptError::Busy
                    | crabber_session::AttemptError::LiveOwner
                    | crabber_session::AttemptError::CleanupBlocked
                    | crabber_session::AttemptError::InvalidTransition
                    | crabber_session::AttemptError::Unsupported
                    | crabber_session::AttemptError::StaleOwner),
                )) => report.skipped.push(SkippedRun {
                    session_id,
                    run_id,
                    reason: RecoverySkipReason::Attempt(reason),
                }),
                Err(
                    RuntimeError::ProviderBinding(
                        crabber_providers::ProviderBindingError::Unavailable
                        | crabber_providers::ProviderBindingError::VersionConflict,
                    )
                    | RuntimeError::AdmissionExecution(
                        AdmissionExecutionError::MissingEvidence
                        | AdmissionExecutionError::Unsupported,
                    ),
                ) => {
                    report.skipped.push(SkippedRun {
                        session_id,
                        run_id,
                        reason: RecoverySkipReason::Store(StoreError::HostUnsupported),
                    });
                }
                Err(error) => return Err(error),
            }
        }
        Ok(report)
    }

    /// Fixed interruption settlement deliberately runs no pre-stage or result chain;
    /// unfinished calls carry no tool-authored payload into persistence.
    async fn settle_unfinished_calls(
        &self,
        execution: &dyn ExecutionStore,
        session_id: &SessionId,
        run_id: &RunId,
        plan: &RunPlan,
        include_pending: bool,
        turn_id: Option<&TurnId>,
    ) -> Result<(), RuntimeError> {
        // execute_tool may already have settled a redacted Interrupted result.
        // Listing only unfinished calls keeps both live and resumed cleanup single-write.
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

    /// Writes only the bare runtime-authored interrupted text, with no pre-stage,
    /// transform or final redactor. Fixed settlements notify `EventPublished` only.
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
            text: INTERRUPTED_RESULT_TEXT.into(),
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
        Self::notify_interrupted_settlement(plan, &event, false);
        Ok(())
    }

    fn notify_interrupted_settlement(plan: &RunPlan, event: &EventRecord, protected: bool) {
        // Notification callbacks cannot delay sibling settlement. Keep the plan lease
        // until delivery finishes so mount shutdown still waits for these callbacks.
        let plan = plan.clone();
        let state_sink = crabber_extension::current_state_sink();
        let projection = serde_json::to_value(event).unwrap_or(Value::Null);
        tokio::spawn(async move {
            let delivery = async {
                plan.dispatcher
                    .notify::<EventPublished>(projection.clone())
                    .await;
                if protected {
                    plan.dispatcher
                        .notify::<crabber_extension::ToolSettled>(projection)
                        .await;
                }
            };
            // Preserve state reads and best-effort writes; the original sink still
            // rejects writes after terminal settlement through its execution fence.
            if let Some(sink) = state_sink {
                crabber_extension::with_state_sink(sink, delivery).await;
            } else {
                delivery.await;
            }
            plan.release();
        });
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
    /// Once polled, preparation/admission remain owned if this waiter is dropped.
    /// Returns the same admission errors as `start`.
    pub async fn start_with_context(
        &self,
        request: Request,
        context: Option<TraceContext>,
    ) -> Result<RunHandle, RuntimeError> {
        let runtime = self.clone();
        self.own_preparation(async move { runtime.start_admission(request, context).await })
            .await
    }

    async fn start_admission(
        &self,
        request: Request,
        context: Option<TraceContext>,
    ) -> Result<RunHandle, RuntimeError> {
        let runtime = self.with_context(context);
        let request = runtime.resolve_session_defaults(request).await?;
        let (plan, admission, _) = runtime.prepare_admission(&request).await?;
        let admitted = self
            .store
            .admit_bound_run(admission)
            .await
            .map_err(admission_error)?;
        Ok(runtime.spawn_admitted(
            request,
            plan,
            admitted.fence,
            &admitted.session,
            InitialEntry::Unkeyed,
            None,
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
    /// Once polled, preparation/admission remain owned if this waiter is dropped.
    /// Returns the same admission errors as `start_keyed`.
    pub async fn start_keyed_with_context(
        &self,
        request: Request,
        options: AdmissionOptions,
        context: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        let runtime = self.clone();
        self.own_preparation(async move {
            runtime
                .start_keyed_admission(request, options, context)
                .await
        })
        .await
    }

    async fn start_keyed_admission(
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
        if let Some(receipt) = self.replay_admission(&request, &options).await? {
            return Ok(Admission::Replayed(receipt));
        }
        let request = self.resolve_session_defaults(request).await?;
        let (plan, keyed) = self
            .prepare_keyed_admission(&request, options.clone())
            .await?;
        let outcome = self.store.admit_bound_keyed_run(keyed).await;
        let outcome = match outcome {
            Err(StoreError::AdmissionConflict)
                if request.input_envelope.is_none() || !request.selection_policy.is_explicit() =>
            {
                plan.release();
                let original = self
                    .retained_admission(&request, &options)
                    .await?
                    .ok_or(StoreError::AdmissionConflict)?;
                return Ok(Admission::Replayed(original.receipt));
            }
            result => result.map_err(admission_error)?,
        };
        match outcome {
            KeyedAdmitOutcome::Started { receipt, admitted } => Ok(Admission::Started {
                receipt,
                handle: self.with_context(context).spawn_admitted(
                    request,
                    plan,
                    admitted.fence,
                    &admitted.session,
                    InitialEntry::Keyed,
                    None,
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
    /// Once polled, preparation/claim remain owned if this waiter is dropped.
    /// Returns the same failures as `recover_admission`.
    pub async fn recover_admission_with_context(
        &self,
        request: Request,
        options: AdmissionOptions,
        context: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        let runtime = self.clone();
        self.own_preparation(async move {
            runtime
                .recover_retained_admission(request, options, context)
                .await
        })
        .await
    }

    async fn recover_retained_admission(
        &self,
        request: Request,
        options: AdmissionOptions,
        context: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        let retained = self
            .retained_admission(&request, &options)
            .await?
            .ok_or(StoreError::NotFound)?;
        if let Some(receipt) = self.recovery_replay(&retained)? {
            return Ok(Admission::Replayed(receipt));
        }
        let admission::Retained {
            session: persisted,
            receipt,
            record,
            run: _,
            binding: original_binding,
        } = retained;
        let session = request.session_id.as_ref().ok_or(StoreError::NotFound)?;
        let mut captured = Self::original_default_request(&request, &original_binding)?;
        captured
            .input_envelope
            .clone_from(&original_binding.input_envelope);
        let (plan, keyed) = self.prepare_keyed_admission(&captured, options).await?;
        if keyed.binding != original_binding {
            return Err(StoreError::AdmissionConflict.into());
        }
        let keyed = keyed.keyed;
        let capsule = keyed
            .execution
            .as_ref()
            .ok_or(AdmissionExecutionError::MissingEvidence)?;
        // Check actual restored instance semantics against the retained capsule
        // before claiming ownership; metadata already checked frozen declarations.
        record.capsule.validate(&keyed)?;
        if receipt.semantic_digest_version != 2
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
        let bound = self
            .restore_provider(&plan, &original_binding.provider)
            .await?;
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
                Request::from_execution(&claimed.record.capsule),
                plan,
                claimed.fence,
                &persisted,
                InitialEntry::Keyed,
                Some(bound),
            ),
        })
    }

    pub(crate) async fn prepare_keyed_admission(
        &self,
        request: &Request,
        options: AdmissionOptions,
    ) -> Result<(RunPlan, crabber_session::BoundKeyedAdmitRequest), RuntimeError> {
        if options.behavior_fingerprint != self.behavior_fingerprint {
            return Err(StoreError::AdmissionConflict.into());
        }
        let (plan, bound, runtime_semantics) = self.prepare_admission(request).await?;
        let admission = bound.request;
        let mut keyed = KeyedAdmitRequest {
            request: admission,
            options,
            execution: None,
        };
        let capsule = AdmissionExecutionCapsule {
            input_envelope: keyed.request.input_envelope.clone(),
            version: 2,
            request: request.admission_data()?,
            runtime_semantics,
            config_hash: keyed.request.config_hash.clone(),
            plan_fingerprint: keyed.request.plan_fingerprint.clone(),
            fingerprint: keyed.options.fingerprint.clone(),
            behavior_fingerprint: keyed.options.behavior_fingerprint.clone(),
            semantic_digest: keyed.options.fingerprint.clone(),
        };
        keyed.execution = Some(capsule);
        let semantic_digest = keyed.semantic_digest()?;
        keyed
            .execution
            .as_mut()
            .ok_or(AdmissionExecutionError::MissingEvidence)?
            .semantic_digest = semantic_digest;
        keyed
            .execution
            .as_ref()
            .ok_or(AdmissionExecutionError::MissingEvidence)?
            .validate(&keyed)?;
        Ok((
            plan,
            crabber_session::BoundKeyedAdmitRequest {
                keyed,
                binding: bound.binding,
            },
        ))
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

    /// Check routing identity before acquiring any contextual plan. Admission
    /// repeats the check in its transaction to close concurrent-create races.
    /// # Errors
    /// Returns identity mismatch or store validation/availability errors.
    pub async fn validate_context(&self, context: &SessionContext) -> Result<(), RuntimeError> {
        crabber_session::ensure_storable_text(&context.session_id().0)?;
        crabber_session::ensure_storable_text(context.workspace_id())?;
        crabber_session::ensure_storable_text(context.directory())?;
        if let Some(session) = self.store.get_session(context.session_id()).await? {
            session.ensure_identity(context.workspace_id(), context.directory())?;
        }
        Ok(())
    }

    pub(crate) async fn prepare_admission(
        &self,
        request: &Request,
    ) -> Result<(RunPlan, crabber_session::BoundAdmitRequest, Value), RuntimeError> {
        if !self.store.supports_owned_attempts()
            || !self.store.supports_owned_tool_phases()
            || !self.store.supports_pre_effect_approvals()
        {
            return Err(StoreError::HostUnsupported.into());
        }
        if request.max_output_tokens == Some(0) {
            return Err(RuntimeError::InvalidConfiguration(
                "max output tokens must be positive",
            ));
        }
        let now = self.clock.now();
        let session_id = request.session_id.clone().unwrap_or_default();
        let context = SessionContext::new(
            session_id.clone(),
            request.workspace_id.clone(),
            request.directory.clone(),
        );
        self.validate_context(&context).await?;
        let input = self.capture_input(request, &session_id).await?;
        let mut messages = input.materialize_messages(&session_id, now)?;
        let first_message = messages.remove(0);
        let plan = self.acquire_execution_plan(&context).await?;
        let plan = Self::bind_selection_policy(plan, &request.selection_policy)?;
        let provider = self.describe_provider(&plan, &request.selection)?;
        let plan = Self::bind_plan_provider(plan, &provider, &self.behavior_fingerprint)?;
        // Hash structured, independently observed semantics, never a caller's claim.
        let mut config = json!([
            "crabber.runtime.admission.v1",
            request.selection.provider_id,
            request.selection.model_id,
            request.system_prompt,
            request.max_output_tokens,
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
            input_envelope: Some(input.clone()),
            additional_messages: messages,
            expected_revision: Some(input.expected_revision()),
            session_id: request.session_id.clone(),
            workspace_id: request.workspace_id.clone(),
            directory: request.directory.clone(),
            title: request.title.clone(),
            user_message: first_message,
            config_hash,
            plan_fingerprint: plan.fingerprint().to_string(),
            owner: RunId::new().to_string(),
            lease: Duration::from_secs(30),
        };
        let mut stable = request.clone();
        stable.session_id = Some(session_id);
        let mut binding = crabber_session::ExecutionBindingCapsule {
            input_fingerprint: None,
            input_envelope: Some(input),
            version: 3,
            provider,
            request: stable.admission_data()?,
            runtime_semantics: config.clone(),
            config_hash: crabber_core::InputFingerprint::new(admission.config_hash.clone())?,
            plan_fingerprint: crabber_core::InputFingerprint::new(
                admission.plan_fingerprint.clone(),
            )?,
            behavior_fingerprint: self.behavior_fingerprint.clone(),
        };
        binding.input_fingerprint = binding.computed_input_fingerprint()?;
        binding.validate(&admission)?;
        Ok((
            plan,
            crabber_session::BoundAdmitRequest {
                request: admission,
                binding,
            },
            config,
        ))
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
        bound: Option<crabber_providers::BoundProvider>,
    ) -> RunHandle {
        let work_session = RunSession::from(session);
        self.spawn_worker(
            session.id.clone(),
            fence.run_id.clone(),
            move |runtime, cancellation| async move {
                Box::pin(runtime.run(
                    fence,
                    work_session,
                    InitialRequest {
                        request,
                        entry,
                        bound,
                    },
                    plan,
                    cancellation,
                ))
                .await
            },
        )
    }

    fn spawn_worker<F, W>(&self, session_id: SessionId, run_id: RunId, work: F) -> RunHandle
    where
        F: FnOnce(Self, CancellationToken) -> W + Send + 'static,
        W: std::future::Future<Output = Result<RunResult, RuntimeError>> + Send + 'static,
    {
        let (sender, done) = oneshot::channel();
        let (completion_sender, completion) = watch::channel(false);
        let cancellation = CancellationToken::new();
        let mut runtime = self.clone();
        let (event_observer, events) = crate::execution_events::ExecutionEventObserver::channel(
            runtime.observer.clone(),
            session_id.clone(),
            run_id.clone(),
            completion.clone(),
        );
        runtime.observer = event_observer.clone();
        let cancellation_for_work = cancellation.clone();
        let task = self.execution_tasks.spawn(run_id.clone(), async move {
            let guard = event_observer.guard();
            let result = Box::pin(work(runtime, cancellation_for_work)).await;
            drop(guard);
            let _ = sender.send(result);
            let _ = completion_sender.send(true);
            // Keep the event sender alive until completion is published, even
            // when no native tool retained its observer/context.
            drop(event_observer);
        });
        RunHandle {
            session_id,
            run_id,
            store: Arc::clone(&self.store),
            clock: Arc::clone(&self.clock),
            done,
            task,
            tasks: self.execution_tasks.clone(),
            completion,
            cancellation,
            events: Some(events),
        }
    }

    async fn run(
        &self,
        fence: RunFence,
        session: RunSession,
        initial: InitialRequest,
        plan: RunPlan,
        cancellation: CancellationToken,
    ) -> Result<RunResult, RuntimeError> {
        let lost = AtomicBool::new(false);
        let mut measurement = Measurement::new(
            self.observer.as_ref(),
            self.monotonic_clock.as_ref(),
            &session.id,
            &fence.run_id,
            OperationKind::Run,
            &cancellation,
            &lost,
        );
        let result = Box::pin(self.run_inner(
            fence.clone(),
            session.clone(),
            initial,
            plan,
            cancellation.clone(),
            &lost,
        ))
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
        session: RunSession,
        initial: InitialRequest,
        plan: RunPlan,
        cancellation: CancellationToken,
        observed_lost: &AtomicBool,
    ) -> Result<RunResult, RuntimeError> {
        let session_id = session.id.clone();
        let InitialRequest {
            request,
            entry,
            bound: prebound,
        } = initial;
        let binding = self.stored_binding(&session_id, &fence.run_id).await?;
        self.verify_binding_request(&request, &session_id, &binding)?;
        let mut request = request;
        request.input_envelope.clone_from(&binding.input_envelope);
        if plan.fingerprint().to_string() != binding.plan_fingerprint.as_str() {
            return Err(RuntimeError::PlanChanged);
        }
        let execution: Arc<dyn ExecutionStore> = self.store.execution(fence.clone()).await?.into();
        if matches!(entry, InitialEntry::Keyed) {
            execution.begin_admission_execution().await?;
        }
        let mut worker = self.clone();
        worker.owned_attempt = Some(Arc::new(
            self.begin_owned_attempt(execution.as_ref(), &session_id, &fence.run_id, false)
                .await?,
        ));
        let self_ = &worker;
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
            let run_future = crabber_extension::with_state_sink(state_sink, async {
                let bound = if let Some(bound) = prebound {
                    if bound.descriptor() != &binding.provider {
                        return Err(crabber_providers::ProviderBindingError::VersionConflict.into());
                    }
                    bound
                } else {
                    self.restore_provider(&plan, &binding.provider).await?
                };
                self_
                    .run_loop(
                        execution.as_ref(),
                        &run_id,
                        &session,
                        &request,
                        &plan,
                        &bound,
                        &cancellation,
                        true,
                        Usage::default(),
                        0,
                        lost.as_ref(),
                        &current_turn,
                    )
                    .await
            });
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
                let turn = current_turn.lock().expect("turn context poisoned").clone();
                self_
                    .finish_failed_run(
                        terminal::FailureContext {
                            execution: execution.as_ref(),
                            fence: &fence,
                            session: &session_id,
                            plan: &plan,
                            started_at: run_started_at,
                            lost: lost.as_ref(),
                            observed_lost,
                        },
                        error,
                        turn.as_ref(),
                    )
                    .await
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
        session: &RunSession,
        request: &Request,
        plan: &RunPlan,
        bound: &crabber_providers::BoundProvider,
        cancellation: &CancellationToken,
        initial: bool,
        mut usage: Usage,
        mut turns_used: usize,
        lease_lost: &AtomicBool,
        current_turn: &std::sync::Mutex<Option<TurnId>>,
    ) -> Result<Usage, RuntimeError> {
        if cancellation.is_cancelled() {
            return Err(RuntimeError::Interrupted);
        }
        let session_id = &session.id;
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
        while turns_used < self.max_turns {
            turns_used += 1;
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
            let mut snapshot = self.snapshot(run_id, session, request, plan).await?;
            snapshot.identity.turn_id = turn_id.clone();
            let mut compacted = false;
            if let Some(context_limit) = bound.descriptor().limits().context_tokens {
                let history_bytes =
                    serde_json::to_vec(&snapshot.messages).map_or(0, |bytes| bytes.len());
                let context_bytes = if snapshot.context.is_empty() {
                    0
                } else {
                    serde_json::to_vec(&snapshot.context).map_or(0, |bytes| bytes.len())
                };
                let estimated = history_bytes.saturating_add(context_bytes) / 4;
                #[allow(clippy::cast_precision_loss)]
                if snapshot.messages.len() > self.compaction.keep_tail_messages
                    && context_limit > 0
                    && (estimated as f64) >= (context_limit as f64 * self.compaction.trigger_ratio)
                {
                    self.compact(
                        execution,
                        run_id,
                        session_id,
                        &snapshot,
                        plan,
                        bound,
                        false,
                        cancellation,
                        lease_lost,
                    )
                    .await?;
                    snapshot.messages = self.store.list_messages(session_id, None).await?;
                    compacted = true;
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
                &session.workspace,
                bound,
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
            let permissions = self
                .authorize_tools(
                    execution,
                    session,
                    run_id,
                    plan,
                    &staged,
                    &usage,
                    turns_used,
                    Some(turn_id.clone()),
                    lease_lost,
                )
                .await?;
            if staged.iter().any(|call| {
                plan.tools
                    .iter()
                    .find(|tool| tool.info.name == call.name)
                    .is_some_and(|tool| {
                        self.policy.interrupt_policy(&tool.info, &call.arguments)
                            == InterruptPolicy::Pause
                    })
            }) {
                let event = self.conclude_owned_attempt(execution, run_id,
                    crabber_session::RunBoundary::Suspended {
                        checkpoint: json!({"version":4,"turns_used":turns_used,"source_turn_id":turn_id,"pending_calls": staged.iter().map(|call| call.id.to_string()).collect::<Vec<_>>(),
                            "request": CheckpointRequest::new(request), "usage": usage}),
                        usage: usage.clone(),
                    }, Some(turn_id.clone()), run_started_at).await?;
                self.observer.emit(&event);
                return Err(RuntimeError::Paused);
            }
            self.execute_prepared_tools(
                tool_batch::Batch {
                    execution,
                    session,
                    run: run_id,
                    plan,
                    turn: Some(turn_id.clone()),
                    cancellation,
                    lease_lost,
                },
                staged,
                permissions,
            )
            .await?;
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
                match self
                    .conclude_owned_attempt(
                        execution,
                        run_id,
                        crabber_session::RunBoundary::Terminal {
                            status: RunStatus::Completed,
                            error: None,
                            usage: usage.clone(),
                        },
                        None,
                        run_started_at,
                    )
                    .await
                {
                    Ok(settled) => {
                        self.observer.emit(&settled);
                        let projection = serde_json::to_value(&settled).unwrap_or(Value::Null);
                        plan.dispatcher
                            .notify::<EventPublished>(projection.clone())
                            .await;
                        plan.dispatcher.notify::<RunSettled>(projection).await;
                        return Ok(usage);
                    }
                    Err(RuntimeError::Attempt(crabber_session::AttemptError::PendingInput)) => {
                        if self.claim_input(execution, InboxKind::Steer).await?
                            + self.claim_input(execution, InboxKind::FollowUp).await?
                            == 0
                        {
                            return Err(RuntimeError::Store(StoreError::PendingInput));
                        }
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        Err(RuntimeError::TurnLimit)
    }

    async fn snapshot(
        &self,
        run_id: &RunId,
        session: &RunSession,
        request: &Request,
        plan: &RunPlan,
    ) -> Result<TurnSnapshot, RuntimeError> {
        let session_id = &session.id;
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
        let workspace = &session.workspace;
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
            context: request
                .input_envelope
                .as_ref()
                .map_or_else(Vec::new, |input| input.context().to_vec()),
            identity: RequestIdentity {
                session_id: session_id.clone(),
                run_id: run_id.clone(),
                turn_id: TurnId::new(),
            },
            selection: request.selection.clone(),
            messages,
            system: (!system.is_empty()).then_some(system),
            max_output_tokens: request.max_output_tokens,
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
        workspace: &WorkspaceContext,
        bound: &crabber_providers::BoundProvider,
        mut compacted: bool,
        cancellation: &CancellationToken,
        lease_lost: &AtomicBool,
    ) -> Result<(Vec<PendingCall>, Usage), RuntimeError> {
        let streamer = bound.streamer();
        let mut snapshot = snapshot.clone();
        let mut retries = 0u32;
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let system = Self::attempt_system(
                plan,
                &snapshot,
                workspace,
                attempt,
                compacted,
                cancellation,
                self.workspace_reader_resolver.clone(),
            )
            .await?;
            if !plan.prompt_contributors.is_empty() {
                ensure_lease(lease_lost)?;
            }
            match self
                .model_turn(
                    execution,
                    run_id,
                    session_id,
                    &snapshot,
                    system,
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
                            bound,
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
                        tokio::select! {
                            biased;
                            () = cancellation.cancelled() => return Err(RuntimeError::Interrupted),
                            () = tokio::time::sleep(Duration::from_millis(decision.delay_ms)) => (),
                        }
                        continue;
                    }
                    return Err(RuntimeError::Provider(error));
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn attempt_system(
        plan: &RunPlan,
        snapshot: &TurnSnapshot,
        workspace: &WorkspaceContext,
        attempt: u32,
        compacted: bool,
        cancellation: &CancellationToken,
        workspace_reader_resolver: Option<Arc<dyn WorkspaceReaderResolver>>,
    ) -> Result<Option<String>, RuntimeError> {
        if plan.prompt_contributors.is_empty() {
            return Ok(snapshot.system.clone());
        }
        let context = PromptAttemptContext::new(
            snapshot.identity.session_id.clone(),
            snapshot.identity.run_id.clone(),
            snapshot.identity.turn_id.clone(),
            workspace.clone(),
            snapshot.selection.provider_id.clone(),
            snapshot.selection.model_id.clone(),
            attempt,
            compacted,
        )
        .with_cancellation(cancellation.clone());
        let workspace_reader_resolver = workspace_reader_resolver.map(|host| {
            Arc::new(AttemptWorkspaceReaderResolver {
                admitted: workspace.clone(),
                host,
            }) as Arc<dyn WorkspaceReaderResolver>
        });
        match collect_prompt_contributions_with_resolver(
            &plan.prompt_contributors,
            context,
            workspace_reader_resolver,
        )
        .await
        {
            PromptContributionOutcome::Completed { sections } => {
                let mut system = snapshot.system.clone().unwrap_or_default();
                for section in sections {
                    if !system.is_empty() {
                        system.push('\n');
                    }
                    system.push_str(&section.text);
                }
                Ok((!system.is_empty()).then_some(system))
            }
            PromptContributionOutcome::Failed { contributor } => Err(RuntimeError::Extension(
                prompt_contribution_failed_message(&contributor),
            )),
            PromptContributionOutcome::Interrupted => Err(RuntimeError::Interrupted),
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
        bound: &crabber_providers::BoundProvider,
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
        let streamer = bound.streamer();
        let limits = bound.descriptor().limits();
        let context_limit = limits.context_tokens.unwrap_or(0);
        let summary_output_limit = snapshot
            .max_output_tokens
            .unwrap_or(1024)
            .min(limits.output_tokens.unwrap_or(1024))
            .min(1024);
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
                context: snapshot.context.clone(),
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
                max_tokens: Some(summary_output_limit),
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
            // Compaction is an internal model request with a dedicated prompt. System-prompt
            // middleware is intentionally scoped to agent-turn attempts and is not collected
            // here; the next agent-turn attempt refreshes middleware after compaction.
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
        system: Option<String>,
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
            context: snapshot.context.clone(),
            identity: snapshot.identity.clone(),
            selection: snapshot.selection.clone(),
            system,
            messages: snapshot.messages.clone(),
            tools: snapshot.tools.clone(),
            temperature: None,
            max_tokens: snapshot.max_output_tokens,
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
            dispatched = plan.dispatcher.around::<ExtensionModelStream>(json!({"provider":snapshot.selection.provider_id,"model":snapshot.selection.model_id,"session_id":snapshot.identity.session_id,"run_id":snapshot.identity.run_id,"turn_id":snapshot.identity.turn_id,"message_count":snapshot.messages.len(),"temperature":null,"max_tokens":snapshot.max_output_tokens,"tool_choice":null}),terminal) => dispatched,
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
                        call.arguments = Some(match serde_json::from_str(&call.raw) {
                            Ok(parsed) => storable_call_arguments(parsed, &call.raw),
                            Err(_) => Value::String(call.raw.clone()),
                        });
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
        let arguments = if let Some(tool) = &definition {
            let prepared = match validate_arguments(&tool.info, &raw) {
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
            };
            match prepared {
                Ok(value) if is_reserved_arguments(&value) => {
                    json!({PREPARE_ERROR_KEY: RESERVED_ARGUMENT_KEY_TEXT})
                }
                Ok(value) => value,
                Err(error) => json!({PREPARE_ERROR_KEY: error}),
            }
        } else {
            unknown_tool_arguments(&raw)
        };
        ensure_lease(lease_lost)?;
        let mut event = self.event(session_id, run_id, EventKind::ToolCallPending);
        event.turn_id = call.turn_id.clone();
        event.payload = json!({"call_id":call.id,"name":call.name,"status":"pending", "message_id":call.message_id});
        event.correlation = Some(call.id.to_string());
        let mut record = ToolCallRecord {
            id: call.id,
            run_id: run_id.clone(),
            name: call.name,
            arguments,
            status: ToolCallStatus::Pending,
            retry_safe: definition.as_ref().is_some_and(|tool| tool.info.retry_safe),
            result: None,
        };
        if crabber_session::ensure_storable(&record).is_err() {
            record.arguments = json!({PREPARE_ERROR_KEY: UNSTORABLE_ARGUMENTS_TEXT});
        }
        execution
            .create_tool_call(record.clone(), event.clone())
            .await?;
        self.observer.emit(&event);
        plan.dispatcher
            .notify::<EventPublished>(serde_json::to_value(&event).unwrap_or(Value::Null))
            .await;
        Ok(record)
    }
}

fn runtime_error_output(error: &str) -> crabber_core::ToolOutput {
    crabber_core::ToolOutput::from_json(&Value::String(error.into())).unwrap_or_else(|_| {
        crabber_core::ToolOutput::from_json(&Value::String("tool result unavailable".into()))
            .expect("fixed runtime error is valid")
    })
}

/// Test bound from cancellation to durable settlement; store writes have no timer.
pub const INTERRUPT_SETTLEMENT_BOUND: Duration = Duration::from_secs(1);

/// Fixed runtime-authored content for interruption without a protected result.
pub const INTERRUPTED_RESULT_TEXT: &str = "interrupted";

/// Handler id used for sanitized host pre-stage failures.
pub const TOOL_PIPELINE_HANDLER_ID: &str = "crabber/tool-pipeline";

const PREPARE_ERROR_KEY: &str = "$crabber_prepare_error";
const UNKNOWN_TOOL_KEY: &str = "$crabber_unknown_tool";
const RESERVED_KEY_PREFIX: &str = "$crabber_";
/// Recorded (and seeded) when prepared arguments would be read back as a runtime sentinel.
const RESERVED_ARGUMENT_KEY_TEXT: &str = "reserved argument key";
const UNSTORABLE_ARGUMENTS_TEXT: &str = "unstorable arguments";

fn storable_error_text(error: &RuntimeError) -> String {
    error.to_string().replace('\0', "\u{fffd}")
}

fn storable_call_arguments(parsed: Value, raw: &str) -> Value {
    let message_id = MessageId::new();
    let probe = Message {
        id: message_id.clone(),
        session_id: SessionId::new(),
        run_id: Some(RunId::new()),
        role: Role::Assistant,
        parent_id: None,
        parts: vec![Part {
            id: PartId::new(),
            message_id,
            ordinal: 0,
            kind: PartKind::FunctionToolCall,
            content: ContentBlock::ToolCall {
                call_id: ToolCallId::new(),
                name: String::new(),
                arguments: parsed.clone(),
            },
        }],
        created_at: OffsetDateTime::UNIX_EPOCH,
    };
    if crabber_session::ensure_storable(&probe).is_ok() {
        parsed
    } else {
        Value::String(raw.to_owned())
    }
}

/// The record arguments for an unknown tool. Stores write records without a depth limit but decode
/// them with `serde_json::from_str` (128 levels), and both the sentinel and the enclosing
/// `ToolCallRecord` add levels. When the whole record would not decode back unchanged, `raw` holds
/// the arguments' JSON text instead, as for unparseable provider text. That form is a fixed depth
/// (a string is a scalar), so it always decodes.
pub(crate) fn unknown_tool_arguments(raw: &Value) -> Value {
    let arguments = json!({UNKNOWN_TOOL_KEY: {"raw": raw}});
    let probe = ToolCallRecord {
        id: ToolCallId::new(),
        run_id: RunId::new(),
        name: String::new(),
        arguments: arguments.clone(),
        status: ToolCallStatus::Pending,
        retry_safe: false,
        result: None,
    };
    if crabber_session::ensure_storable(&probe).is_ok() {
        return arguments;
    }
    let text = serde_json::to_string(raw).unwrap_or_default();
    json!({UNKNOWN_TOOL_KEY: {"raw": text}})
}

/// Typed result of running a resolved call, so the class never depends on error text.
enum ToolRun {
    Output(TransformOutput),
    PipelineFailed,
    ProtectionInterrupted,
    PermissionDenied,
    ExecutionFailed(String),
}

/// What the stored `record.arguments` say about a call (Recorded answer 1).
#[derive(Debug, PartialEq)]
pub(crate) enum RecordedCall {
    /// A resolved call with real arguments: class comes from execution.
    Execute(Value),
    /// The call does not execute; class, input and the chain's seed text come from the record.
    Settled {
        class: ToolOutcomeClass,
        input: ToolInput,
        resolved: bool,
        seed: String,
    },
}

/// An object whose only key is reserved for the runtime.
fn sole_reserved_key(arguments: &Value) -> Option<(&str, &Value)> {
    let object = arguments.as_object().filter(|object| object.len() == 1)?;
    let (key, value) = object.iter().next()?;
    key.starts_with(RESERVED_KEY_PREFIX)
        .then_some((key.as_str(), value))
}

fn is_reserved_arguments(arguments: &Value) -> bool {
    sole_reserved_key(arguments).is_some()
}

pub(crate) fn read_recorded_call(arguments: Value, name: &str, resolves: bool) -> RecordedCall {
    let unknown_seed = || format!("unknown tool: {name}");
    let prepare_failed = |seed: String| RecordedCall::Settled {
        class: ToolOutcomeClass::PrepareFailed,
        input: ToolInput::Unavailable {
            reason: InputUnavailable::PrepareFailed,
        },
        resolved: true,
        seed,
    };
    match sole_reserved_key(&arguments) {
        Some((UNKNOWN_TOOL_KEY, value)) => RecordedCall::Settled {
            class: ToolOutcomeClass::UnknownTool,
            input: match value.as_object().and_then(|object| object.get("raw")) {
                Some(raw) => ToolInput::Raw(raw.clone()),
                None => ToolInput::Unavailable {
                    reason: InputUnavailable::Unresolved,
                },
            },
            resolved: false,
            seed: unknown_seed(),
        },
        Some((PREPARE_ERROR_KEY, value)) => prepare_failed(
            value
                .as_str()
                .unwrap_or(RESERVED_ARGUMENT_KEY_TEXT)
                .to_owned(),
        ),
        Some(_) => prepare_failed(RESERVED_ARGUMENT_KEY_TEXT.to_owned()),
        None if resolves => RecordedCall::Execute(arguments),
        None => RecordedCall::Settled {
            class: ToolOutcomeClass::UnknownTool,
            input: ToolInput::Unavailable {
                reason: InputUnavailable::Unresolved,
            },
            resolved: false,
            seed: unknown_seed(),
        },
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

#[cfg(test)]
mod recovery_contract_tests {
    use super::*;

    #[test]
    fn checkpoint_request_requires_present_positive_or_null_cap() {
        let mut request = json!({"workspace_id":"w","directory":".","title":"t","text":"hi","provider_id":"fake","model_id":"scripted","system_prompt":null});
        assert!(matches!(
            CheckpointRequest::from_checkpoint(Some(&json!({"request":request}))),
            Err(StoreError::Validation(_))
        ));
        for cap in [json!(0), Value::Null, json!(4096)] {
            request["max_output_tokens"] = cap.clone();
            let saved = CheckpointRequest::from_checkpoint(Some(&json!({"request":request})));
            if cap == 0 {
                assert!(matches!(saved, Err(StoreError::Validation(_))));
            } else {
                assert_eq!(
                    saved.unwrap().unwrap().max_output_tokens,
                    cap.as_u64().map(|v| u32::try_from(v).unwrap())
                );
            }
        }
        assert!(matches!(
            CheckpointRequest::from_checkpoint(Some(&json!({"request":"corrupt"}))),
            Err(StoreError::Validation(_))
        ));
    }

    use crate::result_transform_tests::{
        ClosureExtension, ECHO, Finished, Harness, ScriptedCall, one_turn,
    };
    use async_trait::async_trait;
    use std::sync::Mutex;

    struct Pause;
    impl PermissionPolicy for Pause {
        fn decide(&self, _: &ToolInfo, _: &Value) -> PermissionDecision {
            PermissionDecision::Allow
        }
        fn interrupt_policy(&self, _: &ToolInfo, _: &Value) -> InterruptPolicy {
            InterruptPolicy::Pause
        }
    }

    #[derive(Default)]
    struct PreStage(Mutex<Vec<ToolResultContext>>);
    #[async_trait]
    impl ToolPipeline for PreStage {
        async fn prepare(&self, _: &ToolInfo, input: Value) -> Result<Value, String> {
            Ok(input)
        }
        async fn transform_result(
            &self,
            context: &ToolResultContext,
            _: &ToolInfo,
            _: crabber_core::ToolOutput,
        ) -> Result<TransformOutput, String> {
            self.0.lock().unwrap().push(context.clone());
            Ok(TransformOutput::new(
                crabber_core::ToolOutput::from_json(&(json!("re-executed pre-stage"))).unwrap(),
            ))
        }
    }

    #[tokio::test]
    async fn fixed_unfinished_settlement_bypasses_pre_stage_and_result_chain() {
        let call = ScriptedCall::text(ECHO, "tool-authored input");
        let pipeline = Arc::new(PreStage::default());
        let harness = Harness::builder(one_turn(&[&call]))
            .policy(Arc::new(Pause))
            .tool_pipeline(pipeline.clone())
            .build()
            .await;
        let handle = harness.start().await;
        let session_id = handle.session_id().clone();
        let run_id = handle.run_id().clone();
        assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
        let prepares = harness.probe.prepares();
        let fence = harness
            .store
            .claim_expired_run(&run_id, "fixed-settlement-test")
            .await
            .unwrap();
        let execution = harness.store.execution(fence).await.unwrap();
        let runtime = harness.fresh_runtime();
        let plan = runtime
            .plan_provider
            .acquire_plan(&session_id)
            .await
            .unwrap();
        execution
            .claim_tool_call(
                &call.id,
                runtime.event(&session_id, &run_id, EventKind::ToolCallRunning),
            )
            .await
            .unwrap();
        runtime
            .settle_unfinished_calls(execution.as_ref(), &session_id, &run_id, &plan, false, None)
            .await
            .unwrap();
        let record = Finished {
            harness: &harness,
            session_id,
            run_id,
        }
        .record(&call.id)
        .await;
        let result = record.result.unwrap();
        assert_eq!(result.status, ToolResultStatus::Interrupted);
        assert_eq!(
            result.content,
            vec![ContentBlock::Text {
                text: INTERRUPTED_RESULT_TEXT.into()
            }]
        );
        assert_eq!(harness.probe.executed(), [] as [(String, Value); 0]);
        assert_eq!(harness.probe.prepares(), prepares);
        assert_eq!(pipeline.0.lock().unwrap().len(), 0);
        assert_eq!(harness.probe.results(), [] as [Value; 0]);
        plan.release();
    }

    #[tokio::test]
    async fn paused_pending_reexecutes_stored_identity_and_full_chain_without_prepare() {
        let call = ScriptedCall::text(ECHO, "durable input");
        let pipeline = Arc::new(PreStage::default());
        let final_contexts = Arc::new(Mutex::new(Vec::new()));
        let redactor = ClosureExtension::new("replay-final", {
            let final_contexts = final_contexts.clone();
            move |r| {
                let final_contexts = final_contexts.clone();
                r.on_final_redaction(
                    0,
                    "final",
                    Arc::new(move |context, value| {
                        final_contexts.lock().unwrap().push(context);
                        assert_eq!(
                            value,
                            crabber_core::ToolOutput::from_json(&(json!("re-executed pre-stage")))
                                .unwrap()
                        );
                        Box::pin(async {
                            Ok(TransformOutput::new(
                                crabber_core::ToolOutput::from_json(&(json!("redacted replay")))
                                    .unwrap(),
                            ))
                        })
                    }),
                );
            }
        });
        let harness = Harness::builder(one_turn(&[&call]))
            .mount(redactor, crabber_extension::Scope::Global)
            .policy(Arc::new(Pause))
            .tool_pipeline(pipeline.clone())
            .build()
            .await;
        let handle = harness.start().await;
        let session_id = handle.session_id().clone();
        let run_id = handle.run_id().clone();
        assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
        let prepares = harness.probe.prepares();
        assert_ne!(prepares, [] as [Value; 0]);
        assert_eq!(pipeline.0.lock().unwrap().len(), 0);
        assert_eq!(harness.probe.results(), [] as [Value; 0]);
        let stored = harness
            .store
            .list_unfinished_tool_calls(&run_id)
            .await
            .unwrap();
        assert_eq!(stored.len(), 1);
        let result = harness.fresh_runtime().resume(&run_id).await.unwrap();
        assert_eq!(result.status, RunStatus::Completed);
        assert_eq!(harness.probe.prepares(), prepares);
        assert_eq!(
            harness.probe.executed(),
            vec![(ECHO.into(), stored[0].arguments.clone())]
        );
        let contexts = pipeline.0.lock().unwrap().clone();
        assert_eq!(contexts.len(), 1);
        assert_eq!(contexts[0].call_id(), &stored[0].id);
        assert_eq!(contexts[0].session_id(), &session_id);
        assert_eq!(contexts[0].run_id(), &stored[0].run_id);
        assert_eq!(
            contexts[0].input(),
            &ToolInput::Normalized(stored[0].arguments.clone())
        );
        let results = harness.probe.results();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["result"], json!("re-executed pre-stage"));
        assert_eq!(results[0]["context"]["call_id"], stored[0].id.to_string());
        assert_eq!(results[0]["context"]["session_id"], session_id.to_string());
        assert_eq!(results[0]["context"]["run_id"], run_id.to_string());
        let finals = final_contexts.lock().unwrap().clone();
        assert_eq!(finals.len(), 1);
        assert_eq!(finals[0].call_id(), &stored[0].id);
        assert_eq!(finals[0].session_id(), &session_id);
        assert_eq!(finals[0].run_id(), &run_id);
        let record = Finished {
            harness: &harness,
            session_id,
            run_id,
        }
        .record(&call.id)
        .await;
        assert_eq!(
            record.result.unwrap().content,
            vec![ContentBlock::Text {
                text: "\"redacted replay\"".into()
            }]
        );
    }
}

mod session_defaults;
