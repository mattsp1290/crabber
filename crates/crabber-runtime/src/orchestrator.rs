mod builder;
mod entry;
mod events;
mod execution;
mod helpers;
mod model_support;
mod model_turn;
mod recovery;
mod telemetry;
pub(crate) use helpers::{RecordedCall, read_recorded_call, unknown_tool_arguments};
use helpers::{
    ToolRun, ensure_lease, invalid_provider, is_reserved_arguments, push_part, request_error,
    runtime_error_output, storable_call_arguments, storable_error_text, validate_arguments,
};
mod close_dependents;
mod driver;
mod heartbeat;
mod host_close;
mod model_callbacks;
mod notifications;
mod reasoning;
mod worker;
pub use close_dependents::RuntimeDependentCloseReport;
use heartbeat::HeartbeatGuard;
pub use host_close::{CloseObservation, RuntimeCloseObservation, RuntimeCloseReport};
mod host_callbacks;
#[path = "orchestrator/observation_lifetime.rs"]
mod observation_lifetime;
use observation_lifetime::ObservationLifetime;
#[path = "orchestrator/frontend.rs"]
mod frontend;
#[path = "orchestrator/frontend_control.rs"]
mod frontend_control;
#[path = "orchestrator/tool_recovery.rs"]
mod tool_recovery;
pub use frontend_control::{ContinueFrontend, RecoverFrontend};
pub use tool_recovery::RecoverTool;
#[path = "orchestrator/tool_continuation.rs"]
mod tool_continuation;
#[path = "orchestrator/tool_suspension.rs"]
mod tool_suspension;
pub use tool_continuation::{ContinueTool, ToolContinuation};
#[path = "orchestrator/admission.rs"]
mod admission;
#[path = "orchestrator/approval.rs"]
mod approval;
#[path = "orchestrator/approval_recovery.rs"]
mod approval_recovery;
pub use approval_recovery::RecoverApproval;
mod attempt_control;
#[path = "orchestrator/continuation.rs"]
mod continuation;
mod continuation_source;
mod management;
#[path = "orchestrator/queue.rs"]
mod queue;
mod queue_context;
mod run_control;
#[path = "orchestrator/session_owner.rs"]
mod session_owner;
pub use queue::{QueueAdmissionLease, QueueControl};
#[path = "orchestrator/phase_control.rs"]
mod phase_control;
mod public_session;
pub use continuation::{Continuation, ContinueApproval};
#[path = "orchestrator/attempt.rs"]
mod attempt;
#[path = "orchestrator/binding.rs"]
mod binding;
#[path = "orchestrator/input.rs"]
mod input;
#[path = "orchestrator/preparation.rs"]
mod preparation;
pub use preparation::{
    PreparationRetryProgress, RetainedPreparationProgress, RetainedPreparationStatus,
};
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
    pub model_controls: crabber_core::ModelControls,
}

#[derive(Clone)]
enum InitialEntry {
    Unkeyed,
    Keyed,
    RecoveredKeyed(Option<crabber_session::session_owner::driver::NativeClaimLocator>),
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
            model_controls: self.model_controls.clone(),
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
            model_controls: data.model_controls,
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
    #[error("original preparation must be reconciled before new work")]
    PreparationRecoveryRequired,
    #[error("admission already committed")]
    AdmissionAlreadyCommitted(
        crabber_session::session_owner::preparation::PreparationAdmissionReceipt,
    ),
    #[error("preparation requires explicit reconciliation: {source}")]
    PreparationIncomplete {
        preparation: Box<crabber_session::session_owner::preparation::PreparationReconciliation>,
        #[source]
        source: Box<RuntimeError>,
    },
    #[error("session ownership: {0}")]
    SessionOwner(#[from] crabber_session::session_owner::SessionOwnerError),
    #[error("durable run control: {0}")]
    RunControl(#[from] crabber_session::RunControlError),
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
    #[error("tool continuation: {0}")]
    ToolContinuation(#[from] crabber_session::ToolContinuationError),
    #[error("tool checkpoint boundary outcome is unknown")]
    ToolContinuationBoundaryUnknown {
        run_id: RunId,
        operation_id: crabber_core::ControlId,
        reason: crabber_session::ToolContinuationError,
    },
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
    /// The response exhausted its output limit before assistant history or tools were committed.
    #[error("model output limit reached")]
    OutputLimit { tool_call_count: usize },
    #[error("model response rejected: {0:?}")]
    ModelResponseRejected(crabber_core::ModelFinishReason),
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
    ///
    /// Unstable compatibility variant: the current native execution driver
    /// reports `AttemptBoundaryUnknown` for an unacknowledged attempt conclusion.
    /// This variant is retained but is not emitted by that driver.
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

impl RuntimeError {
    /// Stable diagnostic category that never includes error content.
    #[must_use]
    #[allow(clippy::too_many_lines)] // Exhaustive finite categories mirror the public error vocabulary.
    pub fn error_kind(&self) -> &'static str {
        match self {
            Self::PreparationRecoveryRequired => "preparation_recovery_required",
            Self::AdmissionAlreadyCommitted(..) => "admission_already_committed",
            Self::PreparationIncomplete { .. } => "preparation_incomplete",
            Self::SessionOwner(..) => "session_owner",
            Self::RunControl(..) => "run_control",
            Self::HostClosed => "host_closed",
            Self::SessionBusy => "session_busy",
            Self::Management(..) => "management",
            Self::Store(error) => match error.error_kind() {
                "validation" => "store_validation",
                "limit" => "store_limit",
                "conflict" => "store_conflict",
                "admission_conflict" => "store_admission_conflict",
                "admission_revision_conflict" => "store_admission_revision_conflict",
                "admission_unsupported" => "store_admission_unsupported",
                "admission_recovery_required" => "store_admission_recovery_required",
                "snapshot_unsupported" => "store_snapshot_unsupported",
                "host_unsupported" => "store_host_unsupported",
                "session_identity_mismatch" => "store_session_identity_mismatch",
                "session_deleted" => "store_session_deleted",
                "busy" => "store_busy",
                "not_found" => "store_not_found",
                "pending_input" => "store_pending_input",
                _ => "store",
            },
            Self::AdmissionExecution(..) => "admission_execution",
            Self::Attempt(..) => "attempt",
            Self::Approval(..) => "approval",
            Self::ToolContinuation(..) => "tool_continuation",
            Self::ToolContinuationBoundaryUnknown { .. } => "tool_continuation_boundary_unknown",
            Self::ApprovalBoundaryUnknown { .. } => "approval_boundary_unknown",
            Self::Provider(error) => telemetry::provider_error_kind(error),
            Self::ProviderBinding(..) => "provider_binding",
            Self::Extension(..) => "extension",
            Self::ExtensionPlan(error) => match error.error_kind() {
                "preparation_resource_contract_invalid" => {
                    "extension_preparation_resource_contract_invalid"
                }
                "preparation_resources_unsupported" => {
                    "extension_preparation_resources_unsupported"
                }
                "preparation_lease_already_bound" => "extension_preparation_lease_already_bound",
                "preparation_registration" => "extension_preparation_registration",
                "session_service_dependency" => "extension_session_service_dependency",
                "durable_interaction_unsupported" => "extension_durable_interaction_unsupported",
                "registration_descriptor_unavailable" => {
                    "extension_registration_descriptor_unavailable"
                }
                "plan" => "extension_plan",
                "tool" => "extension_tool",
                "rejected" => "extension_rejected",
                "tool_collision" => "extension_tool_collision",
                "prompt_contributor_collision" => "extension_prompt_contributor_collision",
                "next_not_called" => "extension_next_not_called",
                "next_called_twice" => "extension_next_called_twice",
                "next_expired" => "extension_next_expired",
                "next_outside_callback" => "extension_next_outside_callback",
                "self_close" => "extension_self_close",
                "missing_capability" => "extension_missing_capability",
                "mount_close_timeout" => "extension_mount_close_timeout",
                "mount_cleanup_incomplete" => "extension_mount_cleanup_incomplete",
                "registry_closed" => "extension_registry_closed",
                "context_owner_closed" => "extension_context_owner_closed",
                "context_identity_mismatch" => "extension_context_identity_mismatch",
                "workspace_unavailable" => "extension_workspace_unavailable",
                "context_initialization_failed" => "extension_context_initialization_failed",
                "context_initialization_incomplete" => {
                    "extension_context_initialization_incomplete"
                }
                "context_cleanup_incomplete" => "extension_context_cleanup_incomplete",
                _ => "extension",
            },
            Self::TurnLimit => "turn_limit",
            Self::OutputLimit { .. } => "output_limit",
            Self::ModelResponseRejected(..) => "model_response_rejected",
            Self::TaskStopped => "task_stopped",
            Self::Missing(..) => "missing",
            Self::InvalidConfiguration(..) => "invalid_configuration",
            Self::LeaseLost => "lease_lost",
            Self::Interrupted => "interrupted",
            Self::TerminalOutcomeUnknown { .. } => "terminal_outcome_unknown",
            Self::AttemptBoundaryUnknown { .. } => "attempt_boundary_unknown",
            Self::AttemptCleanupIncomplete { .. } => "attempt_cleanup_incomplete",
            Self::PhaseOutcomeUnknown { .. } => "phase_outcome_unknown",
            Self::ToolOutcomeUnknown { .. } => "tool_outcome_unknown",
            Self::DurableContinuationRequired { .. } => "durable_continuation_required",
            Self::Paused => "paused",
            Self::PlanChanged => "plan_changed",
        }
    }
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
    pub model_controls: crabber_core::ModelControls,
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
#[derive(Clone)]
struct AttemptIdentity {
    attempt: RunId,
    context: Option<TraceContext>,
    parent: Arc<std::sync::Mutex<Option<tracing::Span>>>,
}

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
    attempt: Option<AttemptIdentity>,
    trace_parent: tracing::Span,
    store: Arc<dyn Store>,
    behavior_fingerprint: crabber_core::InputFingerprint,
    host_callbacks: crabber_core::InputFingerprint,
    execution_tasks: crate::execution_tasks::ExecutionTasks,
    worker_resources: Option<crate::execution_tasks::WorkerResources>,
    root_scope: Option<crate::execution_tasks::RootScope>,
    close_coordinator: Arc<host_close::CloseCoordinator>,
    worker_callbacks: Option<crabber_extension::CallbackInvocationCapability>,
    session_invocations: crate::session_invocations::SessionInvocations,
    session_services: crate::session_services::SessionServices,
    preparation_scope: Option<preparation::scope::PlanScope>,
    retained_preparations: preparation::retained::RetainedPreparations,
    pending_drivers: crabber_session::session_owner::owned_scope::PendingDriverTasks,
    owned_attempt: Option<Arc<attempt::OwnedAttempt>>,
    source_delivery: Option<Arc<crate::public_source::SourceDelivery>>,
    observation_lifetime: Arc<ObservationLifetime>,
    tool_resumes: Arc<
        std::sync::Mutex<std::collections::BTreeMap<ToolCallId, crabber_session::ToolPhaseResume>>,
    >,
    frontend_resumes: Arc<
        std::sync::Mutex<
            std::collections::BTreeMap<ToolCallId, crabber_session::FrontendPhaseResume>,
        >,
    >,
    tool_suspensions: Arc<std::sync::Mutex<Vec<tool_suspension::PendingSuspension>>>,
    resolver: Arc<dyn Resolver>,
    workspace_reader_resolver: Option<Arc<dyn WorkspaceReaderResolver>>,
    plan_provider: Arc<dyn RunPlanProvider>,
    contextual_provider: Option<Arc<crabber_extension::ContextualPlanProvider>>,
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
    #[serde(default)]
    model_controls: crabber_core::ModelControls,
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
            model_controls: request.model_controls.clone(),
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
            model_controls: self.model_controls,
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
    contextual_provider: Option<Arc<crabber_extension::ContextualPlanProvider>>,
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

pub struct RunHandle {
    session_id: SessionId,
    run_id: RunId,
    store: Arc<dyn Store>,
    clock: Arc<dyn Clock>,
    done: oneshot::Receiver<Result<RunResult, RuntimeError>>,
    task: Arc<crate::execution_tasks::ExecutionTask>,
    tasks: crate::execution_tasks::ExecutionTasks,
    worker_resources: Arc<crate::execution_tasks::WorkerResources>,
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

impl RunHandle {
    #[cfg(test)]
    pub(crate) fn worker_resources_for_test(&self) -> &crate::execution_tasks::WorkerResources {
        &self.worker_resources
    }

    /// Clone local control while retaining sole ownership of the event receiver.
    #[must_use]
    pub fn control(&self) -> RunControl {
        debug_assert!(
            self.worker_resources
                .matches(&self.session_id, &self.run_id)
        );
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
        let store = self.store.clone();
        let session = self.session_id.clone();
        let clock = self.clock.clone();
        let admitted = self
            .tasks
            .admit(Some(session.clone()), move |scope| async move {
                if scope.cancellation().is_cancelled() {
                    return Err(RuntimeError::HostClosed);
                }
                let message = user_message(session.clone(), text, clock.now());
                store.enqueue_inbox(&session, kind, message).await?;
                Ok(())
            })?;
        let result = admitted.result.await;
        if self.tasks.join(&admitted.task).await != crate::TaskTermination::Returned {
            return Err(RuntimeError::TaskStopped);
        }
        result.map_err(|_| RuntimeError::TaskStopped)?
    }
}

impl Orchestrator {
    async fn join_worker_resources(&self, run: &RunId, bound: Duration) -> bool {
        let joined = match &self.worker_resources {
            Some(resources) if resources.run_matches(run) => resources.join(bound).await,
            _ => false,
        };
        // Refuse known failed OR still-unobserved kernel callbacks. This is a
        // negative gate over actual joins, not a whole-Run ownership proof.
        joined
            && self
                .worker_callbacks
                .as_ref()
                .is_none_or(|callbacks| callbacks.check_observed_joins().is_ok())
    }
    #[cfg(test)]
    pub(crate) fn execution_tasks_for_test(&self) -> &crate::execution_tasks::ExecutionTasks {
        &self.execution_tasks
    }

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
    /// tool `resources` and `background_tasks` (sealed state, pending joins and failures). Active tool scopes
    /// remain open; the host must stop its ingress before a final drain.
    pub async fn drain_execution_tasks(&self, bound: Duration) -> crate::ExecutionTaskDrain {
        self.execution_tasks.drain(bound).await
    }

    /// Join this session's retained preparation, workers, tools and background tasks.
    /// This is a local observation: it neither stops ingress nor proves durable
    /// cleanup or the absence of work on another controller.
    pub async fn drain_session_execution_tasks(
        &self,
        session: &SessionId,
        bound: Duration,
    ) -> crate::ExecutionTaskDrain {
        self.execution_tasks.drain_session(session, bound).await
    }

    /// Observe or cooperatively cancel only this session's independent jobs.
    /// A deadline retains every unfinished join; expiry never counts as Joined.
    pub async fn drain_session_invocations(
        &self,
        session: &SessionId,
        cancel: bool,
        bound: Duration,
    ) -> Vec<crate::SessionInvocationReport> {
        self.session_invocations.drain(session, cancel, bound).await
    }

    fn own_preparation<T, F, W>(
        &self,
        session_id: SessionId,
        build: F,
    ) -> impl std::future::Future<Output = Result<T, RuntimeError>> + Send + '_
    where
        T: Send + 'static,
        F: FnOnce(Self) -> W + Send + 'static,
        W: std::future::Future<Output = Result<T, RuntimeError>> + Send + 'static,
    {
        self.own_root(Some(session_id), build)
    }
    fn own_root<T, F, W>(
        &self,
        session: Option<SessionId>,
        build: F,
    ) -> impl std::future::Future<Output = Result<T, RuntimeError>> + Send + '_
    where
        T: Send + 'static,
        F: FnOnce(Self) -> W + Send + 'static,
        W: std::future::Future<Output = Result<T, RuntimeError>> + Send + 'static,
    {
        let mut runtime = self.clone();
        runtime.trace_parent = tracing::Span::current();
        async move {
            let admitted = self.execution_tasks.admit(session, move |scope| {
                let mut runtime = runtime;
                runtime.root_scope = Some(scope);
                // Public reentry never inherits a prior worker's native/local loan.
                runtime.worker_resources = None;
                runtime.worker_callbacks = None;
                runtime.preparation_scope = None;
                runtime.owned_attempt = None;
                async move {
                    runtime.check_original_stop()?;
                    build(runtime).await
                }
            })?;
            let result = admitted.result.await;
            if self.execution_tasks.join(&admitted.task).await != crate::TaskTermination::Returned {
                return Err(RuntimeError::TaskStopped);
            }
            result.map_err(|_| RuntimeError::TaskStopped)?
        }
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
                let saved = saved.unwrap().unwrap();
                assert_eq!(saved.model_controls, crabber_core::ModelControls::default());
                assert_eq!(
                    saved.max_output_tokens,
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
        let work_session = session_id.clone();
        let work_run = run_id.clone();
        let work_plan = plan.clone();
        let recovery_session = session_id.clone();
        let recovery_run = run_id.clone();
        let recovery = runtime
            .own_preparation(session_id.clone(), move |runtime| async move {
                runtime.spawn_worker(
                    recovery_session,
                    recovery_run,
                    move |worker, _| async move {
                        worker
                            .settle_unfinished_calls(
                                execution.as_ref(),
                                &work_session,
                                &work_run,
                                &work_plan,
                                false,
                                None,
                            )
                            .await?;
                        // This fixture exercises settlement without claiming a native
                        // terminal Run result. Its actual worker still owns delivery.
                        Err(RuntimeError::Interrupted)
                    },
                )
            })
            .await
            .unwrap();
        assert!(matches!(
            recovery.done().await,
            Err(RuntimeError::Interrupted)
        ));
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

impl Orchestrator {
    fn public_source_for_attempt(
        &self,
        session: &SessionId,
    ) -> crabber_extension::PublicSourceWriter {
        self.owned_attempt.as_ref().map_or_else(
            crabber_extension::PublicSourceWriter::default,
            |attempt| {
                crate::public_source::bind(
                    self.store.clone(),
                    session.clone(),
                    crabber_session::PublicSourceAuthority::Attempt(attempt.authority.clone()),
                    self.source_delivery.clone(),
                    None,
                )
            },
        )
    }
}

impl Orchestrator {
    fn new_source_delivery(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<Arc<crate::public_source::SourceDelivery>, RuntimeError> {
        let attempt = self
            .owned_attempt
            .as_ref()
            .ok_or(crabber_session::AttemptError::UnknownStoreOutcome)?;
        let cursor = attempt
            .begin_cursor
            .ok_or(crabber_session::AttemptError::UnknownStoreOutcome)?;
        Ok(crate::public_source::SourceDelivery::new(
            session.clone(),
            run.clone(),
            crabber_core::PublicAttemptIdentity {
                id: attempt.target.id.clone(),
                generation: attempt.target.generation,
            },
            cursor,
            self.observer.clone(),
        ))
    }
}
