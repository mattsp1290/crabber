//! Fresh-run agent orchestration.

mod event_payload;
mod execution_events;
mod telemetry_targets;
pub use execution_events::RunEvents;
mod execution_tasks;
pub use execution_tasks::{
    ExecutionTaskDrain, ExecutionTaskFailure, JoinedExecutionTask, JoinedPreparationTask,
    JoinedUnscopedPreparationTask, PreparationTaskId, RunCloseIssue, RunCloseIssueKind,
    TaskTermination, ToolResourceDrain, WorkerResourceDrain, WorkerResourceKind,
};
mod observation;
pub use crabber_core::{AbandonAuthority, AbandonError, AbandonOutcome, AbandonRequest};

mod orchestrator;
pub use observation::{
    ModelPurpose, MonotonicClock, OperationKind, OperationalObservation, SystemMonotonicClock,
    TerminalReason,
};
mod policy;

pub use orchestrator::{
    Admission, CloseObservation, CompactionPolicy, ConfigSnapshot, Continuation, ContinueApproval,
    ContinueFrontend, ContinueTool, DirectModelStream, ExecutionMode, INTERRUPT_SETTLEMENT_BOUND,
    INTERRUPTED_RESULT_TEXT, ModelStream, NoopObserver, Observer, Orchestrator,
    OrchestratorBuilder, PreparationRetryProgress, QueueControl, RecoverApproval, RecoverFrontend,
    RecoverReport, RecoverTool, RecoverySkipReason, Request, RetainedPreparationProgress,
    RetainedPreparationStatus, RunControl, RunHandle, RunResult, RuntimeCloseObservation,
    RuntimeCloseReport, RuntimeDependentCloseReport, RuntimeError, SkippedRun,
    TOOL_PIPELINE_HANDLER_ID, ToolContinuation, TurnSnapshot,
};
pub use policy::{
    IdentityToolPipeline, InterruptPolicy, PermissionDecision, PermissionPolicy, PermissionRule,
    StaticPolicy, ToolPipeline,
};

/// Retained for the workspace scaffold's smoke test.
pub const CRATE_NAME: &str = "crabber-runtime";

#[cfg(test)]
mod extension_tests;
#[cfg(test)]
mod result_transform_tests;
#[cfg(test)]
mod tests;

#[cfg(test)]
mod prompt_contribution_tests;

#[cfg(test)]
mod model_middleware_tests;

#[cfg(test)]
mod bound_provider_tests;

pub use crabber_session::AdmissionSelectionPolicy;

mod session_invocations;
mod session_services;
pub use session_invocations::{SessionInvocationReport, SessionInvocationStatus};

pub use session_services::{SessionServiceReport, SessionServiceStatus};

mod public_source;
