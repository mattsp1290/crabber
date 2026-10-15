//! Fresh-run agent orchestration.

mod event_payload;
mod execution_events;
pub use execution_events::RunEvents;
mod execution_tasks;
pub use execution_tasks::{
    ExecutionTaskDrain, JoinedExecutionTask, JoinedPreparationTask, PreparationTaskId,
    TaskTermination, ToolResourceDrain,
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
    Admission, CompactionPolicy, ConfigSnapshot, Continuation, ContinueApproval, DirectModelStream,
    ExecutionMode, INTERRUPT_SETTLEMENT_BOUND, INTERRUPTED_RESULT_TEXT, ModelStream, NoopObserver,
    Observer, Orchestrator, OrchestratorBuilder, RecoverReport, RecoverySkipReason, Request,
    RunControl, RunHandle, RunResult, RuntimeError, SkippedRun, TOOL_PIPELINE_HANDLER_ID,
    TurnSnapshot,
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
