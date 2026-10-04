//! Frozen extension plan and tool execution boundary.

mod dispatch;
mod plan;
mod registry;
mod result_transform;
mod state;
mod tool;
pub use tool::{
    ApprovalFacade, HostServices, ProgressSink, Subprocess, ToolContext, UserPrompter,
    WorkspaceContext, WorkspaceFs,
};

pub use result_transform::{
    CleanupTracker, DEFAULT_MOUNT_CLOSE_TIMEOUT, EnvelopeError, FINAL_REDACTION_DEADLINE,
    InputUnavailable, RESULT_TRANSFORM_CONTRACT_VERSION, ResultTransformCallback, ToolInput,
    ToolOutcomeClass, ToolResultContext, ToolResultOutcome, TransformOutput, TransformPhase,
    json_result_transform, parse_result_envelope, result_envelope, result_transform_failed_message,
};

pub use dispatch::{AroundCallback, Callback, Dispatcher, Handler, Mode, Next, Point};
pub use dispatch::{
    ContextAssemble, EventPublished, ModelCompleted, ModelRequestError, ModelRequested,
    ModelStream, RunAdmitted, RunBeforeExecute, RunSettled, RunStarted, ToolExecute, ToolPrepare,
    ToolResultTransform, ToolSettled, ToolStarted, TurnCompleted, TurnPrepare, TurnStarted,
};
pub use registry::{
    CleanupJoinTimeout, CleanupOwner, Extension, GuardContext, GuardDecision, MountCloseObserver,
    MountCloseTimeout, MountHandle, Registrar, Registry, Scope, ToolGuard,
};
pub use state::{StateSink, current_state_sink, with_state_sink};

pub use plan::{
    ComponentIdentity, ExtensionError, PlanFingerprint, PromptSection, RunPlan, RunPlanProvider,
    StaticPlanProvider, ToolDefinition, ToolExecutor, compute_fingerprint,
};

/// Retained for the workspace scaffold's smoke test.
pub const CRATE_NAME: &str = "crabber-extension";
