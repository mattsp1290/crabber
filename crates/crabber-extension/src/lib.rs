//! Frozen extension plan and tool execution boundary.

mod contextual;
mod dispatch;
pub use contextual::{ContextualPlanProvider, ExtensionFactory, FactoryContext, FactoryScope};
mod model_middleware;
pub use model_middleware::*;
mod plan;
mod preparation_resources;
pub use preparation_resources::{
    PREPARATION_RESOURCE_CONTRACT_VERSION, PreparationCleanup, PreparationResourceContext,
    PreparationResourceContract, PreparationResourceLease,
};
mod prompt_contribution;
mod registry;
mod result_transform;
pub use prompt_contribution::*;
mod input_tool;
mod resumable;
mod state;
mod tool;
pub use resumable::ResumableTool;
mod tool_resources;
pub use tool::{
    DurableInteractionUnsupported, HostServices, ProgressSink, Subprocess,
    TOOL_INTERACTION_CONTRACT_VERSION, ToolContext, WorkspaceContext, WorkspaceFs,
};
pub use tool_resources::{
    MAX_TOOL_RESOURCE_TASKS, ResourceDrain, ResourceFailure, ResourceRegistrationError,
    TOOL_RESOURCE_CONTRACT_VERSION, ToolResourceGuard, ToolResourceOwner, ToolResources,
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
    ComponentIdentity, ExtensionError, JsonToolExecutor, PlanFingerprint, PlanReleaseObserver,
    PromptSection, RunPlan, RunPlanProvider, StaticPlanProvider, ToolDefinition, ToolExecutor,
    compute_fingerprint,
};

/// Retained for the workspace scaffold's smoke test.
pub const CRATE_NAME: &str = "crabber-extension";

mod tool_output;
pub use tool_output::{
    TOOL_OUTPUT_ENVELOPE_VERSION, parse_tool_output_envelope, tool_output_envelope,
};

mod session_invocations;
pub use session_invocations::{
    SessionInvocationAdmission, SessionInvocationContext, SessionInvocationError,
    SessionInvocationRequest, SessionInvocationSpawner, SessionInvocationTask, SessionInvocations,
};
