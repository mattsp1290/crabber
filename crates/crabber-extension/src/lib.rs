//! Frozen extension plan and tool execution boundary.

mod dispatch;
mod plan;
mod registry;
mod state;
mod tool;
pub use tool::{
    ApprovalFacade, HostServices, ProgressSink, Subprocess, ToolContext, UserPrompter, WorkspaceFs,
};

pub use dispatch::{AroundCallback, Callback, Dispatcher, Handler, Mode, Next, Point};
pub use dispatch::{
    ContextAssemble, EventPublished, ModelCompleted, ModelRequestError, ModelRequested,
    ModelStream, RunAdmitted, RunBeforeExecute, RunSettled, RunStarted, ToolExecute, ToolPrepare,
    ToolResultTransform, ToolSettled, ToolStarted, TurnCompleted, TurnPrepare, TurnStarted,
};
pub use registry::{
    Extension, GuardContext, GuardDecision, MountHandle, Registrar, Registry, Scope, ToolGuard,
};
pub use state::{StateSink, current_state_sink, with_state_sink};

pub use plan::{
    ComponentIdentity, ExtensionError, PlanFingerprint, PromptSection, RunPlan, RunPlanProvider,
    StaticPlanProvider, ToolDefinition, ToolExecutor, compute_fingerprint,
};

/// Retained for the workspace scaffold's smoke test.
pub const CRATE_NAME: &str = "crabber-extension";
