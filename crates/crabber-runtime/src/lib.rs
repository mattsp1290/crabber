//! Fresh-run agent orchestration.

mod orchestrator;
mod policy;

pub use orchestrator::{
    Admission, CompactionPolicy, ConfigSnapshot, DirectModelStream, ExecutionMode, ModelStream,
    NoopObserver, Observer, Orchestrator, OrchestratorBuilder, Request, RunHandle, RunResult,
    RuntimeError, TurnSnapshot,
};
pub use policy::{
    ApprovalRequester, DefaultDenyApprover, IdentityToolPipeline, InterruptPolicy,
    PermissionDecision, PermissionPolicy, PermissionRule, StaticPolicy, ToolPipeline,
};

/// Retained for the workspace scaffold's smoke test.
pub const CRATE_NAME: &str = "crabber-runtime";

#[cfg(test)]
mod extension_tests;
#[cfg(test)]
mod tests;
