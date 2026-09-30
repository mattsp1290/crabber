//! Fresh-run agent orchestration.

mod orchestrator;
mod policy;

pub use orchestrator::{
    DirectModelStream, ModelStream, NoopObserver, Observer, Orchestrator, OrchestratorBuilder,
    Request, RunHandle, RunResult, RuntimeError, TurnSnapshot,
};
pub use policy::{
    ApprovalRequester, DefaultDenyApprover, IdentityToolPipeline, PermissionDecision,
    PermissionPolicy, StaticPolicy, ToolPipeline,
};

/// Retained for the workspace scaffold's smoke test.
pub const CRATE_NAME: &str = "crabber-runtime";

#[cfg(test)]
mod tests;
