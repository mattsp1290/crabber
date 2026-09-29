//! Frozen extension plan and tool execution boundary.

mod plan;

pub use plan::{
    ComponentIdentity, ExtensionError, PlanFingerprint, PromptSection, RunPlan, RunPlanProvider,
    StaticPlanProvider, ToolDefinition, ToolExecutor, compute_fingerprint,
};

/// Retained for the workspace scaffold's smoke test.
pub const CRATE_NAME: &str = "crabber-extension";
