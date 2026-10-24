//! Explicit-policy bn-backed tracker mutations with bounded process capture.
#![warn(missing_docs)]
use async_trait::async_trait;
use crabber::{ExtensionError, ToolDefinition, ToolExecutor, extension::ToolContext};
use crabber_tools_core::{
    AcquireError, Capacity, Limits, ToolError, WorkspaceRoot, acquire_read, category,
    check_cancelled,
};
use serde_json::Value;
use std::sync::Arc;
mod args;
mod info;
mod policy;
mod result;
mod run;
pub use args::MAX_TEXT_BYTES;
pub use info::info;
pub use policy::TrackerPolicy;
pub use run::{DIAGNOSTIC_CAP, TIMEOUT};
/// Host-owned workspace routing and shared resources.
pub struct Options {
    /// Admitted workspace; used as child cwd, not as the tracker namespace.
    pub root: Arc<WorkspaceRoot>,
    /// Explicit tracker backend policy.
    pub policy: TrackerPolicy,
    /// Finite acquisition and concurrency bounds.
    pub limits: Limits,
    /// Mount-wide capacity. Hub mutations use bn's own lock, not `RootLock`.
    pub capacity: Capacity,
}
/// Validate policy and construct a context-required tracker executor.
///
/// # Errors
/// Returns a plan error if tracker policy or shared execution limits are invalid.
pub fn definition(options: Arc<Options>) -> Result<Arc<ToolDefinition>, ExtensionError> {
    options.policy.validate()?;
    options.limits.validate()?;
    Ok(Arc::new(ToolDefinition {
        info: info(),
        executor: Arc::new(TrackerTool(options)),
    }))
}
struct TrackerTool(Arc<Options>);
#[async_trait]
impl ToolExecutor for TrackerTool {
    async fn execute(&self, _: Value) -> Result<crabber::core::ToolOutput, ExtensionError> {
        crabber_tools_core::context_required()
    }
    async fn execute_with_context(
        &self,
        ctx: ToolContext,
        value: Value,
    ) -> Result<crabber::core::ToolOutput, ExtensionError> {
        self.invoke(ctx, value)
            .await
            .map(|value| crabber_tools_core::to_output(value, &[]))
    }
}
impl TrackerTool {
    async fn invoke(&self, ctx: ToolContext, value: Value) -> Result<Value, ExtensionError> {
        check_cancelled(&ctx.cancel)?;
        let Ok(args) = serde_json::from_value::<args::Args>(value) else {
            return Ok(result::failure(
                "",
                "",
                &ToolError::new(category::VALIDATION, "invalid tracker arguments"),
            ));
        };
        let fail = |e| result::failure(&args.op, &args.id, &e);
        let command_args = match args.command(&self.0.policy) {
            Ok(a) => a,
            Err(e) => return Ok(fail(e)),
        };
        if let Err(e) = self
            .0
            .root
            .matches_session_directory(ctx.workspace().directory())
        {
            return Ok(fail(e));
        }
        let permit = match acquire_read(&self.0.capacity, &self.0.limits, &ctx.cancel).await {
            Ok(p) => p,
            Err(AcquireError::Cancelled) => return Err(ExtensionError::Tool("cancelled".into())),
            Err(AcquireError::Unavailable) => {
                return Ok(fail(ToolError::new(
                    category::UNAVAILABLE,
                    "workspace busy",
                )));
            }
        };
        tokio::select! {biased;
            ()=ctx.cancel.cancelled()=>Err(ExtensionError::Tool("cancelled".into())),
            result=run::run(&self.0,&args,command_args,&ctx,permit)=>Ok(result),
        }
    }
}
