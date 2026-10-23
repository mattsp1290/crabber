//! Preflighted capability-confined multi-file structured patches.
#![warn(missing_docs)]
use async_trait::async_trait;
use crabber::{ExtensionError, ToolDefinition, ToolExecutor, extension::ToolContext};
use crabber_tools_core::{
    AcquireError, Capacity, Limits, RootLock, ToolError, WorkspaceRoot, acquire_mutating, category,
    check_cancelled, run_blocking_owned,
};
use serde::Deserialize;
use serde_json::Value;
use std::sync::Arc;
mod commit;
mod info;
mod matching;
mod parser;
mod preflight;
mod result;
mod targets;
pub use info::info;
/// Maximum patch input size, matching the upstream tool.
pub const MAX_PATCH_BYTES: usize = 1024 * 1024;
/// Maximum source or resulting text file retained during preflight.
pub const MAX_FILE_BYTES: usize = 16 * 1024 * 1024;
/// Maximum aggregate retained output content across a preflight plan.
pub const MAX_PLAN_BYTES: usize = 64 * 1024 * 1024;
/// Host-owned workspace and resources shared across the mount.
pub struct Options {
    /// Host path exclusions, shared across read and write tools.
    pub deny: crabber_tools_core::PathDenyPolicy,
    /// Admitted directory capability.
    pub root: Arc<WorkspaceRoot>,
    /// Finite acquisition and concurrency bounds.
    pub limits: Limits,
    /// Mount-wide capacity.
    pub capacity: Capacity,
}
/// Construct a context-required patch executor.
#[must_use]
pub fn definition(options: Arc<Options>) -> Arc<ToolDefinition> {
    Arc::new(ToolDefinition {
        info: info(),
        executor: Arc::new(PatchTool(options)),
    })
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Args {
    patch_text: String,
}
struct PatchTool(Arc<Options>);
#[async_trait]
impl ToolExecutor for PatchTool {
    async fn execute(&self, _: Value) -> Result<crabber::core::ToolOutput, ExtensionError> {
        crabber_tools_core::context_required()
    }
    async fn execute_with_context(
        &self,
        ctx: ToolContext,
        args: Value,
    ) -> Result<crabber::core::ToolOutput, ExtensionError> {
        self.invoke(ctx, args)
            .await
            .map(|value| crabber_tools_core::to_output(value, &[]))
    }
}
impl PatchTool {
    async fn invoke(&self, ctx: ToolContext, args: Value) -> Result<Value, ExtensionError> {
        check_cancelled(&ctx.cancel)?;
        let fail = |e| result::failure(&e, &[], false);
        let Ok(args) = serde_json::from_value::<Args>(args) else {
            return Ok(fail(ToolError::new(
                category::VALIDATION,
                "invalid arguments",
            )));
        };
        if args.patch_text.len() > MAX_PATCH_BYTES {
            return Ok(fail(ToolError::new(
                category::TOO_LARGE,
                "patch exceeds 1 MiB",
            )));
        }
        if let Err(e) = self
            .0
            .root
            .matches_session_directory(ctx.workspace().directory())
        {
            return Ok(fail(e));
        }
        let guards = match acquire_mutating(
            &RootLock::for_root(&self.0.root),
            &self.0.capacity,
            &self.0.limits,
            &ctx.cancel,
        )
        .await
        {
            Ok(g) => g,
            Err(AcquireError::Cancelled) => return Err(ExtensionError::Tool("cancelled".into())),
            Err(AcquireError::Unavailable) => {
                return Ok(fail(ToolError::new(
                    category::UNAVAILABLE,
                    "workspace busy",
                )));
            }
        };
        let options = self.0.clone();
        run_blocking_owned(&ctx, move |cancel| {
            let _guards = guards;
            let operations = match parser::parse(&args.patch_text) {
                Ok(o) => o,
                Err(e) => return fail(e),
            };
            match preflight::plan(&options.root, &options.deny, operations, cancel) {
                Ok(plan) => commit::apply(&options.root, plan, cancel),
                Err((error, files)) => result::failure(&error, &files, false),
            }
        })
        .await
    }
}
