//! Streaming ripgrep search with fixed process identity and bounded result memory.
#[cfg(not(unix))]
compile_error!("crabber-tools-search requires Unix");
use async_trait::async_trait;
use crabber::{ExtensionError, ToolDefinition, ToolExecutor, extension::ToolContext};
use crabber_tools_core::*;
use serde::Serialize;
use serde_json::Value;
use std::{path::PathBuf, sync::Arc};
mod args;
mod info;
mod parse;
mod rg;
mod submatches;
pub use info::info;
/// Default per-call timeout.
pub const DEFAULT_TIMEOUT_SECONDS: u64 = 60;
/// Maximum per-call timeout.
pub const MAX_TIMEOUT_SECONDS: u64 = 600;
/// Default match count cap.
pub const DEFAULT_LIMIT: usize = 200;
/// Maximum requested match count.
pub const MAX_LIMIT: usize = 1000;
/// Maximum before/after context lines.
pub const MAX_CONTEXT_LINES: usize = 20;
/// Retained bytes per matched line.
pub const MAX_LINE_BYTES: usize = 4096;
/// Maximum serialized match data.
pub const MAX_RESULT_BYTES: usize = 256 * 1024;
/// Retained standard error prefix.
pub const MAX_STDERR_BYTES: usize = 4096;
/// Maximum diagnostic size (diagnostics are sanitized).
pub const MAX_ERROR_MESSAGE_BYTES: usize = 1024;
/// Host-owned ripgrep policy, included in mount identity.
#[derive(Clone, Serialize, Debug, PartialEq, Eq)]
pub struct SearchPolicy {
    /// Absolute executable path, resolved once.
    pub rg_binary: PathBuf,
    /// Explicit child environment; RIPGREP_CONFIG_PATH is always removed.
    pub env: EnvPolicy,
    /// Optional child identity.
    pub run_as: Option<RunAs>,
}
impl SearchPolicy {
    /// Resolve rg on the host PATH once, preserving the supplied environment policy.
    pub fn resolve_rg_from_path(env: EnvPolicy) -> Result<Self, ExtensionError> {
        let policy = Self {
            rg_binary: resolve_binary("rg")?,
            env,
            run_as: None,
        };
        policy.validate()?;
        Ok(policy)
    }
    /// Reject missing executables and invalid environment fields.
    pub fn validate(&self) -> Result<(), ExtensionError> {
        validate_binary(&self.rg_binary)?;
        self.env.validate()
    }
}
/// Shared root, policy, and bounded mount resources.
pub struct Options {
    /// Admitted workspace.
    pub root: Arc<WorkspaceRoot>,
    /// Ripgrep process policy.
    pub policy: SearchPolicy,
    /// Mount bounds.
    pub limits: Limits,
    /// Mount's shared capacity.
    pub capacity: Capacity,
}
/// Validate and construct a context-required search executor.
pub fn definition(options: Arc<Options>) -> Result<Arc<ToolDefinition>, ExtensionError> {
    options.policy.validate()?;
    options.limits.validate()?;
    Ok(Arc::new(ToolDefinition {
        info: info(),
        executor: Arc::new(SearchTool(options)),
    }))
}
struct SearchTool(Arc<Options>);
#[async_trait]
impl ToolExecutor for SearchTool {
    async fn execute(&self, _: Value) -> Result<crabber::core::ToolOutput, ExtensionError> {
        crabber_tools_core::context_required()
    }
    async fn execute_with_context(
        &self,
        ctx: ToolContext,
        value: Value,
    ) -> Result<crabber::core::ToolOutput, ExtensionError> {
        let start = tokio::time::Instant::now();
        self.invoke(ctx, value).await.map(|mut value| {
            for (key, default) in [
                ("matches", serde_json::json!([])),
                ("match_count", serde_json::json!(0)),
            ] {
                value
                    .as_object_mut()
                    .expect("envelope object")
                    .entry(key)
                    .or_insert(default);
            }
            value["duration_ms"] = serde_json::json!(start.elapsed().as_millis() as u64);
            crabber_tools_core::to_output(value, &[])
        })
    }
}
impl SearchTool {
    async fn invoke(&self, ctx: ToolContext, value: Value) -> Result<Value, ExtensionError> {
        check_cancelled(&ctx.cancel)?;
        let args = match args::Args::parse(value) {
            Ok(a) => a,
            Err(e) => return Ok(e.value()),
        };
        if let Err(e) = self
            .0
            .root
            .matches_session_directory(ctx.workspace().directory())
        {
            return Ok(e.value());
        }
        let permit = match acquire_read(&self.0.capacity, &self.0.limits, &ctx.cancel).await {
            Ok(p) => p,
            Err(AcquireError::Cancelled) => return Err(ExtensionError::Tool("cancelled".into())),
            Err(AcquireError::Unavailable) => {
                return Ok(failed(category::UNAVAILABLE, "workspace busy"));
            }
        };
        // Follow symlinks through the capability before passing a name to rg.
        // rg opens by path again: hosts must accept the documented race window.
        if let Err(e) = self.0.root.dir().canonicalize(&args.path) {
            return Ok(failed(category_for_io(&e), "search path unavailable"));
        }
        tokio::select! {biased;
            _=ctx.cancel.cancelled()=>Err(ExtensionError::Tool("cancelled".into())),
            result=rg::run(&self.0,args,&ctx,permit)=>Ok(result),
        }
    }
}
