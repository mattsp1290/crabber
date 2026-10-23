//! Explicit-policy Unix shell execution with bounded capture and group cleanup.
#![warn(missing_docs)]
#[cfg(not(unix))]
compile_error!("crabber-tools-shell requires Unix");
use async_trait::async_trait;
use crabber::{ExtensionError, ToolDefinition, ToolExecutor, extension::ToolContext};
use crabber_tools_core::{
    AcquireError, Capacity, EnvPolicy, Limits, RootLock, RunAs, WorkspaceRoot, acquire_mutating,
    category, check_cancelled, failed, validate_binary,
};
use serde::Serialize;
use serde_json::Value;
use std::{path::PathBuf, sync::Arc};
mod args;
mod info;
mod run;
pub use info::info;
/// Default per-call timeout (zero selects this value).
pub const DEFAULT_TIMEOUT_SECONDS: u64 = 60;
/// Maximum permitted per-call timeout.
pub const MAX_TIMEOUT_SECONDS: u64 = 600;
/// Whether the supplied shell sources login startup files.
#[derive(Clone, Copy, Serialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "kebab-case")]
pub enum StartupMode {
    /// Invoke with -lc; startup files are host-controlled.
    Login,
    /// Invoke with -c.
    NonLogin,
}
/// Required host-owned shell execution policy.
#[derive(Clone, Serialize, Debug, PartialEq, Eq)]
pub struct ShellPolicy {
    /// Absolute executable accepting -lc and -c.
    pub shell_binary: PathBuf,
    /// Login startup behavior.
    pub startup_mode: StartupMode,
    /// Explicit child environment.
    pub env: EnvPolicy,
    /// Optional child identity.
    pub run_as: Option<RunAs>,
    /// Retained bytes per output stream, 1..=16 MiB.
    pub output_cap_bytes: usize,
}
impl ShellPolicy {
    /// Eino-shaped login policy with 256 KiB caps; binary and environment remain explicit.
    #[must_use]
    pub fn eino_default(shell_binary: PathBuf, env: EnvPolicy) -> Self {
        Self {
            shell_binary,
            startup_mode: StartupMode::Login,
            env,
            run_as: None,
            output_cap_bytes: 256 * 1024,
        }
    }
    /// Reject missing executables, invalid environments, or unbounded output caps.
    ///
    /// # Errors
    /// Returns a plan error for invalid shell, environment, identity, or output bounds.
    pub fn validate(&self) -> Result<(), ExtensionError> {
        validate_binary(&self.shell_binary)?;
        self.env.validate()?;
        if !(1..=16 * 1024 * 1024).contains(&self.output_cap_bytes) {
            return Err(ExtensionError::Plan("shell output cap".into()));
        }
        Ok(())
    }
}
/// Shared shell resources. Construct capacity once for a whole extension.
pub struct Options {
    /// Admitted workspace, used as cwd.
    pub root: Arc<WorkspaceRoot>,
    /// Host shell policy.
    pub policy: ShellPolicy,
    /// Shared bounds.
    pub limits: Limits,
    /// Shared work slots.
    pub capacity: Capacity,
}
/// Validate resources and return a context-required shell executor.
///
/// # Errors
/// Returns a plan error if the shell policy or shared execution limits are invalid.
pub fn definition(options: Arc<Options>) -> Result<Arc<ToolDefinition>, ExtensionError> {
    options.policy.validate()?;
    options.limits.validate()?;
    Ok(Arc::new(ToolDefinition {
        info: info(),
        executor: Arc::new(ShellTool(options)),
    }))
}
struct ShellTool(Arc<Options>);
#[async_trait]
impl ToolExecutor for ShellTool {
    async fn execute(&self, _: Value) -> Result<crabber::core::ToolOutput, ExtensionError> {
        crabber_tools_core::context_required()
    }
    #[allow(
        clippy::cast_possible_truncation,
        reason = "Retain the timing ABI; u64 milliseconds overflow only after 584 million years."
    )]
    async fn execute_with_context(
        &self,
        ctx: ToolContext,
        value: Value,
    ) -> Result<crabber::core::ToolOutput, ExtensionError> {
        let start = tokio::time::Instant::now();
        self.invoke(ctx, value).await.map(|mut value| {
            for (key, default) in [
                ("exit_code", serde_json::json!(-1)),
                ("stdout", serde_json::json!("")),
                ("stderr", serde_json::json!("")),
            ] {
                value
                    .as_object_mut()
                    .expect("envelope object")
                    .entry(key)
                    .or_insert(default);
            }
            value["duration_ms"] = serde_json::json!(start.elapsed().as_millis() as u64);
            crabber_tools_core::to_output(value, &["stdout", "stderr"])
        })
    }
}
impl ShellTool {
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
        let lock = RootLock::for_root(&self.0.root);
        let guards = match acquire_mutating(&lock, &self.0.capacity, &self.0.limits, &ctx.cancel)
            .await
        {
            Ok(g) => g,
            Err(AcquireError::Cancelled) => return Err(ExtensionError::Tool("cancelled".into())),
            Err(AcquireError::Unavailable) => {
                return Ok(failed(category::UNAVAILABLE, "workspace busy"));
            }
        };
        tokio::select! {biased;
            ()=ctx.cancel.cancelled()=>Err(ExtensionError::Tool("cancelled".into())),
            result=run::run(&self.0,args,&ctx,guards)=>Ok(result),
        }
    }
}
