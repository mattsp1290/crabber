//! Session-scoped Python for trusted host workloads, with bounded results.
//!
//! Hosts provision Python 3.11–3.14 and own permissions and process policy.
//! Python has host-user authority: private directories and isolated flags do
//! not form a sandbox. Interpreter globals are process-local, while tool results
//! are persisted by Crabber. Use Tokio with process/I/O support (`enable_all()`).
//! Closing any mount closes the shared instance; inspect live_runners after close.
mod config;
mod factory;
pub use crate::contextual::{OwnerReport, SharedCapacity};
pub use factory::Factory;
mod input;
mod lifecycle;
mod manager;
mod private_dirs;
mod runner;
mod session;
#[cfg(all(test, unix))]
mod tests;
mod tools;

pub use config::{Environment, EnvironmentMode, Limits, Options};
use crabber::extension::ExtensionError;
use manager::Manager;
pub use runner::{BoundedText, Status as ExecuteStatus};
use serde::Serialize;
use std::sync::Arc;

/// Tool executing Python against the owner's live globals.
pub const EXECUTE_TOOL: &str = "python_repl";
/// Tool discarding the owner's interpreter without recreating it.
pub const CLEAR_TOOL: &str = "python_repl_clear";
/// Execution permission metadata; the host decides access.
pub const PERMISSION_EXECUTE: &str = "process.python.execute";
/// Clear permission metadata; the host decides access.
pub const PERMISSION_MANAGE: &str = "process.python.manage";

/// A bounded result and notice about previously discarded interpreter state.
#[derive(Debug, Serialize)]
pub struct ExecuteResult {
    /// `completed` or `python_error`.
    pub status: ExecuteStatus,
    /// Captured Python stdout.
    pub stdout: BoundedText,
    /// Captured Python stderr.
    pub stderr: BoundedText,
    /// Trailing expression repr, empty on Python errors.
    pub result: BoundedText,
    /// Trimmed traceback, empty on success.
    pub exception: BoundedText,
    /// Count of resets of previously existing interpreters.
    pub generation: u64,
    /// Whether an earlier reset notice is being delivered.
    pub state_reset: bool,
    /// Empty, `canceled`, `timed_out`, `cleared`, or `runner_failed`.
    pub state_reset_reason: String,
}
/// Result of clearing an owner's live state.
#[derive(Debug, Serialize)]
pub struct ClearResult {
    /// Whether a live interpreter was discarded.
    pub had_state: bool,
    /// Reset generation after clearing.
    pub generation: u64,
}
/// One interpreter per durable (session, workspace) pair.
///
/// Construction validates paths and freezes environment without executing Python.
/// State and private directories do not survive a host restart. Detached children
/// and host-owned SIGCHLD reaping are outside the cleanup contract. Hosts settle
/// unfinished runs before changing fingerprinted policy. A killed host may leave
/// orphaned interpreters and private directories; hosts own stale-state handling.
pub struct PythonRepl {
    manager: Arc<Manager>,
    hash: String,
}
impl PythonRepl {
    /// Validate and freeze policy. Non-Unix targets reject construction.
    pub fn new(options: Options) -> Result<Self, ExtensionError> {
        let (configuration, hash) = config::validate(options)?;
        Ok(Self {
            manager: Arc::new(Manager::new(configuration)),
            hash,
        })
    }
    fn from_configuration(
        configuration: Arc<config::Configuration>,
        binding: crate::contextual::Binding,
        shared: Option<SharedCapacity>,
        hash: String,
    ) -> Self {
        let mut policy = Manager::new(configuration.as_ref().clone());
        policy.binding = Some(binding);
        policy.shared = shared;
        Self {
            manager: Arc::new(policy),
            hash,
        }
    }
    /// Observe local resources without exposing paths, output or process IDs.
    pub fn owner_report(&self) -> OwnerReport {
        self.manager.owner_report()
    }

    /// Snapshot of starting/live/quarantined interpreters, including survivors
    /// after bounded close. Includes runners that exited while idle until the
    /// owner's next execute, clear or close observes their exit.
    /// Does not expose paths, PIDs or environment values.
    pub fn live_runners(&self) -> usize {
        self.manager.live_runners()
    }
}
fn input_error(code: &'static str) -> ExtensionError {
    ExtensionError::Tool(format!("python repl input invalid: {code}"))
}
fn runtime_error(code: &'static str) -> ExtensionError {
    ExtensionError::Tool(format!("python repl runtime invalid: {code}"))
}
fn failure(code: &'static str) -> ExtensionError {
    ExtensionError::Tool(format!("python repl operation failed: {code}"))
}

#[async_trait::async_trait]
impl crabber::extension::Extension for PythonRepl {
    fn id(&self) -> &str {
        "crabber-extensions/python-repl"
    }
    fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }
    fn config_hash(&self) -> String {
        self.hash.clone()
    }
    async fn install(
        &self,
        registrar: &mut crabber::extension::Registrar,
    ) -> Result<(), ExtensionError> {
        use crabber::{
            core::ToolInfo,
            extension::{ToolDefinition, ToolExecutor},
        };
        use serde_json::json;
        let tools: [(_, _, _, _, Arc<dyn ToolExecutor>); 2] = [
            (
                EXECUTE_TOOL,
                "Execute Python in state scoped to the durable session and workspace. Python has host-user authority and results are bounded. Child-process output is discarded unless captured, for example with subprocess.run(capture_output=True).",
                json!({"type":"object","additionalProperties":false,"required":["code"],"properties":{"code":{"type":"string"},"timeout_seconds":{"type":"integer","minimum":0,"maximum":self.manager.configuration.limits.max_timeout.as_secs(),"description":"Whole seconds; 0 or omitted uses the host default. Queue wait and runner startup are additional."}}}),
                PERMISSION_EXECUTE,
                Arc::new(tools::ExecuteTool(self.manager.clone())),
            ),
            (
                CLEAR_TOOL,
                "Discard live Python interpreter state for the durable session and workspace without recreating it.",
                json!({"type":"object","additionalProperties":false,"properties":{}}),
                PERMISSION_MANAGE,
                Arc::new(tools::ClearTool(self.manager.clone())),
            ),
        ];
        for (name, description, parameters, permission, executor) in tools {
            registrar.tool(Arc::new(ToolDefinition {
                info: ToolInfo {
                    name: name.into(),
                    description: description.into(),
                    parameters,
                    retry_safe: false,
                    required_permissions: vec![permission.into()],
                },
                executor,
            }));
        }
        Ok(())
    }
    async fn shutdown(&self) {
        let bound = self.manager.configuration.limits.shutdown_grace;
        let _ = self.manager.close(bound).await;
        let _ = self.manager.cleanup.join(bound).await;
    }
}
