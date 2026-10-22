//! Bounded, non-interactive jobs owned by a durable session and workspace.
//!
//! Hosts provision the POSIX shell and own process policy, credentials and trust.
//! Working-directory checks constrain launch only; this is not a sandbox.
//! Jobs and output live in memory and do not survive a host restart. Crabber may
//! execute a pending start once when resuming a paused run; Running calls are
//! interrupted on recovery. Hosts settle runs before changing fingerprinted policy.

mod config;
mod factory;
pub use crate::contextual::{OwnerReport, SharedCapacity};
pub use factory::Factory;
mod input;
mod job;
mod manager;
mod registry;
#[cfg(all(test, unix))]
mod tests;
mod time;
mod tools;

pub use config::{Environment, EnvironmentMode, Limits, Options};
use crabber::extension::ExtensionError;
use manager::Policy;
use std::sync::Arc;

/// Tool for starting a bounded command.
pub const START_TOOL: &str = "background_job_start";
/// Tool for reading state and bounded output tails.
pub const STATUS_TOOL: &str = "background_job_status";
/// Tool for listing the caller's tracked jobs.
pub const LIST_TOOL: &str = "background_job_list";
/// Tool for group-wide termination.
pub const KILL_TOOL: &str = "background_job_kill";
/// Start permission metadata; the host decides access.
pub const PERMISSION_START: &str = "background.process.start";
/// Status/list permission metadata; the host decides access.
pub const PERMISSION_READ: &str = "background.process.read";
/// Kill permission metadata; the host decides access.
pub const PERMISSION_KILL: &str = "background.process.kill";

/// Trusted process manager with shared capacity across mounts.
///
/// Use a Tokio runtime with `enable_all()`: process support needs the I/O driver
/// and installs a process-wide SIGCHLD handler on first use. Closing any mount
/// closes all jobs of this instance, so use separate instances for independent
/// registries/tenants. Interrupt active runs before closing mounts. A close is
/// bounded; inspect [`Self::live_jobs`] for survivors. No shell discovery,
/// provisioning, sandbox or durable process recovery is provided.
pub struct BackgroundJobs {
    policy: Arc<Policy>,
    hash: String,
}
impl BackgroundJobs {
    /// Validate bounds and identities, canonicalize the executable, and freeze
    /// the effective environment. Non-Unix targets reject construction.
    ///
    /// Environment values do not enter the fingerprint: rotate the environment
    /// identity whenever effective values or host routing policy change.
    pub fn new(options: Options) -> Result<Self, ExtensionError> {
        let (configuration, hash) = config::validate(options)?;
        Ok(Self {
            policy: Arc::new(Policy::new(configuration)),
            hash,
        })
    }

    fn from_configuration(
        configuration: Arc<config::Configuration>,
        binding: crate::contextual::Binding,
        shared: Option<SharedCapacity>,
        hash: String,
    ) -> Self {
        let mut policy = Policy::new(configuration.as_ref().clone());
        policy.binding = Some(binding);
        policy.shared = shared;
        Self {
            policy: Arc::new(policy),
            hash,
        }
    }
    /// Observe local resources without exposing paths, output or process IDs.
    pub fn owner_report(&self) -> OwnerReport {
        self.policy.owner_report()
    }

    /// Diagnostic snapshot of running, starting and hidden jobs. Finished
    /// tracked jobs do not count. A bounded close may leave live survivors.
    pub fn live_jobs(&self) -> usize {
        self.policy.live_jobs()
    }
}

fn input_error(code: &'static str) -> ExtensionError {
    ExtensionError::Tool(format!("background jobs input invalid: {code}"))
}
fn runtime_error(code: &'static str) -> ExtensionError {
    ExtensionError::Tool(format!("background jobs runtime invalid: {code}"))
}
fn failure(code: &'static str) -> ExtensionError {
    ExtensionError::Tool(format!("background jobs operation failed: {code}"))
}

#[async_trait::async_trait]
impl crabber::extension::Extension for BackgroundJobs {
    fn id(&self) -> &str {
        "crabber-extensions/background-jobs"
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
        use tools::{KillTool, ListTool, StartTool, StatusTool};
        let id_schema = json!({"type":"object","additionalProperties":false,"required":["id"],"properties":{"id":{"type":"string"}}});
        let tools: [(_, _, _, _, _, Arc<dyn ToolExecutor>); 4] = [
            (
                START_TOOL,
                "Start one bounded non-interactive shell command owned by this session/workspace. Output is retained as bounded tails.",
                json!({"type":"object","additionalProperties":false,"required":["command"],"properties":{"command":{"type":"string"},"working_directory":{"type":"string"},"timeout_seconds":{"type":"integer"}}}),
                false,
                PERMISSION_START,
                Arc::new(StartTool(self.policy.clone())),
            ),
            (
                STATUS_TOOL,
                "Read state and bounded output tails of a job owned by this session/workspace.",
                id_schema.clone(),
                true,
                PERMISSION_READ,
                Arc::new(StatusTool(self.policy.clone())),
            ),
            (
                LIST_TOOL,
                "List tracked jobs owned by this session/workspace in start order.",
                json!({"type":"object","additionalProperties":false,"properties":{}}),
                true,
                PERMISSION_READ,
                Arc::new(ListTool(self.policy.clone())),
            ),
            (
                KILL_TOOL,
                "Terminate the whole process group of a job owned by this session/workspace, with bounded escalation and reaping.",
                id_schema,
                false,
                PERMISSION_KILL,
                Arc::new(KillTool(self.policy.clone())),
            ),
        ];
        for (name, description, parameters, retry_safe, permission, executor) in tools {
            registrar.tool(Arc::new(ToolDefinition {
                info: ToolInfo {
                    name: name.into(),
                    description: description.into(),
                    parameters,
                    retry_safe,
                    required_permissions: vec![permission.into()],
                },
                executor,
            }));
        }
        Ok(())
    }
    async fn shutdown(&self) {
        let grace = self.policy.configuration.limits.shutdown_grace;
        let _ = self.policy.close(grace).await;
        let _ = self.policy.cleanup.join(grace).await;
    }
}
