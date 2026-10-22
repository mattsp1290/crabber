//! Bounded delegation to a trusted host runner.
//!
//! This native extension creates no child agents, sandbox, providers or
//! credentials. Hosts own provisioning, policy and resource limits. Input and
//! output travel through the durable tool-call path; hosts needing redaction
//! should mount the tool result redactor. Side effects are not transactional.
use crabber::JsonToolExecutor;
mod attribution;
mod background;
mod factory;
pub use crate::contextual::{OwnerReport, SharedCapacity};
pub use factory::Factory;
mod input;
mod runner;
pub use runner::{Request, Response, Runner};

use crate::safe_future::SafeFuture;
use async_trait::async_trait;
use crabber::{
    core::{SessionId, ToolInfo},
    extension::{CleanupOwner, Extension, ExtensionError, Registrar, ToolContext, ToolDefinition},
};
use serde::Serialize;
use serde_json::{Value, json};
use std::{
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Semaphore, watch};
use tokio_util::sync::CancellationToken;

/// Model-visible tool name.
pub const TOOL_NAME: &str = "delegate_task";
/// Permission metadata; the host permission policy decides access.
pub const PERMISSION_DELEGATE: &str = "session.subagent";
/// Hard byte cap for an opaque ASCII profile identifier.
pub const MAX_PROFILE_BYTES: usize = 256;
/// Start a background task.
pub const START_TOOL: &str = "delegate_task_start";
/// Read a retained task.
pub const STATUS_TOOL: &str = "delegate_task_status";
/// List owned tasks.
pub const LIST_TOOL: &str = "delegate_task_list";
/// Wait for named tasks.
pub const WAIT_TOOL: &str = "delegate_task_wait";
/// Permission metadata for background reads.
pub const PERMISSION_DELEGATE_READ: &str = "session.subagent.read";
const TASK_ID_PREFIX: &str = "task_";
const TASK_ID_EPOCH_BYTES: usize = 32;
const TASK_ID_COUNTER_BYTES: usize = 16;
const TASK_ID_SEPARATOR: usize = TASK_ID_PREFIX.len() + TASK_ID_EPOCH_BYTES;
/// Fixed byte length of a process-local background task ID.
pub const TASK_ID_BYTES: usize = TASK_ID_SEPARATOR + 1 + TASK_ID_COUNTER_BYTES;
const MAX_TASK_BYTES: usize = 64 * 1024;
const MAX_RESULT_BYTES: usize = 256 * 1024;
const MAX_IN_FLIGHT: usize = 256;
const MAX_WAIT: Duration = Duration::from_secs(60 * 60);
const MAX_TRACKED: usize = 256;
const MAX_RECONCILIATIONS: usize = 64;
const MAX_BACKGROUND_DURATION: Duration = Duration::from_secs(24 * 3600);
const MAX_WAIT_IDS: usize = 64;
const MAX_WAIT_REPLY_BYTES: usize = 320 * 1024;
const MAX_SHUTDOWN_GRACE: Duration = Duration::from_secs(5 * 60);

/// Finite bounds shared by all mounts of one instance.
///
/// Results are bounded per call before JSON escaping. `max_in_flight` bounds
/// simultaneous live runners, not results accumulated in a turn or conversation.
/// Completed calls release slots for further calls in the same turn. Hosts size
/// result bounds together with their tool-call and context budgets.
#[derive(Clone, Serialize)]
pub struct Limits {
    /// Maximum UTF-8 bytes in the task (up to 64 KiB).
    pub max_task_bytes: usize,
    /// Maximum bytes in the profile (up to 256).
    pub max_profile_bytes: usize,
    /// Maximum UTF-8 output bytes (up to 256 KiB), before JSON escaping.
    pub max_result_bytes: usize,
    /// Maximum live runner futures (up to 256).
    pub max_in_flight: usize,
    /// Inline deadline and default/max read wait (one second to one hour).
    /// Explicit read timeouts use whole seconds up to floor(max_wait); omitted
    /// timeouts preserve the full configured duration, including fractions.
    pub max_wait: Duration,
    /// Maximum shutdown join time (up to five minutes).
    pub shutdown_grace: Duration,
    /// Retained records per instance (up to 256).
    pub max_tracked: usize,
    /// Background runner deadline (up to 24 hours).
    pub max_background_duration: Duration,
    /// IDs per wait (up to 64).
    pub max_wait_ids: usize,
    /// Output bytes per wait before escaping (up to 320 KiB). Outputs larger
    /// than this are always omitted by wait and must be read with status.
    pub max_wait_reply_bytes: usize,
}

impl Limits {
    /// Saturating raw-output bound for tracked records, registration reservations
    /// and live lifecycles (including evicted records awaiting final publication).
    /// Excludes record metadata and evicted records pinned by in-flight waits.
    pub fn worst_case_retained_bytes(&self) -> usize {
        self.max_tracked
            .saturating_add(self.max_tracked.min(MAX_RECONCILIATIONS))
            .saturating_add(self.max_in_flight)
            .saturating_mul(self.max_result_bytes)
    }

    /// Saturating bound for JSON-escaped wait output and row envelopes.
    pub fn worst_case_wait_bytes(&self) -> usize {
        self.max_wait_reply_bytes
            .saturating_mul(6)
            .saturating_add(self.max_wait_ids.saturating_mul(160))
            .saturating_add(64)
    }
}

/// Construction options for [`DelegateTask`].
pub struct Options {
    /// Bounded, non-secret identity of host runner behavior.
    pub runner_identity: String,
    /// Host-owned runner callback.
    pub runner: Runner,
    /// Required finite request, response and lifecycle limits.
    pub limits: Limits,
}

/// Bounded native delegation with shared or factory-scoped policy.
///
/// Inline runners remain extension-owned. Shutdown waits at most shutdown_grace
/// for them, while stragglers retain their permits until they exit. Interrupt
/// active inline calls before close; their durable native outcome can remain
/// uncertain even after local workers exit.
///
/// Background runners are runtime-owned independent session invocations. Any
/// shared mount's shutdown permanently refuses starts, settles running records
/// cancelled and signals their tokens. It does not join runtime-owned jobs:
/// hosts use Agent::drain_session_invocations to observe actual joins. A terminal
/// row or elapsed close deadline does not prove resource exit. Reads and inline
/// calls remain callable, so hosts stop ingress before closing.
///
/// Records and unresolved-registration diagnostics are process-local. Fresh
/// controllers cannot infer durable owner completion from an empty local drain.
/// Use Factory for owner isolation; dropping an instance alone is not close.
pub struct DelegateTask {
    policy: Arc<Policy>,
    hash: String,
}
struct Configuration {
    runner: Runner,
    limits: Limits,
}
struct Policy {
    hash: String,
    reconciliation: Arc<Semaphore>,
    publication_failures: Arc<AtomicU64>,
    binding: Option<crate::contextual::Binding>,
    shared: Option<SharedCapacity>,
    runner: Runner,
    limits: Limits,
    capacity: Arc<Semaphore>,
    cleanup: CleanupOwner,
    tasks: Mutex<background::Tasks>,
    settled: watch::Sender<u64>,
}
impl DelegateTask {
    /// Shared capacity currently held by inline or background runners.
    /// Includes detached runners until their futures exit. This is a snapshot;
    /// concurrent starts and completions may change it immediately.
    pub fn live_runners(&self) -> usize {
        self.policy.limits.max_in_flight - self.policy.capacity.available_permits()
    }

    /// Validates all bounds and the runner identity before construction.
    pub fn new(options: Options) -> Result<Self, ExtensionError> {
        let limits = &options.limits;
        if limits.max_task_bytes == 0
            || limits.max_task_bytes > MAX_TASK_BYTES
            || limits.max_profile_bytes == 0
            || limits.max_profile_bytes > MAX_PROFILE_BYTES
            || limits.max_result_bytes == 0
            || limits.max_result_bytes > MAX_RESULT_BYTES
            || limits.max_in_flight == 0
            || limits.max_in_flight > MAX_IN_FLIGHT
            || limits.max_wait < Duration::from_secs(1)
            || limits.max_wait > MAX_WAIT
            || limits.max_tracked == 0
            || limits.max_tracked > MAX_TRACKED
            || limits.max_background_duration.is_zero()
            || limits.max_background_duration > MAX_BACKGROUND_DURATION
            || limits.max_wait_ids == 0
            || limits.max_wait_ids > MAX_WAIT_IDS
            || limits.max_wait_reply_bytes == 0
            || limits.max_wait_reply_bytes > MAX_WAIT_REPLY_BYTES
            || limits.shutdown_grace.is_zero()
            || limits.shutdown_grace > MAX_SHUTDOWN_GRACE
        {
            return Err(crate::config_error("delegate-task-limits"));
        }
        if !crate::valid_identity(&options.runner_identity) {
            return Err(crate::config_error("delegate-task-policy"));
        }
        let hash = crate::config_hash(&(
            "delegate-task-v3",
            background::COMPONENT_ID,
            attribution::ACTIVITY_TYPE,
            [TOOL_NAME, START_TOOL, STATUS_TOOL, LIST_TOOL, WAIT_TOOL],
            [PERMISSION_DELEGATE, PERMISSION_DELEGATE_READ],
            &options.runner_identity,
            limits,
        ));
        Ok(Self::from_configuration(
            Arc::new(Configuration {
                runner: options.runner,
                limits: options.limits,
            }),
            None,
            None,
            hash,
        ))
    }
    fn from_configuration(
        configuration: Arc<Configuration>,
        binding: Option<crate::contextual::Binding>,
        shared: Option<SharedCapacity>,
        hash: String,
    ) -> Self {
        Self {
            policy: Arc::new(Policy {
                hash: hash.clone(),
                reconciliation: Arc::new(Semaphore::new(MAX_RECONCILIATIONS)),
                publication_failures: Arc::new(AtomicU64::new(0)),
                runner: configuration.runner.clone(),
                limits: configuration.limits.clone(),
                capacity: Arc::new(Semaphore::new(configuration.limits.max_in_flight)),
                binding,
                shared,
                cleanup: CleanupOwner::new(),
                tasks: Mutex::new(background::Tasks::new()),
                settled: watch::channel(0).0,
            }),
            hash,
        }
    }
    /// Observe retained records and live runner capacity without private payloads.
    pub fn owner_report(&self) -> OwnerReport {
        self.policy.owner_report()
    }
}
fn failure(code: &'static str) -> ExtensionError {
    ExtensionError::Tool(format!("delegate task failed: {code}"))
}
fn invalid(code: &'static str) -> ExtensionError {
    ExtensionError::Tool(format!("delegate task input invalid: {code}"))
}

struct InlineExecutor(Arc<Policy>);
#[async_trait]
impl JsonToolExecutor for InlineExecutor {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        Err(failure("context"))
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        arguments: Value,
    ) -> Result<Value, ExtensionError> {
        self.0.run_inline(context, arguments).await
    }
}
impl Policy {
    fn owner_report(&self) -> OwnerReport {
        let (tracked, closing) = self.tasks.lock().unwrap().counts();
        let live = self.limits.max_in_flight - self.capacity.available_permits();
        OwnerReport {
            live,
            tracked,
            closing,
            publication_failures: self.publication_failures.load(Ordering::Relaxed),
            unresolved_invocations: self.tasks.lock().unwrap().unresolved_count(),
            closed: closing && live == 0 && self.cleanup.tracker().pending() == 0,
            ..Default::default()
        }
    }
    async fn run_inline(
        self: &Arc<Self>,
        context: ToolContext,
        arguments: Value,
    ) -> Result<Value, ExtensionError> {
        if context.cancel.is_cancelled() {
            return Err(failure("cancelled"));
        }
        if context.workspace().workspace_id().is_none()
            || self
                .binding
                .as_ref()
                .is_some_and(|binding| !binding.matches(&context))
        {
            return Err(failure("owner"));
        }
        let (task, profile) = input::normalize(arguments, &self.limits)?;
        let shared_permit = match self.shared.as_ref().map(|pool| pool.acquire()).transpose() {
            Ok(permit) => permit,
            Err(()) => return Ok(json!({"status":"unavailable"})),
        };
        let permit = match self.capacity.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => return Ok(json!({"status":"unavailable"})),
        };
        let attribution = attribution::Attribution::new(
            &context,
            None,
            context.public_source().clone(),
            self.publication_failures.clone(),
        );
        attribution.start("inline", &profile).await;
        let settlement = attribution.settlement(&context);
        let token = context.cancel.child_token();
        let _guard = token.clone().drop_guard();
        let request = Request::new(
            &context,
            (task, profile),
            token.clone(),
            self.limits.max_wait,
            false,
        );
        let deadline = tokio::time::sleep(self.limits.max_wait);
        tokio::pin!(deadline);
        let runner = self.runner.clone();
        let policy = self.clone();
        let mut task = self.cleanup.tracker().spawn(async move {
            let _permit = permit;
            let _shared_permit = shared_permit;
            let _policy = policy;
            guarded(&runner, request)?.wait().await
        });
        let result = tokio::select! {
            biased;
            _ = context.cancel.cancelled() => Err(failure("cancelled")),
            _ = &mut deadline => Ok(json!({"status":"timed_out"})),
            reply = &mut task => match reply {
                Ok(Ok(response)) => map_reply(response, &self.limits),
                _ => Err(failure("runner")),
            },
        };
        let status = match &result {
            Ok(value) => match value["status"].as_str() {
                Some("completed") => "completed",
                Some("failed") => "failed",
                Some("rejected") => "rejected",
                Some("unavailable") => "unavailable",
                Some("timed_out") => "timed_out",
                _ => "error",
            },
            Err(_) if context.cancel.is_cancelled() => "cancelled",
            Err(_) => "error",
        };
        token.cancel();
        settlement.finish(status).await;
        result
    }
}

fn guarded(runner: &Runner, request: Request) -> Result<SafeFuture<Response>, ExtensionError> {
    let host = catch_unwind(AssertUnwindSafe(|| runner(request))).map_err(|_| failure("runner"))?;
    Ok(SafeFuture::new(host, || failure("runner")))
}
fn map_reply(response: Response, limits: &Limits) -> Result<Value, ExtensionError> {
    let response = response.checked(limits)?;
    let (status, _) = response.status_output();
    let mut value = json!({"status":status});
    // Move the output into JSON; status and validation stay canonical.
    let (Response::Completed(output)
    | Response::Failed(output)
    | Response::Rejected(output)
    | Response::Unavailable(output)) = response;
    if !output.is_empty() {
        value["output"] = Value::String(output);
    }
    Ok(value)
}
fn task_schema() -> Value {
    json!({"type":"object","additionalProperties":false,"required":["task","profile"],"properties":{"task":{"type":"string"},"profile":{"type":"string"}}})
}
#[async_trait]
impl Extension for DelegateTask {
    fn id(&self) -> &str {
        "crabber-extensions/delegate-task"
    }
    fn version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }
    fn config_hash(&self) -> String {
        self.hash.clone()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        registrar.tool(Arc::new(ToolDefinition {
            info: ToolInfo { name: TOOL_NAME.into(), description: "Delegate one bounded task under an opaque host profile. Returns completed, failed, rejected, unavailable, or timed_out.".into(), parameters: task_schema(), retry_safe: false, required_permissions: vec![PERMISSION_DELEGATE.into()] }, executor: Arc::new(InlineExecutor(self.policy.clone())),
        }));
        background::install(&self.policy, registrar);
        Ok(())
    }
    async fn shutdown(&self) {
        self.policy.close_tasks();
        let _ = self
            .policy
            .cleanup
            .join(self.policy.limits.shutdown_grace)
            .await;
    }
}
