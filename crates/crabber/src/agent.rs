#[path = "agent/close.rs"]
mod close;
pub use close::{HostCloseReport, HostExtensionClose};
#[path = "agent/frontend_continuation.rs"]
mod frontend_continuation;
#[path = "agent/tool_continuation.rs"]
mod tool_continuation;
pub use crabber_runtime::ContinueTool;
pub use crabber_runtime::{ContinueFrontend, RecoverFrontend};
pub use tool_continuation::ToolContinuation;
#[path = "agent/approval.rs"]
mod approval;

mod attempt_control;
mod observers;
use observers::AgentObservers;
mod management;
#[path = "agent/phase.rs"]
mod phase;
#[path = "agent/preparation.rs"]
mod preparation;
mod public_session;
mod run_control;
mod session_owner;
use crate::ExecutionContext;
pub use approval::Continuation;
use async_trait::async_trait;
#[cfg(test)]
use crabber_core::EventRecord;
use crabber_core::{
    AdmissionKey, AdmissionOptions, AdmissionReceipt, RunId, SessionId, TraceContext,
};
use crabber_extension::{
    ContextualPlanProvider, Extension, ExtensionError, ExtensionFactory, PromptSection, Registrar,
    Registry, Scope, StaticPlanProvider, ToolDefinition, WorkspaceReaderResolver,
};
use crabber_providers::{Resolver, Selection};
use crabber_runtime::{
    CompactionPolicy, ExecutionMode, Observer, Orchestrator, PermissionPolicy, Request, RunResult,
    RuntimeError,
};
pub use crabber_runtime::{ContinueApproval, RecoverApproval};
#[cfg(feature = "postgres")]
use crabber_session::PostgresStore;
#[cfg(feature = "sqlite")]
use crabber_session::SqliteStore;
use crabber_session::{MemoryStore, Store};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

/// Host-owned settings frozen into each prompt request.
#[derive(Debug, Clone)]
pub struct AgentConfig {
    pub selection: Selection,
    pub workspace_id: String,
    pub directory: String,
    pub title: String,
    pub system_prompt: Option<String>,
    pub max_output_tokens: Option<u32>,
}

impl AgentConfig {
    #[must_use]
    pub fn new(selection: Selection) -> Self {
        Self {
            selection,
            workspace_id: "default".into(),
            directory: ".".into(),
            title: "Crabber session".into(),
            system_prompt: None,
            max_output_tokens: None,
        }
    }
    /// Caps output tokens for every model turn; model-stream around hooks may override it.
    #[must_use]
    pub fn max_output_tokens(mut self, cap: u32) -> Self {
        self.max_output_tokens = Some(cap);
        self
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("an agent needs a provider resolver")]
    NoProvider,
    #[error("an agent needs a configuration")]
    NoConfig,
    #[error("invalid agent configuration: {0}")]
    InvalidConfig(&'static str),
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
}

pub struct AgentBuilder {
    behavior_fingerprint: Option<crabber_core::InputFingerprint>,
    observers: Vec<Arc<dyn Observer>>,
    monotonic_clock: Option<Arc<dyn crabber_runtime::MonotonicClock>>,
    store: Option<Arc<dyn Store>>,
    resolver: Option<Arc<dyn Resolver>>,
    workspace_reader_resolver: Option<Arc<dyn WorkspaceReaderResolver>>,
    config: Option<AgentConfig>,
    tools: Vec<Arc<ToolDefinition>>,
    prompts: Vec<Arc<PromptSection>>,
    policy: Option<Arc<dyn PermissionPolicy>>,
    #[cfg(feature = "datadog")]
    datadog: Option<crabber_obs::DatadogConfig>,
    #[cfg(feature = "datadog")]
    datadog_observer: Option<crabber_obs::DatadogObserver>,
    extensions: Vec<(Arc<dyn Extension>, Scope)>,
    factories: Vec<Arc<dyn ExtensionFactory>>,
    initialization_timeout: Duration,
    extension_close_timeout: Duration,
    execution_mode: ExecutionMode,
    compaction: CompactionPolicy,
}

impl AgentBuilder {
    /// Declare the nonsecret version of native callbacks/policy for this host.
    #[must_use]
    pub fn behavior_fingerprint(mut self, fingerprint: crabber_core::InputFingerprint) -> Self {
        self.behavior_fingerprint = Some(fingerprint);
        self
    }
    /// Hash an explicit host behavior version; credentials are excluded.
    /// # Panics
    /// Panics only if a library SHA-256 digest ceases to be a valid fingerprint.
    #[must_use]
    pub fn behavior_version(self, version: &str) -> Self {
        let fingerprint =
            crabber_core::InputFingerprint::new(crabber_session::admission_config_hash(
                &serde_json::json!(["crabber.host-behavior.v1", version]),
            ))
            .expect("SHA-256 is a valid fingerprint");
        self.behavior_fingerprint(fingerprint)
    }

    /// Injects execution timing independently of event/lease wall time.
    #[must_use]
    pub fn monotonic_clock(mut self, clock: Arc<dyn crabber_runtime::MonotonicClock>) -> Self {
        self.monotonic_clock = Some(clock);
        self
    }
    /// Adds a host observer alongside broadcasts and optional Datadog export.
    /// Each registered observer receives each callback once. Hosts own tracing setup.
    #[must_use]
    pub fn observer(mut self, observer: Arc<dyn Observer>) -> Self {
        self.observers.push(observer);
        self
    }
    /// Mount a SHA-256 verified Component Model extension shared by all contexts.
    /// Its mutable state must support concurrent use across sessions/workspaces.
    #[cfg(feature = "wasm")]
    #[must_use]
    pub fn shared_wasm_extension(self, config: crabber_wasm::ModuleConfig) -> Self {
        self.shared_extension(
            Arc::new(crabber_wasm::WasmExtension::new(config)),
            Scope::Global,
        )
    }
    /// Enables agentless Datadog export when `DD_API_KEY` is set.
    #[cfg(feature = "datadog")]
    #[must_use]
    pub fn datadog_from_env(mut self) -> Self {
        self.datadog = crabber_obs::DatadogConfig::from_env();
        self
    }
    /// Enables agentless Datadog export with explicit settings.
    #[cfg(feature = "datadog")]
    #[must_use]
    pub fn datadog(mut self, config: crabber_obs::DatadogConfig) -> Self {
        self.datadog = Some(config);
        self
    }
    /// Shares a host-owned exporter with this agent.
    /// Hosts using `Telemetry` should shut down through `Telemetry::shutdown`.
    #[cfg(feature = "datadog")]
    #[must_use]
    pub fn datadog_observer(mut self, observer: crabber_obs::DatadogObserver) -> Self {
        self.datadog_observer = Some(observer);
        self
    }
    /// Registers the real providers compiled into this binary, using their
    /// environment variables and the local `ChatGPT` credential store.
    #[cfg(any(
        feature = "anthropic",
        feature = "openai",
        feature = "codex",
        feature = "opencode-go"
    ))]
    #[must_use]
    pub fn providers_from_env(self) -> Self {
        self.provider(Arc::new(crabber_providers::HttpResolver::from_env()))
    }
    #[must_use]
    pub fn memory(mut self) -> Self {
        self.store = Some(Arc::new(MemoryStore::new()));
        self
    }

    /// Opens an explicitly migrated PostgreSQL store.
    /// # Errors
    /// Returns a sanitized connection or schema error.
    #[cfg(feature = "postgres")]
    pub async fn postgres(mut self, url: &str) -> Result<Self, crabber_session::StoreError> {
        self.store = Some(Arc::new(PostgresStore::connect(url).await?));
        Ok(self)
    }

    /// Opens an explicitly migrated SQLite store.
    /// # Errors
    /// Returns a sanitized connection or schema error.
    #[cfg(feature = "sqlite")]
    pub async fn sqlite(
        mut self,
        path: impl AsRef<std::path::Path>,
    ) -> Result<Self, crabber_session::StoreError> {
        self.store = Some(Arc::new(SqliteStore::connect(path).await?));
        Ok(self)
    }

    #[must_use]
    pub fn store(mut self, store: Arc<dyn Store>) -> Self {
        self.store = Some(store);
        self
    }

    #[must_use]
    pub fn provider(mut self, provider: Arc<dyn Resolver>) -> Self {
        self.resolver = Some(provider);
        self
    }

    /// Grants typed model middleware host-authorized, per-workspace readers.
    #[must_use]
    pub fn workspace_reader_resolver(mut self, resolver: Arc<dyn WorkspaceReaderResolver>) -> Self {
        self.workspace_reader_resolver = Some(resolver);
        self
    }

    #[must_use]
    pub fn config(mut self, config: AgentConfig) -> Self {
        self.config = Some(config);
        self
    }

    #[must_use]
    /// Register a tool whose captured state is safe to share across all contexts.
    /// Use a contextual factory when tool state belongs to one workspace/session.
    pub fn shared_tool(mut self, tool: Arc<ToolDefinition>) -> Self {
        self.tools.push(tool);
        self
    }

    #[must_use]
    pub fn prompt_section(mut self, section: Arc<PromptSection>) -> Self {
        self.prompts.push(section);
        self
    }

    #[must_use]
    pub fn policy(mut self, policy: Arc<dyn PermissionPolicy>) -> Self {
        self.policy = Some(policy);
        self
    }

    #[must_use]
    pub fn execution_mode(mut self, mode: ExecutionMode) -> Self {
        self.execution_mode = mode;
        self
    }

    #[must_use]
    pub fn compaction(mut self, policy: CompactionPolicy) -> Self {
        self.compaction = policy;
        self
    }

    #[must_use]
    /// Register one shared instance. Scope filters visibility; it does not isolate
    /// mutable state. The instance must support concurrent use in every context
    /// admitted by that scope. Use a factory for independently owned state.
    pub fn shared_extension(mut self, extension: Arc<dyn Extension>, scope: Scope) -> Self {
        self.extensions.push((extension, scope));
        self
    }

    /// Instantiate a fresh native extension/tool/service graph per declared owner.
    #[must_use]
    pub fn extension_factory(mut self, factory: Arc<dyn ExtensionFactory>) -> Self {
        self.factories.push(factory);
        self
    }

    /// Register explicitly shareable host capabilities with stable nonsecret
    /// behavior/configuration identity. Scoped factories can override capabilities.
    #[must_use]
    pub fn shared_host_services(
        self,
        identity: crabber_extension::ComponentIdentity,
        config_hash: crabber_core::InputFingerprint,
        services: crabber_extension::HostServices,
    ) -> Self {
        self.shared_extension(
            Arc::new(crate::host::SharedServices {
                identity,
                config_hash,
                services,
            }),
            Scope::Global,
        )
    }

    /// Bound the waiter for owned initialization. Timeout retains initialization;
    /// it grants no execution plan and a later readiness/retry can reconcile it.
    #[must_use]
    pub fn initialization_timeout(mut self, bound: Duration) -> Self {
        self.initialization_timeout = bound;
        self
    }

    /// Bounds each mount's wait for run leases and extension-owned cleanup.
    #[must_use]
    pub fn extension_close_timeout(mut self, bound: Duration) -> Self {
        self.extension_close_timeout = bound;
        self
    }

    /// Builds the embeddable agent.
    ///
    /// # Errors
    ///
    /// Returns an error if the provider, config, or runtime dependencies are invalid.
    pub fn build(self) -> Result<Agent, BuildError> {
        let resolver = self.resolver.ok_or(BuildError::NoProvider)?;
        let config = self.config.ok_or(BuildError::NoConfig)?;
        if config.max_output_tokens == Some(0) {
            return Err(BuildError::InvalidConfig(
                "max_output_tokens must be positive",
            ));
        }
        #[cfg(feature = "datadog")]
        let datadog = self
            .datadog_observer
            .or_else(|| self.datadog.as_ref().map(crabber_obs::DatadogObserver::new));
        let configured_observer = !self.observers.is_empty();
        #[cfg(feature = "datadog")]
        let configured_observer = configured_observer || datadog.is_some();
        let observer = Arc::new(AgentObservers {
            observers: self.observers,
            #[cfg(feature = "datadog")]
            datadog: datadog.clone(),
        });
        let extensions = self.extensions;
        let has_extensions = !extensions.is_empty() || !self.factories.is_empty();
        let registry = has_extensions.then(|| {
            let observer = Arc::clone(&observer);
            Registry::new()
                .with_close_timeout(self.extension_close_timeout)
                .with_close_observer(Arc::new(move |timeout| {
                    observer.mount_close_timed_out(timeout);
                }))
        });
        let mut mounts = extensions;
        if has_extensions && (!self.tools.is_empty() || !self.prompts.is_empty()) {
            mounts.insert(
                0,
                (
                    Arc::new(BuiltinExtension {
                        tools: self.tools.clone(),
                        prompts: self.prompts.clone(),
                    }),
                    Scope::Global,
                ),
            );
        }
        let contextual = registry
            .as_ref()
            .map(|registry| {
                ContextualPlanProvider::new(
                    registry.clone(),
                    mounts,
                    self.factories,
                    self.initialization_timeout,
                )
            })
            .transpose()
            .map_err(RuntimeError::ExtensionPlan)?
            .map(Arc::new);
        let plan_provider: Arc<dyn crabber_extension::RunPlanProvider> =
            if let Some(provider) = &contextual {
                provider.clone()
            } else {
                Arc::new(StaticPlanProvider::new(self.tools, self.prompts))
            };
        let mut runtime = Orchestrator::builder()
            .store(self.store.unwrap_or_else(|| Arc::new(MemoryStore::new())))
            .resolver(resolver)
            .plan_provider(plan_provider)
            .execution_mode(self.execution_mode)
            .compaction(self.compaction);
        if let Some(provider) = &contextual {
            runtime = runtime.contextual_plan_provider(provider.clone());
        }
        if configured_observer {
            runtime = runtime.observer(observer);
        }
        if let Some(behavior) = self.behavior_fingerprint {
            runtime = runtime.behavior_fingerprint(behavior);
        }
        if let Some(workspace_reader_resolver) = self.workspace_reader_resolver {
            runtime = runtime.workspace_reader_resolver(workspace_reader_resolver);
        }
        if let Some(clock) = self.monotonic_clock {
            runtime = runtime.monotonic_clock(clock);
        }
        if let Some(policy) = self.policy {
            runtime = runtime.policy(policy);
        }
        Ok(Agent {
            runtime: runtime.build()?,
            config,
            #[cfg(feature = "datadog")]
            datadog,
            #[cfg(test)]
            registry,
            contextual,
            extension_close_timeout: self.extension_close_timeout,
            closed: AtomicBool::new(false),
            close_observer: close::CloseCoordinator::default(),
        })
    }
}

/// A configured agent ready to prompt a new or existing session.
pub struct Agent {
    runtime: Orchestrator,
    config: AgentConfig,
    #[cfg(feature = "datadog")]
    datadog: Option<crabber_obs::DatadogObserver>,
    #[cfg(test)]
    registry: Option<Registry>,
    pub(crate) contextual: Option<Arc<ContextualPlanProvider>>,
    extension_close_timeout: Duration,
    closed: AtomicBool,
    close_observer: close::CloseCoordinator,
}

impl Agent {
    /// Declared native behavior identity to retain with keyed requests.
    #[must_use]
    pub fn behavior_fingerprint(&self) -> &crabber_core::InputFingerprint {
        self.runtime.behavior_fingerprint()
    }

    pub(crate) async fn initialize_extensions(&self) -> Result<(), RuntimeError> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(RuntimeError::HostClosed);
        }
        self.runtime
            .initialize_extensions()
            .await
            .map_err(|error| match error {
                RuntimeError::ExtensionPlan(ExtensionError::RegistryClosed) => {
                    RuntimeError::HostClosed
                }
                error => error,
            })
    }

    /// Await retained initial, keyed-recovery and resume worker joins within a deadline.
    /// Dropping a run handle detaches its result subscription; it does not
    /// cancel the worker. Stop ingress before draining. This does not cancel
    /// work or certify external resource cleanup or durable settlement. Inspect
    /// `pending`, `pending_preparations`, tool-task `resources`, `background_tasks`, and durable state.
    pub async fn drain_execution_tasks(
        &self,
        bound: Duration,
    ) -> crabber_runtime::ExecutionTaskDrain {
        self.runtime.drain_execution_tasks(bound).await
    }

    /// Join retained local work for one session within a shared deadline.
    /// Stop that session's ingress first. This does not cancel work, close
    /// extensions, or certify durable cleanup across other controllers.
    pub async fn drain_session_execution_tasks(
        &self,
        session: &SessionId,
        bound: Duration,
    ) -> crabber_runtime::ExecutionTaskDrain {
        self.runtime
            .drain_session_execution_tasks(session, bound)
            .await
    }

    /// Observe or cooperatively cancel independent jobs for exactly one session.
    /// Pending or uncertain owners remain retained after the deadline.
    pub async fn drain_session_invocations(
        &self,
        session: &SessionId,
        cancel: bool,
        bound: Duration,
    ) -> Vec<crabber_runtime::SessionInvocationReport> {
        self.runtime
            .drain_session_invocations(session, cancel, bound)
            .await
    }

    /// Terminally closes the extension registry without interrupting live runs.
    /// Hosts should interrupt runs first. On timeout, extension-owned cleanup and
    /// retained plan leases keep the detached close task waiting until completion.
    /// New admissions and readiness are rejected once closing starts, including
    /// for agents with no extensions. This does not join active executions.
    ///
    /// # Errors
    /// Rejects contextual self-close before stopping intake, or reports retained
    /// cleanup and the first observed mount error under the configured bound.
    pub async fn close_extensions(&self) -> Result<(), ExtensionError> {
        let bound = self.extension_close_timeout.saturating_mul(2);
        let deadline = tokio::time::Instant::now().checked_add(bound);
        // Preserve a rejected contextual self-close before changing intake.
        let observation = self
            .contextual
            .as_ref()
            .map(|provider| provider.begin_close_all(bound))
            .transpose()?;
        self.closed.store(true, Ordering::SeqCst);
        self.runtime.seal_ingress();
        match (observation, deadline) {
            (None, _) => Ok(()),
            (Some(observation), Some(deadline)) => observation.observe_until(deadline).await,
            (Some(_), None) => Err(ExtensionError::ContextCleanupIncomplete),
        }
    }

    /// Returns local export health, or `None` when Datadog is disabled.
    #[cfg(feature = "datadog")]
    #[must_use]
    pub fn export_health(&self) -> Option<crabber_obs::ExportHealth> {
        self.datadog
            .as_ref()
            .map(crabber_obs::DatadogObserver::health)
    }
    /// Waits for queued Datadog exports, if enabled.
    #[cfg(feature = "datadog")]
    /// # Errors
    /// Returns an intake or worker error.
    pub async fn flush(&self) -> Result<(), crabber_obs::ExportError> {
        if let Some(datadog) = &self.datadog {
            datadog.flush().await
        } else {
            Ok(())
        }
    }
    /// Stops the Datadog export worker after flushing, if enabled.
    /// For a shared telemetry exporter, prefer `Telemetry::shutdown`; this
    /// method preserves the raw `WorkerStopped` error after an earlier shutdown.
    #[cfg(feature = "datadog")]
    /// # Errors
    /// Returns an intake or worker error.
    pub async fn shutdown(&self) -> Result<(), crabber_obs::ExportError> {
        if let Some(datadog) = &self.datadog {
            datadog.shutdown().await
        } else {
            Ok(())
        }
    }
    #[must_use]
    pub fn builder() -> AgentBuilder {
        AgentBuilder {
            behavior_fingerprint: None,
            observers: Vec::new(),
            monotonic_clock: None,
            store: None,
            resolver: None,
            workspace_reader_resolver: None,
            config: None,
            tools: Vec::new(),
            prompts: Vec::new(),
            policy: None,
            #[cfg(feature = "datadog")]
            datadog: None,
            #[cfg(feature = "datadog")]
            datadog_observer: None,
            extensions: Vec::new(),
            factories: Vec::new(),
            initialization_timeout: crabber_extension::DEFAULT_MOUNT_CLOSE_TIMEOUT,
            extension_close_timeout: crabber_extension::DEFAULT_MOUNT_CLOSE_TIMEOUT,
            execution_mode: ExecutionMode::Sequential,
            compaction: CompactionPolicy::default(),
        }
    }

    /// Starts a prompt and returns a handle for events and completion.
    ///
    /// # Errors
    ///
    /// Returns admission, provider, or plan failures from the runtime.
    pub async fn prompt(
        &self,
        session_id: Option<SessionId>,
        text: impl Into<String>,
    ) -> Result<RunHandle, RuntimeError> {
        self.prompt_with_context(session_id, text, None).await
    }

    /// Starts a prompt with validated host correlation identity.
    /// # Errors
    /// Returns admission, provider or plan failures.
    pub async fn prompt_with_context(
        &self,
        session_id: Option<SessionId>,
        text: impl Into<String>,
        context: Option<TraceContext>,
    ) -> Result<RunHandle, RuntimeError> {
        let request = self.request(session_id, text.into());
        self.start_request(request, context).await
    }

    pub(crate) async fn start_request(
        &self,
        request: Request,
        context: Option<TraceContext>,
    ) -> Result<RunHandle, RuntimeError> {
        let inner = self.runtime.start_with_context(request, context).await?;
        Ok(RunHandle::from_runtime(inner))
    }

    /// Starts one keyed prompt; identical retries return metadata only.
    /// A supplied session ID can create its first session atomically.
    /// # Errors
    /// Returns semantic conflict, identity mismatch, Busy, or runtime/store errors.
    pub async fn prompt_keyed(
        &self,
        session_id: SessionId,
        text: impl Into<String>,
        options: AdmissionOptions,
    ) -> Result<Admission, RuntimeError> {
        self.prompt_keyed_with_context(session_id, text, options, None)
            .await
    }

    /// Keyed prompt with transport metadata. Retries do not replace the winner's context.
    /// # Errors
    /// Returns semantic conflict, identity mismatch, Busy or runtime/store errors.
    pub async fn prompt_keyed_with_context(
        &self,
        session_id: SessionId,
        text: impl Into<String>,
        options: AdmissionOptions,
        context: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        self.start_keyed_request(
            self.request(Some(session_id), text.into()),
            options,
            context,
        )
        .await
    }

    pub(crate) async fn start_keyed_request(
        &self,
        request: Request,
        options: AdmissionOptions,
        context: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        Ok(Admission::from_runtime(
            self.runtime
                .start_keyed_with_context(request, options, context)
                .await?,
        ))
    }

    /// Complete a retained Unstarted admission after its owner expires. Restore
    /// the original request/config/behavior first; a replay grants no authority.
    /// # Errors
    /// Returns typed eligibility denials, semantic conflicts and runtime errors.
    pub async fn recover_admission(
        &self,
        session_id: SessionId,
        text: impl Into<String>,
        options: AdmissionOptions,
    ) -> Result<Admission, RuntimeError> {
        self.recover_admission_with_context(session_id, text, options, None)
            .await
    }
    /// Recover using fresh transport metadata.
    /// # Errors
    /// Returns the same errors as `recover_admission`.
    pub async fn recover_admission_with_context(
        &self,
        session_id: SessionId,
        text: impl Into<String>,
        options: AdmissionOptions,
        context: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        self.recover_request(
            self.request(Some(session_id), text.into()),
            options,
            context,
        )
        .await
    }

    pub(crate) async fn recover_request(
        &self,
        request: Request,
        options: AdmissionOptions,
        context: Option<TraceContext>,
    ) -> Result<Admission, RuntimeError> {
        Ok(Admission::from_runtime(
            self.runtime
                .recover_admission_with_context(request, options, context)
                .await?,
        ))
    }

    pub(crate) fn context_request(&self, context: &ExecutionContext, text: String) -> Request {
        self.request_at(
            Some(context.session().session_id().clone()),
            text,
            context.session().workspace_id(),
            context.session().directory(),
            context.selection(),
        )
    }

    fn request(&self, session_id: Option<SessionId>, text: String) -> Request {
        self.request_at(
            session_id,
            text,
            &self.config.workspace_id,
            &self.config.directory,
            &self.config.selection,
        )
    }

    fn request_at(
        &self,
        session_id: Option<SessionId>,
        text: String,
        workspace_id: &str,
        directory: &str,
        selection: &Selection,
    ) -> Request {
        Request {
            selection_policy: crabber_session::AdmissionSelectionPolicy::Explicit,
            captured_revision: None,
            input_envelope: None,
            session_id,
            text,
            workspace_id: workspace_id.to_owned(),
            directory: directory.to_owned(),
            title: self.config.title.clone(),
            selection: selection.clone(),
            system_prompt: self.config.system_prompt.clone(),
            max_output_tokens: self.config.max_output_tokens,
        }
    }

    /// Looks up a receipt without executing or claiming a run.
    /// # Errors
    /// Returns store errors. An absent receipt may still be in flight.
    pub async fn lookup_admission(
        &self,
        session: &SessionId,
        key: &AdmissionKey,
    ) -> Result<Option<AdmissionReceipt>, RuntimeError> {
        self.runtime.lookup_admission(session, key).await
    }

    /// Legacy-only settlement; native execution and durable approval barriers
    /// require explicit reconciliation controls and are refused without mutation.
    /// Durably interrupts abandoned work without mounting extensions or executing
    /// providers, tools or hooks. The host must verify process/coordinator death
    /// before asserting `HostStoppedOwner`; a fence alone is not that evidence.
    /// # Errors
    /// Returns typed eligibility, ownership or durable settlement failures.
    /// Retry an identical request to reconcile an unknown response.
    pub async fn abandon(
        &self,
        request: crabber_core::AbandonRequest,
    ) -> Result<crabber_core::AbandonOutcome, crabber_core::AbandonError> {
        self.runtime.abandon(request).await
    }

    pub fn interrupt(&self, run: &RunHandle) {
        run.interrupt();
    }

    /// Resumes a paused or expired run from its durable tool calls.
    ///
    /// # Errors
    ///
    /// Returns an error if the lease cannot be claimed or the plan changed.
    pub async fn resume(&self, run_id: &RunId) -> Result<RunResult, RuntimeError> {
        self.resume_with_context(run_id, None).await
    }

    /// Resumes with host-selected current attempt identity; lease checks are unchanged.
    /// # Errors
    /// Returns initialization, plan or lease errors.
    pub async fn resume_with_context(
        &self,
        run_id: &RunId,
        context: Option<TraceContext>,
    ) -> Result<RunResult, RuntimeError> {
        self.resume_execution_with_context(run_id, context)
            .await?
            .done()
            .await
    }

    /// Resume with the same bounded event/control/join handle as a fresh prompt.
    /// Eligibility and claim are performed by its owned worker; handle creation
    /// grants no execution authority. Dropping observation never cancels work.
    /// # Errors
    /// Returns initialization/identity errors; `done` reports claim and execution errors.
    pub async fn resume_execution(&self, run_id: &RunId) -> Result<RunHandle, RuntimeError> {
        self.resume_execution_with_context(run_id, None).await
    }
    /// Resume with explicit host correlation metadata and an owned stream.
    /// # Errors
    /// Returns the same failures as `resume_execution`.
    pub async fn resume_execution_with_context(
        &self,
        run_id: &RunId,
        context: Option<TraceContext>,
    ) -> Result<RunHandle, RuntimeError> {
        let inner = self
            .runtime
            .resume_execution_with_context(run_id, context)
            .await?;
        Ok(RunHandle::from_runtime(inner))
    }

    /// Reclaims all expired unfinished runs and reports each expired run it
    /// left unfinished, for example one whose session no longer matches its
    /// checkpoint.
    ///
    /// # Errors
    ///
    /// Returns a store or execution error for a claimed run.
    pub async fn recover(&self) -> Result<crabber_runtime::RecoverReport, RuntimeError> {
        self.recover_with_context(|_| None).await
    }

    /// Selects context per expired run, preventing shared ambient recovery identity.
    /// # Errors
    /// Returns initialization, store or execution errors.
    pub async fn recover_with_context<F>(
        &self,
        context_for: F,
    ) -> Result<crabber_runtime::RecoverReport, RuntimeError>
    where
        F: FnMut(&crabber_core::Run) -> Option<TraceContext> + Send,
    {
        self.runtime.recover_with_context(context_for).await
    }
}

/// Only the winner of keyed admission receives a live execution handle.
pub enum Admission {
    Started {
        receipt: AdmissionReceipt,
        handle: RunHandle,
    },
    Replayed(AdmissionReceipt),
}
impl Admission {
    fn from_runtime(admission: crabber_runtime::Admission) -> Self {
        match admission {
            crabber_runtime::Admission::Started {
                receipt,
                handle: inner,
            } => Admission::Started {
                receipt,
                handle: RunHandle::from_runtime(inner),
            },
            crabber_runtime::Admission::Replayed(receipt) => Admission::Replayed(receipt),
        }
    }

    #[must_use]
    pub fn receipt(&self) -> &AdmissionReceipt {
        match self {
            Self::Started { receipt, .. } | Self::Replayed(receipt) => receipt,
        }
    }
}
impl std::fmt::Debug for Admission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Started { receipt, .. } => f.debug_tuple("Started").field(receipt).finish(),
            Self::Replayed(receipt) => f.debug_tuple("Replayed").field(receipt).finish(),
        }
    }
}

pub use crabber_runtime::RunEvents;

/// A running prompt with a live event receiver and terminal result.
pub struct RunHandle {
    run_id: RunId,
    inner: crabber_runtime::RunHandle,
}

impl RunHandle {
    fn from_runtime(inner: crabber_runtime::RunHandle) -> Self {
        Self {
            run_id: inner.run_id().clone(),
            inner,
        }
    }

    /// Clone local control; the event receiver remains exclusive to this handle.
    #[must_use]
    pub fn control(&self) -> crabber_runtime::RunControl {
        self.inner.control()
    }
    pub fn interrupt(&self) {
        self.inner.interrupt();
    }
    #[must_use]
    pub fn session_id(&self) -> &SessionId {
        self.inner.session_id()
    }

    #[must_use]
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }

    /// Takes the live event receiver. Call this before waiting for completion.
    ///
    /// # Panics
    ///
    /// Panics if the receiver was already taken from this handle.
    #[must_use]
    pub fn events(&mut self) -> RunEvents {
        self.inner.events()
    }

    /// Waits for the worker result and actual task join. Extension resource
    /// cleanup and durable outcome reconciliation remain separate.
    ///
    /// # Errors
    ///
    /// Returns a runtime error if the run fails.
    pub async fn done(self) -> Result<RunResult, RuntimeError> {
        self.inner.done().await
    }

    /// Queues a steering message for the next model request.
    ///
    /// # Errors
    ///
    /// Returns a store error if the inbox write fails.
    pub async fn steer(&self, text: impl Into<String>) -> Result<(), RuntimeError> {
        self.inner.steer(text).await
    }

    /// Queues a follow-up message for the next idle turn.
    ///
    /// # Errors
    ///
    /// Returns a store error if the inbox write fails.
    pub async fn follow_up(&self, text: impl Into<String>) -> Result<(), RuntimeError> {
        self.inner.follow_up(text).await
    }
}

struct BuiltinExtension {
    tools: Vec<Arc<ToolDefinition>>,
    prompts: Vec<Arc<PromptSection>>,
}
#[async_trait]
impl Extension for BuiltinExtension {
    fn id(&self) -> &'static str {
        "crabber/builtin"
    }
    fn version(&self) -> &'static str {
        "2"
    }
    fn config_hash(&self) -> String {
        crabber_session::admission_config_hash(&serde_json::json!({
            "contract": "crabber.builtin-registration.v1",
            "tools": self.tools.iter().map(|tool| &tool.info).collect::<Vec<_>>(),
            "prompts": self.prompts.iter().map(|prompt| (&prompt.name, prompt.order, &prompt.text)).collect::<Vec<_>>(),
        }))
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        for tool in &self.tools {
            registrar.tool(Arc::clone(tool));
        }
        for prompt in &self.prompts {
            registrar.prompt(Arc::clone(prompt));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabber_providers::{FakeProvider, ProviderError, ProviderErrorKind, StreamDelta};
    use std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    struct BuilderReader;
    #[async_trait]
    impl crate::WorkspaceReader for BuilderReader {
        async fn read_limited(
            &self,
            _: &str,
            _: usize,
        ) -> Result<Vec<u8>, crate::WorkspaceReadError> {
            Ok(b"builder-reader".to_vec())
        }
    }
    struct BuilderResolver(Arc<AtomicBool>);
    #[async_trait]
    impl crate::WorkspaceReaderResolver for BuilderResolver {
        async fn resolve(
            &self,
            _: &crabber_extension::WorkspaceContext,
        ) -> Result<Arc<dyn crate::WorkspaceReader>, crate::WorkspaceReadError> {
            self.0.store(true, Ordering::SeqCst);
            Ok(Arc::new(BuilderReader))
        }
    }
    struct BuilderMiddleware;
    #[async_trait]
    impl crabber_extension::SystemPromptMiddleware for BuilderMiddleware {
        async fn contribute(
            &self,
            context: crabber_extension::ModelAttemptContext,
        ) -> Result<Option<String>, String> {
            let resolver = context.workspace_reader_resolver().ok_or("missing")?;
            let reader = resolver
                .resolve(context.workspace())
                .await
                .map_err(|_| "resolve")?;
            let bytes = reader
                .read_limited("AGENTS.md", 32)
                .await
                .map_err(|_| "read")?;
            String::from_utf8(bytes)
                .map(Some)
                .map_err(|_| "utf8".into())
        }
    }

    struct Uncalled;
    #[async_trait]
    impl crabber_extension::JsonToolExecutor for Uncalled {
        async fn execute(&self, _: serde_json::Value) -> Result<serde_json::Value, ExtensionError> {
            panic!("metadata must not execute tools");
        }
    }
    #[test]
    fn builtin_registration_identity_binds_tool_schema_and_prompt_data() {
        let tool = |parameters| {
            Arc::new(ToolDefinition {
                info: crabber_core::ToolInfo {
                    name: "declared".into(),
                    description: "description".into(),
                    parameters,
                    retry_safe: false,
                    required_permissions: vec![],
                },
                executor: Arc::new(Uncalled),
            })
        };
        let config = |parameters, text: &str| {
            BuiltinExtension {
                tools: vec![tool(parameters)],
                prompts: vec![Arc::new(PromptSection {
                    name: "prompt".into(),
                    order: 0,
                    text: text.into(),
                })],
            }
            .config_hash()
        };
        let original = config(
            serde_json::json!({"type":"object","additionalProperties":false}),
            "first",
        );
        assert_eq!(
            original,
            config(
                serde_json::json!({"additionalProperties":false,"type":"object"}),
                "first"
            )
        );
        assert_ne!(
            original,
            config(serde_json::json!({"type":"string"}), "first")
        );
        assert_ne!(
            original,
            config(
                serde_json::json!({"type":"object","additionalProperties":false}),
                "changed"
            )
        );
    }
    struct BuilderExtension;
    #[async_trait]
    impl Extension for BuilderExtension {
        fn id(&self) -> &'static str {
            "builder-resolver"
        }
        fn version(&self) -> &'static str {
            "1"
        }
        fn config_hash(&self) -> String {
            String::new()
        }
        async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
            registrar.system_prompt_middleware(
                "builder-reader",
                0,
                crabber_extension::MiddlewareDescriptor::new(
                    "builder-test",
                    "1",
                    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                )
                .unwrap(),
                Arc::new(BuilderMiddleware),
            );
            Ok(())
        }
    }

    #[tokio::test]
    async fn agent_builder_workspace_resolver_is_forwarded() {
        let called = Arc::new(AtomicBool::new(false));
        let fake = FakeProvider::scripted(vec![vec![
            StreamDelta::TextDelta("ok".into()),
            StreamDelta::Completed,
        ]]);
        let agent = Agent::builder()
            .behavior_version("agent-unit-tests.v1")
            .memory()
            .provider(Arc::new(fake.clone()))
            .config(AgentConfig::new(Selection {
                provider_id: "fake".into(),
                model_id: "scripted".into(),
            }))
            .workspace_reader_resolver(Arc::new(BuilderResolver(called.clone())))
            .shared_extension(Arc::new(BuilderExtension), Scope::Global)
            .build()
            .unwrap();
        agent
            .prompt(None, "hello")
            .await
            .unwrap()
            .done()
            .await
            .unwrap();
        assert!(called.load(Ordering::SeqCst));
        assert_eq!(fake.requests()[0].system.as_deref(), Some("builder-reader"));
        let _: crate::WorkspaceReadErrorKind = crate::WorkspaceReadErrorKind::Io;
    }

    struct CloseProbe {
        ready: Arc<tokio::sync::Semaphore>,
        release: Arc<tokio::sync::Semaphore>,
        stopped: Arc<tokio::sync::Semaphore>,
    }
    #[async_trait]
    impl Extension for CloseProbe {
        fn id(&self) -> &'static str {
            "close-probe"
        }
        fn version(&self) -> &'static str {
            "1"
        }
        fn config_hash(&self) -> String {
            String::new()
        }
        async fn install(&self, r: &mut Registrar) -> Result<(), ExtensionError> {
            let ready = self.ready.clone();
            let release = self.release.clone();
            r.on_result_transform(
                0,
                "cleanup",
                Arc::new(move |context, value| {
                    let ready = ready.clone();
                    let release = release.clone();
                    Box::pin(async move {
                        let closing = context.cleanup().closing();
                        context.cleanup().spawn(async move {
                            closing.await;
                            ready.add_permits(1);
                            release.acquire().await.unwrap().forget();
                        });
                        Ok(crabber_extension::TransformOutput::new(value))
                    })
                }),
            );
            Ok(())
        }
        async fn shutdown(&self) {
            self.stopped.add_permits(1);
        }
    }
    #[derive(Default)]
    struct CloseCapture(std::sync::Mutex<Vec<crabber_extension::MountCloseTimeout>>);
    impl Observer for CloseCapture {
        fn emit(&self, _: &EventRecord) {}
        fn mount_close_timed_out(&self, timeout: &crabber_extension::MountCloseTimeout) {
            self.0.lock().unwrap().push(timeout.clone());
        }
    }

    struct PanickingCloseObserver;
    impl Observer for PanickingCloseObserver {
        fn emit(&self, _: &EventRecord) {}
        fn mount_close_timed_out(&self, _: &crabber_extension::MountCloseTimeout) {
            panic!("local close observer failure");
        }
    }

    #[tokio::test]
    async fn close_extensions_is_terminal_and_reports_retained_work() {
        let ready = Arc::new(tokio::sync::Semaphore::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let stopped = Arc::new(tokio::sync::Semaphore::new(0));
        let first = Arc::new(CloseCapture::default());
        let second = Arc::new(CloseCapture::default());
        let bound = Duration::from_millis(50);
        let agent = Agent::builder()
            .behavior_version("agent-unit-tests.v1")
            .memory()
            .provider(Arc::new(FakeProvider::scripted(vec![])))
            .config(AgentConfig::new(Selection {
                provider_id: "fake".into(),
                model_id: "scripted".into(),
            }))
            .observer(Arc::new(PanickingCloseObserver))
            .observer(first.clone())
            .observer(second.clone())
            .extension_close_timeout(bound)
            .shared_extension(
                Arc::new(CloseProbe {
                    ready: ready.clone(),
                    release: release.clone(),
                    stopped: stopped.clone(),
                }),
                Scope::Global,
            )
            .build()
            .unwrap();
        agent.initialize_extensions().await.unwrap();
        let session = SessionId::new();
        let plan = agent
            .registry
            .as_ref()
            .unwrap()
            .try_acquire(&session)
            .unwrap();
        let context = crabber_extension::ToolResultContext::new(
            "echo".into(),
            true,
            crabber_extension::ToolInput::Normalized(serde_json::json!({})),
            crabber_core::ToolCallId::new(),
            session,
            RunId::new(),
            crabber_extension::ToolOutcomeClass::Succeeded,
        );
        plan.dispatcher
            .transform_tool_result(
                context,
                crabber_extension::TransformOutput::new(
                    crabber_core::ToolOutput::from_json(&(serde_json::Value::Null)).unwrap(),
                ),
            )
            .await;
        let closing = agent.close_extensions();
        tokio::pin!(closing);
        tokio::select! {
            result = &mut closing => panic!("close returned before cleanup signal: {result:?}"),
            permit = ready.acquire() => permit.unwrap().forget(),
        }
        assert!(matches!(
            agent.prompt(None, "during close").await,
            Err(RuntimeError::HostClosed)
        ));
        assert!(
            matches!(closing.await, Err(ExtensionError::MountCloseTimeout { extension }) if extension == "close-probe")
        );
        for capture in [&first, &second] {
            let timeouts = capture.0.lock().unwrap();
            assert_eq!(timeouts.len(), 1);
            assert_eq!(timeouts[0].extension, "close-probe");
            assert_eq!(timeouts[0].bound, bound);
            assert_eq!(timeouts[0].leases, 1);
            assert_eq!(timeouts[0].pending_tasks, 1);
        }
        assert!(matches!(
            agent.prompt(None, "after timeout").await,
            Err(RuntimeError::HostClosed)
        ));
        assert_eq!(stopped.available_permits(), 0);
        plan.release();
        release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(1), stopped.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        agent.close_extensions().await.unwrap();
        assert_eq!(first.0.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn close_without_extensions_is_terminal() {
        let agent = Agent::builder()
            .behavior_version("agent-unit-tests.v1")
            .memory()
            .provider(Arc::new(FakeProvider::scripted(vec![vec![
                StreamDelta::TextDelta("done".into()),
                StreamDelta::Completed,
            ]])))
            .config(AgentConfig::new(Selection {
                provider_id: "fake".into(),
                model_id: "scripted".into(),
            }))
            .build()
            .unwrap();
        agent.close_extensions().await.unwrap();
        assert!(matches!(
            agent.prompt(None, "after close").await,
            Err(RuntimeError::HostClosed)
        ));
    }

    #[cfg(feature = "datadog")]
    #[tokio::test]
    async fn configured_export_health_stays_local_during_provider_failure() {
        check_configured_export_health(false).await;
    }

    #[cfg(feature = "datadog")]
    #[tokio::test]
    async fn datadog_observer_takes_precedence_and_shares_health() {
        check_configured_export_health(true).await;
    }

    #[cfg(feature = "datadog")]
    async fn check_configured_export_health(shared: bool) {
        struct Capture(std::sync::atomic::AtomicU64);
        impl Observer for Capture {
            fn emit(&self, _: &crabber_core::EventRecord) {}
            fn operational_completed(&self, _: &crabber_runtime::OperationalObservation) {
                self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let mut config = crabber_obs::DatadogConfig::from_lookup(&|name| {
            if name == "DD_API_KEY" {
                Ok("credential-free-test".into())
            } else {
                Err(std::env::VarError::NotPresent)
            }
        })
        .unwrap();
        config.api_origin = Some(origin.clone());
        config.logs_origin = Some(origin);
        config.timeout = Duration::from_millis(40);
        let observer = crabber_obs::DatadogObserver::new(&config);
        let capture = Arc::new(Capture(std::sync::atomic::AtomicU64::new(0)));
        let builder = Agent::builder()
            .behavior_version("agent-unit-tests.v1")
            .memory()
            .provider(Arc::new(FakeProvider::scripted(vec![vec![
                StreamDelta::Error(ProviderError {
                    kind: ProviderErrorKind::Server,
                    message: "PROMPT_SECRET".into(),
                    retryable: false,
                }),
            ]])))
            .observer(capture.clone())
            .datadog(config);
        let builder = if shared {
            builder.datadog_observer(observer.clone())
        } else {
            builder
        };
        let agent = builder
            .config(AgentConfig::new(Selection {
                provider_id: "fake".into(),
                model_id: "scripted".into(),
            }))
            .build()
            .unwrap();
        assert_eq!(agent.export_health().unwrap().accepted, 0);
        let run = agent.prompt(None, "PROMPT_SECRET").await.unwrap();
        assert!(run.done().await.is_err());
        assert_eq!(capture.0.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert!(agent.export_health().unwrap().accepted >= 2);
        if shared {
            assert_eq!(agent.export_health(), Some(observer.health()));
        } else {
            assert_eq!(observer.health().accepted, 0);
            observer.stop_now();
        }
        assert_eq!(
            agent.export_health().unwrap().last_success_unix_seconds,
            None
        );
        assert!(agent.shutdown().await.is_err());
        tokio::time::timeout(Duration::from_secs(1), async {
            while agent.export_health().unwrap().worker_status != crabber_obs::WorkerStatus::Stopped
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let health = agent.export_health().unwrap();
        assert_eq!(health.queue_depth, 0);
        assert_eq!(health.pending_depth, 0);
        assert!(health.dropped >= 2);
    }

    #[tokio::test]
    async fn provider_failure_ends_events_and_done_returns_error() {
        let provider = FakeProvider::scripted(vec![vec![
            StreamDelta::TextDelta("partial".into()),
            StreamDelta::Error(ProviderError {
                kind: ProviderErrorKind::Server,
                message: "scripted failure".into(),
                retryable: false,
            }),
        ]]);
        let agent = Agent::builder()
            .behavior_version("agent-unit-tests.v1")
            .memory()
            .provider(Arc::new(provider))
            .config(AgentConfig::new(Selection {
                provider_id: "fake".into(),
                model_id: "scripted".into(),
            }))
            .build()
            .unwrap();
        #[cfg(feature = "datadog")]
        {
            assert_eq!(agent.export_health(), None);
            agent.flush().await.unwrap();
            agent.shutdown().await.unwrap();
        }
        let mut run = agent.prompt(None, "fail").await.unwrap();
        let mut events = run.events();
        let observed = tokio::time::timeout(Duration::from_secs(2), async {
            let mut kinds = Vec::new();
            while let Some(event) = events.recv().await.unwrap() {
                kinds.push(event.kind.clone());
            }
            kinds
        })
        .await
        .expect("event stream must end after provider failure");
        assert!(observed.contains(&crabber_core::EventKind::TextDelta));
        assert!(observed.contains(&crabber_core::EventKind::RunSettled));
        assert!(matches!(run.done().await, Err(RuntimeError::Provider(_))));
    }
}

mod queue;
mod session_defaults;
