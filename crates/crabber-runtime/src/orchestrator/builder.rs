#[cfg(test)]
use super::ResultContextObserver;
use super::{
    Arc, Clock, CompactionPolicy, ConfigSnapshot, DirectModelStream, Duration, ExecutionMode,
    HostServices, IdentityToolPipeline, ModelStream, MonotonicClock, NoopObserver, Observer,
    Orchestrator, OrchestratorBuilder, PermissionPolicy, Resolver, RunPlanProvider, RuntimeError,
    StaticPolicy, Store, SystemClock, SystemMonotonicClock, ToolPipeline, WorkspaceReaderResolver,
    admission_config_hash, host_callbacks, json, preparation,
};

impl OrchestratorBuilder {
    #[cfg(test)]
    pub(crate) fn context_observer(mut self, observer: ResultContextObserver) -> Self {
        self.context_observer = Some(observer);
        self
    }

    /// Declare the nonsecret version of opaque native policy/callback behavior.
    /// Change it whenever relevant behavior changes; credentials are excluded.
    #[must_use]
    pub fn behavior_fingerprint(mut self, value: crabber_core::InputFingerprint) -> Self {
        self.behavior_fingerprint = Some(value);
        self
    }
    /// Hash an explicit host behavior version in the library's versioned domain.
    /// # Panics
    /// Panics only if a library SHA-256 digest ceases to be a valid fingerprint.
    #[must_use]
    pub fn behavior_version(self, version: &str) -> Self {
        let fingerprint = crabber_core::InputFingerprint::new(admission_config_hash(&json!([
            "crabber.host-behavior.v1",
            version
        ])))
        .expect("SHA-256 is a valid fingerprint");
        self.behavior_fingerprint(fingerprint)
    }

    #[must_use]
    pub fn store(mut self, value: Arc<dyn Store>) -> Self {
        self.store = Some(value);
        self
    }
    #[must_use]
    pub fn resolver(mut self, value: Arc<dyn Resolver>) -> Self {
        self.resolver = Some(value);
        self
    }
    /// Grants typed model middleware host-authorized, per-workspace readers.
    #[must_use]
    pub fn workspace_reader_resolver(mut self, value: Arc<dyn WorkspaceReaderResolver>) -> Self {
        self.workspace_reader_resolver = Some(value);
        self
    }
    #[must_use]
    pub fn plan_provider(mut self, value: Arc<dyn RunPlanProvider>) -> Self {
        self.plan_provider = Some(value);
        self.contextual_provider = None;
        self
    }
    /// Configure this concrete provider for owned initialization and acquisition.
    #[must_use]
    pub fn contextual_plan_provider(
        mut self,
        provider: Arc<crabber_extension::ContextualPlanProvider>,
    ) -> Self {
        self.plan_provider = Some(provider.clone());
        self.contextual_provider = Some(provider);
        self
    }

    #[must_use]
    pub fn clock(mut self, value: Arc<dyn Clock>) -> Self {
        self.clock = Some(value);
        self
    }
    /// Injects execution timing independently of event/lease wall time.
    #[must_use]
    pub fn monotonic_clock(mut self, value: Arc<dyn MonotonicClock>) -> Self {
        self.monotonic_clock = Some(value);
        self
    }
    #[must_use]
    pub fn observer(mut self, value: Arc<dyn Observer>) -> Self {
        self.observer = Some(value);
        self
    }
    #[must_use]
    pub fn model_stream(mut self, value: Arc<dyn ModelStream>) -> Self {
        self.model_stream = Some(value);
        self
    }
    #[must_use]
    pub fn policy(mut self, value: Arc<dyn PermissionPolicy>) -> Self {
        self.policy = Some(value);
        self
    }
    #[must_use]
    pub fn tool_pipeline(mut self, value: Arc<dyn ToolPipeline>) -> Self {
        self.tool_pipeline = Some(value);
        self
    }
    #[must_use]
    pub fn host_services(mut self, value: HostServices) -> Self {
        self.host_services = Some(value);
        self
    }
    #[must_use]
    pub fn max_turns(mut self, value: usize) -> Self {
        self.max_turns = Some(value);
        self
    }
    #[must_use]
    pub fn heartbeat_interval(mut self, value: Duration) -> Self {
        self.heartbeat_interval = Some(value);
        self
    }
    #[must_use]
    pub fn execution_mode(mut self, value: ExecutionMode) -> Self {
        self.execution_mode = Some(value);
        self
    }
    #[must_use]
    pub fn compaction(mut self, value: CompactionPolicy) -> Self {
        self.compaction = Some(value);
        self
    }
    #[must_use]
    pub fn config_snapshot(mut self, value: ConfigSnapshot) -> Self {
        self.execution_mode = Some(value.execution_mode);
        self.compaction = Some(value.compaction);
        self.max_turns = Some(value.max_turns);
        self
    }

    /// Builds an orchestrator from its required dependencies.
    ///
    /// # Errors
    ///
    /// Returns `Missing` if the store, resolver, or plan provider is absent.
    pub fn build(self) -> Result<Orchestrator, RuntimeError> {
        let host_callbacks = host_callbacks::capture(&self);
        let heartbeat_interval = self.heartbeat_interval.unwrap_or(Duration::from_secs(5));
        if heartbeat_interval.is_zero() || heartbeat_interval >= Duration::from_secs(15) {
            return Err(RuntimeError::InvalidConfiguration(
                "heartbeat interval must be positive and below 15 seconds",
            ));
        }
        if matches!(
            self.execution_mode,
            Some(ExecutionMode::Parallel { max: 0 })
        ) {
            return Err(RuntimeError::InvalidConfiguration(
                "parallel max must be positive",
            ));
        }
        if self.max_turns == Some(0) {
            return Err(RuntimeError::InvalidConfiguration(
                "max turns must be positive",
            ));
        }
        let compaction = self.compaction.unwrap_or_default();
        if !(0.0..=1.0).contains(&compaction.trigger_ratio) || compaction.trigger_ratio == 0.0 {
            return Err(RuntimeError::InvalidConfiguration(
                "compaction trigger ratio must be in (0,1]",
            ));
        }
        Ok(Orchestrator {
            host_callbacks,
            execution_tasks: crate::execution_tasks::ExecutionTasks::default(),
            worker_resources: None,
            root_scope: None,
            close_coordinator: Arc::default(),
            worker_callbacks: None,
            session_invocations: crate::session_invocations::SessionInvocations::default(),
            session_services: crate::session_services::SessionServices::default(),
            preparation_scope: None,
            retained_preparations: preparation::retained::RetainedPreparations::default(),
            pending_drivers:
                crabber_session::session_owner::owned_scope::PendingDriverTasks::default(),
            owned_attempt: None,
            source_delivery: None,
            observation_lifetime: Arc::default(),
            tool_resumes: Arc::default(),
            frontend_resumes: Arc::default(),
            tool_suspensions: Arc::default(),
            store: self.store.ok_or(RuntimeError::Missing("store"))?,
            resolver: self.resolver.ok_or(RuntimeError::Missing("resolver"))?,
            workspace_reader_resolver: self.workspace_reader_resolver,
            contextual_provider: self.contextual_provider,
            plan_provider: self
                .plan_provider
                .ok_or(RuntimeError::Missing("plan provider"))?,
            behavior_fingerprint: self
                .behavior_fingerprint
                .ok_or(RuntimeError::Missing("host behavior fingerprint"))?,
            clock: self.clock.unwrap_or_else(|| Arc::new(SystemClock)),
            monotonic_clock: self
                .monotonic_clock
                .unwrap_or_else(|| Arc::new(SystemMonotonicClock::default())),
            observer: self.observer.unwrap_or_else(|| Arc::new(NoopObserver)),
            model_stream: self
                .model_stream
                .unwrap_or_else(|| Arc::new(DirectModelStream)),
            policy: self
                .policy
                .unwrap_or_else(|| Arc::new(StaticPolicy::default())),
            tool_pipeline: self
                .tool_pipeline
                .unwrap_or_else(|| Arc::new(IdentityToolPipeline)),
            host_services: self.host_services.unwrap_or_default(),
            max_turns: self.max_turns.unwrap_or(64),
            heartbeat_interval,
            execution_mode: self.execution_mode.unwrap_or_default(),
            compaction,
            #[cfg(test)]
            context_observer: self.context_observer,
        })
    }
}
