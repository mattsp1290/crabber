use async_trait::async_trait;
use crabber_core::{EventRecord, RunId, SessionId};
use crabber_extension::{
    Extension, ExtensionError, MountHandle, PromptSection, Registrar, Registry, Scope,
    StaticPlanProvider, ToolDefinition,
};
use crabber_providers::{Resolver, Selection};
use crabber_runtime::{
    CompactionPolicy, ExecutionMode, Observer, Orchestrator, PermissionPolicy, Request, RunResult,
    RuntimeError,
};
#[cfg(feature = "postgres")]
use crabber_session::PostgresStore;
use crabber_session::{MemoryStore, Store};
use std::sync::Arc;
use tokio::sync::{OnceCell, broadcast};

/// Host-owned settings frozen into each prompt request.
#[derive(Debug, Clone)]
pub struct AgentConfig {
    pub selection: Selection,
    pub workspace_id: String,
    pub directory: String,
    pub title: String,
    pub system_prompt: Option<String>,
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
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("an agent needs a provider resolver")]
    NoProvider,
    #[error("an agent needs a configuration")]
    NoConfig,
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
}

pub struct AgentBuilder {
    store: Option<Arc<dyn Store>>,
    resolver: Option<Arc<dyn Resolver>>,
    config: Option<AgentConfig>,
    tools: Vec<Arc<ToolDefinition>>,
    prompts: Vec<Arc<PromptSection>>,
    policy: Option<Arc<dyn PermissionPolicy>>,
    #[cfg(feature = "datadog")]
    datadog: Option<crabber_obs::DatadogConfig>,
    extensions: Vec<(Arc<dyn Extension>, Scope)>,
    execution_mode: ExecutionMode,
    compaction: CompactionPolicy,
}

impl AgentBuilder {
    /// Mounts a SHA-256 verified Component Model extension.
    #[cfg(feature = "wasm")]
    #[must_use]
    pub fn wasm_extension(self, config: crabber_wasm::ModuleConfig) -> Self {
        self.extension(
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
    /// Registers the real providers compiled into this binary, using their
    /// environment variables and the local ChatGPT credential store.
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

    #[must_use]
    pub fn config(mut self, config: AgentConfig) -> Self {
        self.config = Some(config);
        self
    }

    #[must_use]
    pub fn tool(mut self, tool: Arc<ToolDefinition>) -> Self {
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
    pub fn extension(mut self, extension: Arc<dyn Extension>, scope: Scope) -> Self {
        self.extensions.push((extension, scope));
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
        let (events, _) = broadcast::channel(256);
        #[cfg(feature = "datadog")]
        let datadog = self.datadog.as_ref().map(crabber_obs::DatadogObserver::new);
        let observer = Arc::new(EventBroadcaster {
            events: events.clone(),
            #[cfg(feature = "datadog")]
            datadog: datadog.clone(),
        });
        let extensions = self.extensions;
        let has_extensions = !extensions.is_empty();
        let registry = has_extensions.then(Registry::new);
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
        let plan_provider: Arc<dyn crabber_extension::RunPlanProvider> =
            if let Some(registry) = &registry {
                Arc::new(registry.clone())
            } else {
                Arc::new(StaticPlanProvider::new(self.tools, self.prompts))
            };
        let mut runtime = Orchestrator::builder()
            .store(self.store.unwrap_or_else(|| Arc::new(MemoryStore::new())))
            .resolver(resolver)
            .plan_provider(plan_provider)
            .execution_mode(self.execution_mode)
            .compaction(self.compaction)
            .observer(observer);
        if let Some(policy) = self.policy {
            runtime = runtime.policy(policy);
        }
        Ok(Agent {
            runtime: runtime.build()?,
            config,
            events,
            #[cfg(feature = "datadog")]
            datadog,
            registry,
            extensions: mounts,
            mounted: OnceCell::new(),
        })
    }
}

struct EventBroadcaster {
    events: broadcast::Sender<Arc<EventRecord>>,
    #[cfg(feature = "datadog")]
    datadog: Option<crabber_obs::DatadogObserver>,
}

impl Observer for EventBroadcaster {
    fn emit(&self, event: &EventRecord) {
        let _ = self.events.send(Arc::new(event.clone()));
        #[cfg(feature = "datadog")]
        if let Some(datadog) = &self.datadog {
            datadog.emit(event);
            crabber_obs::tracing_bridge::emit(event);
        }
    }
    fn model_completed(&self, event: &EventRecord) {
        #[cfg(feature = "datadog")]
        if let Some(datadog) = &self.datadog {
            datadog.emit(event);
        }
        #[cfg(not(feature = "datadog"))]
        let _ = event;
    }
}

/// A configured agent ready to prompt a new or existing session.
pub struct Agent {
    runtime: Orchestrator,
    config: AgentConfig,
    events: broadcast::Sender<Arc<EventRecord>>,
    #[cfg(feature = "datadog")]
    datadog: Option<crabber_obs::DatadogObserver>,
    registry: Option<Registry>,
    extensions: Vec<(Arc<dyn Extension>, Scope)>,
    mounted: OnceCell<Vec<MountHandle>>,
}

impl Agent {
    async fn initialize_extensions(&self) -> Result<(), RuntimeError> {
        if let Some(registry) = &self.registry {
            self.mounted
                .get_or_try_init(|| async {
                    let mut handles = Vec::new();
                    for (extension, scope) in &self.extensions {
                        match registry.mount(Arc::clone(extension), scope.clone()).await {
                            Ok(handle) => handles.push(handle),
                            Err(error) => {
                                for handle in handles.iter().rev() {
                                    let _ = handle.close().await;
                                }
                                return Err(RuntimeError::Extension(error.to_string()));
                            }
                        }
                    }
                    Ok(handles)
                })
                .await?;
        }
        Ok(())
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
            store: None,
            resolver: None,
            config: None,
            tools: Vec::new(),
            prompts: Vec::new(),
            policy: None,
            #[cfg(feature = "datadog")]
            datadog: None,
            extensions: Vec::new(),
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
        self.initialize_extensions().await?;
        let receiver = self.events.subscribe();
        let inner = self
            .runtime
            .start(Request {
                session_id,
                workspace_id: self.config.workspace_id.clone(),
                directory: self.config.directory.clone(),
                title: self.config.title.clone(),
                text: text.into(),
                selection: self.config.selection.clone(),
                system_prompt: self.config.system_prompt.clone(),
            })
            .await?;
        Ok(RunHandle {
            run_id: inner.run_id().clone(),
            completion: inner.completion_signal(),
            inner,
            events: Some(receiver),
        })
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
        self.initialize_extensions().await?;
        self.runtime.resume(run_id).await
    }

    /// Reclaims all expired unfinished runs.
    ///
    /// # Errors
    ///
    /// Returns a store or execution error for a claimed run.
    pub async fn recover(&self) -> Result<Vec<RunResult>, RuntimeError> {
        self.initialize_extensions().await?;
        self.runtime.recover().await
    }
}

/// Filters one agent's event broadcast to a single run.
pub struct RunEvents {
    run_id: RunId,
    receiver: broadcast::Receiver<Arc<EventRecord>>,
    completion: tokio::sync::watch::Receiver<bool>,
}

impl RunEvents {
    /// Receives the next event for this run, or `None` once the run task ends.
    ///
    /// # Errors
    ///
    /// Returns a broadcast lag or closure error if events cannot be read.
    pub async fn recv(&mut self) -> Result<Option<Arc<EventRecord>>, broadcast::error::RecvError> {
        loop {
            if *self.completion.borrow() {
                match self.receiver.try_recv() {
                    Ok(event) if event.run_id == self.run_id => return Ok(Some(event)),
                    Ok(_) => continue,
                    Err(
                        broadcast::error::TryRecvError::Empty
                        | broadcast::error::TryRecvError::Closed,
                    ) => return Ok(None),
                    Err(broadcast::error::TryRecvError::Lagged(count)) => {
                        return Err(broadcast::error::RecvError::Lagged(count));
                    }
                }
            }
            tokio::select! {
                biased;
                event = self.receiver.recv() => {
                    let event = event?;
                    if event.run_id == self.run_id { return Ok(Some(event)); }
                }
                changed = self.completion.changed() => {
                    if changed.is_err() { return Ok(None); }
                }
            }
        }
    }
}

/// A running prompt with a live event receiver and terminal result.
pub struct RunHandle {
    run_id: RunId,
    inner: crabber_runtime::RunHandle,
    events: Option<broadcast::Receiver<Arc<EventRecord>>>,
    completion: tokio::sync::watch::Receiver<bool>,
}

impl RunHandle {
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
        RunEvents {
            run_id: self.run_id.clone(),
            receiver: self.events.take().expect("events receiver already taken"),
            completion: self.completion.clone(),
        }
    }

    /// Waits for the run's terminal result.
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
        "1"
    }
    fn config_hash(&self) -> String {
        String::new()
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
    use std::time::Duration;

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
            .memory()
            .provider(Arc::new(provider))
            .config(AgentConfig::new(Selection {
                provider_id: "fake".into(),
                model_id: "scripted".into(),
            }))
            .build()
            .unwrap();
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
