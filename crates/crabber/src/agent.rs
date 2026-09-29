use crabber_core::{EventRecord, RunId, SessionId};
use crabber_extension::{PromptSection, StaticPlanProvider, ToolDefinition};
use crabber_providers::{Resolver, Selection};
use crabber_runtime::{Observer, Orchestrator, PermissionPolicy, Request, RunResult, RuntimeError};
use crabber_session::{MemoryStore, Store};
use std::sync::Arc;
use tokio::sync::broadcast;

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
}

impl AgentBuilder {
    #[must_use]
    pub fn memory(mut self) -> Self {
        self.store = Some(Arc::new(MemoryStore::new()));
        self
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

    /// Builds the embeddable agent.
    ///
    /// # Errors
    ///
    /// Returns an error if the provider, config, or runtime dependencies are invalid.
    pub fn build(self) -> Result<Agent, BuildError> {
        let resolver = self.resolver.ok_or(BuildError::NoProvider)?;
        let config = self.config.ok_or(BuildError::NoConfig)?;
        let (events, _) = broadcast::channel(256);
        let observer = Arc::new(EventBroadcaster {
            events: events.clone(),
        });
        let mut runtime = Orchestrator::builder()
            .store(self.store.unwrap_or_else(|| Arc::new(MemoryStore::new())))
            .resolver(resolver)
            .plan_provider(Arc::new(StaticPlanProvider::new(self.tools, self.prompts)))
            .observer(observer);
        if let Some(policy) = self.policy {
            runtime = runtime.policy(policy);
        }
        Ok(Agent {
            runtime: runtime.build()?,
            config,
            events,
        })
    }
}

struct EventBroadcaster {
    events: broadcast::Sender<Arc<EventRecord>>,
}

impl Observer for EventBroadcaster {
    fn emit(&self, event: &EventRecord) {
        let _ = self.events.send(Arc::new(event.clone()));
    }
}

/// A configured agent ready to prompt a new or existing session.
pub struct Agent {
    runtime: Orchestrator,
    config: AgentConfig,
    events: broadcast::Sender<Arc<EventRecord>>,
}

impl Agent {
    #[must_use]
    pub fn builder() -> AgentBuilder {
        AgentBuilder {
            store: None,
            resolver: None,
            config: None,
            tools: Vec::new(),
            prompts: Vec::new(),
            policy: None,
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
            inner,
            events: Some(receiver),
        })
    }
}

/// Filters one agent's event broadcast to a single run.
pub struct RunEvents {
    run_id: RunId,
    receiver: broadcast::Receiver<Arc<EventRecord>>,
}

impl RunEvents {
    /// Receives the next event for this run.
    ///
    /// # Errors
    ///
    /// Returns a broadcast lag or closure error if events cannot be read.
    pub async fn recv(&mut self) -> Result<Arc<EventRecord>, broadcast::error::RecvError> {
        loop {
            let event = self.receiver.recv().await?;
            if event.run_id == self.run_id {
                return Ok(event);
            }
        }
    }
}

/// A running prompt with a live event receiver and terminal result.
pub struct RunHandle {
    run_id: RunId,
    inner: crabber_runtime::RunHandle,
    events: Option<broadcast::Receiver<Arc<EventRecord>>>,
}

impl RunHandle {
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
