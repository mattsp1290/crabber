use async_trait::async_trait;
use crabber::extension::{
    ComponentIdentity, Extension, ExtensionError, ExtensionFactory, FactoryContext, FactoryScope,
    Registrar,
};
use crabber::providers::{
    DeltaStream, ModelRequest, ProviderBindingError, ProviderError, Resolver, Streamer,
};
use crabber::{Agent, AgentConfig, FakeProvider, Selection, StreamDelta};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{Notify, Semaphore};

pub const BOUND: std::time::Duration = std::time::Duration::from_secs(5);
pub fn selection() -> Selection {
    Selection {
        provider_id: "fake".into(),
        model_id: "close-fixture".into(),
    }
}
pub fn builder(provider: Arc<dyn Resolver>) -> crabber::AgentBuilder {
    Agent::builder()
        .behavior_version("orderly-public-close.v1")
        .memory()
        .provider(provider)
        .config(AgentConfig::new(selection()))
}
pub struct Release(pub Arc<Semaphore>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.add_permits(16);
    }
}
#[derive(Clone)]
pub struct Provider {
    fake: FakeProvider,
    pub entered: Arc<Notify>,
    pub release: Arc<Semaphore>,
    pub calls: Arc<AtomicUsize>,
    hold: bool,
}
impl Provider {
    pub fn new(hold: bool, deltas: usize) -> Self {
        let mut script = vec![StreamDelta::TextDelta("text".into()); deltas];
        script.push(StreamDelta::Completed);
        Self {
            fake: FakeProvider::scripted(vec![script]),
            entered: Arc::default(),
            release: Arc::new(Semaphore::new(0)),
            calls: Arc::default(),
            hold,
        }
    }
}
#[async_trait]
impl Resolver for Provider {
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<crabber::core::ProviderBindingDescriptor, ProviderBindingError> {
        self.fake.binding_descriptor(selection)
    }
    async fn resolve(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        Ok(Arc::new(self.clone()))
    }
}
#[async_trait]
impl Streamer for Provider {
    async fn stream(&self, request: ModelRequest) -> Result<DeltaStream, ProviderError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        if self.hold {
            self.release.acquire().await.unwrap().forget();
        }
        self.fake.stream(request).await
    }
}
pub struct Factory {
    pub id: &'static str,
    pub entered: Notify,
    pub release: Arc<Semaphore>,
    pub closed: Arc<Semaphore>,
    pub returned: AtomicUsize,
    pub dropped: Arc<AtomicUsize>,
    pub hold_create: bool,
    pub resource: bool,
}
impl Factory {
    pub fn new(id: &'static str, hold_create: bool, resource: bool) -> Arc<Self> {
        Arc::new(Self {
            id,
            entered: Notify::new(),
            release: Arc::new(Semaphore::new(0)),
            closed: Arc::new(Semaphore::new(0)),
            returned: AtomicUsize::new(0),
            dropped: Arc::default(),
            hold_create,
            resource,
        })
    }
}
struct Instance {
    id: &'static str,
    dropped: Arc<AtomicUsize>,
}
impl Drop for Instance {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl Extension for Instance {
    fn id(&self) -> &'static str {
        self.id
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "close-fixture".into()
    }
    async fn install(&self, _: &mut Registrar) -> Result<(), ExtensionError> {
        Ok(())
    }
}
#[async_trait]
impl ExtensionFactory for Factory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: self.id.into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "close-fixture".into()
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Session
    }
    async fn create(&self, context: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError> {
        if self.resource {
            let closing = context.cleanup().closing();
            let (closed, release) = (self.closed.clone(), self.release.clone());
            context.cleanup().spawn(async move {
                closing.await;
                closed.add_permits(1);
                release.acquire().await.unwrap().forget();
            });
        }
        self.entered.notify_one();
        if self.hold_create {
            self.release.acquire().await.unwrap().forget();
        }
        self.returned.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(Instance {
            id: self.id,
            dropped: self.dropped.clone(),
        }))
    }
}

#[derive(Default)]
pub struct SelfCloseTool {
    pub agent: std::sync::Mutex<std::sync::Weak<Agent>>,
    pub observed: std::sync::Mutex<Option<crabber::CloseObservation>>,
    pub finished: Notify,
    pub calls: AtomicUsize,
}
#[async_trait]
impl crabber::JsonToolExecutor for SelfCloseTool {
    async fn execute(&self, _: serde_json::Value) -> Result<serde_json::Value, ExtensionError> {
        Err(ExtensionError::Tool(
            "actual callback context required".into(),
        ))
    }
    async fn execute_with_context(
        &self,
        _: crabber::extension::ToolContext,
        _: serde_json::Value,
    ) -> Result<serde_json::Value, ExtensionError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let agent = self.agent.lock().unwrap().upgrade().unwrap();
        // Self-close is a caller-context decision before any deadline wait.
        let report = agent.close(std::time::Duration::ZERO).await;
        *self.observed.lock().unwrap() = Some(report.observation);
        self.finished.notify_one();
        Ok(serde_json::json!({"observed":true}))
    }
}

#[derive(Default)]
pub struct ReentrantInitializer {
    pub agent: std::sync::Mutex<std::sync::Weak<Agent>>,
    pub error: std::sync::Mutex<Option<crabber::RuntimeError>>,
    pub timed_out: std::sync::atomic::AtomicBool,
    pub installs: AtomicUsize,
}
#[async_trait]
impl Extension for ReentrantInitializer {
    fn id(&self) -> &'static str {
        "same-agent-reentrant-initializer"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "same-original-initializer.v1".into()
    }
    async fn install(&self, _: &mut Registrar) -> Result<(), ExtensionError> {
        self.installs.fetch_add(1, Ordering::SeqCst);
        let agent = self.agent.lock().unwrap().upgrade().unwrap();
        match tokio::time::timeout(BOUND, agent.prompt(None, "forbidden initializer reentry")).await
        {
            Ok(Err(error)) => *self.error.lock().unwrap() = Some(error),
            Ok(Ok(handle)) => {
                // A defective intake must still stop/retain its actual worker.
                handle.interrupt();
                drop(handle);
            }
            Err(_) => self.timed_out.store(true, Ordering::SeqCst),
        }
        Ok(())
    }
}

pub struct HeldShutdown {
    pub entered: Notify,
    pub release: Arc<Semaphore>,
    pub calls: AtomicUsize,
}
impl Default for HeldShutdown {
    fn default() -> Self {
        Self {
            entered: Notify::new(),
            release: Arc::new(Semaphore::new(0)),
            calls: AtomicUsize::new(0),
        }
    }
}
#[async_trait]
impl Extension for HeldShutdown {
    fn id(&self) -> &'static str {
        "held-shared-shutdown"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "held-original-shutdown.v1".into()
    }
    async fn install(&self, _: &mut Registrar) -> Result<(), ExtensionError> {
        Ok(())
    }
    async fn shutdown(&self) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        self.release.acquire().await.unwrap().forget();
    }
}
