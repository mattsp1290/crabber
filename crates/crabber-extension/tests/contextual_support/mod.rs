#![allow(dead_code)]
use async_trait::async_trait;
use crabber_core::{SessionContext, ToolInfo};
use crabber_extension::*;
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
pub struct Observed {
    pub created: AtomicUsize,
    pub bindings: Mutex<Vec<(String, Option<String>)>>,
    pub stopped: Mutex<Vec<String>>,
    pub shutdown: Mutex<Vec<String>>,
}
pub struct Factory {
    pub name: &'static str,
    pub scope: FactoryScope,
    pub observed: Arc<Observed>,
}
#[async_trait]
impl ExtensionFactory for Factory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: self.name.into(),
            version: "1".into(),
        }
    }
    fn config_hash(&self) -> String {
        "{\"a\":1,\"b\":2}".into()
    }
    fn scope(&self) -> FactoryScope {
        self.scope
    }
    async fn create(&self, context: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError> {
        self.observed.created.fetch_add(1, Ordering::SeqCst);
        let workspace = context
            .workspace()
            .workspace_id()
            .unwrap_or_default()
            .to_owned();
        let session = context.session_id().map(ToString::to_string);
        self.observed
            .bindings
            .lock()
            .unwrap()
            .push((workspace.clone(), session.clone()));
        let label = format!(
            "{}:{workspace}:{}",
            self.name,
            session.as_deref().unwrap_or("workspace")
        );
        let observed = self.observed.clone();
        let stopped_label = label.clone();
        let closing = context.cleanup().closing();
        context.cleanup().spawn(async move {
            closing.await;
            observed.stopped.lock().unwrap().push(stopped_label);
        });
        Ok(Arc::new(CounterExtension {
            name: self.name,
            label,
            counter: Arc::new(AtomicUsize::new(0)),
            observed: self.observed.clone(),
        }))
    }
}
pub struct CounterExtension {
    pub name: &'static str,
    pub label: String,
    pub counter: Arc<AtomicUsize>,
    pub observed: Arc<Observed>,
}
struct Counter(Arc<AtomicUsize>);
#[async_trait]
impl JsonToolExecutor for Counter {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        Ok(json!(self.0.fetch_add(1, Ordering::SeqCst) + 1))
    }
}
#[async_trait]
impl Extension for CounterExtension {
    fn id(&self) -> &'static str {
        self.name
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "fixture-v1".into()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        registrar.tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: self.name.into(),
                description: "Counts owner-local calls".into(),
                parameters: json!({"type":"object"}),
                retry_safe: true,
                required_permissions: vec![],
            },
            executor: Arc::new(Counter(self.counter.clone())),
        }));
        registrar.on_transform(
            ContextAssemble::ID,
            0,
            "counter-handler",
            Arc::new(|value| Box::pin(async move { Ok(value) })),
        );
        Ok(())
    }
    async fn shutdown(&self) {
        self.observed
            .shutdown
            .lock()
            .unwrap()
            .push(self.label.clone());
    }
}
pub fn context(session: &str, workspace: &str) -> SessionContext {
    SessionContext::new(session.into(), workspace, format!("/admitted/{workspace}"))
}
pub async fn count(plan: &RunPlan, name: &str) -> Value {
    (plan
        .tools
        .iter()
        .find(|tool| tool.info.name == name)
        .unwrap()
        .executor
        .execute(json!({}))
        .await
        .unwrap())
    .json_value()
    .unwrap()
}
pub fn provider(factories: Vec<Arc<dyn ExtensionFactory>>) -> ContextualPlanProvider {
    ContextualPlanProvider::new(
        Registry::new().with_close_timeout(std::time::Duration::from_millis(30)),
        vec![],
        factories,
        std::time::Duration::from_secs(2),
    )
    .unwrap()
}
