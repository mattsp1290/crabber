use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, BuildError, FakeProvider, Selection, StreamDelta, ToolDefinition,
    ToolExecutor,
    core::{RunStatus, ToolCallId, ToolInfo},
    extension::{Extension, ExtensionError, ModelStream, Next, Point, Registrar, Scope},
    runtime::{InterruptPolicy, PermissionDecision, PermissionPolicy},
    session::{MemoryStore, Store},
};
use serde_json::{Value, json};
use std::sync::Arc;

fn config() -> AgentConfig {
    AgentConfig::new(Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    })
}
fn text() -> Vec<StreamDelta> {
    vec![
        StreamDelta::TextDelta("done".into()),
        StreamDelta::Completed,
    ]
}
fn tool_call() -> Vec<StreamDelta> {
    let call_id = ToolCallId::new();
    vec![
        StreamDelta::ToolCallStart {
            call_id: call_id.clone(),
            name: "echo".into(),
        },
        StreamDelta::ToolCallArgsDelta {
            call_id: call_id.clone(),
            text: "{}".into(),
        },
        StreamDelta::ToolCallDone { call_id },
        StreamDelta::Completed,
    ]
}
struct Echo;
#[async_trait]
impl ToolExecutor for Echo {
    async fn execute(&self, input: Value) -> Result<Value, ExtensionError> {
        Ok(input)
    }
}
fn tool() -> Arc<ToolDefinition> {
    Arc::new(ToolDefinition {
        info: ToolInfo {
            name: "echo".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
            retry_safe: true,
            required_permissions: vec![],
        },
        executor: Arc::new(Echo),
    })
}
#[tokio::test]
async fn default_cap_is_absent() {
    assert_eq!(config().max_output_tokens, None);
    let provider = Arc::new(FakeProvider::scripted(vec![text()]));
    let agent = Agent::builder()
        .memory()
        .provider(provider.clone())
        .config(config())
        .build()
        .unwrap();
    agent
        .prompt(None, "hi")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    assert_eq!(provider.requests()[0].max_tokens, None);
}
#[tokio::test]
async fn facade_cap_sets_every_turn_request() {
    let provider = Arc::new(FakeProvider::scripted(vec![tool_call(), text()]));
    let agent = Agent::builder()
        .memory()
        .provider(provider.clone())
        .config(config().max_output_tokens(4096))
        .tool(tool())
        .build()
        .unwrap();
    agent
        .prompt(None, "hi")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|r| r.max_tokens == Some(4096)));
}
struct CapHook(Option<Value>);
#[async_trait]
impl Extension for CapHook {
    fn id(&self) -> &'static str {
        "test/cap"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        String::new()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        let cap = self.0.clone();
        registrar.on_around(
            ModelStream::ID,
            0,
            "cap",
            Arc::new(move |mut value: Value, next: Next| {
                let cap = cap.clone();
                Box::pin(async move {
                    assert_eq!(value["max_tokens"], 4096);
                    if let Some(cap) = cap {
                        value["max_tokens"] = cap;
                    } else {
                        value.as_object_mut().unwrap().remove("max_tokens");
                    }
                    next.call(value).await
                })
            }),
        );
        Ok(())
    }
}
#[tokio::test]
async fn around_hook_overrides_or_preserves_facade_cap() {
    for (override_cap, expected) in [
        (Some(json!(32)), Some(32)),
        (Some(Value::Null), None),
        (None, Some(4096)),
    ] {
        let provider = Arc::new(FakeProvider::scripted(vec![text()]));
        let agent = Agent::builder()
            .memory()
            .provider(provider.clone())
            .config(config().max_output_tokens(4096))
            .extension(Arc::new(CapHook(override_cap)), Scope::Global)
            .build()
            .unwrap();
        agent
            .prompt(None, "hi")
            .await
            .unwrap()
            .done()
            .await
            .unwrap();
        assert_eq!(provider.requests()[0].max_tokens, expected);
    }
}
#[test]
fn zero_cap_is_rejected_at_build() {
    let result = Agent::builder()
        .memory()
        .provider(Arc::new(FakeProvider::scripted(vec![])))
        .config(config().max_output_tokens(0))
        .build();
    assert!(matches!(result, Err(BuildError::InvalidConfig(_))));
}
struct Pause;
impl PermissionPolicy for Pause {
    fn decide(&self, _: &ToolInfo, _: &Value) -> PermissionDecision {
        PermissionDecision::Allow
    }
    fn interrupt_policy(&self, _: &ToolInfo, _: &Value) -> InterruptPolicy {
        InterruptPolicy::Pause
    }
}
#[tokio::test]
async fn cap_survives_pause_checkpoint_resume() {
    let store = Arc::new(MemoryStore::new());
    let provider = Arc::new(FakeProvider::scripted(vec![tool_call(), text()]));
    let agent = Agent::builder()
        .store(store.clone())
        .provider(provider.clone())
        .config(config().max_output_tokens(4096))
        .tool(tool())
        .policy(Arc::new(Pause))
        .build()
        .unwrap();
    let result = agent
        .prompt(None, "hi")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    assert_eq!(result.status, RunStatus::Paused);
    let saved = store.get_run(&result.run_id).await.unwrap().unwrap();
    assert_eq!(
        saved.checkpoint.unwrap()["request"]["max_output_tokens"],
        4096
    );
    // A fresh host with a different config must restore the checkpoint's cap.
    let resumed = Agent::builder()
        .store(store)
        .provider(provider.clone())
        .config(config().max_output_tokens(64))
        .tool(tool())
        .build()
        .unwrap();
    assert_eq!(
        resumed.resume(&result.run_id).await.unwrap().status,
        RunStatus::Completed
    );
    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    assert!(requests.iter().all(|r| r.max_tokens == Some(4096)));
}
