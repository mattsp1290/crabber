#![cfg(feature = "wasm")]
use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, ExtensionError, FakeProvider, PermissionDecision, Selection, StaticPolicy,
    StreamDelta, ToolDefinition, ToolExecutor,
    core::{ToolCallId, ToolInfo},
    session::{MemoryStore, Store},
    wasm::{InstanceMode, Limits, ModuleConfig},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

fn fixture(name: &str) -> ModuleConfig {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../fixtures/wasm/target/wasm32-wasip2/release")
        .canonicalize()
        .expect("cargo xtask build-fixtures");
    let path = root.join(format!("{}.wasm", name.replace('-', "_")));
    let hash = Sha256::digest(std::fs::read(&path).unwrap()).into();
    ModuleConfig {
        name: name.into(),
        path,
        allowed_root: root,
        expected_sha256: hash,
        config_json: "{}".into(),
        limits: Limits::default(),
        instance_mode: InstanceMode::PerCall,
    }
}

fn call(name: &str, input: &Value) -> Vec<StreamDelta> {
    let id = ToolCallId::new();
    vec![
        StreamDelta::ToolCallStart {
            call_id: id.clone(),
            name: name.into(),
        },
        StreamDelta::ToolCallArgsDelta {
            call_id: id.clone(),
            text: input.to_string(),
        },
        StreamDelta::ToolCallDone { call_id: id },
        StreamDelta::Completed,
    ]
}

fn provider(script: Vec<StreamDelta>) -> FakeProvider {
    FakeProvider::scripted(vec![
        script,
        vec![
            StreamDelta::TextDelta("done".into()),
            StreamDelta::Completed,
        ],
    ])
}

fn config() -> AgentConfig {
    AgentConfig::new(Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    })
}

struct CountTool(Arc<AtomicUsize>);

#[async_trait]
impl ToolExecutor for CountTool {
    async fn execute(&self, _input: Value) -> Result<Value, ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(json!({"value":"secret"}))
    }
}

fn native_tool(name: &str, calls: &Arc<AtomicUsize>) -> Arc<ToolDefinition> {
    Arc::new(ToolDefinition {
        info: ToolInfo {
            name: name.into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
            retry_safe: true,
            required_permissions: vec![],
        },
        executor: Arc::new(CountTool(Arc::clone(calls))),
    })
}

#[tokio::test]
async fn echo_executes_through_runtime() {
    let fake = provider(call("echo", &json!({"value":"hello"})));
    let store = Arc::new(MemoryStore::new());
    let agent = Agent::builder()
        .store(store.clone())
        .provider(Arc::new(fake))
        .config(config())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .wasm_extension(fixture("echo-tool"))
        .build()
        .unwrap();
    let result = agent
        .prompt(None, "echo")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let messages = store.list_messages(&result.session_id, None).await.unwrap();
    assert!(serde_json::to_string(&messages).unwrap().contains("hello"));
}

#[tokio::test]
async fn policy_denies_dangerous_tool() {
    let fake = provider(call("dangerous", &json!({})));
    let calls = Arc::new(AtomicUsize::new(0));
    let agent = Agent::builder()
        .memory()
        .provider(Arc::new(fake))
        .config(config())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .tool(native_tool("dangerous", &calls))
        .wasm_extension(fixture("deny-policy"))
        .build()
        .unwrap();
    agent
        .prompt(None, "dangerous")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn guest_allow_overrides_host_deny_and_ask_requires_approval() {
    let calls = Arc::new(AtomicUsize::new(0));
    let allow_agent = Agent::builder()
        .memory()
        .provider(Arc::new(provider(call("safe", &json!({})))))
        .config(config())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Deny)))
        .tool(native_tool("safe", &calls))
        .wasm_extension(fixture("deny-policy"))
        .build()
        .unwrap();
    allow_agent
        .prompt(None, "safe")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let ask_agent = Agent::builder()
        .memory()
        .provider(Arc::new(provider(call("ask_me", &json!({})))))
        .config(config())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .tool(native_tool("ask_me", &calls))
        .wasm_extension(fixture("deny-policy"))
        .build()
        .unwrap();
    ask_agent
        .prompt(None, "ask_me")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn policy_guest_receives_permission_and_run_context() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut info = native_tool("contextual", &calls).info.clone();
    info.required_permissions = vec!["network".into()];
    let agent = Agent::builder()
        .memory()
        .provider(Arc::new(provider(call("contextual", &json!({})))))
        .config(config())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Deny)))
        .tool(Arc::new(ToolDefinition {
            info,
            executor: Arc::new(CountTool(Arc::clone(&calls))),
        }))
        .wasm_extension(fixture("deny-policy"))
        .build()
        .unwrap();
    agent
        .prompt(None, "contextual")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn prompt_section_renders_for_each_run_with_its_run_id() {
    let fake = FakeProvider::scripted(vec![
        vec![StreamDelta::TextDelta("one".into()), StreamDelta::Completed],
        vec![StreamDelta::TextDelta("two".into()), StreamDelta::Completed],
    ]);
    let agent = Agent::builder()
        .memory()
        .provider(Arc::new(fake.clone()))
        .config(config())
        .wasm_extension(fixture("all-in-one"))
        .build()
        .unwrap();
    let first = agent.prompt(None, "first").await.unwrap();
    let first_id = first.run_id().to_string();
    first.done().await.unwrap();
    let second = agent.prompt(None, "second").await.unwrap();
    let second_id = second.run_id().to_string();
    second.done().await.unwrap();
    let requests = fake.requests();
    assert_eq!(requests.len(), 2);
    assert!(
        requests[0]
            .system
            .as_deref()
            .unwrap_or_default()
            .contains(&first_id)
    );
    assert!(
        requests[1]
            .system
            .as_deref()
            .unwrap_or_default()
            .contains(&second_id)
    );
    assert_ne!(first_id, second_id);
}

#[tokio::test]
async fn banner_reaches_model_request() {
    let fake = FakeProvider::scripted(vec![vec![
        StreamDelta::TextDelta("done".into()),
        StreamDelta::Completed,
    ]]);
    let agent = Agent::builder()
        .memory()
        .provider(Arc::new(fake.clone()))
        .config(config())
        .wasm_extension(fixture("banner-context"))
        .build()
        .unwrap();
    agent
        .prompt(None, "hello")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    assert!(
        fake.requests()[0]
            .system
            .as_deref()
            .unwrap_or_default()
            .contains("banner turn")
    );
}

#[tokio::test]
async fn middleware_rewrites_tool_output() {
    let fake = provider(call("native", &json!({})));
    let store = Arc::new(MemoryStore::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let agent = Agent::builder()
        .store(store.clone())
        .provider(Arc::new(fake))
        .config(config())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .tool(native_tool("native", &calls))
        .wasm_extension(fixture("redact-middleware"))
        .build()
        .unwrap();
    let result = agent
        .prompt(None, "native")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let messages = store.list_messages(&result.session_id, None).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(
        serde_json::to_string(&messages)
            .unwrap()
            .contains("[REDACTED]")
    );
}
