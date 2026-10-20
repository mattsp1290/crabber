//! Continuation restores the admitted provider/model instead of new defaults.
use super::fixture::*;
use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, ExtensionError, FakeProvider, InputFingerprint, JsonToolExecutor,
    StreamDelta, ToolDefinition,
    core::{RunStatus, ToolCallId, ToolInfo},
    session::Store,
};
use crabber_runtime::{InterruptPolicy, PermissionDecision, PermissionPolicy};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
struct Pause;
impl PermissionPolicy for Pause {
    fn decide(&self, _: &ToolInfo, _: &Value) -> PermissionDecision {
        PermissionDecision::Allow
    }
    fn interrupt_policy(&self, _: &ToolInfo, _: &Value) -> InterruptPolicy {
        InterruptPolicy::Pause
    }
}
struct Echo(Arc<AtomicUsize>);
#[async_trait]
impl JsonToolExecutor for Echo {
    async fn execute(&self, input: Value) -> Result<Value, ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(input)
    }
}
fn host(native: Arc<dyn Store>, probe: &Probe, effects: Arc<AtomicUsize>) -> Agent {
    let mut config = AgentConfig::new(selection("host-provider", "host-model"));
    config.workspace_id = "defaults-workspace".into();
    config.directory = "/defaults".into();
    Agent::builder()
        .behavior_fingerprint(InputFingerprint::new("b".repeat(64)).unwrap())
        .store(native)
        .provider(Arc::new(probe.clone()))
        .config(config)
        .policy(Arc::new(Pause))
        .shared_tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "echo".into(),
                description: "echo".into(),
                parameters: json!({"type":"object"}),
                retry_safe: true,
                required_permissions: vec![],
            },
            executor: Arc::new(Echo(effects)),
        }))
        .build()
        .unwrap()
}
pub(super) async fn run(native: Arc<dyn Store>) {
    let session = create(
        native.as_ref(),
        defaults(Some("frozen-provider"), Some("frozen-model")),
    )
    .await;
    let mut probe = Probe::new(0);
    let call = ToolCallId::new();
    probe.fake = FakeProvider::scripted(vec![
        vec![
            StreamDelta::ToolCallStart {
                call_id: call.clone(),
                name: "echo".into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: call.clone(),
                text: "{}".into(),
            },
            StreamDelta::ToolCallDone { call_id: call },
            StreamDelta::Completed,
        ],
        vec![
            StreamDelta::TextDelta("continued".into()),
            StreamDelta::Completed,
        ],
    ]);
    let effects = Arc::new(AtomicUsize::new(0));
    let original = host(native.clone(), &probe, effects.clone());
    let handle = original
        .prompt_session(session.clone(), "pause", None)
        .await
        .unwrap();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    update(
        native.as_ref(),
        &session,
        defaults(Some("changed-provider"), Some("changed-model")),
    )
    .await;
    original.close_extensions().await.unwrap();
    let restored = host(native, &probe, effects.clone());
    let handle = restored.resume_execution(&run).await.unwrap();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(probe.fake.requests().len(), 2);
    assert!(
        probe
            .fake
            .requests()
            .iter()
            .all(|request| request.selection == selection("frozen-provider", "frozen-model"))
    );
}
