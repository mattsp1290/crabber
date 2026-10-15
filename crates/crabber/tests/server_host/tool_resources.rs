//! Public ownership probes; logical outcome and native resource cleanup differ.
use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, ExtensionError, FakeProvider, JsonToolExecutor, PermissionDecision,
    Selection, StaticPolicy, StreamDelta, ToolDefinition,
    core::{RunStatus, ToolCallId, ToolInfo},
    extension::{ResourceFailure, ResourceRegistrationError, ToolContext, ToolResources},
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::{
    sync::Notify,
    time::{Duration, timeout},
};

const BOUND: Duration = Duration::from_secs(3);
#[derive(Default)]
struct Fixture {
    entered: Notify,
    release: Notify,
    resources: Mutex<Option<ToolResources>>,
}
struct ResourceTool(Arc<Fixture>);
#[async_trait]
impl JsonToolExecutor for ResourceTool {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        Err(ExtensionError::Tool("context required".into()))
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        input: Value,
    ) -> Result<Value, ExtensionError> {
        let fixture = self.0.clone();
        let mode = input["mode"].as_str().unwrap().to_owned();
        let closing = context.resources().closing().unwrap();
        context
            .resources()
            .spawn(async move {
                if mode == "cooperative" {
                    closing.await;
                } else {
                    fixture.release.notified().await;
                }
                if mode == "failed" {
                    Err(ResourceFailure::CleanupFailed)
                } else {
                    Ok(())
                }
            })
            .unwrap();
        *self.0.resources.lock().unwrap() = Some(context.resources().clone());
        self.0.entered.notify_one();
        if input["block_body"] == true {
            std::future::pending::<()>().await;
        }
        Ok(json!({"done":true}))
    }
}
fn agent(fixture: Arc<Fixture>, mode: &str, block_body: bool) -> Agent {
    let call_id = ToolCallId::new();
    let script = vec![
        StreamDelta::ToolCallStart {
            call_id: call_id.clone(),
            name: "resources".into(),
        },
        StreamDelta::ToolCallArgsDelta {
            call_id: call_id.clone(),
            text: json!({"mode":mode,"block_body":block_body}).to_string(),
        },
        StreamDelta::ToolCallDone { call_id },
        StreamDelta::Completed,
    ];
    Agent::builder()
        .behavior_version("tool-resource-probe.v1")
        .memory()
        .provider(Arc::new(FakeProvider::scripted(vec![
            script,
            vec![
                StreamDelta::TextDelta("done".into()),
                StreamDelta::Completed,
            ],
        ])))
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "resources".into(),
                description: "owned task fixture".into(),
                parameters: json!({"type":"object"}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(ResourceTool(fixture)),
        }))
        .build()
        .unwrap()
}

#[tokio::test]
async fn incomplete_worker_retains_noncooperative_resource_join_after_timeout() {
    let fixture = Arc::new(Fixture::default());
    let agent = agent(fixture.clone(), "noncooperative", false);
    let handle = agent.prompt(None, "work").await.unwrap();
    let run_id = handle.run_id().clone();
    assert!(matches!(
        timeout(BOUND, handle.done()).await.unwrap(),
        Err(crabber::RuntimeError::AttemptCleanupIncomplete { .. })
    ));
    let report = agent.drain_execution_tasks(Duration::from_millis(1)).await;
    assert_eq!(report.resources.len(), 1);
    assert_eq!(report.resources[0].run_id, run_id);
    assert!(report.resources[0].resources.sealed);
    assert_eq!(report.resources[0].resources.pending, 1);
    let retained = fixture.resources.lock().unwrap().take().unwrap();
    assert_eq!(
        retained.spawn(async { Ok(()) }),
        Err(ResourceRegistrationError::Sealed)
    );
    drop(retained);
    fixture.release.notify_one();
    let report = agent.drain_execution_tasks(BOUND).await;
    assert_eq!(report.resources[0].resources.joined, 1);
    assert_eq!(report.resources[0].resources.pending, 0);
    assert_eq!(report.resources[0].resources.failures, []);
    assert_eq!(
        agent.drain_execution_tasks(Duration::ZERO).await.resources,
        []
    );
}

#[tokio::test]
async fn interrupted_tool_seals_scope_and_joins_cooperative_resources() {
    let fixture = Arc::new(Fixture::default());
    let agent = agent(fixture.clone(), "cooperative", true);
    let handle = agent.prompt(None, "interrupt").await.unwrap();
    timeout(BOUND, fixture.entered.notified()).await.unwrap();
    handle.interrupt();
    assert_eq!(
        timeout(BOUND, handle.done()).await.unwrap().unwrap().status,
        RunStatus::Interrupted
    );
    let report = agent.drain_execution_tasks(BOUND).await;
    assert_eq!(report.resources.len(), 1);
    assert_eq!(report.resources[0].resources.joined, 1);
    assert_eq!(report.resources[0].resources.pending, 0);
    assert_eq!(report.resources[0].resources.failures, []);
}

#[tokio::test]
async fn resource_failure_blocks_worker_success_and_remains_visible_across_drains() {
    let fixture = Arc::new(Fixture::default());
    let agent = agent(fixture.clone(), "failed", false);
    let handle = agent.prompt(None, "fail cleanup").await.unwrap();
    timeout(BOUND, fixture.entered.notified()).await.unwrap();
    fixture.release.notify_one();
    assert!(matches!(
        timeout(BOUND, handle.done()).await.unwrap(),
        Err(crabber::RuntimeError::AttemptCleanupIncomplete { .. })
    ));
    for _ in 0..2 {
        let report = agent.drain_execution_tasks(BOUND).await;
        assert_eq!(report.resources[0].resources.pending, 0);
        assert_eq!(
            report.resources[0].resources.failures,
            [ResourceFailure::CleanupFailed]
        );
    }
}

#[tokio::test]
async fn snapshot_drain_does_not_seal_an_active_tool_scope() {
    let fixture = Arc::new(Fixture::default());
    let agent = agent(fixture.clone(), "cooperative", true);
    let handle = agent.prompt(None, "active").await.unwrap();
    let run_id = handle.run_id().clone();
    timeout(BOUND, fixture.entered.notified()).await.unwrap();
    let report = agent.drain_execution_tasks(Duration::from_millis(1)).await;
    assert!(report.pending.contains(&run_id));
    assert_eq!(report.resources.len(), 1);
    assert!(!report.resources[0].resources.sealed);
    fixture
        .resources
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .spawn(async { Ok(()) })
        .unwrap();
    handle.interrupt();
    assert_eq!(
        timeout(BOUND, handle.done()).await.unwrap().unwrap().status,
        RunStatus::Interrupted
    );
    let report = agent.drain_execution_tasks(BOUND).await;
    assert_eq!(report.resources[0].resources.joined, 2);
    assert_eq!(report.resources[0].resources.pending, 0);
    assert_eq!(report.resources[0].resources.failures, []);
}
