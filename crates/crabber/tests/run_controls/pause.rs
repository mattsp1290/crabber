use super::{
    contract::{agent, cancel_action, request},
    provider::Gate,
    store::TestStore,
};
use async_trait::async_trait;
use crabber::core::{RunStatus, ToolCallId, ToolInfo};
use crabber::session::{RunControlError, RunControlStatus, Store, WaitingCancellation};
use crabber::{
    Agent, AgentConfig, ExtensionError, FakeProvider, InputFingerprint, JsonToolExecutor,
    RuntimeError, Selection, StreamDelta, ToolDefinition,
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
fn worker(store: Arc<dyn Store>, gate: &Gate, effects: Arc<AtomicUsize>) -> Agent {
    Agent::builder()
        .store(store)
        .provider(Arc::new(gate.clone()))
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .behavior_fingerprint(InputFingerprint::new("b".repeat(64)).unwrap())
        .policy(Arc::new(Pause))
        .shared_tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "echo".into(),
                description: "echo".into(),
                parameters: json!({"type":"object"}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(Echo(effects)),
        }))
        .build()
        .unwrap()
}
pub(super) async fn run(native: Arc<dyn Store>) {
    for loss in [0, 1, 2] {
        race(native.clone(), loss).await;
    }
}
fn gate() -> Gate {
    let mut gate = Gate::new(0);
    let call = ToolCallId::new();
    gate.fake = FakeProvider::scripted(vec![vec![
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
    ]]);
    gate
}
async fn race(native: Arc<dyn Store>, loss: usize) {
    let wrapped = Arc::new(TestStore::new(native.clone()));
    wrapped.hold_pause.store(true, Ordering::SeqCst);
    wrapped
        .skip_control_observation
        .store(true, Ordering::SeqCst);
    wrapped.waiting_loss.store(loss, Ordering::SeqCst);
    let effects = Arc::new(AtomicUsize::new(0));
    let gate = gate();
    let worker = worker(wrapped.clone(), &gate, effects.clone());
    let controller = agent(native.clone(), &gate);
    let handle = worker
        .prompt(None, "cancel during pause commit")
        .await
        .unwrap();
    gate.entered.notified().await;
    gate.release.notify_one();
    wrapped.pause_entered.notified().await;
    let target = controller
        .run_control_binding(handle.session_id(), handle.run_id())
        .await
        .unwrap()
        .target;
    let cancel = request(native.as_ref(), &target, cancel_action()).await;
    controller.control_run(cancel.clone()).await.unwrap();
    assert_eq!(
        controller.run_controls(&target).await.unwrap().controls[0].status,
        RunControlStatus::Accepted
    );
    wrapped.pause_release.notify_one();
    let result = handle.done().await;
    if loss == 0 {
        assert_eq!(result.unwrap().status, RunStatus::Interrupted);
    } else {
        assert!(matches!(
            result,
            Err(RuntimeError::RunControl(
                RunControlError::UnknownStoreOutcome
            ))
        ));
    }
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert_eq!(
        wrapped.terminal_calls.load(Ordering::SeqCst),
        0,
        "host completion preserves original worker suspension"
    );
    let restored = agent(native.clone(), &gate);
    restored.close_extensions().await.unwrap();
    let progress = WaitingCancellation {
        target: target.clone(),
        original_cancel_id: cancel.id,
        expected_revision: native.session_revision(&target.session_id).await.unwrap(),
    };
    let summary = restored
        .settle_waiting_cancellation(progress.clone())
        .await
        .unwrap();
    assert_eq!(
        summary.status,
        RunControlStatus::TerminalCommitted {
            status: RunStatus::Interrupted
        }
    );
    let mut stale = progress;
    stale.expected_revision = crabber::core::SessionRevision::INITIAL;
    assert_eq!(
        restored.settle_waiting_cancellation(stale).await.unwrap(),
        summary
    );
    assert_eq!(
        native
            .get_run(&target.run_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Interrupted
    );
    assert_eq!(
        native
            .list_unfinished_tool_calls(&target.run_id)
            .await
            .unwrap(),
        []
    );
    assert_eq!(gate.fake.requests().len(), 1);
}
