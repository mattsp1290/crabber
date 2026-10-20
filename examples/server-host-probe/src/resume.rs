//! Legal native Paused resume and exact selected-session original task joins.
use super::{Error, call, done, selection};
use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, ExtensionError, FakeProvider, JsonToolExecutor, PermissionDecision,
    ToolDefinition,
    core::{RunStatus, SessionId, ToolInfo},
    extension::ToolContext,
    runtime::{InterruptPolicy, PermissionPolicy},
    session::Store,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{sync::Notify, time::timeout};
const BOUND: Duration = Duration::from_secs(30);
#[derive(Default)]
struct Gate {
    entered: Notify,
    release: Notify,
    effects: AtomicUsize,
}
struct Release([Arc<Gate>; 2]);
impl Drop for Release {
    fn drop(&mut self) {
        for gate in &self.0 {
            gate.release.notify_one();
        }
    }
}
#[derive(Default)]
struct Tool(Mutex<BTreeMap<SessionId, Arc<Gate>>>);
#[async_trait]
impl JsonToolExecutor for Tool {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        Err(ExtensionError::Rejected(
            "actual invocation context required",
        ))
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        _: Value,
    ) -> Result<Value, ExtensionError> {
        let gate = self
            .0
            .lock()
            .unwrap()
            .get(&context.session_id)
            .cloned()
            .ok_or(ExtensionError::Rejected("missing original session gate"))?;
        gate.entered.notify_one();
        gate.release.notified().await;
        gate.effects.fetch_add(1, Ordering::SeqCst);
        Ok(json!({"resumed":true}))
    }
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
fn host(store: Arc<dyn Store>, provider: &FakeProvider, tool: Arc<Tool>) -> Result<Agent, Error> {
    Ok(Agent::builder()
        .behavior_version("public-resume-drain-probe.v1")
        .store(store)
        .provider(Arc::new(provider.clone()))
        .config(AgentConfig::new(selection()))
        .policy(Arc::new(Pause))
        .shared_tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "gated_resume".into(),
                description: "Resume once in its original session".into(),
                parameters: json!({"type":"object"}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: tool,
        }))
        .build()?)
}
pub(crate) async fn run(store: Arc<dyn Store>) -> Result<usize, Error> {
    let tool = Arc::new(Tool::default());
    let provider = FakeProvider::scripted(vec![
        call("gated_resume"),
        call("gated_resume"),
        done(),
        done(),
    ]);
    let original = host(store.clone(), &provider, tool.clone())?;
    let mut originals = Vec::new();
    for _ in 0..2 {
        let handle = original.prompt(None, "pause before the effect").await?;
        let scope = (handle.session_id().clone(), handle.run_id().clone());
        assert_eq!(handle.done().await?.status, RunStatus::Paused);
        originals.push(scope);
    }
    assert_eq!(provider.requests().len(), 2);
    original.close_extensions().await?;
    let gates = [Arc::new(Gate::default()), Arc::new(Gate::default())];
    let _release = Release(gates.clone());
    for ((session, _), gate) in originals.iter().zip(&gates) {
        tool.0.lock().unwrap().insert(session.clone(), gate.clone());
    }
    let restored = host(store.clone(), &provider, tool)?;
    let left = restored.resume_execution(&originals[0].1).await?;
    let right = restored.resume_execution(&originals[1].1).await?;
    let controls = [left.control(), right.control()];
    for ((session, run), control) in originals.iter().zip(&controls) {
        assert_eq!(control.session_id(), session);
        assert_eq!(control.run_id(), run);
        assert!(!*control.completion_signal().borrow());
    }
    for gate in &gates {
        timeout(BOUND, gate.entered.notified()).await?;
    }
    join_selected_sessions(&restored, &originals, &gates, &controls).await?;
    assert_eq!(left.done().await?.status, RunStatus::Completed);
    assert_eq!(right.done().await?.status, RunStatus::Completed);
    for ((_, run), gate) in originals.iter().zip(&gates) {
        assert_eq!(
            store.get_run(run).await?.unwrap().status,
            RunStatus::Completed
        );
        assert_eq!(gate.effects.load(Ordering::SeqCst), 1);
    }
    assert_eq!(provider.requests().len(), 4);
    restored.close_extensions().await?;
    Ok(2)
}

async fn join_selected_sessions(
    host: &Agent,
    originals: &[(SessionId, crabber::core::RunId)],
    gates: &[Arc<Gate>; 2],
    controls: &[crabber::runtime::RunControl; 2],
) -> Result<(), Error> {
    let pending = host
        .drain_session_execution_tasks(&originals[0].0, Duration::from_millis(1))
        .await;
    assert_eq!(pending.pending, vec![originals[0].1.clone()]);
    assert!(
        pending
            .joined
            .iter()
            .all(|joined| joined.session_id == originals[0].0)
    );
    gates[0].release.notify_one();
    let joined = host
        .drain_session_execution_tasks(&originals[0].0, BOUND)
        .await;
    assert_eq!(joined.pending, Vec::<crabber::core::RunId>::new());
    assert_eq!(
        joined.pending_preparations,
        Vec::<crabber::runtime::PreparationTaskId>::new()
    );
    assert!(
        joined
            .joined
            .iter()
            .any(|joined| joined.run_id == originals[0].1)
    );
    assert!(
        joined
            .joined
            .iter()
            .all(|joined| joined.session_id == originals[0].0)
    );
    assert!(*controls[0].completion_signal().borrow());
    assert!(!*controls[1].completion_signal().borrow());
    assert_eq!(gates[0].effects.load(Ordering::SeqCst), 1);
    assert_eq!(gates[1].effects.load(Ordering::SeqCst), 0);
    let other = host
        .drain_session_execution_tasks(&originals[1].0, Duration::ZERO)
        .await;
    assert_eq!(other.pending, vec![originals[1].1.clone()]);
    gates[1].release.notify_one();
    let other = host
        .drain_session_execution_tasks(&originals[1].0, BOUND)
        .await;
    assert_eq!(other.pending, Vec::<crabber::core::RunId>::new());
    assert!(
        other
            .joined
            .iter()
            .any(|joined| joined.run_id == originals[1].1)
    );
    Ok(())
}
