use async_trait::async_trait;
use crabber::{
    Agent, AgentBuilder, AgentConfig, ExtensionError, FakeProvider, JsonToolExecutor,
    PermissionDecision, Selection, StaticPolicy, StreamDelta, ToolDefinition,
    core::{ContentBlock, RunStatus, ToolCallId, ToolInfo},
    extension::ToolContext,
    session::{MemoryStore, Store},
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::{
    sync::Notify,
    time::{Duration, timeout},
};

const BOUND: Duration = Duration::from_secs(3);
#[derive(Default)]
struct Probes {
    entered: Notify,
    release: Notify,
    captured: Mutex<Option<ToolContext>>,
}
struct ProgressTool(Arc<Probes>);
#[async_trait]
impl JsonToolExecutor for ProgressTool {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        Err(ExtensionError::Tool("context required".into()))
    }
    async fn execute_with_context(
        &self,
        context: ToolContext,
        input: Value,
    ) -> Result<Value, ExtensionError> {
        match input["mode"].as_str().unwrap() {
            "slow" => {
                self.0.entered.notify_one();
                self.0.release.notified().await;
            }
            "noise" => {
                for index in 0..600 {
                    context.progress(ContentBlock::Text {
                        text: format!("noise-{index}"),
                    });
                }
            }
            "capture" => *self.0.captured.lock().unwrap() = Some(context),
            _ => unreachable!(),
        }
        Ok(json!({"done":true}))
    }
}
fn call(mode: &str) -> Vec<StreamDelta> {
    let call_id = ToolCallId::new();
    vec![
        StreamDelta::ToolCallStart {
            call_id: call_id.clone(),
            name: "progress".into(),
        },
        StreamDelta::ToolCallArgsDelta {
            call_id: call_id.clone(),
            text: json!({"mode":mode}).to_string(),
        },
        StreamDelta::ToolCallDone { call_id },
        StreamDelta::Completed,
    ]
}
fn done() -> Vec<StreamDelta> {
    vec![
        StreamDelta::TextDelta("done".into()),
        StreamDelta::Completed,
    ]
}
fn builder(
    scripts: Vec<Vec<StreamDelta>>,
    probes: Arc<Probes>,
) -> (AgentBuilder, Arc<MemoryStore>) {
    let store = Arc::new(MemoryStore::new());
    let agent = Agent::builder().behavior_version("execution-event-probes.v1")
        .store(store.clone()).provider(Arc::new(FakeProvider::scripted(scripts)))
        .config(AgentConfig::new(Selection { provider_id: "fake".into(), model_id: "scripted".into() }))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(Arc::new(ToolDefinition { info: ToolInfo { name: "progress".into(), description: "progress fixture".into(), parameters: json!({"type":"object","properties":{"mode":{"type":"string"}},"required":["mode"]}), retry_safe: false, required_permissions: vec![] }, executor: Arc::new(ProgressTool(probes)) }))
        ;
    (agent, store)
}
fn agent(scripts: Vec<Vec<StreamDelta>>, probes: Arc<Probes>) -> (Agent, Arc<MemoryStore>) {
    let (builder, store) = builder(scripts, probes);
    (builder.build().unwrap(), store)
}
struct PauseTools;
impl crabber::runtime::PermissionPolicy for PauseTools {
    fn decide(&self, _: &ToolInfo, _: &Value) -> PermissionDecision {
        PermissionDecision::Allow
    }
    fn interrupt_policy(&self, _: &ToolInfo, _: &Value) -> crabber::runtime::InterruptPolicy {
        crabber::runtime::InterruptPolicy::Pause
    }
}
fn paused_agent(probes: Arc<Probes>) -> (Agent, Arc<MemoryStore>) {
    let (builder, store) = builder(vec![call("slow"), done()], probes);
    (
        builder
            .behavior_version("paused-execution-event-probes.v1")
            .policy(Arc::new(PauseTools))
            .build()
            .unwrap(),
        store,
    )
}
async fn collect(mut events: crabber::RunEvents) -> Vec<Arc<crabber::core::EventRecord>> {
    timeout(BOUND, async {
        let mut records = Vec::new();
        while let Some(event) = events.recv().await.unwrap() {
            records.push(event);
        }
        records
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn another_session_cannot_saturate_an_execution_event_queue() {
    let probes = Arc::new(Probes::default());
    let (agent, store) = agent(
        vec![call("slow"), call("noise"), done(), done()],
        probes.clone(),
    );
    let mut slow = agent.prompt(None, "slow").await.unwrap();
    let session = slow.session_id().clone();
    let run = slow.run_id().clone();
    let mut events = slow.events();
    timeout(BOUND, probes.entered.notified()).await.unwrap();
    let noisy = agent.prompt(None, "noisy").await.unwrap();
    timeout(BOUND, noisy.done()).await.unwrap().unwrap();
    assert_eq!(
        store.get_run(&run).await.unwrap().unwrap().status,
        RunStatus::Running
    );
    probes.release.notify_one();
    timeout(BOUND, slow.done()).await.unwrap().unwrap();
    let mut count = 0;
    timeout(BOUND, async {
        while let Some(event) = events.recv().await.unwrap() {
            assert_eq!(event.session_id, session);
            assert_eq!(event.run_id, run);
            count += 1;
        }
    })
    .await
    .unwrap();
    assert!(count > 0);
    assert_eq!(
        store.get_run(&run).await.unwrap().unwrap().status,
        RunStatus::Completed
    );
}
#[tokio::test]
async fn lag_ends_only_observation_and_cannot_be_resumed_as_a_complete_stream() {
    let (agent, store) = agent(vec![call("noise"), done()], Arc::new(Probes::default()));
    let mut handle = agent.prompt(None, "flood").await.unwrap();
    let run = handle.run_id().clone();
    let mut events = handle.events();
    assert_eq!(
        timeout(BOUND, handle.done()).await.unwrap().unwrap().status,
        RunStatus::Completed
    );
    assert!(matches!(
        events.recv().await,
        Err(tokio::sync::broadcast::error::RecvError::Lagged(_))
    ));
    assert!(events.recv().await.unwrap().is_none());
    assert_eq!(
        store.get_run(&run).await.unwrap().unwrap().status,
        RunStatus::Completed
    );
}
#[tokio::test]
async fn dropping_the_observer_does_not_cancel_owned_work() {
    let probes = Arc::new(Probes::default());
    let (agent, store) = agent(vec![call("slow"), done()], probes.clone());
    let mut handle = agent.prompt(None, "slow").await.unwrap();
    let run = handle.run_id().clone();
    drop(handle.events());
    timeout(BOUND, probes.entered.notified()).await.unwrap();
    probes.release.notify_one();
    assert_eq!(
        timeout(BOUND, handle.done()).await.unwrap().unwrap().status,
        RunStatus::Completed
    );
    assert_eq!(
        store.get_run(&run).await.unwrap().unwrap().status,
        RunStatus::Completed
    );
}
#[tokio::test]
async fn retained_progress_context_cannot_publish_into_an_ended_execution() {
    let probes = Arc::new(Probes::default());
    let (agent, _) = agent(vec![call("capture"), done()], probes.clone());
    let mut handle = agent.prompt(None, "capture").await.unwrap();
    let mut events = handle.events();
    timeout(BOUND, handle.done()).await.unwrap().unwrap();
    probes
        .captured
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .progress(ContentBlock::Text {
            text: "LATE_PROGRESS_SENTINEL".into(),
        });
    timeout(BOUND, async {
        while let Some(event) = events.recv().await.unwrap() {
            assert!(!event.payload.to_string().contains("LATE_PROGRESS_SENTINEL"));
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn initial_and_resumed_workers_with_the_same_run_have_separate_event_streams() {
    use crabber::core::EventKind;
    let probes = Arc::new(Probes::default());
    let (agent, store) = paused_agent(probes.clone());
    let mut initial = agent.prompt(None, "pause").await.unwrap();
    let session = initial.session_id().clone();
    let run = initial.run_id().clone();
    let initial_events = initial.events();
    assert_eq!(initial.done().await.unwrap().status, RunStatus::Paused);
    let mut resumed = agent.resume_execution(&run).await.unwrap();
    assert_eq!(resumed.run_id(), &run);
    assert_eq!(resumed.session_id(), &session);
    let resumed_events = resumed.events();
    timeout(BOUND, probes.entered.notified()).await.unwrap();
    probes.release.notify_one();
    assert_eq!(
        timeout(BOUND, resumed.done())
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Completed
    );
    // Deliberately read the original exchange only after continuation finishes.
    let original = collect(initial_events).await;
    assert!(
        original
            .iter()
            .any(|event| event.kind == EventKind::RunPaused)
    );
    assert!(
        !original
            .iter()
            .any(|event| event.kind == EventKind::RunResumed)
    );
    let continuation = collect(resumed_events).await;
    assert!(
        continuation
            .iter()
            .any(|event| event.kind == EventKind::RunResumed)
    );
    assert!(
        !continuation
            .iter()
            .any(|event| event.kind == EventKind::RunAdmitted)
    );
    assert!(
        continuation
            .iter()
            .all(|event| event.run_id == run && event.session_id == session)
    );
    assert_eq!(
        store.get_run(&run).await.unwrap().unwrap().status,
        RunStatus::Completed
    );
}
#[tokio::test]
async fn resumed_handle_interrupt_reports_phase_uncertainty_without_a_terminal_event() {
    use crabber::core::EventKind;
    let probes = Arc::new(Probes::default());
    let (agent, store) = paused_agent(probes.clone());
    let initial = agent.prompt(None, "pause").await.unwrap();
    let run = initial.run_id().clone();
    assert_eq!(initial.done().await.unwrap().status, RunStatus::Paused);
    let mut resumed = agent.resume_execution(&run).await.unwrap();
    let events = resumed.events();
    timeout(BOUND, probes.entered.notified()).await.unwrap();
    let control = resumed.control().clone();
    assert_eq!(control.run_id(), &run);
    control.interrupt();
    let crabber::RuntimeError::PhaseOutcomeUnknown { phase, .. } =
        timeout(BOUND, resumed.done()).await.unwrap().unwrap_err()
    else {
        panic!("interrupted executor requires reconciliation")
    };
    let persisted = store.get_run(&run).await.unwrap().unwrap();
    assert_eq!(persisted.status, RunStatus::Running);
    assert_eq!(
        store.tool_phase(&run, &phase.id).await.unwrap().phase.state,
        crabber::session::PhaseState::Uncertain
    );
    let records = collect(events).await;
    assert!(!records.iter().any(|event| matches!(
        event.kind,
        EventKind::RunSettled | EventKind::ToolCallSettled
    )));
    let durable = store
        .list_events(&persisted.session_id, None, 1000)
        .await
        .unwrap();
    assert!(!durable.iter().any(|event| matches!(
        event.kind,
        EventKind::RunSettled | EventKind::ToolCallSettled
    )));
}
