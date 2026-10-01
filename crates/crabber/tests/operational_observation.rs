use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, EventKind, EventRecord, FakeProvider, MonotonicClock, Observer,
    OperationKind, OperationalObservation, PermissionDecision, Selection, StaticPolicy,
    StreamDelta, TerminalReason, ToolDefinition, ToolExecutor, TraceContext,
    core::{RunId, ToolCallId, ToolInfo},
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
#[derive(Default)]
struct Clock(AtomicU64);
impl MonotonicClock for Clock {
    fn now(&self) -> Duration {
        Duration::from_millis(self.0.load(Ordering::SeqCst))
    }
}
#[derive(Default)]
struct Capture(Mutex<Vec<(OperationalObservation, Option<TraceContext>, RunId)>>);
impl Observer for Capture {
    fn emit(&self, _: &EventRecord) {}
    fn operational_completed_in_attempt(
        &self,
        observation: &OperationalObservation,
        context: Option<&TraceContext>,
        attempt: &RunId,
    ) {
        self.0
            .lock()
            .unwrap()
            .push((observation.clone(), context.cloned(), attempt.clone()));
    }
}
struct Echo(Arc<Clock>);
#[async_trait]
impl ToolExecutor for Echo {
    async fn execute(&self, value: Value) -> Result<Value, crabber::ExtensionError> {
        self.0.0.store(12, Ordering::SeqCst);
        Ok(value)
    }
}
#[tokio::test]
async fn composed_callbacks_keep_attempt_context_and_broadcast_native_tool_turn() {
    let first = Arc::new(Capture::default());
    let second = Arc::new(Capture::default());
    let clock = Arc::new(Clock::default());
    let id = ToolCallId::new();
    let agent = Agent::builder()
        .memory()
        .monotonic_clock(clock.clone())
        .provider(Arc::new(FakeProvider::scripted(vec![
            vec![
                StreamDelta::ToolCallStart {
                    call_id: id.clone(),
                    name: "echo".into(),
                },
                StreamDelta::ToolCallArgsDelta {
                    call_id: id.clone(),
                    text: r#"{"text":"SECRET argument"}"#.into(),
                },
                StreamDelta::ToolCallDone { call_id: id },
                StreamDelta::Completed,
            ],
            vec![
                StreamDelta::TextDelta("SECRET token".into()),
                StreamDelta::TextDelta("more".into()),
                StreamDelta::Completed,
            ],
        ])))
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .observer(first.clone())
        .observer(second.clone())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "echo".into(),
                description: "echo".into(),
                parameters: json!({"type":"object"}),
                retry_safe: true,
                required_permissions: vec![],
            },
            executor: Arc::new(Echo(clock)),
        }))
        .build()
        .unwrap();
    let context = TraceContext::new("1234567890abcdef", "1234567890abcdef").unwrap();
    let mut handle = agent
        .prompt_with_context(None, "SECRET prompt", Some(context.clone()))
        .await
        .unwrap();
    let mut events = Vec::new();
    let mut receiver = handle.events();
    while let Some(event) = receiver.recv().await.unwrap() {
        events.push(event);
    }
    let result = handle.done().await.unwrap();
    let first = first.0.lock().unwrap();
    let second = second.0.lock().unwrap();
    assert_eq!(*first, *second);
    assert_eq!(first.len(), 4);
    assert!(
        first
            .iter()
            .all(|(value, observed, attempt)| value.run_id == result.run_id
                && observed.as_ref() == Some(&context)
                && attempt == &first[0].2
                && value.reason == TerminalReason::Success)
    );
    assert_eq!(first[1].0.elapsed, Duration::from_millis(12));
    assert!(matches!(first[1].0.kind, OperationKind::Tool { .. }));
    assert_eq!(first[0].0.first_token, None);
    assert_eq!(first[2].0.first_token, Some(Duration::ZERO));
    assert_eq!(first[3].0.kind, OperationKind::Run);
    assert_eq!(first[3].0.elapsed, Duration::from_millis(12));
    assert!(!format!("{first:?}").contains("SECRET"));
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == EventKind::RunSettled)
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == EventKind::ToolCallSettled)
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == EventKind::TextDelta)
            .count(),
        2
    );
}
