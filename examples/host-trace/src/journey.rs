use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, EventKind, EventRecord, ExtensionError, FakeProvider, Observer,
    PermissionDecision, Selection, StaticPolicy, StreamDelta, ToolDefinition, ToolExecutor,
    TraceContext,
    core::{RunId, ToolCallId, ToolInfo},
    runtime::ExecutionMode,
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

#[derive(Default)]
pub struct Capture {
    #[cfg(feature = "datadog")]
    pub exporter: Option<crabber::obs::DatadogObserver>,
    #[cfg(feature = "datadog")]
    pub live_exporter: Option<crabber::obs::DatadogObserver>,
    pub observation_attempt: Mutex<Option<RunId>>,
    pub events: Mutex<Vec<(EventRecord, Option<TraceContext>)>>,
    pub models: Mutex<Vec<(EventRecord, Option<TraceContext>)>>,
}
impl Observer for Capture {
    fn emit(&self, event: &EventRecord) {
        self.emit_with_context(event, None);
    }
    fn emit_with_context(&self, event: &EventRecord, context: Option<&TraceContext>) {
        #[cfg(feature = "datadog")]
        for exporter in self.exporter.iter().chain(self.live_exporter.iter()) {
            exporter.emit_with_context(event, context);
        }
        self.events
            .lock()
            .unwrap()
            .push((event.clone(), context.cloned()));
    }
    fn model_completed_with_context(&self, event: &EventRecord, context: Option<&TraceContext>) {
        #[cfg(feature = "datadog")]
        for exporter in self.exporter.iter().chain(self.live_exporter.iter()) {
            exporter.model_completed_with_context(event, context);
        }
        self.models
            .lock()
            .unwrap()
            .push((event.clone(), context.cloned()));
    }
    fn emit_in_attempt(
        &self,
        event: &EventRecord,
        context: Option<&TraceContext>,
        attempt: &RunId,
    ) {
        *self.observation_attempt.lock().unwrap() = Some(attempt.clone());
        #[cfg(feature = "datadog")]
        for exporter in self.exporter.iter().chain(self.live_exporter.iter()) {
            exporter.emit_in_attempt(event, context, attempt);
        }
        #[cfg(feature = "datadog")]
        crabber::obs::tracing_bridge::emit_with_context(event, context);
        #[cfg(not(feature = "datadog"))]
        let _ = attempt;
        self.events
            .lock()
            .unwrap()
            .push((event.clone(), context.cloned()));
    }
    fn model_completed_in_attempt(
        &self,
        event: &EventRecord,
        context: Option<&TraceContext>,
        attempt: &RunId,
    ) {
        *self.observation_attempt.lock().unwrap() = Some(attempt.clone());
        #[cfg(feature = "datadog")]
        for exporter in self.exporter.iter().chain(self.live_exporter.iter()) {
            exporter.model_completed_in_attempt(event, context, attempt);
        }
        #[cfg(not(feature = "datadog"))]
        let _ = attempt;
        self.models
            .lock()
            .unwrap()
            .push((event.clone(), context.cloned()));
    }
}
struct ParallelTool(Arc<tokio::sync::Barrier>);
#[async_trait]
impl ToolExecutor for ParallelTool {
    async fn execute(&self, arguments: Value) -> Result<Value, ExtensionError> {
        // This cannot complete under sequential execution: prove parallel task dispatch.
        self.0.wait().await;
        Ok(arguments)
    }
}

pub fn agent(capture: Arc<Capture>) -> Agent {
    let mut script = Vec::new();
    for _ in 0..2 {
        let call_id = ToolCallId::new();
        script.extend([
            StreamDelta::ToolCallStart {
                call_id: call_id.clone(),
                name: "echo".into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: call_id.clone(),
                text: r#"{"value":"ARGUMENT_SECRET"}"#.into(),
            },
            StreamDelta::ToolCallDone { call_id },
        ]);
    }
    script.push(StreamDelta::Completed);
    Agent::builder()
        .memory()
        .provider(Arc::new(FakeProvider::scripted(vec![
            script,
            vec![
                StreamDelta::TextDelta("OUTPUT_SECRET".into()),
                StreamDelta::Completed,
            ],
        ])))
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .observer(capture)
        .execution_mode(ExecutionMode::Parallel { max: 2 })
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "echo".into(),
                description: "safe demo".into(),
                parameters: json!({"type":"object"}),
                retry_safe: true,
                required_permissions: vec![],
            },
            executor: Arc::new(ParallelTool(Arc::new(tokio::sync::Barrier::new(2)))),
        }))
        .build()
        .unwrap()
}

pub fn host_context() -> TraceContext {
    let trace = std::env::var("CRABBER_HOST_TRACE_ID")
        .unwrap_or_else(|_| "1234567890abcdef1234567890abcdef".into());
    let span = std::env::var("CRABBER_HOST_SPAN_ID").unwrap_or_else(|_| "1234567890abcdef".into());
    TraceContext::new(&trace, &span).unwrap()
}

pub async fn journey() -> (RunId, usize, usize) {
    #[cfg(feature = "datadog")]
    let export = crate::export::ExportCapture::new();
    let capture = Arc::new(Capture {
        #[cfg(feature = "datadog")]
        exporter: Some(export.observer.clone()),
        #[cfg(feature = "datadog")]
        live_exporter: export.live.clone(),
        ..Capture::default()
    });
    let agent = agent(capture.clone());
    let context = host_context();
    let mut run = agent
        .prompt_with_context(
            None,
            "PROMPT_SECRET PRIVATE_PATH_SECRET CREDENTIAL_SECRET",
            Some(context.clone()),
        )
        .await
        .unwrap();
    let mut events = run.events();
    let mut broadcast = Vec::new();
    while let Some(event) = events.recv().await.unwrap() {
        broadcast.push(serde_json::to_value(event.as_ref()).unwrap());
    }
    let result = run.done().await.unwrap();
    let counts = {
        let captured = capture.events.lock().unwrap();
        assert_eq!(captured.len(), broadcast.len());
        for (event, observed) in captured.iter() {
            assert_eq!(observed.as_ref(), Some(&context));
            assert_eq!(
                broadcast
                    .iter()
                    .filter(|value| **value == serde_json::to_value(event).unwrap())
                    .count(),
                1
            );
            assert_eq!(event.run_id, result.run_id);
        }
        for kind in [
            EventKind::RunStarted,
            EventKind::RunSettled,
            EventKind::ToolCallRunning,
            EventKind::ToolCallSettled,
        ] {
            assert!(captured.iter().any(|(event, _)| event.kind == kind));
        }
        let models = capture.models.lock().unwrap();
        assert_eq!(models.len(), 2);
        assert!(
            models
                .iter()
                .all(|(event, observed)| event.run_id == result.run_id
                    && observed.as_ref() == Some(&context))
        );
        (result.run_id, captured.len(), models.len())
    };
    #[cfg(feature = "datadog")]
    export.finish(&context, counts.2).await;
    counts
}
