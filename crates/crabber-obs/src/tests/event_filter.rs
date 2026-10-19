use super::{config, event};
use crate::{DatadogObserver, RedactionPolicy, SafeEvent, log, series, spans};
use crabber_core::{EventKind, RunId, SessionId};
use crabber_runtime::Observer;
use serde_json::{Value, json};

fn inert_kinds() -> Vec<EventKind> {
    vec![
        EventKind::RunControlChanged,
        EventKind::InteractionChanged,
        EventKind::ContinuationChanged,
        EventKind::ContinuationWaiting,
        EventKind::ExecutionAttemptChanged,
        EventKind::ToolPhaseChanged,
        EventKind::PublicStateSnapshot,
        EventKind::PublicStateDelta,
        EventKind::PublicPresentation,
        EventKind::ActivitySnapshot,
        EventKind::ActivityDelta,
        EventKind::InvocationStarted,
        EventKind::InvocationFinished,
        EventKind::InvocationError,
        EventKind::ToolProgress,
        EventKind::TurnStarted,
        EventKind::TurnCompleted,
        EventKind::MessageStarted,
        EventKind::MessageStreamEnded,
        EventKind::ToolCallStarted,
        EventKind::ToolCallArgsDelta,
        EventKind::ToolCallArgsCompleted,
        EventKind::MessageCommitted,
        EventKind::TextDelta,
        EventKind::ReasoningDelta,
        EventKind::ToolCallPending,
        EventKind::ToolCallRunning,
        EventKind::PermissionRequested,
        EventKind::RunPaused,
        EventKind::ContextEpochStarted,
        EventKind::ExtensionNotice,
        EventKind::Custom {
            name: "private_event".repeat(1024),
        },
        EventKind::Custom {
            name: "MODEL_CALL".into(),
        },
        EventKind::Custom {
            name: "model_call ".into(),
        },
        EventKind::Custom {
            name: " wasm_call".into(),
        },
    ]
}

#[tokio::test]
async fn inert_events_never_enter_any_observer_queue_path() {
    let mut configuration = config();
    configuration.channel_capacity = 1;
    let observer = DatadogObserver::new(&configuration);
    let session = SessionId::new();
    let run = RunId::new();
    let policy = RedactionPolicy {
        capture_input_summary: true,
        capture_output_summary: true,
        max_summary_bytes: 4096,
    };
    for kind in inert_kinds() {
        let record = event(
            kind,
            &session,
            &run,
            json!({
                "input_summary": "PRIVATE_INPUT_SENTINEL",
                "output_summary": "PRIVATE_OUTPUT_SENTINEL",
            }),
        );
        assert!(SafeEvent::from_event(&record, &policy).is_none());
        observer.emit(&record);
        observer.emit_with_context(&record, None);
        observer.emit_in_attempt(&record, None, &run);
    }
    let health = observer.health();
    assert_eq!(health.accepted, 0);
    assert_eq!(health.dropped, 0);
    assert_eq!(health.queue_depth, 0);
    assert_eq!(health.pending_depth, 0);
    observer.abort.abort();
    tokio::task::yield_now().await;
}

fn supported_signals() -> Vec<(EventKind, Value)> {
    vec![
        (
            EventKind::RunAdmitted,
            json!({
                "spans": [], "metrics": [], "log": "run admitted",
            }),
        ),
        (
            EventKind::RunStarted,
            json!({
                "spans": ["crabber.attempt.admission", "crabber.workflow.admission"],
                "metrics": [], "log": null,
            }),
        ),
        (
            EventKind::RunResumed,
            json!({
                "spans": ["crabber.attempt.admission", "crabber.workflow.admission"],
                "metrics": [], "log": null,
            }),
        ),
        (
            EventKind::RunSettled,
            json!({
                "spans": [], "metrics": ["crabber.run.count", "crabber.run.duration_ms"],
                "log": "run settled",
            }),
        ),
        (
            EventKind::ToolCallSettled,
            json!({
                "spans": ["crabber.tool_call"], "metrics": ["crabber.tool.calls"],
                "log": "tool settled",
            }),
        ),
        (
            EventKind::PermissionDecided,
            json!({
                "spans": [], "metrics": ["crabber.permission.decisions"],
                "log": "permission decided",
            }),
        ),
        (
            EventKind::ContextEpochFinished,
            json!({
                "spans": ["crabber.compaction"], "metrics": [], "log": null,
            }),
        ),
        (
            EventKind::Custom {
                name: "model_call".into(),
            },
            json!({
                "spans": ["crabber.model_call"], "metrics": ["crabber.model.calls",
                    "crabber.model.latency_ms", "crabber.model.tokens.input", "crabber.model.tokens.output"],
                "log": null,
            }),
        ),
        (
            EventKind::Custom {
                name: "wasm_call".into(),
            },
            json!({
                "spans": [], "metrics": ["crabber.wasm.calls"], "log": null,
            }),
        ),
    ]
}

#[test]
fn every_supported_kind_preserves_its_existing_exported_signals() {
    let session = SessionId::new();
    let run = RunId::new();
    for (kind, expected) in supported_signals() {
        let record = event(
            kind,
            &session,
            &run,
            json!({
                "status": "ok", "provider": "fake", "model": "demo", "name": "tool",
                "input_tokens": 2, "output_tokens": 3,
                "prompt": "PRIVATE_PROMPT_SENTINEL", "reasoning": "PRIVATE_REASONING_SENTINEL",
            }),
        );
        let safe = SafeEvent::from_event(&record, &RedactionPolicy::default()).unwrap();
        let exported_spans = spans(&safe);
        let exported_metrics = series(&config(), &safe);
        let exported_log = log(&config(), &safe);
        assert_eq!(
            json!({
                "spans": exported_spans.iter().map(|span| &span["name"]).collect::<Vec<_>>(),
                "metrics": exported_metrics.iter().map(|metric| &metric["metric"]).collect::<Vec<_>>(),
                "log": exported_log.as_ref().map(|entry| &entry["message"]),
            }),
            expected
        );
        let encoded = json!([exported_spans, exported_metrics, exported_log]).to_string();
        assert!(!encoded.contains("PRIVATE_PROMPT_SENTINEL"));
        assert!(!encoded.contains("PRIVATE_REASONING_SENTINEL"));
    }
}
