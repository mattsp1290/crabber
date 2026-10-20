use crabber::core::{ContentBlock, EventKind, EventRecord, Role, RunId, SessionId};
use crabber::{Agent, AgentConfig, FakeProvider, Selection, StreamDelta};
use crabber_agui::ag_ui_core::event::Event;
use crabber_agui::{Completion, ProjectionConfig, ProjectionError, Projector, encode_sse};
use crabber_session::{MemoryStore, Store};
use serde_json::{Value, json};
use std::sync::Arc;

fn record(kind: EventKind, payload: Value) -> EventRecord {
    let correlation = (kind == EventKind::ContinuationWaiting)
        .then(|| payload["operation_id"].as_str().unwrap().to_owned());
    let live_only = kind != EventKind::ContinuationWaiting;
    EventRecord {
        session_id: SessionId::from("session"),
        run_id: RunId::from("source"),
        turn_id: None,
        cursor: None,
        kind,
        payload,
        correlation,
        live_only,
        created_at: time::OffsetDateTime::UNIX_EPOCH,
    }
}
fn projector(config: ProjectionConfig) -> Projector {
    Projector::new(
        SessionId::from("session"),
        RunId::from("source"),
        "thread".into(),
        "alias".into(),
        config,
    )
    .unwrap()
}
fn wire(events: &[Event]) -> Vec<Value> {
    events
        .iter()
        .map(|e| serde_json::to_value(e).unwrap())
        .collect()
}
fn start(p: &mut Projector) {
    p.push(&record(EventKind::RunStarted, Value::Null)).unwrap();
}
fn message(p: &mut Projector) {
    p.push(&record(
        EventKind::MessageStarted,
        json!({"message_id":"assistant"}),
    ))
    .unwrap();
}
fn text(p: &mut Projector, text: &str) -> Vec<Event> {
    p.push(&record(
        EventKind::TextDelta,
        json!({"message_id":"assistant", "text":text}),
    ))
    .unwrap()
}
fn end(p: &mut Projector, outcome: &str) -> Vec<Event> {
    p.push(&record(
        EventKind::MessageStreamEnded,
        json!({"message_id":"assistant", "outcome":outcome}),
    ))
    .unwrap()
}
fn commit(p: &mut Projector) {
    p.push(&record(
        EventKind::MessageCommitted,
        json!({"message_id":"assistant", "role":"assistant"}),
    ))
    .unwrap();
}
fn types(events: &[Event]) -> Vec<String> {
    wire(events)
        .iter()
        .map(|e| e["type"].as_str().unwrap().into())
        .collect()
}

fn human_wait(p: &mut Projector) {
    message(p);
    p.push(&record(
        EventKind::ToolCallStarted,
        json!({"message_id":"assistant","call_id":"human-call","name":"question"}),
    ))
    .unwrap();
    p.push(&record(
        EventKind::ToolCallArgsCompleted,
        json!({"message_id":"assistant","call_id":"human-call"}),
    ))
    .unwrap();
    end(p, "completed");
    commit(p);
    let source = crabber_core::continuation_source::WaitingSource {
        version: 1,
        session_id: SessionId::from("session"),
        run_id: RunId::from("source"),
        attempt: crabber_core::continuation_source::AttemptIdentity {
            id: crabber_core::AttemptId::from("attempt"),
            generation: crabber_core::AttemptGeneration::INITIAL,
        },
        current_turn: None,
        continuation: crabber_core::continuation_source::ContinuationIdentity {
            id: crabber_core::ContinuationId::from("continuation"),
            generation: crabber_core::ContinuationGeneration::INITIAL,
        },
        operation_id: crabber_core::ControlId::from("wait"),
        semantic_digest: crabber_core::InputFingerprint::new("a".repeat(64)).unwrap(),
        revision: crabber_core::SessionRevision::new(1).unwrap(),
        source_event_cursor: crabber_core::EventCursor(1),
        frontend_pending: vec![],
        backend_pending: vec![crabber_core::ToolCallId::from("human-call")],
        interrupts: vec![crabber_core::continuation_source::HumanInterrupt {
            interaction_id: crabber_core::InteractionId::from("actual-question"),
            call_id: crabber_core::ToolCallId::from("human-call"),
            kind: crabber_core::InteractionKind::UserInput,
            name: "question".into(),
            prompt: "Declared public question".into(),
            response_schema: json!({"type":"string"}),
        }],
    };
    p.push(&record(
        EventKind::ContinuationWaiting,
        serde_json::to_value(source).unwrap(),
    ))
    .unwrap();
    p.push(&record(EventKind::RunPaused, Value::Null)).unwrap();
}

#[test]
fn wire_identity_optional_fields_and_framing() {
    let mut p = projector(ProjectionConfig::default());
    let start = p.push(&record(EventKind::RunStarted, Value::Null)).unwrap();
    assert_eq!(
        wire(&start)[0],
        json!({"type":"RUN_STARTED", "threadId":"thread", "runId":"alias", "protocolVersion":"1.0", "timestamp":0})
    );
    message(&mut p);
    assert_eq!(text(&mut p, ""), [] as [Event; 0]);
    let events = text(&mut p, "é😀\n\r\"中");
    assert_eq!(
        types(&events),
        ["TEXT_MESSAGE_START", "TEXT_MESSAGE_CONTENT"]
    );
    assert_eq!(wire(&events)[0]["messageId"], "assistant");
    let frame = encode_sse(&events[1], 4096).unwrap();
    assert_eq!(frame.split(|b| *b == b'\n').count(), 3);
    assert!(frame.starts_with(b"data: ") && frame.ends_with(b"\n\n"));
    let decoded: Event = serde_json::from_slice(&frame[6..frame.len() - 2]).unwrap();
    assert_eq!(
        serde_json::to_value(decoded).unwrap(),
        crabber_agui::ag_ui_core::processing::normalize_producer_event(&events[1]).unwrap()
    );
    assert_eq!(encode_sse(&events[1], 1), Err(ProjectionError::Limit));
    assert_eq!(types(&end(&mut p, "completed")), ["TEXT_MESSAGE_END"]);
    commit(&mut p);
    p.push(&record(
        EventKind::RunSettled,
        json!({"status":"completed"}),
    ))
    .unwrap();
    let terminal = p.finish(Completion::Completed).unwrap();
    assert_eq!(wire(&terminal)[0]["outcome"], json!({"type":"success"}));
    assert_eq!(p.finish(Completion::Failed).unwrap(), [] as [Event; 0]);
    assert_eq!(
        p.push(&record(EventKind::TextDelta, Value::Null)),
        Err(ProjectionError::Terminal)
    );
}

#[test]
fn terminals_require_source_truth_and_task_failure_wins() {
    for (completion, status, expected) in [
        (Completion::Completed, Some("completed"), "success"),
        (Completion::Cancelled, Some("interrupted"), "cancelled"),
        (Completion::Paused, None, "interrupt"),
        (Completion::Failed, Some("completed"), "error"),
        (Completion::LeaseLost, Some("completed"), "error"),
        (Completion::Completed, None, "error"),
    ] {
        let mut p = projector(ProjectionConfig::default());
        start(&mut p);
        if let Some(status) = status {
            p.push(&record(EventKind::RunSettled, json!({"status":status})))
                .unwrap();
        }
        if completion == Completion::Paused {
            human_wait(&mut p);
        }
        let events = wire(&p.finish(completion).unwrap());
        if expected == "error" {
            assert_eq!(events[0]["type"], "RUN_ERROR");
        } else {
            assert_eq!(events[0]["outcome"]["type"], expected);
        }
    }
    let mut p = projector(ProjectionConfig::default());
    assert_eq!(
        types(&p.finish(Completion::Failed).unwrap()),
        ["RUN_STARTED", "RUN_ERROR"]
    );
}

#[test]
fn failed_public_attempt_blocks_retry_but_filtered_attempt_can_retry() {
    for (reasoning, public) in [(false, false), (true, true)] {
        let mut p = projector(ProjectionConfig {
            reasoning,
            ..ProjectionConfig::default()
        });
        start(&mut p);
        message(&mut p);
        let events = p
            .push(&record(
                EventKind::ReasoningDelta,
                json!({"message_id":"assistant", "item_id":"summary", "provenance":"public_summary", "text":"public summary"}),
            ))
            .unwrap();
        assert_eq!(!events.is_empty(), public);
        let closures = end(&mut p, "failed");
        if public {
            assert_eq!(types(&closures), ["REASONING_MESSAGE_END", "REASONING_END"]);
            assert_eq!(
                p.push(&record(
                    EventKind::MessageStarted,
                    json!({"message_id":"retry"})
                )),
                Err(ProjectionError::SourceAttemptFailed)
            );
            assert_eq!(
                wire(&p.finish(Completion::Completed).unwrap())[0]["code"],
                "crabber_source_attempt_failed"
            );
        } else {
            p.push(&record(
                EventKind::MessageStarted,
                json!({"message_id":"retry"}),
            ))
            .unwrap();
            p.push(&record(
                EventKind::MessageStreamEnded,
                json!({"message_id":"retry", "outcome":"completed"}),
            ))
            .unwrap();
            p.push(&record(
                EventKind::MessageCommitted,
                json!({"message_id":"retry", "role":"assistant"}),
            ))
            .unwrap();
            p.push(&record(
                EventKind::RunSettled,
                json!({"status":"completed"}),
            ))
            .unwrap();
            assert_eq!(
                types(&p.finish(Completion::Completed).unwrap()),
                ["RUN_FINISHED"]
            );
        }
    }
}

#[test]
fn tool_before_text_multiple_calls_and_private_result_filtering() {
    let mut p = projector(ProjectionConfig::default());
    start(&mut p);
    message(&mut p);
    for id in ["a", "b"] {
        let events = p
            .push(&record(
                EventKind::ToolCallStarted,
                json!({"message_id":"assistant", "call_id":id, "name":"echo"}),
            ))
            .unwrap();
        assert_eq!(
            types(&events),
            if id == "a" {
                vec!["TEXT_MESSAGE_START", "TOOL_CALL_START"]
            } else {
                vec!["TOOL_CALL_START"]
            }
        );
        p.push(&record(
            EventKind::ToolCallArgsDelta,
            json!({"message_id":"assistant", "call_id":id, "text":"{}"}),
        ))
        .unwrap();
        p.push(&record(
            EventKind::ToolCallArgsCompleted,
            json!({"message_id":"assistant", "call_id":id}),
        ))
        .unwrap();
    }
    assert_eq!(
        types(&text(&mut p, "after tools")),
        ["TEXT_MESSAGE_CONTENT"]
    );
    end(&mut p, "completed");
    commit(&mut p);
    for id in ["b", "a"] {
        let events = p.push(&record(EventKind::ToolCallSettled, json!({"call_id":id,"message_id":format!("result-{id}"),"is_error":true,
            "content":[{"type":"text","text":"redacted"},{"type":"provider_state","codec_id":"secret","payload":"SECRET"},
            {"type":"tool_result","call_id":"nested","is_error":false,"content":[{"type":"provider_state","codec_id":"secret","payload":"SECRET"}]}]}))).unwrap();
        let result = &wire(&events)[0];
        assert_eq!(result["metadata"]["crabber"]["isError"], true);
        assert_eq!(
            result["content"],
            json!([{"type":"text","text":"redacted"}])
        );
        assert!(!result.to_string().contains("SECRET"));
    }
    p.push(&record(
        EventKind::RunSettled,
        json!({"status":"completed"}),
    ))
    .unwrap();
    assert_eq!(
        types(&p.finish(Completion::Completed).unwrap()),
        ["RUN_FINISHED"]
    );
}

#[test]
fn malformed_mixed_run_duplicate_and_limit_faults_are_permanent() {
    let config = ProjectionConfig {
        max_text_bytes: 3,
        max_argument_bytes: 2,
        max_open_calls: 1,
        max_messages: 1,
        ..ProjectionConfig::default()
    };
    let mut p = projector(config.clone());
    start(&mut p);
    message(&mut p);
    text(&mut p, "abc");
    assert_eq!(
        p.push(&record(
            EventKind::TextDelta,
            json!({"message_id":"assistant", "text":"d"})
        )),
        Err(ProjectionError::Limit)
    );
    assert_eq!(
        types(&p.finish(Completion::Completed).unwrap())
            .last()
            .unwrap(),
        "RUN_ERROR"
    );
    for invalid in [
        record(EventKind::TextDelta, Value::Null),
        record(
            EventKind::ToolCallArgsCompleted,
            json!({"message_id":"assistant", "call_id":"absent"}),
        ),
        record(EventKind::MessageStarted, json!({"message_id":"assistant"})),
    ] {
        let mut p = projector(config.clone());
        start(&mut p);
        message(&mut p);
        assert!(p.push(&invalid).is_err());
        assert_eq!(
            types(&p.finish(Completion::Completed).unwrap())
                .last()
                .unwrap(),
            "RUN_ERROR"
        );
    }
    let mut p = projector(config);
    start(&mut p);
    let mut other = record(EventKind::MessageStarted, json!({"message_id":"a"}));
    other.run_id = RunId::from("other");
    assert_eq!(p.push(&other), Err(ProjectionError::Identity));
    assert!(
        Projector::new(
            SessionId::new(),
            RunId::new(),
            "a".into(),
            "b".into(),
            ProjectionConfig {
                max_batch: 0,
                ..ProjectionConfig::default()
            }
        )
        .is_err()
    );
}

#[tokio::test]
async fn real_runtime_transcript_matches_store_and_retry_never_reports_success() {
    for retry in [false, true] {
        let store = Arc::new(MemoryStore::new());
        let mut scripts = Vec::new();
        if retry {
            scripts.push(vec![
                StreamDelta::TextDelta("stale".into()),
                StreamDelta::Error(crabber::providers::ProviderError {
                    kind: crabber::providers::ProviderErrorKind::Server,
                    message: "SECRET".into(),
                    retryable: true,
                }),
            ]);
        }
        scripts.push(vec![
            StreamDelta::TextDelta("public 🌍".into()),
            StreamDelta::Completed,
        ]);
        let agent = Agent::builder()
            .behavior_version("integration-fixture.v1")
            .store(store.clone())
            .provider(Arc::new(FakeProvider::scripted(scripts)))
            .config(AgentConfig::new(Selection {
                provider_id: "fake".into(),
                model_id: "scripted".into(),
            }))
            .build()
            .unwrap();
        let mut run = agent.prompt(None, "hello").await.unwrap();
        let session = run.session_id().clone();
        let mut p = Projector::new(
            session.clone(),
            run.run_id().clone(),
            "thread".into(),
            "alias".into(),
            ProjectionConfig::default(),
        )
        .unwrap();
        let mut receiver = run.events();
        let mut events = Vec::new();
        while let Some(record) = receiver.recv().await.unwrap() {
            if let Ok(batch) = p.push(&record) {
                events.extend(batch);
            }
        }
        assert_eq!(
            run.done().await.unwrap().status,
            crabber::core::RunStatus::Completed
        );
        events.extend(p.finish(Completion::Completed).unwrap());
        assert_eq!(
            events
                .iter()
                .filter(|e| matches!(e, Event::RunFinished(_) | Event::RunError(_)))
                .count(),
            1
        );
        if retry {
            assert_eq!(types(&events).last().unwrap(), "RUN_ERROR");
        } else {
            let stored = store.list_messages(&session, None).await.unwrap();
            let assistant = stored.iter().find(|m| m.role == Role::Assistant).unwrap();
            let projected: String = events
                .iter()
                .filter_map(|e| match e {
                    Event::TextMessageContent(e) => {
                        assert_eq!(e.message_id.to_string(), assistant.id.to_string());
                        Some(e.delta.as_str())
                    }
                    _ => None,
                })
                .collect();
            let stored_text: String = assistant
                .parts
                .iter()
                .filter_map(|p| match &p.content {
                    ContentBlock::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect();
            assert_eq!(projected, stored_text);
            assert_eq!(types(&events).last().unwrap(), "RUN_FINISHED");
        }
    }
}

#[test]
fn argument_call_message_event_and_batch_limits_have_exact_edges() {
    let config = ProjectionConfig {
        max_argument_bytes: 2,
        max_open_calls: 1,
        max_messages: 1,
        ..ProjectionConfig::default()
    };
    for extra_call in [false, true] {
        let mut p = projector(config.clone());
        start(&mut p);
        message(&mut p);
        p.push(&record(
            EventKind::ToolCallStarted,
            json!({"message_id":"assistant", "call_id":"a", "name":"echo"}),
        ))
        .unwrap();
        p.push(&record(
            EventKind::ToolCallArgsDelta,
            json!({"message_id":"assistant", "call_id":"a", "text":"{}"}),
        ))
        .unwrap();
        let invalid = if extra_call {
            record(
                EventKind::ToolCallStarted,
                json!({"message_id":"assistant", "call_id":"b", "name":"echo"}),
            )
        } else {
            record(
                EventKind::ToolCallArgsDelta,
                json!({"message_id":"assistant", "call_id":"a", "text":"x"}),
            )
        };
        assert_eq!(p.push(&invalid), Err(ProjectionError::Limit));
    }
    let mut p = projector(config);
    start(&mut p);
    message(&mut p);
    end(&mut p, "completed");
    commit(&mut p);
    assert_eq!(
        p.push(&record(
            EventKind::MessageStarted,
            json!({"message_id":"second"})
        )),
        Err(ProjectionError::Limit)
    );
    let mut p = projector(ProjectionConfig {
        max_batch: 1,
        ..ProjectionConfig::default()
    });
    start(&mut p);
    message(&mut p);
    assert_eq!(
        p.push(&record(
            EventKind::TextDelta,
            json!({"message_id":"assistant", "text":"x"})
        )),
        Err(ProjectionError::Limit)
    );
    assert_eq!(types(&p.finish(Completion::Failed).unwrap()), ["RUN_ERROR"]);
    let mut p = projector(ProjectionConfig {
        max_event_bytes: 512,
        ..ProjectionConfig::default()
    });
    start(&mut p);
    message(&mut p);
    assert_eq!(
        p.push(&record(
            EventKind::TextDelta,
            json!({"message_id":"assistant", "text":"x".repeat(600)})
        )),
        Err(ProjectionError::Limit)
    );
    let mut p = projector(ProjectionConfig::default());
    start(&mut p);
    message(&mut p);
    let event = text(&mut p, "edge").pop().unwrap();
    let size = crabber_agui::frame_budget(&event, 4096).unwrap();
    assert!(encode_sse(&event, size).is_ok());
    assert_eq!(encode_sse(&event, size - 1), Err(ProjectionError::Limit));
}

#[test]
fn delivery_rejection_rolls_back_unpublished_boundaries() {
    let mut p = projector(ProjectionConfig::default());
    start(&mut p);
    message(&mut p);
    let rejected = p.push_with_delivery(
        &record(
            EventKind::ToolCallStarted,
            json!({"message_id":"assistant", "call_id":"rejected", "name":"echo"}),
        ),
        |events| {
            assert!(types(events).contains(&"TOOL_CALL_START".into()));
            Err(ProjectionError::Transport)
        },
    );
    assert_eq!(rejected, Err(ProjectionError::Transport));
    let terminal = p.finish(Completion::Failed).unwrap();
    assert_eq!(types(&terminal), ["RUN_ERROR"]);
    assert_eq!(wire(&terminal)[0]["code"], "crabber_transport");
}
