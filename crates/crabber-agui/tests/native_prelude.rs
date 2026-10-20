use std::sync::Arc;

use crabber::{Agent, AgentConfig, FakeProvider, Selection, StreamDelta};
use crabber_agui::{
    Completion, ProjectionConfig, ProjectionError, Projector, ag_ui_core::event::Event, encode_sse,
};
use crabber_core::{EventKind, EventRecord};
use crabber_session::{MemoryStore, Store};
use serde_json::{Value, json};

mod support;

async fn native_fixture() -> (Vec<EventRecord>, Vec<crabber_core::Message>) {
    let store = Arc::new(MemoryStore::new());
    let agent = Agent::builder()
        .behavior_version("native-prelude.v1")
        .store(store.clone())
        .provider(Arc::new(FakeProvider::scripted(vec![vec![
            StreamDelta::TextDelta("actual output".into()),
            StreamDelta::Completed,
        ]])))
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .build()
        .unwrap();
    let mut run = agent.prompt(None, "request").await.unwrap();
    let session = run.session_id().clone();
    let mut events = run.events();
    while events.recv().await.unwrap().is_some() {}
    assert_eq!(
        run.done().await.unwrap().status,
        crabber_core::RunStatus::Completed
    );
    (
        store.list_events(&session, None, 1000).await.unwrap(),
        store.list_all_messages(&session).await.unwrap(),
    )
}

async fn native_history() -> Vec<EventRecord> {
    native_fixture().await.0
}

#[tokio::test]
async fn complete_actual_native_history_passes_pinned_client_protocol_and_reducer() {
    let (records, messages) = native_fixture().await;
    let mut p = projector(&records[0], ProjectionConfig::default());
    let mut output = Vec::new();
    for record in records {
        if record.kind == EventKind::MessageCommitted {
            let message = messages
                .iter()
                .find(|message| record.payload["message_id"] == message.id.0)
                .unwrap();
            assert_eq!(message.role, crabber_core::Role::Assistant);
            // Durable rows retain committed identity/content; regenerate only
            // their live presentation boundaries, exactly as recovery does.
            let mut live = record.clone();
            live.live_only = true;
            live.kind = EventKind::MessageStarted;
            live.payload = json!({"message_id":message.id});
            output.extend(p.push(&live).unwrap());
            for part in &message.parts {
                let crabber_core::ContentBlock::Text { text } = &part.content else {
                    panic!("text fixture")
                };
                live.kind = EventKind::TextDelta;
                live.payload = json!({"message_id":message.id,"text":text});
                output.extend(p.push(&live).unwrap());
            }
            live.kind = EventKind::MessageStreamEnded;
            live.payload = json!({"message_id":message.id,"outcome":"completed"});
            output.extend(p.push(&live).unwrap());
        }
        output.extend(p.push(&record).unwrap());
    }
    output.extend(p.finish(Completion::Completed).unwrap());
    assert_eq!(
        output
            .iter()
            .filter(|event| matches!(event, Event::RunStarted(_)))
            .count(),
        1
    );
    assert!(matches!(output.last(), Some(Event::RunFinished(_))));
    let result = support::consume(&output).await;
    assert!(matches!(
        result.outcome,
        Some(crabber_agui::ag_ui_core::types::RunFinishedOutcome::Success { .. })
    ));
    assert_eq!(
        wire(&output)
            .iter()
            .filter(|event| event["type"] == "TEXT_MESSAGE_CONTENT")
            .map(|event| event["delta"].as_str().unwrap())
            .collect::<String>(),
        "actual output"
    );
}

fn projector(record: &EventRecord, config: ProjectionConfig) -> Projector {
    Projector::new(
        record.session_id.clone(),
        record.run_id.clone(),
        "thread".into(),
        "wire".into(),
        config,
    )
    .unwrap()
}

fn wire(events: &[Event]) -> Vec<Value> {
    events
        .iter()
        .map(|event| {
            let frame = encode_sse(event, 1_048_576).unwrap();
            serde_json::from_slice(&frame[6..frame.len() - 2]).unwrap()
        })
        .collect()
}

#[tokio::test]
async fn actual_native_reserve_begin_and_terminal_are_ordered_safe_activities() {
    let records = native_history().await;
    let attempts: Vec<_> = records
        .iter()
        .filter(|record| record.kind == EventKind::ExecutionAttemptChanged)
        .collect();
    assert!(attempts.len() >= 3);
    assert_eq!(attempts[0].payload["attempt"]["state"], "Unstarted");
    assert_eq!(attempts[1].payload["attempt"]["state"], "Started");
    let mut p = projector(attempts[0], ProjectionConfig::default());
    let first = wire(&p.push(attempts[0]).unwrap());
    assert_eq!(first[0]["type"], "RUN_STARTED");
    assert_eq!(first[1]["type"], "ACTIVITY_SNAPSHOT");
    assert_eq!(first[1]["content"]["state"], "Unstarted");
    let mut resumed = attempts[0].clone();
    resumed.kind = EventKind::RunResumed;
    resumed.payload = Value::Null;
    assert_eq!(p.push(&resumed).unwrap(), [] as [Event; 0]);
    assert_eq!(
        wire(&p.push(attempts[1]).unwrap())[0]["content"]["state"],
        "Started"
    );
    let started = records
        .iter()
        .find(|record| record.kind == EventKind::RunStarted)
        .unwrap();
    assert_eq!(p.push(started).unwrap(), [] as [Event; 0]);
    for record in &attempts[2..] {
        let projected = wire(&p.push(record).unwrap());
        assert_eq!(projected.len(), 1);
        assert_eq!(
            projected[0]["content"],
            json!({
                "target":record.payload["attempt"]["target"],
                "state":record.payload["attempt"]["state"],
                "cleanup":record.payload["attempt"]["cleanup"],
            })
        );
        assert_eq!(projected[0]["messageId"], first[1]["messageId"]);
        assert!(projected[0].get("command").is_none());
        assert!(projected[0].get("settlement").is_none());
    }
    assert_eq!(p.push(started), Err(ProjectionError::Order));
}

#[tokio::test]
async fn opening_wire_does_not_authorize_business_or_completed_source() {
    let records = native_history().await;
    let reserve = records
        .iter()
        .find(|record| record.kind == EventKind::ExecutionAttemptChanged)
        .unwrap();
    for kind in [
        EventKind::MessageStarted,
        EventKind::TextDelta,
        EventKind::ToolCallStarted,
        EventKind::TurnStarted,
    ] {
        let mut p = projector(reserve, ProjectionConfig::default());
        p.push(reserve).unwrap();
        let mut invalid = reserve.clone();
        invalid.kind = kind;
        invalid.payload = json!({"message_id":"not-started","text":"secret"});
        assert_eq!(p.push(&invalid), Err(ProjectionError::Order));
        assert_eq!(
            wire(&p.finish(Completion::Failed).unwrap())[0]["type"],
            "RUN_ERROR"
        );
    }
    let mut p = projector(reserve, ProjectionConfig::default());
    p.push(reserve).unwrap();
    assert_eq!(
        wire(&p.finish(Completion::Completed).unwrap())[0]["type"],
        "RUN_ERROR"
    );
}

#[tokio::test]
async fn native_identity_limits_and_delivery_faults_are_permanent() {
    let records = native_history().await;
    let reserve = records
        .iter()
        .find(|record| record.kind == EventKind::ExecutionAttemptChanged)
        .unwrap();
    for (field, value, expected) in [
        ("version", json!(99), ProjectionError::Malformed),
        (
            "attempt",
            json!({"session_id":"foreign"}),
            ProjectionError::Identity,
        ),
    ] {
        let mut invalid = reserve.clone();
        invalid.payload[field] = value;
        let mut p = projector(reserve, ProjectionConfig::default());
        assert_eq!(p.push(&invalid), Err(expected));
        assert_eq!(p.push(reserve), Err(expected));
    }
    let mut p = projector(
        reserve,
        ProjectionConfig {
            max_batch: 1,
            ..ProjectionConfig::default()
        },
    );
    assert_eq!(p.push(reserve), Err(ProjectionError::Limit));
    let mut p = projector(reserve, ProjectionConfig::default());
    assert_eq!(
        p.push_with_delivery(reserve, |events| {
            assert_eq!(events.len(), 2);
            Err(ProjectionError::Transport)
        }),
        Err(ProjectionError::Transport)
    );
    let terminal = wire(&p.finish(Completion::Failed).unwrap());
    assert_eq!(terminal[0]["type"], "RUN_STARTED");
    assert_eq!(terminal[1]["type"], "RUN_ERROR");
    assert_eq!(terminal[1]["code"], "crabber_transport");
}

#[test]
fn native_continuation_receipt_outcomes_use_actual_internal_operation_tag() {
    let target = crabber_session::ContinuationTarget {
        id: crabber_core::ContinuationId::from("continuation"),
        generation: crabber_core::ContinuationGeneration::INITIAL,
    };
    let outcomes = [
        serde_json::to_value(crabber_session::ApprovalOutcome::Suspended {
            continuation: target.clone(),
        })
        .unwrap(),
        serde_json::to_value(crabber_session::ToolContinuationOutcome::Suspended {
            continuation: target,
        })
        .unwrap(),
    ];
    for (index, outcome) in outcomes.into_iter().enumerate() {
        assert_eq!(outcome["operation"], "suspended");
        let event = EventRecord {
            session_id: crabber_core::SessionId::from("session"),
            run_id: crabber_core::RunId::from("run"),
            turn_id: None,
            cursor: None,
            kind: EventKind::ContinuationChanged,
            payload: json!({"version":1,"operation_id":"operation","semantic_digest":"a".repeat(64),"revision":1,"outcome":outcome,"kind":if index == 1 {json!("tool_continuation")} else {Value::Null}}),
            correlation: Some("operation".into()),
            live_only: false,
            created_at: time::OffsetDateTime::UNIX_EPOCH,
        };
        let mut event = event;
        if index == 0 {
            event.payload.as_object_mut().unwrap().remove("kind");
        }
        let mut p = projector(&event, ProjectionConfig::default());
        let events = wire(&p.push(&event).unwrap());
        assert_eq!(events[1]["content"], json!({"status":"suspended"}));
        assert!(!events[1].to_string().contains("semantic_digest"));
    }
}
