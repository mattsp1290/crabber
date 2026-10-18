use crabber_agui::{
    Completion, ProjectionConfig, ProjectionError, Projector, ag_ui_core::event::Event,
};
use crabber_core::{
    AttemptGeneration, AttemptId, ContinuationGeneration, ContinuationId, ControlId, EventCursor,
    EventKind, EventRecord, InputFingerprint, InteractionId, InteractionKind, MessageId,
    PublicStateContents, RunId, SessionId, SessionRevision, ToolCallId,
    continuation_source::{
        AttemptIdentity, ContinuationIdentity, FrozenCall, HumanInterrupt, ResumeMode,
        ResumeSource, WaitingSource,
    },
};
use serde_json::{Value, json};
mod support;

fn record(kind: EventKind, payload: Value) -> EventRecord {
    let correlation = (kind == EventKind::ContinuationWaiting)
        .then(|| payload["operation_id"].as_str().unwrap().to_owned());
    let live_only = kind != EventKind::ContinuationWaiting;
    EventRecord {
        cursor: None,
        session_id: SessionId::from("session"),
        run_id: RunId::from("run"),
        turn_id: None,
        kind,
        payload,
        correlation,
        live_only,
        created_at: time::OffsetDateTime::UNIX_EPOCH,
    }
}
fn projector() -> Projector {
    Projector::new(
        SessionId::from("session"),
        RunId::from("run"),
        "thread".into(),
        "wire".into(),
        ProjectionConfig::default(),
    )
    .unwrap()
}
fn attempt() -> AttemptIdentity {
    AttemptIdentity {
        id: AttemptId::from("attempt"),
        generation: AttemptGeneration::INITIAL,
    }
}
fn target() -> ContinuationIdentity {
    ContinuationIdentity {
        id: ContinuationId::from("continuation"),
        generation: ContinuationGeneration::INITIAL,
    }
}
fn waiting(human: bool) -> WaitingSource {
    WaitingSource {
        version: 1,
        session_id: SessionId::from("session"),
        run_id: RunId::from("run"),
        attempt: attempt(),
        current_turn: None,
        continuation: target(),
        operation_id: ControlId::from("suspend"),
        semantic_digest: InputFingerprint::new("a".repeat(64)).unwrap(),
        revision: SessionRevision::new(1).unwrap(),
        source_event_cursor: EventCursor(7),
        interrupts: if human {
            vec![HumanInterrupt {
                interaction_id: InteractionId::from("actual-interaction"),
                call_id: ToolCallId::from("call"),
                kind: InteractionKind::UserInput,
                name: "question".into(),
                prompt: "Choose a value".into(),
                response_schema: json!({"type":"string"}),
            }]
        } else {
            vec![]
        },
        frontend_pending: if human {
            vec![]
        } else {
            vec![ToolCallId::from("call")]
        },
        backend_pending: if human {
            vec![ToolCallId::from("call")]
        } else {
            vec![]
        },
    }
}
fn resume() -> ResumeSource {
    ResumeSource {
        version: 1,
        session_id: SessionId::from("session"),
        run_id: RunId::from("run"),
        attempt: attempt(),
        current_turn: None,
        continuation: Some(target()),
        mode: ResumeMode::Continued,
        revision: SessionRevision::new(1).unwrap(),
        contents: PublicStateContents::new(Value::Null, vec![]).unwrap(),
        calls: vec![FrozenCall {
            call_id: ToolCallId::from("call"),
            message_id: MessageId::from("assistant"),
            name: "frontend".into(),
            arguments: json!({}),
        }],
    }
}
fn suspended(human: bool) -> Vec<Event> {
    suspended_source(waiting(human))
}
fn suspended_source(source: WaitingSource) -> Vec<Event> {
    let mut p = projector();
    let mut output = Vec::new();
    for event in [
        record(EventKind::RunStarted, Value::Null),
        record(EventKind::MessageStarted, json!({"message_id":"assistant"})),
        record(
            EventKind::ToolCallStarted,
            json!({"message_id":"assistant","call_id":"call","name":"frontend"}),
        ),
        record(
            EventKind::ToolCallArgsDelta,
            json!({"message_id":"assistant","call_id":"call","text":"{}"}),
        ),
        record(
            EventKind::ToolCallArgsCompleted,
            json!({"message_id":"assistant","call_id":"call"}),
        ),
        record(
            EventKind::MessageStreamEnded,
            json!({"message_id":"assistant","outcome":"completed"}),
        ),
        record(
            EventKind::MessageCommitted,
            json!({"message_id":"assistant","role":"assistant"}),
        ),
        record(
            EventKind::ContinuationWaiting,
            serde_json::to_value(source).unwrap(),
        ),
        record(EventKind::RunPaused, Value::Null),
    ] {
        output.extend(p.push(&event).unwrap());
    }
    output.extend(p.finish(Completion::Paused).unwrap());
    output
}

#[tokio::test]
async fn frontend_wait_and_fresh_resume_reuse_actual_call_parent_in_real_sdk() {
    let first = support::consume(&suspended(false)).await;
    let finished = suspended(false).pop().unwrap();
    let terminal = serde_json::to_value(finished).unwrap();
    assert_eq!(terminal["outcome"]["pendingToolCallIds"], json!(["call"]));
    let mut p = projector();
    let mut output = p
        .push(&record(
            EventKind::RunResumed,
            serde_json::to_value(resume()).unwrap(),
        ))
        .unwrap();
    assert!(
        !output
            .iter()
            .any(|event| matches!(event, Event::ToolCallStart(_)))
    );
    output.extend(p.push(&record(EventKind::ToolCallSettled, json!({"call_id":"call","message_id":"actual-result","is_error":false,"content":[{"type":"text","text":"done"}]}))).unwrap());
    p.push(&record(
        EventKind::RunSettled,
        json!({"status":"completed"}),
    ))
    .unwrap();
    output.extend(p.finish(Completion::Completed).unwrap());
    let result = support::consume_with_messages(&output, first.messages).await;
    let messages = serde_json::to_value(result.messages).unwrap();
    assert!(
        messages
            .as_array()
            .unwrap()
            .iter()
            .any(|message| message["id"] == "actual-result" && message["toolCallId"] == "call")
    );
    assert_eq!(
        messages
            .as_array()
            .unwrap()
            .iter()
            .filter(|message| message["id"] == "assistant")
            .count(),
        1
    );
}

#[tokio::test]
async fn human_wait_exposes_declared_prompt_schema_and_real_interaction_only() {
    let output = suspended(true);
    let terminal = serde_json::to_value(output.last().unwrap()).unwrap();
    let interrupt = &terminal["outcome"]["interrupts"][0];
    assert_eq!(interrupt["id"], "actual-interaction");
    assert_eq!(interrupt["message"], "Choose a value");
    assert_eq!(interrupt["responseSchema"], json!({"type":"string"}));
    support::consume(&output).await;
}

#[test]
fn bare_malformed_or_foreign_resume_cannot_open_business_or_fake_wait() {
    let mut p = projector();
    assert_eq!(
        p.push(&record(EventKind::RunResumed, Value::Null)).unwrap(),
        [] as [Event; 0]
    );
    assert_eq!(
        p.push(&record(
            EventKind::MessageStarted,
            json!({"message_id":"early"})
        )),
        Err(ProjectionError::Order)
    );
    let mut p = projector();
    assert_eq!(
        p.push(&record(EventKind::RunResumed, json!({"version":1}))),
        Err(ProjectionError::Malformed)
    );
    let mut p = projector();
    let mut source = resume();
    source.session_id = SessionId::from("foreign");
    assert_eq!(
        p.push(&record(
            EventKind::RunResumed,
            serde_json::to_value(source).unwrap()
        )),
        Err(ProjectionError::Identity)
    );
    let mut p = projector();
    p.push(&record(EventKind::RunStarted, Value::Null)).unwrap();
    p.push(&record(EventKind::RunPaused, Value::Null)).unwrap();
    assert!(
        p.finish(Completion::Paused)
            .unwrap()
            .iter()
            .any(|event| matches!(event, Event::RunError(_)))
    );
}

#[tokio::test]
async fn boolean_interrupt_schemas_preserve_semantics_in_sdk_object_boundary() {
    for value in [true, false] {
        let mut source = waiting(true);
        source.interrupts[0].response_schema = json!(value);
        let events = suspended_source(source);
        let terminal = serde_json::to_value(events.last().unwrap()).unwrap();
        assert_eq!(
            terminal["outcome"]["interrupts"][0]["responseSchema"],
            json!({"allOf":[value]})
        );
        support::consume(&events).await;
    }
}

#[test]
fn expanded_interrupt_terminal_obeys_frame_limit_with_one_finite_error() {
    let mut source = waiting(true);
    source.continuation.id = ContinuationId::from("c".repeat(512));
    source.backend_pending.clear();
    source.interrupts.clear();
    for index in 0..32 {
        let call_id = ToolCallId::from(format!("call-{index}"));
        source.backend_pending.push(call_id.clone());
        source.interrupts.push(HumanInterrupt {
            interaction_id: InteractionId::from(format!("interaction-{index}")),
            call_id,
            kind: InteractionKind::UserInput,
            name: "declared".into(),
            prompt: "Choose".into(),
            response_schema: json!({"type":"string"}),
        });
    }
    source.validate().unwrap();
    let frame_limit = serde_json::to_vec(&source).unwrap().len() + 128;
    let mut p = Projector::new(
        SessionId::from("session"),
        RunId::from("run"),
        "thread".into(),
        "wire".into(),
        ProjectionConfig {
            max_event_bytes: frame_limit,
            ..ProjectionConfig::default()
        },
    )
    .unwrap();
    p.push(&record(EventKind::RunStarted, Value::Null)).unwrap();
    p.push(&record(
        EventKind::MessageStarted,
        json!({"message_id":"assistant"}),
    ))
    .unwrap();
    for call in &source.backend_pending {
        p.push(&record(
            EventKind::ToolCallStarted,
            json!({"message_id":"assistant","call_id":call,"name":"declared"}),
        ))
        .unwrap();
        p.push(&record(
            EventKind::ToolCallArgsCompleted,
            json!({"message_id":"assistant","call_id":call}),
        ))
        .unwrap();
    }
    p.push(&record(
        EventKind::MessageStreamEnded,
        json!({"message_id":"assistant","outcome":"completed"}),
    ))
    .unwrap();
    p.push(&record(
        EventKind::MessageCommitted,
        json!({"message_id":"assistant","role":"assistant"}),
    ))
    .unwrap();
    p.push(&record(
        EventKind::ContinuationWaiting,
        serde_json::to_value(source).unwrap(),
    ))
    .unwrap();
    let events = p.finish(Completion::Paused).unwrap();
    assert!(
        matches!(events.as_slice(), [Event::RunError(error)] if error.code.as_deref() == Some("crabber_projection_limit"))
    );
    assert!(
        crabber_agui::encode_sse(&events[0], frame_limit)
            .unwrap()
            .len()
            <= frame_limit
    );
    assert_eq!(p.finish(Completion::Paused).unwrap(), [] as [Event; 0]);
}
