use crabber_agui::{
    Completion, ProjectionConfig, ProjectionError, Projector,
    ag_ui_core::{
        event::Event,
        processing::{PROTOCOL_VERSION, normalize_producer_event},
    },
    ag_ui_encoder::Encoder,
    encode_sse, encode_sse_recorded, frame_budget,
};
use crabber_core::{EventKind, EventRecord, RunId, SessionId};
use serde_json::{Value, json};

fn event(value: Value) -> Event {
    serde_json::from_value(value).unwrap()
}

fn wire(frame: &[u8]) -> Value {
    assert!(frame.starts_with(b"data: ") && frame.ends_with(b"\n\n"));
    serde_json::from_slice(&frame[6..frame.len() - 2]).unwrap()
}

#[test]
fn projector_and_both_encoder_paths_agree_on_protocol_version() {
    let mut projector = Projector::new(
        SessionId::from("session"),
        RunId::from("run"),
        "thread".into(),
        "wire".into(),
        ProjectionConfig::default(),
    )
    .unwrap();
    let start = projector
        .push(&EventRecord {
            session_id: SessionId::from("session"),
            run_id: RunId::from("run"),
            turn_id: None,
            cursor: None,
            kind: EventKind::RunStarted,
            payload: Value::Null,
            correlation: None,
            live_only: true,
            created_at: time::OffsetDateTime::UNIX_EPOCH,
        })
        .unwrap()
        .remove(0);
    let Event::RunStarted(ref declaration) = start else {
        panic!("RUN_STARTED required")
    };
    assert_eq!(
        declaration.protocol_version.as_deref(),
        Some(PROTOCOL_VERSION)
    );
    let encoder = Encoder::default();
    assert_eq!(
        encoder.encode(&start).unwrap(),
        encoder.encode_live(&start).unwrap()
    );
    assert_eq!(
        encode_sse(&start, 1024).unwrap(),
        encode_sse_recorded(&start, 1024).unwrap()
    );
    assert!(matches!(
        projector.finish(Completion::Failed).unwrap().as_slice(),
        [Event::RunError(_)]
    ));

    let recorded = event(json!({"type":"RUN_STARTED", "threadId":"t", "runId":"r"}));
    assert!(
        wire(&encode_sse_recorded(&recorded, 1024).unwrap())
            .get("protocolVersion")
            .is_none()
    );
    assert_eq!(
        wire(&encode_sse(&recorded, 1024).unwrap())["protocolVersion"],
        PROTOCOL_VERSION
    );
}

#[test]
fn every_projector_event_kind_survives_sdk_normalization_and_encoding() {
    // All variants constructed by projector.rs, including both terminal outcomes.
    let variants = [
        json!({"type":"RUN_STARTED", "threadId":"t", "runId":"r", "protocolVersion":PROTOCOL_VERSION}),
        json!({"type":"RUN_FINISHED", "threadId":"t", "runId":"r", "outcome":{"type":"success"}}),
        json!({"type":"RUN_FINISHED", "threadId":"t", "runId":"r", "outcome":{"type":"cancelled"}}),
        json!({"type":"RUN_FINISHED", "threadId":"t", "runId":"r", "outcome":{"type":"interrupt", "interrupts":[{"id":"i", "reason":"waiting"}]}}),
        json!({"type":"RUN_ERROR", "message":"Failed", "code":"crabber_source_malformed"}),
        json!({"type":"TEXT_MESSAGE_START", "messageId":"m", "role":"assistant"}),
        json!({"type":"TEXT_MESSAGE_CONTENT", "messageId":"m", "delta":"é😀\n"}),
        json!({"type":"TEXT_MESSAGE_END", "messageId":"m"}),
        json!({"type":"TOOL_CALL_START", "toolCallId":"c", "toolCallName":"echo", "parentMessageId":"m"}),
        json!({"type":"TOOL_CALL_ARGS", "toolCallId":"c", "delta":"{}"}),
        json!({"type":"TOOL_CALL_END", "toolCallId":"c"}),
        json!({"type":"TOOL_CALL_RESULT", "messageId":"result", "toolCallId":"c", "content":"{}", "role":"tool"}),
        json!({"type":"REASONING_START", "messageId":"reasoning"}),
        json!({"type":"REASONING_MESSAGE_START", "messageId":"reasoning", "role":"reasoning"}),
        json!({"type":"REASONING_MESSAGE_CONTENT", "messageId":"reasoning", "delta":"summary"}),
        json!({"type":"REASONING_MESSAGE_END", "messageId":"reasoning"}),
        json!({"type":"REASONING_END", "messageId":"reasoning"}),
        json!({"type":"ACTIVITY_SNAPSHOT", "messageId":"attempt", "activityType":"crabber.execution_attempt", "content":{"state":"Started"}, "replace":true}),
        json!({"type":"ACTIVITY_DELTA", "messageId":"activity", "activityType":"progress", "patch":[{"op":"add","path":"/done","value":true}]}),
        json!({"type":"STATE_SNAPSHOT", "snapshot":null}),
        json!({"type":"STATE_DELTA", "delta":[{"op":"remove","path":"/old","value":null}]}),
        json!({"type":"STEP_STARTED", "stepName":"turn:actual"}),
        json!({"type":"STEP_FINISHED", "stepName":"turn:actual"}),
        json!({"type":"MESSAGES_SNAPSHOT", "messages":[]}),
        json!({"type":"SUBAGENT_STARTED", "subagentRunId":"child", "name":"crabber.invocation"}),
        json!({"type":"SUBAGENT_FINISHED", "subagentRunId":"child", "outcome":{"type":"success"}}),
        json!({"type":"SUBAGENT_ERROR", "subagentRunId":"child", "message":"Presentation closed", "code":"crabber_projection_scope_closed"}),
        json!({"type":"RAW", "source":"declared", "event":{"null":null}}),
        json!({"type":"CUSTOM", "name":"declared", "value":[null,true]}),
    ];
    for value in variants {
        let event = event(value);
        let expected = normalize_producer_event(&event).unwrap();
        assert_eq!(wire(&encode_sse(&event, 4096).unwrap()), expected);
    }
}

#[test]
fn whole_frame_limit_includes_delimiters_and_sdk_faults_are_malformed() {
    let snapshot = event(json!({"type":"STATE_SNAPSHOT", "snapshot":null}));
    let size = frame_budget(&snapshot, 1024).unwrap();
    assert_eq!(encode_sse(&snapshot, size).unwrap().len(), size);
    assert_eq!(encode_sse(&snapshot, size - 1), Err(ProjectionError::Limit));
    let mut malformed = snapshot;
    let Event::StateSnapshot(ref mut state) = malformed else {
        unreachable!()
    };
    state.base.timestamp = Some(i64::MAX);
    assert_eq!(
        encode_sse(&malformed, 1024),
        Err(ProjectionError::Malformed)
    );
}

#[test]
fn patch_extensions_round_trip_and_every_shadow_is_rejected() {
    use crabber_agui::ag_ui_core::{
        event::{ActivityDeltaEvent, BaseEvent, StateDeltaEvent},
        types::JsonPatchOperation,
    };
    let base: BaseEvent = serde_json::from_value(json!({})).unwrap();
    for op in ["add", "remove", "replace", "move", "copy", "test"] {
        let mut source = json!({"op":op, "path":"/x"});
        let owns_value = matches!(op, "add" | "replace" | "test");
        let owns_from = matches!(op, "move" | "copy");
        if owns_value {
            source["value"] = json!({"nested":[null, "é😀"]});
        }
        if owns_from {
            source["from"] = json!("/before");
        }
        let original: JsonPatchOperation = serde_json::from_value(source).unwrap();
        for key in ["op", "path", "value", "from", "extension"] {
            let mut patch = original.clone();
            let extra = match &mut patch {
                JsonPatchOperation::Add { extra, .. }
                | JsonPatchOperation::Remove { extra, .. }
                | JsonPatchOperation::Replace { extra, .. }
                | JsonPatchOperation::Move { extra, .. }
                | JsonPatchOperation::Copy { extra, .. }
                | JsonPatchOperation::Test { extra, .. } => extra,
            };
            let extension = json!({"exact":[null, "é😀", 17]});
            extra.insert(key.into(), extension.clone());
            let shadow = matches!(key, "op" | "path")
                || (key == "value" && owns_value)
                || (key == "from" && owns_from);
            let events = [
                Event::StateDelta(StateDeltaEvent {
                    base: base.clone(),
                    delta: vec![patch.clone()],
                }),
                Event::ActivityDelta(ActivityDeltaEvent {
                    base: base.clone(),
                    message_id: "a".into(),
                    activity_type: "work".into(),
                    patch: vec![patch.clone()],
                }),
            ];
            for event in events {
                if shadow {
                    assert_eq!(
                        encode_sse(&event, 4096),
                        Err(ProjectionError::Malformed),
                        "{op}:{key}"
                    );
                } else {
                    let frame = encode_sse(&event, 4096).unwrap();
                    let decoded: Event = serde_json::from_value(wire(&frame)).unwrap();
                    let patches = match decoded {
                        Event::StateDelta(state) => state.delta,
                        Event::ActivityDelta(activity) => activity.patch,
                        _ => unreachable!(),
                    };
                    assert_eq!(patches, [patch.clone()], "{op}:{key}");
                    let expected = normalize_producer_event(&event).unwrap();
                    let expected_frame =
                        format!("data: {}\n\n", serde_json::to_string(&expected).unwrap());
                    assert_eq!(frame, expected_frame.as_bytes(), "{op}:{key}");
                }
            }
        }
    }
}
