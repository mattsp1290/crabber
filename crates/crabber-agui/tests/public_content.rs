use crabber_agui::{Completion, ProjectionConfig, Projector, ag_ui_core::event::Event, encode_sse};
use crabber_core::{EventKind, EventRecord, RunId, SessionId};
use serde_json::{Value, json};

fn record(kind: EventKind, payload: Value) -> EventRecord {
    EventRecord {
        cursor: None,
        session_id: SessionId::from("session"),
        run_id: RunId::from("run"),
        turn_id: None,
        kind,
        payload,
        correlation: None,
        live_only: true,
        created_at: time::OffsetDateTime::UNIX_EPOCH,
    }
}

fn started() -> Projector {
    let mut p = Projector::new(
        SessionId::from("session"),
        RunId::from("run"),
        "thread".into(),
        "wire".into(),
        ProjectionConfig {
            reasoning: true,
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
    p
}

fn wire(events: &[Event]) -> Vec<Value> {
    events
        .iter()
        .map(|event| {
            let bytes = encode_sse(event, 1_048_576).unwrap();
            serde_json::from_slice(&bytes[6..bytes.len() - 2]).unwrap()
        })
        .collect()
}

#[test]
fn generic_reasoning_flag_never_relabels_legacy_private_or_unspecified_deltas() {
    let mut p = started();
    for provenance in [None, Some("private"), Some("unspecified")] {
        let mut payload = json!({"message_id":"assistant","text":"PRIVATE_SENTINEL"});
        if let Some(provenance) = provenance {
            payload["provenance"] = json!(provenance);
        }
        assert_eq!(
            p.push(&record(EventKind::ReasoningDelta, payload)).unwrap(),
            [] as [Event; 0]
        );
    }
    let mut output = Vec::new();
    for (item, text) in [
        ("summary-a", "visible A"),
        ("summary-b", "visible B"),
        ("summary-a", " more A"),
    ] {
        output.extend(p.push(&record(EventKind::ReasoningDelta, json!({"message_id":"assistant","item_id":item,"provenance":"public_summary","text":text}))).unwrap());
    }
    output.extend(
        p.push(&record(
            EventKind::MessageStreamEnded,
            json!({"message_id":"assistant","outcome":"completed"}),
        ))
        .unwrap(),
    );
    let events = wire(&output);
    assert!(
        !serde_json::to_string(&events)
            .unwrap()
            .contains("PRIVATE_SENTINEL")
    );
    let ids: Vec<_> = events
        .iter()
        .filter(|event| event["type"] == "REASONING_START")
        .map(|event| event["messageId"].as_str().unwrap())
        .collect();
    assert_eq!(ids.len(), 2);
    assert_ne!(ids[0], ids[1]);
    for id in ids {
        assert_eq!(
            events
                .iter()
                .filter(|event| event["type"] == "REASONING_END" && event["messageId"] == id)
                .count(),
            1
        );
    }
}

fn settle_tool(content: &Value) -> Vec<Event> {
    let mut p = started();
    p.push(&record(
        EventKind::ToolCallStarted,
        json!({"message_id":"assistant","call_id":"call","name":"mixed"}),
    ))
    .unwrap();
    p.push(&record(
        EventKind::ToolCallArgsCompleted,
        json!({"message_id":"assistant","call_id":"call"}),
    ))
    .unwrap();
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
    let output = p
        .push(&record(
            EventKind::ToolCallSettled,
            json!({"call_id":"call","message_id":"result","content":content,"is_error":true}),
        ))
        .unwrap();
    p.push(&record(
        EventKind::RunSettled,
        json!({"status":"completed"}),
    ))
    .unwrap();
    assert!(matches!(
        p.finish(Completion::Completed).unwrap().as_slice(),
        [Event::RunFinished(_)]
    ));
    output
}

#[test]
fn typed_mixed_tool_results_keep_real_sources_and_recursively_remove_private_artifacts() {
    let events = settle_tool(&json!([
        {"type":"text","text":"visible"},
        {"type":"media_content","kind":"image","mime_type":"image/png","source":{"kind":"data","base64":"aW1hZ2U="}},
        {"type":"media_content","kind":"audio","mime_type":"audio/wav","source":{"kind":"url","url":"https://example.invalid/audio"}},
        {"type":"media_content","kind":"document","mime_type":"application/pdf","source":{"kind":"file","file_id":"provider-file"}},
        {"type":"reasoning","text":"PRIVATE_SENTINEL"},
        {"type":"provider_state","codec_id":"private","payload":"PRIVATE_SENTINEL"},
        {"type":"tool_result","call_id":"nested","is_error":false,"content":[
            {"type":"reasoning","provenance":"private","item_id":"private","text":"PRIVATE_SENTINEL","provider_state":{"signature":"PRIVATE_SENTINEL"}},
            {"type":"reasoning","provenance":"public_summary","item_id":"summary","text":"visible summary","provider_state":{"signature":"PRIVATE_SENTINEL"}},
            {"type":"text","text":"nested visible"}
        ]}
    ]));
    let wire = wire(&events);
    assert_eq!(wire[0]["toolCallId"], "call");
    assert_eq!(wire[0]["messageId"], "result");
    assert_eq!(wire[0]["metadata"]["crabber"]["isError"], true);
    assert_eq!(
        wire[0]["content"][1],
        json!({"type":"image","source":{"type":"data","value":"aW1hZ2U=","mimeType":"image/png"}})
    );
    assert_eq!(wire[0]["content"][2]["source"]["type"], "url");
    assert_eq!(wire[0]["content"][3]["source"]["type"], "file");
    assert!(
        !serde_json::to_string(&wire)
            .unwrap()
            .contains("PRIVATE_SENTINEL")
    );
    assert_eq!(wire[0]["content"].as_array().unwrap().len(), 6);
}

#[test]
fn actual_long_provider_summary_identity_remains_public_without_private_state() {
    let item = format!("responses:{}:summary:0", "i".repeat(512));
    let mut p = started();
    let events = wire(&p.push(&record(EventKind::ReasoningDelta, json!({"message_id":"assistant","item_id":item,"provenance":"public_summary","text":"visible"}))).unwrap());
    assert!(
        events.iter().any(
            |event| event["type"] == "REASONING_MESSAGE_CONTENT" && event["delta"] == "visible"
        )
    );
    assert!(events.iter().any(|event| {
        event["messageId"]
            .as_str()
            .is_some_and(|id| id.ends_with(&item))
    }));
    let events = wire(&settle_tool(
        &json!([{ "type":"reasoning", "provenance":"public_summary", "item_id":item, "text":"visible nested summary", "provider_state":{"signature":"PRIVATE_SENTINEL"} }]),
    ));
    assert_eq!(events[0]["content"][0]["text"], "visible nested summary");
    assert!(
        !serde_json::to_string(&events)
            .unwrap()
            .contains("PRIVATE_SENTINEL")
    );
}
