use crabber_agui::{WireInputError, convert_run_input};
use crabber_core::{
    AdmissionKind, ContinuationGeneration, FrontendResultStatus, InputContent, InputState,
    MediaKind, MediaSource, MessageContent, RunId, SessionRevision, ToolCallId,
};
use serde_json::{Value, json};

fn input(messages: Value) -> Value {
    let mut value = json!({"threadId":"wire-thread", "runId":"wire-exchange"});
    value["messages"] = messages;
    value
}
fn ordinary(value: Value) -> Result<crabber_core::InputEnvelope, WireInputError> {
    convert_run_input(value)?.envelope(SessionRevision::INITIAL, AdmissionKind::Ordinary)
}

#[test]
fn normalization_uses_sdk_rules_and_preserves_nested_json_and_transport_data() {
    let mut value = input(json!([{"id":"u", "role":"user", "content":"question"}]));
    value["state"] = Value::Null;
    value["futureField"] = json!({"value":"ignored by SDK policy"});
    value["forwardedProps"] = json!({"nested":null});
    let normalized = convert_run_input(value.clone()).unwrap();
    let sdk = crabber_agui::ag_ui_core::processing::normalize_run_input(value).unwrap();
    assert_eq!(normalized.wire(), &sdk.input);
    assert_eq!(normalized.diagnostics(), sdk.diagnostics);
    assert_eq!(normalized.effective_state(), &json!({}));
    assert_eq!(normalized.wire().forwarded_props, json!({"nested":null}));
    let envelope = normalized
        .envelope(SessionRevision::INITIAL, AdmissionKind::Ordinary)
        .unwrap();
    assert_eq!(envelope.state(), &InputState::Present { value: json!({}) });
    assert_eq!(envelope.messages()[0].id.as_deref(), Some("u"));
    assert!(!format!("{normalized:?}").contains("question"));
    for field in ["tools", "context"] {
        let mut null_list = input(json!([{"id":"u", "role":"user", "content":"question"}]));
        null_list[field] = Value::Null;
        assert!(
            crabber_agui::ag_ui_core::processing::normalize_run_input(null_list.clone()).is_err()
        );
        assert!(matches!(
            convert_run_input(null_list),
            Err(WireInputError::Malformed)
        ));
    }

    let mut value = input(json!([{"id":"u", "role":"user", "content":"question"}]));
    value["state"] = json!({"nested":[null, {"value":null}]});
    let converted = ordinary(value).unwrap();
    assert_eq!(
        converted.state(),
        &InputState::Present {
            value: json!({"nested":[null,{"value":null}]})
        }
    );
}

#[test]
fn typed_media_context_frontend_schema_and_ui_activity_stay_distinct() {
    let mut value = input(json!([
        {"id":"u", "role":"user", "content":[
            {"type":"text", "id":"text-part", "text":"image"},
            {"type":"image", "id":"image-part", "source":{"type":"url", "value":"https://example.invalid/image.png", "mimeType":"image/png"}}
        ]},
        {"id":"status", "role":"activity", "activityType":"progress", "content":{"stage":null}}
    ]));
    value["context"] = json!([{"description":"literal", "value":"{\"x\":null}"}]);
    value["tools"] =
        json!([{"name":"frontend", "description":"display", "parameters":{"type":"object"}}]);
    let normalized = convert_run_input(value).unwrap();
    let activity = normalized.activities().unwrap();
    assert_eq!(activity.len(), 1);
    assert_eq!(activity[0].content(), &json!({"stage":null}));
    let envelope = normalized
        .envelope(SessionRevision::INITIAL, AdmissionKind::Ordinary)
        .unwrap();
    assert_eq!(envelope.messages().len(), 1);
    assert_eq!(
        envelope.messages()[0].parts[1].id.as_deref(),
        Some("image-part")
    );
    assert!(matches!(&envelope.messages()[0].parts[1].content,
        InputContent::Content { content: MessageContent::Media { kind: MediaKind::Image, mime_type, source: MediaSource::Url { url } } }
        if mime_type == "image/png" && url == "https://example.invalid/image.png"));
    assert_eq!(
        envelope.context()[0].value,
        Value::String("{\"x\":null}".into())
    );
    assert!(envelope.frontend_tools()[0].result_contract.is_none());
}

#[test]
fn frontend_results_preserve_partial_failure_and_require_selected_complete_ids() {
    let normalized = convert_run_input(input(json!([
        {"id":"result", "role":"tool", "toolCallId":"call", "error":"partial failure", "content":[
            {"type":"text", "text":"partial"},
            {"type":"audio", "source":{"type":"data", "value":"YQ==", "mimeType":"audio/wav"}}
        ]}
    ])))
    .unwrap();
    let results = normalized
        .frontend_results(&[ToolCallId::from("call")])
        .unwrap();
    assert_eq!(results[0].status(), FrontendResultStatus::Failed);
    assert!(results[0].output().is_error());
    assert!(
        matches!(&results[0].output().content()[1], MessageContent::Media { kind: MediaKind::Audio, source: MediaSource::Data { base64 }, .. } if base64 == "YQ==")
    );
    assert_eq!(
        normalized.frontend_results(&[ToolCallId::from("missing")]),
        Err(WireInputError::Malformed)
    );
    assert_eq!(
        normalized.frontend_results(&[ToolCallId::from("call"), ToolCallId::from("call")]),
        Err(WireInputError::Malformed)
    );
}

#[test]
fn resume_is_response_data_and_does_not_choose_a_runtime_run_or_generation() {
    let mut value = input(json!([]));
    value["parentRunId"] = json!("earlier-wire-exchange");
    value["resume"] = json!([{"interruptId":"exact-interaction", "status":"resolved", "payload":{"choice":null}}]);
    let normalized = convert_run_input(value).unwrap();
    assert_eq!(
        normalized.resume_entries()[0].payload,
        Some(json!({"choice":null}))
    );
    assert_eq!(
        normalized.envelope(SessionRevision::INITIAL, AdmissionKind::Ordinary),
        Err(WireInputError::Malformed)
    );
    let kind = AdmissionKind::Continuation {
        run_id: RunId::from("host-selected-runtime-run"),
        generation: ContinuationGeneration::INITIAL,
    };
    let envelope = normalized
        .envelope(SessionRevision::INITIAL, kind.clone())
        .unwrap();
    assert_eq!(envelope.admission_kind(), &kind);
    assert_eq!(normalized.wire().run_id.as_str(), "wire-exchange");
    assert_eq!(
        normalized.wire().parent_run_id.as_ref().unwrap().as_str(),
        "earlier-wire-exchange"
    );
}

#[test]
fn unsupported_attribution_and_media_are_rejected_without_guessing() {
    for message in [
        json!({"id":"m", "role":"developer", "content":"private"}),
        json!({"id":"m", "role":"reasoning", "content":"private"}),
        json!({"id":"m", "role":"user", "content":"private", "encryptedValue":"private"}),
        json!({"id":"m", "role":"user", "content":"private", "subagentRunId":"forged"}),
        json!({"id":"m", "role":"user", "content":[{"type":"image", "source":{"type":"url", "value":"https://example.invalid/a"}}]}),
        json!({"id":"m", "role":"user", "content":[{"type":"image", "source":{"type":"file", "provider":"different-provider", "value":"opaque", "mimeType":"image/png"}}]}),
    ] {
        assert_eq!(
            ordinary(input(json!([message]))),
            Err(WireInputError::Unsupported)
        );
    }
}

#[test]
fn malformed_identity_counts_and_depth_are_bounded_before_conversion() {
    let mut duplicate = input(json!([
        {"id":"same", "role":"user", "content":"text"},
        {"id":"same", "role":"activity", "activityType":"progress", "content":{}}
    ]));
    assert!(matches!(
        convert_run_input(duplicate.clone()),
        Err(WireInputError::Malformed)
    ));
    duplicate["messages"] = json!([]);
    duplicate["resume"] = json!([
        {"interruptId":"same", "status":"cancelled"},
        {"interruptId":"same", "status":"cancelled"}
    ]);
    assert!(matches!(
        convert_run_input(duplicate),
        Err(WireInputError::Malformed)
    ));
    let mut deep = Value::Null;
    for _ in 0..40 {
        deep = json!([deep]);
    }
    let mut value = input(json!([]));
    value["state"] = deep;
    assert!(matches!(
        convert_run_input(value),
        Err(WireInputError::Limit)
    ));
    let huge = input(
        json!([{"id":"u", "role":"user", "content":"x".repeat(crabber_core::input::MAX_INPUT_BYTES)}]),
    );
    assert!(matches!(
        convert_run_input(huge),
        Err(WireInputError::Limit)
    ));
}

#[test]
fn child_activity_and_unrepresentable_frontend_part_identity_are_refused() {
    let normalized = convert_run_input(input(json!([
        {"id":"u", "role":"user", "content":"question"},
        {"id":"child-status", "role":"activity", "activityType":"progress", "subagentRunId":"child", "content":{}}
    ]))).unwrap();
    assert_eq!(normalized.activities(), Err(WireInputError::Unsupported));
    assert_eq!(
        normalized.envelope(SessionRevision::INITIAL, AdmissionKind::Ordinary),
        Err(WireInputError::Unsupported)
    );
    let normalized = convert_run_input(input(json!([
        {"id":"result", "role":"tool", "toolCallId":"call", "content":[{"type":"text", "id":"original-part", "text":"answer"}]}
    ]))).unwrap();
    assert_eq!(
        normalized.frontend_results(&[ToolCallId::from("call")]),
        Err(WireInputError::Unsupported)
    );
    assert_eq!(
        normalized.envelope(SessionRevision::INITIAL, AdmissionKind::Ordinary),
        Err(WireInputError::Unsupported)
    );
}

#[test]
fn sdk_dropped_content_requires_explicit_host_policy_before_runtime_conversion() {
    let value = input(json!([{"id":"u", "role":"user", "content":[
        {"type":"text", "text":"keep"},
        {"type":"future-media", "source":{"opaque":"do not silently lose"}}
    ]}]));
    let normalized = convert_run_input(value).unwrap();
    assert!(
        normalized
            .diagnostics()
            .iter()
            .any(|diagnostic| diagnostic.category
                == crabber_agui::ag_ui_core::processing::DiagnosticCategory::UnknownVariant)
    );
    assert_eq!(
        normalized.envelope(SessionRevision::INITIAL, AdmissionKind::Ordinary),
        Err(WireInputError::Unsupported)
    );
    assert_eq!(normalized.activities(), Err(WireInputError::Unsupported));
    assert_eq!(
        normalized.frontend_results(&[]),
        Err(WireInputError::Unsupported)
    );
}
