use base64::Engine;
use crabber_core::{
    AdmissionKind, ContinuationGeneration, FrontendTool, InputContent, InputContext, InputEnvelope,
    InputFingerprint, InputMessage, InputPart, InputState, MessageContent,
    ProviderBindingDescriptor, ProviderBindingLimits, Role, SessionContext, SessionRevision,
};
use serde_json::{Value, json};

fn message() -> InputMessage {
    InputMessage {
        id: Some("message-1".into()),
        parent_id: None,
        role: Role::User,
        parts: vec![InputPart {
            id: Some("part-1".into()),
            content: InputContent::Content {
                content: MessageContent::Text {
                    text: "hello".into(),
                },
            },
        }],
    }
}
fn envelope() -> InputEnvelope {
    InputEnvelope::new(vec![message()], vec![FrontendTool { name: "ui".into(), description: "frontend".into(), input_parameters: json!({"type":"object","properties":{"b":{"type":"integer"},"a":{"type":"string"}}}), result_contract: None }], vec![InputContext { description: "context".into(), value: json!({"b":[2,3],"a":1}) }], InputState::Present { value: json!({"root":null,"zero":0}) }, SessionRevision::new(7).unwrap(), AdmissionKind::Ordinary).unwrap()
}
fn fingerprint(envelope: &InputEnvelope) -> InputFingerprint {
    envelope
        .fingerprint(
            &SessionContext::new("session".into(), "workspace", "/workspace"),
            &binding("v1"),
            &InputFingerprint::new("c".repeat(64)).unwrap(),
            &InputFingerprint::new("d".repeat(64)).unwrap(),
        )
        .unwrap()
}
fn binding(version: &str) -> ProviderBindingDescriptor {
    ProviderBindingDescriptor::new(
        "provider",
        "model",
        "backend",
        version,
        InputFingerprint::new("a".repeat(64)).unwrap(),
        ProviderBindingLimits {
            context_tokens: Some(8192),
            output_tokens: Some(1024),
        },
    )
    .unwrap()
}
fn changed(base: &Value, mutate: impl FnOnce(&mut Value)) -> InputEnvelope {
    let mut wire = base.clone();
    mutate(&mut wire);
    serde_json::from_value(wire).unwrap()
}

#[test]
fn canonical_fingerprint_preserves_ids_order_null_state_and_continuation_semantics() {
    let input = envelope();
    let original = fingerprint(&input);
    assert_eq!(
        original.as_str(),
        "1ffe28b30ce04dde9f6d37c2865f64147d733a749b424af9dfea53863a43d0ca"
    );
    let wire = serde_json::to_value(&input).unwrap();
    let equivalent = changed(&wire, |v| {
        v["context"][0]["value"] = serde_json::from_str(r#"{"a":1,"b":[2,3]}"#).unwrap();
    });
    assert_eq!(fingerprint(&equivalent), original);
    for mutation in [
        changed(&wire, |v| v["messages"][0]["id"] = json!("other")),
        changed(&wire, |v| {
            v["messages"][0]["parent_id"] = json!("prior-message");
        }),
        changed(&wire, |v| v["messages"][0]["parts"][0]["id"] = Value::Null),
        changed(&wire, |v| v["context"][0]["value"]["b"] = json!([3, 2])),
        changed(&wire, |v| {
            v["frontend_tools"][0]["input_parameters"]["properties"]["a"]["type"] = json!("number");
        }),
        changed(&wire, |v| {
            v["frontend_tools"][0]["result_contract"] = json!({"type":"string"});
        }),
        changed(&wire, |v| v["state"] = json!({"kind":"absent"})),
        changed(&wire, |v| {
            v["state"] = json!({"kind":"present","value":null});
        }),
        changed(&wire, |v| v["expected_revision"] = json!(8)),
        changed(&wire, |v| {
            v["admission"] = json!({"kind":"continuation","run_id":"run","generation":1});
        }),
        changed(&wire, |v| {
            v["admission"] = json!({"kind":"continuation","run_id":"run","generation":2});
        }),
    ] {
        assert_ne!(fingerprint(&mutation), original);
    }
    let alternate = input
        .fingerprint(
            &SessionContext::new("session".into(), "workspace", "/other"),
            &binding("v1"),
            &InputFingerprint::new("c".repeat(64)).unwrap(),
            &InputFingerprint::new("d".repeat(64)).unwrap(),
        )
        .unwrap();
    assert_ne!(alternate, original);
    let alternate = input
        .fingerprint(
            &SessionContext::new("session".into(), "workspace", "/workspace"),
            &binding("v2"),
            &InputFingerprint::new("c".repeat(64)).unwrap(),
            &InputFingerprint::new("d".repeat(64)).unwrap(),
        )
        .unwrap();
    assert_ne!(alternate, original);
}

#[test]
fn rejects_duplicate_identity_role_confusion_unknown_versions_and_fields() {
    let wire = serde_json::to_value(envelope()).unwrap();
    let invalid = [
        {
            let mut v = wire.clone();
            v["version"] = json!(0);
            v
        },
        {
            let mut v = wire.clone();
            v["ignored"] = json!(true);
            v
        },
        {
            let mut v = wire.clone();
            v["messages"][0]["id"] = json!("");
            v
        },
        {
            let mut v = wire.clone();
            v["messages"][0]["role"] = json!("tool");
            v
        },
        {
            let mut v = wire.clone();
            let copy = v["messages"][0].clone();
            v["messages"].as_array_mut().unwrap().push(copy);
            v
        },
        {
            let mut v = wire.clone();
            let copy = v["messages"][0]["parts"][0].clone();
            v["messages"][0]["parts"].as_array_mut().unwrap().push(copy);
            v
        },
        {
            let mut v = wire.clone();
            let copy = v["frontend_tools"][0].clone();
            v["frontend_tools"].as_array_mut().unwrap().push(copy);
            v
        },
        {
            let mut v = wire.clone();
            v["admission"] = json!({"kind":"continuation","run_id":"run","generation":0});
            v
        },
    ];
    for invalid in invalid {
        assert!(serde_json::from_value::<InputEnvelope>(invalid).is_err());
    }
    assert!(
        InputEnvelope::new(
            vec![],
            vec![],
            vec![],
            InputState::Absent,
            SessionRevision::INITIAL,
            AdmissionKind::Ordinary
        )
        .is_err()
    );
    assert!(
        InputEnvelope::new(
            vec![],
            vec![],
            vec![],
            InputState::Absent,
            SessionRevision::INITIAL,
            AdmissionKind::Continuation {
                run_id: "run".into(),
                generation: ContinuationGeneration::INITIAL
            }
        )
        .is_ok()
    );
}

#[test]
fn explicit_media_is_preserved_without_fetching_or_guessing_references() {
    let base = serde_json::to_value(envelope()).unwrap();
    for (kind, mime_type) in [
        ("image", "image/png"),
        ("audio", "audio/wav"),
        ("video", "video/mp4"),
        ("document", "application/pdf"),
    ] {
        for source in [
            json!({"kind":"data","base64":"aGVsbG8="}),
            json!({"kind":"url","url":"https://example.invalid/object?signature=PRIVATE_SENTINEL"}),
            json!({"kind":"file","file_id":"opaque-file"}),
        ] {
            let input = changed(
                &base,
                |v| v["messages"][0]["parts"][0]["content"] = json!({"type":"content","content":{"type":"media","kind":kind,"mime_type":mime_type,"source":source}}),
            );
            let encoded = serde_json::to_value(&input).unwrap();
            assert_eq!(
                encoded["messages"][0]["parts"][0]["content"]["content"]["source"],
                source
            );
            assert!(!format!("{input:?}").contains("PRIVATE_SENTINEL"));
        }
    }
    for content in [
        json!({"type":"media","kind":"image","mime_type":"image/png","source":{"kind":"url","url":"file:///secret"}}),
        json!({"type":"media","kind":"image","mime_type":"image/png","source":{"kind":"url","url":"https://user:password@example.invalid/image"}}),
        json!({"type":"media","kind":"image","mime_type":"image/png/extra","source":{"kind":"file","file_id":"id"}}),
        json!({"type":"media","kind":"image","mime_type":"audio/wav","source":{"kind":"file","file_id":"id"}}),
        json!({"type":"media","kind":"image","mime_type":"image/png","source":{"kind":"data","base64":"not base64"}}),
    ] {
        let mut wire = base.clone();
        wire["messages"][0]["parts"][0]["content"] = json!({"type":"content","content":content});
        assert!(serde_json::from_value::<InputEnvelope>(wire).is_err());
    }
}

#[test]
fn bounds_escaped_bytes_nesting_counts_and_retains_arbitrary_json_state() {
    let base = serde_json::to_value(envelope()).unwrap();
    let arbitrary = changed(&base, |v| {
        v["state"] =
            json!({"kind":"present","value":{"nul\u{0}":"value\u{0}","json":[null,true,42,{},[]]}});
    });
    let encoded = serde_json::to_value(&arbitrary).unwrap();
    assert_eq!(encoded["state"]["value"]["nul\u{0}"], "value\u{0}");
    for text in [
        "x".repeat(crabber_core::input::MAX_INPUT_BYTES),
        "\n".repeat(crabber_core::input::MAX_INPUT_BYTES / 2),
    ] {
        let mut wire = base.clone();
        wire["messages"][0]["parts"][0]["content"]["content"]["text"] = json!(text);
        let error = serde_json::from_value::<InputEnvelope>(wire).unwrap_err();
        assert!(error.to_string().contains("invalid typed input"));
    }
    let mut nested = Value::Null;
    for _ in 0..=crabber_core::input::MAX_INPUT_JSON_DEPTH {
        nested = json!([nested]);
    }
    let mut wire = base.clone();
    wire["context"][0]["value"] = nested;
    assert!(serde_json::from_value::<InputEnvelope>(wire).is_err());
    let mut wire = base;
    wire["messages"][0]["parts"] = json!([]);
    assert!(serde_json::from_value::<InputEnvelope>(wire).is_err());
}

#[test]
fn response_sets_keep_output_separate_from_frontend_input_schema_and_redact_rejections() {
    let base = serde_json::to_value(envelope()).unwrap();
    let result = changed(&base, |v| {
        v["admission"] = json!({"kind":"continuation","run_id":"run","generation":1});
        v["messages"][0]["role"] = json!("tool");
        v["messages"][0]["parts"][0]["content"] = json!({"type":"tool_result","call_id":"call","content":[{"type":"text","text":"text result for object input parameters"}],"is_error":false});
    });
    assert_eq!(result.messages()[0].role, Role::Tool);
    let second = changed(&serde_json::to_value(&result).unwrap(), |v| {
        v["admission"]["generation"] = json!(2);
    });
    assert_ne!(fingerprint(&result), fingerprint(&second));
    let mut invalid = base;
    invalid["messages"][0]["parts"][0]["content"]["type"] = json!("PRIVATE_REJECTION_SENTINEL");
    let error = serde_json::from_value::<InputEnvelope>(invalid).unwrap_err();
    assert!(!error.to_string().contains("PRIVATE_REJECTION_SENTINEL"));
}

#[test]
fn counts_and_inline_media_are_bounded_before_acceptance() {
    let mut raw = message();
    raw.id = None;
    raw.parts[0].id = None;
    assert!(
        InputEnvelope::new(
            vec![raw.clone(); crabber_core::input::MAX_INPUT_MESSAGES + 1],
            vec![],
            vec![],
            InputState::Absent,
            SessionRevision::INITIAL,
            AdmissionKind::Ordinary
        )
        .is_err()
    );
    raw.parts = vec![raw.parts[0].clone(); crabber_core::input::MAX_INPUT_PARTS + 1];
    assert!(
        InputEnvelope::new(
            vec![raw],
            vec![],
            vec![],
            InputState::Absent,
            SessionRevision::INITIAL,
            AdmissionKind::Ordinary
        )
        .is_err()
    );
    let oversized = base64::engine::general_purpose::STANDARD.encode(vec![
        0;
        crabber_core::input::MAX_INLINE_MEDIA_BYTES
            + 1
    ]);
    let mut wire = serde_json::to_value(envelope()).unwrap();
    wire["messages"][0]["parts"][0]["content"] = json!({"type":"content","content":{"type":"media","kind":"image","mime_type":"image/png","source":{"kind":"data","base64":oversized}}});
    assert!(serde_json::from_value::<InputEnvelope>(wire).is_err());
}

#[test]
fn ordered_parts_and_declared_behavior_config_are_distinct_hash_inputs() {
    let base = serde_json::to_value(envelope()).unwrap();
    let ordered = changed(&base, |v| {
        let mut second = v["messages"][0]["parts"][0].clone();
        second["id"] = json!("part-2");
        second["content"]["content"]["text"] = json!("world");
        v["messages"][0]["parts"]
            .as_array_mut()
            .unwrap()
            .push(second);
    });
    let reversed = changed(&serde_json::to_value(&ordered).unwrap(), |v| {
        v["messages"][0]["parts"].as_array_mut().unwrap().reverse();
    });
    assert_ne!(fingerprint(&ordered), fingerprint(&reversed));
    for (config, behavior) in [('e', 'd'), ('c', 'e')] {
        let changed = ordered
            .fingerprint(
                &SessionContext::new("session".into(), "workspace", "/workspace"),
                &binding("v1"),
                &InputFingerprint::new(config.to_string().repeat(64)).unwrap(),
                &InputFingerprint::new(behavior.to_string().repeat(64)).unwrap(),
            )
            .unwrap();
        assert_ne!(changed, fingerprint(&ordered));
    }
}
