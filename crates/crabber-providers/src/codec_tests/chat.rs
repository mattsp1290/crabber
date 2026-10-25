use super::*;

#[test]
fn finish_reason_is_preserved_without_ending_before_late_usage() {
    use crabber_core::ModelFinishReason;
    for (wire, normalized) in [
        ("stop", ModelFinishReason::Stop),
        ("tool_calls", ModelFinishReason::ToolCalls),
        ("length", ModelFinishReason::Length),
        ("content_filter", ModelFinishReason::ContentFilter),
        ("PRIVATE_VENDOR_REASON", ModelFinishReason::Other),
    ] {
        let mut codec = chat::Codec::default();
        let terminal = codec
            .event(&event(
                "",
                &json!({"choices":[{"delta":{},"finish_reason":wire}]}),
            ))
            .unwrap();
        assert_eq!(terminal, [StreamDelta::FinishReason(normalized)]);
        let usage = codec
            .event(&event(
                "",
                &json!({"choices":[],"usage":{"prompt_tokens":4,"completion_tokens":32}}),
            ))
            .unwrap();
        assert!(
            matches!(usage.as_slice(), [StreamDelta::Usage(value)] if value.input_tokens == 4 && value.output_tokens == 32)
        );
        let done = codec
            .event(&sse::Event {
                name: String::new(),
                data: "[DONE]".into(),
            })
            .unwrap();
        assert_eq!(done, [StreamDelta::Completed]);
    }
}

fn chat_error(event_name: &str, data: &str) -> ProviderError {
    let deltas = chat::Codec::default()
        .event(&sse::Event {
            name: event_name.into(),
            data: data.into(),
        })
        .unwrap();
    assert_eq!(deltas.len(), 1);
    match deltas.into_iter().next().unwrap() {
        StreamDelta::Error(error) => error,
        other => panic!("expected Chat stream error, got {other:?}"),
    }
}

#[test]
fn stream_error_classification_and_sanitization() {
    let typed = [
        (
            "rate_limit_exceeded",
            401_u64,
            ProviderErrorKind::RateLimited,
            true,
        ),
        ("authentication", 429, ProviderErrorKind::Auth, false),
        ("permission_denied", 429, ProviderErrorKind::Auth, false),
        (
            "context_length_exceeded",
            429,
            ProviderErrorKind::ContextOverflow,
            false,
        ),
        ("provider_overloaded", 400, ProviderErrorKind::Server, true),
        ("provider_unavailable", 400, ProviderErrorKind::Server, true),
        ("invalid_request", 429, ProviderErrorKind::Invalid, false),
    ];
    for (error_type, code, kind, retryable) in typed {
        let data = format!(
            r#"{{"error":{{"code":{code},"message":"SECRET_MESSAGE","metadata":{{"error_type":"{error_type}","provider_name":"SECRET_PROVIDER"}}}},"provider_code":"SECRET_CODE"}}"#
        );
        let error = chat_error("", &data);
        assert_eq!((&error.kind, error.retryable), (&kind, retryable));
        assert_eq!(error.message, "Chat stream error");
        let diagnostic = format!("{error} {error:?}");
        for secret in [
            "SECRET_MESSAGE",
            "SECRET_PROVIDER",
            "SECRET_CODE",
            error_type,
        ] {
            assert!(!diagnostic.contains(secret), "secret leaked: {diagnostic}");
        }
    }

    let fallback = [
        (429, ProviderErrorKind::RateLimited, true),
        (401, ProviderErrorKind::Auth, false),
        (403, ProviderErrorKind::Auth, false),
        (400, ProviderErrorKind::Invalid, false),
        (402, ProviderErrorKind::Invalid, false),
        (404, ProviderErrorKind::Invalid, false),
        (408, ProviderErrorKind::Invalid, false),
        (413, ProviderErrorKind::Invalid, false),
        (422, ProviderErrorKind::Invalid, false),
        (500, ProviderErrorKind::Server, true),
        (599, ProviderErrorKind::Server, true),
        (200, ProviderErrorKind::Server, true),
        (u64::MAX, ProviderErrorKind::Server, true),
    ];
    for metadata in [None, Some("unknown"), Some("Rate_Limit_Exceeded")] {
        for (code, kind, retryable) in &fallback {
            let metadata = metadata.map_or(String::new(), |value| {
                format!(r#","metadata":{{"error_type":"{value}"}}"#)
            });
            let data =
                format!(r#"{{"error":{{"code":{code},"message":"SECRET_MESSAGE"{metadata}}}}}"#);
            let error = chat_error("", &data);
            assert_eq!((&error.kind, error.retryable), (kind, *retryable));
        }
    }
    for code in [-1_i64, i64::MIN] {
        let negative = chat_error(
            "",
            &format!(r#"{{"error":{{"code":{code},"message":"SECRET_MESSAGE"}}}}"#),
        );
        assert_eq!(
            (negative.kind, negative.retryable),
            (ProviderErrorKind::Server, true)
        );
    }
    for metadata in [r"{}", r#"{"error_type":null}"#] {
        let data = format!(
            r#"{{"error":{{"code":429,"message":"SECRET_MESSAGE","metadata":{metadata}}}}}"#
        );
        let error = chat_error("", &data);
        assert_eq!(
            (error.kind, error.retryable),
            (ProviderErrorKind::RateLimited, true)
        );
    }

    let named = chat_error(
        "error",
        r#"{"error":{"code":503,"message":"SECRET_MESSAGE","metadata":{"error_type":"provider_overloaded"}}}"#,
    );
    assert_eq!(
        (named.kind, named.retryable),
        (ProviderErrorKind::Server, true)
    );
}

#[test]
fn stream_error_malformed_forms_are_sanitized() {
    let malformed = [
        ("error", "not-json"),
        ("error", "[DONE]"),
        ("error", "[]"),
        ("error", r#"{"unrelated":"SECRET_UNRELATED"}"#),
        ("", r#"{"error":null}"#),
        ("", r#"{"error":"SECRET_SCALAR"}"#),
        ("", r#"{"error":[]}"#),
        ("", r#"{"error":{"message":"SECRET_MESSAGE"}}"#),
        ("", r#"{"error":{"code":null,"message":"SECRET_MESSAGE"}}"#),
        (
            "",
            r#"{"error":{"code":"SECRET_CODE","message":"SECRET_MESSAGE"}}"#,
        ),
        ("", r#"{"error":{"code":429.5,"message":"SECRET_MESSAGE"}}"#),
        ("", r#"{"error":{"code":429}}"#),
        ("", r#"{"error":{"code":429,"message":null}}"#),
        ("", r#"{"error":{"code":429,"message":[]}}"#),
        (
            "",
            r#"{"error":{"code":429,"message":"SECRET_MESSAGE","metadata":null}}"#,
        ),
        (
            "",
            r#"{"error":{"code":429,"message":"SECRET_MESSAGE","metadata":[]}}"#,
        ),
        (
            "",
            r#"{"error":{"code":429,"message":"SECRET_MESSAGE","metadata":{"error_type":7}}}"#,
        ),
    ];
    for (name, data) in malformed {
        let error = chat_error(name, data);
        assert_eq!(error.kind, ProviderErrorKind::Invalid);
        assert!(!error.retryable);
        assert_eq!(error.message, "invalid Chat stream error");
        let diagnostic = format!("{error} {error:?}");
        for secret in [
            "SECRET_UNRELATED",
            "SECRET_SCALAR",
            "SECRET_CODE",
            "SECRET_MESSAGE",
        ] {
            assert!(!diagnostic.contains(secret));
        }
    }
}

#[test]
fn stream_error_does_not_change_normal_events() {
    let mut codec = chat::Codec::default();
    let text = codec
        .event(&event(
            "",
            &json!({"choices":[{"delta":{"content":"hello"}}]}),
        ))
        .unwrap();
    assert!(matches!(text.as_slice(), [StreamDelta::TextDelta(value)] if value == "hello"));
    let usage = codec
        .event(&event(
            "",
            &json!({"usage":{"prompt_tokens":2,"completion_tokens":3},"choices":[]}),
        ))
        .unwrap();
    assert!(
        matches!(usage.as_slice(), [StreamDelta::Usage(value)] if value.input_tokens == 2 && value.output_tokens == 3)
    );
    let tool = codec.event(&event("", &json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call","function":{"name":"echo","arguments":"{}"}}]}}]}))).unwrap();
    assert!(matches!(
        tool.as_slice(),
        [
            StreamDelta::ToolCallStart { .. },
            StreamDelta::ToolCallArgsDelta { .. }
        ]
    ));
    assert!(matches!(
        codec
            .event(&sse::Event {
                name: String::new(),
                data: "[DONE]".into()
            })
            .unwrap()
            .as_slice(),
        [StreamDelta::Completed]
    ));
}
