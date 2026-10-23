use super::*;
use crabber_core::{
    Clock, ContentBlock, Message, MessageId, Part, PartId, PartKind, Role, RunId, SessionId,
    ToolCallId, TurnId,
};
use serde_json::json;
#[path = "codec_tests/anthropic.rs"]
mod anthropic;
#[path = "codec_tests/reasoning.rs"]
mod reasoning;

fn event(kind: &str, value: &serde_json::Value) -> sse::Event {
    sse::Event {
        name: kind.into(),
        data: value.to_string(),
    }
}
fn message(role: Role, kind: &PartKind, blocks: Vec<ContentBlock>) -> Message {
    let id = MessageId::new();
    Message {
        id: id.clone(),
        session_id: SessionId::new(),
        run_id: None,
        role,
        parent_id: None,
        parts: blocks
            .into_iter()
            .enumerate()
            .map(|(index, content)| Part {
                id: PartId::new(),
                message_id: id.clone(),
                ordinal: u32::try_from(index).unwrap(),
                kind: kind.clone(),
                content,
            })
            .collect(),
        created_at: crabber_core::SystemClock.now(),
    }
}
fn request(
    messages: Vec<Message>,
    tools: Vec<crabber_core::ToolInfo>,
    tool_choice: Option<&str>,
) -> ModelRequest {
    ModelRequest {
        context: vec![],
        identity: RequestIdentity {
            session_id: SessionId::new(),
            run_id: RunId::new(),
            turn_id: TurnId::new(),
        },
        selection: Selection {
            provider_id: "anthropic".into(),
            model_id: "model".into(),
        },
        system: None,
        messages,
        tools,
        temperature: None,
        max_tokens: None,
        reasoning_effort: None,
        tool_choice: tool_choice.map(Into::into),
    }
}
fn fixture(text: &str, decode: impl FnMut(&sse::Event) -> Vec<StreamDelta>) -> Vec<StreamDelta> {
    let mut parser = sse::Parser::default();
    parser
        .push(text.as_bytes())
        .unwrap()
        .iter()
        .flat_map(decode)
        .collect()
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
fn chat_stream_error_classification_and_sanitization() {
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
    let negative = chat_error("", r#"{"error":{"code":-1,"message":"SECRET_MESSAGE"}}"#);
    assert_eq!(
        (negative.kind, negative.retryable),
        (ProviderErrorKind::Server, true)
    );
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
fn chat_stream_error_malformed_forms_are_sanitized() {
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
            r#"{"error":{"code":429,"message":"SECRET_MESSAGE","metadata":[]}}"#,
        ),
        (
            "",
            r#"{"error":{"code":429,"message":"SECRET_MESSAGE","metadata":null}}"#,
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
fn chat_stream_error_does_not_change_normal_events() {
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
    let tool = codec
        .event(&event("", &json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call","function":{"name":"echo","arguments":"{}"}}]}}]})))
        .unwrap();
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

#[test]
fn responses_fixture() {
    let mut codec = responses::Codec::new(false);
    let deltas = fixture(include_str!("../testdata/openai/tool.sse"), |e| {
        codec.event(e).unwrap()
    });
    assert!(matches!(&deltas[0], StreamDelta::ToolCallStart { name, .. } if name == "echo"));
    let args: String = deltas
        .iter()
        .filter_map(|d| match d {
            StreamDelta::ToolCallArgsDelta { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(args, r#"{"text":"hello"}"#);
    assert!(matches!(deltas.last(), Some(StreamDelta::Completed)));
    assert!(deltas.iter().any(
        |d| matches!(d, StreamDelta::Usage(u) if u.input_tokens == 12 && u.output_tokens == 7)
    ));
}
#[test]
fn codex_fixture_and_echo_golden() {
    let mut codec = responses::Codec::new(true);
    let deltas = fixture(include_str!("../testdata/codex/tool.sse"), |e| {
        codec.event(e).unwrap()
    });
    let state = deltas
        .iter()
        .find_map(|d| match d {
            StreamDelta::ProviderState { payload, .. } => Some(payload.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!(state["encrypted_content"], "opaque");
    assert!(deltas.iter().any(
        |d| matches!(d, StreamDelta::Usage(u) if u.input_tokens == 20 && u.output_tokens == 9)
    ));
    let message_id = MessageId::new();
    let make_part = |ordinal, kind, content| Part {
        id: PartId::new(),
        message_id: message_id.clone(),
        ordinal,
        kind,
        content,
    };
    let assistant = Message {
        id: message_id.clone(),
        session_id: SessionId::from("s"),
        run_id: None,
        role: Role::Assistant,
        parent_id: None,
        parts: vec![
            make_part(
                0,
                PartKind::ProviderState,
                ContentBlock::ProviderState {
                    codec_id: responses::CODEX_STATE.into(),
                    payload: state,
                },
            ),
            make_part(
                1,
                PartKind::FunctionToolCall,
                ContentBlock::ToolCall {
                    call_id: ToolCallId::from("call_2"),
                    name: "echo".into(),
                    arguments: json!({"text":"hi"}),
                },
            ),
        ],
        created_at: crabber_core::SystemClock.now(),
    };
    let result = Message {
        id: MessageId::new(),
        session_id: SessionId::from("s"),
        run_id: None,
        role: Role::Tool,
        parent_id: None,
        parts: vec![make_part(
            0,
            PartKind::FunctionToolResult,
            ContentBlock::ToolResult {
                call_id: ToolCallId::from("call_2"),
                content: vec![ContentBlock::Text { text: "ok".into() }],
                is_error: false,
            },
        )],
        created_at: crabber_core::SystemClock.now(),
    };
    let request = ModelRequest {
        context: Vec::new(),
        identity: RequestIdentity {
            session_id: SessionId::from("s"),
            run_id: RunId::from("r"),
            turn_id: TurnId::from("t"),
        },
        selection: Selection {
            provider_id: "codex".into(),
            model_id: "gpt-5.5".into(),
        },
        system: None,
        messages: vec![assistant, result],
        tools: vec![],
        temperature: None,
        max_tokens: None,
        reasoning_effort: None,
        tool_choice: None,
    };
    let body = responses::body(&request, true);
    assert_eq!(body["input"][0]["type"], "reasoning");
    assert_eq!(body["input"][1]["type"], "function_call");
    assert_eq!(body["input"][2]["type"], "function_call_output");
    assert_eq!(body["store"], false);
    assert_eq!(body["stream"], true);
}
#[test]
fn anthropic_fixture() {
    let mut codec = messages::Codec::default();
    let deltas = fixture(include_str!("../testdata/anthropic/tool.sse"), |e| {
        codec.event(e).unwrap()
    });
    assert!(matches!(&deltas[0], StreamDelta::ToolCallStart { name, .. } if name == "echo"));
    assert!(
        matches!(&deltas[1], StreamDelta::ToolCallArgsDelta { text, .. } if text == r#"{"text":"hello"}"#)
    );
    assert!(deltas.iter().any(
        |d| matches!(d, StreamDelta::Usage(u) if u.input_tokens == 14 && u.output_tokens == 5)
    ));
}
#[test]
fn anthropic_empty_tool_input_is_valid_json() {
    let mut codec = messages::Codec::default();
    let deltas = fixture(include_str!("../testdata/anthropic/empty_tool.sse"), |e| {
        codec.event(e).unwrap()
    });
    assert!(matches!(&deltas[0], StreamDelta::ToolCallStart { name, .. } if name == "echo"));
    assert!(matches!(&deltas[1], StreamDelta::ToolCallArgsDelta { text, .. } if text == "{}"));
    assert!(matches!(&deltas[2], StreamDelta::ToolCallDone { .. }));
    assert!(deltas.iter().any(
        |d| matches!(d, StreamDelta::Usage(u) if u.input_tokens == 8 && u.output_tokens == 2)
    ));
}
#[test]
fn opencode_chat_fixture() {
    let mut codec = chat::Codec::default();
    let deltas = fixture(include_str!("../testdata/opencode_go/chat.sse"), |e| {
        codec.event(e).unwrap()
    });
    let args: String = deltas
        .iter()
        .filter_map(|d| match d {
            StreamDelta::ToolCallArgsDelta { text, .. } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(args, r#"{"text":"go"}"#);
    assert!(deltas.iter().any(
        |d| matches!(d, StreamDelta::Usage(u) if u.input_tokens == 18 && u.output_tokens == 6)
    ));
    assert!(matches!(deltas.last(), Some(StreamDelta::Completed)));
}

#[test]
fn chat_body_reasoning_effort_per_value() {
    use crabber_core::ReasoningEffort::{High, Low, Medium};

    for (effort, expected) in [
        (None, None),
        (Some(Low), Some("low")),
        (Some(Medium), Some("medium")),
        (Some(High), Some("high")),
    ] {
        let mut request = request(vec![], vec![], None);
        request.reasoning_effort = effort;
        let body = chat::body(&request, chat::TokenMode::MaxTokens);
        assert_eq!(
            body.get("reasoning_effort")
                .and_then(|value| value.as_str()),
            expected
        );
        if effort.is_none() {
            assert_eq!(
                body,
                json!({
                    "model": "model",
                    "messages": [],
                    "stream": true,
                    "stream_options": {"include_usage": true}
                })
            );
        }
    }
}

#[test]
fn responses_body_reasoning_effort_per_value() {
    use crabber_core::ReasoningEffort::{High, Low, Medium};
    for codex in [false, true] {
        for effort in [None, Some(Low), Some(Medium), Some(High)] {
            let request = ModelRequest {
                context: vec![],
                identity: RequestIdentity {
                    session_id: "s".into(),
                    run_id: "r".into(),
                    turn_id: "t".into(),
                },
                selection: Selection {
                    provider_id: "provider".into(),
                    model_id: "model".into(),
                },
                system: None,
                messages: vec![],
                tools: vec![],
                temperature: None,
                max_tokens: None,
                reasoning_effort: effort,
                tool_choice: None,
            };
            let body = responses::body(&request, codex);
            assert_eq!(
                body.get("reasoning").cloned(),
                match effort {
                    Some(effort) => Some(json!({"effort":effort.as_str()})),
                    None if codex => Some(serde_json::Value::Null),
                    None => None,
                }
            );
            assert_eq!(body.get("include").is_some(), codex);
            if effort.is_none() {
                let mut expected = json!({"model":"model","input":[],"tools":[],"tool_choice":"auto",
                    "parallel_tool_calls":false,"store":false,"stream":true});
                if codex {
                    expected["reasoning"] = serde_json::Value::Null;
                    expected["include"] = json!(["reasoning.encrypted_content"]);
                }
                assert_eq!(
                    serde_json::to_vec(&body).unwrap(),
                    serde_json::to_vec(&expected).unwrap()
                );
            }
        }
    }
}
