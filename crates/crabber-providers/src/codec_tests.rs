use super::*;
use crabber_core::{
    Clock, ContentBlock, Message, MessageId, Part, PartId, PartKind, Role, RunId, SessionId,
    ToolCallId, TurnId,
};
use serde_json::json;
#[path = "codec_tests/anthropic.rs"]
mod anthropic;
#[path = "codec_tests/chat.rs"]
mod chat_tests;
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
