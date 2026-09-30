use super::*;
use crabber_core::{
    Clock, ContentBlock, Message, MessageId, Part, PartId, PartKind, Role, RunId, SessionId,
    ToolCallId, TurnId,
};
use serde_json::json;

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
