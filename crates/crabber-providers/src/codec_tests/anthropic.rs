//! Messages stop reasons, stream error classification, and tool-free bodies.
use super::{event, fixture, message, messages, request, sse};
use crate::{ProviderError, ProviderErrorKind, StreamDelta};
use crabber_core::{
    ContentBlock, Message, PartKind, ReasoningProvenance, Role, ToolCallId, ToolInfo,
};
use serde_json::json;

fn decode(text: &str) -> Vec<StreamDelta> {
    let mut codec = messages::Codec::default();
    fixture(text, |e| codec.event(e).unwrap())
}
/// Decoded deltas up to and including the first terminal error, as the
/// adapter stream consumes them.
fn decode_until_error(text: &str) -> Vec<StreamDelta> {
    let mut deltas = decode(text);
    if let Some(end) = deltas
        .iter()
        .position(|d| matches!(d, StreamDelta::Error(_)))
    {
        deltas.truncate(end + 1);
    }
    deltas
}
fn last_error(deltas: &[StreamDelta]) -> &ProviderError {
    match deltas.last() {
        Some(StreamDelta::Error(error)) => error,
        other => panic!("expected terminal error, got {other:?}"),
    }
}
fn text_of(deltas: &[StreamDelta]) -> String {
    deltas
        .iter()
        .filter_map(|d| match d {
            StreamDelta::TextDelta(text) => Some(text.as_str()),
            _ => None,
        })
        .collect()
}
fn message_start() -> sse::Event {
    event(
        "message_start",
        &json!({"type":"message_start","message":{"usage":{"input_tokens":1}}}),
    )
}
fn text_block(text: &str) -> [sse::Event; 2] {
    [
        event(
            "content_block_start",
            &json!({"index":0,"content_block":{"type":"text","text":""}}),
        ),
        event(
            "content_block_delta",
            &json!({"index":0,"delta":{"type":"text_delta","text":text}}),
        ),
    ]
}
fn stream_error(kind: &str) -> sse::Event {
    event(
        "error",
        &json!({"type":"error","error":{"type":kind,"message":"PRIVATE_BODY_SENTINEL"}}),
    )
}
fn run(events: &[sse::Event]) -> Vec<StreamDelta> {
    let mut codec = messages::Codec::default();
    events
        .iter()
        .flat_map(|e| codec.event(e).unwrap())
        .collect()
}

#[test]
fn anthropic_text_thinking_fixture() {
    let deltas = decode(include_str!("../../testdata/anthropic/text_thinking.sse"));
    assert!(deltas.iter().any(|d| matches!(d,
        StreamDelta::Reasoning { item_id, provenance: ReasoningProvenance::Private, .. }
        if item_id == "anthropic:0")));
    assert!(deltas.iter().any(|d| matches!(d,
        StreamDelta::ReasoningSignature { item_id, signature }
        if item_id == "anthropic:0" && signature == "SIGNATURE_A")));
    assert_eq!(text_of(&deltas), "OK.");
    assert!(deltas.iter().any(
        |d| matches!(d, StreamDelta::Usage(u) if u.input_tokens == 11 && u.output_tokens == 7)
    ));
    assert!(matches!(deltas.last(), Some(StreamDelta::Completed)));
    assert!(!deltas.iter().any(|d| matches!(d, StreamDelta::Error(_))));
    // ping and future_event produce nothing: reasoning start, thought,
    // signature, two text deltas, usage, and completion.
    assert_eq!(deltas.len(), 7);
}

#[test]
fn anthropic_refusal_fixture_is_terminal_invalid_error() {
    let deltas = decode_until_error(include_str!("../../testdata/anthropic/refusal.sse"));
    assert_eq!(text_of(&deltas), "PARTIAL_REFUSED_TEXT");
    let error = last_error(&deltas);
    assert_eq!(error.kind, ProviderErrorKind::Invalid);
    assert_eq!(error.message, "Messages refusal");
    assert!(!error.retryable);
    assert!(!deltas.iter().any(|d| matches!(d, StreamDelta::Completed)));
}

#[test]
fn anthropic_truncated_tool_input_is_an_error() {
    let deltas = decode_until_error(include_str!("../../testdata/anthropic/truncated_tool.sse"));
    assert!(matches!(&deltas[0], StreamDelta::ToolCallStart { name, .. } if name == "echo"));
    assert!(matches!(&deltas[1], StreamDelta::ToolCallArgsDelta { .. }));
    assert!(matches!(&deltas[2], StreamDelta::ToolCallDone { .. }));
    let error = last_error(&deltas);
    assert_eq!(error.kind, ProviderErrorKind::Invalid);
    assert_eq!(error.message, "Messages output truncated");
    assert!(!error.retryable);
    assert!(!deltas.iter().any(|d| matches!(d, StreamDelta::Completed)));
}

#[test]
fn anthropic_thinking_only_truncation_is_an_error() {
    let deltas = decode_until_error(include_str!(
        "../../testdata/anthropic/thinking_only_max_tokens.sse"
    ));
    assert_eq!(last_error(&deltas).message, "Messages output truncated");
    assert!(!deltas.iter().any(|d| matches!(d, StreamDelta::Completed)));
}

#[test]
fn anthropic_text_truncation_completes() {
    let [start, delta] = text_block("partial answer");
    let deltas = run(&[
        message_start(),
        start,
        delta,
        event(
            "message_delta",
            &json!({"delta":{"stop_reason":"max_tokens"},"usage":{"output_tokens":2}}),
        ),
        event("message_stop", &json!({"type":"message_stop"})),
    ]);
    assert!(!deltas.iter().any(|d| matches!(d, StreamDelta::Error(_))));
    assert!(matches!(
        &deltas[deltas.len() - 2..],
        [StreamDelta::Usage(_), StreamDelta::Completed]
    ));
}

#[test]
fn anthropic_stream_error_types_are_classified() {
    for (kind, expected, retryable) in [
        ("overloaded_error", ProviderErrorKind::Server, true),
        ("api_error", ProviderErrorKind::Server, true),
        ("rate_limit_error", ProviderErrorKind::RateLimited, true),
        ("authentication_error", ProviderErrorKind::Auth, false),
        ("permission_error", ProviderErrorKind::Auth, false),
        ("billing_error", ProviderErrorKind::Invalid, false),
        ("invalid_request_error", ProviderErrorKind::Invalid, false),
        ("not_found_error", ProviderErrorKind::Invalid, false),
        (
            "request_too_large",
            ProviderErrorKind::ContextOverflow,
            false,
        ),
        ("future_error", ProviderErrorKind::Server, true),
    ] {
        let deltas = run(&[message_start(), stream_error(kind)]);
        let error = last_error(&deltas);
        assert_eq!(error.kind, expected, "{kind}");
        assert_eq!(error.retryable, retryable, "{kind}");
        assert_eq!(error.message, "Messages stream error");
        assert!(!format!("{deltas:?}").contains("PRIVATE_BODY_SENTINEL"));
    }
    let missing = run(&[event("error", &json!({"type":"error"}))]);
    assert_eq!(last_error(&missing).kind, ProviderErrorKind::Server);
}

#[test]
fn anthropic_overloaded_fixture_before_content_is_retryable() {
    let deltas = decode(include_str!("../../testdata/anthropic/overloaded.sse"));
    let error = last_error(&deltas);
    assert_eq!(error.kind, ProviderErrorKind::Server);
    assert!(error.retryable);
    assert!(!format!("{deltas:?}").contains("PRIVATE_BODY_SENTINEL"));
}

#[test]
fn anthropic_context_window_exceeded_and_unknown_stop_reasons() {
    let deltas = run(&[
        message_start(),
        event(
            "message_delta",
            &json!({"delta":{"stop_reason":"model_context_window_exceeded"}}),
        ),
    ]);
    let error = last_error(&deltas);
    assert_eq!(error.kind, ProviderErrorKind::ContextOverflow);
    assert_eq!(error.message, "Messages context window exceeded");
    assert!(!error.retryable);

    let [start, delta] = text_block("hi");
    let deltas = run(&[
        message_start(),
        start,
        delta,
        event(
            "message_delta",
            &json!({"delta":{"stop_reason":"future_reason"},"usage":{"output_tokens":1}}),
        ),
        event("message_stop", &json!({"type":"message_stop"})),
    ]);
    assert!(!deltas.iter().any(|d| matches!(d, StreamDelta::Error(_))));
    assert!(matches!(
        &deltas[deltas.len() - 2..],
        [StreamDelta::Usage(_), StreamDelta::Completed]
    ));
}

fn echo_tool() -> ToolInfo {
    ToolInfo {
        name: "echo".into(),
        description: "Echo".into(),
        parameters: json!({"type":"object"}),
        retry_safe: true,
        required_permissions: vec![],
    }
}
fn user_hi() -> Message {
    message(
        Role::User,
        &PartKind::InputText,
        vec![ContentBlock::Text { text: "hi".into() }],
    )
}

#[test]
fn anthropic_tool_choice_none_and_named_tool_map_exactly() {
    let body = messages::body(&request(vec![user_hi()], vec![echo_tool()], Some("none")));
    assert_eq!(body["tool_choice"], json!({"type":"none"}));
    assert_eq!(body["tools"][0]["name"], "echo");
    let body = messages::body(&request(vec![user_hi()], vec![echo_tool()], Some("echo")));
    assert_eq!(body["tool_choice"], json!({"type":"tool","name":"echo"}));
    let body = messages::body(&request(vec![user_hi()], vec![], Some("any")));
    assert!(body.get("tools").is_none());
    assert!(body.get("tool_choice").is_none());
    assert_eq!(body["max_tokens"], 4096);
    assert_eq!(body["stream"], true);
}

#[test]
fn anthropic_tool_history_keeps_tools_array_without_tool_list() {
    let call = message(
        Role::Assistant,
        &PartKind::InputText,
        vec![ContentBlock::ToolCall {
            call_id: ToolCallId::from("toolu_1"),
            name: "echo".into(),
            arguments: json!({"text":"hello"}),
        }],
    );
    let result = message(
        Role::Tool,
        &PartKind::InputText,
        vec![ContentBlock::ToolResult {
            call_id: ToolCallId::from("toolu_1"),
            content: vec![ContentBlock::Text { text: "ok".into() }],
            is_error: false,
        }],
    );
    let body = messages::body(&request(
        vec![user_hi(), call, result],
        vec![],
        Some("auto"),
    ));
    assert_eq!(body["tools"], json!([]));
    assert!(body.get("tool_choice").is_none());
    assert_eq!(body["messages"][1]["content"][0]["type"], "tool_use");
    assert_eq!(body["messages"][2]["content"][0]["type"], "tool_result");
}

fn thinking_start(index: u64) -> sse::Event {
    event(
        "content_block_start",
        &json!({"index":index,"content_block":{"type":"thinking","thinking":""}}),
    )
}

#[test]
fn anthropic_private_thinking_before_overload_stays_retryable() {
    let deltas = run(&[
        message_start(),
        thinking_start(0),
        event(
            "content_block_delta",
            &json!({"index":0,"delta":{"type":"thinking_delta","thinking":"PRIVATE"}}),
        ),
        stream_error("overloaded_error"),
    ]);
    let error = last_error(&deltas);
    assert_eq!(error.kind, ProviderErrorKind::Server);
    assert!(error.retryable, "private thinking is never published");
}

#[test]
fn anthropic_truncated_unsigned_thinking_after_text_is_an_error() {
    let [start, delta] = text_block("partial");
    let deltas = run(&[
        message_start(),
        start,
        delta,
        thinking_start(1),
        event(
            "content_block_delta",
            &json!({"index":1,"delta":{"type":"thinking_delta","thinking":"cut"}}),
        ),
        event(
            "message_delta",
            &json!({"delta":{"stop_reason":"max_tokens"},"usage":{"output_tokens":9}}),
        ),
    ]);
    assert_eq!(last_error(&deltas).message, "Messages output truncated");
}

#[test]
fn anthropic_message_delta_without_usage_keeps_output_tokens() {
    let [start, delta] = text_block("hi");
    let deltas = run(&[
        message_start(),
        start,
        delta,
        event(
            "message_delta",
            &json!({"delta":{"stop_reason":null},"usage":{"output_tokens":4}}),
        ),
        event(
            "message_delta",
            &json!({"delta":{"stop_reason":"end_turn"}}),
        ),
        event("message_stop", &json!({"type":"message_stop"})),
    ]);
    assert!(
        deltas
            .iter()
            .any(|d| matches!(d, StreamDelta::Usage(u) if u.output_tokens == 4))
    );
}

#[test]
fn anthropic_required_tool_choice_maps_to_any_and_empty_text_is_dropped() {
    let body = messages::body(&request(
        vec![user_hi()],
        vec![echo_tool()],
        Some("required"),
    ));
    assert_eq!(body["tool_choice"], json!({"type":"any"}));
    let empty = message(
        Role::User,
        &PartKind::InputText,
        vec![
            ContentBlock::Text {
                text: String::new(),
            },
            ContentBlock::Text { text: "hi".into() },
        ],
    );
    let body = messages::body(&request(vec![empty], vec![], Some("auto")));
    assert_eq!(
        body["messages"][0]["content"],
        json!([{"type":"text","text":"hi"}])
    );
}
