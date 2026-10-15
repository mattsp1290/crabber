use super::*;
use crabber_core::{
    ContentBlock, MediaKind, MediaSource, Message, MessageId, Part, PartId, PartKind, Role,
    ToolCallId,
};
use serde_json::{Value, json};

async fn capture(protocol: Protocol, messages: Vec<Message>) -> Value {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = timeout(LOOPBACK_TIMEOUT, listener.accept())
            .await
            .unwrap()
            .unwrap();
        let (_, body) = read_request(&mut socket).await;
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 14\r\nConnection: close\r\n\r\ndata: [DONE]\n\n").await.unwrap();
        serde_json::from_slice(&body).unwrap()
    });
    let adapter =
        HttpAdapter::custom("media-fixture", endpoint, protocol).with_api_key("fixture-media");
    let mut request = request("media-fixture");
    request.messages = messages;
    drop(adapter.stream(request).await.unwrap());
    timeout(LOOPBACK_TIMEOUT, server).await.unwrap().unwrap()
}
fn mixed(source: MediaSource) -> Vec<ContentBlock> {
    vec![
        ContentBlock::Text {
            text: "caption".into(),
        },
        ContentBlock::MediaContent {
            kind: MediaKind::Image,
            mime_type: "image/png".into(),
            source,
        },
        ContentBlock::Text {
            text: "tail".into(),
        },
    ]
}
fn message(role: Role, blocks: Vec<ContentBlock>) -> Message {
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
                kind: PartKind::Media,
                content,
            })
            .collect(),
        created_at: std::time::UNIX_EPOCH.into(),
    }
}
#[tokio::test]
async fn responses_sends_ordered_mixed_input_and_tool_output_without_fetching_media() {
    for source in [
        MediaSource::Data {
            base64: "AQ==".into(),
        },
        MediaSource::Url {
            url: "https://example.invalid/image".into(),
        },
        MediaSource::File {
            file_id: "opaque-file".into(),
        },
    ] {
        let body = capture(
            Protocol::Responses,
            vec![
                message(Role::User, mixed(source.clone())),
                message(
                    Role::Tool,
                    vec![ContentBlock::ToolResult {
                        call_id: ToolCallId::from("call"),
                        content: mixed(source.clone()),
                        is_error: false,
                    }],
                ),
            ],
        )
        .await;
        assert_eq!(body["input"][0]["role"], "user");
        assert_eq!(body["input"][1]["type"], "function_call_output");
        assert_eq!(body["input"][1]["call_id"], "call");
        for content in [&body["input"][0]["content"], &body["input"][1]["output"]] {
            assert_eq!(content.as_array().unwrap().len(), 3);
            assert_eq!(content[0], json!({"type":"input_text","text":"caption"}));
            assert_eq!(content[2], json!({"type":"input_text","text":"tail"}));
            let expected = match &source {
                MediaSource::Data { base64 } => {
                    json!({"type":"input_image","detail":"auto","image_url":format!("data:image/png;base64,{base64}")})
                }
                MediaSource::Url { url } => {
                    json!({"type":"input_image","detail":"auto","image_url":url})
                }
                MediaSource::File { file_id } => {
                    json!({"type":"input_image","detail":"auto","file_id":file_id})
                }
            };
            assert_eq!(content[1], expected);
        }
    }
}
#[tokio::test]
async fn chat_preserves_user_image_order_and_system_history_role() {
    for source in [
        MediaSource::Data {
            base64: "AQ==".into(),
        },
        MediaSource::Url {
            url: "https://example.invalid/image".into(),
        },
    ] {
        let body = capture(
            Protocol::ChatCompletions,
            vec![
                message(
                    Role::System,
                    vec![ContentBlock::Text {
                        text: "host authorized policy".into(),
                    }],
                ),
                message(Role::User, mixed(source.clone())),
            ],
        )
        .await;
        assert_eq!(
            body["messages"][0],
            json!({"role":"system","content":"host authorized policy"})
        );
        let content = &body["messages"][1]["content"];
        assert_eq!(content.as_array().unwrap().len(), 3);
        assert_eq!(content[0], json!({"type":"text","text":"caption"}));
        assert_eq!(content[2], json!({"type":"text","text":"tail"}));
        let url = match &source {
            MediaSource::Data { base64 } => format!("data:image/png;base64,{base64}"),
            MediaSource::Url { url } => url.clone(),
            MediaSource::File { .. } => unreachable!(),
        };
        assert_eq!(
            content[1],
            json!({"type":"image_url","image_url":{"url":url}})
        );
    }
}
