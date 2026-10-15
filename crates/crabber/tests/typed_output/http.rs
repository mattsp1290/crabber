//! Public Agent -> native mixed tool -> actual HTTP Responses request.
use super::*;
use crabber::core::{InputFingerprint, ProviderBindingDescriptor, ProviderBindingLimits};
use crabber::providers::{HttpAdapter, HttpResolver, Protocol, VersionedResolver};
use std::fmt::Write as _;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{Duration, timeout},
};

const BOUND: Duration = Duration::from_secs(10);
async fn read_body(socket: &mut TcpStream) -> Value {
    let mut bytes = Vec::new();
    let mut buffer = [0; 4096];
    let header_end = loop {
        let read = socket.read(&mut buffer).await.unwrap();
        assert_ne!(read, 0);
        bytes.extend_from_slice(&buffer[..read]);
        if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            break end + 4;
        }
    };
    let length = std::str::from_utf8(&bytes[..header_end])
        .unwrap()
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .unwrap()
        .1
        .trim()
        .parse::<usize>()
        .unwrap();
    while bytes.len() < header_end + length {
        let read = socket.read(&mut buffer).await.unwrap();
        assert_ne!(read, 0);
        bytes.extend_from_slice(&buffer[..read]);
    }
    serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap()
}
fn first_turn() -> String {
    let events = [
        json!({"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","call_id":"wire-call","name":"mixed"}}),
        json!({"type":"response.function_call_arguments.done","output_index":0,"arguments":"{}"}),
        json!({"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","call_id":"wire-call","name":"mixed"}}),
        json!({"type":"response.completed","response":{}}),
    ];
    let mut stream = String::new();
    for event in events {
        write!(&mut stream, "data: {event}\n\n").unwrap();
    }
    stream
}
async fn serve(listener: TcpListener) -> Vec<Value> {
    let replies = [first_turn(), "data: {\"type\":\"response.output_text.delta\",\"delta\":\"done\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{}}\n\n".into()];
    let mut bodies = Vec::new();
    for reply in replies {
        let (mut socket, _) = listener.accept().await.unwrap();
        bodies.push(read_body(&mut socket).await);
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}", reply.len()).as_bytes()).await.unwrap();
    }
    bodies
}
#[tokio::test]
async fn public_responses_loop_sends_protected_mixed_tool_output() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(timeout(BOUND, serve(listener)));
    let selection = Selection {
        provider_id: "fixture".into(),
        model_id: "model".into(),
    };
    let adapter =
        HttpAdapter::custom("fixture", endpoint, Protocol::Responses).with_api_key("fixture-key");
    let resolver = VersionedResolver::new(
        Arc::new(HttpResolver::new().with_adapter(adapter)),
        vec![
            ProviderBindingDescriptor::new(
                "fixture",
                "model",
                "loopback-responses",
                "1",
                InputFingerprint::new("a".repeat(64)).unwrap(),
                ProviderBindingLimits {
                    context_tokens: Some(8192),
                    output_tokens: Some(4096),
                },
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let store = Arc::new(MemoryStore::new());
    let agent = Agent::builder()
        .behavior_version("typed-http-output.v1")
        .store(store.clone())
        .provider(Arc::new(resolver))
        .config(AgentConfig::new(selection))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "mixed".into(),
                description: "mixed".into(),
                parameters: json!({"type":"object"}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(MixedTool(false)),
        }))
        .shared_extension(Arc::new(Transforms(false)), Scope::Global)
        .build()
        .unwrap();
    let handle = timeout(BOUND, agent.prompt(None, "show image"))
        .await
        .unwrap()
        .unwrap();
    let session = handle.session_id().clone();
    timeout(BOUND, handle.done()).await.unwrap().unwrap();
    let bodies = server.await.unwrap().unwrap();
    assert_eq!(bodies.len(), 2);
    let result = bodies[1]["input"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["type"] == "function_call_output")
        .unwrap();
    assert_eq!(result["call_id"], "wire-call");
    assert_eq!(
        result["output"],
        json!([
            {"type":"input_text","text":"public caption"},
            {"type":"input_image","detail":"auto","file_id":"opaque-image"},
            {"type":"input_text","text":"trailing text"},
        ])
    );
    let messages = store.list_all_messages(&session).await.unwrap();
    assert!(messages.iter().flat_map(|message| &message.parts).any(|part| matches!(&part.content, ContentBlock::ToolResult { call_id, content, is_error: false } if call_id.0 == "wire-call" && content == &ToolOutput::new(super::content("public caption"), false).unwrap().to_content_blocks())));
    for artifact in [
        bodies[1].to_string(),
        serde_json::to_string(&messages).unwrap(),
    ] {
        assert!(!artifact.contains("PRIVATE_CAPTION"));
    }
}
