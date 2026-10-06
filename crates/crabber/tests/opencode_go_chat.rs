#![cfg(feature = "opencode-go")]
use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, Selection, ToolDefinition, ToolExecutor,
    core::{RunStatus, ToolInfo},
    extension::ExtensionError,
    providers::{HttpAdapter, HttpResolver, Protocol},
};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};

const LIMIT: Duration = Duration::from_secs(10);
type Captured = (BTreeMap<String, String>, Value);
async fn read_request(socket: &mut TcpStream) -> Captured {
    let mut raw = Vec::new();
    let mut buffer = [0u8; 4096];
    let end = loop {
        let n = socket.read(&mut buffer).await.unwrap();
        assert_ne!(n, 0, "request ended before headers");
        raw.extend_from_slice(&buffer[..n]);
        if let Some(end) = raw.windows(4).position(|b| b == b"\r\n\r\n") {
            break end + 4;
        }
        assert!(raw.len() < 65536);
    };
    let headers: BTreeMap<_, _> = std::str::from_utf8(&raw[..end])
        .unwrap()
        .lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .map(|(key, value)| (key.to_ascii_lowercase(), value.trim().to_owned()))
        .collect();
    let length: usize = headers["content-length"].parse().unwrap();
    assert!(length < 65536);
    while raw.len() < end + length {
        let n = socket.read(&mut buffer).await.unwrap();
        assert_ne!(n, 0, "request ended before body");
        raw.extend_from_slice(&buffer[..n]);
    }
    (
        headers,
        serde_json::from_slice(&raw[end..end + length]).unwrap(),
    )
}
async fn server(responses: Vec<String>) -> (String, tokio::task::JoinHandle<Vec<Captured>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        timeout(LIMIT, async move {
            let mut captured = Vec::new();
            for body in responses {
                let (mut socket, _) = listener.accept().await.unwrap();
                captured.push(read_request(&mut socket).await);
                let response = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                socket.write_all(response.as_bytes()).await.unwrap();
            }
            captured
        }).await.expect("loopback server timed out")
    });
    (url, task)
}
fn text() -> String {
    "data: {\"choices\":[{\"delta\":{\"content\":\"OK\"}}]}\n\ndata: [DONE]\n\n".into()
}
fn builder(url: &str) -> crabber::AgentBuilder {
    let adapter = HttpAdapter::opencode_go(Protocol::ChatCompletions)
        .with_base_url(url)
        .with_api_key("key")
        .try_with_user_agent_product("crabber-channels/0.1.0")
        .unwrap();
    Agent::builder()
        .memory()
        .provider(Arc::new(HttpResolver::new().with_adapter(adapter)))
        .config(
            AgentConfig::new(Selection {
                provider_id: "opencode-go".into(),
                model_id: "deepseek-v4-flash".into(),
            })
            .max_output_tokens(4096),
        )
}
#[tokio::test]
async fn facade_cap_and_product_token_reach_opencode_chat_wire() {
    let (url, task) = server(vec![text()]).await;
    let agent = builder(&url).build().unwrap();
    let result = timeout(LIMIT, async {
        agent
            .prompt(None, "hi")
            .await
            .unwrap()
            .done()
            .await
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(result.status, RunStatus::Completed);
    let captured = task.await.unwrap();
    assert_eq!(captured.len(), 1);
    let (headers, body) = &captured[0];
    assert!(body.get("tools").is_none());
    assert!(body.get("tool_choice").is_none());
    assert!(body.get("max_completion_tokens").is_none());
    assert_eq!(body["max_tokens"], 4096);
    assert_eq!(body["model"], "deepseek-v4-flash");
    assert_eq!(headers["authorization"], "Bearer key");
    assert_eq!(headers["x-opencode-session"], result.session_id.to_string());
    assert_eq!(headers["user-agent"], "crabber-channels/0.1.0 crabber/0.1");
}
struct Echo;
#[async_trait]
impl ToolExecutor for Echo {
    async fn execute(&self, value: Value) -> Result<Value, ExtensionError> {
        Ok(value)
    }
}
#[tokio::test]
async fn facade_cap_persists_on_tool_call_turn() {
    let call = "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"type\":\"function\",\"function\":{\"name\":\"echo\",\"arguments\":\"{}\"}}]}}]}\n\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n";
    let (url, task) = server(vec![call.into(), text()]).await;
    let tool = Arc::new(ToolDefinition {
        info: ToolInfo {
            name: "echo".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
            retry_safe: true,
            required_permissions: vec![],
        },
        executor: Arc::new(Echo),
    });
    let agent = builder(&url).tool(tool).build().unwrap();
    let result = timeout(LIMIT, async {
        agent
            .prompt(None, "hi")
            .await
            .unwrap()
            .done()
            .await
            .unwrap()
    })
    .await
    .unwrap();
    assert_eq!(result.status, RunStatus::Completed);
    let captured = task.await.unwrap();
    assert_eq!(captured.len(), 2);
    for (headers, body) in &captured {
        assert_eq!(body["max_tokens"], 4096);
        assert!(body.get("max_completion_tokens").is_none());
        assert_eq!(headers["user-agent"], "crabber-channels/0.1.0 crabber/0.1");
    }
    assert_eq!(captured[0].1["tools"].as_array().unwrap().len(), 1);
}
