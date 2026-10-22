#![cfg(feature = "opencode-go")]
use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, JsonToolExecutor, Selection, ToolDefinition,
    core::{RunStatus, ToolInfo},
    extension::ExtensionError,
    providers::{HttpAdapter, HttpResolver, Protocol},
};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::time::timeout;

#[path = "http_loopback/support.rs"]
mod loopback;
use loopback::{LIMIT, server};
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
        .behavior_fingerprint(crabber::core::InputFingerprint::new("b".repeat(64)).unwrap())
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
    let (url, task) = server(vec![text()], "/chat/completions").await;
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
impl JsonToolExecutor for Echo {
    async fn execute(&self, value: Value) -> Result<Value, ExtensionError> {
        Ok(value)
    }
}
#[tokio::test]
async fn facade_cap_persists_on_tool_call_turn() {
    let call = "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"call-1\",\"type\":\"function\",\"function\":{\"name\":\"echo\",\"arguments\":\"{}\"}}]}}]}\n\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n";
    let (url, task) = server(vec![call.into(), text()], "/chat/completions").await;
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
    let agent = builder(&url).shared_tool(tool).build().unwrap();
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
