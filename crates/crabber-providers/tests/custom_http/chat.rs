use super::*;

#[tokio::test]
async fn custom_chat_wire_body_omits_empty_tools_and_uses_completion_tokens() {
    let completed = b"data: [DONE]\n\n";
    let server = RawServer::start(raw_response("200 OK", completed.len(), completed)).await;
    let mut request = request("custom");
    request.tool_choice = Some("required".into());
    let items: Vec<_> = HttpAdapter::custom("custom", &server.url, Protocol::ChatCompletions)
        .with_api_key("key")
        .with_chat_token_field(ChatTokenField::MaxCompletionTokens)
        .stream(request)
        .await
        .unwrap()
        .collect()
        .await;
    assert_eq!(
        items
            .iter()
            .filter(|item| matches!(item, StreamDelta::Completed))
            .count(),
        1
    );
    let body: serde_json::Value = serde_json::from_slice(&server.body().await).unwrap();
    assert!(body.get("tools").is_none());
    assert!(body.get("tool_choice").is_none());
    assert!(body.get("max_tokens").is_none());
    assert_eq!(body["max_completion_tokens"], 8);
}

#[cfg(feature = "opencode-go")]
#[tokio::test]
async fn built_in_opencode_chat_wire_body_is_tool_free_and_caps_max_tokens() {
    let completed = b"data: [DONE]\n\n";
    let server = RawServer::start(raw_response("200 OK", completed.len(), completed)).await;
    let adapter = HttpAdapter::opencode_go(Protocol::ChatCompletions)
        .with_base_url(&server.url)
        .with_api_key("key");
    let mut input = request("opencode-go");
    input.max_tokens = Some(4096);
    input.tool_choice = Some("required".into());
    let deltas = adapter
        .stream(input)
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
    assert_eq!(deltas.len(), 1);
    assert!(matches!(deltas[0], StreamDelta::Completed));
    let (headers, body) = server.headers_and_body().await;
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(body.get("tools").is_none());
    assert!(body.get("tool_choice").is_none());
    assert!(body.get("max_completion_tokens").is_none());
    assert_eq!(body["max_tokens"], 4096);
    assert_eq!(body["model"], "model");
    assert_eq!(body["stream"], true);
    assert_eq!(values(&headers, "authorization"), ["Bearer key"]);
    assert_eq!(values(&headers, "x-opencode-session"), ["session"]);
    assert_eq!(values(&headers, "user-agent"), ["crabber/0.1"]);
    assert_eq!(values(&headers, "x-api-key"), Vec::<String>::new());
}

#[cfg(feature = "opencode-go")]
#[tokio::test]
async fn built_in_opencode_chat_wire_carries_host_product_token() {
    let completed = b"data: [DONE]\n\n";
    let server = RawServer::start(raw_response("200 OK", completed.len(), completed)).await;
    let adapter = HttpAdapter::opencode_go(Protocol::ChatCompletions)
        .with_base_url(&server.url)
        .with_api_key("key")
        .try_with_user_agent_product("crabber-channels/0.1.0")
        .unwrap();
    let deltas = adapter
        .stream(request("opencode-go"))
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
    assert!(matches!(deltas.as_slice(), [StreamDelta::Completed]));
    let (headers, _) = server.headers_and_body().await;
    assert_eq!(
        values(&headers, "user-agent"),
        ["crabber-channels/0.1.0 crabber/0.1"]
    );
}

#[tokio::test]
async fn custom_adapter_carries_host_product_token() {
    let completed = b"data: [DONE]\n\n";
    let server = RawServer::start(raw_response("200 OK", completed.len(), completed)).await;
    let adapter = HttpAdapter::custom("custom", &server.url, Protocol::ChatCompletions)
        .with_api_key("key")
        .try_with_user_agent_product("host/1")
        .unwrap()
        .with_request_header_hook(Arc::new(SequenceHook {
            values: Mutex::new(VecDeque::from([Ok(HeaderMap::from_iter([(
                HeaderName::from_static("x-host"),
                HeaderValue::from_static("harmless"),
            )]))])),
            calls: AtomicUsize::new(0),
        }));
    let deltas = adapter
        .stream(request("custom"))
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
    assert!(matches!(deltas.as_slice(), [StreamDelta::Completed]));
    let (headers, _) = server.headers_and_body().await;
    assert_eq!(values(&headers, "user-agent"), ["host/1 crabber/0.1"]);
    assert_eq!(values(&headers, "x-host"), ["harmless"]);
}
