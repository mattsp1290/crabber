//! Built-in Anthropic adapter: headers, classification, body, images, catalog.
use super::*;

fn loopback_response(status: &str, content_type: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

const MESSAGES_TEXT_SSE: &str = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":1}}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";

#[tokio::test]
async fn anthropic_headers_use_x_api_key_and_version() {
    let adapter = HttpAdapter::anthropic().with_api_key("test-key");
    let headers = adapter.headers(&request("anthropic")).await.unwrap();
    assert_eq!(headers["x-api-key"], "test-key");
    assert_eq!(headers["anthropic-version"], "2023-06-01");
    assert_eq!(headers["user-agent"], "crabber/0.1");
    assert!(!headers.contains_key("authorization"));
    assert!(!headers.contains_key("x-opencode-session"));
    let headers = adapter
        .try_with_user_agent_product("crabber-channels/0.1.0")
        .unwrap()
        .headers(&request("anthropic"))
        .await
        .unwrap();
    assert_eq!(headers["user-agent"], "crabber-channels/0.1.0 crabber/0.1");
    assert_eq!(headers.get_all("user-agent").iter().count(), 1);
}

#[tokio::test]
async fn anthropic_missing_key_fails_before_network() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let mut adapter = HttpAdapter::anthropic()
        .with_base_url(format!("http://{}", listener.local_addr().unwrap()));
    // Never mutate the process environment; point at a variable that is unset.
    adapter.key_env = Some("CRABBER_TEST_ABSENT_ANTHROPIC_KEY");
    let Err(error) = adapter.stream(request("anthropic")).await else {
        panic!("missing key should fail");
    };
    assert_eq!(error.kind, ProviderErrorKind::Auth);
    assert_eq!(
        listener.accept().err().map(|e| e.kind()),
        Some(std::io::ErrorKind::WouldBlock)
    );
}

#[tokio::test]
async fn anthropic_rejection_redacts_x_api_key() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = accept_loopback_request(&listener).await;
        let response = loopback_response(
            "500 Server Error",
            "text/plain",
            "x-secret-key anthropic-error",
        );
        write_loopback_response(&mut socket, response.as_bytes()).await;
    });
    let Err(error) = HttpAdapter::anthropic()
        .with_api_key("x-secret-key")
        .with_base_url(&base_url)
        .stream(request("anthropic"))
        .await
    else {
        panic!("500 should fail");
    };
    assert_eq!(error.kind, ProviderErrorKind::Server);
    assert!(error.retryable);
    assert!(!error.message.contains("x-secret-key"));
    assert!(error.message.ends_with("[REDACTED] anthropic-error"));
    timeout(LOOPBACK_TIMEOUT, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn anthropic_overflow_and_billing_statuses_classify() {
    let cases = [
        (
            "413 Payload Too Large",
            "too large",
            ProviderErrorKind::ContextOverflow,
        ),
        (
            "400 Bad Request",
            r#"{"error":{"message":"Prompt is too long: 300000 tokens"}}"#,
            ProviderErrorKind::ContextOverflow,
        ),
        (
            "400 Bad Request",
            "input length and max_tokens exceed context limit",
            ProviderErrorKind::ContextOverflow,
        ),
        ("400 Bad Request", "other", ProviderErrorKind::Invalid),
        (
            "400 Bad Request",
            "the context field exceeds 10 items",
            ProviderErrorKind::Invalid,
        ),
        (
            "402 Payment Required",
            "billing",
            ProviderErrorKind::Invalid,
        ),
    ];
    for (status, body, expected) in cases {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = accept_loopback_request(&listener).await;
            let response = loopback_response(status, "application/json", body);
            write_loopback_response(&mut socket, response.as_bytes()).await;
        });
        let Err(error) = HttpAdapter::anthropic()
            .with_api_key("k")
            .with_base_url(&base_url)
            .stream(request("anthropic"))
            .await
        else {
            panic!("{status} should fail");
        };
        assert_eq!(error.kind, expected, "{status} {body}");
        assert!(!error.retryable, "{status} {body}");
        timeout(LOOPBACK_TIMEOUT, server).await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn anthropic_tool_free_body_omits_tools_and_tool_choice() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, request, body) = accept_loopback_request_with_body(&listener).await;
        assert!(request.starts_with("POST /messages HTTP/1.1"));
        let response = loopback_response("200 OK", "text/event-stream", MESSAGES_TEXT_SSE);
        write_loopback_response(&mut socket, response.as_bytes()).await;
        serde_json::from_slice::<Value>(&body).unwrap()
    });
    let mut req = request("anthropic");
    req.tool_choice = Some("auto".into());
    let adapter = HttpAdapter::anthropic()
        .with_api_key("k")
        .with_base_url(&base_url);
    drop(adapter.stream(req).await.unwrap());
    let body = timeout(LOOPBACK_TIMEOUT, server).await.unwrap().unwrap();
    assert!(body.get("tools").is_none());
    assert!(body.get("tool_choice").is_none());
    assert_eq!(body["stream"], true);
    assert_eq!(body["max_tokens"], 4096);
    assert_eq!(body["model"], "model");
}

#[cfg(feature = "opencode-go")]
#[tokio::test]
async fn opencode_messages_route_shares_codec_but_not_anthropic_classifier() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _, body) = accept_loopback_request_with_body(&listener).await;
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert!(body.get("tools").is_none());
        let response = loopback_response("413 Payload Too Large", "text/plain", "too large");
        write_loopback_response(&mut socket, response.as_bytes()).await;
    });
    let Err(error) = HttpAdapter::opencode_go(Protocol::Messages)
        .with_api_key("k")
        .with_base_url(&base_url)
        .stream(request("opencode-go"))
        .await
    else {
        panic!("413 should fail");
    };
    assert_eq!(error.kind, ProviderErrorKind::Invalid);
    timeout(LOOPBACK_TIMEOUT, server).await.unwrap().unwrap();
}

#[cfg(feature = "custom-http")]
#[tokio::test]
async fn custom_messages_route_shares_codec_but_not_anthropic_classifier() {
    use futures::StreamExt;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let refusal = include_str!("../../../testdata/anthropic/refusal.sse");
    let server = tokio::spawn(async move {
        for (status, content_type, body) in [
            ("413 Payload Too Large", "text/plain", "too large"),
            ("200 OK", "text/event-stream", refusal),
        ] {
            let (mut socket, _) = accept_loopback_request(&listener).await;
            let response = loopback_response(status, content_type, body);
            write_loopback_response(&mut socket, response.as_bytes()).await;
        }
    });
    let adapter = HttpAdapter::custom("anthropic", &base_url, Protocol::Messages).with_api_key("k");
    let Err(error) = adapter.stream(request("anthropic")).await else {
        panic!("413 should fail");
    };
    assert_eq!(error.kind, ProviderErrorKind::Invalid);
    let deltas: Vec<_> = adapter
        .stream(request("anthropic"))
        .await
        .unwrap()
        .collect()
        .await;
    assert!(matches!(
        deltas.last(),
        Some(crate::StreamDelta::Error(error)) if error.message == "Messages refusal"
    ));
    timeout(LOOPBACK_TIMEOUT, server).await.unwrap().unwrap();
}

#[tokio::test]
async fn anthropic_user_image_reaches_wire_without_custom_http() {
    use crabber_core::{
        ContentBlock, MediaKind, MediaSource, Message, MessageId, Part, PartId, PartKind, Role,
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _, body) = accept_loopback_request_with_body(&listener).await;
        let response = loopback_response("200 OK", "text/event-stream", MESSAGES_TEXT_SSE);
        write_loopback_response(&mut socket, response.as_bytes()).await;
        serde_json::from_slice::<Value>(&body).unwrap()
    });
    let id = MessageId::new();
    let mut req = request("anthropic");
    req.messages = vec![Message {
        id: id.clone(),
        session_id: SessionId::from("session-1"),
        run_id: None,
        role: Role::User,
        parent_id: None,
        parts: [
            ContentBlock::MediaContent {
                kind: MediaKind::Image,
                mime_type: "image/png".into(),
                source: MediaSource::Data {
                    base64: "AQ==".into(),
                },
            },
            ContentBlock::Text {
                text: "describe".into(),
            },
        ]
        .into_iter()
        .zip(0..)
        .map(|(content, ordinal)| Part {
            id: PartId::new(),
            message_id: id.clone(),
            ordinal,
            kind: PartKind::Media,
            content,
        })
        .collect(),
        created_at: std::time::UNIX_EPOCH.into(),
    }];
    let adapter = HttpAdapter::anthropic()
        .with_api_key("k")
        .with_base_url(&base_url);
    assert_eq!(adapter.input_support().media.len(), 3);
    drop(adapter.stream(req).await.unwrap());
    let body = timeout(LOOPBACK_TIMEOUT, server).await.unwrap().unwrap();
    let content = &body["messages"][0]["content"];
    assert_eq!(content[0]["type"], "image");
    assert_eq!(content[0]["source"]["type"], "base64");
    assert_eq!(content[0]["source"]["media_type"], "image/png");
    assert_eq!(
        content[1],
        serde_json::json!({"type":"text","text":"describe"})
    );
}

#[tokio::test]
async fn anthropic_models_request_uses_x_api_key_version_and_limit() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, request) = accept_loopback_request(&listener).await;
        assert!(request.starts_with("GET /models?limit=1000 HTTP/1.1"));
        let lower = request.to_ascii_lowercase();
        assert!(lower.contains("x-api-key: k\r\n"));
        assert!(lower.contains("anthropic-version: 2023-06-01\r\n"));
        assert!(lower.contains("user-agent: crabber-channels/0.1.0 crabber/0.1\r\n"));
        assert!(!lower.contains("authorization:"));
        let body = r#"{"data":[{"id":"claude-fixture","max_input_tokens":200000},{"id":"no-limit","max_input_tokens":null}],"has_more":false}"#;
        let response = loopback_response("200 OK", "application/json", body);
        write_loopback_response(&mut socket, response.as_bytes()).await;
    });
    let models = HttpAdapter::anthropic()
        .with_api_key("k")
        .with_base_url(&base_url)
        .try_with_user_agent_product("crabber-channels/0.1.0")
        .unwrap()
        .models()
        .await
        .unwrap();
    timeout(LOOPBACK_TIMEOUT, server).await.unwrap().unwrap();
    assert_eq!(
        models,
        [
            ModelDescriptor {
                provider_id: "anthropic".into(),
                id: "claude-fixture".into(),
                context_limit: 200_000,
            },
            ModelDescriptor {
                provider_id: "anthropic".into(),
                id: "no-limit".into(),
                context_limit: 0,
            },
        ]
    );
}

#[tokio::test]
async fn anthropic_models_error_redacts_x_api_key() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = accept_loopback_request(&listener).await;
        let response = loopback_response("500 Server Error", "text/plain", "model-secret");
        write_loopback_response(&mut socket, response.as_bytes()).await;
    });
    let error = HttpAdapter::anthropic()
        .with_api_key("model-secret")
        .with_base_url(&base_url)
        .models()
        .await
        .expect_err("model endpoint 500 should fail");
    assert_eq!(error.kind, ProviderErrorKind::Server);
    assert!(error.retryable);
    assert!(!error.message.contains("model-secret"));
    assert!(error.message.ends_with("[REDACTED]"));
    timeout(LOOPBACK_TIMEOUT, server).await.unwrap().unwrap();
}

#[cfg(feature = "custom-http")]
#[tokio::test]
async fn anthropic_catalog_is_builtin_only() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let custom = HttpAdapter::custom("anthropic", &base_url, Protocol::Messages).with_api_key("k");
    assert_eq!(
        custom.models().await.unwrap(),
        Vec::<ModelDescriptor>::new()
    );
    assert_eq!(
        listener.accept().err().map(|e| e.kind()),
        Some(std::io::ErrorKind::WouldBlock)
    );
    let real = HttpAdapter::anthropic().with_api_key("k");
    assert!(real.catalog_request().unwrap().is_some());
}

/// Streams one SSE body through the built-in adapter and collects every delta.
async fn stream_fixture(body: &'static str) -> Vec<crate::StreamDelta> {
    use futures::StreamExt;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = accept_loopback_request(&listener).await;
        let response = loopback_response("200 OK", "text/event-stream", body);
        write_loopback_response(&mut socket, response.as_bytes()).await;
    });
    let deltas = HttpAdapter::anthropic()
        .with_api_key("k")
        .with_base_url(&base_url)
        .stream(request("anthropic"))
        .await
        .unwrap()
        .collect()
        .await;
    timeout(LOOPBACK_TIMEOUT, server).await.unwrap().unwrap();
    deltas
}
fn terminal_error(deltas: &[crate::StreamDelta]) -> &ProviderError {
    match deltas.last() {
        Some(crate::StreamDelta::Error(error)) => error,
        other => panic!("expected terminal error, got {other:?}"),
    }
}

#[tokio::test]
async fn anthropic_stream_error_retryability_follows_published_output() {
    let before = stream_fixture(include_str!("../../../testdata/anthropic/overloaded.sse")).await;
    assert!(terminal_error(&before).retryable);

    // An empty text block publishes nothing, so a retry duplicates nothing.
    let empty_block: &'static str = concat!(
        "event: content_block_start\ndata: {\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
        "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"x\"}}\n\n",
    );
    assert!(terminal_error(&stream_fixture(empty_block).await).retryable);

    let after_text = stream_fixture(include_str!(
        "../../../testdata/anthropic/text_then_overloaded.sse"
    ))
    .await;
    let error = terminal_error(&after_text);
    assert_eq!(error.kind, ProviderErrorKind::Server);
    assert!(!error.retryable);

    let after_tool: &'static str = concat!(
        "event: content_block_start\ndata: {\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"echo\"}}\n\n",
        "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\",\"message\":\"x\"}}\n\n",
    );
    let error = terminal_error(&stream_fixture(after_tool).await).clone();
    assert_eq!(error.kind, ProviderErrorKind::RateLimited);
    assert!(!error.retryable);
}

#[tokio::test]
async fn anthropic_stream_error_after_message_stop_is_not_retryable() {
    let body: &'static str = concat!(
        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":1}}}\n\n",
        "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
        "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"x\"}}\n\n",
    );
    let deltas = stream_fixture(body).await;
    let error = terminal_error(&deltas);
    assert_eq!(error.kind, ProviderErrorKind::Server);
    assert!(!error.retryable);
    assert!(
        !deltas
            .iter()
            .any(|d| matches!(d, crate::StreamDelta::Completed))
    );
}
