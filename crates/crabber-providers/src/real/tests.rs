#[cfg(feature = "custom-http")]
use super::errors::{
    ERROR_EXCERPT_MAX_BYTES, ERROR_MESSAGE_PREFIX_BYTES, sanitize_excerpt, status_classification,
};
use super::*;
use crate::ProviderErrorKind;
#[test]
fn user_agent_product_validation_accepts_tokens_and_rejects_others() {
    for product in [
        "crabber-channels/0.1.0",
        "host",
        "a.b_c~1/2+3",
        &"a".repeat(64),
        &format!("{}/{}", "a".repeat(64), "v".repeat(64)),
    ] {
        assert!(user_agent_product_is_valid(product), "{product}");
    }
    for product in [
        "",
        "a b",
        "a/b/c",
        "/1.0",
        "name/",
        "a(b)/1",
        "a\u{7f}",
        "a\r\n",
        "ünïcode/1",
        &"a".repeat(65),
        &format!("a/{}", "v".repeat(65)),
        "crabber/9.9",
        "CRABBER",
    ] {
        assert!(!user_agent_product_is_valid(product));
        #[cfg(feature = "custom-http")]
        {
            let adapter =
                HttpAdapter::custom("test", "http://example.invalid", Protocol::ChatCompletions);
            let Err(error) = adapter.try_with_user_agent_product(product) else {
                panic!("invalid product accepted");
            };
            assert_eq!(error.kind, ProviderErrorKind::Invalid);
            assert!(!error.retryable);
            assert_eq!(error.message, "invalid provider setting");
        }
    }
}

#[cfg(feature = "opencode-go")]
#[tokio::test]
async fn opencode_headers_carry_host_product_token() {
    let adapter = HttpAdapter::opencode_go(Protocol::ChatCompletions).with_api_key("k");
    assert_eq!(
        adapter.headers(&request("opencode-go")).await.unwrap()["user-agent"],
        "crabber/0.1"
    );
    let adapter = adapter
        .try_with_user_agent_product("crabber-channels/0.1.0")
        .unwrap();
    let headers = adapter.headers(&request("opencode-go")).await.unwrap();
    assert_eq!(headers["user-agent"], "crabber-channels/0.1.0 crabber/0.1");
    assert_eq!(headers.get_all("user-agent").iter().count(), 1);
    let adapter = adapter.try_with_user_agent_product("other/2").unwrap();
    assert_eq!(
        adapter.headers(&request("opencode-go")).await.unwrap()["user-agent"],
        "other/2 crabber/0.1"
    );
}

#[cfg(feature = "opencode-go")]
#[tokio::test]
async fn opencode_models_request_sends_user_agent() {
    for product in [None, Some("crabber-channels/0.1.0")] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let expected =
            product.map_or_else(|| "crabber/0.1".to_owned(), |p| format!("{p} crabber/0.1"));
        let server = tokio::spawn(async move {
            let (mut socket, request) = accept_loopback_request(&listener).await;
            let headers: Vec<_> = request
                .lines()
                .filter_map(|line| line.split_once(':'))
                .filter(|(name, _)| name.eq_ignore_ascii_case("user-agent"))
                .map(|(_, value)| value.trim())
                .collect();
            assert_eq!(headers, [expected.as_str()]);
            let body = r#"{"data":[{"id":"m"}]}"#;
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            write_loopback_response(&mut socket, response.as_bytes()).await;
        });
        let mut adapter = HttpAdapter::opencode_go(Protocol::ChatCompletions)
            .with_base_url(base_url)
            .with_api_key("k");
        if let Some(product) = product {
            adapter = adapter.try_with_user_agent_product(product).unwrap();
        }
        assert_eq!(adapter.models().await.unwrap().len(), 1);
        timeout(LOOPBACK_TIMEOUT, server).await.unwrap().unwrap();
    }
}

#[cfg(any(feature = "custom-http", feature = "codex", feature = "opencode-go"))]
use crate::RequestIdentity;
#[cfg(any(feature = "custom-http", feature = "codex", feature = "opencode-go"))]
use crabber_core::{RunId, SessionId, TurnId};
#[cfg(any(feature = "codex", feature = "opencode-go"))]
use std::time::Duration;
#[cfg(any(feature = "codex", feature = "opencode-go"))]
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};

#[cfg(any(feature = "codex", feature = "opencode-go"))]
const LOOPBACK_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(any(feature = "codex", feature = "opencode-go"))]
async fn accept_loopback_request(listener: &TcpListener) -> (TcpStream, String) {
    let (mut socket, _) = timeout(LOOPBACK_TIMEOUT, listener.accept())
        .await
        .expect("timed out waiting for provider request")
        .expect("failed to accept provider request");
    let mut raw = Vec::new();
    let mut buffer = [0_u8; 4096];
    let header_end = loop {
        let n = timeout(LOOPBACK_TIMEOUT, socket.read(&mut buffer))
            .await
            .expect("timed out reading provider request")
            .expect("failed to read provider request");
        assert_ne!(n, 0, "request ended before its headers");
        raw.extend_from_slice(&buffer[..n]);
        if let Some(position) = raw.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
    };
    let headers = String::from_utf8(raw[..header_end].to_vec())
        .expect("provider request headers were not UTF-8");
    let content_length = headers
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
        .map_or(0, |(_, value)| {
            value
                .trim()
                .parse::<usize>()
                .expect("invalid request Content-Length")
        });
    let request_end = header_end
        .checked_add(content_length)
        .expect("request length overflowed");
    while raw.len() < request_end {
        let n = timeout(LOOPBACK_TIMEOUT, socket.read(&mut buffer))
            .await
            .expect("timed out reading provider request body")
            .expect("failed to read provider request body");
        assert_ne!(n, 0, "request ended before its declared body");
        raw.extend_from_slice(&buffer[..n]);
    }
    (socket, headers)
}

#[cfg(any(feature = "codex", feature = "opencode-go"))]
async fn write_loopback_response(socket: &mut TcpStream, response: &[u8]) {
    timeout(LOOPBACK_TIMEOUT, socket.write_all(response))
        .await
        .expect("timed out writing provider response")
        .expect("failed to write provider response");
}
#[cfg(feature = "custom-http")]
#[test]
fn error_excerpts_are_lossy_utf8_redacted_and_byte_bounded() {
    let below = sanitize_excerpt(b"short error", &[], false);
    assert_eq!(below, "short error");

    let above = sanitize_excerpt(&vec![b'x'; ERROR_EXCERPT_MAX_BYTES + 100], &[], true);
    assert_eq!(above.len(), ERROR_EXCERPT_MAX_BYTES);

    let invalid = sanitize_excerpt(&vec![0xff; ERROR_EXCERPT_MAX_BYTES], &[], true);
    assert!(invalid.len() <= ERROR_EXCERPT_MAX_BYTES);
    assert!(invalid.is_char_boundary(invalid.len()));
    assert!(invalid.contains('\u{fffd}'));

    let mut split = vec![b'a'; ERROR_EXCERPT_MAX_BYTES - 1];
    split.push(0xf0);
    let split = sanitize_excerpt(&split, &[], true);
    assert!(split.len() <= ERROR_EXCERPT_MAX_BYTES);
    assert!(split.is_char_boundary(split.len()));

    let expanded = sanitize_excerpt(
        &vec![b'k'; ERROR_EXCERPT_MAX_BYTES],
        &["k".to_owned()],
        true,
    );
    assert!(expanded.len() <= ERROR_EXCERPT_MAX_BYTES);
    assert!(!expanded.contains('k'));
    assert!(
        ERROR_MESSAGE_PREFIX_BYTES + expanded.len()
            <= ERROR_MESSAGE_PREFIX_BYTES + ERROR_EXCERPT_MAX_BYTES
    );

    let credential = "sec\"ret\\token";
    let serialized = serde_json::to_string(credential).unwrap();
    let redacted = sanitize_excerpt(serialized.as_bytes(), &[credential.to_owned()], false);
    assert_eq!(redacted, "[REDACTED]");
    assert!(!redacted.contains(credential));
    assert!(!redacted.contains(&serialized));
}

#[cfg(feature = "custom-http")]
#[test]
fn default_status_taxonomy_is_stable() {
    assert_eq!(
        status_classification(reqwest::StatusCode::UNAUTHORIZED),
        (ProviderErrorKind::Auth, false)
    );
    assert_eq!(
        status_classification(reqwest::StatusCode::FORBIDDEN),
        (ProviderErrorKind::Auth, false)
    );
    assert_eq!(
        status_classification(reqwest::StatusCode::TOO_MANY_REQUESTS),
        (ProviderErrorKind::RateLimited, true)
    );
    assert_eq!(
        status_classification(reqwest::StatusCode::INTERNAL_SERVER_ERROR),
        (ProviderErrorKind::Server, true)
    );
    assert_eq!(
        status_classification(reqwest::StatusCode::BAD_REQUEST),
        (ProviderErrorKind::Invalid, false)
    );
}

#[cfg(any(feature = "custom-http", feature = "codex", feature = "opencode-go"))]
fn request(provider: &str) -> ModelRequest {
    ModelRequest {
        identity: RequestIdentity {
            session_id: SessionId::from("session-1"),
            run_id: RunId::from("run-1"),
            turn_id: TurnId::from("turn-1"),
        },
        selection: Selection {
            provider_id: provider.into(),
            model_id: "model".into(),
        },
        system: None,
        messages: vec![],
        tools: vec![],
        temperature: None,
        max_tokens: None,
        tool_choice: None,
    }
}
#[cfg(feature = "codex")]
#[test]
fn real_codex_selects_privileged_responses_mode() {
    use crabber_auth::MemoryCredentialStore;

    let adapter = HttpAdapter::codex(Arc::new(MemoryCredentialStore::default()));
    assert!(adapter.uses_codex_responses_mode());
}
#[cfg(feature = "opencode-go")]
#[test]
fn real_opencode_selects_special_models_and_session_behavior() {
    let adapter = HttpAdapter::opencode_go(Protocol::Responses);
    assert!(adapter.uses_opencode_models());
    assert!(adapter.uses_opencode_session());
}
#[cfg(all(feature = "custom-http", feature = "codex"))]
#[test]
fn real_and_custom_codex_ids_have_distinct_privilege() {
    use crabber_auth::MemoryCredentialStore;

    let real = HttpAdapter::codex(Arc::new(MemoryCredentialStore::default()));
    let impostor = HttpAdapter::custom("codex", "https://example.test", Protocol::Responses);
    assert!(real.uses_codex_responses_mode());
    assert!(!impostor.inner.uses_codex_responses_mode());
}
#[cfg(all(feature = "custom-http", feature = "opencode-go"))]
#[test]
fn real_and_custom_opencode_ids_have_distinct_privilege() {
    let real = HttpAdapter::opencode_go(Protocol::Responses);
    let impostor = HttpAdapter::custom("opencode-go", "https://example.test", Protocol::Responses);
    assert!(real.uses_opencode_models());
    assert!(!impostor.inner.uses_opencode_models());
    assert!(real.uses_opencode_session());
    assert!(!impostor.inner.uses_opencode_session());
}
#[cfg(feature = "custom-http")]
#[test]
fn static_headers_accept_unprotected_names() {
    let mut headers = HeaderMap::new();
    headers.insert("x-request-label", HeaderValue::from_static("safe-value"));

    let adapter = HttpAdapter::custom("custom", "https://example.test", Protocol::Responses)
        .try_with_static_headers(headers)
        .expect("unprotected static header should be accepted");

    assert_eq!(
        adapter.inner.static_headers["x-request-label"],
        "safe-value"
    );
}
#[cfg(feature = "custom-http")]
#[test]
fn static_headers_reject_protected_names_without_disclosure() {
    for name in [
        "authorization",
        "x-api-key",
        "content-type",
        "user-agent",
        "anthropic-version",
    ] {
        let secret = format!("secret-for-{name}");
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_str(&secret).unwrap(),
        );

        let error = HttpAdapter::custom(
            "custom",
            "https://credential@example.test",
            Protocol::Responses,
        )
        .try_with_static_headers(headers)
        .err()
        .unwrap_or_else(|| panic!("protected header {name} should be rejected"));

        assert_eq!(error.kind, ProviderErrorKind::Invalid);
        assert!(!error.retryable);
        let rendered = error.to_string();
        assert!(!rendered.contains(&secret));
        assert!(!rendered.contains("credential"));
    }
}
#[cfg(feature = "custom-http")]
#[test]
fn custom_builder_precedence_and_defaults_are_stable() {
    struct Source;
    #[async_trait]
    impl CredentialSource for Source {
        async fn credential(&self, _request: &ModelRequest) -> Result<String, ProviderError> {
            Ok("dynamic".into())
        }

        async fn invalidate(&self, _stale: &str) {}
    }

    let adapter = HttpAdapter::custom("custom", "https://example.test", Protocol::Responses);
    assert!(matches!(
        adapter.inner.credential_placement,
        CredentialPlacement::Bearer
    ));
    assert!(matches!(
        adapter.inner.chat_token_mode,
        crate::chat::TokenMode::MaxTokens
    ));

    let dynamic = adapter
        .with_api_key("static")
        .with_credential_source(Arc::new(Source));
    assert!(dynamic.inner.key_override.is_none());
    assert!(dynamic.inner.credential_source.is_some());

    let static_key = dynamic.with_api_key("replacement");
    assert_eq!(
        static_key.inner.key_override.as_deref(),
        Some("replacement")
    );
    assert!(static_key.inner.credential_source.is_none());
}
#[cfg(feature = "custom-http")]
#[tokio::test]
async fn custom_dynamic_authorization_is_sensitive_before_encoding() {
    struct Source;
    #[async_trait]
    impl CredentialSource for Source {
        async fn credential(&self, _request: &ModelRequest) -> Result<String, ProviderError> {
            Ok("raw-secret".into())
        }
        async fn invalidate(&self, _stale: &str) {}
    }

    let headers = HttpAdapter::custom("custom", "https://example.test", Protocol::Responses)
        .with_credential_source(Arc::new(Source))
        .inner
        .headers(&request("custom"))
        .await
        .unwrap();
    let authorization = headers.get("authorization").unwrap();
    assert_eq!(authorization, "Bearer raw-secret");
    assert!(authorization.is_sensitive());
}
#[cfg(feature = "custom-http")]
#[test]
fn invalid_proxy_error_does_not_disclose_url_credentials() {
    let error = HttpProxyConfig::all("http://proxy-user:super-secret@[invalid")
        .err()
        .expect("proxy URL should be rejected");
    let rendered = error.to_string();
    assert!(!rendered.contains("proxy-user"));
    assert!(!rendered.contains("super-secret"));
}
#[cfg(feature = "codex")]
#[tokio::test]
async fn real_codex_401_refreshes_once_against_loopback() {
    use crabber_auth::{
        CredentialStore, MemoryCredentialStore, OAuthCredentials, TokenManager, now_ms,
    };
    use std::sync::Mutex;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let server_seen = seen.clone();
    let server = tokio::spawn(async move {
        for _ in 0..3 {
            let (mut socket, headers) = accept_loopback_request(&listener).await;
            let first = headers.lines().next().unwrap().to_owned();
            let authorization = headers
                .lines()
                .find(|line| line.to_ascii_lowercase().starts_with("authorization:"))
                .unwrap_or_default()
                .to_owned();
            server_seen
                .lock()
                .unwrap()
                .push((first.clone(), authorization));
            let (status, content_type, body) = if first.contains("/token") {
                (
                    "200 OK",
                    "application/json",
                    r#"{"access_token":"fresh","refresh_token":"rotated","expires_in":3600}"#,
                )
            } else if server_seen
                .lock()
                .unwrap()
                .iter()
                .filter(|(line, _)| line.contains("/responses"))
                .count()
                == 1
            {
                ("401 Unauthorized", "text/plain", "no")
            } else {
                ("500 Server Error", "text/plain", "stale fresh codex-error")
            };
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            write_loopback_response(&mut socket, response.as_bytes()).await;
        }
    });

    let store: Arc<dyn CredentialStore> = Arc::new(MemoryCredentialStore::default());
    store
        .save(&OAuthCredentials {
            client_id: "client".into(),
            host_id: "host".into(),
            account_id: "account".into(),
            access_token: "stale".into(),
            refresh_token: "refresh".into(),
            id_token: "id".into(),
            scopes: vec!["chatgpt.tokens.use.direct".into()],
            expires_unix_ms: now_ms() + 3_600_000,
        })
        .unwrap();
    let mut adapter = HttpAdapter::codex(store.clone()).with_base_url(&base_url);
    adapter.tokens = Some(Arc::new(
        TokenManager::new(store).with_token_url(format!("{base_url}/token")),
    ));
    let error = adapter
        .stream(request("codex"))
        .await
        .err()
        .expect("post-refresh 500 should fail");
    assert_eq!(error.kind, ProviderErrorKind::Server);
    assert!(!error.message.contains("stale"));
    assert!(!error.message.contains("fresh"));
    assert!(error.message.ends_with("[REDACTED] [REDACTED] codex-error"));
    timeout(LOOPBACK_TIMEOUT, server)
        .await
        .expect("loopback server did not finish")
        .unwrap();
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 3);
    assert!(seen[0].1.ends_with("Bearer stale"));
    assert!(seen[1].0.contains("/token"));
    assert!(seen[2].1.ends_with("Bearer fresh"));
}
#[cfg(feature = "opencode-go")]
#[tokio::test]
async fn real_opencode_models_use_endpoint_but_custom_same_id_does_not() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, request) = accept_loopback_request(&listener).await;
        assert!(request.starts_with("GET /models HTTP/1.1"));
        assert!(
            request
                .to_ascii_lowercase()
                .contains("authorization: bearer key")
        );
        let body = r#"{"data":[{"id":"real-model"}]}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        write_loopback_response(&mut socket, response.as_bytes()).await;
    });
    let real = HttpAdapter::opencode_go(Protocol::Responses)
        .with_api_key("key")
        .with_base_url(&base_url);
    assert_eq!(real.models().await.unwrap()[0].id, "real-model");
    timeout(LOOPBACK_TIMEOUT, server)
        .await
        .expect("loopback server did not finish")
        .unwrap();

    #[cfg(feature = "custom-http")]
    {
        let custom = HttpAdapter::custom("opencode-go", "http://127.0.0.1:1", Protocol::Responses)
            .with_api_key("key");
        assert_eq!(
            custom.models().await.unwrap(),
            Vec::<ModelDescriptor>::new()
        );
        let custom_headers = custom.inner.headers(&request("opencode-go")).await.unwrap();
        assert!(!custom_headers.contains_key("x-opencode-session"));
    }
}
#[cfg(feature = "opencode-go")]
#[tokio::test]
async fn opencode_models_error_redacts_bearer_credential() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, request) = accept_loopback_request(&listener).await;
        assert!(request.starts_with("GET /models HTTP/1.1"));
        let body = "model-secret";
        let response = format!(
            "HTTP/1.1 500 Server Error\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        write_loopback_response(&mut socket, response.as_bytes()).await;
    });
    let error = HttpAdapter::opencode_go(Protocol::Responses)
        .with_api_key("model-secret")
        .with_base_url(&base_url)
        .models()
        .await
        .expect_err("model endpoint 500 should fail");
    assert_eq!(error.kind, ProviderErrorKind::Server);
    assert!(!error.message.contains("model-secret"));
    assert!(error.message.ends_with("[REDACTED]"));
    timeout(LOOPBACK_TIMEOUT, server)
        .await
        .expect("loopback server did not finish")
        .unwrap();
}
#[cfg(all(feature = "custom-http", feature = "opencode-go"))]
#[tokio::test]
async fn real_opencode_session_header_is_wire_privileged() {
    use futures::StreamExt;
    use std::sync::Mutex;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_url = format!("http://{}", listener.local_addr().unwrap());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let server_seen = seen.clone();
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (mut socket, request) = accept_loopback_request(&listener).await;
            server_seen.lock().unwrap().push(request);
            let body = "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n";
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            write_loopback_response(&mut socket, response.as_bytes()).await;
        }
    });

    let real = HttpAdapter::opencode_go(Protocol::Responses)
        .with_api_key("real-key")
        .with_base_url(&base_url);
    let _real_events: Vec<_> = real
        .stream(request("opencode-go"))
        .await
        .unwrap()
        .collect()
        .await;

    let custom = HttpAdapter::custom("opencode-go", &base_url, Protocol::Responses)
        .with_api_key("custom-key");
    let _custom_events: Vec<_> = custom
        .stream(request("opencode-go"))
        .await
        .unwrap()
        .collect()
        .await;

    timeout(LOOPBACK_TIMEOUT, server)
        .await
        .expect("loopback server did not finish")
        .unwrap();
    let seen = seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert!(
        seen[0]
            .to_ascii_lowercase()
            .contains("x-opencode-session: session-1")
    );
    assert!(!seen[1].to_ascii_lowercase().contains("x-opencode-session:"));
}

#[cfg(feature = "opencode-go")]
#[tokio::test]
async fn opencode_headers_follow_protocol() {
    let req = request("opencode-go");
    for protocol in [
        Protocol::Responses,
        Protocol::ChatCompletions,
        Protocol::Messages,
    ] {
        let headers = HttpAdapter::opencode_go(protocol)
            .with_api_key("test-key")
            .headers(&req)
            .await
            .unwrap();
        assert_eq!(headers["x-opencode-session"], "session-1");
        if protocol == Protocol::Messages {
            assert_eq!(headers["x-api-key"], "test-key");
            assert_eq!(headers["anthropic-version"], "2023-06-01");
            assert!(!headers.contains_key("authorization"));
        } else {
            assert_eq!(headers["authorization"], "Bearer test-key");
            assert!(!headers.contains_key("x-api-key"));
        }
    }
    let mut invalid = req.clone();
    invalid.identity.session_id = SessionId::from("session with spaces");
    let headers = HttpAdapter::opencode_go(Protocol::Responses)
        .with_api_key("test-key")
        .headers(&invalid)
        .await
        .unwrap();
    assert!(!headers.contains_key("x-opencode-session"));
}
#[cfg(feature = "codex")]
#[tokio::test]
async fn codex_headers_have_exact_session_and_originator() {
    use crabber_auth::{CredentialStore, MemoryCredentialStore, OAuthCredentials, now_ms};
    let store: Arc<dyn CredentialStore> = Arc::new(MemoryCredentialStore::default());
    store
        .save(&OAuthCredentials {
            client_id: "client".into(),
            host_id: "host".into(),
            account_id: "account".into(),
            access_token: "test-token".into(),
            refresh_token: "refresh".into(),
            id_token: "id".into(),
            scopes: vec!["chatgpt.tokens.use.direct".into()],
            expires_unix_ms: now_ms() + 3_600_000,
        })
        .unwrap();
    let headers = HttpAdapter::codex(store)
        .headers(&request("codex"))
        .await
        .unwrap();
    assert_eq!(headers["authorization"], "Bearer test-token");
    assert_eq!(headers["originator"], "advisor");
    assert_eq!(headers["session_id"], "session-1");
}
