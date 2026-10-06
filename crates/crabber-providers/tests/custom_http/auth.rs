use super::*;

#[tokio::test]
async fn dynamic_401_rebuilds_all_headers_once_and_preserves_merge_order() {
    let server = Server::start(vec![Reply::status(401), Reply::status(200)]).await;
    let source = Arc::new(SequenceSource::new(["stale", "fresh"]));
    let hook = Arc::new(SequenceHook {
        values: Mutex::new([Ok(hook_headers(1)), Ok(hook_headers(2))].into()),
        calls: AtomicUsize::new(0),
    });
    let observer = Arc::new(Observer::default());
    let mut static_headers = HeaderMap::new();
    static_headers.append("x-replaced", HeaderValue::from_static("static-a"));
    static_headers.append("x-replaced", HeaderValue::from_static("static-b"));
    static_headers.append("x-preserved", HeaderValue::from_static("one"));
    static_headers.append("x-preserved", HeaderValue::from_static("two"));
    let adapter = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
        .try_with_static_headers(static_headers)
        .unwrap()
        .with_credential_source(source.clone())
        .with_request_header_hook(hook.clone())
        .with_response_observer(observer.clone());

    let _stream = adapter.stream(request("custom")).await.unwrap();
    let captured = server.captured();
    assert_eq!(captured.len(), 2);
    assert_eq!(values(&captured[0], "authorization"), ["Bearer stale"]);
    assert_eq!(values(&captured[1], "authorization"), ["Bearer fresh"]);
    for (index, headers) in captured.iter().enumerate() {
        assert_eq!(values(headers, "x-api-key"), Vec::<String>::new());
        assert_eq!(values(headers, "content-type"), ["application/json"]);
        assert_eq!(values(headers, "user-agent"), ["crabber/0.1"]);
        assert_eq!(values(headers, "x-preserved"), ["one", "two"]);
        assert_eq!(
            values(headers, "x-replaced"),
            [
                format!("hook-{}-a", index + 1),
                format!("hook-{}-b", index + 1)
            ]
        );
    }
    assert_eq!(source.calls.load(Ordering::SeqCst), 2);
    assert_eq!(*source.invalidated.lock().unwrap(), ["stale"]);
    assert_eq!(hook.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        *observer.0.lock().unwrap(),
        [StatusCode::UNAUTHORIZED, StatusCode::OK]
    );
}

#[tokio::test]
async fn x_api_key_second_401_is_nonretryable_auth_with_exact_counts() {
    struct PanicClassifier;
    impl ErrorClassifier for PanicClassifier {
        fn classify(&self, _: StatusCode, _: &str) -> (ProviderErrorKind, bool) {
            panic!("second 401 must override the classifier")
        }
    }
    let server = Server::start(vec![
        Reply::status(401).with_body("old"),
        Reply::status(401).with_body("old new retained-excerpt"),
    ])
    .await;
    let source = Arc::new(SequenceSource::new(["old", "new"]));
    let hook = Arc::new(SequenceHook {
        values: Mutex::new([Ok(HeaderMap::new()), Ok(HeaderMap::new())].into()),
        calls: AtomicUsize::new(0),
    });
    let observer = Arc::new(Observer::default());
    let adapter = HttpAdapter::custom("custom", &server.url, Protocol::Messages)
        .with_auth_scheme(AuthScheme::XApiKey)
        .with_credential_source(source.clone())
        .with_request_header_hook(hook.clone())
        .with_response_observer(observer.clone())
        .with_error_classifier(Arc::new(PanicClassifier));
    let error = adapter.stream(request("custom")).await.err().unwrap();
    assert_eq!(error.kind, ProviderErrorKind::Auth);
    assert!(!error.retryable);
    assert!(
        error
            .message
            .ends_with("[REDACTED] [REDACTED] retained-excerpt")
    );
    assert!(!error.message.contains("old"));
    assert!(!error.message.contains("new"));
    let captured = server.captured();
    assert_eq!(captured.len(), 2);
    assert_eq!(values(&captured[0], "x-api-key"), ["old"]);
    assert_eq!(values(&captured[1], "x-api-key"), ["new"]);
    assert!(
        captured
            .iter()
            .all(|h| values(h, "authorization").is_empty())
    );
    assert!(
        captured
            .iter()
            .all(|h| values(h, "anthropic-version") == ["2023-06-01"])
    );
    assert_eq!(source.calls.load(Ordering::SeqCst), 2);
    assert_eq!(*source.invalidated.lock().unwrap(), ["old"]);
    assert_eq!(hook.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        *observer.0.lock().unwrap(),
        [StatusCode::UNAUTHORIZED, StatusCode::UNAUTHORIZED]
    );
}

#[tokio::test]
async fn all_protected_hook_names_are_rejected_sanitized_before_send() {
    for protected in [
        "authorization",
        "x-api-key",
        "content-type",
        "user-agent",
        "anthropic-version",
    ] {
        let server = Server::start(vec![Reply::status(200)]).await;
        let secret = format!("secret-{protected}");
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_bytes(protected.as_bytes()).unwrap(),
            HeaderValue::from_str(&secret).unwrap(),
        );
        let hook = Arc::new(SequenceHook {
            values: Mutex::new([Ok(headers)].into()),
            calls: AtomicUsize::new(0),
        });
        let error = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
            .with_api_key("key")
            .with_request_header_hook(hook)
            .stream(request("custom"))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind, ProviderErrorKind::Invalid);
        assert!(!error.retryable);
        assert!(!error.to_string().contains(&secret));
        assert_eq!(server.count(), 0);
    }
}

#[tokio::test]
async fn static_401_and_dynamic_non_401_never_retry_and_observer_sees_each_response() {
    let static_server = Server::start(vec![Reply::status(401), Reply::status(200)]).await;
    let static_observer = Arc::new(Observer::default());
    let error = HttpAdapter::custom("custom", &static_server.url, Protocol::Responses)
        .with_api_key("static")
        .with_response_observer(static_observer.clone())
        .stream(request("custom"))
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind, ProviderErrorKind::Auth);
    assert_eq!(static_server.count(), 1);
    assert_eq!(
        *static_observer.0.lock().unwrap(),
        [StatusCode::UNAUTHORIZED]
    );

    for status in [302, 429, 500] {
        let server = Server::start(vec![Reply::status(status), Reply::status(200)]).await;
        let source = Arc::new(SequenceSource::new(["only"]));
        let observer = Arc::new(Observer::default());
        let _ = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
            .with_credential_source(source.clone())
            .with_response_observer(observer.clone())
            .stream(request("custom"))
            .await
            .err()
            .unwrap();
        assert_eq!(server.count(), 1);
        assert_eq!(source.calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            *observer.0.lock().unwrap(),
            [StatusCode::from_u16(status).unwrap()]
        );
    }
}

#[tokio::test]
async fn hook_and_credential_failures_are_pre_send_and_preserved_or_sanitized() {
    let server = Server::start(vec![Reply::status(200)]).await;
    let expected = invalid_error("hook unavailable");
    let hook = Arc::new(SequenceHook {
        values: Mutex::new([Err(expected.clone())].into()),
        calls: AtomicUsize::new(0),
    });
    let error = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
        .with_api_key("key")
        .with_request_header_hook(hook)
        .stream(request("custom"))
        .await
        .err()
        .unwrap();
    assert_eq!(error, expected);
    assert_eq!(server.count(), 0);

    for credential in ["", "bad\ncredential"] {
        let source = Arc::new(SequenceSource::new([credential]));
        let error = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
            .with_credential_source(source)
            .stream(request("custom"))
            .await
            .err()
            .unwrap();
        assert_eq!(error.kind, ProviderErrorKind::Invalid);
        if !credential.is_empty() {
            assert!(!error.to_string().contains(credential));
        }
        assert_eq!(server.count(), 0);
    }
}

#[tokio::test]
async fn retry_hook_failure_and_transport_failure_do_not_send_or_observe_phantoms() {
    let server = Server::start(vec![Reply::status(401), Reply::status(200)]).await;
    let source = Arc::new(SequenceSource::new(["stale", "fresh"]));
    let expected = invalid_error("retry hook unavailable");
    let hook = Arc::new(SequenceHook {
        values: Mutex::new([Ok(HeaderMap::new()), Err(expected.clone())].into()),
        calls: AtomicUsize::new(0),
    });
    let observer = Arc::new(Observer::default());
    let error = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
        .with_credential_source(source.clone())
        .with_request_header_hook(hook.clone())
        .with_response_observer(observer.clone())
        .stream(request("custom"))
        .await
        .err()
        .unwrap();
    assert_eq!(error, expected);
    assert_eq!(server.count(), 1);
    assert_eq!(source.calls.load(Ordering::SeqCst), 2);
    assert_eq!(hook.calls.load(Ordering::SeqCst), 2);
    assert_eq!(*observer.0.lock().unwrap(), [StatusCode::UNAUTHORIZED]);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let transport_observer = Arc::new(Observer::default());
    let error = HttpAdapter::custom("custom", dead_url, Protocol::Responses)
        .with_api_key("key")
        .with_response_observer(transport_observer.clone())
        .stream(request("custom"))
        .await
        .err()
        .unwrap();
    assert_eq!(error.kind, ProviderErrorKind::Transport);
    assert!(transport_observer.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn retry_credential_failure_stops_before_second_hook_send_and_observer() {
    let server = Server::start(vec![Reply::status(401), Reply::status(200)]).await;
    let expected = invalid_error("retry credential unavailable");
    let source = Arc::new(SequenceSource {
        values: Mutex::new([Ok("stale".into()), Err(expected.clone())].into()),
        calls: AtomicUsize::new(0),
        invalidated: Mutex::new(vec![]),
    });
    let hook = Arc::new(SequenceHook {
        values: Mutex::new([Ok(HeaderMap::new()), Ok(HeaderMap::new())].into()),
        calls: AtomicUsize::new(0),
    });
    let observer = Arc::new(Observer::default());

    let error = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
        .with_credential_source(source.clone())
        .with_request_header_hook(hook.clone())
        .with_response_observer(observer.clone())
        .stream(request("custom"))
        .await
        .err()
        .unwrap();

    assert_eq!(error, expected);
    assert_eq!(source.calls.load(Ordering::SeqCst), 2);
    assert_eq!(source.invalidated.lock().unwrap().as_slice(), ["stale"]);
    assert_eq!(hook.calls.load(Ordering::SeqCst), 1);
    assert_eq!(server.count(), 1);
    assert_eq!(*observer.0.lock().unwrap(), [StatusCode::UNAUTHORIZED]);
}

struct ConcurrentSource {
    token: tokio::sync::Mutex<Option<String>>,
    refreshes: AtomicUsize,
    invalidations: Mutex<Vec<String>>,
}
#[async_trait]
impl CredentialSource for ConcurrentSource {
    async fn credential(&self, _request: &ModelRequest) -> Result<String, ProviderError> {
        let mut token = self.token.lock().await;
        if token.is_none() {
            self.refreshes.fetch_add(1, Ordering::SeqCst);
            tokio::task::yield_now().await;
            *token = Some("U".into());
        }
        Ok(token.clone().unwrap())
    }
    async fn invalidate(&self, stale: &str) {
        self.invalidations.lock().unwrap().push(stale.into());
        let mut token = self.token.lock().await;
        if token.as_deref() == Some(stale) {
            *token = None;
        }
    }
}

#[tokio::test]
async fn concurrent_source_compare_invalidates_and_refreshes_single_flight() {
    let server = Server::concurrent(vec![
        Reply::status(401),
        Reply::status(401),
        Reply::status(200),
        Reply::status(200),
    ])
    .await;
    let source = Arc::new(ConcurrentSource {
        token: tokio::sync::Mutex::new(Some("T".into())),
        refreshes: AtomicUsize::new(0),
        invalidations: Mutex::new(vec![]),
    });
    let adapter = Arc::new(
        HttpAdapter::custom("codex", &server.url, Protocol::Responses)
            .with_credential_source(source.clone()),
    );
    let (left, right) = tokio::join!(
        adapter.stream(request("codex")),
        adapter.stream(request("codex"))
    );
    assert!(left.is_ok() && right.is_ok());
    assert_eq!(server.count(), 4);
    let auth: Vec<String> = server
        .captured()
        .iter()
        .flat_map(|h| values(h, "authorization"))
        .collect();
    assert_eq!(auth.iter().filter(|v| *v == "Bearer T").count(), 2);
    assert_eq!(auth.iter().filter(|v| *v == "Bearer U").count(), 2);
    assert_eq!(source.refreshes.load(Ordering::SeqCst), 1);
    assert_eq!(source.invalidations.lock().unwrap().as_slice(), ["T", "T"]);
    assert_eq!(source.token.lock().await.as_deref(), Some("U"));
}

#[tokio::test]
async fn second_dynamic_401_body_failures_remain_terminal_auth() {
    struct PanicClassifier;
    impl ErrorClassifier for PanicClassifier {
        fn classify(&self, _: StatusCode, _: &str) -> (ProviderErrorKind, bool) {
            panic!("second 401 must override the classifier")
        }
    }
    for scheme in [AuthScheme::Bearer, AuthScheme::XApiKey] {
        for stalled_body in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move {
                let mut captured = Vec::new();
                for attempt in 0..2 {
                    let (mut socket, _) = timeout(LOOPBACK_TIMEOUT, listener.accept())
                        .await
                        .unwrap()
                        .unwrap();
                    captured.push(read_request(&mut socket).await.0);
                    let response = if attempt == 0 {
                        raw_response("401 Unauthorized", 0, b"")
                    } else {
                        raw_response("401 Unauthorized", 100, b"old new cut")
                    };
                    socket.write_all(&response).await.unwrap();
                    if attempt == 1 && stalled_body {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
                captured
            });
            let source = Arc::new(SequenceSource::new(["old", "new"]));
            let hook = Arc::new(SequenceHook {
                values: Mutex::new([Ok(HeaderMap::new()), Ok(HeaderMap::new())].into()),
                calls: AtomicUsize::new(0),
            });
            let observer = Arc::new(Observer::default());
            let adapter = HttpAdapter::custom("custom", url, Protocol::Messages)
                .with_auth_scheme(scheme)
                .with_credential_source(source.clone())
                .with_request_header_hook(hook.clone())
                .with_response_observer(observer.clone())
                .with_error_classifier(Arc::new(PanicClassifier))
                .try_with_client_config(
                    HttpClientConfig::new().read_timeout(Duration::from_millis(50)),
                )
                .unwrap();
            let error = timeout(LOOPBACK_TIMEOUT, adapter.stream(request("custom")))
                .await
                .unwrap()
                .err()
                .unwrap();
            assert_eq!(error.kind, ProviderErrorKind::Auth);
            assert!(!error.retryable);
            assert_eq!(
                error.message,
                "provider HTTP 401: response body unavailable"
            );
            let captured = timeout(LOOPBACK_TIMEOUT, server).await.unwrap().unwrap();
            assert_eq!(captured.len(), 2);
            assert_eq!(source.calls.load(Ordering::SeqCst), 2);
            assert_eq!(*source.invalidated.lock().unwrap(), ["old"]);
            assert_eq!(hook.calls.load(Ordering::SeqCst), 2);
            assert_eq!(
                *observer.0.lock().unwrap(),
                [StatusCode::UNAUTHORIZED, StatusCode::UNAUTHORIZED]
            );
            for (headers, key) in captured.iter().zip(["old", "new"]) {
                match scheme {
                    AuthScheme::Bearer => {
                        assert_eq!(values(headers, "authorization"), [format!("Bearer {key}")]);
                    }
                    AuthScheme::XApiKey => assert_eq!(values(headers, "x-api-key"), [key]),
                }
            }
        }
    }
}
