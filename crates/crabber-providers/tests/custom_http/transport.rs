use super::*;

#[tokio::test]
async fn observer_receives_actual_response_headers() {
    let server = Server::start(vec![Reply {
        status: 200,
        headers: vec![("X-Observer-Proof".into(), "wire-value".into())],
        body: None,
    }])
    .await;
    let observer = Arc::new(HeaderObserver::default());

    let _stream = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
        .with_api_key("key")
        .with_response_observer(observer.clone())
        .stream(request("custom"))
        .await
        .unwrap();

    assert_eq!(
        observer.0.lock().unwrap().as_slice(),
        [(StatusCode::OK, Some("wire-value".into()))]
    );
}

#[tokio::test]
async fn cross_origin_redirect_is_not_followed_and_identity_headers_do_not_leak() {
    let target = Server::start(vec![Reply::status(200)]).await;
    let origin = Server::start(vec![Reply::redirect(format!("{}/responses", target.url))]).await;
    let observer = Arc::new(Observer::default());
    let mut static_headers = HeaderMap::new();
    static_headers.insert("x-static-identity", HeaderValue::from_static("static"));
    let mut hook_map = HeaderMap::new();
    hook_map.insert("x-hook-identity", HeaderValue::from_static("hook"));
    let hook = Arc::new(SequenceHook {
        values: Mutex::new([Ok(hook_map)].into()),
        calls: AtomicUsize::new(0),
    });
    let _ = HttpAdapter::custom("custom", &origin.url, Protocol::Responses)
        .with_api_key("secret")
        .try_with_static_headers(static_headers)
        .unwrap()
        .with_request_header_hook(hook)
        .with_response_observer(observer.clone())
        .stream(request("custom"))
        .await
        .err()
        .unwrap();
    tokio::task::yield_now().await;
    assert_eq!(origin.count(), 1);
    assert_eq!(target.count(), 0);
    assert_eq!(*observer.0.lock().unwrap(), [StatusCode::FOUND]);
}

#[tokio::test]
async fn status_mapping_and_observer_headers_are_preserved() {
    let observer = Arc::new(HeaderObserver::default());
    let server = Server::start(vec![Reply {
        status: 429,
        headers: vec![("X-Observer-Proof".into(), "rate".into())],
        body: None,
    }])
    .await;
    let error = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
        .with_api_key("key")
        .with_response_observer(observer.clone())
        .stream(request("custom"))
        .await
        .err()
        .expect("429 should fail");
    assert_eq!(error.kind, ProviderErrorKind::RateLimited);
    assert!(error.retryable);
    assert!(error.message.ends_with("error"));
    assert_eq!(
        observer.0.lock().unwrap().as_slice(),
        [(StatusCode::TOO_MANY_REQUESTS, Some("rate".into()))]
    );

    let server = Server::start(vec![Reply::status(500)]).await;
    let error = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
        .with_api_key("key")
        .stream(request("custom"))
        .await
        .err()
        .expect("500 should fail");
    assert_eq!(error.kind, ProviderErrorKind::Server);
    assert!(error.retryable);
}

#[tokio::test]
async fn malformed_custom_endpoints_are_sanitized_invalid_before_hooks_or_send() {
    for (endpoint, secret) in [
        ("mailto:not-http@example.test", "not-http"),
        ("relative/custom-host", "custom-host"),
        ("http://", "http://"),
        ("http://user:credential@[::1", "credential"),
    ] {
        let hook = Arc::new(SequenceHook {
            values: Mutex::new([Ok(HeaderMap::new())].into()),
            calls: AtomicUsize::new(0),
        });
        let observer = Arc::new(Observer::default());
        let error = HttpAdapter::custom("custom", endpoint, Protocol::Responses)
            .with_api_key("key")
            .with_request_header_hook(hook.clone())
            .with_response_observer(observer.clone())
            .stream(request("custom"))
            .await
            .err()
            .expect("malformed endpoint should fail");

        assert_eq!(
            error.kind,
            ProviderErrorKind::Invalid,
            "unexpected classification for {endpoint:?}: {error:?}"
        );
        assert!(!error.retryable);
        assert_eq!(error.message, "invalid provider setting");
        assert!(!error.to_string().contains(secret));
        assert_eq!(hook.calls.load(Ordering::SeqCst), 0);
        assert!(observer.0.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn query_and_fragment_endpoints_are_rejected_before_credentials_hooks_or_send() {
    for suffix in ["?tenant=query-secret", "#fragment-secret"] {
        let server = Server::start(vec![Reply::status(200)]).await;
        let source = Arc::new(SequenceSource::new(["credential"]));
        let hook = Arc::new(SequenceHook {
            values: Mutex::new([Ok(HeaderMap::new())].into()),
            calls: AtomicUsize::new(0),
        });
        let observer = Arc::new(Observer::default());
        let endpoint = format!("{}{suffix}", server.url);
        let error = HttpAdapter::custom("custom", endpoint, Protocol::Responses)
            .with_credential_source(source.clone())
            .with_request_header_hook(hook.clone())
            .with_response_observer(observer.clone())
            .stream(request("custom"))
            .await
            .err()
            .expect("query/fragment endpoint should fail");

        assert_eq!(error.kind, ProviderErrorKind::Invalid);
        assert!(!error.retryable);
        assert_eq!(error.message, "invalid provider setting");
        assert!(!error.message.contains("secret"));
        assert_eq!(source.calls.load(Ordering::SeqCst), 0);
        assert_eq!(hook.calls.load(Ordering::SeqCst), 0);
        assert!(observer.0.lock().unwrap().is_empty());
        assert_eq!(server.count(), 0);
    }
}

#[tokio::test]
async fn connect_and_timeout_transport_messages_are_stable_and_sanitized() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead_url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let error = HttpAdapter::custom("custom", dead_url, Protocol::Responses)
        .with_api_key("key")
        .stream(request("custom"))
        .await
        .err()
        .expect("closed listener should reject connection");
    assert_eq!(error.message, "provider transport connect");

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = timeout(LOOPBACK_TIMEOUT, listener.accept())
            .await
            .expect("timed out accepting timeout-test request")
            .expect("failed to accept timeout-test request");
        let mut request_bytes = Vec::new();
        // The client is expected to close this silent connection when its timeout fires.
        // Reading to EOF deliberately tolerates that close at any request boundary.
        let _ = timeout(LOOPBACK_TIMEOUT, socket.read_to_end(&mut request_bytes)).await;
    });
    let error = HttpAdapter::custom("custom", url, Protocol::Responses)
        .with_api_key("key")
        .try_with_client_config(HttpClientConfig::new().timeout(Duration::from_millis(250)))
        .unwrap()
        .stream(request("custom"))
        .await
        .err()
        .expect("silent server should time out");
    assert_eq!(error.message, "provider transport timeout");
    assert!(error.retryable);
    timeout(LOOPBACK_TIMEOUT, server).await.unwrap().unwrap();
}
