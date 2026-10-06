use super::*;

#[tokio::test]
async fn custom_classifier_receives_bounded_excerpt() {
    let server = Server::start(vec![Reply::status(400)]).await;
    let error = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
        .with_api_key("key")
        .with_error_classifier(Arc::new(ContextClassifier))
        .stream(request("custom"))
        .await
        .err()
        .expect("400 response should fail");
    assert_eq!(error.kind, ProviderErrorKind::ContextOverflow);
    assert!(!error.retryable);
    assert!(error.message.contains("400"));
    assert!(error.message.contains("error"));
}

#[tokio::test]
async fn embedded_json_escaped_sensitive_and_canonical_credentials_are_redacted() {
    let canonical = "sec\"ret\\token";
    let sensitive = "ven\"dor\\credential";
    let canonical_serialized = serde_json::to_string(canonical).unwrap();
    let sensitive_serialized = serde_json::to_string(sensitive).unwrap();
    let canonical_payload = canonical_serialized
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap();
    let sensitive_payload = sensitive_serialized
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .unwrap();
    let body =
        format!(r#"{{\"error\":\"prefix {canonical_payload} and {sensitive_payload} suffix\"}}"#);
    let expected = r#"{\"error\":\"prefix [REDACTED] and [REDACTED] suffix\"}"#;
    let server = Server::start(vec![Reply::status(400).with_body(body)]).await;
    let mut headers = HeaderMap::new();
    let mut sensitive_value = HeaderValue::from_str(sensitive).unwrap();
    sensitive_value.set_sensitive(true);
    headers.insert("x-vendor-credential", sensitive_value);

    let error = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
        .with_api_key(canonical)
        .try_with_static_headers(headers)
        .unwrap()
        .with_error_classifier(Arc::new(ExactExcerptClassifier(expected.into())))
        .stream(request("custom"))
        .await
        .err()
        .expect("400 response should fail");

    assert_eq!(error.kind, ProviderErrorKind::ContextOverflow);
    assert!(error.message.ends_with(expected));
    assert!(error.message.contains("[REDACTED]"));
    for protected in [
        canonical,
        sensitive,
        canonical_payload,
        sensitive_payload,
        &canonical_serialized,
        &sensitive_serialized,
    ] {
        assert!(
            !error.message.contains(protected),
            "credential representation leaked: {protected:?}"
        );
    }
}

#[tokio::test]
async fn complete_short_credential_prefix_is_preserved_for_classifier_and_message() {
    for body in ["rate", "example"] {
        let server = Server::start(vec![Reply::status(400).with_body(body)]).await;
        let error = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
            .with_api_key("example-key")
            .with_error_classifier(Arc::new(ExactExcerptClassifier(body.into())))
            .stream(request("custom"))
            .await
            .err()
            .expect("400 response should fail");

        assert_eq!(error.kind, ProviderErrorKind::ContextOverflow);
        assert_eq!(error.message, format!("provider HTTP 400: {body}"));
        assert!(!error.message.contains("[REDACTED]"));
    }
}

#[tokio::test]
async fn w3_review_overlapping_retry_credentials_are_fully_redacted() {
    let stale = "token";
    let fresh = "token-new";
    let escaped = serde_json::to_string(&serde_json::json!({
        "stale": stale,
        "fresh": fresh,
        "stale_authorization": format!("Bearer {stale}"),
        "fresh_authorization": format!("Bearer {fresh}"),
    }))
    .unwrap();
    let body =
        format!("plain={stale}|{fresh}|Bearer {stale}|Bearer {fresh}; json={escaped}; retained");
    let server = Server::start(vec![Reply::status(401), Reply::status(401).with_body(body)]).await;
    let source = Arc::new(SequenceSource::new([stale, fresh]));

    let error = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
        .with_credential_source(source.clone())
        .stream(request("custom"))
        .await
        .err()
        .expect("second 401 should fail");

    assert_eq!(error.kind, ProviderErrorKind::Auth);
    assert!(!error.retryable);
    assert!(error.message.ends_with("; retained"));
    assert!(!error.message.contains(stale));
    assert!(!error.message.contains(fresh));
    assert!(!error.message.contains("[REDACTED]-new"));
    assert!(error.message.len() <= 19 + 4096);
    assert!(error.message.is_char_boundary(error.message.len()));
    let captured = server.captured();
    assert_eq!(captured.len(), 2);
    assert_eq!(values(&captured[0], "authorization"), ["Bearer token"]);
    assert_eq!(values(&captured[1], "authorization"), ["Bearer token-new"]);
    assert_eq!(source.calls.load(Ordering::SeqCst), 2);
    assert_eq!(source.invalidated.lock().unwrap().as_slice(), [stale]);
}

#[tokio::test]
async fn sensitive_hook_values_from_every_retry_attempt_are_fully_redacted() {
    let static_secret = "static-vendor-secret";
    let static_extra = "static-vendor-secret-side";
    let stale = "vendor-token";
    let stale_extra = "vendor-token-side";
    let fresh = "vendor-token-new";
    let fresh_extra = "vendor-token-new-side";
    let serialized = serde_json::to_string(&serde_json::json!({
        "static": static_secret,
        "static_extra": static_extra,
        "stale": stale,
        "stale_extra": stale_extra,
        "fresh": fresh,
        "fresh_extra": fresh_extra,
    }))
    .unwrap();
    let body = format!(
        "plain={static_secret}|{static_extra}|{stale}|{stale_extra}|{fresh}|{fresh_extra}; json={serialized}; retained"
    );
    let server = Server::start(vec![Reply::status(401), Reply::status(500).with_body(body)]).await;
    let source = Arc::new(SequenceSource::new(["primary-stale", "primary-fresh"]));
    let hook = Arc::new(SequenceHook {
        values: Mutex::new(
            [
                Ok(sensitive_hook_headers(&[stale, stale_extra])),
                Ok(sensitive_hook_headers(&[fresh, fresh_extra])),
            ]
            .into(),
        ),
        calls: AtomicUsize::new(0),
    });
    let observer = Arc::new(Observer::default());
    let mut static_headers = HeaderMap::new();
    for secret in [static_secret, static_extra] {
        let mut value = HeaderValue::from_str(secret).unwrap();
        value.set_sensitive(true);
        static_headers.append("x-static-auth", value);
    }

    let error = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
        .try_with_static_headers(static_headers)
        .unwrap()
        .with_credential_source(source.clone())
        .with_request_header_hook(hook.clone())
        .with_response_observer(observer.clone())
        .stream(request("custom"))
        .await
        .err()
        .expect("retry response should fail");

    assert_eq!(error.kind, ProviderErrorKind::Server);
    assert!(error.retryable);
    assert!(error.message.ends_with("; retained"));
    for secret in [
        static_secret,
        static_extra,
        stale,
        stale_extra,
        fresh,
        fresh_extra,
    ] {
        assert!(!error.message.contains(secret), "leaked {secret:?}");
    }
    assert!(!error.message.contains("[REDACTED]-new"));
    assert_eq!(server.count(), 2);
    assert_eq!(source.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        source.invalidated.lock().unwrap().as_slice(),
        ["primary-stale"]
    );
    assert_eq!(hook.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        observer.0.lock().unwrap().as_slice(),
        [StatusCode::UNAUTHORIZED, StatusCode::INTERNAL_SERVER_ERROR]
    );
}

#[tokio::test]
async fn bounded_error_excerpt_redacts_credentials_and_body_failures_are_transport() {
    let secret = "echoed-secret";
    let mut body = format!("Bearer {secret} {secret} ").into_bytes();
    body.extend(vec![b'x'; 5000]);
    let server = RawServer::start(raw_response("500 Server Error", body.len(), &body)).await;
    let error = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
        .with_api_key(secret)
        .stream(request("custom"))
        .await
        .err()
        .expect("500 response should fail");
    assert_eq!(error.kind, ProviderErrorKind::Server);
    assert!(error.retryable);
    assert!(!error.message.contains(secret));
    assert!(error.message.contains("[REDACTED]"));
    assert!(error.message.len() <= 19 + 4096);
    let _ = server.body().await;

    let invalid = vec![0xff; 5000];
    let server = RawServer::start(raw_response("500 Server Error", invalid.len(), &invalid)).await;
    let error = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
        .with_api_key("key")
        .stream(request("custom"))
        .await
        .err()
        .expect("invalid UTF-8 error response should fail");
    assert_eq!(error.kind, ProviderErrorKind::Server);
    assert!(error.message.contains('\u{fffd}'));
    assert!(error.message.len() <= 19 + 4096);
    let _ = server.body().await;

    let server = RawServer::start(raw_response("400 Bad Request", 100, b"cut")).await;
    let error = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
        .with_api_key("key")
        .stream(request("custom"))
        .await
        .err()
        .expect("aborted error body should fail");
    assert_eq!(error.kind, ProviderErrorKind::Transport);
    assert_eq!(error.message, "provider transport body");
    assert!(error.retryable);
    let _ = server.body().await;
}

#[tokio::test]
async fn boundary_split_credentials_are_redacted_in_all_protected_representations() {
    let cases = {
        let plain_secret = "plain-boundary-secret".to_owned();
        let bearer = format!("Bearer {plain_secret}");
        let bearer_prefix_len = 11;
        let plain_body = format!("{}{bearer}", "p".repeat(4096 - bearer_prefix_len));

        let json_secret = "json-\"secret\\tail".to_owned();
        let serialized = serde_json::to_string(&json_secret).unwrap();
        let serialized_prefix_len = 12;
        let json_body = format!("{}{serialized}", "j".repeat(4096 - serialized_prefix_len));

        let embedded_secret = "embedded-\"secret\\é-tail".to_owned();
        let embedded_serialized = serde_json::to_string(&embedded_secret).unwrap();
        let embedded_payload = embedded_serialized
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
            .unwrap();
        let multibyte_start = embedded_payload.find('é').unwrap();
        let embedded_prefix_len = multibyte_start + 1;
        let embedded_body = format!(
            "{}{embedded_payload}",
            "e".repeat(4096 - embedded_prefix_len)
        );

        let long_secret = "long-secret-".repeat(500);
        let long_body = long_secret.clone();

        vec![
            (
                plain_secret,
                plain_body,
                bearer[..bearer_prefix_len].to_owned(),
            ),
            (
                json_secret,
                json_body,
                serialized[..serialized_prefix_len].to_owned(),
            ),
            (
                embedded_secret,
                embedded_body,
                embedded_payload[..multibyte_start].to_owned(),
            ),
            (
                long_secret.clone(),
                long_body,
                long_secret[..4096].to_owned(),
            ),
        ]
    };

    for (credential, body, retained_prefix) in cases {
        let server = RawServer::start(raw_response(
            "500 Server Error",
            body.len(),
            body.as_bytes(),
        ))
        .await;
        let error = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
            .with_api_key(credential)
            .stream(request("custom"))
            .await
            .err()
            .expect("500 response should fail");

        assert_eq!(error.kind, ProviderErrorKind::Server);
        assert!(error.message.contains("[REDACTED]"));
        assert!(
            !error.message.contains(&retained_prefix),
            "credential prefix leaked at excerpt boundary: {retained_prefix:?}"
        );
        assert!(error.message.len() <= 19 + 4096);
        assert!(error.message.is_char_boundary(error.message.len()));
        let _ = server.body().await;
    }
}

#[tokio::test]
async fn multibyte_sensitive_header_split_at_byte_cap_is_redacted_before_lossy_decode() {
    let credential = format!("{}é", "a".repeat(4095));
    let mut value = HeaderValue::from_bytes(credential.as_bytes()).unwrap();
    value.set_sensitive(true);
    let mut headers = HeaderMap::new();
    headers.insert("x-vendor-credential", value);
    let server = RawServer::start(raw_response(
        "500 Server Error",
        credential.len(),
        credential.as_bytes(),
    ))
    .await;

    let error = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
        .with_api_key("primary-key")
        .try_with_static_headers(headers)
        .unwrap()
        .stream(request("custom"))
        .await
        .err()
        .expect("500 response should fail");

    assert_eq!(error.kind, ProviderErrorKind::Server);
    assert!(error.message.contains("[REDACTED]"));
    assert!(!error.message.contains(&"a".repeat(4095)));
    assert!(error.message.len() <= 19 + 4096);
    assert!(error.message.is_char_boundary(error.message.len()));
    let _ = server.body().await;
}

#[tokio::test]
async fn error_excerpt_stops_after_cap_without_polling_another_chunk() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = timeout(LOOPBACK_TIMEOUT, listener.accept())
            .await
            .expect("timed out accepting capped error request")
            .expect("failed to accept capped error request");
        let _ = read_request(&mut socket).await;
        let headers = b"HTTP/1.1 500 Server Error\r\nContent-Type: text/plain\r\nContent-Length: 4097\r\nConnection: close\r\n\r\n";
        socket.write_all(headers).await.unwrap();
        socket.write_all(&vec![b'x'; 4096]).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
    });

    let error = timeout(
        Duration::from_millis(500),
        HttpAdapter::custom("custom", url, Protocol::Responses)
            .with_api_key("key")
            .stream(request("custom")),
    )
    .await
    .expect("error reader polled beyond its 4096-byte cap")
    .err()
    .expect("500 should fail");
    assert_eq!(error.kind, ProviderErrorKind::Server);
    assert_eq!(error.message.len(), 19 + 4096);
    server.abort();
    let _ = server.await;
}
