use super::*;
use tokio::sync::Notify;

#[derive(Clone, Copy)]
enum Fault {
    Truncated(u16),
    Oversized,
    Stall(&'static str),
}
async fn faulty_idp(fault: Fault) -> (Idp, Arc<Notify>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let output = requests.clone();
    let entered = Arc::new(Notify::new());
    let seen = entered.clone();
    let token = sign(&claims(), "test", &keys().pem);
    let task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (path, body) = read_request(&mut socket).await;
            output.lock().unwrap().push((path.clone(), body));
            if matches!(fault,Fault::Stall(stage) if stage==path) {
                seen.notify_one();
                std::future::pending::<()>().await;
            }
            if path == "/token"
                && let Fault::Truncated(status) = fault
            {
                let response = format!(
                    "HTTP/1.1 {status} Test\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{{"
                );
                socket.write_all(response.as_bytes()).await.unwrap();
                socket.shutdown().await.unwrap();
                continue;
            }
            if path == "/usercode" && matches!(fault, Fault::Oversized) {
                let body = "x".repeat(crate::http_body::MAX_AUTH_BODY_BYTES + 1);
                let response = format!(
                    "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:x}\r\n{body}\r\n0\r\n\r\n",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
                continue;
            }
            let body = match path.as_str() {
                "/usercode" => usercode(),
                "/token" => json!({"authorization_code":"code","code_verifier":"verifier"}),
                "/oauth/token" => {
                    json!({"access_token":"access","refresh_token":"refresh","id_token":token})
                }
                "/jwks" => json!({"keys":[keys().jwk]}),
                _ => panic!("unexpected auth path"),
            }
            .to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
        }
    });
    (
        Idp {
            endpoints: DeviceEndpoints {
                usercode_url: format!("{base}/usercode"),
                token_url: format!("{base}/token"),
                exchange_url: format!("{base}/oauth/token"),
                jwks_url: format!("{base}/jwks"),
                ..DeviceEndpoints::default()
            },
            requests,
            task,
        },
        entered,
    )
}
#[tokio::test]
async fn truncated_poll_bodies_preserve_transport_failure_without_retry() {
    for status in [200, 403, 404] {
        let (idp, _) = faulty_idp(Fault::Truncated(status)).await;
        let store = Arc::new(MemoryCredentialStore::default());
        let result = login(store.clone(), &idp)
            .run(|_| {}, std::future::pending())
            .await;
        assert!(matches!(result, Err(AuthError::Transport)));
        assert_eq!(
            idp.requests
                .lock()
                .unwrap()
                .iter()
                .map(|(path, _)| path.as_str())
                .collect::<Vec<_>>(),
            vec!["/usercode", "/token"]
        );
        assert!(store.load().unwrap().is_none());
    }
}
#[tokio::test]
async fn oversized_chunked_auth_body_is_bounded() {
    let (idp, _) = faulty_idp(Fault::Oversized).await;
    let store = Arc::new(MemoryCredentialStore::default());
    let result = login(store.clone(), &idp)
        .run(|_| panic!("oversize prompt"), std::future::pending())
        .await;
    assert!(matches!(
        result,
        Err(AuthError::Authentication(
            "authentication response too large"
        ))
    ));
    assert!(store.load().unwrap().is_none());
}
#[tokio::test]
async fn stalled_http_stages_are_cancellable_and_capped() {
    for stage in ["/usercode", "/oauth/token", "/jwks"] {
        let (idp, entered) = faulty_idp(Fault::Stall(stage)).await;
        let store = Arc::new(MemoryCredentialStore::default());
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            login(store.clone(), &idp).run(|_| {}, async move { entered.notified().await }),
        )
        .await
        .unwrap();
        assert!(matches!(result, Err(AuthError::Cancelled)));
        assert!(store.load().unwrap().is_none());
        let (idp, entered) = faulty_idp(Fault::Stall(stage)).await;
        let store = Arc::new(MemoryCredentialStore::default());
        let (result, ()) = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(
                login(store.clone(), &idp)
                    .with_poll_cap(Duration::from_secs(1))
                    .run(|_| {}, std::future::pending()),
                entered.notified()
            )
        })
        .await
        .unwrap();
        assert!(matches!(result, Err(AuthError::DeviceExpired)));
        assert!(store.load().unwrap().is_none());
    }
}
