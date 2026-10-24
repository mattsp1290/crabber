use super::tests::{LOOPBACK_TIMEOUT, accept_loopback_request, request, write_loopback_response};
use super::*;
use crate::ProviderErrorKind;
use crabber_auth::{
    CredentialProfile, CredentialStore, MemoryCredentialStore, OAuthCredentials, TokenManager,
    now_ms,
};
use tokio::{net::TcpListener, time::timeout};
fn credentials() -> OAuthCredentials {
    OAuthCredentials {
        profile: CredentialProfile::CodexCli,
        client_id: "client".into(),
        host_id: "host".into(),
        account_id: "account".into(),
        access_token: "broker-stale".into(),
        refresh_token: None,
        id_token: String::new(),
        scopes: vec![],
        expires_unix_ms: now_ms() + 120_000,
    }
}
#[tokio::test]
async fn codex_cli_headers_carry_account_id_and_profile_gate() {
    let store = Arc::new(MemoryCredentialStore::default());
    let mut creds = credentials();
    store.save(&creds).unwrap();
    let adapter = HttpAdapter::codex_cli(store.clone());
    let headers = adapter.headers(&request("codex")).await.unwrap();
    assert_eq!(headers["chatgpt-account-id"], "account");
    assert_eq!(headers["originator"], "advisor");
    assert_eq!(headers["session_id"], "session-1");
    creds.profile = CredentialProfile::Siwc;
    store.save(&creds).unwrap();
    assert_eq!(
        adapter.headers(&request("codex")).await.unwrap_err().kind,
        ProviderErrorKind::Auth
    );
    assert!(
        !HttpAdapter::codex(store.clone())
            .headers(&request("codex"))
            .await
            .unwrap()
            .contains_key("chatgpt-account-id")
    );
    for account in ["", "bad\naccount"] {
        creds.profile = CredentialProfile::CodexCli;
        creds.account_id = account.into();
        store.save(&creds).unwrap();
        assert!(adapter.headers(&request("codex")).await.is_err());
    }
}
#[tokio::test]
async fn codex_cli_401_rereads_access_only_store() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let store = Arc::new(MemoryCredentialStore::default());
    store.save(&credentials()).unwrap();
    let rotated = store.clone();
    let server = tokio::spawn(async move {
        for index in 0..2 {
            let (mut socket, headers) = accept_loopback_request(&listener).await;
            assert!(
                headers.starts_with("POST /responses "),
                "unexpected token endpoint: {headers}"
            );
            assert!(headers.contains(if index == 0 {
                "Bearer broker-stale"
            } else {
                "Bearer broker-rotated"
            }));
            let (status, body) = if index == 0 {
                let mut creds = credentials();
                creds.access_token = "broker-rotated".into();
                rotated.save(&creds).unwrap();
                ("401 Unauthorized", "no")
            } else {
                (
                    "500 Server Error",
                    "broker-stale broker-rotated broker-error",
                )
            };
            write_loopback_response(
                &mut socket,
                format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            )
            .await;
        }
    });
    let mut adapter = HttpAdapter::codex_cli(store.clone()).with_base_url(&base);
    adapter.tokens = Some(Arc::new(
        TokenManager::new(store).with_token_url(format!("{base}/token")),
    ));
    let error = adapter.stream(request("codex")).await.err().unwrap();
    assert_eq!(error.kind, ProviderErrorKind::Server);
    assert!(!error.message.contains("broker-stale"));
    assert!(!error.message.contains("broker-rotated"));
    assert!(error.message.contains("[REDACTED]"));
    timeout(LOOPBACK_TIMEOUT, server).await.unwrap().unwrap();
}
#[tokio::test]
async fn access_only_token_never_appears_in_sanitized_error() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = accept_loopback_request(&listener).await;
        let body = "echo broker-stale";
        write_loopback_response(&mut socket,format!("HTTP/1.1 500 Server Error\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await;
    });
    let store = Arc::new(MemoryCredentialStore::default());
    store.save(&credentials()).unwrap();
    let error = HttpAdapter::codex_cli(store)
        .with_base_url(base)
        .stream(request("codex"))
        .await
        .err()
        .unwrap();
    assert!(!error.message.contains("broker-stale"));
    assert!(error.message.contains("[REDACTED]"));
    timeout(LOOPBACK_TIMEOUT, server).await.unwrap().unwrap();
}
