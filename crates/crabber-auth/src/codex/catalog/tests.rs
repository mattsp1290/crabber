use super::*;
use crate::now_ms;
use std::sync::{Arc, Mutex};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};
fn credentials() -> OAuthCredentials {
    OAuthCredentials {
        profile: CredentialProfile::CodexCli,
        client_id: String::new(),
        host_id: String::new(),
        account_id: "account".into(),
        access_token: "catalog-access".into(),
        refresh_token: None,
        id_token: String::new(),
        scopes: vec![],
        expires_unix_ms: now_ms() + 120_000,
    }
}
async fn server(
    body: String,
    status: u16,
) -> (String, Arc<Mutex<String>>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let seen = Arc::new(Mutex::new(String::new()));
    let output = seen.clone();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = [0; 4096];
        let n = socket.read(&mut bytes).await.unwrap();
        *output.lock().unwrap() = String::from_utf8_lossy(&bytes[..n]).into_owned();
        let header = format!(
            "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        let _ = socket.write_all(header.as_bytes()).await;
        let _ = socket.write_all(body.as_bytes()).await;
    });
    (base, seen, task)
}
fn request(base: &str) -> CatalogRequest<'_> {
    CatalogRequest {
        client_version: "agentcraft-test",
        base_url: Some(base),
        user_agent: Some("test/1"),
    }
}
#[tokio::test]
async fn catalog_request_shape_filter_and_sort() {
    let models = serde_json::json!({"models":[
        {"slug":"later","display_name":"Later","visibility":"list","priority":2,"supported_reasoning_levels":[{"effort":"high","description":"thorough"}]},
        {"slug":"hidden","display_name":"Hidden","visibility":"hidden","priority":0,"supported_reasoning_levels":[]},
        {"slug":"first","display_name":"First","visibility":"list","priority":1,"supported_reasoning_levels":[],"default_reasoning_level":"medium"}
    ]});
    let (base, seen, task) = server(models.to_string(), 200).await;
    let result = list_models(&reqwest::Client::new(), &credentials(), request(&base))
        .await
        .unwrap();
    task.await.unwrap();
    assert_eq!(
        result.iter().map(|m| m.slug.as_str()).collect::<Vec<_>>(),
        ["first", "later"]
    );
    let seen = seen.lock().unwrap().to_ascii_lowercase();
    assert!(seen.starts_with("get /models?client_version=agentcraft-test "));
    for header in [
        "authorization: bearer catalog-access",
        "chatgpt-account-id: account",
        "originator: advisor",
        "accept: application/json",
        "user-agent: test/1",
    ] {
        assert!(seen.contains(header), "missing {header}");
    }
}
#[tokio::test]
async fn catalog_profile_gate_size_cap_and_errors() {
    let client = reqwest::Client::new();
    let mut creds = credentials();
    creds.profile = CredentialProfile::Siwc;
    assert!(matches!(
        list_models(&client, &creds, request("http://127.0.0.1:1")).await,
        Err(AuthError::ProfileMismatch)
    ));
    creds.profile = CredentialProfile::CodexCli;
    for version in ["", "bad\nversion"] {
        assert!(matches!(
            list_models(
                &client,
                &creds,
                CatalogRequest {
                    client_version: version,
                    ..request("http://127.0.0.1:1")
                }
            )
            .await,
            Err(AuthError::Authentication("invalid client version"))
        ));
    }
    for (status, auth) in [(401, true), (403, true), (500, false)] {
        let (base, _, task) = server("{}".into(), status).await;
        let result = list_models(&client, &creds, request(&base)).await;
        assert!(if auth {
            matches!(result, Err(AuthError::Authentication("catalog rejected")))
        } else {
            matches!(result, Err(AuthError::Transport))
        });
        task.await.unwrap();
    }
    let (base, _, task) = server(" ".repeat(8 * 1024 * 1024 + 1), 200).await;
    assert!(matches!(
        list_models(&client, &creds, request(&base)).await,
        Err(AuthError::Transport)
    ));
    task.await.unwrap();
}
