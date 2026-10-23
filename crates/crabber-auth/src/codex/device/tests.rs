use super::*;
use crate::{MemoryCredentialStore, codex::ISSUER};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use rsa::{
    RsaPrivateKey, RsaPublicKey,
    pkcs1::{EncodeRsaPrivateKey, LineEnding},
    traits::PublicKeyParts,
};
use std::sync::{Mutex, OnceLock};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

struct Keys {
    pem: String,
    jwk: Value,
}
fn keys() -> &'static Keys {
    static KEYS: OnceLock<Keys> = OnceLock::new();
    KEYS.get_or_init(|| {
        let key = RsaPrivateKey::new(&mut rand_core::OsRng, 2048).unwrap();
        let public = RsaPublicKey::from(&key);
        Keys { pem: key.to_pkcs1_pem(LineEnding::LF).unwrap().to_string(), jwk: json!({"kty":"RSA","kid":"test","alg":"RS256","use":"sig","n":URL_SAFE_NO_PAD.encode(public.n().to_bytes_be()),"e":URL_SAFE_NO_PAD.encode(public.e().to_bytes_be())}) }
    })
}
fn claims() -> Value {
    json!({"iss":ISSUER,"sub":"subject","aud":CODEX_CLI_CLIENT_ID,"exp":now_ms()/1000+3600,"https://api.openai.com/auth":{"chatgpt_account_id":"account"}})
}
fn sign(claims: &Value, kid: &str, pem: &str) -> String {
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some(kid.into());
    jsonwebtoken::encode(
        &header,
        claims,
        &EncodingKey::from_rsa_pem(pem.as_bytes()).unwrap(),
    )
    .unwrap()
}
struct Idp {
    endpoints: DeviceEndpoints,
    requests: Arc<Mutex<Vec<(String, String)>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Idp {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn idp(polls: Vec<(u16, Value)>, usercode: Value) -> Idp {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let output = Arc::clone(&requests);
    let token = sign(&claims(), "test", &keys().pem);
    let task = tokio::spawn(async move {
        let mut polls = std::collections::VecDeque::from(polls);
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let Some((path, body)) = read_request(&mut socket).await else {
                continue;
            };
            output.lock().unwrap().push((path.clone(), body));
            let (status, body) = match path.as_str() {
                "/usercode" => (200, usercode.clone()),
                "/token" => polls.pop_front().unwrap_or((403, json!({}))),
                "/oauth/token" => (
                    200,
                    json!({"access_token":"access","refresh_token":"refresh","id_token":token}),
                ),
                "/jwks" => (200, json!({"keys":[keys().jwk]})),
                _ => panic!("unexpected path"),
            };
            let body = body.to_string();
            let response = format!(
                "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(response.as_bytes()).await;
        }
    });
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
    }
}
fn usercode() -> Value {
    json!({"device_auth_id":"device","user_code":"ABCD-EFGH","interval":"0"})
}
fn login(store: Arc<dyn CredentialStore>, idp: &Idp) -> DeviceLogin {
    DeviceLogin::new(store)
        .with_endpoints(idp.endpoints.clone())
        .with_poll_floor(Duration::from_millis(1))
        .with_poll_cap(Duration::from_secs(2))
}

#[tokio::test]
async fn device_login_success_stores_codex_cli_profile() {
    let idp = idp(
        vec![
            (403, json!({})),
            (404, json!({})),
            (
                200,
                json!({"authorization_code":"code","code_verifier":"verifier"}),
            ),
        ],
        usercode(),
    )
    .await;
    let store = Arc::new(MemoryCredentialStore::default());
    let result = login(store.clone(), &idp)
        .run(
            |prompt| assert_eq!(prompt.user_code, "ABCD-EFGH"),
            std::future::pending(),
        )
        .await
        .unwrap();
    assert_eq!(result.profile, CredentialProfile::CodexCli);
    assert!(result.refresh_token.is_some());
    assert_eq!(result.account_id, "account");
    assert_eq!(store.load().unwrap().unwrap().access_token, "access");
    let requests = idp.requests.lock().unwrap();
    assert_eq!(requests.iter().filter(|(p, _)| p == "/token").count(), 3);
    for (_, body) in requests.iter().filter(|(p, _)| p == "/token") {
        let body: Value = serde_json::from_str(body).unwrap();
        assert_eq!(body["device_auth_id"], "device");
        assert_eq!(body["user_code"], "ABCD-EFGH");
    }
    let form: std::collections::HashMap<_, _> = url::form_urlencoded::parse(
        requests
            .iter()
            .find(|(p, _)| p == "/oauth/token")
            .unwrap()
            .1
            .as_bytes(),
    )
    .into_owned()
    .collect();
    assert_eq!(form["redirect_uri"], DEVICE_REDIRECT_URI);
    assert_eq!(form["code_verifier"], "verifier");
}
#[tokio::test]
async fn device_login_denied_and_expired() {
    for (status, body, expired) in [
        (400, json!({"error":"access_denied"}), false),
        (410, json!({}), true),
        (400, json!({"error":"expired_token"}), true),
    ] {
        let idp = idp(vec![(status, body)], usercode()).await;
        let store = Arc::new(MemoryCredentialStore::default());
        let err = login(store.clone(), &idp)
            .run(|_| {}, std::future::pending())
            .await
            .unwrap_err();
        assert!(if expired {
            matches!(err, AuthError::DeviceExpired)
        } else {
            matches!(err, AuthError::DeviceDenied)
        });
        assert!(store.load().unwrap().is_none());
    }
}
#[tokio::test]
async fn device_login_cap_and_cancel() {
    let idp = idp(vec![], usercode()).await;
    let store = Arc::new(MemoryCredentialStore::default());
    assert!(matches!(
        login(store.clone(), &idp)
            .with_poll_cap(Duration::from_millis(50))
            .run(|_| {}, std::future::pending())
            .await,
        Err(AuthError::DeviceExpired)
    ));
    let login = login(store.clone(), &idp).with_poll_floor(Duration::from_secs(2));
    let start = std::time::Instant::now();
    assert!(matches!(
        login
            .run(|_| {}, tokio::time::sleep(Duration::from_millis(10)))
            .await,
        Err(AuthError::Cancelled)
    ));
    assert!(start.elapsed() < Duration::from_millis(200));
    assert!(store.load().unwrap().is_none());
}
#[tokio::test]
async fn device_login_rejects_invalid_user_code() {
    for code in ["abcD", "ABC", "ABCDEFGHIJKLMNOPQ", "ABCD<script>"] {
        let idp = idp(
            vec![],
            json!({"device_auth_id":"device","user_code":code,"interval":0}),
        )
        .await;
        let store = Arc::new(MemoryCredentialStore::default());
        assert!(matches!(
            login(store.clone(), &idp)
                .run(|_| panic!("invalid prompt"), std::future::pending())
                .await,
            Err(AuthError::Authentication(_))
        ));
        assert!(store.load().unwrap().is_none());
    }
}
#[tokio::test]
async fn validated_claims_negative_cases() {
    let idp = idp(vec![], usercode()).await;
    let client = reqwest::Client::new();
    let verify = |token: String, nonce| {
        let client = &client;
        let url = &idp.endpoints.jwks_url;
        async move { validated_claims(client, url, &token, CODEX_CLI_CLIENT_ID, nonce).await }
    };
    let good = sign(&claims(), "test", &keys().pem);
    assert!(verify(good, None).await.is_ok());
    for (field, value) in [
        ("iss", json!("wrong")),
        ("aud", json!("wrong")),
        ("exp", json!(now_ms() / 1000 - 3600)),
    ] {
        let mut bad = claims();
        bad[field] = value;
        assert!(matches!(
            verify(sign(&bad, "test", &keys().pem), None).await,
            Err(AuthError::Authentication(_))
        ));
    }
    assert!(matches!(
        verify(sign(&claims(), "missing", &keys().pem), None).await,
        Err(AuthError::Authentication(_))
    ));
    let other = RsaPrivateKey::new(&mut rand_core::OsRng, 2048)
        .unwrap()
        .to_pkcs1_pem(LineEnding::LF)
        .unwrap();
    assert!(matches!(
        verify(sign(&claims(), "test", other.as_str()), None).await,
        Err(AuthError::Authentication(_))
    ));
    let unsigned = format!(
        "{}.{}.",
        URL_SAFE_NO_PAD.encode(r#"{"alg":"none","kid":"test"}"#),
        URL_SAFE_NO_PAD.encode(claims().to_string())
    );
    assert!(matches!(
        verify(unsigned, None).await,
        Err(AuthError::Authentication(_))
    ));
    assert!(matches!(
        verify(sign(&claims(), "test", &keys().pem), Some("nonce")).await,
        Err(AuthError::Authentication(_))
    ));
    let mut with_nonce = claims();
    with_nonce["nonce"] = json!("wrong");
    assert!(matches!(
        verify(sign(&with_nonce, "test", &keys().pem), Some("nonce")).await,
        Err(AuthError::Authentication(_))
    ));
    with_nonce["nonce"] = json!("nonce");
    assert!(
        verify(sign(&with_nonce, "test", &keys().pem), Some("nonce"))
            .await
            .is_ok()
    );
}
#[test]
fn account_id_precedence() {
    let mut value = claims();
    value["chatgpt_account_id"] = json!("top");
    value["organizations"] = json!([{"id":"organization"}]);
    let read = |v: Value| account_id_from_claims(&serde_json::from_value(v).unwrap()).unwrap();
    assert_eq!(read(value.clone()), "top");
    value.as_object_mut().unwrap().remove("chatgpt_account_id");
    assert_eq!(read(value.clone()), "account");
    value["https://api.openai.com/auth"] = json!({"organization_id":"nested"});
    assert_eq!(read(value.clone()), "nested");
    value["https://api.openai.com/auth"] = json!({});
    assert_eq!(read(value), "organization");
}

async fn read_request(socket: &mut tokio::net::TcpStream) -> Option<(String, String)> {
    let mut bytes = Vec::new();
    let mut chunk = [0; 4096];
    let (header_end, length) = loop {
        let n = read_client_bytes(socket, &mut chunk).await;
        if n == 0 {
            return None;
        }
        bytes.extend_from_slice(&chunk[..n]);
        if let Some(end) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
            let header = String::from_utf8_lossy(&bytes[..end]);
            let length = header
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            break (end + 4, length);
        }
    };
    while bytes.len() < header_end + length {
        let n = read_client_bytes(socket, &mut chunk).await;
        if n == 0 {
            return None;
        }
        bytes.extend_from_slice(&chunk[..n]);
    }
    let path = String::from_utf8_lossy(&bytes)
        .split_whitespace()
        .nth(1)
        .unwrap()
        .to_owned();
    Some((
        path,
        String::from_utf8_lossy(&bytes[header_end..]).into_owned(),
    ))
}

// A capped or cancelled client can close before sending its complete request.
async fn read_client_bytes(socket: &mut tokio::net::TcpStream, chunk: &mut [u8]) -> usize {
    match socket.read(chunk).await {
        Ok(n) => n,
        Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => 0,
        Err(error) => panic!("test request read failed: {error}"),
    }
}

#[path = "tests/transport.rs"]
mod transport;
