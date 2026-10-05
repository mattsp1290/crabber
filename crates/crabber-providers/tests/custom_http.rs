use async_trait::async_trait;
use crabber_core::{RunId, SessionId, TurnId};
use crabber_providers::{
    AuthScheme, ChatTokenField, CredentialSource, ErrorClassifier, HttpAdapter, HttpClientConfig,
    ModelRequest, Protocol, ProviderError, ProviderErrorKind, RequestHeaderHook, RequestIdentity,
    ResponseObserver, Selection, StreamDelta, Streamer,
};
use futures::StreamExt;
use reqwest::{
    StatusCode,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use std::{
    collections::VecDeque,
    fmt::Write as _,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Barrier,
    task::{JoinHandle, JoinSet},
    time::timeout,
};

const LOOPBACK_TIMEOUT: Duration = Duration::from_secs(5);

async fn read_request(socket: &mut TcpStream) -> (Headers, Vec<u8>) {
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
    let text = String::from_utf8(raw[..header_end].to_vec())
        .expect("provider request headers were not UTF-8");
    let content_length = text
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
    let headers = text
        .lines()
        .skip(1)
        .take_while(|line| !line.is_empty())
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_owned()))
        .collect();
    (headers, raw[header_end..request_end].to_vec())
}

#[derive(Clone)]
struct Reply {
    status: u16,
    headers: Vec<(String, String)>,
    body: Option<String>,
}
impl Reply {
    fn status(status: u16) -> Self {
        Self {
            status,
            headers: vec![],
            body: None,
        }
    }
    fn redirect(location: String) -> Self {
        Self {
            status: 302,
            headers: vec![("Location".into(), location)],
            body: None,
        }
    }
    fn with_body(mut self, body: impl Into<String>) -> Self {
        self.body = Some(body.into());
        self
    }
}

type Headers = Vec<(String, String)>;
type CapturedRequests = Arc<Mutex<Vec<Headers>>>;

struct Server {
    url: String,
    requests: CapturedRequests,
    task: JoinHandle<()>,
}
impl Server {
    async fn start(replies: Vec<Reply>) -> Self {
        Self::start_inner(replies, false).await
    }
    async fn concurrent(replies: Vec<Reply>) -> Self {
        Self::start_inner(replies, true).await
    }
    async fn start_inner(replies: Vec<Reply>, gate_first_two: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let saved = requests.clone();
        let replies = Arc::new(replies);
        let next = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Barrier::new(2));
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                let (mut socket, _) = match timeout(LOOPBACK_TIMEOUT, listener.accept()).await {
                    Ok(Ok(accepted)) => accepted,
                    Ok(Err(error)) => panic!("failed to accept provider request: {error}"),
                    Err(_) => break,
                };
                let replies = replies.clone();
                let saved = saved.clone();
                let next = next.clone();
                let gate = gate.clone();
                connections.spawn(async move {
                    let (headers, _) = read_request(&mut socket).await;
                    saved.lock().unwrap().push(headers);
                    let index = next.fetch_add(1, Ordering::SeqCst);
                    if gate_first_two && index < 2 {
                        timeout(LOOPBACK_TIMEOUT, gate.wait())
                            .await
                            .expect("concurrent request gate timed out");
                    }
                    let reply = replies
                        .get(index)
                        .cloned()
                        .unwrap_or_else(|| Reply::status(500));
                    let reason = match reply.status {
                        200 => "OK",
                        401 => "Unauthorized",
                        302 => "Found",
                        429 => "Too Many Requests",
                        _ => "Server Error",
                    };
                    let body = reply.body.unwrap_or_else(|| if reply.status == 200 {
                        "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n"
                            .into()
                    } else {
                        "error".into()
                    });
                    let mut response = format!(
                        "HTTP/1.1 {} {}\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n",
                        reply.status,
                        reason,
                        body.len()
                    );
                    for (name, value) in reply.headers {
                        write!(response, "{name}: {value}\r\n").unwrap();
                    }
                    response.push_str("\r\n");
                    response.push_str(&body);
                    timeout(LOOPBACK_TIMEOUT, socket.write_all(response.as_bytes()))
                        .await
                        .expect("timed out writing provider response")
                        .expect("failed to write provider response");
                });
            }
            connections.abort_all();
            while let Some(result) = timeout(LOOPBACK_TIMEOUT, connections.join_next())
                .await
                .expect("timed out joining provider connection task")
            {
                match result {
                    Ok(()) => {}
                    Err(error) if error.is_cancelled() => {}
                    Err(error) => panic!("provider connection task failed: {error}"),
                }
            }
        });
        Self {
            url,
            requests,
            task,
        }
    }
    fn count(&self) -> usize {
        self.requests.lock().unwrap().len()
    }
    fn captured(&self) -> Vec<Vec<(String, String)>> {
        self.requests.lock().unwrap().clone()
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct RawServer {
    url: String,
    body: tokio::sync::oneshot::Receiver<Vec<u8>>,
    task: JoinHandle<()>,
}
impl RawServer {
    async fn start(response: Vec<u8>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (sender, body) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let (mut socket, _) = timeout(LOOPBACK_TIMEOUT, listener.accept())
                .await
                .expect("timed out accepting raw request")
                .expect("failed to accept raw request");
            let (_, request_body) = read_request(&mut socket).await;
            let _ = sender.send(request_body);
            let _ = socket.write_all(&response).await;
        });
        Self { url, body, task }
    }

    async fn body(self) -> Vec<u8> {
        let body = timeout(LOOPBACK_TIMEOUT, self.body)
            .await
            .expect("timed out waiting for request body")
            .expect("raw server dropped request body");
        timeout(LOOPBACK_TIMEOUT, self.task)
            .await
            .expect("raw server task timed out")
            .expect("raw server task failed");
        body
    }
}

fn raw_response(status: &str, declared_length: usize, body: &[u8]) -> Vec<u8> {
    let mut response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/event-stream\r\nContent-Length: {declared_length}\r\nConnection: close\r\n\r\n"
    )
    .into_bytes();
    response.extend_from_slice(body);
    response
}

fn stream_errors(items: &[StreamDelta]) -> Vec<&ProviderError> {
    items
        .iter()
        .filter_map(|item| match item {
            StreamDelta::Error(error) => Some(error),
            _ => None,
        })
        .collect()
}

fn assert_single_protocol_error(items: &[StreamDelta], expected_message: &str) {
    let terminals = items
        .iter()
        .filter(|item| matches!(item, StreamDelta::Completed | StreamDelta::Error(_)))
        .collect::<Vec<_>>();
    assert_eq!(terminals.len(), 1, "unexpected terminal deltas: {items:?}");
    assert!(matches!(
        terminals[0],
        StreamDelta::Error(error)
            if error.message == expected_message && error.kind != ProviderErrorKind::Transport
    ));
    assert_eq!(
        items
            .iter()
            .filter(|item| matches!(item, StreamDelta::Completed))
            .count(),
        0
    );
    assert_eq!(stream_errors(items).len(), 1);
    assert!(stream_errors(items)[0].message != "provider transport body");
}

fn request(provider: &str) -> ModelRequest {
    ModelRequest {
        identity: RequestIdentity {
            session_id: SessionId::from("session"),
            run_id: RunId::from("run"),
            turn_id: TurnId::from("turn"),
        },
        selection: Selection {
            provider_id: provider.into(),
            model_id: "model".into(),
        },
        system: None,
        messages: vec![],
        tools: vec![],
        temperature: None,
        max_tokens: Some(8),
        tool_choice: None,
    }
}
fn values(headers: &[(String, String)], name: &str) -> Vec<String> {
    headers
        .iter()
        .filter(|(n, _)| n == name)
        .map(|(_, v)| v.clone())
        .collect()
}

struct SequenceSource {
    values: Mutex<VecDeque<Result<String, ProviderError>>>,
    calls: AtomicUsize,
    invalidated: Mutex<Vec<String>>,
}
impl SequenceSource {
    fn new(values: impl IntoIterator<Item = &'static str>) -> Self {
        Self {
            values: Mutex::new(values.into_iter().map(|v| Ok(v.into())).collect()),
            calls: AtomicUsize::new(0),
            invalidated: Mutex::new(vec![]),
        }
    }
}
#[async_trait]
impl CredentialSource for SequenceSource {
    async fn credential(&self, _request: &ModelRequest) -> Result<String, ProviderError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.values.lock().unwrap().pop_front().unwrap()
    }
    async fn invalidate(&self, stale: &str) {
        self.invalidated.lock().unwrap().push(stale.into());
    }
}

struct SequenceHook {
    values: Mutex<VecDeque<Result<HeaderMap, ProviderError>>>,
    calls: AtomicUsize,
}
impl RequestHeaderHook for SequenceHook {
    fn headers(&self, _request: &ModelRequest) -> Result<HeaderMap, ProviderError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.values.lock().unwrap().pop_front().unwrap()
    }
}
#[derive(Default)]
struct Observer(Mutex<Vec<StatusCode>>);
impl ResponseObserver for Observer {
    fn observe(&self, status: StatusCode, _headers: &HeaderMap) {
        self.0.lock().unwrap().push(status);
    }
}

#[derive(Default)]
struct HeaderObserver(Mutex<Vec<(StatusCode, Option<String>)>>);
impl ResponseObserver for HeaderObserver {
    fn observe(&self, status: StatusCode, headers: &HeaderMap) {
        self.0.lock().unwrap().push((
            status,
            headers
                .get("x-observer-proof")
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
        ));
    }
}
fn hook_headers(attempt: usize) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.append(
        "x-replaced",
        HeaderValue::from_str(&format!("hook-{attempt}-a")).unwrap(),
    );
    headers.append(
        "x-replaced",
        HeaderValue::from_str(&format!("hook-{attempt}-b")).unwrap(),
    );
    headers.insert(
        "x-attempt",
        HeaderValue::from_str(&attempt.to_string()).unwrap(),
    );
    headers
}

fn sensitive_hook_headers(values: &[&str]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for value in values {
        let mut value = HeaderValue::from_str(value).unwrap();
        value.set_sensitive(true);
        headers.append("x-auth-token", value);
    }
    headers
}
fn invalid_error(message: &str) -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::Invalid,
        message: message.into(),
        retryable: false,
    }
}

struct ContextClassifier;
impl ErrorClassifier for ContextClassifier {
    fn classify(&self, _status: StatusCode, excerpt: &str) -> (ProviderErrorKind, bool) {
        assert_eq!(excerpt, "error");
        (ProviderErrorKind::ContextOverflow, false)
    }
}

struct ExactExcerptClassifier(String);
impl ErrorClassifier for ExactExcerptClassifier {
    fn classify(&self, _status: StatusCode, excerpt: &str) -> (ProviderErrorKind, bool) {
        assert_eq!(excerpt, self.0);
        (ProviderErrorKind::ContextOverflow, false)
    }
}

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
async fn w3_review_completed_is_emitted_once_only_after_clean_finalization() {
    let completed = b"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n";
    let mut malformed = [completed.as_slice(), completed.as_slice()].concat();
    malformed.push(0xff);
    let server = RawServer::start(raw_response("200 OK", malformed.len(), &malformed)).await;
    let items: Vec<_> = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
        .with_api_key("key")
        .stream(request("custom"))
        .await
        .unwrap()
        .collect()
        .await;
    assert!(
        !items
            .iter()
            .any(|item| matches!(item, StreamDelta::Completed))
    );
    assert_eq!(stream_errors(&items)[0].message, "provider transport body");
    let _ = server.body().await;

    let server = RawServer::start(raw_response("200 OK", completed.len() + 10, completed)).await;
    let items: Vec<_> = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
        .with_api_key("key")
        .stream(request("custom"))
        .await
        .unwrap()
        .collect()
        .await;
    assert!(
        !items
            .iter()
            .any(|item| matches!(item, StreamDelta::Completed))
    );
    assert_eq!(stream_errors(&items)[0].message, "provider transport body");
    let _ = server.body().await;

    let text = b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"before\"}\n\n";
    let duplicated = [text.as_slice(), completed.as_slice(), completed.as_slice()].concat();
    let server = RawServer::start(raw_response("200 OK", duplicated.len(), &duplicated)).await;
    let mut stream = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
        .with_api_key("key")
        .stream(request("custom"))
        .await
        .unwrap();
    let mut consumer_items = Vec::new();
    while let Some(item) = stream.next().await {
        let terminal = matches!(item, StreamDelta::Completed);
        consumer_items.push(item);
        if terminal {
            break;
        }
    }
    assert!(matches!(
        consumer_items.first(),
        Some(StreamDelta::TextDelta(text)) if text == "before"
    ));
    assert!(matches!(
        consumer_items.last(),
        Some(StreamDelta::Completed)
    ));
    assert_eq!(
        consumer_items
            .iter()
            .filter(|item| matches!(item, StreamDelta::Completed))
            .count(),
        1
    );
    assert!(stream.next().await.is_none());
    assert_eq!(stream_errors(&consumer_items), Vec::<&ProviderError>::new());
    let _ = server.body().await;

    let incomplete = b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n";
    let server = RawServer::start(raw_response("200 OK", incomplete.len(), incomplete)).await;
    let items: Vec<_> = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
        .with_api_key("key")
        .stream(request("custom"))
        .await
        .unwrap()
        .collect()
        .await;
    assert_eq!(stream_errors(&items)[0].message, "provider transport body");
    let _ = server.body().await;

    let invalid_json = b"data: {\n";
    let server = RawServer::start(raw_response("200 OK", invalid_json.len(), invalid_json)).await;
    let items: Vec<_> = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
        .with_api_key("key")
        .stream(request("custom"))
        .await
        .unwrap()
        .collect()
        .await;
    assert_eq!(stream_errors(&items)[0].kind, ProviderErrorKind::Invalid);
    let _ = server.body().await;
}

#[tokio::test]
async fn ordinary_deltas_after_protocol_completion_are_suppressed() {
    let responses = concat!(
        "event: response.completed\n",
        "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":3,\"output_tokens\":5}}}\n\n",
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"after\"}\n\n"
    );
    let chat = concat!(
        "data: [DONE]\n\n",
        "data: {\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":11},\"choices\":[{\"delta\":{\"content\":\"after\"}}]}\n\n"
    );

    for (protocol, body) in [
        (Protocol::Responses, responses),
        (Protocol::ChatCompletions, chat),
    ] {
        let server = RawServer::start(raw_response("200 OK", body.len(), body.as_bytes())).await;
        let items: Vec<_> = HttpAdapter::custom("custom", &server.url, protocol)
            .with_api_key("key")
            .stream(request("custom"))
            .await
            .unwrap()
            .collect()
            .await;

        assert_eq!(
            items
                .iter()
                .filter(|item| matches!(item, StreamDelta::Completed))
                .count(),
            1,
            "unexpected terminal output: {items:?}"
        );
        assert!(
            !items
                .iter()
                .any(|item| matches!(item, StreamDelta::TextDelta(text) if text == "after")),
            "post-completion text leaked: {items:?}"
        );
        assert_eq!(stream_errors(&items), Vec::<&ProviderError>::new());
        match protocol {
            Protocol::Responses => assert!(matches!(
                items.as_slice(),
                [StreamDelta::Usage(_), StreamDelta::Completed]
            )),
            Protocol::ChatCompletions => {
                assert!(matches!(items.as_slice(), [StreamDelta::Completed]));
            }
            Protocol::Messages => unreachable!(),
        }
        let _ = server.body().await;
    }
}

#[tokio::test]
async fn valid_protocol_errors_are_the_only_terminal_delta() {
    let cases = [
        (
            Protocol::Responses,
            concat!(
                "data: {\"type\":\"response.output_text.delta\",\"delta\":\"before\"}\n\n",
                "event: response.failed\n",
                "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_error\"}}}\n\n"
            ),
            "Responses response.failed: server_error",
        ),
        (
            Protocol::Responses,
            concat!(
                "event: response.completed\n",
                "data: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n",
                "event: response.incomplete\n",
                "data: {\"type\":\"response.incomplete\",\"response\":{\"error\":{\"code\":\"context_limit\"}}}\n\n"
            ),
            "Responses response.incomplete: context_limit",
        ),
        (
            Protocol::Messages,
            concat!(
                "event: content_block_delta\n",
                "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"before\"}}\n\n",
                "event: error\n",
                "data: {\"type\":\"error\",\"error\":{\"type\":\"api_error\"}}\n\n"
            ),
            "Messages stream error",
        ),
        (
            Protocol::Messages,
            concat!(
                "event: message_stop\n",
                "data: {\"type\":\"message_stop\"}\n\n",
                "event: error\n",
                "data: {\"type\":\"error\",\"error\":{\"type\":\"api_error\"}}\n\n"
            ),
            "Messages stream error",
        ),
    ];

    for (protocol, body, expected_message) in cases {
        let server = RawServer::start(raw_response("200 OK", body.len(), body.as_bytes())).await;
        let mut stream = HttpAdapter::custom("custom", &server.url, protocol)
            .with_api_key("key")
            .stream(request("custom"))
            .await
            .unwrap();
        let mut consumer_items = Vec::new();
        while let Some(item) = stream.next().await {
            let terminal = matches!(item, StreamDelta::Completed | StreamDelta::Error(_));
            consumer_items.push(item);
            if terminal {
                break;
            }
        }

        assert_single_protocol_error(&consumer_items, expected_message);
        assert!(stream.next().await.is_none());
        let _ = server.body().await;
    }
}

#[tokio::test]
async fn decode_stops_at_first_protocol_error_in_same_chunk() {
    let cases = [
        (
            Protocol::Responses,
            concat!(
                "data: {\"type\":\"response.output_text.delta\",\"delta\":\"before\"}\n\n",
                "event: response.failed\n",
                "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_error\"}}}\n\n",
                "data: {\n\n"
            ),
            "Responses response.failed: server_error",
        ),
        (
            Protocol::Messages,
            concat!(
                "event: content_block_delta\n",
                "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"before\"}}\n\n",
                "event: error\n",
                "data: {\"type\":\"error\",\"error\":{\"type\":\"api_error\"}}\n\n",
                "data: {\n\n"
            ),
            "Messages stream error",
        ),
    ];

    for (protocol, body, expected_message) in cases {
        let server = RawServer::start(raw_response("200 OK", body.len(), body.as_bytes())).await;
        let items: Vec<_> = HttpAdapter::custom("custom", &server.url, protocol)
            .with_api_key("key")
            .stream(request("custom"))
            .await
            .unwrap()
            .collect()
            .await;

        assert!(matches!(
            items.first(),
            Some(StreamDelta::TextDelta(text)) if text == "before"
        ));
        assert_single_protocol_error(&items, expected_message);
        assert_eq!(items.len(), 2, "unexpected decoded items: {items:?}");
        let _ = server.body().await;
    }
}

#[tokio::test]
async fn parser_batches_preserve_order_before_terminal_or_parser_error() {
    let protocol_cases = [
        (
            Protocol::Responses,
            concat!(
                "data: {\"type\":\"response.output_text.delta\",\"delta\":\"before\"}\n\n",
                "event: response.failed\n",
                "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_error\"}}}\n\n"
            ),
            "Responses response.failed: server_error",
        ),
        (
            Protocol::Messages,
            concat!(
                "event: content_block_delta\n",
                "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"before\"}}\n\n",
                "event: error\n",
                "data: {\"type\":\"error\",\"error\":{\"type\":\"api_error\"}}\n\n"
            ),
            "Messages stream error",
        ),
    ];
    for (protocol, prefix, expected_message) in protocol_cases {
        let mut body = prefix.as_bytes().to_vec();
        body.extend_from_slice(b"data: \xff\n\n");
        let server = RawServer::start(raw_response("200 OK", body.len(), &body)).await;
        let items: Vec<_> = HttpAdapter::custom("custom", &server.url, protocol)
            .with_api_key("key")
            .stream(request("custom"))
            .await
            .unwrap()
            .collect()
            .await;

        assert!(matches!(items.first(), Some(StreamDelta::TextDelta(text)) if text == "before"));
        assert_single_protocol_error(&items, expected_message);
        assert_eq!(
            items.len(),
            2,
            "later parser failure replaced terminal: {items:?}"
        );
        let _ = server.body().await;
    }

    let prefix = concat!(
        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"before\"}\n\n",
        "event: response.failed\n",
        "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_error\"}}}\n\n"
    );
    let mut oversized = prefix.as_bytes().to_vec();
    oversized.extend_from_slice(b"data: ");
    oversized.resize(
        oversized.len() + crabber_providers::sse::MAX_LINE_BYTES + 1,
        b'x',
    );
    oversized.push(b'\n');
    let server = RawServer::start(raw_response("200 OK", oversized.len(), &oversized)).await;
    let items: Vec<_> = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
        .with_api_key("key")
        .stream(request("custom"))
        .await
        .unwrap()
        .collect()
        .await;
    assert!(matches!(items.first(), Some(StreamDelta::TextDelta(text)) if text == "before"));
    assert_single_protocol_error(&items, "Responses response.failed: server_error");
    assert_eq!(
        items.len(),
        2,
        "oversized tail replaced terminal: {items:?}"
    );
    let _ = server.body().await;

    let mut malformed =
        b"data: {\"type\":\"response.output_text.delta\",\"delta\":\"before\"}\n\n".to_vec();
    malformed.extend_from_slice(b"data: \xff\n\n");
    let server = RawServer::start(raw_response("200 OK", malformed.len(), &malformed)).await;
    let items: Vec<_> = HttpAdapter::custom("custom", &server.url, Protocol::Responses)
        .with_api_key("key")
        .stream(request("custom"))
        .await
        .unwrap()
        .collect()
        .await;
    assert!(matches!(items.first(), Some(StreamDelta::TextDelta(text)) if text == "before"));
    assert!(matches!(
        items.get(1),
        Some(StreamDelta::Error(error))
            if error.kind == ProviderErrorKind::Invalid && error.message == "invalid SSE UTF-8"
    ));
    assert_eq!(items.len(), 2, "unexpected parser-error output: {items:?}");
    let _ = server.body().await;
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

#[cfg(feature = "openai")]
#[tokio::test]
async fn built_in_status_mapping_ignores_custom_classifier() {
    struct PanicClassifier;
    impl ErrorClassifier for PanicClassifier {
        fn classify(&self, _: StatusCode, _: &str) -> (ProviderErrorKind, bool) {
            panic!("built-in adapter must not invoke custom classifier")
        }
    }
    let secret = "built-in-secret";
    let server = Server::start(vec![Reply::status(400).with_body(secret)]).await;
    let error = HttpAdapter::openai()
        .with_base_url(&server.url)
        .with_api_key(secret)
        .with_error_classifier(Arc::new(PanicClassifier))
        .stream(request("openai"))
        .await
        .err()
        .expect("400 should fail");
    assert_eq!(error.kind, ProviderErrorKind::Invalid);
    assert!(!error.message.contains(secret));
    assert!(error.message.ends_with("[REDACTED]"));
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
