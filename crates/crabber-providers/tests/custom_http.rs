use async_trait::async_trait;
use crabber_core::{RunId, SessionId, TurnId};
use crabber_providers::{
    AuthScheme, CredentialSource, HttpAdapter, ModelRequest, Protocol, ProviderError,
    ProviderErrorKind, RequestHeaderHook, RequestIdentity, ResponseObserver, Selection, Streamer,
};
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

async fn read_request(socket: &mut TcpStream) -> Headers {
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
    text.lines()
        .skip(1)
        .take_while(|line| !line.is_empty())
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_owned()))
        .collect()
}

#[derive(Clone)]
struct Reply {
    status: u16,
    headers: Vec<(String, String)>,
}
impl Reply {
    fn status(status: u16) -> Self {
        Self {
            status,
            headers: vec![],
        }
    }
    fn redirect(location: String) -> Self {
        Self {
            status: 302,
            headers: vec![("Location".into(), location)],
        }
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
                    let headers = read_request(&mut socket).await;
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
                    let body = if reply.status == 200 {
                        "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n"
                    } else {
                        "error"
                    };
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
                    response.push_str(body);
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
fn invalid_error(message: &str) -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::Invalid,
        message: message.into(),
        retryable: false,
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
    let server = Server::start(vec![Reply::status(401), Reply::status(401)]).await;
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
        .with_response_observer(observer.clone());
    let error = adapter.stream(request("custom")).await.err().unwrap();
    assert_eq!(error.kind, ProviderErrorKind::Auth);
    assert!(!error.retryable);
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

#[tokio::test]
async fn observer_receives_actual_response_headers() {
    let server = Server::start(vec![Reply {
        status: 200,
        headers: vec![("X-Observer-Proof".into(), "wire-value".into())],
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
