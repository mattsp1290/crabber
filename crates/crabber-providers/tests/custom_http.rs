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
    body: tokio::sync::oneshot::Receiver<(Headers, Vec<u8>)>,
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
            let captured = read_request(&mut socket).await;
            let _ = sender.send(captured);
            let _ = socket.write_all(&response).await;
        });
        Self { url, body, task }
    }

    async fn body(self) -> Vec<u8> {
        self.headers_and_body().await.1
    }

    async fn headers_and_body(self) -> (Headers, Vec<u8>) {
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

#[path = "custom_http/auth.rs"]
mod auth;
#[path = "custom_http/chat.rs"]
mod chat;
#[path = "custom_http/redaction.rs"]
mod redaction;
#[path = "custom_http/streaming.rs"]
mod streaming;
#[path = "custom_http/transport.rs"]
mod transport;
