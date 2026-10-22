use super::*;
use crate::transport::{Body, Transport};
use async_trait::async_trait;
use std::{collections::VecDeque, net::SocketAddr, sync::Mutex};
use tokio_util::sync::CancellationToken;
struct Reply {
    status: u16,
    location: Option<String>,
    length: Option<u64>,
    chunks: VecDeque<Vec<u8>>,
}
impl Reply {
    fn text(bytes: Vec<u8>) -> Self {
        Self {
            status: 200,
            location: None,
            length: None,
            chunks: VecDeque::from([bytes]),
        }
    }
    fn redirect(location: &str) -> Self {
        Self {
            status: 302,
            location: Some(location.into()),
            length: None,
            chunks: VecDeque::new(),
        }
    }
}
#[async_trait]
impl Body for Reply {
    fn status(&self) -> u16 {
        self.status
    }
    fn location(&self) -> Result<Option<String>, ToolError> {
        Ok(self.location.clone())
    }
    fn length(&self) -> Option<u64> {
        self.length
    }
    async fn chunk(&mut self) -> Result<Option<Vec<u8>>, ToolError> {
        Ok(self.chunks.pop_front())
    }
}
#[derive(Default)]
struct Mock {
    resolutions: Mutex<VecDeque<Vec<SocketAddr>>>,
    replies: Mutex<VecDeque<Reply>>,
    resolved: Mutex<Vec<String>>,
    connections: Mutex<Vec<(String, Vec<SocketAddr>)>>,
}
impl Mock {
    fn new(resolutions: Vec<Vec<&str>>, replies: Vec<Reply>) -> Self {
        Self {
            resolutions: Mutex::new(
                resolutions
                    .into_iter()
                    .map(|r| r.into_iter().map(|a| a.parse().unwrap()).collect())
                    .collect(),
            ),
            replies: Mutex::new(replies.into()),
            ..Self::default()
        }
    }
}
#[async_trait]
impl Transport for Mock {
    async fn resolve(&self, host: &str, _: u16) -> Result<Vec<SocketAddr>, ToolError> {
        self.resolved.lock().unwrap().push(host.into());
        Ok(self
            .resolutions
            .lock()
            .unwrap()
            .pop_front()
            .expect("scripted DNS"))
    }
    async fn get(
        &self,
        url: &Url,
        host: &str,
        addresses: &[SocketAddr],
    ) -> Result<Box<dyn Body>, ToolError> {
        assert_eq!(url.host().unwrap().to_string(), host);
        self.connections
            .lock()
            .unwrap()
            .push((url.as_str().into(), addresses.into()));
        Ok(Box::new(
            self.replies
                .lock()
                .unwrap()
                .pop_front()
                .expect("scripted response"),
        ))
    }
}
async fn fetch(mock: &Mock, url: &str) -> Result<String, ToolError> {
    fetch::https(mock, &UrlFetchPolicy::default(), Url::parse(url).unwrap()).await
}
#[tokio::test]
async fn checked_addresses_are_pinned_and_dns_is_rechecked_on_same_host_redirect() {
    let mock = Mock::new(
        vec![vec!["8.8.8.8:0", "1.1.1.1:0"], vec!["127.0.0.1:0"]],
        vec![Reply::redirect("/next")],
    );
    assert_eq!(
        fetch(&mock, "https://EXAMPLE.COM.:8443/start")
            .await
            .unwrap_err()
            .category,
        "validation"
    );
    assert_eq!(
        *mock.resolved.lock().unwrap(),
        ["example.com", "example.com"]
    );
    let connections = mock.connections.lock().unwrap();
    assert_eq!(connections.len(), 1);
    assert_eq!(
        connections[0].1,
        [
            "8.8.8.8:8443".parse::<SocketAddr>().unwrap(),
            "1.1.1.1:8443".parse().unwrap()
        ]
    );
}
#[tokio::test]
async fn private_literals_mixed_dns_and_redirects_fail_before_connection() {
    for ip in [
        "127.0.0.1",
        "10.1.2.3",
        "169.254.169.254",
        "172.16.0.1",
        "192.168.0.1",
        "100.64.0.1",
        "0.0.0.0",
        "224.0.0.1",
        "[::1]",
        "[fe80::1]",
        "[fc00::1]",
        "[::ffff:127.0.0.1]",
        "[64:ff9b::7f00:1]",
        "[2002:7f00:1::]",
        "[3fff::1]",
        "[3fff:fff:ffff:ffff:ffff:ffff:ffff:ffff]",
        "192.88.99.2",
        "[::ffff:192.88.99.2]",
    ] {
        let mock = Mock::default();
        assert_eq!(
            fetch(&mock, &format!("https://{ip}/"))
                .await
                .unwrap_err()
                .category,
            "validation",
            "{ip}"
        );
        assert!(mock.connections.lock().unwrap().is_empty());
        let mock = Mock::new(
            vec![vec!["8.8.8.8:443"]],
            vec![Reply::redirect(&format!("https://{ip}/"))],
        );
        assert_eq!(
            fetch(&mock, "https://example.com/")
                .await
                .unwrap_err()
                .category,
            "validation",
            "{ip}"
        );
        assert_eq!(mock.connections.lock().unwrap().len(), 1);
    }
    let mock = Mock::new(vec![vec!["8.8.8.8:443", "10.0.0.1:443"]], vec![]);
    assert_eq!(
        fetch(&mock, "https://example.com/")
            .await
            .unwrap_err()
            .category,
        "validation"
    );
    assert!(mock.connections.lock().unwrap().is_empty());
}
#[tokio::test]
async fn allowlist_revalidates_every_redirect_and_can_explicitly_allow_private_networks() {
    let policy = UrlFetchPolicy {
        allow: vec![
            HostPattern::exact("example.com").unwrap(),
            HostPattern::subdomains("example.org").unwrap(),
        ],
        ..Default::default()
    };
    for denied in [
        "https://example.org/",
        "https://badexample.org/",
        "https://example.com.evil.test/",
        "http://example.com/",
        "file:///etc/hosts",
        "https://user:pass@example.com/",
    ] {
        let mock = Mock::new(vec![vec!["8.8.8.8:443"]], vec![Reply::redirect(denied)]);
        assert_eq!(
            fetch::https(&mock, &policy, Url::parse("https://example.com/").unwrap())
                .await
                .unwrap_err()
                .category,
            "validation"
        );
        assert_eq!(mock.connections.lock().unwrap().len(), 1);
    }
    let mock = Mock::new(
        vec![vec!["8.8.8.8:443"], vec!["1.1.1.1:443"]],
        vec![
            Reply::redirect("https://a.example.org/"),
            Reply::text(b"ok".to_vec()),
        ],
    );
    assert_eq!(
        fetch::https(&mock, &policy, Url::parse("https://example.com/").unwrap())
            .await
            .unwrap(),
        "ok"
    );
    let mock = Mock::new(vec![], vec![Reply::text(b"local".to_vec())]);
    assert_eq!(
        fetch::https(
            &mock,
            &UrlFetchPolicy {
                allow: vec![],
                deny_private_ranges: false
            },
            Url::parse("https://127.0.0.1/").unwrap()
        )
        .await
        .unwrap(),
        "local"
    );
}
#[tokio::test]
async fn bounded_redirects_statuses_and_streamed_bodies() {
    let mock = Mock::new(
        vec![vec!["8.8.8.8:443"]; 6],
        (0..6).map(|_| Reply::redirect("/again")).collect(),
    );
    assert_eq!(
        fetch(&mock, "https://example.com/")
            .await
            .unwrap_err()
            .message,
        "redirect limit exceeded"
    );
    assert_eq!(mock.connections.lock().unwrap().len(), 6);
    for (reply, category) in [
        (
            Reply {
                status: 404,
                ..Reply::text(vec![])
            },
            "not_found",
        ),
        (
            Reply {
                status: 500,
                ..Reply::text(vec![])
            },
            "network",
        ),
        (Reply::text(vec![0xff]), "binary"),
        (
            Reply {
                length: Some(MAX_BODY_BYTES as u64 + 1),
                ..Reply::text(vec![])
            },
            "too_large",
        ),
        (
            Reply {
                chunks: VecDeque::from([vec![b'a'; MAX_BODY_BYTES], vec![b'b']]),
                ..Reply::text(vec![])
            },
            "too_large",
        ),
    ] {
        let mock = Mock::new(vec![vec!["8.8.8.8:443"]], vec![reply]);
        assert_eq!(
            fetch(&mock, "https://example.com/")
                .await
                .unwrap_err()
                .category,
            category
        );
    }
    let mock = Mock::new(
        vec![vec!["8.8.8.8:443"]],
        vec![Reply::text(vec![b'a'; MAX_BODY_BYTES])],
    );
    assert_eq!(
        fetch(&mock, "https://example.com/").await.unwrap().len(),
        MAX_BODY_BYTES
    );
}
struct Pending;
#[async_trait]
impl Transport for Pending {
    async fn resolve(&self, _: &str, _: u16) -> Result<Vec<SocketAddr>, ToolError> {
        std::future::pending().await
    }
    async fn get(&self, _: &Url, _: &str, _: &[SocketAddr]) -> Result<Box<dyn Body>, ToolError> {
        unreachable!()
    }
}
use crabber_tools_testkit as support;
fn tool(root: &std::path::Path) -> FetchTool {
    let limits = support::limits();
    FetchTool {
        options: Arc::new(Options {
            root: WorkspaceRoot::open(root).unwrap(),
            policy: UrlFetchPolicy::default(),
            capacity: Capacity::new(&limits).unwrap(),
            limits,
        }),
        transport: Arc::new(Pending),
    }
}
#[tokio::test(start_paused = true)]
async fn total_timeout_includes_dns_and_releases_capacity() {
    let root = tempfile::tempdir().unwrap();
    let tool = tool(root.path());
    let result = tool
        .execute_owned_context(
            support::context(&tool.options.root, CancellationToken::new()),
            json!({"url":"https://example.com/"}),
        )
        .await
        .unwrap()
        .json_value()
        .unwrap();
    assert_eq!(result["error"]["category"], "timeout");
    assert_eq!(TIMEOUT, Duration::from_secs(30));
    let _permit = acquire_read(
        &tool.options.capacity,
        &tool.options.limits,
        &CancellationToken::new(),
    )
    .await
    .unwrap();
}
#[tokio::test]
async fn cancellation_during_dns_returns_executor_error_and_releases_capacity() {
    let root = tempfile::tempdir().unwrap();
    let tool = tool(root.path());
    let cancel = CancellationToken::new();
    let ctx = support::context(&tool.options.root, cancel.clone());
    let work = tool.execute_owned_context(ctx, json!({"url":"https://example.com/"}));
    tokio::pin!(work);
    tokio::select! { biased; result = &mut work => panic!("unexpected result: {result:?}"), _ = tokio::task::yield_now() => {} }
    cancel.cancel();
    assert!(work.await.is_err());
    let _permit = acquire_read(
        &tool.options.capacity,
        &tool.options.limits,
        &CancellationToken::new(),
    )
    .await
    .unwrap();
}
#[test]
fn policy_construction_is_normalized_and_checked() {
    assert_eq!(
        HostPattern::exact("EXAMPLE.COM.").unwrap(),
        HostPattern::Exact("example.com".into())
    );
    assert!(HostPattern::subdomains("127.0.0.1").is_err());
    for pattern in [
        HostPattern::Exact("EXAMPLE.COM".into()),
        HostPattern::Subdomains("*.example.com".into()),
        HostPattern::Exact("host/path".into()),
    ] {
        assert!(
            UrlFetchPolicy {
                allow: vec![pattern],
                ..Default::default()
            }
            .validate()
            .is_err()
        );
    }
    let policy = UrlFetchPolicy::default();
    for ip in ["8.8.8.8", "1.1.1.1", "2606:4700:4700::1111"] {
        policy.check_ip(ip.parse().unwrap()).unwrap();
    }
}

// Append inside src/tests.rs after owner approval; private FetchTool seam is reused.
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
struct PendingBody {
    started: Arc<tokio::sync::Notify>,
    dropped: Arc<AtomicBool>,
}
impl Drop for PendingBody {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}
#[async_trait]
impl Body for PendingBody {
    fn status(&self) -> u16 {
        200
    }
    fn location(&self) -> Result<Option<String>, ToolError> {
        Ok(None)
    }
    fn length(&self) -> Option<u64> {
        None
    }
    async fn chunk(&mut self) -> Result<Option<Vec<u8>>, ToolError> {
        self.started.notify_one();
        std::future::pending().await
    }
}
struct BodyTransport {
    started: Arc<tokio::sync::Notify>,
    dropped: Arc<AtomicBool>,
    hops: AtomicUsize,
    delay_redirect: bool,
}
#[async_trait]
impl Transport for BodyTransport {
    async fn resolve(&self, _: &str, port: u16) -> Result<Vec<SocketAddr>, ToolError> {
        Ok(vec![SocketAddr::new("8.8.8.8".parse().unwrap(), port)])
    }
    async fn get(&self, _: &Url, _: &str, _: &[SocketAddr]) -> Result<Box<dyn Body>, ToolError> {
        if self.delay_redirect && self.hops.fetch_add(1, Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_secs(20)).await;
            return Ok(Box::new(Reply::redirect("/final")));
        }
        Ok(Box::new(PendingBody {
            started: self.started.clone(),
            dropped: self.dropped.clone(),
        }))
    }
}
fn body_transport(delay_redirect: bool) -> Arc<BodyTransport> {
    Arc::new(BodyTransport {
        started: Arc::new(tokio::sync::Notify::new()),
        dropped: Arc::new(AtomicBool::new(false)),
        hops: AtomicUsize::new(0),
        delay_redirect,
    })
}
#[tokio::test(start_paused = true)]
async fn response_body_and_delayed_redirect_share_total_deadline() {
    for delay_redirect in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let mut tool = tool(root.path());
        let transport = body_transport(delay_redirect);
        tool.transport = transport.clone();
        let start = tokio::time::Instant::now();
        let result = tool
            .execute_owned_context(
                support::context(&tool.options.root, CancellationToken::new()),
                json!({"url":"https://example.com/"}),
            )
            .await
            .unwrap()
            .json_value()
            .unwrap();
        assert_eq!(result["error"]["category"], "timeout");
        assert_eq!(start.elapsed(), TIMEOUT);
        assert!(transport.dropped.load(Ordering::SeqCst));
        let _permit = acquire_read(
            &tool.options.capacity,
            &tool.options.limits,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    }
}
#[tokio::test]
async fn cancellation_drops_streaming_body_and_recovers_capacity() {
    let root = tempfile::tempdir().unwrap();
    let mut tool = tool(root.path());
    let transport = body_transport(false);
    tool.transport = transport.clone();
    let cancel = CancellationToken::new();
    let work = tool.execute_owned_context(
        support::context(&tool.options.root, cancel.clone()),
        json!({"url":"https://example.com/"}),
    );
    tokio::pin!(work);
    tokio::select! { biased;
        result = &mut work => panic!("unexpected result {result:?}"),
        _ = transport.started.notified() => {}
    }
    cancel.cancel();
    assert!(work.await.is_err());
    assert!(transport.dropped.load(Ordering::SeqCst));
    let _permit = acquire_read(
        &tool.options.capacity,
        &tool.options.limits,
        &CancellationToken::new(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn special_purpose_dns_and_prefix_boundaries() {
    for ip in [
        "3fff::1",
        "3fff:fff:ffff:ffff:ffff:ffff:ffff:ffff",
        "192.88.99.2",
        "::ffff:192.88.99.2",
    ] {
        let mock = Mock {
            resolutions: Mutex::new(VecDeque::from([vec![SocketAddr::new(
                ip.parse().unwrap(),
                443,
            )]])),
            ..Mock::default()
        };
        assert_eq!(
            fetch(&mock, "https://example.com/")
                .await
                .unwrap_err()
                .category,
            "validation",
            "{ip}"
        );
        assert!(mock.connections.lock().unwrap().is_empty());
    }
    for ip in ["3fff:1000::", "192.88.98.255", "192.88.100.0"] {
        UrlFetchPolicy::default()
            .check_ip(ip.parse().unwrap())
            .unwrap();
    }
}

use support::ExecuteOwned as _;
