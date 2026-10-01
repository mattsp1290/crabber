//! Credential-free public host adoption; shared with the external consumer.
use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, EventKind, EventRecord, ExtensionError, MonotonicClock, Observer,
    OperationKind, OperationalObservation, PermissionDecision, Selection, StaticPolicy,
    StreamDelta, TerminalReason, ToolDefinition, ToolExecutor,
    core::{ToolCallId, ToolInfo},
    obs::{DatadogConfig, DatadogObserver, MetricDimensions, WorkerStatus},
    providers::{DeltaStream, ModelRequest, ProviderError, ProviderErrorKind, Resolver, Streamer},
};
use futures::StreamExt;
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    io::Read,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU16, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Notify,
};
#[derive(Default)]
struct Clock(AtomicU64);
impl MonotonicClock for Clock {
    fn now(&self) -> Duration {
        Duration::from_millis(self.0.load(Ordering::SeqCst))
    }
}
#[derive(Default)]
struct Capture {
    values: Mutex<Vec<OperationalObservation>>,
    text: Notify,
}
impl Observer for Capture {
    fn emit(&self, event: &EventRecord) {
        if event.kind == EventKind::TextDelta {
            self.text.notify_one();
        }
    }
    fn operational_completed(&self, value: &OperationalObservation) {
        self.values.lock().unwrap().push(value.clone());
    }
}
#[derive(Clone, Default)]
struct Script {
    startup_error: bool,
    setup_block: bool,
    end_block: bool,
    deltas: Vec<(u64, StreamDelta)>,
}
#[derive(Clone)]
struct Provider {
    clock: Arc<Clock>,
    scripts: Arc<Mutex<VecDeque<Script>>>,
    entered: Arc<Notify>,
}
impl Provider {
    fn new(clock: Arc<Clock>, scripts: Vec<Script>) -> Self {
        Self {
            clock,
            scripts: Arc::new(Mutex::new(scripts.into())),
            entered: Arc::new(Notify::new()),
        }
    }
}
fn error() -> ProviderError {
    ProviderError {
        kind: ProviderErrorKind::Server,
        message: "SECRET response credential header".into(),
        retryable: false,
    }
}
#[async_trait]
impl Resolver for Provider {
    async fn resolve(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        Ok(Arc::new(self.clone()))
    }
}
#[async_trait]
impl Streamer for Provider {
    async fn stream(&self, _: ModelRequest) -> Result<DeltaStream, ProviderError> {
        let script = self.scripts.lock().unwrap().pop_front().unwrap();
        self.clock.0.fetch_add(10, Ordering::SeqCst);
        self.entered.notify_one();
        if script.setup_block {
            futures::future::pending::<()>().await;
        }
        if script.startup_error {
            return Err(error());
        }
        let clock = self.clock.clone();
        let stream = futures::stream::iter(script.deltas).map(move |(increment, delta)| {
            clock.0.fetch_add(increment, Ordering::SeqCst);
            delta
        });
        if script.end_block {
            Ok(Box::pin(stream.chain(futures::stream::pending())))
        } else {
            Ok(Box::pin(stream))
        }
    }
}
struct Echo {
    clock: Arc<Clock>,
    fail: bool,
}
#[async_trait]
impl ToolExecutor for Echo {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        self.clock.0.fetch_add(15, Ordering::SeqCst);
        if self.fail {
            Err(ExtensionError::Plan("SECRET tool result".into()))
        } else {
            Ok(json!({"result":"SECRET result"}))
        }
    }
}
fn selection() -> Selection {
    Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    }
}
fn text() -> Script {
    Script {
        deltas: vec![
            (5, StreamDelta::TextDelta(String::new())),
            (5, StreamDelta::ReasoningDelta("SECRET reasoning".into())),
            (10, StreamDelta::TextDelta("SECRET token".into())),
            (10, StreamDelta::TextDelta("SECRET second".into())),
            (10, StreamDelta::Completed),
        ],
        ..Script::default()
    }
}
fn tool_script() -> Script {
    let id = ToolCallId::new();
    Script {
        deltas: vec![
            (
                0,
                StreamDelta::ToolCallStart {
                    call_id: id.clone(),
                    name: "echo".into(),
                },
            ),
            (
                0,
                StreamDelta::ToolCallArgsDelta {
                    call_id: id.clone(),
                    text: r#"{"argument":"SECRET argument"}"#.into(),
                },
            ),
            (0, StreamDelta::ToolCallDone { call_id: id }),
            (0, StreamDelta::Completed),
        ],
        ..Script::default()
    }
}
fn agent(
    provider: Provider,
    capture: Arc<Capture>,
    second: Arc<Capture>,
    export: Option<&DatadogObserver>,
    fail: bool,
) -> Agent {
    let clock = provider.clock.clone();
    let mut builder = Agent::builder()
        .memory()
        .provider(Arc::new(provider))
        .monotonic_clock(clock.clone())
        .config(AgentConfig::new(selection()))
        .observer(capture)
        .observer(second)
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "echo".into(),
                description: "native fixture".into(),
                parameters: json!({"type":"object"}),
                retry_safe: true,
                required_permissions: vec![],
            },
            executor: Arc::new(Echo { clock, fail }),
        }));
    if let Some(export) = export {
        builder = builder.observer(Arc::new(export.clone()));
    }
    builder.build().unwrap()
}
async fn complete(agent: &Agent, ok: bool) -> Vec<Arc<EventRecord>> {
    let mut handle = agent.prompt(None, "SECRET prompt").await.unwrap();
    let mut receiver = handle.events();
    let mut events = Vec::new();
    while let Some(event) = receiver.recv().await.unwrap() {
        events.push(event);
    }
    assert_eq!(handle.done().await.is_ok(), ok);
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == EventKind::RunSettled)
            .count(),
        1
    );
    events
}
fn values(capture: &Capture) -> Vec<OperationalObservation> {
    capture.values.lock().unwrap().clone()
}
#[derive(Clone)]
struct Request {
    path: String,
    body: Value,
    accepted: bool,
}
struct Intake {
    origin: String,
    requests: Arc<Mutex<Vec<Request>>>,
    status: Arc<AtomicU16>,
    reject_logs_once: Arc<AtomicU16>,
    task: tokio::task::JoinHandle<()>,
}
impl Intake {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let status = Arc::new(AtomicU16::new(202));
        let reject_logs_once = Arc::new(AtomicU16::new(0));
        let (records, code, reject) = (requests.clone(), status.clone(), reject_logs_once.clone());
        let task = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let (records, code, reject) = (records.clone(), code.clone(), reject.clone());
                tokio::spawn(async move {
                    let mut bytes = Vec::new();
                    let header_end = loop {
                        let mut chunk = [0; 4096];
                        let n = socket.read(&mut chunk).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        bytes.extend_from_slice(&chunk[..n]);
                        if let Some(pos) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                            break pos + 4;
                        }
                    };
                    let headers = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
                    let length: usize = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse().unwrap())
                        })
                        .unwrap();
                    while bytes.len() < header_end + length {
                        let mut chunk = [0; 4096];
                        let n = socket.read(&mut chunk).await.unwrap();
                        if n == 0 {
                            return;
                        }
                        bytes.extend_from_slice(&chunk[..n]);
                    }
                    let payload = &bytes[header_end..header_end + length];
                    let mut decoded = Vec::new();
                    if headers
                        .to_ascii_lowercase()
                        .contains("content-encoding: deflate")
                    {
                        flate2::read::ZlibDecoder::new(payload)
                            .read_to_end(&mut decoded)
                            .unwrap();
                    } else if headers
                        .to_ascii_lowercase()
                        .contains("content-encoding: gzip")
                    {
                        flate2::read::GzDecoder::new(payload)
                            .read_to_end(&mut decoded)
                            .unwrap();
                    } else {
                        decoded.extend_from_slice(payload);
                    }
                    let path = headers
                        .lines()
                        .next()
                        .unwrap()
                        .split_whitespace()
                        .nth(1)
                        .unwrap()
                        .to_owned();
                    let mut status = code.load(Ordering::SeqCst);
                    if path == "/api/v2/logs" && reject.swap(0, Ordering::SeqCst) != 0 {
                        status = 400;
                    }
                    records.lock().unwrap().push(Request {
                        path,
                        body: serde_json::from_slice(&decoded).unwrap(),
                        accepted: status == 202,
                    });
                    let body = if status == 202 {
                        ""
                    } else {
                        "SECRET response body URL credential"
                    };
                    let response = format!(
                        "HTTP/1.1 {status} fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });
        Self {
            origin,
            requests,
            status,
            reject_logs_once,
            task,
        }
    }
    fn config(&self) -> DatadogConfig {
        let mut config = DatadogConfig::from_lookup(&|name| {
            if name == "DD_API_KEY" {
                Ok("SECRET credential".into())
            } else {
                Err(std::env::VarError::NotPresent)
            }
        })
        .unwrap();
        config.api_origin = Some(self.origin.clone());
        config.logs_origin = Some(self.origin.clone());
        config.metric_dimensions = MetricDimensions {
            providers: vec!["fake".into()],
            models: vec!["scripted".into()],
            tools: vec!["echo".into()],
        };
        config.timeout = Duration::from_secs(2);
        config.batch_size = 1000;
        config.channel_capacity = 4096;
        config
    }
}
impl Drop for Intake {
    fn drop(&mut self) {
        self.task.abort();
    }
}
#[allow(clippy::too_many_lines)] // Outcome assertions stay beside their real runtime fixture.
async fn terminals(export: &DatadogObserver) -> Vec<OperationalObservation> {
    let mut all = Vec::new();
    for (script, reason, first, elapsed) in [
        (
            Script {
                startup_error: true,
                ..Script::default()
            },
            TerminalReason::ProviderError,
            None,
            10,
        ),
        (
            Script {
                deltas: vec![
                    (5, StreamDelta::ReasoningDelta("SECRET reasoning".into())),
                    (5, StreamDelta::Error(error())),
                ],
                ..Script::default()
            },
            TerminalReason::ProviderError,
            None,
            20,
        ),
        (
            Script {
                deltas: vec![
                    (10, StreamDelta::TextDelta("SECRET failing token".into())),
                    (10, StreamDelta::Error(error())),
                ],
                ..Script::default()
            },
            TerminalReason::ProviderError,
            Some(20),
            30,
        ),
        (text(), TerminalReason::Success, Some(30), 50),
        (
            Script {
                deltas: vec![
                    (10, StreamDelta::ReasoningDelta("SECRET reasoning".into())),
                    (20, StreamDelta::Completed),
                ],
                ..Script::default()
            },
            TerminalReason::Success,
            None,
            40,
        ),
        (
            Script {
                deltas: vec![(10, StreamDelta::Completed)],
                ..Script::default()
            },
            TerminalReason::ProviderError,
            None,
            20,
        ),
    ] {
        let clock = Arc::new(Clock::default());
        let capture = Arc::new(Capture::default());
        let second = Arc::new(Capture::default());
        let host = agent(
            Provider::new(clock, vec![script]),
            capture.clone(),
            second.clone(),
            Some(export),
            false,
        );
        assert_eq!(host.export_health(), None);
        complete(&host, reason == TerminalReason::Success).await;
        let observed = values(&capture);
        assert_eq!(observed, values(&second));
        assert_eq!(observed.len(), 2);
        assert!(
            observed
                .iter()
                .all(|v| v.reason == reason && v.elapsed == Duration::from_millis(elapsed))
        );
        assert_eq!(observed[0].first_token, first.map(Duration::from_millis));
        assert_eq!(observed[1].kind, OperationKind::Run);
        all.extend(observed);
    }
    for fail in [false, true] {
        let capture = Arc::new(Capture::default());
        let second = Arc::new(Capture::default());
        let host = agent(
            Provider::new(Arc::new(Clock::default()), vec![tool_script(), text()]),
            capture.clone(),
            second.clone(),
            Some(export),
            fail,
        );
        let events = complete(&host, true).await;
        assert_eq!(
            events
                .iter()
                .filter(|e| e.kind == EventKind::ToolCallSettled)
                .count(),
            1
        );
        assert_eq!(
            events
                .iter()
                .filter(|e| e.kind == EventKind::TextDelta)
                .count(),
            3
        );
        let observed = values(&capture);
        assert_eq!(observed, values(&second));
        assert_eq!(observed.len(), 4);
        assert_eq!(observed[0].elapsed, Duration::from_millis(10));
        assert_eq!(observed[0].first_token, None);
        assert_eq!(observed[1].elapsed, Duration::from_millis(15));
        assert_eq!(
            observed[1].reason,
            if fail {
                TerminalReason::ToolError
            } else {
                TerminalReason::Success
            }
        );
        assert_eq!(observed[2].first_token, Some(Duration::from_millis(30)));
        assert_eq!(observed[3].reason, TerminalReason::Success);
        assert_eq!(observed[3].elapsed, Duration::from_millis(75));
        all.extend(observed);
    }
    for setup_block in [true, false] {
        let capture = Arc::new(Capture::default());
        let second = Arc::new(Capture::default());
        let provider = Provider::new(
            Arc::new(Clock::default()),
            vec![Script {
                setup_block,
                end_block: true,
                deltas: vec![(10, StreamDelta::TextDelta("SECRET partial".into()))],
                ..Script::default()
            }],
        );
        let entered = provider.entered.clone();
        let host = agent(
            provider,
            capture.clone(),
            second.clone(),
            Some(export),
            false,
        );
        let handle = host.prompt(None, "SECRET prompt").await.unwrap();
        if setup_block {
            entered.notified().await;
        } else {
            capture.text.notified().await;
        }
        handle.interrupt();
        assert_eq!(
            handle.done().await.unwrap().status,
            crabber::core::RunStatus::Interrupted
        );
        let observed = values(&capture);
        assert_eq!(observed, values(&second));
        assert_eq!(observed.len(), 2);
        assert!(
            observed
                .iter()
                .all(|v| v.reason == TerminalReason::Cancelled)
        );
        assert_eq!(
            observed[0].first_token,
            if setup_block {
                None
            } else {
                Some(Duration::from_millis(20))
            }
        );
        all.extend(observed);
    }
    all
}
async fn lease() -> Vec<OperationalObservation> {
    use crabber::{
        core::ManualClock,
        extension::StaticPlanProvider,
        runtime::{Orchestrator, Request},
        session::{MemoryStore, Store},
    };
    tokio::time::pause();
    let now = time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let wall = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(wall.clone()));
    let capture = Arc::new(Capture::default());
    let provider = Provider::new(
        Arc::new(Clock::default()),
        vec![Script {
            setup_block: true,
            ..Script::default()
        }],
    );
    let entered = provider.entered.clone();
    let runtime = Orchestrator::builder()
        .store(store.clone())
        .clock(wall.clone())
        .monotonic_clock(provider.clock.clone())
        .resolver(Arc::new(provider))
        .plan_provider(Arc::new(StaticPlanProvider::new(vec![], vec![])))
        .observer(capture.clone())
        .heartbeat_interval(Duration::from_secs(1))
        .build()
        .unwrap();
    let handle = runtime
        .start(Request {
            session_id: None,
            workspace_id: "fixture".into(),
            directory: ".".into(),
            title: "fixture".into(),
            text: "SECRET prompt".into(),
            selection: selection(),
            system_prompt: None,
        })
        .await
        .unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    entered.notified().await;
    wall.set(now + time::Duration::seconds(31));
    let replacement = store.claim_expired_run(&run, "replacement").await.unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(matches!(
        handle.done().await,
        Err(crabber::RuntimeError::LeaseLost)
    ));
    let observed = values(&capture);
    assert_eq!(observed.len(), 2);
    assert!(
        observed
            .iter()
            .all(|v| v.reason == TerminalReason::LeaseLost && v.first_token.is_none())
    );
    assert!(
        store
            .list_events(&session, None, 100)
            .await
            .unwrap()
            .iter()
            .all(|e| e.kind != EventKind::RunSettled)
    );
    assert_eq!(
        store.get_run(&run).await.unwrap().unwrap().claim_token,
        replacement.claim_token
    );
    tokio::time::resume();
    observed
}
fn sample_key(value: &OperationalObservation) -> &'static str {
    match value.kind {
        OperationKind::Run => "crabber.run.elapsed_ms",
        OperationKind::Model { .. } => "crabber.model.elapsed_ms",
        OperationKind::Tool { .. } => "crabber.tool.elapsed_ms",
    }
}
fn captured_samples(requests: &[Request]) -> Vec<(String, u64)> {
    let mut samples = Vec::new();

    for request in requests
        .iter()
        .filter(|r| r.accepted && r.path == "/api/v1/distribution_points")
    {
        for series in request.body["series"].as_array().unwrap() {
            assert!(series.get("type").is_none());
            for point in series["points"].as_array().unwrap() {
                for sample in point[1].as_array().unwrap() {
                    let number = sample.as_f64().unwrap();
                    assert!(number.is_finite() && number >= 0.0);
                    samples.push((
                        series["metric"].as_str().unwrap().into(),
                        number.to_string().parse().unwrap(),
                    ));
                }
            }
        }
    }
    samples.sort();
    samples
}
fn expected_samples(observed: &[OperationalObservation]) -> Vec<(String, u64)> {
    let mut expected = Vec::new();
    for value in observed {
        expected.push((
            sample_key(value).into(),
            u64::try_from(value.elapsed.as_millis()).unwrap(),
        ));
        if let Some(first) = value.first_token {
            expected.push((
                "crabber.model.first_token_ms".into(),
                u64::try_from(first.as_millis()).unwrap(),
            ));
        }
    }
    expected.sort();
    expected
}
async fn stopped(observer: &DatadogObserver) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while observer.health().worker_status != WorkerStatus::Stopped {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
/// Runs assertions and prints only safe counts. No credentials or external services.
#[allow(clippy::too_many_lines)] // Verify all captured signals against the composed runtime observations.
pub async fn run() {
    // No network tasks exist during the deliberately paused fencing fixture.
    let fenced = lease().await;
    let intake = Intake::new().await;
    let export = DatadogObserver::new(&intake.config());
    assert_eq!(export.health().last_success_unix_seconds, None);
    tokio::time::pause();
    let mut observed = terminals(&export).await;
    tokio::time::resume();
    // Forward genuine local fence callbacks after returning to real-time IO.
    for value in &fenced {
        export.operational_completed(value);
    }
    observed.extend(fenced);
    export.flush().await.unwrap();
    let requests = intake.requests.lock().unwrap().clone();
    validate_requests(&requests);
    assert_eq!(captured_samples(&requests), expected_samples(&observed));
    let spans: Vec<_> = requests
        .iter()
        .filter(|r| r.accepted && r.path == "/api/v2/llmobs")
        .flat_map(|r| r.body.as_array().unwrap())
        .flat_map(|envelope| envelope["spans"].as_array().unwrap())
        .collect();
    let runs = observed
        .iter()
        .filter(|v| v.kind == OperationKind::Run)
        .count();
    assert_eq!(
        spans
            .iter()
            .filter(|span| span["meta"]["kind"] == "agent")
            .count(),
        runs - 1
    );
    assert_eq!(
        spans
            .iter()
            .filter(|span| span["meta"]["kind"] == "workflow")
            .count(),
        runs - 1
    );
    assert_eq!(
        spans
            .iter()
            .filter(|span| span["meta"]["kind"] == "tool")
            .count(),
        2
    );
    assert_eq!(
        spans
            .iter()
            .filter(|span| span["meta"]["kind"] == "llm")
            .count(),
        observed
            .iter()
            .filter(|v| matches!(v.kind, OperationKind::Model { .. })
                && !matches!(
                    v.reason,
                    TerminalReason::Cancelled | TerminalReason::LeaseLost
                ))
            .count()
    );
    let counts = requests
        .iter()
        .filter(|r| r.accepted && r.path == "/api/v2/series")
        .flat_map(|r| r.body["series"].as_array().unwrap())
        .filter(|series| series["metric"] == "crabber.run.count")
        .count();
    assert_eq!(counts, runs - 1);
    let logs = requests
        .iter()
        .filter(|r| r.accepted && r.path == "/api/v2/logs")
        .flat_map(|r| r.body.as_array().unwrap())
        .filter(|log| log["message"] == "run settled")
        .count();
    assert_eq!(logs, runs - 1);
    assert!(requests.iter().any(|r| r.path == "/api/v2/series"));
    assert!(requests.iter().any(|r| r.path == "/api/v2/logs"));
    assert!(
        !serde_json::to_string(&requests.iter().map(|r| &r.body).collect::<Vec<_>>())
            .unwrap()
            .contains("SECRET")
    );
    assert!(!format!("{observed:?}").contains("SECRET"));
    let health = export.health();
    assert!(health.accepted > 0);
    assert_eq!(health.dropped, 0);
    assert_eq!(health.queue_depth, 0);
    assert_eq!(health.pending_depth, 0);
    assert!(health.last_success_unix_seconds.is_some());
    export.shutdown().await.unwrap();
    stopped(&export).await;
    println!(
        "terminal_fixtures={} outcomes=success,provider_error,tool_error,cancelled,lease_lost distributions={} first_token_samples={} accepted={} dropped={} live_percentiles=UNVERIFIED",
        runs,
        expected_samples(&observed).len(),
        observed.iter().filter(|v| v.first_token.is_some()).count(),
        health.accepted,
        health.dropped
    );
    cardinality().await;
    partial_recovery().await;
    outage_overflow().await;
    control_bounds().await;
}
async fn partial_recovery() {
    let intake = Intake::new().await;
    intake.reject_logs_once.store(1, Ordering::SeqCst);
    let mut config = intake.config();
    config.max_payload_bytes = 2200;
    let export = DatadogObserver::new(&config);
    let capture = Arc::new(Capture::default());
    let host = agent(
        Provider::new(
            Arc::new(Clock::default()),
            vec![tool_script(), text(), text(), text()],
        ),
        capture.clone(),
        Arc::new(Capture::default()),
        Some(&export),
        false,
    );
    tokio::time::pause();
    for _ in 0..3 {
        complete(&host, true).await;
    }
    tokio::time::resume();
    assert!(export.flush().await.is_err());
    let before = export.health();
    assert_eq!(before.last_success_unix_seconds, None);
    assert!(before.failures > 0);
    assert!(before.pending_depth > 0);
    let first = intake.requests.lock().unwrap().clone();
    let stages: Vec<_> = first
        .iter()
        .filter(|r| r.accepted)
        .map(|r| (r.path.clone(), r.body.clone()))
        .collect();
    assert!(
        first
            .iter()
            .filter(|r| r.path == "/api/v2/llmobs" && r.accepted)
            .count()
            > 1
    );
    assert!(
        first
            .iter()
            .any(|r| r.path == "/api/v1/distribution_points" && r.accepted)
    );
    export.flush().await.unwrap();
    let after = intake.requests.lock().unwrap().clone();
    for (path, body) in stages {
        assert_eq!(
            after
                .iter()
                .filter(|r| r.accepted && r.path == path && r.body == body)
                .count(),
            1,
            "known accepted stage/chunk replayed"
        );
    }
    assert_eq!(
        captured_samples(&after),
        expected_samples(&values(&capture))
    );
    let recovered = export.health();
    assert_eq!(recovered.accepted, before.accepted);
    assert!(recovered.retries > before.retries);
    assert!(recovered.failures >= before.failures);
    assert_eq!(recovered.pending_depth, 0);
    assert!(recovered.last_success_unix_seconds.is_some());
    export.shutdown().await.unwrap();
    stopped(&export).await;
    println!(
        "partial_requests={} accepted_requests={} rejected_requests={} recovery=complete known_chunks_replayed=0",
        after.len(),
        after.iter().filter(|r| r.accepted).count(),
        after.iter().filter(|r| !r.accepted).count()
    );
}
async fn outage_overflow() {
    let intake = Intake::new().await;
    intake.status.store(503, Ordering::SeqCst);
    let capture = Arc::new(Capture::default());
    let host = Agent::builder()
        .memory()
        .provider(Arc::new(Provider::new(
            Arc::new(Clock::default()),
            vec![text()],
        )))
        .config(AgentConfig::new(selection()))
        .observer(capture.clone())
        .datadog(intake.config())
        .build()
        .unwrap();
    assert_eq!(
        host.export_health().unwrap().last_success_unix_seconds,
        None
    );
    let events = complete(&host, true).await;
    assert!(events.iter().any(|e| e.kind == EventKind::TextDelta));
    assert!(host.flush().await.is_err());
    let unavailable = host.export_health().unwrap();
    assert!(unavailable.accepted > 0);
    assert!(unavailable.failures > 0);
    assert!(unavailable.retries > 0);
    assert!(unavailable.pending_depth > 0);
    assert_eq!(unavailable.last_success_unix_seconds, None);
    assert_eq!(unavailable.worker_status, WorkerStatus::Running);
    intake.status.store(202, Ordering::SeqCst);
    host.flush().await.unwrap();
    let recovered = host.export_health().unwrap();
    assert_eq!(recovered.accepted, unavailable.accepted);
    assert_eq!(recovered.dropped, 0);
    assert_eq!(recovered.queue_depth, 0);
    assert_eq!(recovered.pending_depth, 0);
    assert!(recovered.last_success_unix_seconds.is_some());
    host.shutdown().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while host.export_health().unwrap().worker_status != WorkerStatus::Stopped {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let terminal = host.export_health().unwrap();
    assert_eq!(terminal.accepted, recovered.accepted);
    assert_eq!(terminal.dropped, 0);
    assert_eq!(terminal.queue_depth, 0);
    assert_eq!(terminal.pending_depth, 0);
    // Stress only uses captured genuine runtime measurements; main outcomes above execute runtime.
    let mut config = intake.config();
    config.channel_capacity = 2;
    config.batch_size = 2;
    intake.status.store(503, Ordering::SeqCst);
    let export = DatadogObserver::new(&config);
    let sample = values(&capture)[0].clone();
    // current-thread worker cannot poll until this bounded synchronous producer returns.
    for _ in 0..100 {
        export.operational_completed(&sample);
    }
    let overflow = export.health();
    assert_eq!(overflow.accepted, 2);
    assert_eq!(overflow.dropped, 98);
    assert_eq!(overflow.queue_depth, 2);
    assert_eq!(overflow.pending_depth, 0);
    assert!(export.flush().await.is_err());
    let retained = export.health();
    assert_eq!(retained.accepted, 2);
    assert_eq!(retained.dropped, 98);
    assert_eq!(retained.pending_depth, 2);
    assert!(retained.failures > 0);
    assert!(retained.retries > 0);
    assert_eq!(retained.last_success_unix_seconds, None);
    intake.status.store(202, Ordering::SeqCst);
    export.flush().await.unwrap();
    let drained = export.health();
    assert_eq!(drained.accepted, 2);
    assert_eq!(drained.dropped, 98);
    assert_eq!(drained.queue_depth, 0);
    assert_eq!(drained.pending_depth, 0);
    let mut expected = values(&capture);
    expected.extend([sample.clone(), sample]);
    assert_eq!(
        captured_samples(&intake.requests.lock().unwrap()),
        expected_samples(&expected)
    );
    export.shutdown().await.unwrap();
    stopped(&export).await;
    assert_eq!(export.health().worker_status, WorkerStatus::Stopped);
    println!(
        "outage_completion=independent failures={} retries={} overflow_accepted={} overflow_dropped={} recovery=complete",
        unavailable.failures, unavailable.retries, overflow.accepted, overflow.dropped
    );
}
async fn control_bounds() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let entered = Arc::new(Notify::new());
    let signal = entered.clone();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = [0; 4096];
        let _ = socket.read(&mut bytes).await.unwrap();
        signal.notify_one();
        futures::future::pending::<()>().await;
        drop(socket);
    });
    let intake = Intake::new().await;
    let mut config = intake.config();
    config.api_origin = Some(origin);
    config.timeout = Duration::from_secs(10);
    let export = DatadogObserver::new(&config);
    let capture = Arc::new(Capture::default());
    let host = agent(
        Provider::new(Arc::new(Clock::default()), vec![text()]),
        capture,
        Arc::new(Capture::default()),
        Some(&export),
        false,
    );
    complete(&host, true).await;
    let flushing = export.clone();
    let (start_tx, start_rx) = tokio::sync::oneshot::channel();
    let flush = tokio::spawn(async move {
        let start = tokio::time::Instant::now();
        start_tx.send(start).unwrap();
        let result = flushing.flush().await;
        (result, start.elapsed())
    });
    let flush_start = start_rx.await.unwrap();
    entered.notified().await; // Request is actually stalled before pausing virtual time.
    tokio::time::pause();
    let remaining = (flush_start + Duration::from_secs(10))
        .saturating_duration_since(tokio::time::Instant::now());
    tokio::time::advance(remaining).await;
    let (result, elapsed) = flush.await.unwrap();
    assert!(matches!(result, Err(crabber::obs::ExportError::Timeout)));
    assert!(
        elapsed <= Duration::from_secs(10) + Duration::from_millis(1),
        "flush elapsed: {elapsed:?}"
    );
    assert_eq!(export.health().worker_status, WorkerStatus::Running);
    assert_eq!(export.health().last_success_unix_seconds, None);
    // Synchronize actual control polling, rather than relying on a single yield.
    let started = Arc::new(Notify::new());
    let signal = started.clone();
    let shutting = export.clone();
    let shutdown = tokio::spawn(async move {
        let start = tokio::time::Instant::now();
        signal.notify_one();
        let result = shutting.shutdown().await;
        (result, start.elapsed())
    });
    started.notified().await;
    tokio::time::advance(Duration::from_secs(10)).await;
    let (result, elapsed) = shutdown.await.unwrap();
    assert!(result.is_err());
    assert!(
        elapsed <= Duration::from_secs(10) + Duration::from_millis(1),
        "shutdown elapsed: {elapsed:?}"
    );
    stopped(&export).await;
    let health = export.health();
    assert_eq!(health.queue_depth, 0);
    assert_eq!(health.pending_depth, 0);
    assert_eq!(health.worker_status, WorkerStatus::Stopped);
    assert!(health.dropped > 0);
    tokio::time::resume();
    server.abort();
    println!(
        "control_timeout_budget_seconds=10 timer_precision_milliseconds=1 flush_timeout_worker=running shutdown_worker=stopped outstanding_dropped={}",
        health.dropped
    );
}

fn validate_tags(series: &Value) {
    for tag in series["tags"].as_array().unwrap() {
        let tag = tag.as_str().unwrap();
        assert!(
            !["session", "run_id", "attempt", "trace", "span", "call_id"]
                .iter()
                .any(|id| tag.contains(id))
        );
        if let Some((name, value)) = tag.split_once(':') {
            match name {
                "provider" => assert!(matches!(value, "fake" | "overflow")),
                "model" => assert!(matches!(value, "scripted" | "overflow")),
                "tool" => assert!(matches!(value, "echo" | "overflow")),
                "purpose" => assert!(matches!(value, "turn" | "compaction")),
                "reason" => assert!(matches!(
                    value,
                    "success"
                        | "provider_error"
                        | "tool_error"
                        | "cancelled"
                        | "lease_lost"
                        | "paused"
                        | "runtime_error"
                )),
                _ => {}
            }
        }
    }
}

fn validate_requests(requests: &[Request]) {
    for request in requests.iter().filter(|r| {
        r.accepted
            && matches!(
                r.path.as_str(),
                "/api/v2/series" | "/api/v1/distribution_points"
            )
    }) {
        for series in request.body["series"].as_array().unwrap() {
            validate_tags(series);
        }
    }
}
async fn cardinality() {
    use std::collections::BTreeSet;
    let intake = Intake::new().await;
    let names: Vec<String> = (0..64).map(|n| format!("identity-{n}")).collect();
    let mut config = intake.config();
    config.metric_dimensions = MetricDimensions {
        providers: names.clone(),
        models: names.clone(),
        tools: names.clone(),
    };
    let export = DatadogObserver::new(&config);
    let capture = Arc::new(Capture::default());
    tokio::time::pause();
    for name in &names {
        let clock = Arc::new(Clock::default());
        let mut script = tool_script();
        for (_, delta) in &mut script.deltas {
            if let StreamDelta::ToolCallStart { name: tool, .. } = delta {
                tool.clone_from(name);
            }
        }
        let host = Agent::builder()
            .memory()
            .provider(Arc::new(Provider::new(clock.clone(), vec![script, text()])))
            .monotonic_clock(clock.clone())
            .config(AgentConfig::new(Selection {
                provider_id: name.clone(),
                model_id: name.clone(),
            }))
            .observer(capture.clone())
            .observer(Arc::new(export.clone()))
            .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
            .tool(Arc::new(ToolDefinition {
                info: ToolInfo {
                    name: name.clone(),
                    description: "native cardinality fixture".into(),
                    parameters: json!({"type":"object"}),
                    retry_safe: true,
                    required_permissions: vec![],
                },
                executor: Arc::new(Echo { clock, fail: false }),
            }))
            .build()
            .unwrap();
        complete(&host, true).await;
    }
    tokio::time::resume();
    export.flush().await.unwrap();
    let requests = intake.requests.lock().unwrap().clone();
    assert_eq!(
        captured_samples(&requests),
        expected_samples(&values(&capture))
    );
    let mut dimensions = [BTreeSet::new(), BTreeSet::new(), BTreeSet::new()];
    for request in requests.iter().filter(|r| {
        r.accepted
            && matches!(
                r.path.as_str(),
                "/api/v2/series" | "/api/v1/distribution_points"
            )
    }) {
        for series in request.body["series"].as_array().unwrap() {
            for tag in series["tags"].as_array().unwrap() {
                let tag = tag.as_str().unwrap();
                assert!(
                    !["session", "run_id", "attempt", "trace", "span", "call_id"]
                        .iter()
                        .any(|id| tag.contains(id))
                );
                if let Some((dimension, value)) = tag.split_once(':')
                    && let Some(index) = ["provider", "model", "tool"]
                        .iter()
                        .position(|name| *name == dimension)
                {
                    dimensions[index].insert(value.to_owned());
                }
            }
        }
    }
    let expected: BTreeSet<_> = names
        .iter()
        .take(32)
        .cloned()
        .chain(["overflow".to_owned()])
        .collect();
    assert!(dimensions.iter().all(|dimension| *dimension == expected));
    assert_eq!(export.health().dropped, 0);
    export.shutdown().await.unwrap();
    stopped(&export).await;
    println!(
        "runtime_identities_per_dimension=64 metric_values_per_dimension=33 allowlist_cap=32 overflow=verified"
    );
}
