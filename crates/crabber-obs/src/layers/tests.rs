use super::*;
use crate::{DatadogConfig, DatadogObserver};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::JoinHandle,
    time::Duration,
};
use tracing_subscriber::prelude::*;

type Requests = Arc<Mutex<Vec<(String, Value, bool)>>>;
struct Intake {
    observer: DatadogObserver,
    health: Arc<LayerHealth>,
    requests: Requests,
    reject_logs: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
    server: Option<JoinHandle<()>>,
}
impl Intake {
    fn new(capacity: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let requests: Requests = Arc::default();
        let reject_logs = Arc::new(AtomicBool::new(false));
        let stopped = Arc::new(AtomicBool::new(false));
        let (seen, reject, stop) = (requests.clone(), reject_logs.clone(), stopped.clone());
        let server = std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        // BSD sockets inherit the listener's non-blocking mode.
                        stream.set_nonblocking(false).unwrap();
                        stream
                            .set_read_timeout(Some(Duration::from_secs(3)))
                            .unwrap();
                        let (path, body) = read_request(&mut stream);
                        let accepted = !(path.contains("/logs") && reject.load(Ordering::SeqCst));
                        seen.lock().unwrap().push((path, body, accepted));
                        stream.write_all(if accepted { b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n" } else { b"HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n" }).unwrap();
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(1));
                    }
                    Err(error) => panic!("listener failed: {error}"),
                }
            }
        });
        let mut config = DatadogConfig::from_lookup(&|key| {
            if key == "DD_API_KEY" {
                Ok("local-test".into())
            } else {
                Err(std::env::VarError::NotPresent)
            }
        })
        .unwrap();
        config.api_origin = Some(origin.clone());
        config.logs_origin = Some(origin);
        config.channel_capacity = capacity;
        config.batch_size = 1000;
        Self {
            observer: DatadogObserver::new(&config),
            health: Arc::default(),
            requests,
            reject_logs,
            stopped,
            server: Some(server),
        }
    }
    fn logs(&self) -> Vec<Value> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(path, _, accepted)| path.contains("/logs") && *accepted)
            .flat_map(|(_, body, _)| body.as_array().unwrap().clone())
            .collect()
    }
    fn metrics(&self, distribution: bool) -> Vec<Value> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(path, _, accepted)| {
                *accepted
                    && path.contains(if distribution {
                        "distribution_points"
                    } else {
                        "/api/v2/series"
                    })
            })
            .flat_map(|(_, body, _)| body["series"].as_array().unwrap().clone())
            .collect()
    }
    fn logs_layer(
        &self,
        policy: LogsPolicy,
    ) -> impl tracing_subscriber::Layer<tracing_subscriber::Registry> + use<> {
        let filter = LogsLayer::filter(&policy);
        LogsLayer::new(self.observer.clone(), policy, self.health.clone()).with_filter(filter)
    }
}
impl Drop for Intake {
    fn drop(&mut self) {
        self.observer.stop_now();
        self.stopped.store(true, Ordering::Relaxed);
        self.server.take().unwrap().join().unwrap();
    }
}
fn read_request(stream: &mut TcpStream) -> (String, Value) {
    let mut bytes = Vec::new();
    let mut buffer = [0; 8192];
    let end = loop {
        let n = stream.read(&mut buffer).unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&buffer[..n]);
        if let Some(pos) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let headers = String::from_utf8_lossy(&bytes[..end]).to_ascii_lowercase();
    let length: usize = headers
        .lines()
        .find_map(|line| line.strip_prefix("content-length: "))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    while bytes.len() < end + length {
        let n = stream.read(&mut buffer).unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&buffer[..n]);
    }
    let mut raw = String::new();
    if headers.contains("content-encoding: deflate") {
        flate2::read::ZlibDecoder::new(&bytes[end..end + length])
            .read_to_string(&mut raw)
            .unwrap();
    } else {
        flate2::read::GzDecoder::new(&bytes[end..end + length])
            .read_to_string(&mut raw)
            .unwrap();
    }
    assert!(!raw.contains("PROMPT_SECRET"));
    (
        headers.lines().next().unwrap().to_owned(),
        serde_json::from_str(&raw).unwrap(),
    )
}
fn has_tag(metric: &Value, tag: &str) -> bool {
    metric["tags"]
        .as_array()
        .unwrap()
        .iter()
        .any(|value| value == tag)
}

#[tokio::test]
async fn store_span_produces_distribution_with_finite_tags_and_survives_host_filter() {
    let intake = Intake::new(100);
    let layer = SpanMetricsLayer::new(intake.observer.clone(), intake.health.clone())
        .with_filter(SpanMetricsLayer::filter());
    // A host's independently filtered layer cannot disable our DEBUG transaction.
    let host = tracing_subscriber::fmt::layer()
        .with_writer(std::io::sink)
        .with_filter(tracing_subscriber::filter::LevelFilter::WARN);
    let subscriber = tracing_subscriber::registry().with(layer).with(host);
    tracing::subscriber::with_default(subscriber, || {
        let span = tracing::debug_span!(target: "crabber::session", "crabber.store.transaction", db.system = "sqlite", crabber.store.scope = "admission", outcome = tracing::field::Empty);
        span.record("outcome", "committed");
    });
    intake.observer.flush().await.unwrap();
    let metrics = intake.metrics(true);
    let metric = metrics
        .iter()
        .find(|metric| metric["metric"] == "crabber.store.transaction_ms")
        .unwrap();
    assert!(
        has_tag(metric, "db:sqlite")
            && has_tag(metric, "scope:admission")
            && has_tag(metric, "outcome:committed")
    );
    let mut tags = metric["tags"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect::<Vec<_>>();
    tags.sort_unstable();
    assert_eq!(
        tags,
        vec![
            "db:sqlite",
            "env:unknown",
            "outcome:committed",
            "scope:admission",
            "service:crabber",
            "version:unknown"
        ]
    );
    assert!(
        intake
            .metrics(false)
            .iter()
            .any(|metric| metric["metric"] == "crabber.store.transactions")
    );
}

#[tokio::test]
async fn provider_span_maps_status_class_allowlists_provider_and_runtime_spans_do_not_double_count()
{
    let intake = Intake::new(100);
    let subscriber = tracing_subscriber::registry().with(
        SpanMetricsLayer::new(intake.observer.clone(), intake.health.clone())
            .with_filter(SpanMetricsLayer::filter()),
    );
    tracing::subscriber::with_default(subscriber, || {
        drop(
            tracing::info_span!(target: "crabber::providers", "crabber.provider.request", provider.id = "unlisted", protocol = "openai", http.response.status_code = 503_u64, retry.attempt = 1_u64, outcome = "rejected", error.kind = "server"),
        );
        for name in 0..3 {
            match name {
                0 => drop(tracing::info_span!(target: "crabber::runtime", "crabber.run")),
                1 => drop(tracing::info_span!(target: "crabber::runtime", "crabber.model_call")),
                _ => drop(tracing::info_span!(target: "crabber::runtime", "crabber.tool_call")),
            }
        }
    });
    intake.observer.flush().await.unwrap();
    let metrics = intake.metrics(true);
    assert_eq!(metrics.len(), 1);
    assert!(
        has_tag(&metrics[0], "status_class:5xx")
            && has_tag(&metrics[0], "provider:overflow")
            && has_tag(&metrics[0], "retried:true")
    );
}

#[tokio::test]
async fn final_transport_failure_overrides_stale_401_but_http_rejection_keeps_class() {
    let intake = Intake::new(100);
    let subscriber = tracing_subscriber::registry().with(
        SpanMetricsLayer::new(intake.observer.clone(), intake.health.clone())
            .with_filter(SpanMetricsLayer::filter()),
    );
    tracing::subscriber::with_default(subscriber, || {
        for outcome in ["transport_error", "rejected"] {
            drop(
                tracing::info_span!(target: "crabber::providers", "crabber.provider.request",
                http.response.status_code = 401_u64, retry.attempt = 1_u64,
                outcome = outcome, error.kind = "transport"),
            );
        }
    });
    intake.observer.flush().await.unwrap();
    let metrics = intake.metrics(true);
    assert_eq!(metrics.len(), 2);
    assert!(
        metrics
            .iter()
            .any(|metric| has_tag(metric, "status_class:transport"))
    );
    assert!(
        metrics
            .iter()
            .any(|metric| has_tag(metric, "status_class:4xx"))
    );
    assert!(metrics.iter().all(|metric| has_tag(metric, "retried:true")));
}

#[tokio::test]
async fn owned_extension_close_emits_one_distribution_per_call_and_timeout_counter() {
    use crabber_extension::{Extension, ExtensionError, Registrar, Registry, Scope};
    struct Empty;
    #[async_trait::async_trait]
    impl Extension for Empty {
        fn id(&self) -> &'static str {
            "owned-metrics"
        }
        fn version(&self) -> &'static str {
            "1"
        }
        fn config_hash(&self) -> String {
            "{}".into()
        }
        async fn install(&self, _: &mut Registrar) -> Result<(), ExtensionError> {
            Ok(())
        }
    }
    let intake = Intake::new(100);
    let subscriber = tracing_subscriber::registry().with(
        SpanMetricsLayer::new(intake.observer.clone(), intake.health.clone())
            .with_filter(SpanMetricsLayer::filter()),
    );
    let _guard = tracing::subscriber::set_default(subscriber);
    let registry = Registry::new();
    let handle = registry
        .mount_owned(Arc::new(Empty), Scope::Global)
        .await
        .unwrap();
    let plan = registry.acquire(&crabber_core::SessionId::new());
    assert!(matches!(
        handle.close(Duration::ZERO).await,
        Err(ExtensionError::MountCloseTimeout { .. })
    ));
    plan.release();
    assert_eq!(
        handle
            .close(Duration::from_secs(5))
            .await
            .unwrap()
            .close_task()
            .joined,
        1
    );
    intake.observer.flush().await.unwrap();
    let distributions: Vec<_> = intake
        .metrics(true)
        .into_iter()
        .filter(|metric| metric["metric"] == "crabber.extension.close_ms")
        .collect();
    assert_eq!(distributions.len(), 2);
    assert!(
        distributions
            .iter()
            .any(|metric| has_tag(metric, "outcome:timed_out"))
    );
    assert!(
        distributions
            .iter()
            .any(|metric| has_tag(metric, "outcome:closed"))
    );
    let counters: Vec<_> = intake
        .metrics(false)
        .into_iter()
        .filter(|metric| metric["metric"] == "crabber.extension.close_timeouts")
        .collect();
    assert_eq!(counters.len(), 1);
    assert_eq!(counters[0]["points"][0]["value"], 1);
}

#[tokio::test]
async fn events_become_counters_once() {
    let intake = Intake::new(100);
    let subscriber = tracing_subscriber::registry().with(
        SpanMetricsLayer::new(intake.observer.clone(), intake.health.clone())
            .with_filter(SpanMetricsLayer::filter()),
    );
    tracing::subscriber::with_default(subscriber, || {
        tracing::error!(target: "crabber::runtime::heartbeat", { "error.kind" = "lease_lost", source = "heartbeat" }, "lease ownership lost");
        tracing::warn!(target: "crabber::runtime", { "error.kind" = "lease_lost", source = "run" }, "run attempt lost its lease");
        tracing::warn!(target: "crabber::runtime", { lagged = 7_u64 }, "event receiver lagged");
        tracing::warn!(target: "crabber::runtime::heartbeat", { "error.kind" = "store_conflict" }, "lease renew failed");
        tracing::warn!(target: "crabber::runtime", { "error.kind" = "provider_server", compaction_requested = false }, "model request failed, retry scheduled");
    });
    intake.observer.flush().await.unwrap();
    let metrics = intake.metrics(false);
    assert_eq!(
        metrics
            .iter()
            .filter(|m| m["metric"] == "crabber.lease.lost")
            .count(),
        1
    );
    let lag = metrics
        .iter()
        .find(|m| m["metric"] == "crabber.events.lagged")
        .unwrap();
    assert_eq!(lag["points"][0]["value"], 7);
    assert!(
        metrics
            .iter()
            .any(|m| m["metric"] == "crabber.lease.renew_failures")
    );
    assert!(
        metrics
            .iter()
            .any(|m| m["metric"] == "crabber.model.retries")
    );
}

#[tokio::test]
async fn logs_allowlist_fields_messages_targets_and_lifecycle() {
    struct Sensitive;
    impl std::fmt::Debug for Sensitive {
        fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            panic!("unlisted fields must never be formatted");
        }
    }
    let intake = Intake::new(100);
    let subscriber = tracing_subscriber::registry().with(intake.logs_layer(LogsPolicy::default()));
    tracing::subscriber::with_default(subscriber, || {
        tracing::info!(target: "crabber::runtime", { "session.id" = "session", "skip.reason" = "store", "retry.delay_ms" = 200_u64, prompt = "PROMPT_SECRET" }, "event receiver lagged");
        tracing::error!(target: "crabber_obs", "exporter secret");
        tracing::error!(target: "crabber::providers", "formatted PROMPT_SECRET");
        tracing::info!(target: "crabber::runtime::lifecycle", "run started");
        tracing::info!(target: "crabber::runtime", { payload = ?Sensitive }, "permission decided");
        tracing::warn!(target: "crabber::wasm::guest", { "guest.text" = "GUEST_SECRET" }, "guest log");
    });
    intake.observer.flush().await.unwrap();
    let logs = intake.logs();
    assert_eq!(logs.len(), 2);
    assert_eq!(logs[0]["session.id"], "session");
    assert_eq!(logs[0]["skip.reason"], "store");
    assert_eq!(logs[0]["retry.delay_ms"], 200);
    assert_eq!(logs[0]["crabber.source"], "layer");
    assert_eq!(logs[0]["redacted_count"], 1);
    assert_eq!(logs[1]["message"], "unlisted message");
    assert_eq!(intake.health.snapshot().messages_unlisted, 1);
}

#[tokio::test]
async fn logs_carry_both_trace_conventions_and_opt_in_bounded_guest_text() {
    let intake = Intake::new(100);
    let subscriber = tracing_subscriber::registry().with(intake.logs_layer(LogsPolicy {
        guest_text: true,
        min_level: tracing::Level::WARN,
    }));
    tracing::subscriber::with_default(subscriber, || {
        let span = tracing::info_span!(target: "crabber::runtime", "crabber.run", host.trace_id = "0000000000000000000000000000002a", host.span_id = "000000000000002b", session.id = "session");
        span.in_scope(|| tracing::warn!(target: "crabber::wasm::guest", { "guest.text" = %"g".repeat(1000) }, "guest log"));
        let span = tracing::info_span!(target: "crabber::runtime", "crabber.run", host.trace_id = "000000000000002c", host.span_id = "000000000000002d");
        span.in_scope(|| {
            tracing::info!(target: "crabber::runtime", "event receiver lagged");
            tracing::warn!(target: "crabber::runtime", "lease renew failed");
        });
    });
    intake.observer.flush().await.unwrap();
    let logs = intake.logs();
    assert_eq!(
        logs.len(),
        2,
        "INFO events remain filtered under WARN policy"
    );
    assert_eq!(logs[0]["trace_id"], "0000000000000000000000000000002a");
    assert_eq!(logs[0]["span_id"], "000000000000002b");
    assert_eq!(logs[0]["dd.trace_id"], "42");
    assert_eq!(logs[0]["dd.span_id"], "43");
    assert_eq!(logs[0]["guest.text"].as_str().unwrap().len(), 256);
    assert_eq!(logs[1]["trace_id"], "0000000000000000000000000000002c");
}

#[tokio::test]
async fn overflow_drops_are_counted_in_health() {
    let intake = Intake::new(1);
    let subscriber = tracing_subscriber::registry().with(intake.logs_layer(LogsPolicy::default()));
    let started = std::time::Instant::now();
    tracing::subscriber::with_default(subscriber, || {
        for _ in 0..100 {
            tracing::warn!(target: "crabber::runtime", "lease renew failed");
        }
    });
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(intake.observer.health().dropped > 0 && intake.health.snapshot().logs_dropped > 0);
}

#[tokio::test]
async fn pending_layer_log_survives_failed_logs_stage_and_recovery() {
    let intake = Intake::new(100);
    intake.reject_logs.store(true, Ordering::SeqCst);
    let subscriber = tracing_subscriber::registry().with(intake.logs_layer(LogsPolicy::default()));
    tracing::subscriber::with_default(subscriber, || {
        tracing::warn!(target: "crabber::runtime", "lease renew failed");
    });
    assert!(intake.observer.flush().await.is_err());
    assert_eq!(intake.observer.health().pending_depth, 1);
    assert_eq!(intake.logs(), Vec::<Value>::new());
    intake.reject_logs.store(false, Ordering::SeqCst);
    intake.observer.flush().await.unwrap();
    assert_eq!(intake.logs().len(), 1);
    assert_eq!(intake.logs()[0]["message"], "lease renew failed");
    assert_eq!(intake.observer.health().pending_depth, 0);
    assert_eq!(
        intake
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(path, _, _)| path.contains("/api/v2/series"))
            .count(),
        1
    );
}
