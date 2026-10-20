use super::*;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use prost::Message as _;
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{Mutex, atomic::AtomicBool},
    thread::JoinHandle,
    time::Instant,
};
use tracing::Instrument as _;
use tracing_subscriber::prelude::*;

type CapturedRequest = (String, String, Vec<u8>);

struct Receiver {
    endpoint: String,
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}
impl Receiver {
    fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let stop = Arc::new(AtomicBool::new(false));
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captures = requests.clone();
        let stopped = stop.clone();
        let worker = std::thread::spawn(move || {
            while !stopped.load(Ordering::Relaxed) {
                let Ok((mut socket, _)) = listener.accept() else {
                    std::thread::sleep(Duration::from_millis(2));
                    continue;
                };
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut bytes = Vec::new();
                let mut chunk = [0; 8192];
                let header_end = loop {
                    let count = socket.read(&mut chunk).unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&chunk[..count]);
                    assert!(bytes.len() <= 1_048_576);
                    if let Some(index) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                        break index + 4;
                    }
                };
                let header = String::from_utf8_lossy(&bytes[..header_end]).to_string();
                let content_length: usize = header
                    .lines()
                    .find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap();
                assert!(content_length <= 1_048_576);
                while bytes.len() < header_end + content_length {
                    let count = socket.read(&mut chunk).unwrap();
                    assert!(count > 0);
                    bytes.extend_from_slice(&chunk[..count]);
                }
                let path = header
                    .lines()
                    .next()
                    .unwrap()
                    .split_whitespace()
                    .nth(1)
                    .unwrap()
                    .to_owned();
                let content_type = header
                    .lines()
                    .find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case("content-type")
                            .then(|| value.trim().to_owned())
                    })
                    .unwrap();
                captures.lock().unwrap().push((
                    path,
                    content_type,
                    bytes[header_end..header_end + content_length].to_vec(),
                ));
                socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .unwrap();
            }
        });
        Self {
            endpoint,
            requests,
            stop,
            worker: Some(worker),
        }
    }
    fn config(&self, protocol: OtlpProtocol) -> TelemetryConfig {
        let mut config =
            TelemetryConfig::from_lookup(&|_| Err(std::env::VarError::NotPresent)).unwrap();
        config.service = "telemetry-test".into();
        config.env = Some("test".into());
        config.version = Some("v1".into());
        config
            .resource_tags
            .push(("verify".into(), "loopback".into()));
        config.otlp = Some(OtlpConfig {
            traces_url: format!("{}/v1/traces", self.endpoint),
            protocol,
            timeout: Duration::from_millis(500),
            headers: vec![],
        });
        config
    }
    fn protobuf(&self) -> ExportTraceServiceRequest {
        let requests = self.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let (path, kind, body) = &requests[0];
        assert_eq!(path, "/v1/traces");
        assert_eq!(kind, "application/x-protobuf");
        ExportTraceServiceRequest::decode(body.as_slice()).unwrap()
    }
}
impl Drop for Receiver {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.worker.take().unwrap().join().unwrap();
    }
}

async fn emit_tree() -> crabber_core::TraceContext {
    async {
        let context = Telemetry::current_trace_context().unwrap();
        async {
            tracing::error!(error = "STATUS_SECRET", "EVENT_SECRET");
            async {}
                .instrument(tracing::info_span!(
                    "crabber.model_call",
                    purpose = "turn",
                    outcome = "completed"
                ))
                .await;
        }
        .instrument(tracing::info_span!(
            "crabber.run",
            outcome = "failed",
            otel.status_code = "ERROR",
            request.url = "URL_SECRET"
        ))
        .await;
        context
    }
    .instrument(tracing::info_span!(target: "host::test", "host.request", host.custom = "retained"))
    .await
}

#[tokio::test]
async fn exports_resource_attributes_and_span_tree_over_protobuf() {
    let receiver = Receiver::new();
    let telemetry = Telemetry::new(receiver.config(OtlpProtocol::HttpProtobuf)).unwrap();
    let _guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(telemetry.layer()));
    let context = emit_tree().await;
    telemetry.flush().await.unwrap();
    let request = receiver.protobuf();
    let resource = &request.resource_spans[0];
    let attrs = &resource.resource.as_ref().unwrap().attributes;
    for key in [
        "service.name",
        "deployment.environment.name",
        "deployment.environment",
        "service.version",
        "verify",
    ] {
        assert!(attrs.iter().any(|a| a.key == key));
    }
    let spans = &resource.scope_spans[0].spans;
    let host = spans.iter().find(|s| s.name == "host.request").unwrap();
    let run = spans.iter().find(|s| s.name == "crabber.run").unwrap();
    let model = spans
        .iter()
        .find(|s| s.name == "crabber.model_call")
        .unwrap();
    assert_eq!(run.parent_span_id, host.span_id);
    assert_eq!(model.parent_span_id, run.span_id);
    assert_eq!(run.trace_id, host.trace_id);
    assert_eq!(run.status.as_ref().unwrap().code, 2);
    assert!(host.attributes.iter().any(|a| a.key == "host.custom"));
    assert_eq!(context.trace_id(), hex(&host.trace_id));
    assert_eq!(context.span_id(), hex(&host.span_id));
    for span in spans.iter().filter(|s| s.name.starts_with("crabber.")) {
        assert!(
            span.attributes
                .iter()
                .all(|a| ALLOWED_ATTRIBUTES.contains(&a.key.as_str()))
        );
        assert_eq!(
            span.events,
            Vec::<opentelemetry_proto::tonic::trace::v1::span::Event>::new()
        );
    }
    assert_eq!(telemetry.health().attributes_scrubbed, 1);
    let serialized = format!("{request:?}");
    for sentinel in ["URL_SECRET", "STATUS_SECRET", "EVENT_SECRET"] {
        assert!(!serialized.contains(sentinel));
    }
    telemetry.shutdown().await.unwrap();
    telemetry.shutdown().await.unwrap();
}
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    bytes.iter().fold(
        String::with_capacity(bytes.len() * 2),
        |mut output, byte| {
            write!(&mut output, "{byte:02x}").unwrap();
            output
        },
    )
}

#[tokio::test]
async fn json_protocol_exports_same_tree() {
    let receiver = Receiver::new();
    let telemetry = Telemetry::new(receiver.config(OtlpProtocol::HttpJson)).unwrap();
    let _guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(telemetry.layer()));
    emit_tree().await;
    telemetry.shutdown().await.unwrap();
    let requests = receiver.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let (path, content_type, bytes) = &requests[0];
    assert_eq!(path, "/v1/traces");
    assert_eq!(content_type, "application/json");
    let body: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    let spans = body["resourceSpans"][0]["scopeSpans"][0]["spans"]
        .as_array()
        .unwrap();
    assert_eq!(spans.len(), 3);
    assert!(!String::from_utf8_lossy(bytes).contains("URL_SECRET"));
}

#[tokio::test]
async fn traces_endpoint_variants_post_to_v1_traces() {
    for explicit in [false, true] {
        let receiver = Receiver::new();
        let key = if explicit {
            "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
        } else {
            "OTEL_EXPORTER_OTLP_ENDPOINT"
        };
        let endpoint = if explicit {
            format!("{}/v1/traces", receiver.endpoint)
        } else {
            receiver.endpoint.clone()
        };
        let config = TelemetryConfig::from_lookup(&|name| {
            if name == key {
                Ok(endpoint.clone())
            } else {
                Err(std::env::VarError::NotPresent)
            }
        })
        .unwrap();
        let telemetry = Telemetry::new(config).unwrap();
        {
            let _guard = tracing::subscriber::set_default(
                tracing_subscriber::registry().with(telemetry.layer()),
            );
            tracing::info_span!("host.request").in_scope(|| {});
        }
        telemetry.shutdown().await.unwrap();
        assert_eq!(receiver.protobuf().resource_spans.len(), 1);
    }
}

#[tokio::test]
async fn host_span_exports_after_run_closes() {
    let receiver = Receiver::new();
    let telemetry = Telemetry::new(receiver.config(OtlpProtocol::HttpProtobuf)).unwrap();
    let _guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(telemetry.layer()));
    let host = tracing::info_span!("host.request");
    let run = host.in_scope(|| tracing::info_span!("crabber.run"));
    drop(host);
    tokio::time::sleep(Duration::from_millis(5)).await;
    run.in_scope(|| {});
    drop(run);
    telemetry.shutdown().await.unwrap();
    let request = receiver.protobuf();
    let spans = &request.resource_spans[0].scope_spans[0].spans;
    assert_eq!(spans[0].name, "crabber.run");
    assert_eq!(spans[1].name, "host.request");
    assert!(spans[1].end_time_unix_nano < spans[0].end_time_unix_nano);
}

#[tokio::test]
async fn shutdown_is_bounded_when_collector_is_unreachable() {
    let receiver = Receiver::new();
    let mut config = receiver.config(OtlpProtocol::HttpProtobuf);
    config.otlp.as_mut().unwrap().timeout = Duration::from_millis(200);
    drop(receiver);
    let telemetry = Telemetry::new(config).unwrap();
    {
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry().with(telemetry.layer()),
        );
        tracing::info_span!("host.request").in_scope(|| {});
    }
    let start = Instant::now();
    assert!(matches!(
        telemetry.shutdown().await,
        Err(TelemetryError::Otlp)
    ));
    assert!(start.elapsed() < Duration::from_millis(1200));
}

fn empty_config() -> TelemetryConfig {
    TelemetryConfig::from_lookup(&|_| Err(std::env::VarError::NotPresent)).unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn constructing_and_dropping_inside_multithread_tokio_does_not_panic() {
    check_drop();
}
#[tokio::test]
async fn drop_on_current_thread_is_immediate() {
    check_drop();
}
fn check_drop() {
    let receiver = Receiver::new();
    let mut config = receiver.config(OtlpProtocol::HttpProtobuf);
    config.datadog = crate::DatadogConfig::from_lookup(&|name| {
        if name == "DD_API_KEY" {
            Ok("credential-free".into())
        } else {
            Err(std::env::VarError::NotPresent)
        }
    });
    let telemetry = Telemetry::new(config).unwrap();
    let observer = telemetry.exporter().unwrap();
    let start = Instant::now();
    drop(telemetry);
    assert!(start.elapsed() < Duration::from_millis(100));
    assert_eq!(
        observer.health().worker_status,
        crate::WorkerStatus::Stopped
    );
}
#[test]
fn drop_after_current_thread_stops_driving_is_immediate() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let receiver = Receiver::new();
    let mut config = receiver.config(OtlpProtocol::HttpProtobuf);
    config.datadog = crate::DatadogConfig::from_lookup(&|name| {
        if name == "DD_API_KEY" {
            Ok("credential-free".into())
        } else {
            Err(std::env::VarError::NotPresent)
        }
    });
    let telemetry = runtime.block_on(async { Telemetry::new(config).unwrap() });
    let observer = telemetry.exporter().unwrap();
    let start = Instant::now();
    drop(telemetry);
    assert!(start.elapsed() < Duration::from_millis(100));
    assert_eq!(
        observer.health().worker_status,
        crate::WorkerStatus::Stopped
    );
}
#[tokio::test]
async fn shutdown_tolerates_observer_stopped_first() {
    let mut config = empty_config();
    config.datadog = crate::DatadogConfig::from_lookup(&|name| {
        if name == "DD_API_KEY" {
            Ok("credential-free".into())
        } else {
            Err(std::env::VarError::NotPresent)
        }
    });
    let telemetry = Telemetry::new(config).unwrap();
    telemetry.exporter().unwrap().shutdown().await.unwrap();
    telemetry.shutdown().await.unwrap();
    telemetry.shutdown().await.unwrap();
}

#[tokio::test]
async fn disabled_trace_signal_creates_no_provider_or_socket() {
    let receiver = Receiver::new();
    let mut config = receiver.config(OtlpProtocol::HttpProtobuf);
    config.signals.traces = false;
    let telemetry = Telemetry::new(config).unwrap();
    assert!(telemetry.provider.is_none());
    let _guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(telemetry.layer()));
    tracing::info_span!("host.request").in_scope(|| {
        assert!(Telemetry::current_trace_context().is_none());
    });
    telemetry.shutdown().await.unwrap();
    assert!(receiver.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn empty_bundle_does_not_filter_host_layers() {
    #[derive(Clone)]
    struct HostEvents(Arc<std::sync::atomic::AtomicU64>);
    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for HostEvents {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if event.metadata().target() == "host::neutral_test" {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    let telemetry = Telemetry::new(empty_config()).unwrap();
    let events = HostEvents(Arc::new(std::sync::atomic::AtomicU64::new(0)));
    let subscriber = tracing_subscriber::registry()
        .with(telemetry.layer())
        .with(events.clone());
    let _guard = tracing::subscriber::set_default(subscriber);
    tracing::info!(target: "host::neutral_test", "host info diagnostic");
    tracing::debug!(target: "host::neutral_test", "host debug diagnostic");
    assert_eq!(events.0.load(Ordering::Relaxed), 2);
    assert!(telemetry.exporter().is_none() && telemetry.provider.is_none());
    telemetry.shutdown().await.unwrap();
}
