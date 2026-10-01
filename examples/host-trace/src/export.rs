//! Credential-free real HTTP capture shared by embedding and facade tests.
use crabber::{
    TraceContext,
    obs::{DatadogConfig, DatadogObserver},
};
use flate2::read::{GzDecoder, ZlibDecoder};
use serde_json::Value;
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{Arc, Mutex},
    time::Duration,
};

pub struct ExportCapture {
    pub observer: DatadogObserver,
    pub live: Option<DatadogObserver>,
    requests: Arc<Mutex<Vec<Value>>>,
    stop: std::sync::mpsc::Sender<()>,
    server: std::thread::JoinHandle<()>,
}
impl ExportCapture {
    #[allow(clippy::too_many_lines)] // Keep the local mock transport lifecycle together.
    pub fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let (stop, stopped) = std::sync::mpsc::channel();
        let capture_file = std::env::var_os("CRABBER_TRACE_CAPTURE_FILE");
        let server = std::thread::spawn(move || {
            loop {
                if stopped.try_recv().is_ok() {
                    break;
                }
                let Ok((mut stream, _)) = listener.accept() else {
                    std::thread::sleep(Duration::from_millis(2));
                    continue;
                };
                stream
                    .set_read_timeout(Some(Duration::from_secs(5)))
                    .unwrap();
                let mut all = Vec::new();
                let mut buffer = [0; 8192];
                let (end, length, gzip, deflate) = loop {
                    let n = stream.read(&mut buffer).unwrap();
                    assert!(n > 0);
                    all.extend_from_slice(&buffer[..n]);
                    if let Some(pos) = all.windows(4).position(|w| w == b"\r\n\r\n") {
                        let head = String::from_utf8_lossy(&all[..pos]).to_ascii_lowercase();
                        let length: usize = head
                            .lines()
                            .find_map(|line| line.strip_prefix("content-length: "))
                            .unwrap()
                            .parse()
                            .unwrap();
                        if all.len() >= pos + 4 + length {
                            break (
                                pos + 4,
                                length,
                                head.contains("content-encoding: gzip"),
                                head.contains("content-encoding: deflate"),
                            );
                        }
                    }
                };
                let mut body = String::new();
                if gzip {
                    GzDecoder::new(&all[end..end + length])
                        .read_to_string(&mut body)
                        .unwrap();
                } else if deflate {
                    ZlibDecoder::new(&all[end..end + length])
                        .read_to_string(&mut body)
                        .unwrap();
                } else {
                    body = String::from_utf8(all[end..end + length].to_vec()).unwrap();
                }
                for secret in [
                    "PROMPT_SECRET",
                    "OUTPUT_SECRET",
                    "ARGUMENT_SECRET",
                    "PRIVATE_PATH_SECRET",
                    "CREDENTIAL_SECRET",
                ] {
                    assert!(!body.contains(secret));
                }
                let value: Value = serde_json::from_str(&body).unwrap();
                if value.pointer("/0/event_type").is_some() {
                    for envelope in value.as_array().unwrap() {
                        assert_eq!(envelope["_dd.stage"], "raw");
                        assert_eq!(
                            envelope["_dd.tracer_version"],
                            concat!("crabber-", env!("CARGO_PKG_VERSION"))
                        );
                        assert_eq!(envelope["event_type"], "span");
                        assert_eq!(envelope["spans"].as_array().unwrap().len(), 1);
                        assert!(
                            envelope["spans"][0]["tags"]
                                .as_array()
                                .unwrap()
                                .iter()
                                .any(|tag| tag == "language:rust")
                        );
                    }
                }
                captured.lock().unwrap().push(value);
                if let Some(path) = &capture_file {
                    std::fs::write(
                        path,
                        serde_json::to_vec(&*captured.lock().unwrap()).unwrap(),
                    )
                    .unwrap();
                }
                stream
                    .write_all(
                        b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .unwrap();
            }
        });
        let mut config = DatadogConfig::from_lookup(&|name| {
            if name == "DD_API_KEY" {
                Ok("credential-free-test".into())
            } else {
                Err(std::env::VarError::NotPresent)
            }
        })
        .unwrap();
        config.api_origin = Some(origin.clone());
        config.logs_origin = Some(origin);
        config.timeout = Duration::from_secs(5);
        let live = if std::env::var("CRABBER_TRACE_LIVE").as_deref() == Ok("1") {
            let mut config = DatadogConfig::from_env().expect("approved DD_API_KEY required");
            let marker =
                std::env::var("CRABBER_TRACE_MARKER").expect("CRABBER_TRACE_MARKER required");
            config.tags.push(format!("verify:{marker}"));
            Some(DatadogObserver::new(&config))
        } else {
            None
        };
        Self {
            live,
            observer: DatadogObserver::new(&config),
            requests,
            stop,
            server,
        }
    }
    pub async fn raw(self) -> Vec<Value> {
        self.observer.shutdown().await.unwrap();
        if let Some(live) = &self.live {
            live.shutdown().await.unwrap();
            println!("live_intake=accepted live_correlation=UNVERIFIED");
        }
        self.stop.send(()).unwrap();
        self.server.join().unwrap();
        self.requests.lock().unwrap().clone()
    }
    #[allow(clippy::too_many_lines)] // Assert all captured signal identities together.
    pub async fn finish(self, context: &TraceContext, expected_models: usize) -> Vec<Value> {
        let requests = self.raw().await;
        let spans: Vec<_> = requests
            .iter()
            .filter_map(Value::as_array)
            .flatten()
            .filter_map(|envelope| envelope.get("spans").and_then(Value::as_array))
            .flatten()
            .collect();
        let logs: Vec<_> = requests
            .iter()
            .filter(|body| body.pointer("/0/message").is_some())
            .filter_map(Value::as_array)
            .flatten()
            .collect();
        if requests.is_empty() {
            assert_eq!(expected_models, 0);
            return requests;
        }
        assert_eq!(
            spans
                .iter()
                .filter(|span| span["meta"]["kind"] == "llm")
                .count(),
            expected_models
        );
        let root = spans
            .iter()
            .find(|span| span["meta"]["kind"] == "agent")
            .unwrap();
        let workflow = spans
            .iter()
            .find(|span| span["meta"]["kind"] == "workflow")
            .unwrap();
        assert_eq!(root["parent_id"], "undefined");
        assert_eq!(workflow["parent_id"], root["span_id"]);
        let trace = u128::from_str_radix(context.trace_id(), 16).unwrap();
        let apm = if trace > u128::from(u64::MAX) {
            format!("{trace:032x}")
        } else {
            trace.to_string()
        };
        let host_span = u64::from_str_radix(context.span_id(), 16)
            .unwrap()
            .to_string();
        let mut ids = std::collections::HashSet::new();
        for span in &spans {
            assert!(ids.insert(span["span_id"].as_str().unwrap()));
            assert_eq!(span["trace_id"], root["trace_id"]);
            assert_ne!(span["trace_id"], apm);
            assert_eq!(span["_dd"]["apm_trace_id"], apm);
            assert_eq!(span["_dd"]["span_id"], host_span);
            assert!(span["status"] == "ok" || span["status"] == "error");
            if span["meta"]["kind"] == "llm" || span["meta"]["kind"] == "tool" {
                assert_eq!(span["parent_id"], workflow["span_id"]);
            }
        }
        assert!(!logs.is_empty());
        for log in &logs {
            assert_eq!(log["dd.trace_id"], apm);
            assert_eq!(log["dd.span_id"], host_span);
        }
        if context
            .predecessor()
            .and_then(|link| link.observation_attempt())
            .is_some()
        {
            assert_eq!(root["span_links"].as_array().unwrap().len(), 1);
            assert_ne!(root["span_links"][0]["trace_id"], root["trace_id"]);
        }
        println!(
            "export_assertions spans={} logs={} models={} apm_trace={} apm_span={} llm_trace={} llm_root={} links={}",
            spans.len(),
            logs.len(),
            expected_models,
            apm,
            host_span,
            root["trace_id"],
            root["span_id"],
            usize::from(
                context
                    .predecessor()
                    .and_then(|link| link.observation_attempt())
                    .is_some()
            )
        );
        requests
    }
}
