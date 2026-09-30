//! Redaction-first, nonblocking Datadog export for Crabber runtime events.
use crabber_core::{EventKind, EventRecord};
use crabber_runtime::Observer;
use flate2::{Compression, write::GzEncoder};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fmt,
    io::Write,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};

/// Explicit opt-in for bounded summaries. Defaults never capture content.
#[derive(Debug, Clone)]
pub struct RedactionPolicy {
    pub capture_input_summary: bool,
    pub capture_output_summary: bool,
    pub max_summary_bytes: usize,
}
impl Default for RedactionPolicy {
    fn default() -> Self {
        Self {
            capture_input_summary: false,
            capture_output_summary: false,
            max_summary_bytes: 512,
        }
    }
}
fn bounded_summary(value: Option<&Value>, limit: usize) -> Option<String> {
    let source = value?.as_str()?;
    let mut result = String::new();
    for ch in source.chars() {
        if result.len() + ch.len_utf8() > limit.min(4096) {
            break;
        }
        result.push(ch);
    }
    Some(result)
}

/// Agentless intake settings. The API key is always hidden in `Debug` output.
#[derive(Clone)]
pub struct DatadogConfig {
    pub site: String,
    pub api_key: String,
    pub service: String,
    pub env: String,
    pub version: String,
    pub ml_app: String,
    pub tags: Vec<String>,
    pub redaction: RedactionPolicy,
    pub batch_size: usize,
    pub channel_capacity: usize,
    pub max_payload_bytes: usize,
    pub timeout: Duration,
    /// Overrides the API origin for a local intake test.
    pub api_origin: Option<String>,
    /// Overrides the logs origin for a local intake test.
    pub logs_origin: Option<String>,
}
impl fmt::Debug for DatadogConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DatadogConfig")
            .field("site", &self.site)
            .field("api_key", &"[REDACTED]")
            .field("service", &self.service)
            .field("env", &self.env)
            .field("version", &self.version)
            .field("ml_app", &self.ml_app)
            .field("tags", &self.tags)
            .field("redaction", &self.redaction)
            .field("batch_size", &self.batch_size)
            .field("channel_capacity", &self.channel_capacity)
            .field("max_payload_bytes", &self.max_payload_bytes)
            .finish_non_exhaustive()
    }
}
impl DatadogConfig {
    #[must_use]
    pub fn from_env() -> Option<Self> {
        Self::from_lookup(&|name| std::env::var(name))
    }
    #[must_use]
    pub fn from_lookup(get: &dyn Fn(&str) -> Result<String, std::env::VarError>) -> Option<Self> {
        let api_key = get("DD_API_KEY").ok().filter(|v| !v.is_empty())?;
        let service = get("DD_SERVICE").unwrap_or_else(|_| "crabber".into());
        Some(Self {
            site: get("DD_SITE").unwrap_or_else(|_| "datadoghq.com".into()),
            api_key,
            env: get("DD_ENV").unwrap_or_else(|_| "unknown".into()),
            version: get("DD_VERSION").unwrap_or_else(|_| "unknown".into()),
            ml_app: get("DD_LLMOBS_ML_APP").unwrap_or_else(|_| service.clone()),
            service,
            tags: Vec::new(),
            redaction: RedactionPolicy::default(),
            batch_size: 100,
            channel_capacity: 4096,
            max_payload_bytes: 512_000,
            timeout: Duration::from_secs(10),
            api_origin: None,
            logs_origin: None,
        })
    }
    #[must_use]
    pub fn api_origin(&self) -> String {
        self.api_origin
            .clone()
            .unwrap_or_else(|| format!("https://api.{}", self.site))
    }
    #[must_use]
    pub fn logs_origin(&self) -> String {
        self.logs_origin
            .clone()
            .unwrap_or_else(|| format!("https://http-intake.logs.{}", self.site))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ExportError {
    #[error("export worker stopped")]
    WorkerStopped,
    #[error("export timed out")]
    Timeout,
    #[error("http export failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("gzip failed: {0}")]
    Gzip(#[from] std::io::Error),
    #[error("intake rejected export: HTTP {0}")]
    Status(reqwest::StatusCode),
    #[error("{signal} intake rejected export: HTTP {status} ({diagnostic})")]
    Rejected {
        signal: &'static str,
        status: reqwest::StatusCode,
        diagnostic: String,
    },
    #[error("one observation exceeds the configured payload limit")]
    PayloadTooLarge,
    #[error("metrics intake reported {0} series error(s)")]
    MetricIntakeErrors(usize),
}

/// The export queue drops new observations on overflow and never waits in `emit`.
#[derive(Clone)]
pub struct DatadogObserver {
    tx: mpsc::Sender<Command>,
    dropped: Arc<AtomicU64>,
    timeout: Duration,
    redaction: RedactionPolicy,
}
enum Command {
    Event(Box<SafeEvent>),
    Flush(oneshot::Sender<Result<(), ExportError>>),
    Shutdown(oneshot::Sender<Result<(), ExportError>>),
}
#[derive(Clone)]
struct SafeEvent {
    kind: EventKind,
    session: String,
    run: String,
    time_ns: i128,
    provider: Option<String>,
    model: Option<String>,
    tool: Option<String>,
    tool_id: Option<String>,
    status: Option<String>,
    input_tokens: u64,
    output_tokens: u64,
    latency_ms: f64,
    duration_ms: f64,
    duration_ns: i128,
    latency_ns: i128,
    input_summary: Option<String>,
    output_summary: Option<String>,
    redactions: Vec<String>,
}
impl SafeEvent {
    fn from_event(e: &EventRecord, policy: &RedactionPolicy) -> Option<Self> {
        if matches!(
            e.kind,
            EventKind::TextDelta
                | EventKind::ReasoningDelta
                | EventKind::MessageCommitted
                | EventKind::TurnStarted
                | EventKind::TurnCompleted
        ) {
            return None;
        }
        let safe = |name: &str| {
            e.payload.get(name).and_then(Value::as_str).map(|s| {
                s.chars()
                    .filter(|c| c.is_ascii_alphanumeric() || "._-/".contains(*c))
                    .take(100)
                    .collect()
            })
        };
        Some(Self {
            kind: e.kind.clone(),
            session: e.session_id.to_string(),
            run: e.run_id.to_string(),
            time_ns: e.created_at.unix_timestamp_nanos(),
            provider: safe("provider"),
            model: safe("model"),
            tool: safe("tool"),
            tool_id: safe("tool_id"),
            status: safe("status").map(|status| match status.as_str() {
                "completed" => "ok".to_string(),
                "failed" | "interrupted" => "error".to_string(),
                _ => status,
            }),
            input_tokens: e
                .payload
                .get("input_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            output_tokens: e
                .payload
                .get("output_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            latency_ms: e
                .payload
                .get("latency_ms")
                .and_then(Value::as_f64)
                .unwrap_or(0.0),
            duration_ms: e
                .payload
                .get("duration_ms")
                .and_then(Value::as_f64)
                .unwrap_or(0.0),
            duration_ns: e
                .payload
                .get("duration_ns")
                .and_then(Value::as_i64)
                .map_or_else(
                    || {
                        i128::from(
                            e.payload
                                .get("duration_ms")
                                .and_then(Value::as_i64)
                                .unwrap_or(0)
                                .max(0),
                        ) * 1_000_000
                    },
                    i128::from,
                ),
            latency_ns: i128::from(
                e.payload
                    .get("latency_ms")
                    .and_then(Value::as_i64)
                    .unwrap_or(0)
                    .max(0),
            ) * 1_000_000,
            input_summary: if policy.capture_input_summary {
                bounded_summary(e.payload.get("input_summary"), policy.max_summary_bytes)
            } else {
                None
            },
            output_summary: if policy.capture_output_summary {
                bounded_summary(e.payload.get("output_summary"), policy.max_summary_bytes)
            } else {
                None
            },
            redactions: [
                "text",
                "prompt",
                "reasoning",
                "arguments",
                "output",
                "headers",
                "input_summary",
                "output_summary",
            ]
            .into_iter()
            .filter(|key| e.payload.get(*key).is_some())
            .map(str::to_string)
            .collect(),
        })
    }
}
impl Observer for DatadogObserver {
    fn emit(&self, event: &EventRecord) {
        if let Some(event) = SafeEvent::from_event(event, &self.redaction)
            && self.tx.try_send(Command::Event(Box::new(event))).is_err()
        {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
    fn model_completed(&self, event: &EventRecord) {
        self.emit(event);
    }
}
impl DatadogObserver {
    #[must_use]
    pub fn new(config: &DatadogConfig) -> Self {
        let (tx, rx) = mpsc::channel(config.channel_capacity.clamp(1, 4096));
        let dropped = Arc::new(AtomicU64::new(0));
        tokio::spawn(worker(rx, config.clone(), Arc::clone(&dropped)));
        Self {
            tx,
            dropped,
            timeout: config.timeout,
            redaction: config.redaction.clone(),
        }
    }
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
    /// Waits for queued records to be exported.
    /// # Errors
    /// Returns a worker or intake error, or times out.
    pub async fn flush(&self) -> Result<(), ExportError> {
        self.control(false).await
    }
    /// Flushes and stops the export worker.
    /// # Errors
    /// Returns a worker or intake error, or times out.
    pub async fn shutdown(&self) -> Result<(), ExportError> {
        self.control(true).await
    }
    async fn control(&self, shutdown: bool) -> Result<(), ExportError> {
        let (tx, rx) = oneshot::channel();
        let command = if shutdown {
            Command::Shutdown(tx)
        } else {
            Command::Flush(tx)
        };
        tokio::time::timeout(self.timeout, self.tx.send(command))
            .await
            .map_err(|_| ExportError::Timeout)?
            .map_err(|_| ExportError::WorkerStopped)?;
        tokio::time::timeout(self.timeout, rx)
            .await
            .map_err(|_| ExportError::Timeout)?
            .map_err(|_| ExportError::WorkerStopped)?
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum ExportStage {
    Spans,
    Metrics,
    Logs,
}
struct PendingBatch {
    events: Vec<SafeEvent>,
    stage: ExportStage,
    dropped_snapshot: Option<u64>,
}
impl Default for PendingBatch {
    fn default() -> Self {
        Self {
            events: Vec::new(),
            stage: ExportStage::Spans,
            dropped_snapshot: None,
        }
    }
}
async fn worker(mut rx: mpsc::Receiver<Command>, config: DatadogConfig, dropped: Arc<AtomicU64>) {
    let Ok(client) = reqwest::Client::builder().timeout(config.timeout).build() else {
        return;
    };
    let mut pending = PendingBatch::default();
    let mut retrying = false;
    let batch_limit = config.batch_size.clamp(1, 1000);
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            command=rx.recv() => match command {
                Some(Command::Event(event)) => {
                    if retrying || pending.events.len()>=batch_limit {
                        dropped.fetch_add(1,Ordering::Relaxed);
                    } else {
                        pending.events.push(*event);
                        if pending.events.len()>=batch_limit {
                            retrying=export(&client,&config,&mut pending,&dropped).await.is_err();
                        }
                    }
                }
                Some(Command::Flush(reply)) => {
                    let result=export(&client,&config,&mut pending,&dropped).await;
                    retrying=result.is_err();
                    let _=reply.send(result);
                }
                Some(Command::Shutdown(reply)) => {
                    let result=export(&client,&config,&mut pending,&dropped).await;
                    retrying=result.is_err();
                    let success=result.is_ok();
                    let _=reply.send(result);
                    if success { break; }
                }
                None => break,
            },
            _=tick.tick() => {
                if retrying || !pending.events.is_empty() || dropped.load(Ordering::Relaxed)>0 {
                    retrying=export(&client,&config,&mut pending,&dropped).await.is_err();
                }
            }
        }
    }
}
fn tags(c: &DatadogConfig, e: &SafeEvent) -> Vec<String> {
    let mut t = vec![
        format!("service:{}", c.service),
        format!("env:{}", c.env),
        format!("version:{}", c.version),
    ];
    if let Some(p) = &e.provider {
        t.push(format!("provider:{p}"));
    }
    if let Some(m) = &e.model {
        t.push(format!("model:{m}"));
    }
    t.extend(c.tags.iter().map(|tag| {
        tag.chars()
            .filter(|ch| ch.is_ascii_alphanumeric() || "._:-/".contains(*ch))
            .take(120)
            .collect()
    }));
    t
}
fn datadog_id(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    let bytes: [u8; 8] = digest[..8].try_into().expect("sha256 has eight bytes");
    u64::from_be_bytes(bytes).max(1).to_string()
}
fn spans(e: &SafeEvent) -> Vec<Value> {
    let mut result = Vec::new();
    if matches!(e.kind, EventKind::RunSettled) {
        result.push(span_value(
            e,
            "crabber.session",
            "agent",
            &format!("{}-session", e.session),
            None,
        ));
    }
    let (name, kind, id_seed, parent_seed) = match &e.kind {
        EventKind::RunSettled => (
            "crabber.run",
            "workflow",
            format!("{}-run", e.run),
            Some(format!("{}-session", e.session)),
        ),
        EventKind::Custom { name } if name == "model_call" => (
            "crabber.model_call",
            "llm",
            format!("{}-model-{}", e.run, e.time_ns),
            Some(format!("{}-run", e.run)),
        ),
        EventKind::ToolCallSettled => (
            "crabber.tool_call",
            "tool",
            format!(
                "{}-tool-{}",
                e.run,
                e.tool_id.as_deref().unwrap_or("unknown")
            ),
            Some(format!("{}-run", e.run)),
        ),
        EventKind::ContextEpochFinished => (
            "crabber.compaction",
            "task",
            format!("{}-epoch-{}", e.run, e.time_ns),
            Some(format!("{}-run", e.run)),
        ),
        _ => return result,
    };
    result.push(span_value(e, name, kind, &id_seed, parent_seed));
    result
}
fn span_value(
    e: &SafeEvent,
    name: &str,
    kind: &str,
    id_seed: &str,
    parent_seed: Option<String>,
) -> Value {
    let mut meta = json!({"kind":kind});
    if let Some(model) = &e.model {
        meta["model_name"] = json!(model);
    }
    if let Some(provider) = &e.provider {
        meta["model_provider"] = json!(provider);
    }
    let mut metadata = json!({});
    if let Some(tool) = &e.tool {
        metadata["tool_name"] = json!(tool);
    }
    if !e.redactions.is_empty() {
        metadata["redacted_fields"] = json!(e.redactions.join(","));
    }
    if metadata.as_object().is_some_and(|m| !m.is_empty()) {
        meta["metadata"] = metadata;
    }
    if let Some(summary) = &e.input_summary {
        meta["input"] = json!({"value":summary});
    }
    if let Some(summary) = &e.output_summary {
        meta["output"] = json!({"value":summary});
    }
    let duration = if kind == "llm" {
        e.latency_ns.max(1)
    } else {
        e.duration_ns.max(1)
    };
    json!({"name":name,"span_id":datadog_id(id_seed),"trace_id":datadog_id(&format!("{}-trace",e.run)),
        "parent_id":parent_seed.map_or_else(|| "undefined".to_string(),|p| datadog_id(&p)),
        "start_ns":e.time_ns-duration,"duration":duration,"meta":meta,
        "status":e.status.as_deref().unwrap_or("ok"),
        "metrics":{"input_tokens":e.input_tokens,"output_tokens":e.output_tokens,"total_tokens":e.input_tokens+e.output_tokens},
        "session_id":e.session})
}
fn series(c: &DatadogConfig, e: &SafeEvent) -> Vec<Value> {
    let mut values = Vec::new();
    match &e.kind {
        EventKind::RunSettled => values.extend([
            ("crabber.run.count", 1.0),
            ("crabber.run.duration_ms", e.duration_ms),
        ]),
        EventKind::Custom { name } if name == "model_call" => {
            values.extend([
                ("crabber.model.calls", 1.0),
                ("crabber.model.latency_ms", e.latency_ms),
                (
                    "crabber.model.tokens.input",
                    f64::from(u32::try_from(e.input_tokens).unwrap_or(u32::MAX)),
                ),
                (
                    "crabber.model.tokens.output",
                    f64::from(u32::try_from(e.output_tokens).unwrap_or(u32::MAX)),
                ),
            ]);
        }
        EventKind::ToolCallSettled => values.push(("crabber.tool.calls", 1.0)),
        EventKind::PermissionDecided => values.push(("crabber.permission.decisions", 1.0)),
        EventKind::Custom { name } if name == "wasm_call" => {
            values.push(("crabber.wasm.calls", 1.0));
        }
        _ => (),
    }
    values
        .into_iter()
        .map(|(metric, value)| {
            let gauge = metric.ends_with("_ms");
            let mut item = json!({"metric":metric,"type":if gauge {3} else {1},
            "points":[{"timestamp":e.time_ns/1_000_000_000,"value":value}],"tags":tags(c,e)});
            if !gauge {
                item["interval"] = json!(1);
            }
            item
        })
        .collect()
}
fn log(c: &DatadogConfig, e: &SafeEvent) -> Option<Value> {
    let (message, status) = match e.kind {
        EventKind::RunAdmitted => ("run admitted", "info"),
        EventKind::RunSettled => (
            "run settled",
            if e.status.as_deref() == Some("error") {
                "error"
            } else {
                "info"
            },
        ),
        EventKind::ToolCallSettled => ("tool settled", "info"),
        EventKind::PermissionDecided => ("permission decided", "info"),
        _ => return None,
    };
    Some(
        json!({"message":message,"status":status,"service":c.service,"ddsource":"crabber","ddtags":tags(c,e).join(","),"session_id":e.session,"run_id":e.run,"tool_call_id":e.tool_id,"verify_marker":c.tags.iter().find_map(|tag| tag.strip_prefix("verify:"))}),
    )
}
fn split_payload(body: &Value) -> Option<(Value, Value)> {
    let pointer = if body.is_array() {
        ""
    } else if body.get("series").is_some() {
        "/series"
    } else {
        "/data/attributes/spans"
    };
    let items = body.pointer(pointer)?.as_array()?;
    if items.len() < 2 {
        return None;
    }
    let middle = items.len() / 2;
    let mut first = body.clone();
    let mut second = body.clone();
    *first.pointer_mut(pointer)? = json!(items[..middle]);
    *second.pointer_mut(pointer)? = json!(items[middle..]);
    Some((first, second))
}
fn intake_signal(url: &str) -> &'static str {
    if url.contains("llm-obs") {
        "llmobs"
    } else if url.contains("/series") {
        "metrics"
    } else {
        "logs"
    }
}
async fn rejected(response: reqwest::Response, url: &str) -> ExportError {
    let status = response.status();
    let body = response.text().await.unwrap_or_default();
    let diagnostic = serde_json::from_str::<Value>(&body)
        .ok()
        .and_then(|json| json.get("errors")?.as_array()?.first().cloned())
        .map_or_else(
            || "no structured field detail".to_string(),
            |error| {
                if let Some(message) = error.as_str() {
                    return safe_diagnostic(message);
                }
                let title = error
                    .get("title")
                    .and_then(Value::as_str)
                    .unwrap_or("rejected");
                let pointer = error
                    .pointer("/source/pointer")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown field");
                format!("{} at {}", safe_diagnostic(title), safe_diagnostic(pointer))
            },
        );
    ExportError::Rejected {
        signal: intake_signal(url),
        status,
        diagnostic,
    }
}
fn safe_diagnostic(text: &str) -> String {
    text.chars()
        .filter(|c| c.is_ascii_alphabetic() || "/._- ".contains(*c))
        .take(80)
        .collect()
}
async fn post(
    client: &reqwest::Client,
    config: &DatadogConfig,
    url: String,
    body: Value,
) -> Result<(), ExportError> {
    let mut parts = vec![body];
    'part: while let Some(body) = parts.pop() {
        let raw = body.to_string();
        if raw.len() > config.max_payload_bytes {
            let (first, second) = split_payload(&body).ok_or(ExportError::PayloadTooLarge)?;
            parts.push(second);
            parts.push(first);
            continue;
        }
        let is_llmobs = url.contains("llm-obs");
        let bytes = if is_llmobs {
            raw.into_bytes()
        } else {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(raw.as_bytes())?;
            encoder.finish()?
        };
        for attempt in 0..3 {
            let request = client
                .post(&url)
                .header("DD-API-KEY", &config.api_key)
                .header("Content-Type", "application/json");
            let request = if is_llmobs {
                request
            } else {
                request.header("Content-Encoding", "gzip")
            };
            let result = request.body(bytes.clone()).send().await;
            match result {
                Ok(response) if response.status().is_success() => {
                    if url.contains("llm-obs") && response.status().as_u16() != 202 {
                        return Err(rejected(response, &url).await);
                    }
                    if url.contains("/series") {
                        let body = response.json::<Value>().await.unwrap_or(Value::Null);
                        let errors = body["errors"].as_array().map_or(0, Vec::len);
                        if errors > 0 {
                            return Err(ExportError::MetricIntakeErrors(errors));
                        }
                    }
                    continue 'part;
                }
                Ok(response) if response.status().as_u16() == 413 => {
                    let (first, second) =
                        split_payload(&body).ok_or(ExportError::PayloadTooLarge)?;
                    parts.push(second);
                    parts.push(first);
                    continue 'part;
                }
                Ok(response)
                    if response.status().is_server_error() || response.status().as_u16() == 429 =>
                {
                    if attempt == 2 {
                        return Err(ExportError::Status(response.status()));
                    }
                }
                Ok(response) => return Err(rejected(response, &url).await),
                Err(error) if attempt == 2 => return Err(ExportError::Http(error)),
                Err(_) => (),
            }
            tokio::time::sleep(Duration::from_millis(200 * (1 << attempt))).await;
        }
    }
    Ok(())
}
async fn export(
    client: &reqwest::Client,
    config: &DatadogConfig,
    pending: &mut PendingBatch,
    dropped: &AtomicU64,
) -> Result<(), ExportError> {
    if pending.stage == ExportStage::Spans {
        let spans: Vec<_> = pending.events.iter().flat_map(spans).collect();
        if !spans.is_empty() {
            post(client,config,format!("{}/api/intake/llm-obs/v1/trace/spans",config.api_origin()),
                json!({"data":{"type":"span","attributes":{"ml_app":config.ml_app,"spans":spans,"tags":config.tags}}})).await?;
        }
        pending.stage = ExportStage::Metrics;
    }
    if pending.stage == ExportStage::Metrics {
        let mut metrics: Vec<_> = pending
            .events
            .iter()
            .flat_map(|e| series(config, e))
            .collect();
        let dropped_count = *pending
            .dropped_snapshot
            .get_or_insert_with(|| dropped.load(Ordering::Relaxed));
        if !pending.events.is_empty() {
            metrics.push(json!({"metric":"crabber.export.batches","type":1,"interval":1,"points":[{"timestamp":time_now(),"value":1}],
                "tags":[format!("service:{}",config.service),format!("env:{}",config.env),"outcome:ok"]}));
        }
        if dropped_count > 0 {
            metrics.push(json!({"metric":"crabber.export.dropped","type":1,"interval":1,"points":[{"timestamp":time_now(),"value":dropped_count}],
                "tags":[format!("service:{}",config.service),format!("env:{}",config.env)]}));
        }
        if !metrics.is_empty() {
            post(
                client,
                config,
                format!("{}/api/v2/series", config.api_origin()),
                json!({"series":metrics}),
            )
            .await?;
        }
        if dropped_count > 0 {
            dropped.fetch_sub(dropped_count, Ordering::Relaxed);
        }
        pending.stage = ExportStage::Logs;
    }
    if pending.stage == ExportStage::Logs {
        let logs: Vec<_> = pending
            .events
            .iter()
            .filter_map(|e| log(config, e))
            .collect();
        if !logs.is_empty() {
            post(
                client,
                config,
                format!("{}/api/v2/logs", config.logs_origin()),
                json!(logs),
            )
            .await?;
        }
        pending.events.clear();
        pending.stage = ExportStage::Spans;
        pending.dropped_snapshot = None;
    }
    Ok(())
}
fn time_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX))
}

/// Emits safe lifecycle fields into the host's installed tracing subscriber.
pub mod tracing_bridge {
    use crabber_core::{EventKind, EventRecord};
    pub fn emit(event: &EventRecord) {
        let kind = match event.kind {
            EventKind::RunStarted => "run.started",
            EventKind::RunSettled => "run.settled",
            EventKind::ToolCallSettled => "tool.settled",
            _ => return,
        };
        tracing::info!(session.id=%event.session_id, run.id=%event.run_id, kind, "crabber event");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crabber_core::{RunId, SessionId};
    use flate2::read::GzDecoder;
    use std::{
        io::Read,
        net::{TcpListener, TcpStream},
        sync::atomic::AtomicBool,
    };
    fn config() -> DatadogConfig {
        DatadogConfig {
            site: "datadoghq.com".into(),
            api_key: "test-key".into(),
            service: "crabber".into(),
            env: "test".into(),
            version: "test".into(),
            ml_app: "crabber".into(),
            tags: vec!["verify:test".into()],
            redaction: RedactionPolicy::default(),
            batch_size: 100,
            channel_capacity: 32,
            max_payload_bytes: 512_000,
            timeout: Duration::from_secs(3),
            api_origin: None,
            logs_origin: None,
        }
    }
    fn event(
        kind: EventKind,
        session_id: &SessionId,
        run_id: &RunId,
        payload: Value,
    ) -> EventRecord {
        EventRecord {
            cursor: None,
            session_id: session_id.clone(),
            run_id: run_id.clone(),
            turn_id: None,
            kind,
            payload,
            correlation: None,
            live_only: false,
            created_at: time::OffsetDateTime::now_utc(),
        }
    }
    fn read_request(stream: &mut TcpStream) -> (String, Value) {
        let mut all = Vec::new();
        let mut buffer = [0u8; 8192];
        let (head_end, length) = loop {
            let n = stream.read(&mut buffer).unwrap();
            assert!(n > 0);
            all.extend_from_slice(&buffer[..n]);
            if let Some(pos) = all.windows(4).position(|w| w == b"\r\n\r\n") {
                let end = pos + 4;
                let head = String::from_utf8_lossy(&all[..end]).to_ascii_lowercase();
                let len: usize = head
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length: "))
                    .unwrap()
                    .trim()
                    .parse()
                    .unwrap();
                if all.len() >= end + len {
                    break (end, len);
                }
            }
        };
        while all.len() < head_end + length {
            let n = stream.read(&mut buffer).unwrap();
            assert!(n > 0);
            all.extend_from_slice(&buffer[..n]);
        }
        let headers = String::from_utf8_lossy(&all[..head_end]).to_ascii_lowercase();
        assert!(headers.contains("dd-api-key: test-key"));
        let mut body_text = String::new();
        if headers.contains("/api/intake/llm-obs/") {
            assert!(!headers.contains("content-encoding: gzip"));
            body_text = String::from_utf8(all[head_end..head_end + length].to_vec()).unwrap();
        } else {
            assert!(headers.contains("content-encoding: gzip"));
            GzDecoder::new(&all[head_end..head_end + length])
                .read_to_string(&mut body_text)
                .unwrap();
        }
        assert!(!body_text.contains("PROMPT_SECRET"));
        (headers, serde_json::from_str(&body_text).unwrap())
    }
    #[test]
    fn summaries_are_opt_in_and_byte_bounded() {
        let input = json!({"input_summary":"ééé","prompt":"PROMPT_SECRET"});
        let session = SessionId::new();
        let run = RunId::new();
        let record = event(EventKind::RunStarted, &session, &run, input);
        let default = SafeEvent::from_event(&record, &RedactionPolicy::default()).unwrap();
        assert!(default.input_summary.is_none());
        assert!(default.redactions.contains(&"prompt".to_string()));
        let policy = RedactionPolicy {
            capture_input_summary: true,
            capture_output_summary: false,
            max_summary_bytes: 3,
        };
        let opted = SafeEvent::from_event(&record, &policy).unwrap();
        assert_eq!(opted.input_summary.as_deref(), Some("é"));
    }
    #[test]
    fn oversized_payload_splits_without_losing_items() {
        let body = json!({"data":{"type":"span","attributes":{"ml_app":"app","spans":[1,2,3]}}});
        let (a, b) = split_payload(&body).unwrap();
        assert_eq!(a["data"]["attributes"]["spans"], json!([1]));
        assert_eq!(b["data"]["attributes"]["spans"], json!([2, 3]));
    }
    #[test]
    fn sites_and_config_debug() {
        for site in [
            "datadoghq.com",
            "datadoghq.eu",
            "us3.datadoghq.com",
            "us5.datadoghq.com",
            "ap1.datadoghq.com",
        ] {
            let mut c = config();
            c.site = site.into();
            assert_eq!(c.api_origin(), format!("https://api.{site}"));
            assert_eq!(c.logs_origin(), format!("https://http-intake.logs.{site}"));
            assert!(!format!("{c:?}").contains("test-key"));
        }
    }
    #[tokio::test]
    async fn overflow_is_counted_without_waiting() {
        let mut c = config();
        c.channel_capacity = 1;
        let observer = DatadogObserver::new(&c);
        let session = SessionId::new();
        let run = RunId::new();
        for _ in 0..100 {
            observer.emit(&event(EventKind::RunStarted, &session, &run, Value::Null));
        }
        assert!(observer.dropped() > 0);
    }
    #[tokio::test]
    async fn failed_flush_keeps_pending_signals_and_drop_count() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let recovered = Arc::new(AtomicBool::new(false));
        let recovered_server = Arc::clone(&recovered);
        let server = std::thread::spawn(move || {
            let mut accepted = Vec::new();
            while accepted.len() < 3 {
                let (mut stream, _) = listener.accept().unwrap();
                let request = read_request(&mut stream);
                if recovered_server.load(Ordering::SeqCst) {
                    accepted.push(request);
                    std::io::Write::write_all(
                        &mut stream,
                        b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .unwrap();
                } else {
                    std::io::Write::write_all(&mut stream,b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
                }
            }
            accepted
        });
        let mut c = config();
        c.api_origin = Some(origin.clone());
        c.logs_origin = Some(origin);
        c.channel_capacity = 1;
        c.batch_size = 100;
        let observer = DatadogObserver::new(&c);
        let session = SessionId::new();
        let run = RunId::new();
        observer.emit(&event(
            EventKind::RunSettled,
            &session,
            &run,
            json!({"status":"ok","duration_ms":100,"prompt":"PROMPT_SECRET"}),
        ));
        for _ in 0..200 {
            observer.emit(&event(EventKind::RunStarted, &session, &run, Value::Null));
        }
        assert!(observer.dropped() > 0);
        assert!(observer.flush().await.is_err());
        recovered.store(true, Ordering::SeqCst);
        observer.flush().await.unwrap();
        let accepted = server.join().unwrap();
        assert!(accepted[0].0.contains("/api/intake/llm-obs/"));
        assert_eq!(
            accepted[0].1["data"]["attributes"]["spans"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        let metric_series = accepted[1].1["series"].as_array().unwrap();
        assert!(
            metric_series
                .iter()
                .any(|m| m["metric"] == "crabber.run.count")
        );
        assert!(
            metric_series
                .iter()
                .any(|m| m["metric"] == "crabber.export.dropped"
                    && m["points"][0]["value"].as_u64().unwrap() > 0)
        );
        assert!(
            accepted[2]
                .1
                .as_array()
                .unwrap()
                .iter()
                .any(|l| l["message"] == "run settled")
        );
    }
    struct EchoTool;
    #[async_trait::async_trait]
    impl crabber_extension::ToolExecutor for EchoTool {
        async fn execute(&self, value: Value) -> Result<Value, crabber_extension::ExtensionError> {
            Ok(value)
        }
    }
    #[tokio::test]
    #[allow(clippy::too_many_lines)] // Real runtime and mock intake assertions stay together.
    async fn failed_provider_run_exports_parented_workflow() {
        use crabber_core::{ToolCallId, ToolInfo};
        use crabber_extension::{StaticPlanProvider, ToolDefinition};
        use crabber_providers::{
            FakeProvider, ProviderError, ProviderErrorKind, Selection, StreamDelta,
        };
        use crabber_runtime::{Orchestrator, PermissionDecision, Request, StaticPolicy};
        use crabber_session::MemoryStore;
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let mut requests = Vec::new();
            for _ in 0..3 {
                let (mut stream, _) = listener.accept().unwrap();
                requests.push(read_request(&mut stream));
                std::io::Write::write_all(
                    &mut stream,
                    b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
            }
            requests
        });
        let mut c = config();
        c.api_origin = Some(origin.clone());
        c.logs_origin = Some(origin);
        let observer = DatadogObserver::new(&c);
        let call_id = ToolCallId::new();
        let provider = FakeProvider::scripted(vec![
            vec![
                StreamDelta::ToolCallStart {
                    call_id: call_id.clone(),
                    name: "echo".into(),
                },
                StreamDelta::ToolCallArgsDelta {
                    call_id: call_id.clone(),
                    text: "{}".into(),
                },
                StreamDelta::ToolCallDone { call_id },
                StreamDelta::Completed,
            ],
            vec![StreamDelta::Error(ProviderError {
                kind: ProviderErrorKind::Server,
                message: "PROMPT_SECRET".into(),
                retryable: false,
            })],
        ]);
        let tool = Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "echo".into(),
                description: "echo".into(),
                parameters: json!({"type":"object"}),
                retry_safe: true,
                required_permissions: vec![],
            },
            executor: Arc::new(EchoTool),
        });
        let runtime = Orchestrator::builder()
            .store(Arc::new(MemoryStore::new()))
            .resolver(Arc::new(provider))
            .plan_provider(Arc::new(StaticPlanProvider::new(vec![tool], vec![])))
            .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
            .observer(Arc::new(observer.clone()))
            .build()
            .unwrap();
        let run = runtime
            .start(Request {
                session_id: None,
                workspace_id: "test".into(),
                directory: ".".into(),
                title: "test".into(),
                text: "PROMPT_SECRET".into(),
                selection: Selection {
                    provider_id: "fake".into(),
                    model_id: "demo".into(),
                },
                system_prompt: None,
            })
            .await
            .unwrap();
        assert!(run.done().await.is_err());
        observer.flush().await.unwrap();
        let requests = server.join().unwrap();
        let spans = requests[0].1["data"]["attributes"]["spans"]
            .as_array()
            .unwrap();
        let agent = spans.iter().find(|s| s["meta"]["kind"] == "agent").unwrap();
        let workflow = spans
            .iter()
            .find(|s| s["meta"]["kind"] == "workflow")
            .unwrap();
        assert_eq!(workflow["status"], "error");
        assert_eq!(workflow["parent_id"], agent["span_id"]);
        for kind in ["llm", "tool"] {
            let child = spans.iter().find(|s| s["meta"]["kind"] == kind).unwrap();
            assert_eq!(child["parent_id"], workflow["span_id"]);
            assert!(agent["start_ns"].as_i64().unwrap() <= child["start_ns"].as_i64().unwrap());
            assert!(
                child["start_ns"].as_i64().unwrap() + child["duration"].as_i64().unwrap()
                    <= agent["start_ns"].as_i64().unwrap() + agent["duration"].as_i64().unwrap()
            );
        }
        assert!(
            requests[1].1["series"]
                .as_array()
                .unwrap()
                .iter()
                .any(|m| m["metric"] == "crabber.run.count")
        );
        assert!(
            requests[2]
                .1
                .as_array()
                .unwrap()
                .iter()
                .any(|l| l["message"] == "run settled" && l["status"] == "error")
        );
    }
    #[tokio::test]
    #[allow(clippy::too_many_lines)] // Mock server and payload assertions stay together.
    async fn mock_intake_receives_linked_redacted_signals() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let mut requests = Vec::new();
            for _ in 0..3 {
                let (mut stream, _) = listener.accept().unwrap();
                requests.push(read_request(&mut stream));
                std::io::Write::write_all(
                    &mut stream,
                    b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
            }
            requests
        });
        let mut c = config();
        c.api_origin = Some(origin.clone());
        c.logs_origin = Some(origin);
        let observer = DatadogObserver::new(&c);
        let session = SessionId::new();
        let run = RunId::new();
        observer.emit(&event(
            EventKind::RunAdmitted,
            &session,
            &run,
            json!({"prompt":"PROMPT_SECRET"}),
        ));
        observer.emit(&event(
            EventKind::RunStarted,
            &session,
            &run,
            json!({"prompt":"PROMPT_SECRET"}),
        ));
        observer.emit(&event(EventKind::Custom { name:"model_call".into() },&session,&run,json!({"provider":"fake","model":"demo","input_tokens":2,"output_tokens":3,"prompt":"PROMPT_SECRET"})));
        observer.emit(&event(
            EventKind::ToolCallSettled,
            &session,
            &run,
            json!({"tool":"echo","tool_id":"call-1","output":"PROMPT_SECRET"}),
        ));
        observer.emit(&event(
            EventKind::RunSettled,
            &session,
            &run,
            json!({"prompt":"PROMPT_SECRET","duration_ms":1000,"status":"ok"}),
        ));
        observer.flush().await.unwrap();
        let requests = server.join().unwrap();
        let spans = &requests[0].1["data"]["attributes"]["spans"];
        assert_eq!(requests[0].1["data"]["type"], "span");
        assert_eq!(spans.as_array().unwrap().len(), 4);
        assert_eq!(spans[0]["meta"]["kind"], "llm");
        assert_eq!(spans[1]["meta"]["kind"], "tool");
        assert_eq!(spans[2]["meta"]["kind"], "agent");
        assert_eq!(spans[3]["meta"]["kind"], "workflow");
        assert_eq!(spans[1]["parent_id"], spans[3]["span_id"]);
        assert_eq!(spans[3]["parent_id"], spans[2]["span_id"]);
        let root_start = spans[2]["start_ns"].as_i64().unwrap();
        let root_end = root_start + spans[2]["duration"].as_i64().unwrap();
        for child in [0, 1, 3] {
            let start = spans[child]["start_ns"].as_i64().unwrap();
            let end = start + spans[child]["duration"].as_i64().unwrap();
            assert!(root_start <= start && end <= root_end);
        }
        for item in spans.as_array().unwrap() {
            assert!(
                item["span_id"]
                    .as_str()
                    .unwrap()
                    .chars()
                    .all(|c| c.is_ascii_digit())
            );
            assert!(
                item["trace_id"]
                    .as_str()
                    .unwrap()
                    .chars()
                    .all(|c| c.is_ascii_digit())
            );
        }
        let metrics = &requests[1].1["series"];
        assert!(
            metrics
                .as_array()
                .unwrap()
                .iter()
                .any(|s| s["metric"] == "crabber.model.tokens.input")
        );
        assert!(
            metrics
                .as_array()
                .unwrap()
                .iter()
                .all(|s| s["type"] == 3 || s["interval"] == 1)
        );
        assert!(
            metrics
                .as_array()
                .unwrap()
                .iter()
                .any(|s| s["metric"] == "crabber.model.latency_ms" && s["type"] == 3)
        );
        assert!(metrics.as_array().unwrap().iter().all(|s| {
            s["tags"]
                .as_array()
                .unwrap()
                .iter()
                .any(|t| t == "service:crabber")
        }));
        let logs = &requests[2].1;
        assert!(
            logs.as_array()
                .unwrap()
                .iter()
                .any(|l| l["message"] == "run settled"
                    && l["ddsource"] == "crabber"
                    && l["run_id"] == run.to_string())
        );
    }
}
