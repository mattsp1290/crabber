//! Redaction-first, nonblocking Datadog export for Crabber runtime events.
use crabber_core::{EventKind, EventRecord};
use crabber_runtime::Observer;
use flate2::{Compression, write::GzEncoder};
use serde_json::{Value, json};
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
    #[error("one observation exceeds the configured payload limit")]
    PayloadTooLarge,
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
            status: safe("status"),
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
            duration_ns: i128::from(
                e.payload
                    .get("duration_ms")
                    .and_then(Value::as_i64)
                    .unwrap_or(0)
                    .max(0),
            ) * 1_000_000,
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
        let (tx, rx) = mpsc::channel(config.channel_capacity.max(1));
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
async fn worker(mut rx: mpsc::Receiver<Command>, config: DatadogConfig, dropped: Arc<AtomicU64>) {
    let Ok(client) = reqwest::Client::builder().timeout(config.timeout).build() else {
        return;
    };
    let mut pending = Vec::new();
    let mut last_failure: Option<ExportError> = None;
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            command = rx.recv() => match command {
                Some(Command::Event(event)) => { pending.push(*event); if pending.len() >= config.batch_size.max(1) && let Err(error) = export(&client, &config, &mut pending, &dropped).await { last_failure = Some(error); } },
                Some(Command::Flush(reply)) => { let result = export(&client, &config, &mut pending, &dropped).await.and_then(|()| last_failure.take().map_or(Ok(()), Err)); let _ = reply.send(result); },
                Some(Command::Shutdown(reply)) => { let result = export(&client, &config, &mut pending, &dropped).await.and_then(|()| last_failure.take().map_or(Ok(()), Err)); let _ = reply.send(result); break; },
                None => break,
            },
            _ = tick.tick() => { if (!pending.is_empty() || dropped.load(Ordering::Relaxed)>0) && let Err(error) = export(&client, &config, &mut pending, &dropped).await { last_failure = Some(error); } }
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
fn span(e: &SafeEvent) -> Option<Value> {
    let (name, kind, id, parent) = match &e.kind {
        EventKind::RunAdmitted => (
            "crabber.session",
            "agent",
            format!("{}-session", e.session),
            "undefined".to_string(),
        ),
        EventKind::RunSettled => (
            "crabber.run",
            "workflow",
            format!("{}-run", e.run),
            format!("{}-session", e.session),
        ),
        EventKind::Custom { name } if name == "model_call" => (
            "crabber.model_call",
            "llm",
            format!("{}-model-{}", e.run, e.time_ns),
            format!("{}-run", e.run),
        ),
        EventKind::ToolCallSettled => (
            "crabber.tool_call",
            "tool",
            format!(
                "{}-tool-{}",
                e.run,
                e.tool_id.as_deref().unwrap_or("unknown")
            ),
            format!("{}-run", e.run),
        ),
        EventKind::ContextEpochFinished => (
            "crabber.compaction",
            "task",
            format!("{}-epoch-{}", e.run, e.time_ns),
            format!("{}-run", e.run),
        ),
        _ => return None,
    };
    let mut meta = json!({"kind": kind});
    if let Some(model) = &e.model {
        meta["model_name"] = json!(model);
    }
    if let Some(provider) = &e.provider {
        meta["model_provider"] = json!(provider);
    }
    if let Some(tool) = &e.tool {
        meta["tool"] = json!({"name":tool});
    }
    if let Some(summary) = &e.input_summary {
        meta["input"] = json!(summary);
    }
    if let Some(summary) = &e.output_summary {
        meta["output"] = json!(summary);
    }
    if !e.redactions.is_empty() {
        meta["redactions"] = json!(e.redactions);
    }
    let duration = if kind == "llm" {
        e.latency_ns.max(1)
    } else {
        e.duration_ns.max(1)
    };
    Some(
        json!({"name":name,"span_id":id,"trace_id":e.run,"parent_id":parent,
        "start_ns":e.time_ns-duration,"duration":duration,"meta":meta,
        "status":e.status.as_deref().unwrap_or("ok"),
        "metrics":{"input_tokens":e.input_tokens,"output_tokens":e.output_tokens,"total_tokens":e.input_tokens+e.output_tokens},
        "session_id":e.session}),
    )
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
        EventKind::Custom { name } if name == "run_error" => {
            values.push(("crabber.run.count", 1.0));
        }
        EventKind::ToolCallSettled => values.push(("crabber.tool.calls", 1.0)),
        EventKind::PermissionDecided => values.push(("crabber.permission.decisions", 1.0)),
        EventKind::Custom { name } if name == "wasm_call" => {
            values.push(("crabber.wasm.calls", 1.0));
        }
        _ => (),
    }
    values.into_iter().map(|(metric,value)| json!({"metric":metric,"type":1,"points":[{"timestamp":e.time_ns/1_000_000_000,"value":value}],"tags":tags(c,e)})).collect()
}
fn log(c: &DatadogConfig, e: &SafeEvent) -> Option<Value> {
    let (message, status) = match e.kind {
        EventKind::RunAdmitted => ("run admitted", "info"),
        EventKind::RunSettled => ("run settled", "info"),
        EventKind::Custom { ref name } if name == "run_error" => ("run error", "error"),
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
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(raw.as_bytes())?;
        let bytes = encoder.finish()?;
        for attempt in 0..3 {
            let result = client
                .post(&url)
                .header("DD-API-KEY", &config.api_key)
                .header("Content-Type", "application/json")
                .header("Content-Encoding", "gzip")
                .body(bytes.clone())
                .send()
                .await;
            match result {
                Ok(response) if response.status().is_success() => {
                    if url.contains("llm-obs") && response.status().as_u16() != 202 {
                        return Err(ExportError::Status(response.status()));
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
                Ok(response) => return Err(ExportError::Status(response.status())),
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
    pending: &mut Vec<SafeEvent>,
    dropped: &AtomicU64,
) -> Result<(), ExportError> {
    let batch = std::mem::take(pending);
    let spans: Vec<_> = batch.iter().filter_map(span).collect();
    let metrics: Vec<_> = batch.iter().flat_map(|e| series(config, e)).collect();
    let logs: Vec<_> = batch.iter().filter_map(|e| log(config, e)).collect();
    let dropped_count = dropped.swap(0, Ordering::Relaxed);
    let mut metrics = metrics;
    if !batch.is_empty() {
        metrics.push(json!({"metric":"crabber.export.batches","type":1,"points":[{"timestamp":time_now(),"value":1}],
            "tags":[format!("service:{}",config.service),format!("env:{}",config.env),"outcome:ok"]}));
    }
    if dropped_count > 0 {
        metrics.push(json!({"metric":"crabber.export.dropped","type":1,"points":[{"timestamp":time_now(),"value":dropped_count}],"tags":[format!("service:{}",config.service),format!("env:{}",config.env)]}));
    }
    if !spans.is_empty() {
        post(client,config,format!("{}/api/intake/llm-obs/v1/trace/spans",config.api_origin()),json!({"data":{"type":"span","attributes":{"ml_app":config.ml_app,"spans":spans,"tags":config.tags}}})).await?;
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
    if !logs.is_empty() {
        post(
            client,
            config,
            format!("{}/api/v2/logs", config.logs_origin()),
            json!(logs),
        )
        .await?;
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
    use std::{io::Read, net::TcpListener};
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
    #[allow(clippy::too_many_lines)] // Mock server and payload assertions stay together.
    async fn mock_intake_receives_linked_redacted_signals() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let mut requests = Vec::new();
            for _ in 0..3 {
                let (mut stream, _) = listener.accept().unwrap();
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
                assert!(headers.contains("content-encoding: gzip"));
                let mut decoder = GzDecoder::new(&all[head_end..head_end + length]);
                let mut body_text = String::new();
                decoder.read_to_string(&mut body_text).unwrap();
                assert!(!body_text.contains("PROMPT_SECRET"));
                requests.push((headers, serde_json::from_str::<Value>(&body_text).unwrap()));
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
            json!({"prompt":"PROMPT_SECRET"}),
        ));
        observer.flush().await.unwrap();
        let requests = server.join().unwrap();
        let spans = &requests[0].1["data"]["attributes"]["spans"];
        assert_eq!(requests[0].1["data"]["type"], "span");
        assert_eq!(spans.as_array().unwrap().len(), 4);
        assert_eq!(spans[0]["meta"]["kind"], "agent");
        assert_eq!(spans[1]["meta"]["kind"], "llm");
        assert_eq!(spans[2]["meta"]["kind"], "tool");
        assert_eq!(spans[3]["meta"]["kind"], "workflow");
        assert_eq!(spans[2]["parent_id"], spans[3]["span_id"]);
        let metrics = &requests[1].1["series"];
        assert!(
            metrics
                .as_array()
                .unwrap()
                .iter()
                .any(|s| s["metric"] == "crabber.model.tokens.input")
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
