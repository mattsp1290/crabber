//! Redaction-first, nonblocking Datadog export for Crabber runtime events.
use crabber_core::{EventKind, EventRecord, TraceContext};
use crabber_runtime::{
    ModelPurpose, Observer, OperationKind, OperationalObservation, TerminalReason,
};
use flate2::{
    Compression,
    write::{GzEncoder, ZlibEncoder},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fmt,
    io::Write,
    sync::{
        Arc, Mutex,
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

/// Host-controlled metric identities. At most 32 sanitized values per dimension
/// are honored; all other runtime identities map to `overflow`.
#[derive(Debug, Clone, Default)]
pub struct MetricDimensions {
    pub providers: Vec<String>,
    pub models: Vec<String>,
    pub tools: Vec<String>,
}
fn dimension(value: &str, allowed: &[String]) -> String {
    fn sanitize(value: &str) -> String {
        value
            .chars()
            .take(100)
            .filter(|c| c.is_ascii_alphanumeric() || "._-/".contains(*c))
            .collect()
    }
    let value = sanitize(value);
    if !value.is_empty() && allowed.iter().take(32).any(|name| sanitize(name) == value) {
        value
    } else {
        "overflow".into()
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerStatus {
    Running,
    Stopped,
}
/// Cumulative host-local record/submission counts; queue and pending are current
/// observation depths. Last success is a completed nonempty batch, not a request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportHealth {
    pub accepted: u64,
    pub dropped: u64,
    pub retries: u64,
    pub failures: u64,
    pub queue_depth: u64,
    pub pending_depth: u64,
    pub last_success_unix_seconds: Option<u64>,
    pub worker_status: WorkerStatus,
}
#[derive(Default)]
struct Health {
    gate: Mutex<()>,
    accepted: AtomicU64,
    dropped: AtomicU64,
    retries: AtomicU64,
    resume: AtomicU64,
    failures: AtomicU64,
    outstanding: AtomicU64,
    queued: AtomicU64,
    pending: AtomicU64,
    last_success: AtomicU64,
    stopped: AtomicU64,
}
struct WorkerGuard(Arc<Health>);
impl Drop for WorkerGuard {
    fn drop(&mut self) {
        let _lock = self.0.gate.lock().expect("health gate");
        self.0.dropped.fetch_add(
            self.0.outstanding.swap(0, Ordering::SeqCst),
            Ordering::SeqCst,
        );
        self.0.pending.store(0, Ordering::SeqCst);
        self.0.queued.store(0, Ordering::SeqCst);
        self.0.stopped.store(1, Ordering::SeqCst);
    }
}
#[derive(Clone)]
struct Measurement {
    samples: Vec<Value>,
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
    pub metric_dimensions: MetricDimensions,
    /// Overrides both metrics and native LLM spans origins for a local intake test.
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
            metric_dimensions: MetricDimensions::default(),
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
    #[error("http export failed")]
    Http,
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
    #[error("{signal} intake reported {count} series error(s)")]
    IntakeErrors { signal: &'static str, count: usize },
    #[error("invalid or oversized intake acknowledgement")]
    IntakeResponse,
}

/// The export queue drops new observations on overflow and never waits in `emit`.
#[derive(Clone)]
pub struct DatadogObserver {
    tx: mpsc::Sender<Command>,
    health: Arc<Health>,
    abort: tokio::task::AbortHandle,
    dimensions: MetricDimensions,
    timeout: Duration,
    redaction: RedactionPolicy,
}
enum Command {
    Event(Box<SafeEvent>),
    Measurement(Measurement),
    Flush(oneshot::Sender<Result<(), ExportError>>),
    Shutdown(oneshot::Sender<Result<(), ExportError>>),
}
#[derive(Clone)]
struct SafeEvent {
    kind: EventKind,
    context: Option<TraceContext>,
    attempt: Option<String>,
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
    #[allow(clippy::too_many_lines)] // Construct the bounded allow-listed queue record together.
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
                    .take(100)
                    .filter(|c| c.is_ascii_alphanumeric() || "._-/".contains(*c))
                    .collect()
            })
        };
        Some(Self {
            kind: e.kind.clone(),
            context: None,
            attempt: None,
            session: e.session_id.to_string(),
            run: e.run_id.to_string(),
            time_ns: e.created_at.unix_timestamp_nanos(),
            provider: safe("provider"),
            model: safe("model"),
            tool: safe("name"),
            tool_id: safe("call_id"),
            status: safe("status").map(|status| match status.as_str() {
                "completed" | "paused" | "ok" => "ok".to_string(),
                _ => "error".to_string(),
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
    fn operational_completed(&self, observation: &OperationalObservation) {
        let reason = match observation.reason {
            TerminalReason::Success => "success",
            TerminalReason::ProviderError => "provider_error",
            TerminalReason::ToolError => "tool_error",
            TerminalReason::Cancelled => "cancelled",
            TerminalReason::LeaseLost => "lease_lost",
            TerminalReason::Paused => "paused",
            TerminalReason::RuntimeError => "runtime_error",
        };
        let mut labels = vec![format!("reason:{reason}")];
        let prefix = match &observation.kind {
            OperationKind::Run => "crabber.run",
            OperationKind::Model {
                purpose,
                provider,
                model,
            } => {
                labels.push(format!(
                    "provider:{}",
                    dimension(provider, &self.dimensions.providers)
                ));
                labels.push(format!(
                    "model:{}",
                    dimension(model, &self.dimensions.models)
                ));
                labels.push(format!(
                    "purpose:{}",
                    match purpose {
                        ModelPurpose::Turn => "turn",
                        ModelPurpose::Compaction => "compaction",
                    }
                ));
                "crabber.model"
            }
            OperationKind::Tool { name } => {
                labels.push(format!("tool:{}", dimension(name, &self.dimensions.tools)));
                "crabber.tool"
            }
        };
        let timestamp = time_now();
        let sample = |name: String, duration: Duration| json!({"metric":name,"points":[[timestamp,[duration.as_secs_f64()*1000.0]]],"tags":labels});
        let mut samples = vec![sample(format!("{prefix}.elapsed_ms"), observation.elapsed)];
        if matches!(observation.kind, OperationKind::Model { .. })
            && let Some(first) = observation.first_token
        {
            samples.push(sample("crabber.model.first_token_ms".into(), first));
        }
        let _ = self.enqueue(Command::Measurement(Measurement { samples }));
    }

    fn emit(&self, event: &EventRecord) {
        if let Some(event) = SafeEvent::from_event(event, &self.redaction) {
            let _ = self.enqueue(Command::Event(Box::new(event)));
        }
    }
    fn model_completed(&self, event: &EventRecord) {
        self.emit(event);
    }
    fn emit_with_context(&self, event: &EventRecord, context: Option<&TraceContext>) {
        if let Some(mut event) = SafeEvent::from_event(event, &self.redaction) {
            event.context = context.cloned();
            let _ = self.enqueue(Command::Event(Box::new(event)));
        }
    }
    fn model_completed_with_context(&self, event: &EventRecord, context: Option<&TraceContext>) {
        self.emit_with_context(event, context);
    }
    fn emit_in_attempt(
        &self,
        event: &EventRecord,
        context: Option<&TraceContext>,
        attempt: &crabber_core::RunId,
    ) {
        if let Some(mut event) = SafeEvent::from_event(event, &self.redaction) {
            event.context = context.cloned();
            event.attempt = Some(attempt.to_string());
            let _ = self.enqueue(Command::Event(Box::new(event)));
        }
    }
    fn model_completed_in_attempt(
        &self,
        event: &EventRecord,
        context: Option<&TraceContext>,
        attempt: &crabber_core::RunId,
    ) {
        self.emit_in_attempt(event, context, attempt);
    }
}
impl DatadogObserver {
    #[must_use]
    pub fn new(config: &DatadogConfig) -> Self {
        let (tx, rx) = mpsc::channel(config.channel_capacity.clamp(1, 4096));
        let health = Arc::new(Health::default());
        let handle = tokio::spawn(worker(rx, config.clone(), WorkerGuard(Arc::clone(&health))));
        Self {
            tx,
            health,
            abort: handle.abort_handle(),
            dimensions: config.metric_dimensions.clone(),
            timeout: config.timeout,
            redaction: config.redaction.clone(),
        }
    }
    fn enqueue(&self, command: Command) -> Result<(), ()> {
        let Ok(_lock) = self.health.gate.try_lock() else {
            self.health.dropped.fetch_add(1, Ordering::SeqCst);
            return Err(());
        };
        if self.health.stopped.load(Ordering::SeqCst) != 0 {
            self.health.dropped.fetch_add(1, Ordering::SeqCst);
            return Err(());
        }
        let Ok(permit) = self.tx.try_reserve() else {
            self.health.dropped.fetch_add(1, Ordering::SeqCst);
            return Err(());
        };
        self.health.outstanding.fetch_add(1, Ordering::SeqCst);
        self.health.queued.fetch_add(1, Ordering::SeqCst);
        self.health.accepted.fetch_add(1, Ordering::SeqCst);
        permit.send(command);
        Ok(())
    }
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.health.dropped.load(Ordering::SeqCst)
    }
    #[must_use]
    pub fn health(&self) -> ExportHealth {
        let pending = self.health.pending.load(Ordering::SeqCst);
        let last = self.health.last_success.load(Ordering::SeqCst);
        ExportHealth {
            accepted: self.health.accepted.load(Ordering::SeqCst),
            dropped: self.dropped(),
            retries: self.health.retries.load(Ordering::SeqCst),
            failures: self.health.failures.load(Ordering::SeqCst),
            queue_depth: self
                .health
                .queued
                .load(Ordering::SeqCst)
                .min(self.tx.max_capacity() as u64),
            pending_depth: pending,
            last_success_unix_seconds: (last > 0).then_some(last),
            worker_status: if self.health.stopped.load(Ordering::SeqCst) == 0 {
                WorkerStatus::Running
            } else {
                WorkerStatus::Stopped
            },
        }
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
        let result = tokio::time::timeout(self.timeout, async {
            self.tx
                .send(command)
                .await
                .map_err(|_| ExportError::WorkerStopped)?;
            rx.await.map_err(|_| ExportError::WorkerStopped)?
        })
        .await
        .map_err(|_| ExportError::Timeout)
        .and_then(|result| result);
        if shutdown {
            self.abort.abort();
        }
        result
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum ExportStage {
    Spans,
    Metrics,
    Distributions,
    Logs,
}
struct PendingBatch {
    events: Vec<SafeEvent>,
    measurements: Vec<Measurement>,
    records: usize,
    stage: ExportStage,
    dropped_snapshot: Option<u64>,
    unsent_parts: Vec<Value>,
}
impl Default for PendingBatch {
    fn default() -> Self {
        Self {
            events: Vec::new(),
            measurements: Vec::new(),
            records: 0,
            stage: ExportStage::Spans,
            dropped_snapshot: None,
            unsent_parts: Vec::new(),
        }
    }
}
async fn worker(mut rx: mpsc::Receiver<Command>, config: DatadogConfig, guard: WorkerGuard) {
    let health = Arc::clone(&guard.0);
    let _guard = guard;
    let Ok(client) = reqwest::Client::builder().timeout(config.timeout).build() else {
        return;
    };
    let mut pending = PendingBatch::default();
    let mut retrying = false;
    let batch_limit = config.batch_size.clamp(1, 1000);
    let mut tick = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(1),
        Duration::from_secs(1),
    );
    loop {
        tokio::select! {
            command=rx.recv() => match command {
                Some(Command::Event(event)) => {
                    health.queued.fetch_sub(1,Ordering::SeqCst);
                    if retrying || pending.records>=batch_limit {
                        health.dropped.fetch_add(1,Ordering::SeqCst);
                        health.outstanding.fetch_sub(1,Ordering::SeqCst);
                    } else {
                        pending.events.push(*event);
                        pending.records += 1;
                        health.pending.store(pending.records as u64, Ordering::SeqCst);
                        if pending.records>=batch_limit {
                            retrying=export(&client,&config,&mut pending,&health).await.is_err();
                        }
                    }
                }
                Some(Command::Measurement(measurement)) => {
                    health.queued.fetch_sub(1,Ordering::SeqCst);
                    if retrying || pending.records >= batch_limit {
                        health.dropped.fetch_add(1,Ordering::SeqCst);
                        health.outstanding.fetch_sub(1,Ordering::SeqCst);
                    } else {
                        pending.measurements.push(measurement);
                        pending.records += 1;
                        health.pending.store(pending.records as u64,Ordering::SeqCst);
                        if pending.records >= batch_limit { retrying=export(&client,&config,&mut pending,&health).await.is_err(); }
                    }
                }
                Some(Command::Flush(reply)) => {
                    if retrying { health.resume.store(1,Ordering::SeqCst); }
                    let result=export(&client,&config,&mut pending,&health).await;
                    retrying=result.is_err();
                    let _=reply.send(result);
                }
                Some(Command::Shutdown(reply)) => {
                    if retrying { health.resume.store(1,Ordering::SeqCst); }
                    let result=export(&client,&config,&mut pending,&health).await;
                    let _=reply.send(result);
                    break;
                }
                None => break,
            },
            _=tick.tick() => {
                if retrying || pending.records > 0 {
                    if retrying { health.resume.store(1,Ordering::SeqCst); }
                    retrying=export(&client,&config,&mut pending,&health).await.is_err();
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
fn metric_tags(c: &DatadogConfig, e: &SafeEvent) -> Vec<String> {
    let mut event = e.clone();
    event.provider = e
        .provider
        .as_deref()
        .map(|v| dimension(v, &c.metric_dimensions.providers));
    event.model = e
        .model
        .as_deref()
        .map(|v| dimension(v, &c.metric_dimensions.models));
    tags(c, &event)
}
fn datadog_id(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    let bytes: [u8; 8] = digest[..8].try_into().expect("sha256 has eight bytes");
    u64::from_be_bytes(bytes).max(1).to_string()
}
fn attempt_seed(e: &SafeEvent, attempt: &str) -> String {
    format!("{}:{}:{attempt}", e.session, e.run)
}
fn current_attempt(e: &SafeEvent) -> String {
    attempt_seed(e, e.attempt.as_deref().unwrap_or("legacy"))
}
fn llm_trace(seed: &str) -> String {
    let digest = Sha256::digest(format!("{seed}:llm-trace").as_bytes());
    format!(
        "{:032x}",
        u128::from_be_bytes(digest[..16].try_into().expect("digest length")).max(1)
    )
}
fn apm_trace(trace: &str) -> String {
    let number = u128::from_str_radix(trace, 16).expect("validated trace");
    if number > u128::from(u64::MAX) {
        format!("{number:032x}")
    } else {
        number.to_string()
    }
}
fn spans(e: &SafeEvent) -> Vec<Value> {
    let mut result = Vec::new();
    if matches!(e.kind, EventKind::RunStarted | EventKind::RunResumed) {
        result.push(span_value(
            e,
            "crabber.attempt.admission",
            "agent",
            &format!("{}-session", e.session),
            None,
        ));
    }
    let (name, kind, id_seed, parent_seed) = match &e.kind {
        EventKind::RunStarted | EventKind::RunResumed => (
            "crabber.workflow.admission",
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
    let duration = if matches!(e.kind, EventKind::RunStarted | EventKind::RunResumed) {
        1
    } else if kind == "llm" {
        e.latency_ns.max(1)
    } else {
        e.duration_ns.max(1)
    };
    let attempt = current_attempt(e);
    let mut value = json!({"name":name,"span_id":datadog_id(&format!("{attempt}:{id_seed}")),"trace_id":llm_trace(&attempt),
        "parent_id":parent_seed.map_or_else(|| "undefined".to_string(),|p| datadog_id(&format!("{attempt}:{p}"))),
        "start_ns":e.time_ns-duration,"duration":duration,"meta":meta,
        "status":e.status.as_deref().unwrap_or("ok"),
        "metrics":{"input_tokens":e.input_tokens,"output_tokens":e.output_tokens,"total_tokens":e.input_tokens+e.output_tokens},
        "session_id":e.session});
    value["_dd"] = json!({});
    if let Some(context) = &e.context {
        value["_dd"] = json!({"apm_trace_id":apm_trace(context.trace_id()),"trace_id":apm_trace(context.trace_id()),"span_id":u64::from_str_radix(context.span_id(),16).expect("validated span").to_string()});
        if kind == "agent"
            && let Some(prior) = context.predecessor()
            && let Some(attempt) = prior.observation_attempt()
        {
            let seed = attempt_seed(e, &attempt.to_string());
            value["span_links"] = json!([{"trace_id":llm_trace(&seed),"span_id":datadog_id(&format!("{seed}:{}-session",e.session)),"attributes":{"from":"output","to":"input"}}]);
        }
    }
    value
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
            "points":[{"timestamp":e.time_ns/1_000_000_000,"value":value}],"tags":metric_tags(c,e)});
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
    let mut value = json!({"message":message,"status":status,"service":c.service,"ddsource":"crabber","ddtags":tags(c,e).join(","),"session_id":e.session,"run_id":e.run,"tool_call_id":e.tool_id,"verify_marker":c.tags.iter().find_map(|tag| tag.strip_prefix("verify:"))});
    if let Some(context) = &e.context {
        value["dd.trace_id"] = json!(apm_trace(context.trace_id()));
        value["dd.span_id"] = json!(
            u64::from_str_radix(context.span_id(), 16)
                .expect("validated span")
                .to_string()
        );
    }
    Some(value)
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
    if url.contains("llmobs") {
        "llmobs"
    } else if url.contains("distribution_points") {
        "distributions"
    } else if url.contains("/series") {
        "metrics"
    } else {
        "logs"
    }
}
fn rejected(response: &reqwest::Response, url: &str) -> ExportError {
    let status = response.status();
    let diagnostic = "intake rejected observation".to_string();
    ExportError::Rejected {
        signal: intake_signal(url),
        status,
        diagnostic,
    }
}
async fn acknowledge(
    mut response: reqwest::Response,
    health: &Health,
    signal: &'static str,
) -> Result<(), ExportError> {
    let result = async {
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| ExportError::Http)? {
            if bytes.len() + chunk.len() > 65536 {
                return Err(ExportError::IntakeResponse);
            }
            bytes.extend_from_slice(&chunk);
        }
        let body: Value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).map_err(|_| ExportError::IntakeResponse)?
        };
        let errors = body["errors"].as_array().map_or(0, Vec::len);
        if errors > 0 {
            return Err(ExportError::IntakeErrors {
                signal,
                count: errors,
            });
        }
        Ok(())
    }
    .await;
    if result.is_err() {
        health.failures.fetch_add(1, Ordering::SeqCst);
    }
    result
}
async fn post(
    client: &reqwest::Client,
    config: &DatadogConfig,
    url: String,
    parts: &mut Vec<Value>,
    health: &Health,
) -> Result<(), ExportError> {
    'part: while let Some(body) = parts.last().cloned() {
        let raw = body.to_string();
        if raw.len() > config.max_payload_bytes {
            let (first, second) = split_payload(&body).ok_or(ExportError::PayloadTooLarge)?;
            parts.pop();
            parts.push(second);
            parts.push(first);
            continue;
        }
        let is_llmobs = url.contains("llmobs");
        let distribution = url.contains("distribution_points");
        let bytes = if is_llmobs {
            raw.into_bytes()
        } else if distribution {
            let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(raw.as_bytes())?;
            encoder.finish()?
        } else {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder.write_all(raw.as_bytes())?;
            encoder.finish()?
        };
        for attempt in 0..3 {
            if attempt > 0 || health.resume.swap(0, Ordering::SeqCst) > 0 {
                health.retries.fetch_add(1, Ordering::SeqCst);
            }
            let request = client
                .post(&url)
                .header("DD-API-KEY", &config.api_key)
                .header("Content-Type", "application/json");
            let request = if is_llmobs {
                request
            } else {
                request.header(
                    "Content-Encoding",
                    if distribution { "deflate" } else { "gzip" },
                )
            };
            let result = request.body(bytes.clone()).send().await;
            if !result
                .as_ref()
                .is_ok_and(|response| response.status().is_success())
            {
                health.failures.fetch_add(1, Ordering::SeqCst);
            }
            match result {
                Ok(response) if response.status().is_success() => {
                    if url.contains("/series") || distribution {
                        acknowledge(response, health, intake_signal(&url)).await?;
                    }
                    parts.pop();
                    continue 'part;
                }
                Ok(response) if response.status().as_u16() == 413 => {
                    let (first, second) =
                        split_payload(&body).ok_or(ExportError::PayloadTooLarge)?;
                    parts.pop();
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
                Ok(response) => return Err(rejected(&response, &url)),
                Err(_) if attempt == 2 => return Err(ExportError::Http),
                Err(_) => (),
            }
            tokio::time::sleep(Duration::from_millis(200 * (1 << attempt))).await;
        }
    }
    Ok(())
}
#[allow(clippy::too_many_lines)] // Preserve ordered stage progress in one state machine.
async fn export(
    client: &reqwest::Client,
    config: &DatadogConfig,
    pending: &mut PendingBatch,
    health: &Health,
) -> Result<(), ExportError> {
    if pending.stage == ExportStage::Spans {
        if pending.unsent_parts.is_empty() {
            let spans: Vec<_> = pending
                .events
                .iter()
                .flat_map(|event| spans(event).into_iter().map(move |span| (event, span)))
                .map(|(event, mut span)| {
                    let mut labels = tags(config, event);
                    labels.push(format!("ml_app:{}", config.ml_app));
                    labels.push("language:rust".into());
                    span["tags"] = json!(labels);
                    span["service"] = json!(config.service);
                    span
                })
                .collect();
            if !spans.is_empty() {
                let envelopes: Vec<_> = spans.into_iter().map(|span| json!({"_dd.stage":"raw","_dd.tracer_version":concat!("crabber-",env!("CARGO_PKG_VERSION")),"event_type":"span","spans":[span]})).collect();
                pending.unsent_parts.push(json!(envelopes));
            }
        }
        post(
            client,
            config,
            format!(
                "{}/api/v2/llmobs",
                config
                    .api_origin
                    .clone()
                    .unwrap_or_else(|| format!("https://llmobs-intake.{}", config.site))
            ),
            &mut pending.unsent_parts,
            health,
        )
        .await?;
        pending.stage = ExportStage::Metrics;
    }
    if pending.stage == ExportStage::Metrics {
        let dropped_count = *pending
            .dropped_snapshot
            .get_or_insert_with(|| health.dropped.load(Ordering::SeqCst));
        if pending.unsent_parts.is_empty() {
            let mut metrics: Vec<_> = pending
                .events
                .iter()
                .flat_map(|e| series(config, e))
                .collect();
            if pending.records > 0 {
                metrics.push(json!({"metric":"crabber.export.batches","type":1,"interval":1,"points":[{"timestamp":time_now(),"value":1}],
                    "tags":[format!("service:{}",config.service),format!("env:{}",config.env),"outcome:ok"]}));
            }
            if dropped_count > 0 {
                metrics.push(json!({"metric":"crabber.export.dropped","type":3,"points":[{"timestamp":time_now(),"value":dropped_count}],
                    "tags":[format!("service:{}",config.service),format!("env:{}",config.env)]}));
            }
            if !metrics.is_empty() {
                pending.unsent_parts.push(json!({"series":metrics}));
            }
        }
        post(
            client,
            config,
            format!("{}/api/v2/series", config.api_origin()),
            &mut pending.unsent_parts,
            health,
        )
        .await?;
        pending.stage = ExportStage::Distributions;
    }
    if pending.stage == ExportStage::Distributions {
        if pending.unsent_parts.is_empty() {
            let samples: Vec<_> = pending
                .measurements
                .iter()
                .flat_map(|m| m.samples.iter().cloned())
                .map(|mut sample| {
                    let tags = sample["tags"].as_array_mut().expect("measurement tags");
                    tags.push(json!(format!("service:{}", config.service)));
                    tags.push(json!(format!("env:{}", config.env)));
                    tags.push(json!(format!("version:{}", config.version)));
                    sample
                })
                .collect();
            if !samples.is_empty() {
                pending.unsent_parts.push(json!({"series":samples}));
            }
        }
        post(
            client,
            config,
            format!("{}/api/v1/distribution_points", config.api_origin()),
            &mut pending.unsent_parts,
            health,
        )
        .await?;
        pending.stage = ExportStage::Logs;
    }
    if pending.stage == ExportStage::Logs {
        if pending.unsent_parts.is_empty() {
            let logs: Vec<_> = pending
                .events
                .iter()
                .filter_map(|e| log(config, e))
                .collect();
            if !logs.is_empty() {
                pending.unsent_parts.push(json!(logs));
            }
        }
        post(
            client,
            config,
            format!("{}/api/v2/logs", config.logs_origin()),
            &mut pending.unsent_parts,
            health,
        )
        .await?;
        if pending.records > 0 {
            health
                .last_success
                .store(u64::try_from(time_now()).unwrap_or(0), Ordering::SeqCst);
        }
        health
            .outstanding
            .fetch_sub(pending.records as u64, Ordering::SeqCst);
        health.pending.store(0, Ordering::SeqCst);
        pending.records = 0;
        pending.measurements.clear();
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
    use crabber_core::{EventKind, EventRecord, TraceContext};
    pub fn emit(event: &EventRecord) {
        emit_with_context(event, None);
    }
    /// Emits only approved identity; subscriber installation remains host-owned.
    pub fn emit_with_context(event: &EventRecord, context: Option<&TraceContext>) {
        let kind = match event.kind {
            EventKind::RunStarted => "run.started",
            EventKind::RunSettled => "run.settled",
            EventKind::ToolCallSettled => "tool.settled",
            _ => return,
        };
        tracing::info!(session.id=%event.session_id, run.id=%event.run_id,
            host.trace_id=context.map(TraceContext::trace_id),
            host.span_id=context.map(TraceContext::span_id), kind, "crabber event");
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
    fn span_payload(body: &Value) -> Vec<Value> {
        body.as_array()
            .unwrap()
            .iter()
            .filter_map(|envelope| envelope.get("spans").and_then(Value::as_array))
            .flatten()
            .cloned()
            .collect()
    }
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
            metric_dimensions: MetricDimensions::default(),
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
        if headers.contains("/api/v2/llmobs") {
            assert!(!headers.contains("content-encoding: gzip"));
            body_text = String::from_utf8(all[head_end..head_end + length].to_vec()).unwrap();
        } else if headers.contains("distribution_points") {
            assert!(headers.contains("content-encoding: deflate"));
            flate2::read::ZlibDecoder::new(&all[head_end..head_end + length])
                .read_to_string(&mut body_text)
                .unwrap();
        } else {
            assert!(headers.contains("content-encoding: gzip"));
            GzDecoder::new(&all[head_end..head_end + length])
                .read_to_string(&mut body_text)
                .unwrap();
        }
        assert!(!body_text.contains("PROMPT_SECRET"));
        (headers, serde_json::from_str(&body_text).unwrap())
    }
    #[tokio::test]
    async fn all_intake_and_transport_diagnostics_hide_response_and_endpoint_secrets() {
        for path in ["/api/v2/llmobs", "/api/v2/series", "/api/v2/logs"] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let url = format!("http://{}{path}", listener.local_addr().unwrap());
            let server = std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                let _ = read_request(&mut stream);
                let body = r#"{"errors":["ALPHABETICSECRET /private/PRIVATE_PATH_SECRET CREDENTIAL_SECRET"]}"#;
                write!(
                    stream,
                    "HTTP/1.1 400 Bad Request\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                )
                .unwrap();
            });
            let client = reqwest::Client::new();
            let mut parts = vec![json!({"safe":true})];
            let error = post(&client, &config(), url, &mut parts, &Health::default())
                .await
                .unwrap_err();
            let diagnostic = format!("{error} {error:?}");
            for secret in [
                "ALPHABETICSECRET",
                "PRIVATE_PATH_SECRET",
                "CREDENTIAL_SECRET",
                "/private/",
            ] {
                assert!(!diagnostic.contains(secret));
            }
            server.join().unwrap();
        }
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let mut parts = vec![json!({"safe":true})];
        let error = post(
            &reqwest::Client::new(),
            &config(),
            format!("http://{address}/PRIVATE_PATH_SECRET?token=CREDENTIAL_SECRET"),
            &mut parts,
            &Health::default(),
        )
        .await
        .unwrap_err();
        let diagnostic = format!("{error} {error:?}");
        assert!(!diagnostic.contains("SECRET"));
        assert!(!diagnostic.contains("http://"));
    }
    #[test]
    fn context_free_spans_and_logs_do_not_fabricate_host_identity() {
        let record = event(
            EventKind::RunSettled,
            &SessionId::new(),
            &RunId::new(),
            json!({"status":"paused"}),
        );
        let safe = SafeEvent::from_event(&record, &RedactionPolicy::default()).unwrap();
        assert_eq!(safe.status.as_deref(), Some("ok"));
        for span in spans(&safe) {
            assert!(span["_dd"].get("apm_trace_id").is_none());
        }
        let log = log(&config(), &safe).unwrap();
        assert!(log.get("dd.trace_id").is_none());
        assert!(log.get("dd.span_id").is_none());
    }
    #[tokio::test]
    async fn native_span_chunks_resume_after_413_and_later_chunk_failure() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}/api/v2/llmobs", listener.local_addr().unwrap());
        let recovered = Arc::new(AtomicBool::new(false));
        let ready = recovered.clone();
        let server = std::thread::spawn(move || {
            let mut split = false;
            let mut accepted = Vec::new();
            while accepted.len() < 3 {
                let (mut stream, _) = listener.accept().unwrap();
                let (_, body) = read_request(&mut stream);
                let spans = span_payload(&body);
                let status = if !split {
                    assert_eq!(spans.len(), 3);
                    split = true;
                    "413 Payload Too Large"
                } else if accepted.is_empty() || ready.load(Ordering::SeqCst) {
                    accepted.extend(spans.iter().cloned());
                    "202 Accepted"
                } else {
                    "503 Service Unavailable"
                };
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .unwrap();
            }
            accepted
        });
        let mut parts = vec![
            json!([{"_dd.stage":"raw","event_type":"span","spans":[1]},{"_dd.stage":"raw","event_type":"span","spans":[2]},{"_dd.stage":"raw","event_type":"span","spans":[3]}]),
        ];
        let client = reqwest::Client::new();
        assert!(
            post(
                &client,
                &config(),
                url.clone(),
                &mut parts,
                &Health::default()
            )
            .await
            .is_err()
        );
        recovered.store(true, Ordering::SeqCst);
        post(&client, &config(), url, &mut parts, &Health::default())
            .await
            .unwrap();
        assert_eq!(server.join().unwrap(), vec![json!(1), json!(2), json!(3)]);
    }
    #[test]
    fn legacy_host_predecessor_does_not_fabricate_a_native_llm_link() {
        let prior = TraceContext::new("0000000000000001", "1234567890abcdef").unwrap();
        let context = TraceContext::new("0000000000000002", "1234567890abcdef")
            .unwrap()
            .linked_to(&prior)
            .unwrap();
        let record = event(
            EventKind::RunStarted,
            &SessionId::new(),
            &RunId::new(),
            Value::Null,
        );
        let mut safe = SafeEvent::from_event(&record, &RedactionPolicy::default()).unwrap();
        safe.context = Some(context);
        for span in spans(&safe) {
            assert!(span.get("span_links").is_none());
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
        let body = json!([{"_dd.stage":"raw","event_type":"span","spans":[1]},{"_dd.stage":"raw","event_type":"span","spans":[2]},{"_dd.stage":"raw","event_type":"span","spans":[3]}]);
        let (a, b) = split_payload(&body).unwrap();
        assert_eq!(a[0]["spans"], json!([1]));
        assert_eq!(b[0]["spans"], json!([2]));
        assert_eq!(b[1]["spans"], json!([3]));
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
        c.channel_capacity = 2;
        c.batch_size = 100;
        let observer = DatadogObserver::new(&c);
        let session = SessionId::new();
        let run = RunId::new();
        observer.emit(&event(EventKind::RunStarted, &session, &run, Value::Null));
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
        let outage = observer.health();
        assert_eq!(outage.accepted, 2);
        assert_eq!(outage.dropped, 200);
        assert_eq!(outage.pending_depth, 2);
        assert_eq!(outage.queue_depth, 0);
        assert_eq!(outage.last_success_unix_seconds, None);
        assert_eq!(outage.failures, 3);
        recovered.store(true, Ordering::SeqCst);
        observer.flush().await.unwrap();
        let healthy = observer.health();
        assert_eq!(healthy.accepted, outage.accepted);
        assert_eq!(healthy.dropped, outage.dropped);
        assert_eq!(healthy.pending_depth, 0);
        assert_eq!(healthy.queue_depth, 0);
        assert!(healthy.last_success_unix_seconds.is_some());
        assert!(healthy.retries > outage.retries);
        let accepted = server.join().unwrap();
        assert!(accepted[0].0.contains("/api/v2/llmobs"));
        assert_eq!(span_payload(&accepted[0].1).len(), 2);
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
    #[tokio::test]
    async fn retry_resumes_after_accepted_metric_chunk() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let recovered = Arc::new(AtomicBool::new(false));
        let recovered_server = Arc::clone(&recovered);
        let server = std::thread::spawn(move || {
            let mut split_sent = false;
            let mut accepted_first = false;
            let mut accepted_metrics = Vec::new();
            loop {
                let (mut stream, _) = listener.accept().unwrap();
                let (headers, body) = read_request(&mut stream);
                let response = if headers.contains("/api/v2/series") {
                    let items = body["series"].as_array().unwrap();
                    if !split_sent {
                        assert_eq!(items.len(), 3);
                        split_sent = true;
                        b"HTTP/1.1 413 Payload Too Large\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".as_slice()
                    } else if !accepted_first {
                        assert_eq!(items.len(), 1);
                        accepted_first = true;
                        accepted_metrics.extend(items.iter().cloned());
                        b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            .as_slice()
                    } else if recovered_server.load(Ordering::SeqCst) {
                        accepted_metrics.extend(items.iter().cloned());
                        b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            .as_slice()
                    } else {
                        b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".as_slice()
                    }
                } else {
                    b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        .as_slice()
                };
                std::io::Write::write_all(&mut stream, response).unwrap();
                if headers.contains("/api/v2/logs") {
                    return accepted_metrics;
                }
            }
        });
        let mut c = config();
        c.api_origin = Some(origin.clone());
        c.logs_origin = Some(origin);
        let observer = DatadogObserver::new(&c);
        let session = SessionId::new();
        let run = RunId::new();
        observer.emit(&event(EventKind::RunStarted, &session, &run, Value::Null));
        observer.emit(&event(
            EventKind::RunSettled,
            &session,
            &run,
            json!({"status":"ok","duration_ms":3,"prompt":"PROMPT_SECRET"}),
        ));
        assert!(observer.flush().await.is_err());
        recovered.store(true, Ordering::SeqCst);
        observer.flush().await.unwrap();
        let accepted_metrics = server.join().unwrap();
        let mut names: Vec<_> = accepted_metrics
            .iter()
            .map(|item| item["metric"].as_str().unwrap())
            .collect();
        names.sort_unstable();
        assert_eq!(
            names,
            [
                "crabber.export.batches",
                "crabber.run.count",
                "crabber.run.duration_ms"
            ]
        );
    }

    async fn stopped(observer: &DatadogObserver) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while observer.health().worker_status != WorkerStatus::Stopped {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("worker cancellation must become terminal");
    }
    fn measurement(
        kind: OperationKind,
        elapsed: u64,
        first: Option<u64>,
    ) -> OperationalObservation {
        OperationalObservation {
            session_id: SessionId::new(),
            run_id: RunId::new(),
            kind,
            reason: TerminalReason::Cancelled,
            elapsed: Duration::from_millis(elapsed),
            first_token: first.map(Duration::from_millis),
        }
    }
    #[tokio::test]
    async fn typed_distributions_preserve_values_counts_and_finite_dimensions() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let mut requests = Vec::new();
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().unwrap();
                requests.push(read_request(&mut stream));
                stream
                    .write_all(
                        b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .unwrap();
            }
            requests
        });
        let mut c = config();
        c.api_origin = Some(origin.clone());
        c.logs_origin = Some(origin);
        c.batch_size = 1000;
        c.channel_capacity = 4096;
        c.metric_dimensions.providers = vec!["fake".into()];
        c.metric_dimensions.models = vec!["demo".into()];
        c.metric_dimensions.tools = vec!["echo".into()];
        let observer = DatadogObserver::new(&c);
        observer.operational_completed(&measurement(OperationKind::Run, 50, None));
        observer.operational_completed(&measurement(
            OperationKind::Tool {
                name: "echo".into(),
            },
            10,
            None,
        ));
        for i in 0..100 {
            observer.operational_completed(&measurement(
                OperationKind::Model {
                    purpose: if i % 2 == 0 {
                        ModelPurpose::Turn
                    } else {
                        ModelPurpose::Compaction
                    },
                    provider: if i == 0 {
                        "fake".into()
                    } else {
                        format!("provider-{i}")
                    },
                    model: if i == 0 {
                        "demo".into()
                    } else {
                        format!("model-{i}")
                    },
                },
                20,
                if i < 2 { Some(3) } else { None },
            ));
        }
        observer.flush().await.unwrap();
        let requests = server.join().unwrap();
        assert!(requests[0].0.contains("/api/v2/series"));
        assert!(requests[1].0.contains("/api/v1/distribution_points"));
        let series = requests[1].1["series"].as_array().unwrap();
        assert_eq!(series.len(), 104);
        for (name, value, count) in [
            ("crabber.run.elapsed_ms", 50.0, 1),
            ("crabber.tool.elapsed_ms", 10.0, 1),
            ("crabber.model.elapsed_ms", 20.0, 100),
            ("crabber.model.first_token_ms", 3.0, 2),
        ] {
            let samples: Vec<_> = series.iter().filter(|s| s["metric"] == name).collect();
            assert_eq!(samples.len(), count);
            assert!(samples.iter().all(|s| s["points"][0][1] == json!([value])));
        }
        let tags: std::collections::BTreeSet<_> = series
            .iter()
            .flat_map(|s| {
                s["tags"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|t| t.as_str().unwrap())
            })
            .collect();
        assert!(tags.contains("provider:overflow") && tags.contains("model:overflow"));
        assert!(tags.contains("purpose:turn") && tags.contains("purpose:compaction"));
        assert!(!tags.iter().any(|tag| {
            ["session", "run_id", "attempt", "trace", "span", "call"]
                .iter()
                .any(|name| tag.starts_with(name))
        }));
        let health = observer.health();
        assert_eq!(health.accepted, 102);
        assert_eq!(health.dropped, 0);
        assert_eq!(health.queue_depth, 0);
        assert_eq!(health.pending_depth, 0);
        assert!(health.last_success_unix_seconds.is_some());
        observer.shutdown().await.unwrap();
        stopped(&observer).await;
        assert_eq!(observer.health().worker_status, WorkerStatus::Stopped);
    }
    #[tokio::test]
    async fn distribution_split_retry_never_replays_completed_chunks_or_stages() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let mut paths = Vec::new();
            let mut values = Vec::new();
            let mut step = 0;
            let mut logs = 0;
            loop {
                let (mut stream, _) = listener.accept().unwrap();
                let (headers, body) = read_request(&mut stream);
                let path = headers.lines().next().unwrap().to_string();
                paths.push(path.clone());
                let status = if path.contains("distribution_points") {
                    step += 1;
                    match step {
                        1 => "413 Payload Too Large",
                        3 => "403 Forbidden",
                        _ => {
                            values.extend(
                                body["series"]
                                    .as_array()
                                    .unwrap()
                                    .iter()
                                    .map(|s| s["points"][0][1][0].as_f64().unwrap()),
                            );
                            "202 Accepted"
                        }
                    }
                } else if path.contains("/api/v2/logs") {
                    logs += 1;
                    if logs == 1 {
                        "400 Bad Request"
                    } else {
                        "202 Accepted"
                    }
                } else {
                    "202 Accepted"
                };
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                )
                .unwrap();
                if logs == 2 {
                    return (paths, values);
                }
            }
        });
        let mut c = config();
        c.api_origin = Some(origin.clone());
        c.logs_origin = Some(origin);
        let observer = DatadogObserver::new(&c);
        let session = SessionId::new();
        let run = RunId::new();
        observer.emit(&event(EventKind::RunStarted, &session, &run, Value::Null));
        observer.emit(&event(
            EventKind::RunSettled,
            &session,
            &run,
            json!({"status":"ok"}),
        ));
        for value in [1, 2, 3, 4] {
            observer.operational_completed(&measurement(OperationKind::Run, value, None));
        }
        assert!(observer.flush().await.is_err());
        assert_eq!(observer.health().last_success_unix_seconds, None);
        assert!(observer.flush().await.is_err());
        assert_eq!(observer.health().last_success_unix_seconds, None);
        observer.flush().await.unwrap();
        let (paths, values) = server.join().unwrap();
        assert_eq!(values, [1.0, 2.0, 3.0, 4.0]);
        assert_eq!(
            paths
                .iter()
                .filter(|p| p.contains("/api/v2/series"))
                .count(),
            1
        );
        assert_eq!(observer.health().failures, 3);
        assert!(observer.health().retries >= 1);
        observer.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn bounded_shutdown_is_terminal_offline_and_overflow_health_is_cumulative() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let mut c = config();
        c.api_origin = Some(origin.clone());
        c.logs_origin = Some(origin);
        c.timeout = Duration::from_millis(40);
        c.channel_capacity = 2;
        let observer = DatadogObserver::new(&c);
        for _ in 0..100 {
            observer.operational_completed(&measurement(OperationKind::Run, 1, None));
        }
        assert_eq!(observer.health().accepted, 2);
        assert_eq!(observer.health().dropped, 98);
        let start = tokio::time::Instant::now();
        assert!(observer.shutdown().await.is_err());
        assert!(start.elapsed() < Duration::from_millis(200));
        stopped(&observer).await;
        let health = observer.health();
        assert_eq!(health.worker_status, WorkerStatus::Stopped);
        assert_eq!(health.dropped, 100);
        assert_eq!(health.queue_depth, 0);
        assert_eq!(health.pending_depth, 0);
        assert_eq!(health.last_success_unix_seconds, None);
        assert!(health.failures > 0);
        observer.operational_completed(&measurement(OperationKind::Run, 1, None));
        assert_eq!(observer.health().dropped, 101);
    }

    #[tokio::test]
    async fn distribution_status_and_acknowledgement_errors_are_safe_and_counted() {
        for status in [400, 403, 429, 500, 200, 202] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let url = format!(
                "http://{}/api/v1/distribution_points",
                listener.local_addr().unwrap()
            );
            let attempts = if status == 429 || status == 500 { 3 } else { 1 };
            let server = std::thread::spawn(move || {
                for _ in 0..attempts {
                    let (mut stream, _) = listener.accept().unwrap();
                    let _ = read_request(&mut stream);
                    let body = r#"{"errors":["PROMPT_SECRET RESPONSE_SECRET PRIVATE_URL_SECRET"]}"#;
                    write!(stream,"HTTP/1.1 {status} Error\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).unwrap();
                }
            });
            let health = Health::default();
            let mut parts =
                vec![json!({"series":[{"metric":"crabber.run.elapsed_ms","points":[[1,[2.0]]]}]})];
            let error = post(&reqwest::Client::new(), &config(), url, &mut parts, &health)
                .await
                .unwrap_err();
            let message = format!("{error} {error:?}");
            for secret in [
                "PROMPT_SECRET",
                "RESPONSE_SECRET",
                "PRIVATE_URL_SECRET",
                "http://",
            ] {
                assert!(!message.contains(secret));
            }
            assert_eq!(health.failures.load(Ordering::SeqCst), attempts);
            assert_eq!(health.retries.load(Ordering::SeqCst), attempts - 1);
            assert_eq!(parts.len(), 1);
            server.join().unwrap();
        }
    }
    #[tokio::test]
    async fn slow_intake_flush_timeout_remains_live_shutdown_cancels_worker() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let (arrived_tx, arrived_rx) = oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let _ = read_request(&mut stream);
            let _ = arrived_tx.send(());
            release_rx.recv().unwrap();
        });
        let mut c = config();
        c.api_origin = Some(origin);
        c.timeout = Duration::from_millis(60);
        let observer = DatadogObserver::new(&c);
        observer.operational_completed(&measurement(OperationKind::Run, 1, None));
        let flushing = observer.clone();
        let task = tokio::spawn(async move { flushing.flush().await });
        arrived_rx.await.unwrap();
        let health = observer.health();
        assert_eq!(health.pending_depth, 1);
        assert_eq!(health.queue_depth, 0);
        assert_eq!(health.last_success_unix_seconds, None);
        assert!(task.await.unwrap().is_err());
        assert_eq!(observer.health().worker_status, WorkerStatus::Running);
        assert!(observer.shutdown().await.is_err());
        stopped(&observer).await;
        assert_eq!(observer.health().worker_status, WorkerStatus::Stopped);
        assert_eq!(observer.health().dropped, 1);
        release_tx.send(()).unwrap();
        server.join().unwrap();
    }
    #[test]
    fn dimension_allowlist_has_a_hard_cap_and_sanitized_overflow() {
        let allowed: Vec<_> = (0..100).map(|i| format!("model-{i}")).collect();
        assert_eq!(dimension("model-31", &allowed), "model-31");
        assert_eq!(dimension("model-32", &allowed), "overflow");
        assert_eq!(dimension("", &allowed), "overflow");
        assert_eq!(dimension("model-1 SECRET", &allowed), "overflow");
        assert_eq!(
            dimension(&format!("{}x", "!".repeat(1000)), &["x".into()]),
            "overflow"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_producers_keep_local_observation_depths_bounded() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let mut c = config();
        c.api_origin = Some(origin);
        c.channel_capacity = 8;
        c.batch_size = 4;
        c.timeout = Duration::from_millis(40);
        let observer = DatadogObserver::new(&c);
        std::thread::scope(|scope| {
            for _ in 0..4 {
                let observer = observer.clone();
                scope.spawn(move || {
                    for _ in 0..1000 {
                        observer.operational_completed(&measurement(OperationKind::Run, 1, None));
                        let health = observer.health();
                        assert!(health.queue_depth <= 8);
                        assert!(health.pending_depth <= 4);
                    }
                });
            }
        });
        let _ = observer.shutdown().await;
        stopped(&observer).await;
        let health = observer.health();
        assert_eq!(health.worker_status, WorkerStatus::Stopped);
        assert_eq!(health.dropped, 4000);
        assert_eq!(health.queue_depth, 0);
        assert_eq!(health.pending_depth, 0);
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
            for _ in 0..4 {
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
        let spans = span_payload(&requests[0].1);
        let agent = spans.iter().find(|s| s["meta"]["kind"] == "agent").unwrap();
        let workflow = spans
            .iter()
            .find(|s| s["meta"]["kind"] == "workflow")
            .unwrap();
        assert_eq!(workflow["status"], "ok");
        assert_eq!(workflow["parent_id"], agent["span_id"]);
        let llm_statuses: Vec<_> = spans
            .iter()
            .filter(|s| s["meta"]["kind"] == "llm")
            .map(|s| s["status"].as_str().unwrap())
            .collect();
        assert_eq!(llm_statuses, ["ok", "error"]);
        for kind in ["llm", "tool"] {
            let child = spans.iter().find(|s| s["meta"]["kind"] == kind).unwrap();
            assert_eq!(child["parent_id"], workflow["span_id"]);
            assert!(child["duration"].as_i64().unwrap() > 0);
        }
        assert!(
            requests[1].1["series"]
                .as_array()
                .unwrap()
                .iter()
                .any(|m| m["metric"] == "crabber.run.count")
        );
        assert!(
            requests[3]
                .1
                .as_array()
                .unwrap()
                .iter()
                .any(|l| l["message"] == "run settled" && l["status"] == "error")
        );
    }
    #[tokio::test]
    #[allow(clippy::too_many_lines)] // Each provider terminal path is checked against a real exporter.
    async fn invalid_model_streams_export_error_llm_spans() {
        use crabber_extension::StaticPlanProvider;
        use crabber_providers::{
            FakeProvider, ProviderError, ProviderErrorKind, Selection, StreamDelta,
        };
        use crabber_runtime::{Orchestrator, Request};
        use crabber_session::MemoryStore;
        let cases = [
            vec![StreamDelta::TextDelta("PROMPT_SECRET".into())],
            vec![StreamDelta::Completed],
            vec![StreamDelta::Error(ProviderError {
                kind: ProviderErrorKind::Server,
                message: "PROMPT_SECRET".into(),
                retryable: false,
            })],
        ];
        for deltas in cases {
            let expected_first = deltas
                .iter()
                .any(|delta| matches!(delta,StreamDelta::TextDelta(text) if !text.is_empty()));
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let origin = format!("http://{}", listener.local_addr().unwrap());
            let server = std::thread::spawn(move || {
                let mut requests = Vec::new();
                for _ in 0..4 {
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
            let runtime = Orchestrator::builder()
                .store(Arc::new(MemoryStore::new()))
                .resolver(Arc::new(FakeProvider::scripted(vec![deltas])))
                .plan_provider(Arc::new(StaticPlanProvider::new(vec![], vec![])))
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
            let distributions = requests[2].1["series"].as_array().unwrap();
            assert_eq!(
                distributions
                    .iter()
                    .filter(|s| s["metric"] == "crabber.model.elapsed_ms")
                    .count(),
                1
            );
            assert_eq!(
                distributions
                    .iter()
                    .filter(|s| s["metric"] == "crabber.run.elapsed_ms")
                    .count(),
                1
            );
            assert_eq!(
                distributions
                    .iter()
                    .filter(|s| s["metric"] == "crabber.model.first_token_ms")
                    .count(),
                usize::from(expected_first)
            );
            assert!(distributions.iter().all(|s| {
                s["points"][0][1][0]
                    .as_f64()
                    .is_some_and(|v| v.is_finite() && v >= 0.0)
            }));
            let spans = span_payload(&requests[0].1);
            let workflow = spans
                .iter()
                .find(|s| s["meta"]["kind"] == "workflow")
                .unwrap();
            let llm = spans.iter().find(|s| s["meta"]["kind"] == "llm").unwrap();
            assert_eq!(workflow["status"], "ok");
            assert_eq!(llm["status"], "error");
            assert_eq!(llm["parent_id"], workflow["span_id"]);
            assert!(
                requests[1].1["series"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|m| m["metric"] == "crabber.model.calls")
            );
        }
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
        let spans = json!(span_payload(&requests[0].1));
        assert_eq!(requests[0].1[0]["event_type"], "span");
        assert_eq!(spans.as_array().unwrap().len(), 4);
        assert_eq!(spans[0]["meta"]["kind"], "agent");
        assert_eq!(spans[1]["meta"]["kind"], "workflow");
        assert_eq!(spans[2]["meta"]["kind"], "llm");
        assert_eq!(spans[3]["meta"]["kind"], "tool");
        assert_eq!(spans[3]["parent_id"], spans[1]["span_id"]);
        assert_eq!(spans[1]["parent_id"], spans[0]["span_id"]);
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
                    .all(|c| c.is_ascii_hexdigit())
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
