//! Capture facade-only telemetry without credentials or global subscribers.
#[path = "tracing_spans/admission_limits.rs"]
mod admission_limits;
use super::*;
use tracing::{
    Instrument, Subscriber,
    field::{Field, Visit},
    span::{Attributes, Id, Record},
};
use tracing_subscriber::{
    Layer,
    layer::{Context, SubscriberExt},
    registry::LookupSpan,
};

#[derive(Clone, Debug)]
struct CapturedSpan {
    id: u64,
    name: String,
    parent: Option<u64>,
    fields: BTreeMap<String, String>,
}
#[derive(Clone, Debug)]
struct CapturedEvent {
    target: String,
    level: tracing::Level,
    parent: Option<u64>,
    fields: BTreeMap<String, String>,
}
#[derive(Default)]
struct Capture {
    spans: Vec<CapturedSpan>,
    events: Vec<CapturedEvent>,
    closed: Vec<String>,
}
#[derive(Clone, Default)]
struct CaptureLayer(Arc<Mutex<Capture>>);

struct CaptureSubscriber {
    _guard: tracing::subscriber::DefaultGuard,
    _interest_dispatch: tracing::Dispatch,
}

pub(super) struct RunReasonCapture {
    layer: CaptureLayer,
    _subscriber: CaptureSubscriber,
}
impl RunReasonCapture {
    pub(super) fn new() -> Self {
        let layer = CaptureLayer::default();
        let subscriber = CaptureSubscriber::new(&layer);
        Self {
            layer,
            _subscriber: subscriber,
        }
    }
    pub(super) fn reasons(&self, name: &str) -> Vec<String> {
        self.layer
            .0
            .lock()
            .unwrap()
            .spans
            .iter()
            .filter(|span| span.name == name)
            .map(|span| span.fields["reason"].clone())
            .collect()
    }
}

pub(crate) async fn assert_failed_approval_span<T>(
    name: &str,
    work: impl std::future::Future<Output = Result<T, RuntimeError>>,
) {
    let layer = CaptureLayer::default();
    let _subscriber = CaptureSubscriber::new(&layer);
    assert!(work.await.is_err());
    let capture = layer.0.lock().unwrap();
    let span = capture.spans.iter().find(|span| span.name == name).unwrap();
    assert_eq!(span.fields["outcome"], "failed");
    assert_eq!(span.fields["otel.status_code"], "ERROR");
    assert_ne!(span.fields["error.kind"], "");
}
impl CaptureSubscriber {
    fn new(layer: &CaptureLayer) -> Self {
        // Keep registration in tracing-core's multi-dispatch mode. Otherwise a
        // concurrent unsubscribed test can cache Never for a new shared callsite.
        let interest_dispatch =
            tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
        let guard =
            tracing::subscriber::set_default(tracing_subscriber::registry().with(layer.clone()));
        Self {
            _guard: guard,
            _interest_dispatch: interest_dispatch,
        }
    }
}
struct Fields<'a>(&'a mut BTreeMap<String, String>);
impl Visit for Fields<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().into(), format!("{value:?}"));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().into(), value.into());
    }
}
impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for CaptureLayer {
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let parent = attrs.parent().cloned().or_else(|| {
            if attrs.is_contextual() {
                ctx.current_span().id().cloned()
            } else {
                None
            }
        });
        let mut fields = BTreeMap::new();
        attrs.record(&mut Fields(&mut fields));
        self.0.lock().unwrap().spans.push(CapturedSpan {
            id: id.into_u64(),
            name: attrs.metadata().name().into(),
            parent: parent.map(|id| id.into_u64()),
            fields,
        });
    }
    fn on_record(&self, id: &Id, record: &Record<'_>, _: Context<'_, S>) {
        let mut capture = self.0.lock().unwrap();
        record.record(&mut Fields(
            &mut capture
                .spans
                .iter_mut()
                .rev()
                .find(|s| s.id == id.into_u64())
                .unwrap()
                .fields,
        ));
    }
    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        let mut fields = BTreeMap::new();
        event.record(&mut Fields(&mut fields));
        self.0.lock().unwrap().events.push(CapturedEvent {
            target: event.metadata().target().into(),
            level: *event.metadata().level(),
            parent: ctx.event_span(event).map(|span| span.id().into_u64()),
            fields,
        });
    }
    fn on_close(&self, id: Id, _: Context<'_, S>) {
        let mut c = self.0.lock().unwrap();
        let name = c
            .spans
            .iter()
            .rev()
            .find(|s| s.id == id.into_u64())
            .unwrap()
            .name
            .clone();
        c.closed.push(name);
    }
}
#[derive(Default)]
struct Attempts(Mutex<Vec<RunId>>);
impl Observer for Attempts {
    fn emit(&self, _: &EventRecord) {}
    fn emit_in_attempt(
        &self,
        _: &EventRecord,
        _: Option<&crabber_core::TraceContext>,
        attempt: &RunId,
    ) {
        self.0.lock().unwrap().push(attempt.clone());
    }
}
fn runtime(
    store: Arc<dyn Store>,
    fake: FakeProvider,
    observer: Arc<dyn Observer>,
    decision: PermissionDecision,
) -> Orchestrator {
    Orchestrator::builder()
        .behavior_version("tracing-fixture.v1")
        .store(store)
        .resolver(Arc::new(fake))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(EchoTool(Arc::new(AtomicUsize::new(0)))))],
            vec![],
        )))
        .policy(Arc::new(StaticPolicy::new(decision)))
        .observer(observer)
        .build()
        .unwrap()
}
async fn tool_run(layer: &CaptureLayer) -> Vec<RunId> {
    let _subscriber = CaptureSubscriber::new(layer);
    let observer = Arc::new(Attempts::default());
    let runtime = runtime(
        Arc::new(MemoryStore::new()),
        FakeProvider::scripted(vec![
            call_script(ToolCallId::new(), r#"{"text":"PROMPT_SECRET"}"#),
            text_script("PROMPT_SECRET"),
        ]),
        observer.clone(),
        PermissionDecision::Allow,
    );
    let mut input = request();
    input.text = "PROMPT_SECRET".into();
    let host = tracing::info_span!("host.request");
    let handle = runtime.start(input).instrument(host.clone()).await.unwrap();
    drop(host);
    handle.done().await.unwrap();
    runtime.close(std::time::Duration::from_secs(1)).await;
    observer.0.lock().unwrap().clone()
}
#[tokio::test]
async fn run_span_is_child_of_caller_span_and_has_model_and_tool_children() {
    let layer = CaptureLayer::default();
    tool_run(&layer).await;
    let capture = layer.0.lock().unwrap();
    let host = capture
        .spans
        .iter()
        .find(|s| s.name == "host.request")
        .unwrap();
    let run = capture
        .spans
        .iter()
        .find(|s| s.name == "crabber.run")
        .unwrap();
    assert_eq!(run.parent, Some(host.id));
    let control = capture
        .spans
        .iter()
        .find(|s| s.name == "crabber.test.durable_cancel")
        .expect("the retained durable-control task was joined");
    assert_eq!(
        control.parent,
        Some(run.id),
        "independent control observation must retain its original worker trace"
    );
    let admission = capture
        .spans
        .iter()
        .find(|s| s.name == "crabber.admission")
        .unwrap();
    assert_eq!(admission.parent, Some(host.id));
    let models: Vec<_> = capture
        .spans
        .iter()
        .filter(|s| s.name == "crabber.model_call")
        .collect();
    assert_eq!(models.len(), 2);
    for model in models {
        assert_eq!(model.parent, Some(run.id));
        assert_eq!(model.fields["purpose"], "turn");
        assert_eq!(model.fields["retry.attempt"], "0");
        assert_eq!(model.fields["outcome"], "completed");
    }
    let tool = capture
        .spans
        .iter()
        .find(|s| s.name == "crabber.tool_call")
        .unwrap();
    assert_eq!(tool.parent, Some(run.id));
    assert_eq!(tool.fields["outcome"], "succeeded");
}
#[tokio::test]
async fn attempt_id_matches_observer_attempt() {
    let layer = CaptureLayer::default();
    let attempts = tool_run(&layer).await;
    let capture = layer.0.lock().unwrap();
    let run = capture
        .spans
        .iter()
        .find(|s| s.name == "crabber.run")
        .unwrap();
    assert_ne!(attempts, []);
    for attempt in attempts {
        assert_eq!(run.fields["attempt.id"], attempt.to_string());
    }
}
#[tokio::test]
async fn no_content_fields_anywhere() {
    let layer = CaptureLayer::default();
    tool_run(&layer).await;
    let capture = layer.0.lock().unwrap();
    let allowed = [
        "message",
        "session.id",
        "run.id",
        "attempt.id",
        "outcome",
        "reason",
        "error.kind",
        "otel.status_code",
        "host.trace_id",
        "host.span_id",
        "host.predecessor_trace_id",
        "keyed",
        "provider.id",
        "model.id",
        "purpose",
        "retry.attempt",
        "tokens.input",
        "tokens.output",
        "tool.name",
        "tool.call_id",
        "permission",
    ];
    for fields in capture
        .spans
        .iter()
        .map(|s| &s.fields)
        .chain(capture.events.iter().map(|e| &e.fields))
    {
        for (key, value) in fields {
            assert!(allowed.contains(&key.as_str()), "unexpected field {key}");
            assert!(!value.contains("PROMPT_SECRET"));
        }
    }
}
#[tokio::test]
async fn lifecycle_events_are_emitted_under_lifecycle_target_only() {
    let layer = CaptureLayer::default();
    tool_run(&layer).await;
    let capture = layer.0.lock().unwrap();
    for message in ["run started", "run settled", "tool settled"] {
        let events: Vec<_> = capture
            .events
            .iter()
            .filter(|e| e.fields.get("message").is_some_and(|m| m == message))
            .collect();
        assert_eq!(events.len(), 1, "{message}");
        for event in events {
            assert_eq!(event.target, "crabber::runtime::lifecycle");
            for key in ["session.id", "run.id", "attempt.id"] {
                assert!(event.fields.contains_key(key));
            }
        }
    }
    assert!(capture.events.iter().all(|event| !matches!(
        event.fields.get("message").map(String::as_str),
        Some("permission decided" | "run admitted")
    )));
}
#[tokio::test]
async fn host_span_closes_after_run_span() {
    let layer = CaptureLayer::default();
    tool_run(&layer).await;
    let capture = layer.0.lock().unwrap();
    assert!(
        capture
            .closed
            .iter()
            .position(|s| s == "crabber.run")
            .unwrap()
            < capture
                .closed
                .iter()
                .position(|s| s == "host.request")
                .unwrap()
    );
}
#[tokio::test]
async fn host_context_is_recorded_on_run_span() {
    let layer = CaptureLayer::default();
    let _subscriber = CaptureSubscriber::new(&layer);
    let runtime = runtime(
        Arc::new(MemoryStore::new()),
        FakeProvider::scripted(vec![text_script("done")]),
        Arc::new(Attempts::default()),
        PermissionDecision::Allow,
    );
    let context =
        crabber_core::TraceContext::new("1234567890abcdef1234567890abcdef", "1234567890abcdef")
            .unwrap();
    runtime
        .start_with_context(request(), Some(context.clone()))
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let capture = layer.0.lock().unwrap();
    let run = capture
        .spans
        .iter()
        .find(|s| s.name == "crabber.run")
        .unwrap();
    assert_eq!(run.fields["host.trace_id"], context.trace_id());
    assert_eq!(run.fields["host.span_id"], context.span_id());
}

#[tokio::test(start_paused = true)]
async fn lease_loss_records_outcome_and_single_warn_source() {
    let layer = CaptureLayer::default();
    let _subscriber = CaptureSubscriber::new(&layer);
    let now = time::OffsetDateTime::UNIX_EPOCH;
    let wall = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(wall.clone()));
    let entered = Arc::new(Notify::new());
    let mut stream = operational_stream(Arc::new(OperationalClock::default()));
    stream.entered = Some(entered.clone());
    stream.block_setup = true;
    let runtime = Orchestrator::builder()
        .behavior_version("tracing-lease.v1")
        .store(store.clone())
        .clock(wall.clone())
        .resolver(Arc::new(stream))
        .plan_provider(Arc::new(StaticPlanProvider::new(vec![], vec![])))
        .heartbeat_interval(std::time::Duration::from_millis(10))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    entered.notified().await;
    wall.set(now + time::Duration::seconds(31));
    store
        .claim_expired_run(handle.run_id(), "replacement")
        .await
        .unwrap();
    tokio::time::advance(std::time::Duration::from_millis(10)).await;
    assert!(matches!(handle.done().await, Err(RuntimeError::LeaseLost)));
    let capture = layer.0.lock().unwrap();
    let run = capture
        .spans
        .iter()
        .find(|s| s.name == "crabber.run")
        .unwrap();
    assert_eq!(run.fields["reason"], "lease_lost");
    assert_eq!(run.fields["otel.status_code"], "ERROR");
    let lost: Vec<_> = capture
        .events
        .iter()
        .filter(|e| {
            e.fields
                .get("source")
                .is_some_and(|source| source == "heartbeat")
        })
        .collect();
    assert_eq!(lost.len(), 1);
    assert_eq!(lost[0].level, tracing::Level::ERROR);
    for event in capture
        .events
        .iter()
        .filter(|e| e.target == "crabber::runtime::heartbeat")
    {
        assert_eq!(event.parent, Some(run.id));
    }
}
#[derive(Clone)]
struct RetryProvider(Arc<AtomicUsize>);
#[async_trait]
impl Resolver for RetryProvider {
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<crabber_core::ProviderBindingDescriptor, crabber_providers::ProviderBindingError>
    {
        crabber_core::ProviderBindingDescriptor::new(
            &selection.provider_id,
            &selection.model_id,
            "retry-fixture",
            "1",
            crabber_core::InputFingerprint::new("a".repeat(64)).unwrap(),
            crabber_core::ProviderBindingLimits::default(),
        )
        .map_err(|_| crabber_providers::ProviderBindingError::InvalidDescriptor)
    }
    async fn resolve(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        Ok(Arc::new(self.clone()))
    }
}
#[async_trait]
impl Streamer for RetryProvider {
    async fn stream(&self, _: ModelRequest) -> Result<DeltaStream, ProviderError> {
        if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(ProviderError {
                kind: crabber_providers::ProviderErrorKind::Server,
                message: "SECRET response credential header".into(),
                retryable: true,
            })
        } else {
            Ok(Box::pin(futures::stream::iter(text_script("done"))))
        }
    }
}
#[tokio::test]
async fn retry_emits_warn_with_finite_kind() {
    let layer = CaptureLayer::default();
    let _subscriber = CaptureSubscriber::new(&layer);
    let runtime = Orchestrator::builder()
        .behavior_version("tracing-retry.v1")
        .store(Arc::new(MemoryStore::new()))
        .resolver(Arc::new(RetryProvider(Arc::new(AtomicUsize::new(0)))))
        .plan_provider(Arc::new(StaticPlanProvider::new(vec![], vec![])))
        .build()
        .unwrap();
    runtime
        .start(request())
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let capture = layer.0.lock().unwrap();
    let event = capture
        .events
        .iter()
        .find(|e| {
            e.fields
                .get("message")
                .is_some_and(|message| message == "model request failed, retry scheduled")
        })
        .unwrap();
    assert_eq!(event.level, tracing::Level::WARN);
    assert_eq!(event.fields["error.kind"], "provider_server");
    assert_eq!(event.fields["retry.attempt"], "1");
    for fields in capture
        .spans
        .iter()
        .map(|s| &s.fields)
        .chain(capture.events.iter().map(|e| &e.fields))
    {
        assert!(
            fields
                .values()
                .all(|value| !value.contains("SECRET response credential header"))
        );
    }
}
#[tokio::test]
async fn approval_suspend_and_continue_spans() {
    use crabber_session::{ApprovalAnswer, ApprovalReply, ApprovalResolverClaim, CallApproval};
    let layer = CaptureLayer::default();
    let _subscriber = CaptureSubscriber::new(&layer);
    let store = Arc::new(MemoryStore::new());
    let runtime = runtime(
        store,
        FakeProvider::scripted(vec![
            call_script(ToolCallId::new(), r#"{"text":"approval"}"#),
            text_script("done"),
        ]),
        Arc::new(Attempts::default()),
        PermissionDecision::Ask,
    );
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    let snapshot = runtime
        .approval_snapshot(&session, &run)
        .await
        .unwrap()
        .unwrap();
    let call = &snapshot.calls[0];
    let CallApproval::Ask { id, description } = &call.decision else {
        panic!("expected Ask")
    };
    let receipt = runtime
        .answer_approval(ApprovalAnswer {
            id: crabber_core::ControlId::new(),
            expected_revision: snapshot.revision,
            session_id: session.clone(),
            run_id: run.clone(),
            continuation: snapshot.continuation.clone(),
            interaction_id: id.clone(),
            resolver: ApprovalResolverClaim {
                resolver_id: "test-host".into(),
                authority: description.authority().clone(),
                authority_claim: "test".into(),
            },
            reply: ApprovalReply::Grant,
        })
        .await
        .unwrap();
    let continuation = runtime
        .continue_approval(crate::ContinueApproval {
            id: crabber_core::ControlId::new(),
            session_id: session,
            run_id: run,
            continuation: snapshot.continuation,
            expected_revision: receipt.revision,
        })
        .await
        .unwrap();
    let crate::Continuation::Started { handle, .. } = continuation else {
        panic!("expected started")
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    let capture = layer.0.lock().unwrap();
    let suspend = capture
        .spans
        .iter()
        .find(|s| s.name == "crabber.approval.suspend")
        .unwrap();
    assert_eq!(suspend.fields["outcome"], "suspended");
    assert_eq!(suspend.fields["otel.status_code"], "OK");
    let parent = capture
        .spans
        .iter()
        .find(|s| Some(s.id) == suspend.parent)
        .unwrap();
    assert_eq!(parent.name, "crabber.run");
    let continued = capture
        .spans
        .iter()
        .find(|s| s.name == "crabber.approval.continue")
        .unwrap();
    assert_eq!(continued.fields["outcome"], "approved");
    assert_eq!(continued.fields["otel.status_code"], "OK");
    let parent = capture
        .spans
        .iter()
        .find(|s| Some(s.id) == continued.parent)
        .unwrap();
    assert_eq!(parent.name, "crabber.resume");
}

#[tokio::test]
async fn failed_approval_suspension_records_error_status() {
    let gate = Arc::new(SettlementGate::default());
    gate.fail_permission_event.store(true, Ordering::SeqCst);
    let runtime = runtime(
        Arc::new(DelayedTerminalStore {
            inner: Arc::new(MemoryStore::new()),
            gate,
        }),
        FakeProvider::scripted(vec![call_script(
            ToolCallId::new(),
            r#"{"text":"approval"}"#,
        )]),
        Arc::new(Attempts::default()),
        PermissionDecision::Ask,
    );
    assert_failed_approval_span("crabber.approval.suspend", async {
        runtime.start(request()).await.unwrap().done().await
    })
    .await;
}
