//! Existing private fixture/suite, preserving actual native cuts and joins.
use super::*;

#[derive(Default)]
pub(super) struct OperationalCapture {
    pub(super) records: Mutex<Vec<EventRecord>>,
    pub(super) values: Mutex<Vec<crate::OperationalObservation>>,
    pub(super) text: Notify,
}
impl Observer for OperationalCapture {
    fn emit(&self, event: &EventRecord) {
        self.records.lock().unwrap().push(event.clone());
        if event.kind == EventKind::TextDelta {
            self.text.notify_one();
        }
    }
    fn operational_completed(&self, value: &crate::OperationalObservation) {
        self.values.lock().unwrap().push(value.clone());
    }
}
#[derive(Default)]
pub(super) struct OperationalClock(pub(super) AtomicUsize);
impl crate::MonotonicClock for OperationalClock {
    fn now(&self) -> std::time::Duration {
        std::time::Duration::from_millis(self.0.load(Ordering::SeqCst) as u64)
    }
}
#[derive(Clone)]
pub(super) struct OperationalStream {
    pub(super) clock: Arc<OperationalClock>,
    pub(super) startup_error: bool,
    pub(super) script: Vec<(usize, StreamDelta)>,
    pub(super) entered: Option<Arc<Notify>>,
    pub(super) block_setup: bool,
    pub(super) block_end: bool,
}
#[async_trait]
impl Resolver for OperationalStream {
    fn binding_descriptor(
        &self,
        selection: &Selection,
    ) -> Result<crabber_core::ProviderBindingDescriptor, crabber_providers::ProviderBindingError>
    {
        crabber_core::ProviderBindingDescriptor::new(
            &selection.provider_id,
            &selection.model_id,
            "operational-fixture",
            "1",
            crabber_core::InputFingerprint::new(crabber_session::admission_config_hash(&json!([
                self.startup_error,
                self.block_setup,
                self.block_end
            ])))
            .unwrap(),
            crabber_core::ProviderBindingLimits::default(),
        )
        .map_err(|_| crabber_providers::ProviderBindingError::InvalidDescriptor)
    }

    async fn resolve(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        Ok(Arc::new(self.clone()))
    }
}
#[async_trait]
impl Streamer for OperationalStream {
    async fn stream(&self, _: ModelRequest) -> Result<DeltaStream, ProviderError> {
        self.clock.0.store(10, Ordering::SeqCst);
        if let Some(entered) = &self.entered {
            entered.notify_one();
        }
        if self.block_setup {
            futures::future::pending::<()>().await;
        }
        if self.startup_error {
            return Err(operational_error("SECRET startup"));
        }
        let clock = self.clock.clone();
        let script = self.script.clone();
        let stream = futures::stream::iter(script).map(move |(at, delta)| {
            clock.0.store(at, Ordering::SeqCst);
            delta
        });
        if self.block_end {
            Ok(Box::pin(stream.chain(futures::stream::pending())))
        } else {
            Ok(Box::pin(stream))
        }
    }
}
fn operational_runtime(
    capture: Arc<OperationalCapture>,
    clock: Arc<OperationalClock>,
    stream: OperationalStream,
) -> Orchestrator {
    Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(Arc::new(MemoryStore::new()))
        .resolver(Arc::new(stream))
        .plan_provider(Arc::new(StaticPlanProvider::new(Vec::new(), Vec::new())))
        .observer(capture)
        .monotonic_clock(clock)
        .build()
        .unwrap()
}
pub(super) fn operational_stream(clock: Arc<OperationalClock>) -> OperationalStream {
    OperationalStream {
        clock,
        startup_error: false,
        script: Vec::new(),
        entered: None,
        block_setup: false,
        block_end: false,
    }
}
#[tokio::test]
async fn operational_startup_midstream_success_and_first_text_boundaries() {
    use crate::{OperationKind, TerminalReason};
    for (startup_error, script, reason, first_token, elapsed) in [
        (true, vec![], TerminalReason::ProviderError, None, 10),
        (
            false,
            vec![
                (15, StreamDelta::ReasoningDelta("SECRET reasoning".into())),
                (20, StreamDelta::Error(operational_error("SECRET error"))),
            ],
            TerminalReason::ProviderError,
            None,
            20,
        ),
        (
            false,
            vec![
                (15, StreamDelta::TextDelta(String::new())),
                (20, StreamDelta::TextDelta("SECRET token".into())),
                (30, StreamDelta::TextDelta("SECRET second".into())),
                (40, StreamDelta::Completed),
            ],
            TerminalReason::Success,
            Some(20),
            40,
        ),
        (
            false,
            vec![
                (20, StreamDelta::ReasoningDelta("SECRET reasoning".into())),
                (40, StreamDelta::Completed),
            ],
            TerminalReason::Success,
            None,
            40,
        ),
    ] {
        let clock = Arc::new(OperationalClock::default());
        let capture = Arc::new(OperationalCapture::default());
        let mut stream = operational_stream(clock.clone());
        stream.startup_error = startup_error;
        stream.script = script;
        let runtime = operational_runtime(capture.clone(), clock, stream);
        let result = runtime.start(request()).await.unwrap().done().await;
        assert_eq!(result.is_ok(), reason == TerminalReason::Success);
        let values = capture.values.lock().unwrap().clone();
        assert_eq!(values.len(), 2);
        assert!(matches!(values[0].kind, OperationKind::Model { .. }));
        assert_eq!(values[0].reason, reason);
        assert_eq!(values[0].elapsed, std::time::Duration::from_millis(elapsed));
        assert_eq!(
            values[0].first_token,
            first_token.map(std::time::Duration::from_millis)
        );
        assert_eq!(values[1].kind, OperationKind::Run);
        assert_eq!(values[1].reason, reason);
        assert_eq!(values[1].elapsed, std::time::Duration::from_millis(elapsed));
        assert!(!format!("{values:?}").contains("SECRET"));
    }
}
#[tokio::test]
async fn operational_cancellation_before_and_after_stream_setup() {
    for block_setup in [true, false] {
        let clock = Arc::new(OperationalClock::default());
        let capture = Arc::new(OperationalCapture::default());
        let entered = Arc::new(Notify::new());
        let mut stream = operational_stream(clock.clone());
        stream.entered = Some(entered.clone());
        stream.block_setup = block_setup;
        stream.block_end = true;
        stream.script = vec![(20, StreamDelta::TextDelta("partial".into()))];
        let runtime = operational_runtime(capture.clone(), clock, stream);
        let handle = runtime.start(request()).await.unwrap();
        if block_setup {
            entered.notified().await;
        } else {
            capture.text.notified().await;
        }
        handle.interrupt();
        assert_eq!(handle.done().await.unwrap().status, RunStatus::Interrupted);
        let values = capture.values.lock().unwrap().clone();
        assert_eq!(values.len(), 2);
        assert!(
            values
                .iter()
                .all(|value| value.reason == crate::TerminalReason::Cancelled)
        );
        assert_eq!(values[0].first_token.is_some(), !block_setup);
    }
}
#[tokio::test(start_paused = true)]
async fn operational_lease_loss_is_local_once_and_does_not_settle_replacement() {
    let now = time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
    let wall = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(wall.clone()));
    let clock = Arc::new(OperationalClock::default());
    let capture = Arc::new(OperationalCapture::default());
    let entered = Arc::new(Notify::new());
    let mut stream = operational_stream(clock.clone());
    stream.entered = Some(entered.clone());
    stream.block_setup = true;
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store.clone())
        .clock(wall.clone())
        .monotonic_clock(clock)
        .resolver(Arc::new(stream))
        .plan_provider(Arc::new(StaticPlanProvider::new(Vec::new(), Vec::new())))
        .observer(capture.clone())
        .heartbeat_interval(std::time::Duration::from_secs(1))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    entered.notified().await;
    wall.set(now + time::Duration::seconds(31));
    let replacement = store.claim_expired_run(&run, "replacement").await.unwrap();
    tokio::time::advance(std::time::Duration::from_secs(1)).await;
    assert!(matches!(handle.done().await, Err(RuntimeError::LeaseLost)));
    assert!(
        !capture
            .records
            .lock()
            .unwrap()
            .iter()
            .any(|e| matches!(e.kind, EventKind::MessageCommitted | EventKind::RunSettled))
    );
    let values = capture.values.lock().unwrap().clone();
    assert_eq!(values.len(), 2);
    assert!(
        values
            .iter()
            .all(|value| value.reason == crate::TerminalReason::LeaseLost)
    );
    assert!(
        store
            .list_events(&session, None, 100)
            .await
            .unwrap()
            .iter()
            .all(|event| event.kind != EventKind::RunSettled)
    );
    assert_eq!(
        store.get_run(&run).await.unwrap().unwrap().claim_token,
        replacement.claim_token
    );
}
#[tokio::test]
async fn operational_tool_failure_remains_distinct_when_run_succeeds() {
    let capture = Arc::new(OperationalCapture::default());
    let clock = Arc::new(OperationalClock::default());
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(Arc::new(MemoryStore::new()))
        .resolver(Arc::new(FakeProvider::scripted(vec![
            call_script(ToolCallId::new(), r#"{"text":"SECRET argument"}"#),
            text_script("after"),
        ])))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(OperationalFailingTool(clock.clone())))],
            Vec::new(),
        )))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .monotonic_clock(clock)
        .observer(capture.clone())
        .build()
        .unwrap();
    assert_eq!(
        runtime
            .start(request())
            .await
            .unwrap()
            .done()
            .await
            .unwrap()
            .status,
        RunStatus::Completed
    );
    let values = capture.values.lock().unwrap().clone();
    assert_eq!(values.len(), 4);
    assert_eq!(values[1].reason, crate::TerminalReason::ToolError);
    assert_eq!(values[1].elapsed, std::time::Duration::from_millis(15));
    assert!(matches!(values[1].kind, crate::OperationKind::Tool { .. }));
    assert_eq!(values[3].reason, crate::TerminalReason::Success);
}

fn operational_error(message: &str) -> ProviderError {
    ProviderError {
        kind: crabber_providers::ProviderErrorKind::Invalid,
        message: message.into(),
        retryable: false,
    }
}

struct OperationalFailingTool(Arc<OperationalClock>);
#[async_trait]
impl JsonToolExecutor for OperationalFailingTool {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        self.0.0.store(15, Ordering::SeqCst);
        Err(ExtensionError::Plan("SECRET tool error".into()))
    }
}

#[tokio::test]
async fn operational_regressing_clock_saturates_to_zero() {
    let clock = Arc::new(OperationalClock::default());
    clock.0.store(100, Ordering::SeqCst);
    let capture = Arc::new(OperationalCapture::default());
    let mut stream = operational_stream(clock.clone());
    stream.script = vec![
        (20, StreamDelta::TextDelta("text".into())),
        (30, StreamDelta::Completed),
    ];
    let runtime = operational_runtime(capture.clone(), clock, stream);
    runtime
        .start(request())
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let values = capture.values.lock().unwrap();
    assert!(
        values
            .iter()
            .all(|value| value.elapsed == std::time::Duration::ZERO)
    );
    assert_eq!(values[0].first_token, Some(std::time::Duration::ZERO));
}

#[derive(Default)]
pub(super) struct OperationalAppendGate {
    pub(super) enabled: std::sync::atomic::AtomicBool,
    pub(super) conflict: std::sync::atomic::AtomicBool,
    pub(super) entered: Notify,
    pub(super) release: Notify,
}
#[tokio::test(start_paused = true)]
async fn operational_store_first_takeover_skips_settlement_initial_and_resume() {
    for resume in [false, true] {
        let traces = super::tracing_spans::RunReasonCapture::new();
        let now = time::OffsetDateTime::from_unix_timestamp(1_700_000_000).unwrap();
        let wall = Arc::new(ManualClock::new(now));
        let inner = Arc::new(MemoryStore::with_clock(wall.clone()));
        let append = Arc::new(OperationalAppendGate::default());
        let gate = Arc::new(SettlementGate {
            append: Some(append.clone()),
            ..SettlementGate::default()
        });
        let store = Arc::new(DelayedTerminalStore {
            inner: inner.clone(),
            gate: gate.clone(),
        });
        let capture = Arc::new(OperationalCapture::default());
        let scripts = if resume {
            vec![
                call_script(ToolCallId::new(), r#"{"text":"ok"}"#),
                text_script("resumed"),
            ]
        } else {
            vec![text_script("initial")]
        };
        let mut builder = Orchestrator::builder()
            .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
            .store(store)
            .clock(wall.clone())
            .observer(capture.clone())
            .resolver(Arc::new(FakeProvider::scripted(scripts)))
            .plan_provider(Arc::new(StaticPlanProvider::new(
                vec![tool(Arc::new(EchoTool(Arc::new(AtomicUsize::new(0)))))],
                Vec::new(),
            )));
        if resume {
            builder = builder.policy(Arc::new(PausePolicy));
        }
        let runtime = builder.build().unwrap();
        append.enabled.store(!resume, Ordering::SeqCst);
        let handle = runtime.start(request()).await.unwrap();
        let run_id = handle.run_id().clone();
        let session = handle.session_id().clone();
        let done = if resume {
            assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
            capture.values.lock().unwrap().clear();
            append.enabled.store(true, Ordering::SeqCst);
            let runtime = runtime.clone();
            let run_id = run_id.clone();
            tokio::spawn(async move { runtime.resume(&run_id).await })
        } else {
            tokio::spawn(handle.done())
        };
        append.entered.notified().await;
        gate.settlements.store(0, Ordering::SeqCst);
        gate.tool_settlements.store(0, Ordering::SeqCst);
        gate.unfinished_reads.store(0, Ordering::SeqCst);
        wall.set(now + time::Duration::seconds(31));
        let replacement = inner
            .claim_expired_run(&run_id, "replacement")
            .await
            .unwrap();
        append.release.notify_one();
        assert!(matches!(
            done.await.unwrap(),
            Err(RuntimeError::Store(StoreError::Conflict))
        ));
        assert_eq!(gate.settlements.load(Ordering::SeqCst), 0);
        assert_eq!(gate.tool_settlements.load(Ordering::SeqCst), 0);
        assert_eq!(gate.unfinished_reads.load(Ordering::SeqCst), 0);
        let values = capture.values.lock().unwrap().clone();
        let runs: Vec<_> = values
            .iter()
            .filter(|sample| sample.kind == crate::OperationKind::Run)
            .collect();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].reason, crate::TerminalReason::LeaseLost);
        assert_eq!(
            traces.reasons(if resume {
                "crabber.resume"
            } else {
                "crabber.run"
            }),
            vec!["lease_lost"]
        );
        assert!(
            inner
                .list_events(&session, None, 100)
                .await
                .unwrap()
                .iter()
                .all(|event| event.kind != EventKind::RunSettled)
        );
        assert_eq!(
            inner.get_run(&run_id).await.unwrap().unwrap().claim_token,
            replacement.claim_token
        );
    }
}
#[tokio::test(start_paused = true)]
async fn operational_ordinary_store_conflict_keeps_runtime_error_and_settlement() {
    let traces = super::tracing_spans::RunReasonCapture::new();
    let append = Arc::new(OperationalAppendGate::default());
    append.enabled.store(true, Ordering::SeqCst);
    append.conflict.store(true, Ordering::SeqCst);
    let gate = Arc::new(SettlementGate {
        append: Some(append.clone()),
        ..SettlementGate::default()
    });
    let capture = Arc::new(OperationalCapture::default());
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(Arc::new(DelayedTerminalStore {
            inner: Arc::new(MemoryStore::new()),
            gate: gate.clone(),
        }))
        .resolver(Arc::new(FakeProvider::scripted(vec![text_script("text")])))
        .plan_provider(Arc::new(StaticPlanProvider::new(Vec::new(), Vec::new())))
        .observer(capture.clone())
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    append.entered.notified().await;
    append.release.notify_one();
    assert!(matches!(
        handle.done().await,
        Err(RuntimeError::Store(StoreError::Conflict))
    ));
    assert!(
        !capture
            .records
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.kind == EventKind::MessageCommitted)
    );
    assert_eq!(gate.settlements.load(Ordering::SeqCst), 1);
    assert_eq!(traces.reasons("crabber.run"), vec!["runtime_error"]);
    let values = capture.values.lock().unwrap();
    assert_eq!(
        values.last().unwrap().reason,
        crate::TerminalReason::RuntimeError
    );
}
#[tokio::test]
async fn operational_tool_store_setup_error_is_not_tool_error() {
    for permission_event in [false, true] {
        let gate = Arc::new(SettlementGate::default());
        gate.fail_claim.store(!permission_event, Ordering::SeqCst);
        gate.fail_permission_event
            .store(permission_event, Ordering::SeqCst);
        let capture = Arc::new(OperationalCapture::default());
        let executed = Arc::new(AtomicUsize::new(0));
        let runtime = Orchestrator::builder()
            .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
            .store(Arc::new(DelayedTerminalStore {
                inner: Arc::new(MemoryStore::new()),
                gate,
            }))
            .resolver(Arc::new(FakeProvider::scripted(vec![call_script(
                ToolCallId::new(),
                r#"{"text":"ok"}"#,
            )])))
            .plan_provider(Arc::new(StaticPlanProvider::new(
                vec![tool(Arc::new(EchoTool(executed.clone())))],
                Vec::new(),
            )))
            .policy(Arc::new(StaticPolicy::new(if permission_event {
                PermissionDecision::Ask
            } else {
                PermissionDecision::Allow
            })))
            .observer(capture.clone())
            .build()
            .unwrap();
        let error = runtime
            .start(request())
            .await
            .unwrap()
            .done()
            .await
            .unwrap_err();
        if permission_event {
            assert!(matches!(
                error,
                RuntimeError::ApprovalBoundaryUnknown { .. }
            ));
        } else {
            assert!(matches!(error, RuntimeError::PhaseOutcomeUnknown { .. }));
        }
        assert_eq!(executed.load(Ordering::SeqCst), 0);
        let values = capture.values.lock().unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0].reason, crate::TerminalReason::Success);
        assert_eq!(values[1].reason, crate::TerminalReason::RuntimeError);
        assert!(
            !values
                .iter()
                .any(|value| value.reason == crate::TerminalReason::ToolError)
        );
    }
}

struct OperationalSetupExtension(bool);
#[async_trait]
impl crabber_extension::Extension for OperationalSetupExtension {
    fn id(&self) -> &'static str {
        "operational-setup"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        self.0.to_string()
    }
    async fn install(
        &self,
        registrar: &mut crabber_extension::Registrar,
    ) -> Result<(), ExtensionError> {
        use crabber_extension::Point;
        let reject = self.0;
        registrar.on_around(
            crabber_extension::ModelStream::ID,
            0,
            "setup",
            Arc::new(move |_, _| {
                Box::pin(async move {
                    if reject {
                        Err(ExtensionError::Plan("SECRET extension".into()))
                    } else {
                        Ok(Value::Null)
                    }
                })
            }),
        );
        Ok(())
    }
}
#[tokio::test]
async fn operational_extension_setup_rejection_and_omission_are_runtime_errors() {
    for reject in [true, false] {
        let registry = crabber_extension::Registry::new();
        registry
            .mount(
                Arc::new(OperationalSetupExtension(reject)),
                crabber_extension::Scope::Global,
            )
            .await
            .unwrap();
        let fake = FakeProvider::scripted(vec![text_script("never")]);
        let capture = Arc::new(OperationalCapture::default());
        let runtime = Orchestrator::builder()
            .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
            .store(Arc::new(MemoryStore::new()))
            .resolver(Arc::new(fake.clone()))
            .plan_provider(Arc::new(registry))
            .observer(capture.clone())
            .build()
            .unwrap();
        assert!(matches!(
            runtime.start(request()).await.unwrap().done().await,
            Err(RuntimeError::Extension(_))
        ));
        assert_eq!(fake.requests(), [] as [crabber_providers::ModelRequest; 0]);
        let values = capture.values.lock().unwrap();
        assert_eq!(values.len(), 2);
        assert!(matches!(values[0].kind, crate::OperationKind::Model { .. }));
        assert_eq!(values[1].kind, crate::OperationKind::Run);
        assert!(
            values
                .iter()
                .all(|value| value.reason == crate::TerminalReason::RuntimeError
                    && value.first_token.is_none())
        );
        assert!(!format!("{values:?}").contains("SECRET"));
    }
}

mod source_contract;
mod workspace_context;
