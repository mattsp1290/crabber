//! Real tool capabilities share native publication ordering and retain unknown IDs.
use super::*;
use crabber_core::{ControlId, PublicSourceChange, PublicSourceEvent, SessionRevision};
use crabber_extension::{PublicSourceError, PublicSourceWriter};
use serde::Deserialize;
#[derive(Default)]
struct Capture(Mutex<Vec<EventRecord>>);
impl Observer for Capture {
    fn emit(&self, event: &EventRecord) {
        self.0.lock().unwrap().push(event.clone());
    }
}
struct Saved {
    writer: PublicSourceWriter,
    id: ControlId,
    expected: SessionRevision,
    change: PublicSourceChange,
}
struct PublishingTool {
    lost_ack: bool,
    saved: Arc<Mutex<Option<Saved>>>,
}
#[async_trait]
impl JsonToolExecutor for PublishingTool {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        panic!("actual tool context required")
    }
    async fn execute_with_context(
        &self,
        mut context: crabber_extension::ToolContext,
        _: Value,
    ) -> Result<Value, ExtensionError> {
        let writer = context.public_source().clone();
        // Public routing labels cannot change the private retained writer scope.
        context.session_id = SessionId::new();
        context.run_id = RunId::new();
        context.call_id = ToolCallId::new();
        let expected = writer.snapshot().await.unwrap().revision;
        let id = ControlId::new();
        let change = PublicSourceChange::StateSnapshot {
            state: json!({"public":null}),
        };
        if self.lost_ack {
            assert_eq!(
                writer.publish(id.clone(), expected, change.clone()).await,
                Err(PublicSourceError::UnknownOutcome)
            );
            let current = writer.snapshot().await.unwrap().revision;
            let delta = PublicSourceChange::StateDelta {
                delta: vec![
                    serde_json::from_value(
                        json!({"op":"add","path":"/next","value":true,"extension":null}),
                    )
                    .unwrap(),
                ],
            };
            writer
                .publish(ControlId::new(), current, delta)
                .await
                .unwrap();
        } else {
            let (first, second) = futures::join!(
                writer.publish(id.clone(), expected, change.clone()),
                writer.publish(id.clone(), expected, change.clone())
            );
            assert_eq!(first.unwrap(), second.unwrap());
        }
        *self.saved.lock().unwrap() = Some(Saved {
            writer,
            id,
            expected,
            change,
        });
        Ok(json!({"tool_result":"separate display result"}))
    }
}
async fn journey(
    lost_ack: bool,
) -> (
    Arc<MemoryStore>,
    Arc<Capture>,
    SessionId,
    RunId,
    ToolCallId,
    Saved,
) {
    let memory = Arc::new(MemoryStore::new());
    let gate = Arc::new(SettlementGate::default());
    gate.release.notify_one();
    gate.source_ack_lost.store(lost_ack, Ordering::SeqCst);
    let store = Arc::new(DelayedTerminalStore {
        inner: memory.clone(),
        gate,
    });
    let capture = Arc::new(Capture::default());
    let saved = Arc::new(Mutex::new(None));
    let call = ToolCallId::new();
    let runtime = Orchestrator::builder()
        .behavior_version("public-source-tool-contract.v1")
        .store(store)
        .resolver(Arc::new(FakeProvider::scripted(vec![
            call_script(call.clone(), r#"{"text":"display"}"#),
            text_script("done"),
        ])))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(PublishingTool {
                lost_ack,
                saved: saved.clone(),
            }))],
            vec![],
        )))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .observer(capture.clone())
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    let saved = saved.lock().unwrap().take().unwrap();
    (memory, capture, session, run, call, saved)
}
fn public_events(capture: &Capture) -> Vec<EventRecord> {
    capture
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|event| {
            event.payload.get("version") == Some(&json!(2))
                && matches!(
                    event.kind,
                    EventKind::PublicStateSnapshot
                        | EventKind::PublicStateDelta
                        | EventKind::ActivitySnapshot
                        | EventKind::ActivityDelta
                )
        })
        .cloned()
        .collect()
}
#[tokio::test]
async fn simultaneous_exact_tool_publish_is_observed_once_and_sealed_after_terminal() {
    let (store, capture, session, run, call, saved) = journey(false).await;
    let observed = public_events(&capture);
    assert_eq!(observed.len(), 1);
    let source = PublicSourceEvent::deserialize(&observed[0].payload).unwrap();
    assert_eq!(source.source.session_id, session);
    assert_eq!(source.source.run_id, run);
    assert_eq!(source.source.call_id, Some(call));
    assert!(source.source.invocation.is_none());
    let before = store.public_state(&session).await.unwrap();
    let history = store.list_events(&session, None, 128).await.unwrap();
    assert_eq!(
        saved
            .writer
            .publish(saved.id, saved.expected, saved.change)
            .await
            .unwrap(),
        source.revision
    );
    assert_eq!(
        saved
            .writer
            .publish(
                ControlId::new(),
                before.revision,
                PublicSourceChange::StateSnapshot {
                    state: json!({"late":true})
                }
            )
            .await,
        Err(PublicSourceError::StaleOwner)
    );
    assert_eq!(public_events(&capture), observed);
    assert_eq!(store.public_state(&session).await.unwrap(), before);
    assert_eq!(
        store.list_events(&session, None, 128).await.unwrap(),
        history
    );
    let messages = store.list_messages(&session, None).await.unwrap();
    assert!(!contains_text(&messages, "public"));
}
#[tokio::test]
async fn lost_source_ack_reconciles_before_later_delta_and_exact_retry_does_not_duplicate() {
    let (store, capture, session, _run, _call, saved) = journey(true).await;
    let observed = public_events(&capture);
    assert_eq!(observed.len(), 3);
    assert_eq!(
        observed
            .iter()
            .map(|event| event.kind.clone())
            .collect::<Vec<_>>(),
        vec![
            EventKind::PublicStateSnapshot,
            EventKind::PublicStateSnapshot,
            EventKind::PublicStateDelta
        ]
    );
    assert!(
        observed
            .windows(2)
            .all(|pair| pair[0].cursor.unwrap() < pair[1].cursor.unwrap())
    );
    let delta = PublicSourceEvent::deserialize(&observed[2].payload).unwrap();
    assert_eq!(delta.before_event_cursor, observed[1].cursor);
    assert_eq!(delta.contents.state(), &json!({"public":null,"next":true}));
    assert_eq!(
        store.public_state(&session).await.unwrap().contents,
        delta.contents
    );
    saved
        .writer
        .publish(saved.id, saved.expected, saved.change)
        .await
        .unwrap();
    assert_eq!(public_events(&capture), observed);
}

struct HeldSourceModel {
    gate: Arc<SettlementGate>,
    saved: Arc<Mutex<Option<Saved>>>,
}
#[async_trait]
impl ModelStream for HeldSourceModel {
    async fn stream(
        &self,
        _: ModelRequest,
        _: Arc<dyn Streamer>,
    ) -> Result<DeltaStream, ProviderError> {
        let writer =
            crabber_extension::current_public_source().expect("actual worker attempt capability");
        let expected = writer.snapshot().await.unwrap().revision;
        let id = ControlId::new();
        let change = PublicSourceChange::StateSnapshot {
            state: json!({"held_original":null}),
        };
        *self.saved.lock().unwrap() = Some(Saved {
            writer: writer.clone(),
            id: id.clone(),
            expected,
            change: change.clone(),
        });
        let publishing = tokio::spawn(async move { writer.publish(id, expected, change).await });
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            self.gate.source_worker_entered.notified(),
        )
        .await
        .unwrap();
        publishing.abort();
        assert!(publishing.await.unwrap_err().is_cancelled());
        // No assistant Message/TurnCompleted writes advance the revision after
        // this original request. Failure settlement still reaches the genuine
        // terminal source barrier while the actual Attempt credential is live.
        Err(ProviderError {
            kind: crabber_providers::ProviderErrorKind::Transport,
            message: "held-source model failure".into(),
            retryable: false,
        })
    }
}
struct ReleaseWorker(Arc<SettlementGate>);
impl Drop for ReleaseWorker {
    fn drop(&mut self) {
        self.0.source_worker_release.notify_one();
        self.0.source_absent_lookup_release.notify_one();
    }
}
#[tokio::test]
async fn cancelled_publish_with_live_original_worker_cannot_conclude_or_skip_unresolved_cursor() {
    let memory = Arc::new(MemoryStore::new());
    let gate = Arc::new(SettlementGate::default());
    let _release = ReleaseWorker(gate.clone());
    gate.release.notify_one();
    gate.source_worker_hold.store(true, Ordering::SeqCst);
    gate.source_hold_absent_lookup.store(true, Ordering::SeqCst);
    let store = Arc::new(DelayedTerminalStore {
        inner: memory.clone(),
        gate: gate.clone(),
    });
    let saved = Arc::new(Mutex::new(None));
    let capture = Arc::new(Capture::default());
    let runtime = Orchestrator::builder()
        .behavior_version("held-native-source.v1")
        .store(store)
        .resolver(Arc::new(FakeProvider::scripted(vec![text_script(
            "actual business returned",
        )])))
        .model_stream(Arc::new(HeldSourceModel {
            gate: gate.clone(),
            saved: saved.clone(),
        }))
        .plan_provider(Arc::new(StaticPlanProvider::new(vec![], vec![])))
        .observer(capture.clone())
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        gate.source_absent_lookup_entered.notified(),
    )
    .await
    .unwrap();
    gate.source_worker_release.notify_one();
    let workers = std::mem::take(&mut *gate.source_workers.lock().unwrap());
    for worker in workers {
        tokio::time::timeout(std::time::Duration::from_secs(2), worker)
            .await
            .unwrap()
            .unwrap()
            .expect("same live native original commits before terminal observation returns");
    }
    // Commit while the same terminal metadata read still holds its genuine
    // earlier None. The credential has not been expired by worker cleanup.
    let saved = saved.lock().unwrap().take().unwrap();
    let publication = memory
        .lookup_public_source_publication(&session, &saved.id)
        .await
        .unwrap()
        .expect("same original native worker committed before terminal observer returned");
    gate.source_absent_lookup_release.notify_one();
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), handle.done())
        .await
        .unwrap();
    let before = memory.list_events(&session, None, 128).await.unwrap();
    assert!(matches!(
        outcome,
        Err(RuntimeError::AttemptBoundaryUnknown { .. })
    ));
    assert!(
        before
            .iter()
            .all(|event| event.kind != EventKind::RunSettled)
    );
    assert_eq!(publication.events.len(), 1);
    assert_eq!(
        memory.get_run(&run).await.unwrap().unwrap().status,
        RunStatus::Running
    );
    assert_eq!(public_events(&capture), [] as [EventRecord; 0]);
    assert_eq!(
        saved
            .writer
            .publish(saved.id, saved.expected, saved.change)
            .await
            .unwrap(),
        publication.receipt.revision
    );
    assert!(
        public_events(&capture).is_empty(),
        "sealed parent does not emit a late positive event"
    );
}

struct PublishingModel(Arc<Mutex<Option<Saved>>>);
#[async_trait]
impl ModelStream for PublishingModel {
    async fn stream(
        &self,
        request: ModelRequest,
        next: Arc<dyn Streamer>,
    ) -> Result<DeltaStream, ProviderError> {
        let writer = crabber_extension::current_public_source().unwrap();
        let expected = writer.snapshot().await.unwrap().revision;
        let id = ControlId::new();
        let change = PublicSourceChange::StateSnapshot {
            state: json!({"before_pause":null}),
        };
        writer
            .publish(id.clone(), expected, change.clone())
            .await
            .unwrap();
        *self.0.lock().unwrap() = Some(Saved {
            writer,
            id,
            expected,
            change,
        });
        next.stream(request).await
    }
}
#[tokio::test]
async fn retained_attempt_writer_cannot_emit_or_mutate_after_actual_approval_pause() {
    let store = Arc::new(MemoryStore::new());
    let saved = Arc::new(Mutex::new(None));
    let capture = Arc::new(Capture::default());
    let runtime = Orchestrator::builder()
        .behavior_version("paused-public-source.v1")
        .store(store.clone())
        .resolver(Arc::new(FakeProvider::scripted(vec![call_script(
            ToolCallId::new(),
            r#"{"text":"approval"}"#,
        )])))
        .model_stream(Arc::new(PublishingModel(saved.clone())))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(EchoTool(Arc::new(AtomicUsize::new(0)))))],
            vec![],
        )))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Ask)))
        .observer(capture.clone())
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(5), handle.done())
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Paused
    );
    let before = store.public_state(&session).await.unwrap();
    let history = store.list_events(&session, None, 128).await.unwrap();
    let observed = capture.0.lock().unwrap().clone();
    let saved = saved.lock().unwrap().take().unwrap();
    saved
        .writer
        .publish(saved.id, saved.expected, saved.change)
        .await
        .unwrap();
    assert_eq!(
        saved
            .writer
            .publish(
                ControlId::new(),
                before.revision,
                PublicSourceChange::StateSnapshot {
                    state: json!({"after_pause":true})
                }
            )
            .await,
        Err(PublicSourceError::StaleOwner)
    );
    assert_eq!(capture.0.lock().unwrap().as_slice(), observed.as_slice());
    assert_eq!(store.public_state(&session).await.unwrap(), before);
    assert_eq!(
        store.list_events(&session, None, 128).await.unwrap(),
        history
    );
}
