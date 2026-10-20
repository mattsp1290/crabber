//! Real runtime entrypoints must own native attempt authority before dispatch.
use super::*;
use crabber_session::{AttemptCleanup, AttemptState};

#[tokio::test]
async fn completion_concludes_the_same_started_native_attempt() {
    let store = Arc::new(MemoryStore::new());
    let runtime = orchestrator(
        store.clone(),
        FakeProvider::scripted(vec![text_script("done")]),
        Arc::new(EchoTool(Arc::new(AtomicUsize::new(0)))),
        Arc::new(RecordingObserver::default()),
    );
    let handle = runtime.start(request()).await.unwrap();
    let run = handle.run_id().clone();
    let session = handle.session_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    let latest = store.latest_execution_attempt(&run).await.unwrap().unwrap();
    assert_eq!(latest.attempt.state, AttemptState::Completed);
    assert_eq!(latest.attempt.cleanup, AttemptCleanup::Complete);
    let events = store.list_events(&session, None, 100).await.unwrap();
    assert_eq!(events[0].kind, EventKind::ExecutionAttemptChanged);
    assert_eq!(events[1].kind, EventKind::ExecutionAttemptChanged);
    let terminal = events
        .iter()
        .position(|event| event.kind == EventKind::RunSettled)
        .unwrap();
    assert_eq!(
        events[terminal + 1].kind,
        EventKind::ExecutionAttemptChanged
    );
    assert_eq!(
        latest.revision,
        store.session_revision(&session).await.unwrap()
    );
}

#[tokio::test]
async fn suspension_and_resume_use_distinct_native_generations() {
    let store = Arc::new(MemoryStore::new());
    let count = Arc::new(AtomicUsize::new(0));
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(store.clone())
        .resolver(Arc::new(FakeProvider::scripted(vec![
            call_script(ToolCallId::new(), r#"{"text":"once"}"#),
            text_script("done"),
        ])))
        .plan_provider(Arc::new(StaticPlanProvider::new(
            vec![tool(Arc::new(EchoTool(count.clone())))],
            vec![],
        )))
        .policy(Arc::new(PausePolicy))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    let first = store
        .latest_execution_attempt(&run)
        .await
        .unwrap()
        .unwrap()
        .attempt;
    assert_eq!(first.state, AttemptState::Suspended);
    assert_eq!(first.cleanup, AttemptCleanup::Complete);
    assert_eq!(
        runtime.resume(&run).await.unwrap().status,
        RunStatus::Completed
    );
    let second = store
        .latest_execution_attempt(&run)
        .await
        .unwrap()
        .unwrap()
        .attempt;
    assert_ne!(first.target.id, second.target.id);
    assert_eq!(
        second.target.generation,
        first.target.generation.next().unwrap()
    );
    assert_eq!(second.state, AttemptState::Completed);
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn lost_begin_acknowledgment_never_dispatches_or_retries() {
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let gate = Arc::new(SettlementGate::default());
    gate.begin_ack_lost.store(true, Ordering::SeqCst);
    let provider = FakeProvider::scripted(vec![text_script("must not execute")]);
    let runtime = Orchestrator::builder()
        .behavior_version("lost-begin-ack.v1")
        .clock(clock.clone())
        .store(Arc::new(DelayedTerminalStore {
            inner: store.clone(),
            gate,
        }))
        .resolver(Arc::new(provider.clone()))
        .plan_provider(Arc::new(StaticPlanProvider::new(vec![], vec![])))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let run = handle.run_id().clone();
    assert!(matches!(
        handle.done().await,
        Err(RuntimeError::Attempt(
            crabber_session::AttemptError::UnknownStoreOutcome
        ))
    ));
    assert_eq!(provider.requests(), Vec::new());
    let latest = store.latest_execution_attempt(&run).await.unwrap().unwrap();
    assert_eq!(latest.attempt.state, AttemptState::Started);
    assert_eq!(latest.attempt.cleanup, AttemptCleanup::Pending);
    assert_eq!(
        store.get_run(&run).await.unwrap().unwrap().status,
        RunStatus::Running
    );
    let before = store.get_run(&run).await.unwrap().unwrap();
    clock.set(time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(31));
    assert!(matches!(
        runtime.resume(&run).await,
        Err(RuntimeError::Attempt(crabber_session::AttemptError::Busy))
    ));
    assert_eq!(provider.requests(), Vec::new());
    assert_eq!(before, store.get_run(&run).await.unwrap().unwrap());
}

pub(crate) async fn suspend_fixture(
    store: &MemoryStore,
    execution: &dyn ExecutionStore,
    session: &SessionId,
    checkpoint: Value,
    now: time::OffsetDateTime,
) {
    use crabber_session::{AttemptCommand, AttemptMutation, AttemptMutationOutcome, AttemptTarget};
    let target = AttemptTarget {
        id: crabber_core::AttemptId::new(),
        generation: crabber_core::AttemptGeneration::INITIAL,
    };
    let request = |command, revision| AttemptMutation {
        id: crabber_core::ControlId::new(),
        expected_revision: revision,
        command,
    };
    let AttemptMutationOutcome::Applied {
        receipt,
        authority: Some(authority),
    } = execution
        .mutate_attempt(
            request(
                AttemptCommand::Reserve {
                    target: target.clone(),
                },
                store.session_revision(session).await.unwrap(),
            ),
            None,
        )
        .await
        .unwrap()
    else {
        panic!("fresh reserve must grant authority")
    };
    let AttemptMutationOutcome::Applied { receipt, .. } = execution
        .mutate_attempt(
            request(
                AttemptCommand::Begin {
                    target: target.clone(),
                },
                receipt.revision,
            ),
            Some(authority.clone()),
        )
        .await
        .unwrap()
    else {
        panic!("fresh begin must apply")
    };
    execution
        .conclude_attempt(
            crabber_session::AttemptConclusion {
                finish: request(
                    AttemptCommand::Finish {
                        target,
                        outcome: crabber_session::AttemptFinish::Suspended,
                    },
                    receipt.revision,
                ),
                boundary: crabber_session::RunBoundary::Suspended {
                    checkpoint,
                    usage: Usage::default(),
                },
                created_at: now,
                turn_id: None,
                elapsed: std::time::Duration::ZERO,
            },
            Some(authority),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn expired_pristine_admission_and_unstarted_reservation_enter_the_model_driver_once() {
    use crabber_session::{AttemptCommand, AttemptMutation, AttemptTarget};
    for reserve_before_crash in [false, true] {
        let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
        let store = Arc::new(MemoryStore::with_clock(clock.clone()));
        let provider = FakeProvider::scripted(vec![text_script("recovered")]);
        let runtime = Orchestrator::builder()
            .behavior_version("unstarted-worker.v1")
            .clock(clock.clone())
            .store(store.clone())
            .resolver(Arc::new(provider.clone()))
            .plan_provider(Arc::new(StaticPlanProvider::new(vec![], vec![])))
            .build()
            .unwrap();
        let (plan, prepared, _) = runtime.prepare_admission(&request()).await.unwrap();
        plan.release();
        let admitted = store.admit_bound_run(prepared).await.unwrap();
        let execution = store.execution(admitted.fence).await.unwrap();
        if reserve_before_crash {
            execution
                .mutate_attempt(
                    AttemptMutation {
                        id: crabber_core::ControlId::new(),
                        expected_revision: store
                            .session_revision(&admitted.session.id)
                            .await
                            .unwrap(),
                        command: AttemptCommand::Reserve {
                            target: AttemptTarget {
                                id: crabber_core::AttemptId::new(),
                                generation: crabber_core::AttemptGeneration::INITIAL,
                            },
                        },
                    },
                    None,
                )
                .await
                .unwrap();
        }
        clock.set(time::OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(31));
        assert_eq!(
            runtime.resume(&admitted.run.id).await.unwrap().status,
            RunStatus::Completed
        );
        assert_eq!(provider.requests().len(), 1);
        assert_eq!(
            store
                .latest_execution_attempt(&admitted.run.id)
                .await
                .unwrap()
                .unwrap()
                .attempt
                .state,
            AttemptState::Completed
        );
    }
}
