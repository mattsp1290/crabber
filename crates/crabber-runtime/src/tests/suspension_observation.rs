//! Real native commit with delayed ACK and already in-flight stale observers.
use super::*;
use crabber_session::{AttemptCleanup, AttemptState, RunControlBoundary, RunControlError};
use std::sync::atomic::AtomicBool;

#[derive(Default)]
pub(super) struct ObservationGate {
    pub(super) lose_ack: bool,
    heartbeat_probe: bool,
    owner_read_probe: bool,
    owner_read_armed: AtomicBool,
    owner_read_entered: Notify,
    release_owner_read: Notify,
    owner_read_returned: Notify,
    observe_calls: AtomicUsize,
    renew_armed: AtomicBool,
    model_calls: AtomicUsize,
    wait_for_observe: AtomicBool,
    additional_observe: Notify,
    model_entered: Notify,
    release_model: Notify,
    observer_entered: Notify,
    release_observer: Notify,
    observer_returned: Notify,
    heartbeat_entered: Notify,
    release_heartbeat: Notify,
    heartbeat_returned: Notify,
    committed: Notify,
    release_ack: Notify,
    stale_observations: AtomicUsize,
    stale_renewals: AtomicUsize,
}
impl ObservationGate {
    pub(super) async fn acknowledge(&self) {
        self.committed.notify_one();
        self.release_ack.notified().await;
    }
    pub(super) async fn observe(
        &self,
        inner: &dyn ExecutionStore,
        turn: Option<&crabber_core::TurnId>,
    ) -> Result<RunControlBoundary, RunControlError> {
        if turn.is_some() {
            return inner.observe_run_controls(turn).await;
        }
        if self.observe_calls.fetch_add(1, Ordering::SeqCst) != 0 || self.heartbeat_probe {
            if self.wait_for_observe.swap(false, Ordering::SeqCst) {
                self.additional_observe.notify_one();
            }
            return inner.observe_run_controls(turn).await;
        }
        if self.owner_read_probe {
            // PG maps a fenced read refusal to StaleOwner; the Runtime's
            // follow-up owner read must respect a later quiescent boundary.
            self.owner_read_armed.store(true, Ordering::SeqCst);
            return Err(RunControlError::StaleOwner);
        }
        self.observer_entered.notify_one();
        self.release_observer.notified().await;
        let result = inner.observe_run_controls(turn).await;
        if matches!(result, Err(RunControlError::StaleOwner)) {
            self.stale_observations.fetch_add(1, Ordering::SeqCst);
        }
        self.observer_returned.notify_one();
        result
    }
    pub(super) async fn read_run(
        &self,
        store: &dyn Store,
        id: &RunId,
    ) -> Result<Option<Run>, StoreError> {
        let held = self.owner_read_armed.swap(false, Ordering::SeqCst);
        if held {
            self.owner_read_entered.notify_one();
            self.release_owner_read.notified().await;
        }
        let result = store.get_run(id).await;
        if held {
            self.owner_read_returned.notify_one();
        }
        result
    }
    pub(super) async fn renew(
        &self,
        inner: &dyn ExecutionStore,
        until: time::OffsetDateTime,
    ) -> Result<(), StoreError> {
        if !self.renew_armed.swap(false, Ordering::SeqCst) {
            return inner.renew_lease(until).await;
        }
        self.heartbeat_entered.notify_one();
        self.release_heartbeat.notified().await;
        let result = inner.renew_lease(until).await;
        if result.is_err() {
            self.stale_renewals.fetch_add(1, Ordering::SeqCst);
        }
        self.heartbeat_returned.notify_one();
        result
    }
}
struct HeldModel(Arc<ObservationGate>);
#[async_trait]
impl ModelStream for HeldModel {
    async fn stream(
        &self,
        request: ModelRequest,
        next: Arc<dyn Streamer>,
    ) -> Result<DeltaStream, ProviderError> {
        if self.0.model_calls.fetch_add(1, Ordering::SeqCst) == 0 {
            self.0
                .renew_armed
                .store(self.0.heartbeat_probe, Ordering::SeqCst);
            self.0.model_entered.notify_one();
            self.0.release_model.notified().await;
        } else {
            // Force a real background poll while each later worker is Active.
            self.0.wait_for_observe.store(true, Ordering::SeqCst);
            wait(&self.0.additional_observe, "fresh worker observer").await;
        }
        next.stream(request).await
    }
}
#[derive(Clone, Copy, Debug)]
enum Boundary {
    Approval,
    Generic,
    Tool,
}
fn runtime(
    boundary: Boundary,
    inner: Arc<MemoryStore>,
    gate: Arc<ObservationGate>,
    effects: Arc<AtomicUsize>,
    provider: FakeProvider,
) -> Orchestrator {
    let ordinary = tool(Arc::new(EchoTool(effects.clone())));
    let definition = if matches!(boundary, Boundary::Tool) {
        Arc::new(
            ToolDefinition::resumable(
                ordinary.info.clone(),
                Arc::new(tool_suspension_faults::Pausing(effects)),
            )
            .unwrap(),
        )
    } else {
        ordinary
    };
    let policy: Arc<dyn PermissionPolicy> = match boundary {
        Boundary::Approval => Arc::new(StaticPolicy::new(PermissionDecision::Ask)),
        Boundary::Generic => Arc::new(PausePolicy),
        Boundary::Tool => Arc::new(StaticPolicy::new(PermissionDecision::Allow)),
    };
    let heartbeat_interval = if gate.heartbeat_probe {
        std::time::Duration::from_millis(10)
    } else {
        std::time::Duration::from_secs(10)
    };
    Orchestrator::builder()
        .behavior_version("suspension-observation.v1")
        .store(Arc::new(DelayedTerminalStore {
            inner,
            gate: Arc::new(SettlementGate {
                observation: Some(gate.clone()),
                ..Default::default()
            }),
        }))
        .resolver(Arc::new(provider))
        .model_stream(Arc::new(HeldModel(gate)))
        .plan_provider(Arc::new(StaticPlanProvider::new(vec![definition], vec![])))
        .policy(policy)
        .heartbeat_interval(heartbeat_interval)
        .build()
        .unwrap()
}
async fn wait(signal: &Notify, label: &str) {
    tokio::time::timeout(std::time::Duration::from_secs(3), signal.notified())
        .await
        .unwrap_or_else(|error| panic!("{label}: {error:?}"));
}
#[tokio::test]
async fn committed_suspensions_wait_for_their_own_ack_not_stale_observers() {
    for boundary in [Boundary::Approval, Boundary::Generic, Boundary::Tool] {
        for lose_ack in [false, true] {
            for heartbeat_probe in [false, true] {
                journey(boundary, lose_ack, heartbeat_probe).await;
            }
        }
    }
}
async fn journey(boundary: Boundary, lose_ack: bool, heartbeat_probe: bool) {
    let inner = Arc::new(MemoryStore::new());
    let gate = Arc::new(ObservationGate {
        lose_ack,
        heartbeat_probe,
        ..Default::default()
    });
    let effects = Arc::new(AtomicUsize::new(0));
    let provider = FakeProvider::scripted(vec![
        call_script(ToolCallId::new(), r#"{"text":"once"}"#),
        text_script("done"),
        text_script("independent worker"),
    ]);
    let runtime = runtime(
        boundary,
        inner.clone(),
        gate.clone(),
        effects.clone(),
        provider.clone(),
    );
    let handle = runtime.start(request()).await.unwrap();
    let (session, run) = (handle.session_id().clone(), handle.run_id().clone());
    let done = handle.done();
    tokio::pin!(done);
    tokio::select! {
        () = wait(&gate.model_entered, "model entered") => (),
        early = done.as_mut() => panic!("{boundary:?} before model: {early:?}"),
    }
    if heartbeat_probe {
        wait(&gate.heartbeat_entered, "heartbeat entered").await;
    } else {
        wait(&gate.observer_entered, "observer entered").await;
    }
    gate.release_model.notify_one();
    wait(&gate.committed, "native pause committed").await;
    assert_eq!(
        inner.get_run(&run).await.unwrap().unwrap().status,
        RunStatus::Paused
    );
    if heartbeat_probe {
        gate.release_heartbeat.notify_one();
        wait(&gate.heartbeat_returned, "heartbeat returned stale").await;
    } else {
        gate.release_observer.notify_one();
        wait(&gate.observer_returned, "observer returned stale").await;
    }
    let early = tokio::time::timeout(std::time::Duration::from_millis(40), done.as_mut()).await;
    gate.release_ack.notify_one();
    assert!(
        early.is_err(),
        "{boundary:?} lose_ack={lose_ack}: stale observer won before ACK: {early:?}"
    );
    let result = tokio::time::timeout(std::time::Duration::from_secs(3), done)
        .await
        .unwrap();
    assert_eq!(
        if heartbeat_probe {
            gate.stale_renewals.load(Ordering::SeqCst)
        } else {
            gate.stale_observations.load(Ordering::SeqCst)
        },
        1
    );
    assert_ack_result(boundary, lose_ack, result);
    assert_native_suspension(inner.as_ref(), &session, &run, boundary).await;
    assert_eq!(
        effects.load(Ordering::SeqCst),
        usize::from(matches!(boundary, Boundary::Tool))
    );
    assert_eq!(provider.requests().len(), 1);
    if matches!(boundary, Boundary::Generic) && !lose_ack {
        assert_fresh_workers(&runtime, &run, gate.as_ref(), &provider, effects.as_ref()).await;
    }
}

async fn assert_native_suspension(
    inner: &MemoryStore,
    session: &SessionId,
    run: &RunId,
    boundary: Boundary,
) {
    let parent = inner
        .latest_execution_attempt(run)
        .await
        .unwrap()
        .unwrap()
        .attempt;
    assert_eq!(
        (parent.state, parent.cleanup),
        (AttemptState::Suspended, AttemptCleanup::Complete)
    );
    let events = inner.list_events(session, None, 1000).await.unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == EventKind::RunPaused)
            .count(),
        1
    );
    assert!(!events.iter().any(|e| e.kind == EventKind::RunSettled));
    assert!(!events.iter().any(|e| e.kind == EventKind::ToolCallSettled));
    let phases = inner
        .list_tool_phases(crabber_session::PhasePageRequest {
            session_id: session.clone(),
            run_id: run.clone(),
            limit: 64,
            after: None,
            expected_revision: None,
        })
        .await
        .unwrap()
        .phases;
    assert_eq!(
        phases.len(),
        usize::from(matches!(boundary, Boundary::Tool))
    );
    for phase in phases {
        assert_eq!(
            (phase.state, phase.quiescence),
            (
                crabber_session::PhaseState::Suspended,
                crabber_session::PhaseQuiescence::Complete
            )
        );
    }
    assert!(
        inner
            .list_unfinished_tool_calls(run)
            .await
            .unwrap()
            .iter()
            .all(|call| call.result.is_none())
    );

    let kind = if matches!(boundary, Boundary::Generic) {
        EventKind::ExecutionAttemptChanged
    } else {
        EventKind::ContinuationChanged
    };
    let source = events
        .iter()
        .rev()
        .find(|event| event.kind == kind)
        .unwrap();
    let operation = crabber_core::ControlId(source.correlation.clone().unwrap());
    let revision = inner.session_revision(session).await.unwrap();
    let found = match boundary {
        Boundary::Approval => inner
            .lookup_approval(session, &operation)
            .await
            .unwrap()
            .is_some(),
        Boundary::Tool => inner
            .lookup_tool_continuation(session, &operation)
            .await
            .unwrap()
            .is_some(),
        Boundary::Generic => inner
            .lookup_attempt_mutation(session, &operation)
            .await
            .unwrap()
            .is_some(),
    };
    assert!(found, "lost ACK erased original native metadata");
    assert_eq!(inner.session_revision(session).await.unwrap(), revision);
}

async fn assert_fresh_workers(
    runtime: &Orchestrator,
    run: &RunId,
    gate: &ObservationGate,
    provider: &FakeProvider,
    effects: &AtomicUsize,
) {
    let observed_before = gate.observe_calls.load(Ordering::SeqCst);
    assert_eq!(
        runtime.resume(run).await.unwrap().status,
        RunStatus::Completed
    );
    assert!(
        gate.observe_calls.load(Ordering::SeqCst) > observed_before,
        "a resumed worker inherited the closed observer lifetime"
    );
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    let observed_before = gate.observe_calls.load(Ordering::SeqCst);
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
    assert!(
        gate.observe_calls.load(Ordering::SeqCst) > observed_before,
        "a new worker inherited a closed lifetime through the host clone"
    );
    assert_eq!(provider.requests().len(), 3);
}

fn assert_ack_result(
    boundary: Boundary,
    lose_ack: bool,
    result: Result<crate::RunResult, RuntimeError>,
) {
    if !lose_ack {
        assert_eq!(result.unwrap().status, RunStatus::Paused);
        return;
    }
    match (boundary, result) {
        (
            Boundary::Approval,
            Err(RuntimeError::ApprovalBoundaryUnknown {
                reason: crabber_session::ApprovalError::UnknownStoreOutcome,
                ..
            }),
        )
        | (
            Boundary::Generic,
            Err(RuntimeError::AttemptBoundaryUnknown {
                reason: crabber_session::AttemptError::UnknownStoreOutcome,
                ..
            }),
        )
        | (
            Boundary::Tool,
            Err(RuntimeError::ToolContinuationBoundaryUnknown {
                reason: crabber_session::ToolContinuationError::UnknownStoreOutcome,
                ..
            }),
        ) => (),
        (_, result) => panic!("{boundary:?}: {result:?}"),
    }
}

#[tokio::test]
async fn worker_join_retains_original_heartbeat_renewal_after_main_returns() {
    let inner = Arc::new(MemoryStore::new());
    let gate = Arc::new(ObservationGate {
        heartbeat_probe: true,
        ..Default::default()
    });
    let provider =
        FakeProvider::scripted(vec![call_script(ToolCallId::new(), r#"{"text":"once"}"#)]);
    let runtime = runtime(
        Boundary::Generic,
        inner.clone(),
        gate.clone(),
        Arc::new(AtomicUsize::new(0)),
        provider.clone(),
    );
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    let done = handle.done();
    tokio::pin!(done);
    wait(&gate.model_entered, "model entered").await;
    wait(&gate.heartbeat_entered, "original renewal entered").await;
    gate.release_model.notify_one();
    wait(&gate.committed, "native pause committed").await;
    gate.release_ack.notify_one();
    let early = tokio::time::timeout(std::time::Duration::from_millis(50), done.as_mut()).await;
    assert!(
        early.is_err(),
        "worker joined while its original renewal was blocked: {early:?}"
    );
    let pending = runtime
        .drain_session_execution_tasks(&session, std::time::Duration::from_millis(20))
        .await;
    assert_eq!(pending.pending, vec![run.clone()]);
    assert!(pending.background_tasks.iter().any(|scope| {
        scope.session_id == session
            && scope.run_id == run
            && scope.kind == crate::WorkerResourceKind::LeaseHeartbeat
            && scope.resources.pending == 1
    }));
    assert_eq!(gate.stale_renewals.load(Ordering::SeqCst), 0);
    gate.release_heartbeat.notify_one();
    wait(&gate.heartbeat_returned, "same original renewal returned").await;
    let result = tokio::time::timeout(std::time::Duration::from_secs(3), done)
        .await
        .unwrap();
    assert_ack_result(Boundary::Generic, false, result);
    let joined = runtime
        .drain_session_execution_tasks(&session, std::time::Duration::from_secs(1))
        .await;
    assert_eq!(joined.pending.len(), 0);
    assert_eq!(gate.stale_renewals.load(Ordering::SeqCst), 1);
    assert_eq!(provider.requests().len(), 1);
}

#[tokio::test]
async fn owner_read_finishing_after_quiescent_pause_waits_for_original_ack() {
    for lose_ack in [false, true] {
        let inner = Arc::new(MemoryStore::new());
        let gate = Arc::new(ObservationGate {
            lose_ack,
            owner_read_probe: true,
            ..Default::default()
        });
        let provider =
            FakeProvider::scripted(vec![call_script(ToolCallId::new(), r#"{"text":"once"}"#)]);
        let runtime = runtime(
            Boundary::Generic,
            inner.clone(),
            gate.clone(),
            Arc::new(AtomicUsize::new(0)),
            provider,
        );
        let handle = runtime.start(request()).await.unwrap();
        let (session, run) = (handle.session_id().clone(), handle.run_id().clone());
        let done = handle.done();
        tokio::pin!(done);
        wait(&gate.model_entered, "held model").await;
        wait(&gate.owner_read_entered, "active observer owner read").await;
        gate.release_model.notify_one();
        wait(&gate.committed, "real quiescent pause commit").await;
        gate.release_owner_read.notify_one();
        wait(&gate.owner_read_returned, "read after quiescence").await;
        let early = tokio::time::timeout(std::time::Duration::from_millis(40), done.as_mut()).await;
        gate.release_ack.notify_one();
        assert!(
            early.is_err(),
            "obsolete owner read superseded original ACK: {early:?}"
        );
        let result = tokio::time::timeout(std::time::Duration::from_secs(3), done)
            .await
            .unwrap();
        assert_ack_result(Boundary::Generic, lose_ack, result);
        assert_native_suspension(inner.as_ref(), &session, &run, Boundary::Generic).await;
    }
}
