//! Actual Approval completion stays inside its original parked worker.
use super::*;
use crabber_core::{Clock, ManualClock};
use crabber_session::{ApprovalAnswer, ApprovalReply, ApprovalResolverClaim, CallApproval};
async fn paused(
    store: Arc<dyn Store>,
    clock: Arc<ManualClock>,
) -> (Harness, crate::ContinueApproval, Arc<AtomicUsize>) {
    let gate = Arc::new(SettlementGate::default());
    gate.release.notify_one();
    let effects = Arc::new(AtomicUsize::new(0));
    let mut provider = ScopedProvider::new();
    Arc::get_mut(&mut provider).unwrap().base =
        StaticPlanProvider::new(vec![tool(Arc::new(EchoTool(effects.clone())))], vec![]);
    let fake = FakeProvider::scripted(vec![
        call_script(ToolCallId::new(), r#"{"text":"ask"}"#),
        text_script("done"),
    ]);
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(options().behavior_fingerprint)
        .clock(clock)
        .store(Arc::new(DelayedTerminalStore {
            inner: store.clone(),
            gate: gate.clone(),
        }))
        .resolver(Arc::new(fake.clone()))
        .plan_provider(provider.clone())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Ask)))
        .build()
        .unwrap();
    let mut input = request();
    input.session_id = Some(SessionId::new());
    let h = Harness {
        runtime,
        memory: store,
        provider,
        gate,
        fake,
        input,
    };
    let crate::Admission::Started { handle, receipt } = h
        .runtime
        .start_keyed(h.input.clone(), options())
        .await
        .unwrap()
    else {
        panic!("fresh admission")
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Paused);
    h.finish_cleanup(&h.descriptor().await).await;
    let snapshot = h
        .runtime
        .approval_snapshot(&receipt.session_id, &receipt.run_id)
        .await
        .unwrap()
        .unwrap();
    let CallApproval::Ask { id, description } = &snapshot.calls[0].decision else {
        panic!("actual Ask")
    };
    let answer = h
        .runtime
        .answer_approval(ApprovalAnswer {
            id: crabber_core::ControlId::new(),
            expected_revision: snapshot.revision,
            session_id: receipt.session_id.clone(),
            run_id: receipt.run_id.clone(),
            continuation: snapshot.continuation.clone(),
            interaction_id: id.clone(),
            resolver: ApprovalResolverClaim {
                resolver_id: "host".into(),
                authority: description.authority().clone(),
                authority_claim: "verified".into(),
            },
            reply: ApprovalReply::Grant,
        })
        .await
        .unwrap();
    let choice = crate::ContinueApproval {
        id: crabber_core::ControlId::new(),
        session_id: receipt.session_id,
        run_id: receipt.run_id,
        continuation: snapshot.continuation,
        expected_revision: answer.revision,
    };
    (h, choice, effects)
}
async fn descriptor(h: &Harness, id: &crabber_core::ControlId) -> PreparationReconciliation {
    h.runtime
        .lookup_preparation_operation(
            h.input.session_id.as_ref().unwrap(),
            &PreparationKey::Approval(id.clone()),
        )
        .await
        .unwrap()
        .unwrap()
}
async fn restart(
    h: &mut Harness,
    choice: &crate::ContinueApproval,
    clock: Arc<ManualClock>,
) -> Orchestrator {
    let cold = Orchestrator::builder()
        .behavior_fingerprint(options().behavior_fingerprint)
        .clock(clock)
        .store(Arc::new(DelayedTerminalStore {
            inner: h.memory.clone(),
            gate: h.gate.clone(),
        }))
        .resolver(Arc::new(h.fake.clone()))
        .plan_provider(h.provider.clone())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Ask)))
        .build()
        .unwrap();
    let warm = std::mem::replace(&mut h.runtime, cold);
    assert_eq!(
        h.runtime.retained_preparation_status(&choice.session_id),
        vec![]
    );
    let calls = h.provider.calls.load(Ordering::SeqCst);
    assert!(matches!(
        h.runtime.continue_approval(choice.clone()).await.unwrap(),
        crate::Continuation::Replayed(_)
    ));
    assert_eq!(h.provider.calls.load(Ordering::SeqCst), calls);
    assert_eq!(h.fake.requests().len(), 1);
    // Retain the old controller too: local emptiness/drop is no physical proof.
    warm
}
async fn contract(store: Arc<dyn Store>, clock: Arc<ManualClock>) {
    for fault in [0, 2, 4, 7] {
        let (mut h, choice, effects) = paused(store.clone(), clock.clone()).await;
        h.gate
            .initial_task_diagnostics
            .store(true, Ordering::SeqCst);
        h.gate.initial_task_ids.lock().unwrap().clear();
        h.gate.preparation_fault.store(fault, Ordering::SeqCst);
        let result = h.runtime.continue_approval(choice.clone()).await;
        let desc = descriptor(&h, &choice.id).await;
        if fault == 0 {
            let crate::Continuation::Started { handle, .. } = result.unwrap() else {
                panic!("fresh pending main")
            };
            assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
            assert_eq!(effects.load(Ordering::SeqCst), 1);
            let ids = h.gate.initial_task_ids.lock().unwrap();
            let completed = ids
                .iter()
                .find(|(stage, _)| matches!(stage, InitialTaskStage::CompletePreparation))
                .unwrap()
                .1;
            let registered = ids
                .iter()
                .find(|(stage, _)| matches!(stage, InitialTaskStage::RegisterDriver))
                .unwrap()
                .1;
            let began = ids
                .iter()
                .find(|(stage, _)| matches!(stage, InitialTaskStage::BeginAttempt))
                .unwrap()
                .1;
            assert!(completed.is_some());
            assert_eq!(completed, registered);
            assert_eq!(completed, began);
        } else {
            assert!(result.is_err());
            assert_eq!(effects.load(Ordering::SeqCst), 0);
            assert_eq!(h.fake.requests().len(), 1);
            assert!(h.gate.successor_request.lock().unwrap().is_none());
        }
        let count = h.provider.calls.load(Ordering::SeqCst);
        assert!(matches!(
            h.runtime.continue_approval(choice.clone()).await.unwrap(),
            crate::Continuation::Replayed(_)
        ));
        assert_eq!(h.provider.calls.load(Ordering::SeqCst), count);
        if fault == 0 {
            h.finish_cleanup(&desc).await;
        } else {
            h.runtime.cleanup_retained_preparation(&desc).await.unwrap();
            h.finish_cleanup_state(&desc, true).await;
            clock.set(clock.now() + time::Duration::seconds(31));
            let _warm = restart(&mut h, &choice, clock.clone()).await;
            let recovery = crate::RecoverApproval {
                id: crabber_core::ControlId::new(),
                expected_revision: h.memory.session_revision(&choice.session_id).await.unwrap(),
                original: choice,
            };
            let crate::Continuation::Started { handle, .. } = h
                .runtime
                .recover_approval_continuation(recovery.clone())
                .await
                .unwrap()
            else {
                panic!("fresh recovery pending main")
            };
            assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
            assert_eq!(effects.load(Ordering::SeqCst), 1);
            assert!(matches!(
                h.runtime
                    .recover_approval_continuation(recovery.clone())
                    .await
                    .unwrap(),
                crate::Continuation::Replayed(_)
            ));
            h.finish_cleanup(&descriptor(&h, &recovery.id).await).await;
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn memory_runtime_pending_approval_actual_task_ack_and_cold_recovery() {
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    contract(Arc::new(MemoryStore::with_clock(clock.clone())), clock).await;
}
async fn held_ack(store: Arc<dyn Store>, clock: Arc<ManualClock>) {
    let (h, choice, effects) = paused(store, clock).await;
    h.gate.preparation_hold.store(true, Ordering::SeqCst);
    let runtime = h.runtime.clone();
    let original = choice.clone();
    let caller = tokio::spawn(async move { runtime.continue_approval(original).await.map(|_| ()) });
    tokio::time::timeout(
        Duration::from_secs(5),
        h.gate.preparation_entered.notified(),
    )
    .await
    .unwrap();
    let desc = descriptor(&h, &choice.id).await;
    assert_eq!(h.provider.calls.load(Ordering::SeqCst), 2);
    assert_eq!(h.child_state(&desc).await, SessionOwnerState::Active);
    assert!(h.gate.successor_request.lock().unwrap().is_none());
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    // Stop only signals the original pending task; the original Store future
    // and actual cleanup remain retained until their own acknowledgments.
    h.runtime.cleanup_retained_preparation(&desc).await.unwrap();
    assert_eq!(h.child_state(&desc).await, SessionOwnerState::Active);
    h.gate.preparation_release.notify_one();
    h.finish_cleanup_state(&desc, true).await;
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert_eq!(h.fake.requests().len(), 1);
    assert!(h.gate.successor_request.lock().unwrap().is_none());
    assert!(matches!(
        h.runtime.continue_approval(choice).await.unwrap(),
        crate::Continuation::Replayed(_)
    ));
    assert_eq!(h.provider.calls.load(Ordering::SeqCst), 2);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn memory_runtime_pending_approval_held_ack_retains_cleanup() {
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    held_ack(Arc::new(MemoryStore::with_clock(clock.clone())), clock).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn sqlite_runtime_pending_approval_actual_task_ack_and_cold_recovery() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("pending-approval.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = Arc::new(
        crabber_session::SqliteStore::connect(path)
            .await
            .unwrap()
            .with_clock(clock.clone()),
    );
    contract(store.clone(), clock.clone()).await;
    held_ack(store, clock).await;
}
#[cfg(feature = "postgres")]
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn postgres_runtime_pending_approval_actual_task_ack_and_cold_recovery() {
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert_ne!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref(),
            Ok("1")
        );
        return;
    };
    crabber_session::PostgresStore::migrate(&url).await.unwrap();
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = Arc::new(
        crabber_session::PostgresStore::connect(&url)
            .await
            .unwrap()
            .with_clock(clock.clone()),
    );
    contract(store.clone(), clock.clone()).await;
    held_ack(store, clock).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn pending_approval_preflight_refusal_precedes_factory_and_begin() {
    for fault in [5, 6] {
        let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
        let store = Arc::new(MemoryStore::with_clock(clock.clone()));
        let (h, choice, effects) = paused(store, clock).await;
        h.gate.preparation_fault.store(fault, Ordering::SeqCst);
        let before = h.memory.session_revision(&choice.session_id).await.unwrap();
        assert!(h.runtime.continue_approval(choice.clone()).await.is_err());
        assert_eq!(h.provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(effects.load(Ordering::SeqCst), 0);
        assert_eq!(h.fake.requests().len(), 1);
        assert!(
            h.runtime
                .lookup_preparation_operation(
                    &choice.session_id,
                    &PreparationKey::Approval(choice.id)
                )
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            h.memory.session_revision(&choice.session_id).await.unwrap(),
            before
        );
        assert!(h.gate.successor_request.lock().unwrap().is_none());
    }
}
