//! Actual Tool/Frontend completion, parked task identity and cold recovery.
use super::*;
use crabber_core::{Clock, ManualClock};
#[path = "pending_tool/choice.rs"]
mod choice;
#[path = "pending_tool/faults.rs"]
mod faults;
#[path = "pending_tool/fixture.rs"]
mod fixture;
use choice::Choice;
fn assert_task_identity(h: &Harness) {
    let ids = h.gate.initial_task_ids.lock().unwrap();
    let task = |wanted| ids.iter().find(|(stage, _)| *stage == wanted).unwrap().1;
    let complete = task(InitialTaskStage::CompletePreparation);
    assert!(complete.is_some());
    assert_eq!(complete, task(InitialTaskStage::RegisterDriver));
    assert_eq!(complete, task(InitialTaskStage::BeginAttempt));
}
async fn replay_without_factory(
    h: &Harness,
    choice: &Choice,
) -> crabber_session::ToolContinuationReceipt {
    let count = h.provider.calls.load(Ordering::SeqCst);
    let crate::ToolContinuation::Replayed(receipt) = choice.invoke(&h.runtime).await.unwrap()
    else {
        panic!("metadata only")
    };
    assert_eq!(h.provider.calls.load(Ordering::SeqCst), count);
    receipt
}
async fn restart(h: &mut Harness, choice: &Choice, clock: Arc<ManualClock>) -> Orchestrator {
    let cold = fixture::runtime(h, clock);
    let warm = std::mem::replace(&mut h.runtime, cold);
    assert!(
        h.runtime
            .retained_preparation_status(&choice.original().session_id)
            .is_empty()
    );
    replay_without_factory(h, choice).await;
    assert_eq!(h.fake.requests().len(), 1);
    // Keep the actual old controller: an empty cold registry proves no join.
    warm
}
async fn journey(
    store: Arc<dyn Store>,
    clock: Arc<ManualClock>,
    frontend: bool,
    unkeyed: bool,
    fault: usize,
) {
    let (mut h, choice, effects) = fixture::paused(store, clock.clone(), frontend, unkeyed).await;
    h.gate
        .initial_task_diagnostics
        .store(true, Ordering::SeqCst);
    h.gate.initial_task_ids.lock().unwrap().clear();
    h.gate.preparation_fault.store(fault, Ordering::SeqCst);
    let result = choice.invoke(&h.runtime).await;
    let desc = choice.descriptor(&h).await;
    let fresh = if fault == 0 {
        let crate::ToolContinuation::Started { handle, receipt } = result.unwrap() else {
            panic!("fresh pending main")
        };
        assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
        assert_eq!(
            effects.resumes.load(Ordering::SeqCst),
            usize::from(!frontend)
        );
        assert_eq!(h.fake.requests().len(), 2);
        assert_task_identity(&h);
        Some(receipt)
    } else {
        assert!(result.is_err());
        assert_eq!(effects.resumes.load(Ordering::SeqCst), 0);
        assert_eq!(h.fake.requests().len(), 1);
        assert!(h.gate.successor_request.lock().unwrap().is_none());
        None
    };
    let original_receipt = replay_without_factory(&h, &choice).await;
    if let Some(fresh) = fresh {
        assert_eq!(fresh, original_receipt);
    }
    if fault == 0 {
        h.finish_cleanup(&desc).await;
    } else {
        h.runtime.cleanup_retained_preparation(&desc).await.unwrap();
        h.finish_cleanup_state(&desc, true).await;
        clock.set(clock.now() + time::Duration::seconds(31));
        let _warm = restart(&mut h, &choice, clock).await;
        let recovery = choice.recovery(&h).await;
        h.gate.initial_task_ids.lock().unwrap().clear();
        let crate::ToolContinuation::Started { handle, receipt } =
            recovery.invoke(&h.runtime).await.unwrap()
        else {
            panic!("fresh pending recovery")
        };
        assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
        assert_eq!(
            effects.resumes.load(Ordering::SeqCst),
            usize::from(!frontend)
        );
        assert_eq!(h.fake.requests().len(), 2);
        assert_task_identity(&h);
        assert_eq!(replay_without_factory(&h, &recovery).await, receipt);
        assert_eq!(replay_without_factory(&h, &choice).await, original_receipt);
        h.finish_cleanup(&recovery.descriptor(&h).await).await;
    }
    assert_eq!(
        effects.starts.load(Ordering::SeqCst),
        usize::from(!frontend)
    );
}
async fn contract(store: Arc<dyn Store>, clock: Arc<ManualClock>) {
    for frontend in [false, true] {
        for unkeyed in [false, true] {
            for fault in [0, 2, 4, 7] {
                Box::pin(journey(
                    store.clone(),
                    clock.clone(),
                    frontend,
                    unkeyed,
                    fault,
                ))
                .await;
            }
            Box::pin(faults::held_ack(
                store.clone(),
                clock.clone(),
                frontend,
                unkeyed,
            ))
            .await;
            Box::pin(faults::preflight(
                store.clone(),
                clock.clone(),
                frontend,
                unkeyed,
            ))
            .await;
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_runtime_pending_tool_actual_task_ack_and_cold_recovery() {
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    Box::pin(contract(
        Arc::new(MemoryStore::with_clock(clock.clone())),
        clock,
    ))
    .await;
}
#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_runtime_pending_tool_actual_task_ack_and_cold_recovery() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("pending-tool.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = Arc::new(
        crabber_session::SqliteStore::connect(path)
            .await
            .unwrap()
            .with_clock(clock.clone()),
    );
    Box::pin(contract(store, clock)).await;
}
#[cfg(feature = "postgres")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn postgres_runtime_pending_tool_actual_task_ack_and_cold_recovery() {
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
    Box::pin(contract(store, clock)).await;
}
