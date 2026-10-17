//! Four genuine Tool families keep completion, inspection and main in one task.
mod capacity;
mod cleanup;
mod faults;
use super::tests::interposed::{Interposed, Stage};
use super::*;
use crate::successor_driver_contract::tools::prepared;
use crate::{Store, ToolContinuationClaimOutcome};
use crabber_core::ManualClock;
use std::sync::atomic::{AtomicUsize, Ordering};

fn typed(metadata: &PendingSuccessorReplay, frontend: bool, recovery: bool) -> bool {
    matches!(
        (frontend, recovery, metadata),
        (false, false, PendingSuccessorReplay::ToolClaim(_))
            | (false, true, PendingSuccessorReplay::ToolRecovery(_))
            | (true, false, PendingSuccessorReplay::FrontendClaim(_))
            | (true, true, PendingSuccessorReplay::FrontendRecovery(_))
    )
}

async fn contract<S: Store + 'static>(store: Arc<S>, clock: Arc<ManualClock>) {
    for frontend in [false, true] {
        for recovery in [false, true] {
            for unkeyed in [false, true] {
                Box::pin(same_task(
                    store.clone(),
                    &clock,
                    frontend,
                    recovery,
                    unkeyed,
                ))
                .await;
            }
            Box::pin(faults::contract(store.clone(), &clock, frontend, recovery)).await;
            Box::pin(cleanup::blocked(store.clone(), &clock, frontend, recovery)).await;
        }
    }
}
async fn same_task<S: Store + 'static>(
    store: Arc<S>,
    clock: &ManualClock,
    frontend: bool,
    recovery: bool,
    unkeyed: bool,
) {
    let owner = PendingDriverTasks::default();
    let (context, original, _) =
        Box::pin(prepared(store.as_ref(), clock, frontend, recovery, unkeyed)).await;
    let retry = original.clone();
    let adapter = Interposed::new(store.clone(), Stage::Complete, false);
    adapter.release.notify_one();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let business_adapter = adapter.clone();
    let mut task = PendingSuccessorTask::register(
        &owner,
        adapter.clone(),
        context.clone(),
        original,
        move |context, completion| async move {
            assert!(context.try_enter_main().is_ok());
            assert!(matches!(
                completion,
                PreparationCompletion::ToolContinuation(
                    ToolContinuationClaimOutcome::Applied { .. }
                )
            ));
            let tasks = business_adapter.call_tasks.lock().unwrap();
            assert_eq!(tasks.len(), 4);
            assert!(tasks.iter().all(|id| *id == tokio::task::id()));
            count.fetch_add(1, Ordering::SeqCst);
            context
        },
    )
    .unwrap();
    let control = task.control();
    task.start().unwrap();
    let observed = task.join().await.unwrap();
    assert!(observed.error().is_none(), "{:?}", observed.error());
    assert!(observed.bound_main().is_some());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(adapter.approval_complete_calls.load(Ordering::SeqCst), 1);
    assert_eq!(adapter.approval_inspect_calls.load(Ordering::SeqCst), 1);
    let PendingSuccessorCompletion::Fresh(metadata) = control.completion().await.unwrap() else {
        panic!("fresh typed metadata");
    };
    assert!(typed(&metadata, frontend, recovery));
    let mut replay = PendingSuccessorTask::register(&owner, store, context, retry, |_, _| async {
        panic!("exact replay cannot dispatch a second main")
    })
    .unwrap();
    replay.start().unwrap();
    let observed = replay.join().await.unwrap();
    assert!(observed.error().is_none(), "{:?}", observed.error());
    assert!(observed.bound_main().is_none());
    assert_eq!(observed.replayed(), Some(&metadata));
    owner.wait_for_supervision().await;
}
#[tokio::test]
async fn memory_pending_tool_same_task() {
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    Box::pin(contract(
        Arc::new(crate::MemoryStore::with_clock(clock.clone())),
        clock,
    ))
    .await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_pending_tool_same_task() {
    let path = tempfile::tempdir().unwrap();
    let database = path.path().join("pending-tool.sqlite");
    crate::SqliteStore::migrate(&database).await.unwrap();
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    Box::pin(contract(
        Arc::new(
            crate::SqliteStore::connect(database)
                .await
                .unwrap()
                .with_clock(clock.clone()),
        ),
        clock,
    ))
    .await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_pending_tool_same_task() {
    let _guard = crate::postgres::pending_initial_test_lock().await;
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert_ne!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref(),
            Ok("1")
        );
        return;
    };
    crate::PostgresStore::migrate(&url).await.unwrap();
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    Box::pin(contract(
        Arc::new(
            crate::PostgresStore::connect(&url)
                .await
                .unwrap()
                .with_clock(clock.clone()),
        ),
        clock,
    ))
    .await;
}
