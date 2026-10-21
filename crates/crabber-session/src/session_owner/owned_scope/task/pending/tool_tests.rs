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

#[derive(Clone, Copy)]
enum BatchKind {
    Tool,
    Frontend,
    Mixed,
}

#[derive(Clone, Copy)]
enum Family {
    SuccessReplay,
    HeldAck,
    MalformedReplay,
    Cleanup,
}

async fn contract<S: Store + 'static>(
    store: Arc<S>,
    clock: Arc<ManualClock>,
    frontend: bool,
    recovery: bool,
    exhaustive_validation: bool,
    family: Family,
) {
    match family {
        Family::HeldAck => {
            Box::pin(faults::held_contract(store, &clock, frontend, recovery)).await;
            return;
        }
        Family::MalformedReplay => {
            Box::pin(faults::malformed_contract(
                store,
                &clock,
                frontend,
                recovery,
                exhaustive_validation,
            ))
            .await;
            return;
        }
        Family::Cleanup => {
            Box::pin(cleanup::blocked(store, &clock, frontend, recovery)).await;
            return;
        }
        Family::SuccessReplay => (),
    }
    for unkeyed in [false, true] {
        Box::pin(same_task(
            store.clone(),
            &clock,
            if frontend {
                BatchKind::Frontend
            } else {
                BatchKind::Tool
            },
            recovery,
            unkeyed,
        ))
        .await;
        if frontend {
            Box::pin(same_task(
                store.clone(),
                &clock,
                BatchKind::Mixed,
                recovery,
                unkeyed,
            ))
            .await;
        }
    }
}

async fn same_task<S: Store + 'static>(
    store: Arc<S>,
    clock: &ManualClock,
    kind: BatchKind,
    recovery: bool,
    unkeyed: bool,
) {
    let frontend = !matches!(kind, BatchKind::Tool);
    let mixed = matches!(kind, BatchKind::Mixed);
    let owner = PendingDriverTasks::default();
    let (context, original, batch) = if mixed {
        Box::pin(crate::successor_driver_contract::tools::prepared_mixed(
            store.as_ref(),
            clock,
            recovery,
            unkeyed,
        ))
        .await
    } else {
        Box::pin(prepared(store.as_ref(), clock, frontend, recovery, unkeyed)).await
    };
    if mixed {
        assert!(!batch.phases.is_empty());
        assert!(!batch.frontend.is_empty());
        assert!(batch.calls.len() > batch.phases.len() + batch.frontend.len());
    }
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
// Each family begins with a fresh Store; histories inside a family retain their
// original task identity, faults, replay and cleanup order.
macro_rules! cases {
    ($memory:ident, $sqlite:ident, $postgres:ident, $frontend:literal, $recovery:literal, $family:ident) => {
        #[tokio::test]
        async fn $memory() {
            let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
            Box::pin(contract(
                Arc::new(crate::MemoryStore::with_clock(clock.clone())),
                clock,
                $frontend,
                $recovery,
                true,
                Family::$family,
            ))
            .await;
        }
        #[cfg(feature = "sqlite")]
        #[tokio::test]
        async fn $sqlite() {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("pending-tool.sqlite");
            crate::SqliteStore::migrate(&path).await.unwrap();
            let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
            let store = crate::SqliteStore::connect(path)
                .await
                .unwrap()
                .with_clock(clock.clone());
            Box::pin(contract(
                Arc::new(store),
                clock,
                $frontend,
                $recovery,
                false,
                Family::$family,
            ))
            .await;
        }
        #[cfg(feature = "postgres")]
        #[tokio::test]
        async fn $postgres() {
            use crabber_test_support::postgres::{DatabaseMode, TestDatabase};
            TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
                let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
                let store = crate::PostgresStore::connect(&url)
                    .await
                    .unwrap()
                    .with_clock(clock.clone());
                Box::pin(contract(
                    Arc::new(store),
                    clock,
                    $frontend,
                    $recovery,
                    false,
                    Family::$family,
                ))
                .await;
            })
            .await
            .unwrap();
        }
    };
}
cases!(
    memory_pending_tool_claim,
    sqlite_pending_tool_claim,
    postgres_pending_tool_claim,
    false,
    false,
    SuccessReplay
);
cases!(
    memory_pending_tool_recovery,
    sqlite_pending_tool_recovery,
    postgres_pending_tool_recovery,
    false,
    true,
    SuccessReplay
);
cases!(
    memory_pending_frontend_claim,
    sqlite_pending_frontend_claim,
    postgres_pending_frontend_claim,
    true,
    false,
    SuccessReplay
);
cases!(
    memory_pending_frontend_recovery,
    sqlite_pending_frontend_recovery,
    postgres_pending_frontend_recovery,
    true,
    true,
    SuccessReplay
);

// Every independent family gets its own migrated Store and clock. A held ACK,
// exact replay or cleanup story remains internally sequential in its old helper.
macro_rules! fault_families {
    ($module:ident, $frontend:literal, $recovery:literal) => {
        mod $module {
            use super::*;
            cases!(
                memory_pending_held_ack,
                sqlite_pending_held_ack,
                postgres_pending_held_ack,
                $frontend,
                $recovery,
                HeldAck
            );
            cases!(
                memory_pending_malformed_replay,
                sqlite_pending_malformed_replay,
                postgres_pending_malformed_replay,
                $frontend,
                $recovery,
                MalformedReplay
            );
            cases!(
                memory_pending_cleanup,
                sqlite_pending_cleanup,
                postgres_pending_cleanup,
                $frontend,
                $recovery,
                Cleanup
            );
        }
    };
}
fault_families!(tool_claim, false, false);
fault_families!(tool_recovery, false, true);
fault_families!(frontend_claim, true, false);
fault_families!(frontend_recovery, true, true);
