//! Actual committed calls survive waiter Drop; malformed ACKs grant nothing.
use super::super::tests::interposed::{Interposed, Stage};
use super::*;
use std::time::Duration;

pub(super) async fn held_contract<S: Store + 'static>(
    store: Arc<S>,
    clock: &ManualClock,
    frontend: bool,
    recovery: bool,
) {
    for stage in [
        Stage::Complete,
        Stage::Inspect,
        Stage::Register,
        Stage::Activate,
    ] {
        for fault in [false, true] {
            Box::pin(held(store.clone(), clock, frontend, recovery, stage, fault)).await;
        }
    }
}

pub(super) async fn malformed_contract<S: Store + 'static>(
    store: Arc<S>,
    clock: &ManualClock,
    frontend: bool,
    recovery: bool,
    exhaustive_validation: bool,
) {
    // Corruption validation lives in shared pending execution code. Memory
    // keeps every distinct validator branch; SQL retains forged metadata and
    // wrong typed outcomes through each persisted source/authentication path.
    let corruptions: &[u8] = if exhaustive_validation {
        &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    } else {
        &[0, 8]
    };
    for &corruption in corruptions {
        Box::pin(malformed(
            store.clone(),
            clock,
            frontend,
            recovery,
            corruption,
        ))
        .await;
    }
    Box::pin(replay(store.clone(), clock, frontend, recovery)).await;
}
async fn held<S: Store + 'static>(
    store: Arc<S>,
    clock: &ManualClock,
    frontend: bool,
    recovery: bool,
    stage: Stage,
    fault: bool,
) {
    let owner = PendingDriverTasks::default();
    let (context, original, _) =
        Box::pin(prepared(store.as_ref(), clock, frontend, recovery, false)).await;
    let adapter = Interposed::new(store, stage, fault);
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut task = PendingSuccessorTask::register(
        &owner,
        adapter.clone(),
        context,
        original,
        move |_, _| async move {
            count.fetch_add(1, Ordering::SeqCst);
        },
    )
    .unwrap();
    task.start().unwrap();
    tokio::time::timeout(Duration::from_secs(5), adapter.arrived.notified())
        .await
        .unwrap();
    drop(task);
    assert!(
        tokio::time::timeout(Duration::from_millis(15), owner.wait_for_supervision())
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    adapter.release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), owner.wait_for_supervision())
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(adapter.approval_complete_calls.load(Ordering::SeqCst), 1);
    assert_eq!(owner.available_capacity(), 64);
}
async fn malformed<S: Store + 'static>(
    store: Arc<S>,
    clock: &ManualClock,
    frontend: bool,
    recovery: bool,
    corruption: u8,
) {
    let owner = PendingDriverTasks::default();
    let (context, original, _) =
        Box::pin(prepared(store.as_ref(), clock, frontend, recovery, false)).await;
    let adapter = Interposed::new(store, Stage::Complete, false);
    adapter.release.notify_one();
    match corruption {
        0 => adapter.corrupt_receipt.store(true, Ordering::SeqCst),
        1 => adapter.corrupt_operation.store(true, Ordering::SeqCst),
        2 => adapter.corrupt_version.store(true, Ordering::SeqCst),
        3 => adapter.corrupt_inspection.store(true, Ordering::SeqCst),
        _ => adapter
            .tool_corruption
            .store(corruption - 3, Ordering::SeqCst),
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut task = PendingSuccessorTask::register(
        &owner,
        adapter.clone(),
        context,
        original,
        move |_, _| async move {
            count.fetch_add(1, Ordering::SeqCst);
        },
    )
    .unwrap();
    task.start().unwrap();
    let observed = task.join().await.unwrap();
    assert!(observed.error().is_some());
    assert!(observed.bound_main().is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(adapter.register_calls.load(Ordering::SeqCst), 0);
    owner.wait_for_supervision().await;
}
async fn replay<S: Store + 'static>(
    store: Arc<S>,
    clock: &ManualClock,
    frontend: bool,
    recovery: bool,
) {
    let owner = PendingDriverTasks::default();
    let (context, original, _) =
        Box::pin(prepared(store.as_ref(), clock, frontend, recovery, false)).await;
    let retry = original.clone();
    let adapter = Interposed::new(store.clone(), Stage::Complete, true);
    adapter.release.notify_one();
    let mut first =
        PendingSuccessorTask::register(&owner, adapter, context.clone(), original, |_, _| async {
            panic!("unknown committed ACK cannot dispatch")
        })
        .unwrap();
    first.start().unwrap();
    assert!(first.join().await.unwrap().error().is_some());
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut task =
        PendingSuccessorTask::register(&owner, store, context, retry, move |_, _| async move {
            count.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    task.start().unwrap();
    let observed = task.join().await.unwrap();
    assert!(observed.error().is_none(), "{:?}", observed.error());
    assert!(observed.bound_main().is_none());
    assert!(typed(observed.replayed().unwrap(), frontend, recovery));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    owner.wait_for_supervision().await;
}
