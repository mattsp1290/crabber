//! All eight successor tags share the same existing supervisor budget.
use super::*;
#[tokio::test]
async fn pending_tool_families_share_existing_capacity_and_require_capability() {
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = Arc::new(crate::MemoryStore::with_clock(clock.clone()));
    let adapter = Interposed::new(store.clone(), Stage::Complete, false);
    let owner = PendingDriverTasks::default();
    let mut originals = Vec::new();
    for admission in [true, false] {
        originals
            .push(super::super::tests::fixture::prepared(store.as_ref(), &clock, admission).await);
    }
    for recovery in [false, true] {
        originals.push(
            crate::successor_driver_contract::approval::pending::prepared(
                store.as_ref(),
                &clock,
                recovery,
                false,
            )
            .await,
        );
    }
    for frontend in [false, true] {
        for recovery in [false, true] {
            let (context, original, _) =
                Box::pin(prepared(store.as_ref(), &clock, frontend, recovery, false)).await;
            adapter.pending_supported.store(false, Ordering::SeqCst);
            assert!(matches!(
                PendingSuccessorTask::register(
                    &owner,
                    adapter.clone(),
                    context.clone(),
                    original.clone(),
                    |_, _| async {}
                ),
                Err(Error::Unsupported)
            ));
            adapter.pending_supported.store(true, Ordering::SeqCst);
            originals.push((context, original));
        }
    }
    let mut tasks = Vec::new();
    for index in 0..64 {
        let (context, original) = &originals[index % originals.len()];
        tasks.push(
            PendingSuccessorTask::register(
                &owner,
                adapter.clone(),
                context.clone(),
                original.clone(),
                |_, _| async { panic!("parked task cannot dispatch") },
            )
            .unwrap(),
        );
    }
    assert_eq!(owner.available_capacity(), 0);
    for (context, original) in originals {
        assert!(matches!(
            PendingSuccessorTask::register(
                &owner,
                adapter.clone(),
                context,
                original,
                |_, _| async {}
            ),
            Err(Error::Capacity)
        ));
    }
    assert_eq!(adapter.approval_complete_calls.load(Ordering::SeqCst), 0);
    drop(tasks);
    owner.wait_for_supervision().await;
    assert_eq!(owner.available_capacity(), 64);
    assert_eq!(adapter.approval_complete_calls.load(Ordering::SeqCst), 0);
}
