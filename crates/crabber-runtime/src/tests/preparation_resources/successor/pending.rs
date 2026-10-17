//! The original preclaim task survives caller Drop and owns metadata-only replay.
use super::*;
async fn descriptor(
    h: &Harness,
    receipt: &crabber_core::AdmissionReceipt,
    expired: bool,
) -> PreparationReconciliation {
    let key = if expired {
        PreparationKey::ExpiredRun {
            run_id: receipt.run_id.clone(),
            issuance: 1,
        }
    } else {
        PreparationKey::AdmissionRecovery {
            key: options().key,
            issuance: 1,
        }
    };
    h.runtime
        .lookup_preparation_operation(&receipt.session_id, &key)
        .await
        .unwrap()
        .unwrap()
}
async fn dropped_ack(store: Arc<dyn Store>, clock: Arc<ManualClock>, expired: bool) {
    let gate = Arc::new(SettlementGate::default());
    gate.release.notify_one();
    let wrapped = Arc::new(DelayedTerminalStore {
        inner: store.clone(),
        gate: gate.clone(),
    });
    let h = Harness::on_store_with_clock(wrapped, store, gate, Some(clock.clone()));
    let receipt = initial(&h, &clock, expired).await;
    h.gate.preparation_hold.store(true, Ordering::SeqCst);
    let runtime = h.runtime.clone();
    let input = h.input.clone();
    let run = receipt.run_id.clone();
    let caller = tokio::spawn(async move {
        if expired {
            runtime.resume_execution(&run).await.map(|_| ())
        } else {
            runtime
                .recover_admission(input, options())
                .await
                .map(|_| ())
        }
    });
    tokio::time::timeout(
        Duration::from_secs(5),
        h.gate.preparation_entered.notified(),
    )
    .await
    .unwrap();
    let descriptor = descriptor(&h, &receipt, expired).await;
    let calls = h.gate.preparation_calls.load(Ordering::SeqCst);
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    // Public caller Drop detaches only its result waiter. Explicit cleanup
    // requests cooperative stop without dropping the original Store future.
    h.runtime
        .cleanup_retained_preparation(&descriptor)
        .await
        .unwrap();
    // Neither status nor retry waits for the pending-main mutex/native ACK.
    tokio::time::timeout(Duration::from_millis(200), async {
        assert_ne!(
            h.runtime
                .retained_preparation_status(&descriptor.session_id),
            vec![]
        );
        assert!(
            h.runtime
                .retry_preparation_completion(&descriptor)
                .await
                .is_err()
        );
    })
    .await
    .unwrap();
    assert_eq!(h.gate.preparation_calls.load(Ordering::SeqCst), calls);
    assert!(h.gate.successor_request.lock().unwrap().is_none());
    assert_eq!(h.fake.requests().len(), 0);
    h.gate.preparation_release.notify_one();
    h.finish_cleanup_state(&descriptor, true).await;
    assert!(h.gate.successor_request.lock().unwrap().is_none());
    assert_eq!(h.fake.requests().len(), 0);
    assert_eq!(h.gate.preparation_calls.load(Ordering::SeqCst), calls);
}
async fn replay(store: Arc<dyn Store>, clock: Arc<ManualClock>, expired: bool) {
    let gate = Arc::new(SettlementGate::default());
    gate.release.notify_one();
    let wrapped = Arc::new(DelayedTerminalStore {
        inner: store.clone(),
        gate: gate.clone(),
    });
    let h = Harness::on_store_with_clock(wrapped, store, gate, Some(clock.clone()));
    let receipt = initial(&h, &clock, expired).await;
    h.gate.preparation_fault.store(3, Ordering::SeqCst);
    if expired {
        assert!(h.runtime.resume_execution(&receipt.run_id).await.is_err());
    } else {
        let crate::Admission::Replayed(actual) = h
            .runtime
            .recover_admission(h.input.clone(), options())
            .await
            .unwrap()
        else {
            panic!("actual metadata replay")
        };
        assert_eq!(actual, receipt);
    }
    let descriptor = descriptor(&h, &receipt, expired).await;
    assert!(h.gate.successor_request.lock().unwrap().is_none());
    assert_eq!(h.fake.requests().len(), 0);
    h.finish_cleanup(&descriptor).await;
    assert!(h.gate.successor_request.lock().unwrap().is_none());
    assert_eq!(h.fake.requests().len(), 0);
    assert_eq!(
        h.runtime
            .retained_preparation_status(&descriptor.session_id),
        vec![]
    );
}
async fn dropped_waiter(store: Arc<dyn Store>, clock: Arc<ManualClock>, expired: bool) {
    let gate = Arc::new(SettlementGate::default());
    gate.release.notify_one();
    let wrapped = Arc::new(DelayedTerminalStore {
        inner: store.clone(),
        gate: gate.clone(),
    });
    let h = Harness::on_store_with_clock(wrapped, store, gate, Some(clock.clone()));
    let receipt = initial(&h, &clock, expired).await;
    h.gate.preparation_hold.store(true, Ordering::SeqCst);
    let runtime = h.runtime.clone();
    let input = h.input.clone();
    let run = receipt.run_id.clone();
    let caller = tokio::spawn(async move {
        if expired {
            runtime.resume_execution(&run).await.map(|_| ())
        } else {
            runtime
                .recover_admission(input, options())
                .await
                .map(|_| ())
        }
    });
    tokio::time::timeout(
        Duration::from_secs(5),
        h.gate.preparation_entered.notified(),
    )
    .await
    .unwrap();
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    assert!(h.gate.successor_request.lock().unwrap().is_none());
    h.gate.preparation_release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            // The adapter captures the request before the actual Register
            // transaction, so presence alone cannot imply an owner already exists.
            let request = h.gate.successor_request.lock().unwrap().clone();
            if let Some(request) = request {
                let owner = h
                    .memory
                    .get_session_owner(
                        request.context.session_id(),
                        &request.identities().unwrap().owner,
                    )
                    .await;
                if owner.is_ok_and(|owner| owner.state == SessionOwnerState::Joined) {
                    break;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(h.fake.requests().len(), 1);
    cleanup(&h, expired).await;
}
pub(super) async fn contract(store: Arc<dyn Store>, clock: Arc<ManualClock>) {
    for expired in [false, true] {
        dropped_ack(store.clone(), clock.clone(), expired).await;
        dropped_waiter(store.clone(), clock.clone(), expired).await;
        replay(store.clone(), clock.clone(), expired).await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn memory_runtime_successor_pending_ack_and_replay() {
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    contract(Arc::new(MemoryStore::with_clock(clock.clone())), clock).await;
}
