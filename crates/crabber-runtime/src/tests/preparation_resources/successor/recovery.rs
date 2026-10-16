//! Unknown native completion and restart never replay a business dispatch.
use super::*;
pub(super) async fn contract(store: Arc<dyn Store>, clock: Arc<ManualClock>) {
    for expired in [false, true] {
        for point in [1, 2] {
            let gate = Arc::new(SettlementGate::default());
            gate.release.notify_one();
            let wrapped = Arc::new(DelayedTerminalStore {
                inner: store.clone(),
                gate: gate.clone(),
            });
            let h = Harness::on_store_with_clock(wrapped, store.clone(), gate, Some(clock.clone()));
            let receipt = initial(&h, &clock, expired).await;
            h.gate.preparation_fault.store(point, Ordering::SeqCst);
            let error = if expired {
                h.runtime
                    .resume_execution(&receipt.run_id)
                    .await
                    .err()
                    .unwrap()
            } else {
                h.runtime
                    .recover_admission(h.input.clone(), options())
                    .await
                    .err()
                    .unwrap()
            };
            let descriptor = error.preparation_reconciliation().unwrap().clone();
            assert_eq!(h.provider.calls.load(Ordering::SeqCst), 2);
            assert_eq!(h.fake.requests().len(), 0);
            assert!(h.gate.successor_request.lock().unwrap().is_none());
            let progress = h
                .runtime
                .retry_preparation_completion(&descriptor)
                .await
                .unwrap();
            assert_eq!(progress.replayed, point == 2);
            h.finish_cleanup(&descriptor).await;
            assert_eq!(h.fake.requests().len(), 0);
            clock.set(clock.now() + time::Duration::seconds(31));
            let gate = Arc::new(SettlementGate::default());
            gate.release.notify_one();
            let wrapped = Arc::new(DelayedTerminalStore {
                inner: store.clone(),
                gate: gate.clone(),
            });
            let mut cold =
                Harness::on_store_with_clock(wrapped, store.clone(), gate, Some(clock.clone()));
            cold.input = h.input.clone();
            let handle = start(&cold, &receipt, expired).await;
            tokio::time::timeout(Duration::from_secs(5), handle.done())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(captured(&cold).claim.issuance, 3);
            cleanup(&cold, expired).await;
            assert_eq!(cold.fake.requests().len(), 1);
            assert_eq!(cold.provider.calls.load(Ordering::SeqCst), 1);
            assert_eq!(h.fake.requests().len(), 0);
            assert_eq!(h.provider.calls.load(Ordering::SeqCst), 2);
        }
        unsupported(store.clone(), clock.clone(), expired).await;
    }
}
async fn unsupported(store: Arc<dyn Store>, clock: Arc<ManualClock>, expired: bool) {
    let gate = Arc::new(SettlementGate::default());
    gate.release.notify_one();
    let wrapped = Arc::new(DelayedTerminalStore {
        inner: store.clone(),
        gate: gate.clone(),
    });
    let h = Harness::on_store_with_clock(wrapped, store, gate, Some(clock.clone()));
    let receipt = initial(&h, &clock, expired).await;
    h.gate.successor_disabled.store(true, Ordering::SeqCst);
    let error = if expired {
        h.runtime
            .resume_execution(&receipt.run_id)
            .await
            .err()
            .unwrap()
    } else {
        h.runtime
            .recover_admission(h.input.clone(), options())
            .await
            .err()
            .unwrap()
    };
    assert!(format!("{error:?}").contains("Unsupported"));
    assert_eq!(h.provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(h.fake.requests().len(), 0);
    assert!(h.gate.successor_request.lock().unwrap().is_none());
}
#[tokio::test]
async fn memory_runtime_successor_cold_native_reconciliation() {
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    contract(Arc::new(MemoryStore::with_clock(clock.clone())), clock).await;
}
