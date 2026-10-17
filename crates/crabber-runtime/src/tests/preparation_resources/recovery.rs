//! Actual declared scoped acquisition for recovery, including unknown native ACK.
use super::*;
use crabber_core::{Clock, ManualClock};

pub(super) async fn contract(store: Arc<dyn Store>, clock: Arc<ManualClock>) {
    for point in [0, 1, 2, 3, 4] {
        let gate = Arc::new(SettlementGate::default());
        gate.release.notify_one();
        let wrapped = Arc::new(DelayedTerminalStore {
            inner: store.clone(),
            gate: gate.clone(),
        });
        let h = Harness::on_store_with_clock(wrapped, store.clone(), gate, Some(clock.clone()));
        // A committed admission with no worker grant retained by this controller.
        h.gate.preparation_fault.store(2, Ordering::SeqCst);
        let initial = h
            .runtime
            .start_keyed(h.input.clone(), options())
            .await
            .err()
            .unwrap();
        let initial = initial.preparation_reconciliation().unwrap().clone();
        h.runtime
            .retry_preparation_completion(&initial)
            .await
            .unwrap();
        h.finish_cleanup(&initial).await;
        let receipt = h
            .memory
            .lookup_admission(&initial.session_id, &options().key)
            .await
            .unwrap()
            .unwrap();
        clock.set(clock.now() + time::Duration::seconds(31));
        if point == 3 {
            unsupported_before_factory(&h, &initial).await;
            continue;
        }
        if point == 4 {
            changed_callbacks_before_factory(&h, clock.clone(), &receipt).await;
            continue;
        }
        h.gate.preparation_fault.store(point, Ordering::SeqCst);
        let recovery = h
            .runtime
            .recover_admission(h.input.clone(), options())
            .await;
        let descriptor = if point == 0 {
            let crate::Admission::Started {
                handle,
                receipt: saved,
            } = recovery.unwrap()
            else {
                panic!("fresh actual recovery grant")
            };
            assert_eq!(saved, receipt);
            handle.done().await.unwrap();
            h.runtime
                .lookup_preparation_operation(
                    &initial.session_id,
                    &PreparationKey::AdmissionRecovery {
                        key: options().key,
                        issuance: 1,
                    },
                )
                .await
                .unwrap()
                .unwrap()
        } else {
            let error = recovery.err().unwrap();
            let descriptor = error.preparation_reconciliation().unwrap().clone();
            assert_eq!(h.child_state(&descriptor).await, SessionOwnerState::Active);
            assert_eq!(h.provider.calls.load(Ordering::SeqCst), 2);
            assert_eq!(h.fake.requests().len(), 0);
            let calls = h.gate.preparation_calls.load(Ordering::SeqCst);
            assert!(
                h.runtime
                    .retry_preparation_completion(&descriptor)
                    .await
                    .is_err()
            );
            assert_eq!(h.gate.preparation_calls.load(Ordering::SeqCst), calls);
            descriptor
        };
        h.finish_cleanup_state(&descriptor, point != 0).await;
        assert_eq!(h.provider.calls.load(Ordering::SeqCst), 2);
        assert_eq!(h.fake.requests().len(), usize::from(point == 0));
        assert!(
            matches!(h.runtime.start_keyed(h.input.clone(), options()).await.unwrap(),
            crate::Admission::Replayed(saved) if saved == receipt)
        );
        assert_eq!(h.provider.calls.load(Ordering::SeqCst), 2);
        if point == 2 {
            cold_controller_recovery(store.clone(), clock.clone(), &h, &receipt).await;
        }
    }
}

async fn cold_controller_recovery(
    store: Arc<dyn Store>,
    clock: Arc<ManualClock>,
    previous: &Harness,
    original: &crabber_core::AdmissionReceipt,
) {
    clock.set(clock.now() + time::Duration::seconds(31));
    let gate = Arc::new(SettlementGate::default());
    gate.release.notify_one();
    let mut cold = Harness::on_store_with_clock(store.clone(), store, gate, Some(clock));
    cold.input = previous.input.clone();
    let crate::Admission::Started { handle, receipt } = cold
        .runtime
        .recover_admission(cold.input.clone(), options())
        .await
        .unwrap()
    else {
        panic!("explicit expired-Unstarted recovery after controller restart")
    };
    assert_eq!(&receipt, original);
    handle.done().await.unwrap();
    let descriptor = cold
        .runtime
        .lookup_preparation_operation(
            &receipt.session_id,
            &PreparationKey::AdmissionRecovery {
                key: options().key,
                issuance: 2,
            },
        )
        .await
        .unwrap()
        .unwrap();
    cold.finish_cleanup(&descriptor).await;
    assert_eq!(cold.provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(cold.fake.requests().len(), 1);
    assert_eq!(previous.provider.calls.load(Ordering::SeqCst), 2);
    assert_eq!(previous.fake.requests().len(), 0);
}

async fn changed_callbacks_before_factory(
    h: &Harness,
    clock: Arc<ManualClock>,
    original: &crabber_core::AdmissionReceipt,
) {
    let changed = Orchestrator::builder()
        .behavior_fingerprint(h.runtime.behavior_fingerprint().clone())
        .store(h.memory.clone())
        .resolver(Arc::new(h.fake.clone()))
        .plan_provider(h.provider.clone())
        .clock(clock)
        .observer(Arc::new(crate::NoopObserver))
        .build()
        .unwrap();
    // Metadata remains independently readable; changed configuration cannot
    // enter the keyed Runtime or claim a fresh recovery grant.
    assert_eq!(
        h.memory
            .lookup_admission(&original.session_id, &options().key)
            .await
            .unwrap()
            .as_ref(),
        Some(original)
    );
    assert!(matches!(
        changed.start_keyed(h.input.clone(), options()).await,
        Err(RuntimeError::Store(
            crabber_session::StoreError::AdmissionConflict
        ))
    ));
    assert!(matches!(
        changed.recover_admission(h.input.clone(), options()).await,
        Err(RuntimeError::Store(
            crabber_session::StoreError::AdmissionConflict
        ))
    ));
    assert_eq!(h.provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(h.fake.requests().len(), 0);
    assert_eq!(
        changed.retained_preparation_status(&original.session_id),
        Vec::<crate::RetainedPreparationProgress>::new()
    );
}

async fn unsupported_before_factory(h: &Harness, initial: &PreparationReconciliation) {
    h.gate
        .unsupported_recovery_inspection
        .store(true, Ordering::SeqCst);
    let error = h
        .runtime
        .recover_admission(h.input.clone(), options())
        .await
        .err()
        .unwrap();
    assert!(format!("{error:?}").contains("Unsupported"));
    assert_eq!(h.provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(h.fake.requests().len(), 0);
    assert!(
        h.runtime
            .lookup_preparation_operation(
                &initial.session_id,
                &PreparationKey::AdmissionRecovery {
                    key: options().key,
                    issuance: 1
                }
            )
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn memory_scoped_recovery_and_native_ack_reconciliation() {
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    contract(Arc::new(MemoryStore::with_clock(clock.clone())), clock).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_scoped_recovery_and_native_ack_reconciliation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("scoped-recovery.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = crabber_session::SqliteStore::connect(path)
        .await
        .unwrap()
        .with_clock(clock.clone());
    contract(Arc::new(store), clock).await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_scoped_recovery_and_native_ack_reconciliation() {
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert_ne!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref(),
            Ok("1")
        );
        return;
    };
    crabber_session::PostgresStore::migrate(&url).await.unwrap();
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = crabber_session::PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock.clone());
    contract(Arc::new(store), clock).await;
}
