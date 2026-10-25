//! Actual failed cleanup remains retained and publishes one exact uncertainty.
use super::*;

pub(super) async fn contract(store: Arc<dyn Store>) {
    for (failure, fault) in [(1, 1), (1, 2), (2, 0)] {
        let gate = Arc::new(SettlementGate::default());
        gate.release.notify_one();
        let wrapped = Arc::new(DelayedTerminalStore {
            inner: store.clone(),
            gate: gate.clone(),
        });
        let h = Harness::on_store(wrapped, store.clone(), gate);
        h.provider.cleanup_failure.store(failure, Ordering::SeqCst);
        h.gate
            .cleanup_uncertain_fault
            .store(fault, Ordering::SeqCst);
        let crate::Admission::Started { handle, receipt } = h
            .runtime
            .start_keyed(h.input.clone(), options())
            .await
            .unwrap()
        else {
            panic!("fresh owned admission")
        };
        let descriptor = h.descriptor().await;
        handle.done().await.unwrap();
        h.provider.cleanup_started.notified().await;
        h.provider.cleanup_release.notify_one();
        wait_local_failure(&h, &descriptor, failure, fault, "initial cleanup").await;
        assert_eq!(
            h.child_state(&descriptor).await,
            if fault == 1 {
                SessionOwnerState::Active
            } else {
                SessionOwnerState::Uncertain
            }
        );
        let original = h.gate.cleanup_uncertain_keys.lock().unwrap().clone();
        assert_eq!(original.len(), 1);
        h.runtime
            .cleanup_retained_preparation(&descriptor)
            .await
            .unwrap();
        wait_local_failure(&h, &descriptor, failure, fault, "uncertainty retry").await;
        assert_eq!(
            h.child_state(&descriptor).await,
            SessionOwnerState::Uncertain
        );
        let keys = h.gate.cleanup_uncertain_keys.lock().unwrap().clone();
        assert_eq!(keys.len(), if fault == 1 { 2 } else { 1 });
        assert!(keys.iter().all(|id| id == &original[0]));
        let marked = h
            .memory
            .lookup_session_owner(&descriptor.session_id, &original[0])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            marked.operation,
            crabber_session::session_owner::SessionOwnerOperation::MarkUncertain
        );
        let revision = h
            .memory
            .session_revision(&descriptor.session_id)
            .await
            .unwrap();
        h.runtime
            .cleanup_retained_preparation(&descriptor)
            .await
            .unwrap();
        wait_local_failure(&h, &descriptor, failure, fault, "receipt replay").await;
        assert_eq!(
            h.memory
                .session_revision(&descriptor.session_id)
                .await
                .unwrap(),
            revision
        );
        assert_eq!(
            h.memory
                .lookup_session_owner(&descriptor.session_id, &original[0])
                .await
                .unwrap(),
            Some(marked)
        );
        assert_eq!(h.gate.owned_join_calls.load(Ordering::SeqCst), 0);
        assert_eq!(h.provider.cleanup_completed.load(Ordering::SeqCst), 1);
        assert!(
            matches!(h.runtime.start_keyed(h.input.clone(), options()).await.unwrap(),
            crate::Admission::Replayed(saved) if saved == receipt)
        );
        assert_eq!(h.provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(h.fake.requests().len(), 1);
        assert_eq!(
            h.runtime
                .retained_preparation_status(&descriptor.session_id)
                .len(),
            1
        );
    }
}

async fn wait_local_failure(
    h: &Harness,
    descriptor: &PreparationReconciliation,
    failure: usize,
    fault: usize,
    phase: &str,
) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if h.runtime
                .retained_preparation_status(&descriptor.session_id)
                .first()
                .is_some_and(|status| {
                    status.status == crate::RetainedPreparationStatus::CleanupUncertain
                })
            {
                break;
            }
            // Yielding can immediately reschedule this poll instead of driving
            // the SQL cleanup's I/O. Keep the same deadline while giving the
            // current-thread runtime a timer boundary between observations.
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap_or_else(|error| {
        panic!(
            "{phase} did not retain uncertainty: failure={failure} fault={fault}: {error}; statuses={:?}",
            h.runtime.retained_preparation_status(&descriptor.session_id)
        )
    });
}

#[tokio::test]
async fn memory_actual_cleanup_failure_and_uncertainty_ack_replay() {
    contract(Arc::new(MemoryStore::new())).await;
}

#[tokio::test(start_paused = true)]
async fn cancelled_factory_with_actual_failed_cleanup_stays_uncertain() {
    use crabber_session::session_owner::{RequestSessionOwnerCancel, SessionOwnerResolver};
    let h = Harness::new();
    h.provider.wait_for_close.store(true, Ordering::SeqCst);
    h.provider.cleanup_failure.store(1, Ordering::SeqCst);
    let runtime = h.runtime.clone();
    let input = h.input.clone();
    let worker = tokio::spawn(async move { runtime.start_keyed(input, options()).await });
    h.provider.entered.notified().await;
    let descriptor = h.descriptor().await;
    h.memory
        .request_session_owner_cancel(RequestSessionOwnerCancel {
            id: crabber_core::SessionOwnerOperationId::new(),
            session_id: descriptor.session_id.clone(),
            target: descriptor.owner.clone(),
            expected_revision: None,
            claim: SessionOwnerResolver {
                resolver_id: "host".into(),
                authority_claim: "cancel own factory".into(),
            },
        })
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(5)).await;
    assert_eq!(
        worker
            .await
            .unwrap()
            .err()
            .unwrap()
            .preparation_reconciliation(),
        Some(&descriptor)
    );
    h.runtime
        .cleanup_retained_preparation(&descriptor)
        .await
        .unwrap();
    h.provider.cleanup_started.notified().await;
    h.provider.cleanup_release.notify_one();
    wait_local_failure(&h, &descriptor, 1, 0, "cancelled factory cleanup").await;
    assert_eq!(
        h.child_state(&descriptor).await,
        SessionOwnerState::Uncertain
    );
    assert_eq!(h.provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(h.provider.cleanup_completed.load(Ordering::SeqCst), 1);
    assert_eq!(h.gate.owned_join_calls.load(Ordering::SeqCst), 0);
    assert_eq!(h.fake.requests().len(), 0);
    assert_eq!(
        h.memory
            .lookup_admission(&descriptor.session_id, &options().key)
            .await
            .unwrap(),
        None
    );
}
