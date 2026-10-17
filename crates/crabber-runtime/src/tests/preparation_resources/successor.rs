//! Actual preparation completion feeds only the corresponding successor gate.
use super::*;
#[path = "successor/callbacks.rs"]
mod callbacks;
#[path = "successor/pending.rs"]
mod pending;
#[cfg(feature = "sqlite")]
#[path = "successor/process.rs"]
mod process;
#[path = "successor/recovery.rs"]
mod recovery;
#[path = "successor/retention.rs"]
mod retention;
#[cfg(any(feature = "sqlite", feature = "postgres"))]
#[path = "successor/sql.rs"]
mod sql;
use crabber_core::{Clock, ManualClock};
use crabber_session::session_owner::driver::RegisterSuccessorExecutionDriver;

async fn initial(
    h: &Harness,
    clock: &ManualClock,
    expired: bool,
) -> crabber_core::AdmissionReceipt {
    h.gate.preparation_fault.store(2, Ordering::SeqCst);
    let error = h
        .runtime
        .start_keyed(h.input.clone(), options())
        .await
        .err()
        .unwrap();
    let descriptor = error.preparation_reconciliation().unwrap().clone();
    let calls = h.gate.preparation_calls.load(Ordering::SeqCst);
    assert!(
        h.runtime
            .retry_preparation_completion(&descriptor)
            .await
            .is_err()
    );
    assert_eq!(h.gate.preparation_calls.load(Ordering::SeqCst), calls);
    h.finish_cleanup_state(&descriptor, true).await;
    let receipt = h
        .memory
        .lookup_admission(&descriptor.session_id, &options().key)
        .await
        .unwrap()
        .unwrap();
    if expired {
        let run = h.memory.get_run(&receipt.run_id).await.unwrap().unwrap();
        h.memory
            .execution(crabber_core::RunFence {
                run_id: run.id,
                claim_token: run.claim_token,
            })
            .await
            .unwrap()
            .begin_admission_execution()
            .await
            .unwrap();
    }
    clock.set(clock.now() + time::Duration::seconds(31));
    receipt
}
async fn start(
    h: &Harness,
    receipt: &crabber_core::AdmissionReceipt,
    expired: bool,
) -> crate::RunHandle {
    if expired {
        return h.runtime.resume_execution(&receipt.run_id).await.unwrap();
    }
    let crate::Admission::Started {
        handle,
        receipt: saved,
    } = h
        .runtime
        .recover_admission(h.input.clone(), options())
        .await
        .unwrap()
    else {
        panic!("fresh recovery")
    };
    assert_eq!(&saved, receipt);
    handle
}
fn captured(h: &Harness) -> RegisterSuccessorExecutionDriver {
    h.gate.successor_request.lock().unwrap().clone().unwrap()
}
async fn state(h: &Harness) -> SessionOwnerState {
    let request = captured(h);
    h.memory
        .get_session_owner(
            request.context.session_id(),
            &request.identities().unwrap().owner,
        )
        .await
        .unwrap()
        .state
}
async fn cleanup(h: &Harness, expired: bool) {
    cleanup_state(h, expired, false).await;
}
async fn cleanup_state(h: &Harness, expired: bool, uncertain: bool) {
    let request = captured(h);
    let key = if expired {
        PreparationKey::ExpiredRun {
            run_id: request.fence.run_id.clone(),
            issuance: request.claim.issuance - 1,
        }
    } else {
        PreparationKey::AdmissionRecovery {
            key: options().key,
            issuance: request.claim.issuance - 1,
        }
    };
    let descriptor = h
        .runtime
        .lookup_preparation_operation(request.context.session_id(), &key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(descriptor.join_id, request.claim.root_join_id);
    h.finish_cleanup_state(&descriptor, uncertain).await;
}
async fn contract(store: Arc<dyn Store>, clock: Arc<ManualClock>) {
    for expired in [false, true] {
        for (stage, point) in [
            (0, 0),
            (1, 1),
            (1, 2),
            (1, 3),
            (1, 4),
            (1, 5),
            (2, 1),
            (2, 2),
            (2, 3),
            (2, 4),
            (3, 1),
            (3, 2),
            (3, 3),
            (3, 4),
        ] {
            let gate = Arc::new(SettlementGate::default());
            gate.release.notify_one();
            let wrapped = Arc::new(DelayedTerminalStore {
                inner: store.clone(),
                gate: gate.clone(),
            });
            let h = Harness::on_store_with_clock(wrapped, store.clone(), gate, Some(clock.clone()));
            let receipt = initial(&h, &clock, expired).await;
            match stage {
                1 => h.gate.driver_register_fault.store(point, Ordering::SeqCst),
                2 => h
                    .gate
                    .driver_activation_fault
                    .store(point, Ordering::SeqCst),
                3 => h.gate.driver_join_fault.store(point, Ordering::SeqCst),
                _ => (),
            }
            let handle = start(&h, &receipt, expired).await;
            let outcome = tokio::time::timeout(Duration::from_secs(5), handle.done())
                .await
                .unwrap();
            let dispatched = stage == 0 || stage == 3;
            assert_eq!(
                outcome.is_ok(),
                dispatched,
                "expired={expired} stage={stage} point={point} outcome={outcome:?}"
            );
            assert_eq!(h.fake.requests().len(), usize::from(dispatched));
            assert_eq!(h.provider.calls.load(Ordering::SeqCst), 2);
            if stage != 1 {
                assert_eq!(state(&h).await, SessionOwnerState::Joined);
            }
            let request = captured(&h);
            assert_eq!(request.claim.issuance, 2);
            if stage == 3 {
                assert!(h.gate.driver_join_calls.load(Ordering::SeqCst) >= 2);
            }
            cleanup_state(&h, expired, stage == 1).await;
            let effects = h.fake.requests().len();
            let _ = h.memory.register_successor_execution_driver(request).await;
            assert_eq!(h.fake.requests().len(), effects);
        }
    }
}
#[tokio::test]
async fn memory_runtime_successor_native_faults() {
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    contract(Arc::new(MemoryStore::with_clock(clock.clone())), clock).await;
}
