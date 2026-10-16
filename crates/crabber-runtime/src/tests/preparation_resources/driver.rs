//! Actual driver lifetime and uncertain native acknowledgements.
#[path = "driver/ack.rs"]
mod ack;
#[path = "driver/faults.rs"]
mod faults;
use super::*;
use crabber_session::session_owner::driver::RegisterExecutionDriver;
async fn driver_state(h: &Harness, run: &RunId) -> Option<SessionOwnerState> {
    let persisted = h.memory.get_run(run).await.unwrap().unwrap();
    let request = RegisterExecutionDriver {
        context: SessionContext::new(
            persisted.session_id.clone(),
            h.input.workspace_id.clone(),
            h.input.directory.clone(),
        ),
        fence: crabber_core::RunFence {
            run_id: persisted.id,
            claim_token: persisted.claim_token,
        },
    };
    h.memory
        .lookup_session_owner(
            &persisted.session_id,
            &request.identities().unwrap().reserve,
        )
        .await
        .unwrap()
        .map(|receipt| receipt.owner.state)
}
pub(super) async fn current_driver_state(h: &Harness, run: &RunId) -> SessionOwnerState {
    let persisted = h.memory.get_run(run).await.unwrap().unwrap();
    let request = RegisterExecutionDriver {
        context: SessionContext::new(
            persisted.session_id.clone(),
            h.input.workspace_id.clone(),
            h.input.directory.clone(),
        ),
        fence: crabber_core::RunFence {
            run_id: persisted.id,
            claim_token: persisted.claim_token,
        },
    };
    h.memory
        .get_session_owner(&persisted.session_id, &request.identities().unwrap().owner)
        .await
        .unwrap()
        .state
}
async fn start_initial(h: &Harness, unkeyed: bool) -> crate::RunHandle {
    if unkeyed {
        let mut input = h.input.clone();
        input.session_id = None;
        return h.runtime.start(input).await.unwrap();
    }
    let crate::Admission::Started { handle, .. } = h
        .runtime
        .start_keyed(h.input.clone(), options())
        .await
        .unwrap()
    else {
        panic!("fresh keyed admission");
    };
    handle
}
#[tokio::test]
async fn unknown_driver_registration_never_dispatches_the_model() {
    for unkeyed in [false, true] {
        for point in [1, 2] {
            let h = Harness::new();
            h.gate.driver_register_fault.store(point, Ordering::SeqCst);
            let handle = start_initial(&h, unkeyed).await;
            let run = handle.run_id().clone();
            assert!(handle.done().await.is_err());
            assert_eq!(h.fake.requests().len(), 0);
            assert_eq!(driver_state(&h, &run).await.is_some(), point == 2);
            assert_eq!(h.provider.calls.load(Ordering::SeqCst), 1);
            h.provider.cleanup_release.notify_one();
        }
    }
}
#[tokio::test]
async fn unknown_driver_activation_joins_actual_no_dispatch_without_model_effects() {
    for unkeyed in [false, true] {
        for point in [1, 2] {
            let h = Harness::new();
            h.gate
                .driver_activation_fault
                .store(point, Ordering::SeqCst);
            let handle = start_initial(&h, unkeyed).await;
            let run = handle.run_id().clone();
            assert!(handle.done().await.is_err());
            assert_eq!(h.fake.requests().len(), 0);
            assert_eq!(
                current_driver_state(&h, &run).await,
                SessionOwnerState::Joined
            );
            h.provider.cleanup_release.notify_one();
        }
    }
}
#[tokio::test]
async fn unknown_driver_join_retries_only_original_observation() {
    for unkeyed in [false, true] {
        for point in [1, 2] {
            let h = Harness::new();
            h.gate.driver_join_fault.store(point, Ordering::SeqCst);
            let handle = start_initial(&h, unkeyed).await;
            let run = handle.run_id().clone();
            handle.done().await.unwrap();
            assert_eq!(h.fake.requests().len(), 1);
            assert_eq!(
                current_driver_state(&h, &run).await,
                SessionOwnerState::Joined
            );
            assert_eq!(h.gate.driver_join_calls.load(Ordering::SeqCst), 2);
            h.provider.cleanup_release.notify_one();
        }
    }
}

#[derive(Default)]
struct RunObservationBarrier {
    entered: Notify,
    released: Mutex<bool>,
    changed: std::sync::Condvar,
    panic_after_release: std::sync::atomic::AtomicBool,
}
impl Observer for RunObservationBarrier {
    fn emit(&self, _: &EventRecord) {}
    fn operational_completed(&self, observation: &crate::OperationalObservation) {
        if observation.kind == crate::OperationKind::Run {
            self.entered.notify_one();
            let mut released = self.released.lock().unwrap();
            while !*released {
                released = self.changed.wait(released).unwrap();
            }
            drop(released);
            assert!(
                !self.panic_after_release.load(Ordering::SeqCst),
                "actual outer worker observer panic"
            );
        }
    }
}
struct ReleaseObservation(Arc<RunObservationBarrier>);
impl Drop for ReleaseObservation {
    fn drop(&mut self) {
        *self.0.released.lock().unwrap() = true;
        self.0.changed.notify_all();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_terminal_does_not_join_driver_while_outer_observer_is_running() {
    let mut h = Harness::new();
    let observer = Arc::new(RunObservationBarrier::default());
    let release = ReleaseObservation(observer.clone());
    h.runtime = Orchestrator::builder()
        .behavior_fingerprint(options().behavior_fingerprint)
        .store(Arc::new(DelayedTerminalStore {
            inner: h.memory.clone(),
            gate: h.gate.clone(),
        }))
        .resolver(Arc::new(h.fake.clone()))
        .plan_provider(h.provider.clone())
        .observer(observer.clone())
        .build()
        .unwrap();
    let crate::Admission::Started { handle, .. } = h
        .runtime
        .start_keyed(h.input.clone(), options())
        .await
        .unwrap()
    else {
        panic!("fresh")
    };
    let run = handle.run_id().clone();
    tokio::time::timeout(Duration::from_secs(2), observer.entered.notified())
        .await
        .unwrap();
    assert_eq!(
        h.memory.get_run(&run).await.unwrap().unwrap().status,
        RunStatus::Completed
    );
    assert_eq!(
        current_driver_state(&h, &run).await,
        SessionOwnerState::Active
    );
    assert!(!*handle.completion_signal().borrow());
    let drain = h.runtime.drain_execution_tasks(Duration::ZERO).await;
    assert_eq!(drain.pending, vec![run.clone()]);
    drop(release);
    tokio::time::timeout(Duration::from_secs(2), handle.done())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        current_driver_state(&h, &run).await,
        SessionOwnerState::Joined
    );
    h.provider.cleanup_release.notify_one();
}

#[tokio::test]
async fn dropped_handle_retains_original_driver_ack_future_and_dispatches_once() {
    let h = Harness::new();
    h.gate.driver_register_hold.store(true, Ordering::SeqCst);
    let crate::Admission::Started { handle, .. } = h
        .runtime
        .start_keyed(h.input.clone(), options())
        .await
        .unwrap()
    else {
        panic!("fresh")
    };
    let run = handle.run_id().clone();
    tokio::time::timeout(
        Duration::from_secs(2),
        h.gate.driver_register_entered.notified(),
    )
    .await
    .unwrap();
    drop(handle);
    assert_eq!(
        current_driver_state(&h, &run).await,
        SessionOwnerState::Active
    );
    assert_eq!(h.fake.requests().len(), 0);
    assert_eq!(
        h.runtime
            .drain_execution_tasks(Duration::ZERO)
            .await
            .pending,
        vec![run.clone()]
    );
    h.gate.driver_register_release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let drain = h
                .runtime
                .drain_execution_tasks(Duration::from_millis(50))
                .await;
            if drain.pending.is_empty() {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(h.fake.requests().len(), 1);
    assert_eq!(
        current_driver_state(&h, &run).await,
        SessionOwnerState::Joined
    );
    h.provider.cleanup_release.notify_one();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_outer_worker_panic_survives_supervisor_join_reporting() {
    let mut h = Harness::new();
    let observer = Arc::new(RunObservationBarrier::default());
    observer.panic_after_release.store(true, Ordering::SeqCst);
    let release = ReleaseObservation(observer.clone());
    h.runtime = Orchestrator::builder()
        .behavior_fingerprint(options().behavior_fingerprint)
        .store(Arc::new(DelayedTerminalStore {
            inner: h.memory.clone(),
            gate: h.gate.clone(),
        }))
        .resolver(Arc::new(h.fake.clone()))
        .plan_provider(h.provider.clone())
        .observer(observer.clone())
        .build()
        .unwrap();
    let crate::Admission::Started { handle, .. } = h
        .runtime
        .start_keyed(h.input.clone(), options())
        .await
        .unwrap()
    else {
        panic!("fresh")
    };
    let run = handle.run_id().clone();
    tokio::time::timeout(Duration::from_secs(2), observer.entered.notified())
        .await
        .unwrap();
    drop(handle);
    drop(release);
    let drained = h
        .runtime
        .drain_execution_tasks(Duration::from_secs(2))
        .await;
    assert_eq!(drained.pending, [] as [RunId; 0]);
    assert_eq!(drained.joined.len(), 1);
    assert_eq!(
        drained.joined[0].termination,
        crate::TaskTermination::Panicked
    );
    assert_eq!(
        current_driver_state(&h, &run).await,
        SessionOwnerState::Joined
    );
    assert_eq!(
        h.memory.get_run(&run).await.unwrap().unwrap().status,
        RunStatus::Completed
    );
    h.provider.cleanup_release.notify_one();
}
