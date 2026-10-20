//! Native terminal metadata and control errors never replace actual joins.
use super::*;
#[derive(Default)]
struct MainBarrier {
    entered: Notify,
    released: Mutex<bool>,
    changed: std::sync::Condvar,
}
impl Observer for MainBarrier {
    fn emit(&self, _: &EventRecord) {}
    fn operational_completed(&self, observation: &crate::OperationalObservation) {
        if observation.kind == crate::OperationKind::Run {
            self.entered.notify_one();
            let mut released = self.released.lock().unwrap();
            while !*released {
                released = self.changed.wait(released).unwrap();
            }
        }
    }
}
struct Release(Arc<MainBarrier>);
impl Drop for Release {
    fn drop(&mut self) {
        *self.0.released.lock().unwrap() = true;
        self.0.changed.notify_all();
    }
}
async fn blocked(store: Arc<dyn Store>, clock: Arc<ManualClock>, expired: bool, cancel: bool) {
    let gate = Arc::new(SettlementGate::default());
    gate.release.notify_one();
    let wrapped = Arc::new(DelayedTerminalStore {
        inner: store.clone(),
        gate: gate.clone(),
    });
    let mut h = Harness::on_store_with_clock(wrapped.clone(), store, gate, Some(clock.clone()));
    let observer = Arc::new(MainBarrier::default());
    let release_main = Release(observer.clone());
    h.runtime = Orchestrator::builder()
        .behavior_fingerprint(options().behavior_fingerprint)
        .store(wrapped)
        .resolver(Arc::new(h.fake.clone()))
        .plan_provider(h.provider.clone())
        .observer(observer.clone())
        .clock(clock.clone())
        .build()
        .unwrap();
    let receipt = initial(&h, &clock, expired).await;
    h.gate.driver_poll_panic.store(!cancel, Ordering::SeqCst);
    let handle = start(&h, &receipt, expired).await;
    let run = handle.run_id().clone();
    tokio::time::timeout(Duration::from_secs(3), observer.entered.notified())
        .await
        .unwrap();
    let resources = handle
        .worker_resources_for_test()
        .tool_resources(crabber_core::ToolCallId::new(), None);
    let release = Arc::new(Notify::new());
    let child_release = release.clone();
    let returned = Arc::new(AtomicUsize::new(0));
    let child_completed = returned.clone();
    resources
        .resources()
        .spawn(async move {
            child_release.notified().await;
            child_completed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    if cancel {
        request_cancel(&h, &receipt).await;
        tokio::time::timeout(Duration::from_secs(7), async {
            while !resources.snapshot().sealed {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    } else {
        tokio::time::timeout(
            Duration::from_secs(7),
            h.gate.driver_poll_entered.notified(),
        )
        .await
        .unwrap();
    }
    assert_eq!(
        h.runtime
            .drain_execution_tasks(Duration::from_millis(20))
            .await
            .pending,
        vec![run.clone()]
    );
    assert_eq!(state(&h).await, SessionOwnerState::Active);
    assert_eq!(returned.load(Ordering::SeqCst), 0);
    drop(release_main);
    assert_eq!(
        h.runtime
            .drain_execution_tasks(Duration::from_millis(20))
            .await
            .pending,
        vec![run]
    );
    assert_eq!(state(&h).await, SessionOwnerState::Active);
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(3), handle.done())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state(&h).await, SessionOwnerState::Joined);
    assert_eq!(returned.load(Ordering::SeqCst), 1);
    cleanup(&h, expired).await;
    assert_eq!(h.fake.requests().len(), 1);
}
async fn wait_native_join(h: &Harness) {
    tokio::time::timeout(Duration::from_secs(7), async {
        loop {
            let request = captured(h);
            let ids = request.identities().unwrap();
            if h.memory
                .lookup_session_owner(request.context.session_id(), &ids.reserve)
                .await
                .unwrap()
                .is_some()
                && state(h).await == SessionOwnerState::Joined
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
async fn request_cancel(h: &Harness, receipt: &crabber_core::AdmissionReceipt) {
    let request = captured(h);
    let owner = h
        .memory
        .get_session_owner(&receipt.session_id, &request.identities().unwrap().owner)
        .await
        .unwrap();
    h.memory
        .request_session_owner_cancel(crabber_session::session_owner::RequestSessionOwnerCancel {
            id: crabber_core::SessionOwnerOperationId::new(),
            session_id: receipt.session_id.clone(),
            target: owner.target,
            expected_revision: Some(
                h.memory
                    .session_revision(&receipt.session_id)
                    .await
                    .unwrap(),
            ),
            claim: crabber_session::session_owner::SessionOwnerResolver {
                resolver_id: "host".into(),
                authority_claim: "cancel this actual successor scope".into(),
            },
        })
        .await
        .unwrap();
}
async fn malformed(store: Arc<dyn Store>, clock: Arc<ManualClock>, expired: bool) {
    let gate = Arc::new(SettlementGate::default());
    gate.release.notify_one();
    let wrapped = Arc::new(DelayedTerminalStore {
        inner: store.clone(),
        gate: gate.clone(),
    });
    let h = Harness::on_store_with_clock(wrapped, store, gate, Some(clock.clone()));
    let receipt = initial(&h, &clock, expired).await;
    h.gate.successor_join_bad.store(1, Ordering::SeqCst);
    let handle = start(&h, &receipt, expired).await;
    let run = handle.run_id().clone();
    let mut done = Box::pin(handle.done());
    assert!(
        tokio::time::timeout(Duration::from_millis(150), &mut done)
            .await
            .is_err()
    );
    assert_eq!(
        h.runtime
            .drain_execution_tasks(Duration::from_millis(20))
            .await
            .pending,
        vec![run]
    );
    wait_native_join(&h).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut done)
            .await
            .is_err()
    );
    let keys = h.gate.successor_join_keys.lock().unwrap().clone();
    assert!(keys.len() >= 2);
    assert!(keys.iter().all(|key| key == &keys[0]));
    h.gate.successor_join_bad.store(0, Ordering::SeqCst);
    tokio::time::timeout(Duration::from_secs(3), done)
        .await
        .unwrap()
        .unwrap();
    cleanup(&h, expired).await;
    assert_eq!(h.fake.requests().len(), 1);
    assert_eq!(h.provider.calls.load(Ordering::SeqCst), 2);
}
pub(super) async fn contract(store: Arc<dyn Store>, clock: Arc<ManualClock>) {
    for expired in [false, true] {
        blocked(store.clone(), clock.clone(), expired, false).await;
        blocked(store.clone(), clock.clone(), expired, true).await;
        malformed(store.clone(), clock.clone(), expired).await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn memory_runtime_successor_actual_main_and_cleanup_retention() {
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    contract(Arc::new(MemoryStore::with_clock(clock.clone())), clock).await;
}
