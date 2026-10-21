//! Actual original preparation heartbeat joins survive retry and caller drop.
use super::*;

#[derive(Default)]
pub(in crate::tests) struct Gate {
    pub(super) mode: AtomicUsize,
    pub(super) calls: AtomicUsize,
    pub(super) entered: Notify,
    pub(super) release: Notify,
    pending_hold: std::sync::atomic::AtomicBool,
    pending_entered: Notify,
    pending_release: Notify,
    payload_drops: Arc<AtomicUsize>,
}
impl Gate {
    pub(in crate::tests) async fn renew(&self) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        match self.mode.load(Ordering::SeqCst) {
            1 => panic!("original preparation Store renewal panicked"),
            2 => self.release.notified().await,
            4 => std::panic::panic_any(PanickingPayload(self.payload_drops.clone())),
            _ => (),
        }
    }
}
struct PanickingPayload(Arc<AtomicUsize>);
impl Drop for PanickingPayload {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("original heartbeat panic payload panicked on Drop");
    }
}
impl Gate {
    pub(in crate::tests) async fn pending(&self) {
        if self.pending_hold.swap(false, Ordering::SeqCst) {
            self.pending_entered.notify_one();
            self.pending_release.notified().await;
        }
    }
}

async fn settled_supervisor(h: &Harness, descriptor: &PreparationReconciliation) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let statuses = h
                .runtime
                .retained_preparation_status(&descriptor.session_id);
            if statuses.is_empty()
                || statuses[0].status == crate::RetainedPreparationStatus::CleanupUncertain
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
async fn panic_contract(store: Arc<dyn Store>) {
    for mode in [1, 4] {
        panic_once(store.clone(), mode).await;
    }
}
async fn panic_once(store: Arc<dyn Store>, mode: usize) {
    let heartbeat = Arc::new(Gate::default());
    heartbeat.mode.store(mode, Ordering::SeqCst);
    let gate = Arc::new(SettlementGate {
        preparation_heartbeat: Some(heartbeat.clone()),
        ..Default::default()
    });
    gate.release.notify_one();
    let wrapped = Arc::new(DelayedTerminalStore {
        inner: store.clone(),
        gate: gate.clone(),
    });
    let h = Harness::on_store(wrapped, store, gate);
    // Keep genuine preparation alive until the injected original renewal has
    // actually entered. A fast same-task worker may otherwise finish before
    // the spawned heartbeat ever needs to renew.
    h.gate.preparation_hold.store(true, Ordering::SeqCst);
    let runtime = h.runtime.clone();
    let input = h.input.clone();
    let start = tokio::spawn(async move { runtime.start_keyed(input, options()).await });
    let completed = tokio::time::timeout(
        Duration::from_secs(2),
        h.gate.preparation_entered.notified(),
    )
    .await;
    let renewed = tokio::time::timeout(Duration::from_secs(2), heartbeat.entered.notified()).await;
    h.gate.preparation_hold.store(false, Ordering::SeqCst);
    h.gate.preparation_release.notify_one();
    let crate::Admission::Started { handle, .. } = start.await.unwrap().unwrap() else {
        panic!("genuine owned native preparation");
    };
    let descriptor = h.descriptor().await;
    handle.done().await.unwrap();
    h.provider.cleanup_started.notified().await;
    h.provider.cleanup_release.notify_one();
    settled_supervisor(&h, &descriptor).await;
    assert_eq!(h.child_state(&descriptor).await, SessionOwnerState::Active);
    for _ in 0..2 {
        h.runtime
            .cleanup_retained_preparation(&descriptor)
            .await
            .unwrap();
        settled_supervisor(&h, &descriptor).await;
        assert_eq!(
            h.runtime
                .retained_preparation_status(&descriptor.session_id)
                .len(),
            1,
            "retry forgot the original failed heartbeat join"
        );
        assert_eq!(h.child_state(&descriptor).await, SessionOwnerState::Active);
    }
    assert_eq!(h.gate.owned_join_calls.load(Ordering::SeqCst), 0);
    assert_eq!(h.provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(h.provider.cleanup_completed.load(Ordering::SeqCst), 1);
    assert!(completed.is_ok(), "genuine Complete must reach held ACK");
    assert!(
        renewed.is_ok(),
        "injected original heartbeat renewal must really occur"
    );
    assert_eq!(heartbeat.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        heartbeat.payload_drops.load(Ordering::SeqCst),
        usize::from(mode == 4)
    );
}

#[tokio::test]
async fn memory_preparation_heartbeat_failure_survives_cleanup_retry() {
    panic_contract(Arc::new(MemoryStore::new())).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_preparation_heartbeat_failure_survives_cleanup_retry() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("heartbeat.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    let store = crabber_session::SqliteStore::connect(&path).await.unwrap();
    panic_contract(Arc::new(store)).await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_preparation_heartbeat_failure_survives_cleanup_retry() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crabber_session::PostgresStore::migrate(&url).await.unwrap();
        let store = crabber_session::PostgresStore::connect(&url).await.unwrap();
        panic_contract(Arc::new(store)).await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn dropped_legacy_preparation_retry_retains_original_heartbeat() {
    let inner = Arc::new(MemoryStore::new());
    let heartbeat = Arc::new(Gate::default());
    heartbeat.mode.store(2, Ordering::SeqCst);
    let gate = Arc::new(SettlementGate {
        preparation_heartbeat: Some(heartbeat.clone()),
        ..Default::default()
    });
    gate.preparation_fault.store(2, Ordering::SeqCst);
    let wrapped = Arc::new(DelayedTerminalStore { inner, gate });
    let fake = FakeProvider::scripted(vec![text_script("unused")]);
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(InputFingerprint::new("b".repeat(64)).unwrap())
        .store(wrapped)
        .resolver(Arc::new(fake.clone()))
        .plan_provider(Arc::new(StaticPlanProvider::new(vec![], vec![])))
        .build()
        .unwrap();
    let mut input = request();
    input.session_id = Some(SessionId::new());
    let error = runtime.start_keyed(input, options()).await.err().unwrap();
    let descriptor = error.preparation_reconciliation().unwrap().clone();
    heartbeat.entered.notified().await;
    let stop = runtime
        .retained_preparation_heartbeat_stop(&descriptor)
        .unwrap();
    let mut original = Box::pin(runtime.retry_preparation_completion(&descriptor));
    tokio::select! {
        () = stop.cancelled() => (),
        result = &mut original => panic!("retry skipped original heartbeat: {result:?}"),
    }
    drop(original);
    let mut retry = Box::pin(runtime.retry_preparation_completion(&descriptor));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), retry.as_mut())
            .await
            .is_err(),
        "dropped waiter detached the original heartbeat"
    );
    assert_eq!(
        runtime
            .retained_preparation_status(&descriptor.session_id)
            .len(),
        1
    );
    heartbeat.release.notify_one();
    assert!(
        tokio::time::timeout(Duration::from_secs(3), retry)
            .await
            .unwrap()
            .unwrap()
            .replayed
    );
    assert_eq!(
        runtime.retained_preparation_status(&descriptor.session_id),
        Vec::new()
    );
    assert_eq!(heartbeat.calls.load(Ordering::SeqCst), 1);
    assert_eq!(fake.requests().len(), 0);
}

#[tokio::test]
async fn stopping_preparation_heartbeat_does_not_start_next_authority_io() {
    let store = Arc::new(MemoryStore::new());
    let heartbeat = Arc::new(Gate::default());
    heartbeat.mode.store(2, Ordering::SeqCst);
    let gate = Arc::new(SettlementGate {
        preparation_heartbeat: Some(heartbeat.clone()),
        ..Default::default()
    });
    let wrapped = Arc::new(DelayedTerminalStore {
        inner: store.clone(),
        gate: gate.clone(),
    });
    let h = Harness::on_store(wrapped, store, gate);
    let crate::Admission::Started { handle, .. } = h
        .runtime
        .start_keyed(h.input.clone(), options())
        .await
        .unwrap()
    else {
        panic!("owned preparation")
    };
    heartbeat.entered.notified().await;
    let descriptor = h.descriptor().await;
    let stop = h
        .runtime
        .retained_preparation_heartbeat_stop(&descriptor)
        .unwrap();
    h.gate.release.notify_one();
    handle.done().await.unwrap();
    h.provider.cleanup_started.notified().await;
    h.provider.cleanup_release.notify_one();
    tokio::time::timeout(Duration::from_secs(3), stop.cancelled())
        .await
        .unwrap();
    // Stopping cannot discard the already-started Store future or publish Join.
    assert_eq!(h.gate.owned_join_calls.load(Ordering::SeqCst), 0);
    heartbeat.release.notify_one();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !h
            .runtime
            .retained_preparation_status(&descriptor.session_id)
            .is_empty()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(h.child_state(&descriptor).await, SessionOwnerState::Joined);
    assert_eq!(
        heartbeat.calls.load(Ordering::SeqCst),
        1,
        "stop must prevent new child renewal after original root call finishes"
    );
}

#[tokio::test(start_paused = true)]
async fn stopping_preparation_after_pending_lookup_does_not_start_cancel_observation() {
    use crabber_session::session_owner::{RequestSessionOwnerCancel, SessionOwnerResolver};
    let store = Arc::new(MemoryStore::new());
    let heartbeat = Arc::new(Gate::default());
    let gate = Arc::new(SettlementGate {
        preparation_heartbeat: Some(heartbeat.clone()),
        ..Default::default()
    });
    let wrapped = Arc::new(DelayedTerminalStore {
        inner: store.clone(),
        gate: gate.clone(),
    });
    let h = Harness::on_store(wrapped, store, gate);
    h.provider.wait_for_close.store(true, Ordering::SeqCst);
    let runtime = h.runtime.clone();
    let input = h.input.clone();
    let start = tokio::spawn(async move { runtime.start_keyed(input, options()).await });
    h.provider.entered.notified().await;
    let descriptor = h.descriptor().await;
    heartbeat.pending_hold.store(true, Ordering::SeqCst);
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::time::timeout(
        Duration::from_secs(10),
        heartbeat.pending_entered.notified(),
    )
    .await
    .expect("original pending lookup entered");
    let original_renewals = heartbeat.calls.load(Ordering::SeqCst);
    let stop = h
        .runtime
        .retained_preparation_heartbeat_stop(&descriptor)
        .unwrap();
    h.memory
        .request_session_owner_cancel(RequestSessionOwnerCancel {
            id: crabber_core::SessionOwnerOperationId::new(),
            session_id: descriptor.session_id.clone(),
            target: descriptor.owner.clone(),
            expected_revision: None,
            claim: SessionOwnerResolver {
                resolver_id: "test-controller".into(),
                authority_claim: "cancel original preparation".into(),
            },
        })
        .await
        .unwrap();
    h.runtime
        .cleanup_retained_preparation(&descriptor)
        .await
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(3),
        h.provider.cleanup_started.notified(),
    )
    .await
    .expect("actual scoped cleanup started");
    h.provider.cleanup_release.notify_one();
    tokio::time::timeout(Duration::from_secs(3), stop.cancelled())
        .await
        .unwrap();
    assert_eq!(h.gate.owned_join_calls.load(Ordering::SeqCst), 0);
    heartbeat.pending_release.notify_one();
    let error = start.await.unwrap().err().unwrap();
    assert_eq!(error.preparation_reconciliation(), Some(&descriptor));
    tokio::time::timeout(Duration::from_secs(3), async {
        while !h
            .runtime
            .retained_preparation_status(&descriptor.session_id)
            .is_empty()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        h.gate.owner_observe_keys.lock().unwrap().is_empty(),
        "stop must not start a new ObserveCancel after original pending lookup returns"
    );
    assert_eq!(heartbeat.calls.load(Ordering::SeqCst), original_renewals);
    assert_eq!(h.fake.requests().len(), 0);
}
