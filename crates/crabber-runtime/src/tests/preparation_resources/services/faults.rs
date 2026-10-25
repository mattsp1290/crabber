use super::*;

pub(super) fn harness(store: Arc<dyn Store>) -> (ServiceHarness, Arc<SettlementGate>) {
    let gate = Arc::new(SettlementGate::default());
    gate.release.notify_one();
    let controlled: Arc<dyn Store> = Arc::new(DelayedTerminalStore {
        inner: store,
        gate: gate.clone(),
    });
    (ServiceHarness::new(&controlled), gate)
}
pub(super) async fn started(h: &ServiceHarness) {
    let crate::Admission::Started { handle, .. } =
        h.runtime.start_keyed(h.a.clone(), options()).await.unwrap()
    else {
        panic!("fresh service run")
    };
    handle.done().await.unwrap();
}
pub(super) fn context(h: &ServiceHarness) -> SessionContext {
    SessionContext::new(
        h.a.session_id.clone().unwrap(),
        h.a.workspace_id.clone(),
        h.a.directory.clone(),
    )
}
async fn registration_unknown(store: Arc<dyn Store>) {
    for point in [1, 2] {
        let (h, gate) = harness(store.clone());
        gate.service_register_fault.store(point, Ordering::SeqCst);
        assert!(h.runtime.start_keyed(h.a.clone(), options()).await.is_err());
        assert_eq!(h.seen.created.load(Ordering::SeqCst), 0);
        assert_eq!(
            h.fake.requests(),
            [] as [crabber_providers::ModelRequest; 0]
        );
        let captured = h.provider.contexts.lock().unwrap()[0].clone();
        // The original actual preparation main has already returned. Its old
        // private token still exists, but that cannot reopen local ingress.
        assert!(
            h.provider
                .inner
                .acquire_plan_in_preparation(&captured)
                .await
                .is_err()
        );
        assert_eq!(gate.service_register_calls.load(Ordering::SeqCst), 1);
        assert_eq!(h.seen.created.load(Ordering::SeqCst), 0);
        assert!(
            h.provider
                .inner
                .close_all(Duration::from_millis(30))
                .await
                .is_err()
        );
        assert_eq!(h.seen.resource_finished.load(Ordering::SeqCst), 0);
        if point == 2 {
            let owned = ids(&*store, &h.runtime, &h.a, &h.provider).await;
            states(
                &*store,
                h.a.session_id.as_ref().unwrap(),
                &owned,
                SessionOwnerState::Active,
            )
            .await;
        }
    }
}
async fn actual_resources_and_join_ack(store: Arc<dyn Store>) {
    for point in [1, 2] {
        let (h, gate) = harness(store.clone());
        h.seen.resource_wait.store(true, Ordering::SeqCst);
        started(&h).await;
        let owned = ids(&*store, &h.runtime, &h.a, &h.provider).await;
        gate.service_join_fault.store(point, Ordering::SeqCst);
        assert!(
            h.provider
                .inner
                .close_session(&context(&h), Duration::from_millis(30))
                .await
                .is_err()
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            while h.seen.resource_started.load(Ordering::SeqCst) != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(h.seen.resource_finished.load(Ordering::SeqCst), 0);
        assert_eq!(gate.service_join_calls.load(Ordering::SeqCst), 0);
        states(
            &*store,
            h.a.session_id.as_ref().unwrap(),
            &owned,
            SessionOwnerState::Active,
        )
        .await;
        h.seen.resource_release.add_permits(2);
        h.provider
            .inner
            .close_session(&context(&h), Duration::from_secs(3))
            .await
            .unwrap();
        states(
            &*store,
            h.a.session_id.as_ref().unwrap(),
            &owned,
            SessionOwnerState::Joined,
        )
        .await;
        assert_eq!(h.seen.resource_finished.load(Ordering::SeqCst), 2);
        assert_eq!(gate.service_join_calls.load(Ordering::SeqCst), 3);
        let keys = gate.service_join_keys.lock().unwrap().clone();
        assert_eq!(keys.len(), 3);
        assert!(
            keys.iter()
                .any(|key| keys.iter().filter(|other| *other == key).count() == 2)
        );
        h.provider
            .inner
            .close_all(Duration::from_secs(2))
            .await
            .unwrap();
    }
}
async fn parent_seal_during_activation(store: Arc<dyn Store>) {
    let (h, gate) = harness(store.clone());
    gate.service_activation_hold.store(true, Ordering::SeqCst);
    let runtime = h.runtime.clone();
    let input = h.a.clone();
    let waiter = tokio::spawn(async move { runtime.start_keyed(input, options()).await });
    tokio::time::timeout(
        Duration::from_secs(2),
        gate.service_activation_entered.notified(),
    )
    .await
    .unwrap();
    h.provider.abort_acquisition.notify_one();
    assert!(waiter.await.unwrap().is_err());
    assert_eq!(h.seen.created.load(Ordering::SeqCst), 0);
    gate.service_activation_hold.store(false, Ordering::SeqCst);
    gate.service_activation_release.notify_one();
    h.provider
        .inner
        .close_all(Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(h.seen.created.load(Ordering::SeqCst), 0);
    assert_eq!(h.seen.resource_started.load(Ordering::SeqCst), 0);
    assert_eq!(
        h.fake.requests(),
        [] as [crabber_providers::ModelRequest; 0]
    );
    let owned = ids(&*store, &h.runtime, &h.a, &h.provider).await;
    states(
        &*store,
        h.a.session_id.as_ref().unwrap(),
        &owned,
        SessionOwnerState::Joined,
    )
    .await;
}
async fn caller_drop_retains_native_registration(store: Arc<dyn Store>) {
    let (h, gate) = harness(store.clone());
    gate.service_register_hold.store(true, Ordering::SeqCst);
    let runtime = h.runtime.clone();
    let input = h.a.clone();
    let waiter = tokio::spawn(async move { runtime.start_keyed(input, options()).await });
    tokio::time::timeout(
        Duration::from_secs(2),
        gate.service_register_entered.notified(),
    )
    .await
    .unwrap();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert!(
        h.provider
            .inner
            .close_all(Duration::from_millis(30))
            .await
            .is_err()
    );
    assert_eq!(h.seen.created.load(Ordering::SeqCst), 0);
    gate.service_register_hold.store(false, Ordering::SeqCst);
    gate.service_register_release.notify_one();
    h.provider
        .inner
        .close_all(Duration::from_secs(3))
        .await
        .unwrap();
    assert_eq!(h.seen.created.load(Ordering::SeqCst), 0);
    assert_eq!(
        h.fake.requests(),
        [] as [crabber_providers::ModelRequest; 0]
    );
    let owned = ids(&*store, &h.runtime, &h.a, &h.provider).await;
    states(
        &*store,
        h.a.session_id.as_ref().unwrap(),
        &owned,
        SessionOwnerState::Joined,
    )
    .await;
}
async fn failed_actual_cleanup_remains_uncertain(store: Arc<dyn Store>) {
    for point in [1, 2] {
        let (h, _) = harness(store.clone());
        h.seen.resource_failure.store(point, Ordering::SeqCst);
        started(&h).await;
        let owned = ids(&*store, &h.runtime, &h.a, &h.provider).await;
        assert!(
            h.provider
                .inner
                .close_session(&context(&h), Duration::from_secs(2))
                .await
                .is_err()
        );
        states(
            &*store,
            h.a.session_id.as_ref().unwrap(),
            &owned,
            SessionOwnerState::Uncertain,
        )
        .await;
        assert!(
            h.provider
                .inner
                .close_all(Duration::from_secs(2))
                .await
                .is_err()
        );
        assert_eq!(h.seen.resource_finished.load(Ordering::SeqCst), 2);
    }
}
async fn contract(store: Arc<dyn Store>) {
    tokio::time::timeout(Duration::from_secs(15), registration_unknown(store.clone()))
        .await
        .expect("uncertain registration must return finite control");
    tokio::time::timeout(
        Duration::from_secs(15),
        actual_resources_and_join_ack(store.clone()),
    )
    .await
    .expect("actual resource and native Join ACK retry");
    tokio::time::timeout(
        Duration::from_secs(15),
        parent_seal_during_activation(store.clone()),
    )
    .await
    .expect("parent seal must prevent late activation");
    tokio::time::timeout(
        Duration::from_secs(15),
        caller_drop_retains_native_registration(store.clone()),
    )
    .await
    .expect("caller drop must retain original registration");
    tokio::time::timeout(
        Duration::from_secs(15),
        failed_actual_cleanup_remains_uncertain(store),
    )
    .await
    .expect("failed actual service cleanup remains uncertain");
}
#[tokio::test]
async fn memory_service_native_unknown_acks_and_actual_join_contract() {
    contract(Arc::new(MemoryStore::new())).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_service_native_unknown_acks_and_actual_join_contract() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("services-faults.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    contract(Arc::new(
        crabber_session::SqliteStore::connect(&path).await.unwrap(),
    ))
    .await;
}
#[cfg(feature = "postgres")]
#[test]
fn postgres_service_native_unknown_acks_and_actual_join_contract() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    let history_runtime = tokio::runtime::Builder::new_current_thread();
    TestDatabase::run_with_runtime(DatabaseMode::Current, history_runtime, async move |url| {
        crabber_session::PostgresStore::migrate(&url).await.unwrap();
        contract(Arc::new(
            crabber_session::PostgresStore::connect(&url).await.unwrap(),
        ))
        .await;
    })
    .unwrap();
}
