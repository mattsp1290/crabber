//! Repeated store panics during close retain the exact native proof and joins.
use super::*;
async fn contract(store: Arc<dyn Store>) {
    for point in [1, 2] {
        let (h, gate) = faults::harness(store.clone());
        faults::started(&h).await;
        let owned = ids(&*store, &h.runtime, &h.a, &h.provider).await;
        gate.service_join_panic.store(point, Ordering::SeqCst);
        assert!(
            h.provider
                .inner
                .close_session(&faults::context(&h), Duration::from_secs(3))
                .await
                .is_err()
        );
        assert!(
            gate.service_join_calls.load(Ordering::SeqCst) >= 4,
            "both original supervisors and caller recovery must reach the store panic"
        );
        assert_eq!(h.seen.resource_finished.load(Ordering::SeqCst), 2);
        states(
            &*store,
            h.a.session_id.as_ref().unwrap(),
            &owned,
            if point == 1 {
                SessionOwnerState::Active
            } else {
                SessionOwnerState::Joined
            },
        )
        .await;
        gate.service_join_panic.store(0, Ordering::SeqCst);
        h.provider
            .inner
            .close_session(&faults::context(&h), Duration::from_secs(3))
            .await
            .unwrap();
        states(
            &*store,
            h.a.session_id.as_ref().unwrap(),
            &owned,
            SessionOwnerState::Joined,
        )
        .await;
        assert_eq!(h.seen.created.load(Ordering::SeqCst), 1);
        assert_eq!(h.fake.requests().len(), 1);
        {
            let keys = gate.service_join_keys.lock().unwrap();
            assert_eq!(
                keys.iter().collect::<std::collections::BTreeSet<_>>().len(),
                2
            );
        }
        h.provider
            .inner
            .close_all(Duration::from_secs(2))
            .await
            .unwrap();
    }
}
#[tokio::test]
async fn memory_repeated_join_panics_keep_service_recovery_incomplete() {
    contract(Arc::new(MemoryStore::new())).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_repeated_join_panics_keep_service_recovery_incomplete() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("service-ack.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    contract(Arc::new(
        crabber_session::SqliteStore::connect(&path).await.unwrap(),
    ))
    .await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_repeated_join_panics_keep_service_recovery_incomplete() {
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert_ne!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref(),
            Ok("1"),
            "required PostgreSQL URL missing"
        );
        return;
    };
    crabber_session::PostgresStore::migrate(&url).await.unwrap();
    contract(Arc::new(
        crabber_session::PostgresStore::connect(&url).await.unwrap(),
    ))
    .await;
}
