//! A malformed acknowledgement cannot stand in for the requested native Join.
use super::*;
async fn contract(store: Arc<dyn Store>) {
    for binding in [false, true] {
        let (h, gate) = faults::harness(store.clone());
        faults::started(&h).await;
        let owned = ids(&*store, &h.runtime, &h.a, &h.provider).await;
        let malformed = if binding {
            &gate.service_join_bad_binding
        } else {
            &gate.service_join_bad_termination
        };
        malformed.store(true, Ordering::SeqCst);
        let provider = h.provider.clone();
        let context = faults::context(&h);
        let first = tokio::spawn(async move {
            provider
                .inner
                .close_session(&context, Duration::from_millis(100))
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let mut joined = 0;
                for id in &owned {
                    if store
                        .get_session_owner(h.a.session_id.as_ref().unwrap(), id)
                        .await
                        .unwrap()
                        .state
                        == SessionOwnerState::Joined
                    {
                        joined += 1;
                    }
                }
                if joined == owned.len() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let _ = first.await.unwrap();
        // Both genuine protected native commits exist. Only their malformed
        // DTOs are wrong, so a successful local close here would hide the gap.
        assert!(
            h.provider
                .inner
                .close_session(&faults::context(&h), Duration::from_millis(200))
                .await
                .is_err(),
            "malformed Join ACK must not release the original control"
        );
        malformed.store(false, Ordering::SeqCst);
        h.provider
            .inner
            .close_session(&faults::context(&h), Duration::from_secs(3))
            .await
            .unwrap();
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
async fn memory_malformed_service_join_ack_keeps_original_control() {
    contract(Arc::new(MemoryStore::new())).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_malformed_service_join_ack_keeps_original_control() {
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
async fn postgres_malformed_service_join_ack_keeps_original_control() {
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
