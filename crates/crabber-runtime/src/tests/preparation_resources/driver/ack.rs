//! A committed native Join with a malformed ACK retains its original proof.
use super::*;

const ALL_BAD_ACKS: &[usize] = &[1, 2, 3, 4, 5, 6, 7, 8, 9];
// These mutate only the returned receipt after native Join has completed.
// Memory owns every negative predicate; SQL retains termination and owner
// identity rejection, then genuine Replay through the full predicate.
#[cfg(any(feature = "sqlite", feature = "postgres"))]
const SQL_BAD_ACKS: &[usize] = &[1, 8];

async fn contract(store: Arc<dyn Store>, malformed_acks: &[usize]) {
    // These waits bound test progress, not the malformed-ACK contract below.
    // Parallel PostgreSQL suites can spend seconds queueing fenced operations.
    let progress_bound = Duration::from_secs(15);
    for unkeyed in [false, true] {
        for &bad in malformed_acks {
            let gate = Arc::new(SettlementGate::default());
            gate.release.notify_one();
            gate.driver_join_bad.store(bad, Ordering::SeqCst);
            let wrapped = Arc::new(DelayedTerminalStore {
                inner: store.clone(),
                gate: gate.clone(),
            });
            let h = Harness::on_store(wrapped, store.clone(), gate);
            let handle = start_initial(&h, unkeyed).await;
            let run = handle.run_id().clone();
            tokio::time::timeout(progress_bound, async {
                while driver_state(&h, &run).await.is_none()
                    || current_driver_state(&h, &run).await != SessionOwnerState::Joined
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            let mut done = Box::pin(handle.done());
            assert!(
                tokio::time::timeout(Duration::from_millis(150), &mut done)
                    .await
                    .is_err(),
                "malformed ACK discarded the original initial Driver proof"
            );
            assert_eq!(
                h.runtime
                    .drain_execution_tasks(Duration::ZERO)
                    .await
                    .pending,
                vec![run]
            );
            let keys = h.gate.driver_join_keys.lock().unwrap().clone();
            assert!(keys.len() >= 2);
            assert!(keys.iter().all(|key| key == &keys[0]));
            h.gate.driver_join_bad.store(0, Ordering::SeqCst);
            tokio::time::timeout(progress_bound, done)
                .await
                .unwrap()
                .unwrap();
            let final_keys = h.gate.driver_join_keys.lock().unwrap().clone();
            assert!(final_keys.iter().all(|key| key == &keys[0]));
            assert_eq!(h.fake.requests().len(), 1);
            assert_eq!(h.provider.calls.load(Ordering::SeqCst), 1);
            h.provider.cleanup_release.notify_one();
        }
    }
}
#[tokio::test]
async fn memory_initial_driver_malformed_join_ack_retains_original_proof() {
    contract(Arc::new(MemoryStore::new()), ALL_BAD_ACKS).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_initial_driver_malformed_join_ack_retains_original_proof() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("initial-driver-ack.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    let store = crabber_session::SqliteStore::connect(&path).await.unwrap();
    contract(Arc::new(store), SQL_BAD_ACKS).await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_initial_driver_malformed_join_ack_retains_original_proof() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crabber_session::PostgresStore::migrate(&url).await.unwrap();
        let store = crabber_session::PostgresStore::connect(&url).await.unwrap();
        contract(Arc::new(store), SQL_BAD_ACKS).await;
    })
    .await
    .unwrap();
}
