//! Independent Runtime successor histories on durable backends.
use super::*;
macro_rules! cases {
    ($sqlite:ident, $postgres:ident, $contract:path) => {
        #[cfg(feature = "sqlite")]
        #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
        async fn $sqlite() {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("successor-lifecycle.sqlite");
            crabber_session::SqliteStore::migrate(&path).await.unwrap();
            let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
            let store = crabber_session::SqliteStore::connect(path)
                .await
                .unwrap()
                .with_clock(clock.clone());
            Box::pin($contract(Arc::new(store), clock)).await;
        }
        #[cfg(feature = "postgres")]
        #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
        async fn $postgres() {
            use crabber_test_support::postgres::{DatabaseMode, TestDatabase};
            TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
                crabber_session::PostgresStore::migrate(&url).await.unwrap();
                let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
                let store = crabber_session::PostgresStore::connect(&url)
                    .await
                    .unwrap()
                    .with_clock(clock.clone());
                Box::pin($contract(Arc::new(store), clock)).await;
            })
            .await
            .unwrap();
        }
    };
}
cases!(
    sqlite_runtime_successor_core,
    postgres_runtime_successor_core,
    super::contract
);
cases!(
    sqlite_runtime_successor_retention,
    postgres_runtime_successor_retention,
    retention::contract
);
cases!(
    sqlite_runtime_successor_recovery,
    postgres_runtime_successor_recovery,
    recovery::contract
);
cases!(
    sqlite_runtime_successor_pending,
    postgres_runtime_successor_pending,
    pending::contract
);
