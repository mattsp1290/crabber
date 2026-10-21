use std::sync::Arc;
#[tokio::test]
async fn child() {
    if std::env::var_os("CRABBER_SUCCESSOR_CHILD").is_none() {
        return;
    }
    let url = crabber_test_support::postgres::TestDatabase::attached_url()
        .await
        .unwrap();
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock.clone());
    crate::successor_driver_contract::process::child(&store, &clock).await;
}
#[tokio::test]
async fn physical_successor_commit_boundaries() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        for admission in [true, false] {
            for stage in ["before", "after"] {
                let directory = tempfile::tempdir().unwrap();
                let (request, now) = crate::successor_driver_contract::process::kill(
                    "postgres::session_owner::drivers::tests::successor_process::child",
                    stage,
                    crabber_test_support::ChildDatabase::Postgres(&url),
                    directory.path(),
                    admission,
                );
                let clock = Arc::new(crabber_core::ManualClock::new(now));
                let store = crate::PostgresStore::connect(&url)
                    .await
                    .unwrap()
                    .with_clock(clock);
                crate::successor_driver_contract::process::verify(
                    &store,
                    request,
                    stage == "after",
                )
                .await;
            }
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn physical_approval_successor_commit_boundaries() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        for recovery in [false, true] {
            for stage in ["before", "after"] {
                let directory = tempfile::tempdir().unwrap();
                let (request, now) = crate::successor_driver_contract::process::kill_approval(
                    "postgres::session_owner::drivers::tests::successor_process::child",
                    stage,
                    crabber_test_support::ChildDatabase::Postgres(&url),
                    directory.path(),
                    recovery,
                );
                let clock = Arc::new(crabber_core::ManualClock::new(now));
                let store = crate::PostgresStore::connect(&url)
                    .await
                    .unwrap()
                    .with_clock(clock);
                crate::successor_driver_contract::process::verify(
                    &store,
                    request,
                    stage == "after",
                )
                .await;
            }
        }
    })
    .await
    .unwrap();
}
