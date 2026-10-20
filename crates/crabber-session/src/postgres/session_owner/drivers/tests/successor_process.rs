use std::sync::Arc;
#[tokio::test]
async fn child() {
    if std::env::var_os("CRABBER_SUCCESSOR_CHILD").is_none() {
        return;
    }
    let url = crate::postgres::tests::test_url().unwrap();
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
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    for admission in [true, false] {
        for stage in ["before", "after"] {
            let directory = tempfile::tempdir().unwrap();
            let (request, now) = crate::successor_driver_contract::process::kill(
                "postgres::session_owner::drivers::tests::successor_process::child",
                stage,
                None,
                directory.path(),
                admission,
            );
            let clock = Arc::new(crabber_core::ManualClock::new(now));
            let store = crate::PostgresStore::connect(&url)
                .await
                .unwrap()
                .with_clock(clock);
            crate::successor_driver_contract::process::verify(&store, request, stage == "after")
                .await;
        }
    }
}

#[tokio::test]
async fn physical_approval_successor_commit_boundaries() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    for recovery in [false, true] {
        for stage in ["before", "after"] {
            let directory = tempfile::tempdir().unwrap();
            let (request, now) = crate::successor_driver_contract::process::kill_approval(
                "postgres::session_owner::drivers::tests::successor_process::child",
                stage,
                None,
                directory.path(),
                recovery,
            );
            let clock = Arc::new(crabber_core::ManualClock::new(now));
            let store = crate::PostgresStore::connect(&url)
                .await
                .unwrap()
                .with_clock(clock);
            crate::successor_driver_contract::process::verify(&store, request, stage == "after")
                .await;
        }
    }
}
