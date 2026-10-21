use std::sync::Arc;
#[tokio::test]
async fn child() {
    if std::env::var_os("CRABBER_SUCCESSOR_CHILD").is_none() {
        return;
    }
    let path = std::env::var("CRABBER_SUCCESSOR_DATABASE").unwrap();
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::SqliteStore::connect(path)
        .await
        .unwrap()
        .with_clock(clock.clone());
    crate::successor_driver_contract::process::child(&store, &clock).await;
}
#[tokio::test]
async fn physical_successor_commit_boundaries() {
    for admission in [true, false] {
        for stage in ["before", "after"] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("successor.db");
            crate::SqliteStore::migrate(&path).await.unwrap();
            let (request, now) = crate::successor_driver_contract::process::kill(
                "sqlite::session_owner::drivers::tests::successor_process::child",
                stage,
                crabber_test_support::ChildDatabase::Sqlite(&path),
                directory.path(),
                admission,
            );
            let clock = Arc::new(crabber_core::ManualClock::new(now));
            let store = crate::SqliteStore::connect(path)
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
    for recovery in [false, true] {
        for stage in ["before", "after"] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("successor.db");
            crate::SqliteStore::migrate(&path).await.unwrap();
            let (request, now) = crate::successor_driver_contract::process::kill_approval(
                "sqlite::session_owner::drivers::tests::successor_process::child",
                stage,
                crabber_test_support::ChildDatabase::Sqlite(&path),
                directory.path(),
                recovery,
            );
            let clock = Arc::new(crabber_core::ManualClock::new(now));
            let store = crate::SqliteStore::connect(path)
                .await
                .unwrap()
                .with_clock(clock);
            crate::successor_driver_contract::process::verify(&store, request, stage == "after")
                .await;
        }
    }
}
