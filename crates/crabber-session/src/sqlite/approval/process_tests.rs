use crate::approval_contract::process;

#[tokio::test]
async fn process_helper() {
    let Some(fixture) = process::child::fixture() else {
        return;
    };
    let path = std::env::var("CRABBER_SQLITE_CHILD_PATH").unwrap();
    let store = if let Ok(marker) = std::env::var("CRABBER_APPROVAL_BARRIER") {
        crate::sqlite::test_support::connect_with_test_options(
            std::path::Path::new(&path),
            crate::sqlite::test_support::TestOptions {
                barrier: Some(marker.into()),
                ..Default::default()
            },
        )
        .await
    } else {
        super::SqliteStore::connect(&path).await.unwrap()
    }
    .with_clock(process::fixture_clock(&fixture));
    if std::env::var_os("CRABBER_APPROVAL_WRITE").is_some() {
        process::write(&store, &fixture, |_| {}, false)
            .await
            .unwrap();
    } else {
        process::verify::verify_loss(&store, &fixture).await;
    }
}
#[tokio::test]
async fn lost_acknowledgements_reconcile_in_fresh_process() {
    let (store, path, _directory) = crate::sqlite::tests::temp_store().await;
    let clock = process::clock();
    let store = store.with_clock(clock.clone());
    for mode in process::Mode::ALL {
        let fixture = process::pending(&store, mode, &clock).await;
        let result = process::write(
            &store,
            &fixture,
            |point| {
                store
                    .approval_fault
                    .store(point, std::sync::atomic::Ordering::SeqCst);
            },
            true,
        )
        .await;
        assert_eq!(
            result.unwrap_err(),
            crate::ApprovalError::UnknownStoreOutcome
        );
        process::child::spawn(
            "sqlite::approval::process_tests::process_helper",
            &fixture,
            crabber_test_support::ChildDatabase::Sqlite(&path),
            &[],
        )
        .wait()
        .await;
    }
}
#[tokio::test]
async fn killed_writer_rolls_back_suspend_answer_and_claim() {
    let _guard = crate::sqlite::test_support::BARRIER_LOCK.lock().await;
    let (store, path, directory) = crate::sqlite::tests::temp_store().await;
    let clock = process::clock();
    let store = store.with_clock(clock.clone());
    for (index, mode) in process::Mode::ALL.into_iter().enumerate() {
        let fixture = process::pending(&store, mode, &clock).await;
        let marker = directory.path().join(format!("approval-{index}.marker"));

        // The temporary database is dedicated to this fixture.
        sqlx::query("CREATE TRIGGER approval_barrier AFTER INSERT ON operation_receipts WHEN NEW.kind='approval' BEGIN SELECT 1 WHERE 'a' < 'b' COLLATE crabber_test_barrier; END")
            .execute(&store.writer).await.unwrap();
        let mut child = process::child::spawn(
            "sqlite::approval::process_tests::process_helper",
            &fixture,
            crabber_test_support::ChildDatabase::Sqlite(&path),
            &[
                ("CRABBER_APPROVAL_WRITE", "1".into()),
                ("CRABBER_APPROVAL_BARRIER", marker.to_str().unwrap().into()),
            ],
        );
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while !marker.exists() {
                assert!(child.running(), "writer exited before receipt barrier");
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("child must reach receipt insert before COMMIT");
        process::verify::uncommitted(&store, &fixture).await;
        child.kill();
        sqlx::query("DROP TRIGGER approval_barrier")
            .execute(&store.writer)
            .await
            .unwrap();
        process::verify::uncommitted(&store, &fixture).await;
    }
}
