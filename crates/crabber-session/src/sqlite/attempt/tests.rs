use crate::attempt_contract::process;

#[tokio::test]
async fn attempt_fault_contract() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    let store = store.with_clock(clock.clone());
    crate::attempt_contract::run_fault_contract(
        &store,
        &clock,
        |point: crate::attempt_contract::FaultPoint| {
            store
                .attempt_fault
                .store(point as u8, std::sync::atomic::Ordering::SeqCst);
        },
    )
    .await;
}

#[tokio::test]
async fn sqlite_attempt_contract() {
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    crate::attempt_contract::run_contract(|clock| store.with_clock(clock)).await;
}

#[tokio::test]
async fn process_helper() {
    let Some(fixture) = process::fixture() else {
        return;
    };
    let path = std::env::var("CRABBER_SQLITE_CHILD_PATH").unwrap();
    let store = if let Ok(marker) = std::env::var("CRABBER_ATTEMPT_BARRIER") {
        crate::sqlite::test_support::connect_with_test_options(
            std::path::Path::new(&path),
            crate::sqlite::test_support::TestOptions {
                barrier: Some(marker.into()),
                ..Default::default()
            },
        )
        .await
    } else {
        super::super::SqliteStore::connect(&path).await.unwrap()
    };
    if std::env::var_os("CRABBER_ATTEMPT_WRITE").is_some() {
        process::write(&store, &fixture).await.unwrap();
    } else {
        process::verify_loss(&store, fixture).await;
    }
}
#[tokio::test]
async fn lost_reserve_and_begin_ack_reconcile_in_fresh_process() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let (store, path, _directory) = crate::sqlite::tests::temp_store().await;
    let store = store.with_clock(clock);
    for mode in [process::Mode::Reserve, process::Mode::Begin] {
        let fixture = process::prepare_loss(&store, mode, |point| {
            store
                .attempt_fault
                .store(point, std::sync::atomic::Ordering::SeqCst);
        })
        .await;
        process::spawn(
            "sqlite::attempt::tests::process_helper",
            &fixture,
            Some(&path),
            &[],
        )
        .wait()
        .await;
    }
}

#[tokio::test]
async fn killed_reserve_and_begin_before_commit_leave_no_partial_transition() {
    let _guard = crate::sqlite::test_support::BARRIER_LOCK.lock().await;
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::now_utc(),
    ));
    let (store, path, directory) = crate::sqlite::tests::temp_store().await;
    let store = store.with_clock(clock.clone());
    for (index, mode) in [process::Mode::Reserve, process::Mode::Begin]
        .into_iter()
        .enumerate()
    {
        let fixture = process::pending(&store, mode).await;
        let state = if matches!(mode, process::Mode::Reserve) {
            "Unstarted"
        } else {
            "Started"
        };
        let marker = directory.path().join(format!("attempt-{index}.marker"));
        sqlx::query(&format!("CREATE TRIGGER attempt_barrier AFTER INSERT ON operation_receipts WHEN NEW.kind='attempt' AND json_extract(NEW.data_json,'$.attempt.state')='{state}' BEGIN SELECT 1 WHERE 'a' < 'b' COLLATE crabber_test_barrier; END"))
            .execute(&store.writer).await.unwrap();
        let mut child = process::spawn(
            "sqlite::attempt::tests::process_helper",
            &fixture,
            Some(&path),
            &[
                ("CRABBER_ATTEMPT_WRITE", "1".into()),
                ("CRABBER_ATTEMPT_BARRIER", marker.to_str().unwrap().into()),
            ],
        );
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while !marker.exists() {
                assert!(child.running(), "writer exited before barrier");
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("child must reach receipt insert before COMMIT");
        process::uncommitted(&store, &fixture).await;
        child.kill();
        sqlx::query("DROP TRIGGER attempt_barrier")
            .execute(&store.writer)
            .await
            .unwrap();
        process::uncommitted(&store, &fixture).await;
        process::retry_crash(&store, &clock, &fixture).await;
    }
}
