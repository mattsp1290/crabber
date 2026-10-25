use crate::approval_contract::process;

#[tokio::test]
async fn process_helper() {
    let Some(fixture) = process::child::fixture() else {
        return;
    };
    let store = super::PostgresStore::connect(
        &crabber_test_support::postgres::TestDatabase::attached_url()
            .await
            .expect("child database required"),
    )
    .await
    .unwrap()
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
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        super::PostgresStore::migrate(&url).await.unwrap();
        let clock = process::clock();
        let store = super::PostgresStore::connect(&url)
            .await
            .unwrap()
            .with_clock(clock.clone());
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
                "postgres::approval::process_tests::process_helper",
                &fixture,
                crabber_test_support::ChildDatabase::Postgres(&url),
                &[],
            )
            .wait()
            .await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn killed_writer_rolls_back_suspend_answer_and_claim() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        super::PostgresStore::migrate(&url).await.unwrap();
        let clock = process::clock();
        let store = super::PostgresStore::connect(&url)
            .await
            .unwrap()
            .with_clock(clock.clone());
        for mode in process::Mode::ALL {
            crash(&url, &store, mode, &clock).await;
        }
    })
    .await
    .unwrap();
}
async fn crash(
    url: &str,
    store: &super::PostgresStore,
    mode: process::Mode,
    clock: &crabber_core::ManualClock,
) {
    let fixture = process::pending(store, mode, clock).await;
    let name = format!("approval_barrier_{}", uuid::Uuid::new_v4().simple());
    let key = &fixture.key().0;
    // The session, operation key and SQL identifier are generated test UUIDs.
    sqlx::query(&format!("CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.kind='approval' AND NEW.session_id='{}' AND NEW.operation_id='{key}' THEN PERFORM pg_advisory_xact_lock(751302933); END IF; RETURN NEW; END $$",fixture.session.0)).execute(&store.pool).await.unwrap();
    sqlx::query(&format!("CREATE TRIGGER {name} AFTER INSERT ON operation_receipts FOR EACH ROW EXECUTE FUNCTION {name}()")).execute(&store.pool).await.unwrap();
    let mut held = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(751302933)")
        .execute(&mut *held)
        .await
        .unwrap();
    let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *held)
        .await
        .unwrap();
    let mut child = process::child::spawn(
        "postgres::approval::process_tests::process_helper",
        &fixture,
        crabber_test_support::ChildDatabase::Postgres(url),
        &[("CRABBER_APPROVAL_WRITE", "1".into())],
    );
    let waiter = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            assert!(child.running(), "writer exited before receipt barrier");
            let pid: Option<i32> = sqlx::query_scalar("SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid)) AND query LIKE 'INSERT INTO operation_receipts%'").bind(blocker).fetch_optional(&store.pool).await.unwrap();
            if let Some(pid) = pid { break pid; }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }).await.expect("child must reach receipt insert before COMMIT");
    process::verify::uncommitted(store, &fixture).await;
    child.kill();
    sqlx::query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid=$1 AND datname=current_database()")
        .bind(waiter)
        .execute(&store.pool)
        .await
        .unwrap();
    held.rollback().await.unwrap();
    sqlx::query(&format!("DROP TRIGGER {name} ON operation_receipts"))
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query(&format!("DROP FUNCTION {name}()"))
        .execute(&store.pool)
        .await
        .unwrap();
    process::verify::uncommitted(store, &fixture).await;
}
