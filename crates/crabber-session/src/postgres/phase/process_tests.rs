use crate::phase_contract::process;

#[tokio::test]
async fn process_helper() {
    let Some(fixture) = process::child::fixture() else {
        return;
    };
    let store = super::PostgresStore::connect(
        &crate::postgres::tests::test_url().expect("child database required"),
    )
    .await
    .unwrap();
    if std::env::var_os("CRABBER_PHASE_WRITE").is_some() {
        process::write(&store, &fixture, |_| {}, false)
            .await
            .unwrap();
    } else {
        process::verify::verify_loss(&store, fixture).await;
    }
}
#[tokio::test]
async fn lost_acknowledgements_reconcile_in_fresh_process() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    super::PostgresStore::migrate(&url).await.unwrap();
    let store = super::PostgresStore::connect(&url).await.unwrap();
    for mode in process::Mode::ALL {
        let fixture = process::pending(&store, mode).await;
        let result = process::write(
            &store,
            &fixture,
            |point| {
                store
                    .phase_fault
                    .store(point, std::sync::atomic::Ordering::SeqCst);
            },
            true,
        )
        .await;
        assert_eq!(result.unwrap_err(), crate::PhaseError::UnknownStoreOutcome);
        process::child::spawn(
            "postgres::phase::process_tests::process_helper",
            &fixture,
            None,
            &[],
        )
        .wait()
        .await;
    }
}
#[tokio::test]
async fn killed_writer_rolls_back_claim_start_and_all_resolution_choices() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    super::PostgresStore::migrate(&url).await.unwrap();
    let store = super::PostgresStore::connect(&url).await.unwrap();
    for mode in process::Mode::ALL {
        crash(&store, mode).await;
    }
}
async fn crash(store: &super::PostgresStore, mode: process::Mode) {
    let fixture = process::pending(store, mode).await;
    let name = format!("phase_barrier_{}", uuid::Uuid::new_v4().simple());
    let operation = mode.operation();
    // The session and SQL identifier are generated UUIDs; operation is a fixed enum.
    sqlx::query(&format!("CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.kind='phase' AND NEW.session_id='{}' AND NEW.data_json::jsonb->>'operation'='{operation}' THEN PERFORM pg_advisory_xact_lock(751302925); END IF; RETURN NEW; END $$",fixture.session.0)).execute(&store.pool).await.unwrap();
    sqlx::query(&format!("CREATE TRIGGER {name} AFTER INSERT ON operation_receipts FOR EACH ROW EXECUTE FUNCTION {name}()")).execute(&store.pool).await.unwrap();
    let mut held = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(751302925)")
        .execute(&mut *held)
        .await
        .unwrap();
    let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *held)
        .await
        .unwrap();
    let mut child = process::child::spawn(
        "postgres::phase::process_tests::process_helper",
        &fixture,
        None,
        &[("CRABBER_PHASE_WRITE", "1".into())],
    );
    let waiter = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            assert!(child.running(), "writer exited before receipt barrier");
            let pid: Option<i32> = sqlx::query_scalar("SELECT pid FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND query LIKE 'INSERT INTO operation_receipts%'").bind(blocker).fetch_optional(&store.pool).await.unwrap();
            if let Some(pid) = pid { break pid; }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }).await.expect("child must reach receipt insert before COMMIT");
    process::uncommitted(store, &fixture).await;
    child.kill();
    sqlx::query("SELECT pg_terminate_backend($1)")
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
    process::uncommitted(store, &fixture).await;
    process::recover_killed_writer(store, &fixture).await;
}
