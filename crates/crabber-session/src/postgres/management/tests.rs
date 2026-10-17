use crate::{
    Store,
    management_contract::{self, process},
};
#[tokio::test]
async fn management_contract() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    super::PostgresStore::migrate(&url).await.unwrap();
    let store = super::PostgresStore::connect(&url).await.unwrap();
    management_contract::run_contract(&store).await;
}
#[tokio::test]
async fn management_fault_contract() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    super::PostgresStore::migrate(&url).await.unwrap();
    let store = super::PostgresStore::connect(&url).await.unwrap();
    management_contract::faults(&store, |point| {
        store
            .management_fault
            .store(point, std::sync::atomic::Ordering::SeqCst);
    })
    .await;
}
#[tokio::test]
async fn process_helper() {
    let Some(request) = process::fixture() else {
        return;
    };
    let url = crate::postgres::tests::test_url().unwrap();
    let store = super::PostgresStore::connect(&url).await.unwrap();
    if std::env::var_os("CRABBER_MANAGEMENT_WRITE").is_some() {
        store.manage_session(request).await.unwrap();
    } else {
        process::verify(&store, request).await;
    }
}
#[tokio::test]
async fn committed_management_response_loss_survives_fresh_process() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    super::PostgresStore::migrate(&url).await.unwrap();
    let store = super::PostgresStore::connect(&url).await.unwrap();
    let request = management_contract::create();
    store
        .management_fault
        .store(2, std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        store.manage_session(request.clone()).await.unwrap_err(),
        crate::ManagementError::UnknownStoreOutcome
    );
    store
        .management_fault
        .store(0, std::sync::atomic::Ordering::SeqCst);
    process::spawn(
        "postgres::management::tests::process_helper",
        &request,
        None,
        &[],
    )
    .wait()
    .await;
}
#[tokio::test]
async fn killed_management_writer_rolls_back_session_metadata_revision_audit_and_receipt() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    super::PostgresStore::migrate(&url).await.unwrap();
    let store = super::PostgresStore::connect(&url).await.unwrap();
    let request = management_contract::create();
    let function = format!("management_barrier_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.session_id='{}' THEN PERFORM pg_advisory_xact_lock(751302924); END IF; RETURN NEW; END $$",request.session_id.0)).execute(&store.pool).await.unwrap();
    sqlx::query(&format!("CREATE TRIGGER {function} AFTER INSERT ON operation_receipts FOR EACH ROW EXECUTE FUNCTION {function}()")).execute(&store.pool).await.unwrap();
    let mut held = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(751302924)")
        .execute(&mut *held)
        .await
        .unwrap();
    let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *held)
        .await
        .unwrap();
    let mut child = process::spawn(
        "postgres::management::tests::process_helper",
        &request,
        None,
        &[("CRABBER_MANAGEMENT_WRITE", "1".into())],
    );
    let waiter=tokio::time::timeout(std::time::Duration::from_secs(10),async {
        loop {
            let pid:Option<i32>=sqlx::query_scalar("SELECT pid FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND query LIKE 'INSERT INTO operation_receipts%'").bind(blocker).fetch_optional(&store.pool).await.unwrap();
            if let Some(pid)=pid { break pid; }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }).await.expect("management must reach receipt precommit barrier");
    process::absent(&store, &request).await;
    child.kill();
    sqlx::query("SELECT pg_terminate_backend($1)")
        .bind(waiter)
        .execute(&store.pool)
        .await
        .unwrap();
    held.rollback().await.unwrap();
    sqlx::query(&format!("DROP TRIGGER {function} ON operation_receipts"))
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query(&format!("DROP FUNCTION {function}()"))
        .execute(&store.pool)
        .await
        .unwrap();
    process::absent(&store, &request).await;
    store.manage_session(request.clone()).await.unwrap();
    process::spawn(
        "postgres::management::tests::process_helper",
        &request,
        None,
        &[],
    )
    .wait()
    .await;
}

#[tokio::test]
async fn incoherent_management_indexes_cannot_bypass_archive_admission() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    super::PostgresStore::migrate(&url).await.unwrap();
    let store = super::PostgresStore::connect(&url).await.unwrap();
    for mismatch in ["archive", "workspace"] {
        let saved = store
            .manage_session(management_contract::create())
            .await
            .unwrap();
        if mismatch == "archive" {
            // Both are valid JSON/SQL values, but the indexed visibility disagrees.
            sqlx::query("UPDATE session_management SET data_json=$2 WHERE session_id=$1")
                .bind(&saved.session_id.0).bind(r#"{"archived":true,"defaults":{"provider_id":"host-provider","model_id":"host-model"}}"#).execute(&store.pool).await.unwrap();
        } else {
            sqlx::query("UPDATE session_management SET workspace_id='foreign-workspace' WHERE session_id=$1").bind(&saved.session_id.0).execute(&store.pool).await.unwrap();
        }
        management_contract::corrupt_admission_refused(&store, &saved).await;
    }
}
