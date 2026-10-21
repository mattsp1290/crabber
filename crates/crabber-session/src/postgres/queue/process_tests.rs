use crate::{Store, queue_contract::process};
use crabber_test_support::postgres::{DatabaseMode, TestDatabase};
#[tokio::test]
async fn queue_process_helper() {
    let Some(request) = process::request() else {
        return;
    };
    let url = TestDatabase::attached_url().await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    if std::env::var_os("CRABBER_QUEUE_WRITE").is_some() {
        store.mutate_queue(request).await.unwrap();
        process::committed_marker();
    } else {
        process::verify(&store, request).await;
    }
}
#[tokio::test]
async fn queue_killed_after_claim_prepared_and_admit_commit() {
    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        for step in 0..3 {
            let request = process::setup(&store, step).await;
            let directory = tempfile::tempdir().unwrap();
            let marker = directory.path().join("committed");
            let mut child = spawn(
                &url,
                &request,
                &[
                    ("CRABBER_QUEUE_WRITE", "1".into()),
                    (
                        "CRABBER_QUEUE_COMMITTED",
                        marker.to_string_lossy().into_owned(),
                    ),
                ],
            );
            process::marker(&marker).await;
            child.kill();
            spawn(&url, &request, &[]).wait().await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn queue_killed_before_claim_prepared_and_admit_commit() {
    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        for step in 0..3 {
            kill_before(&url, &store, step).await;
        }
    })
    .await
    .unwrap();
}
async fn kill_before(url: &str, store: &crate::PostgresStore, step: u8) {
    let request = process::setup(store, step).await;
    let revision = store.session_revision(&request.session_id).await.unwrap();
    let function = format!("queue_barrier_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE FUNCTION {function}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.session_id='{}' THEN PERFORM pg_advisory_xact_lock(751302929); END IF; RETURN NEW; END $$", request.session_id.0)).execute(&store.pool).await.unwrap();
    sqlx::query(&format!("CREATE TRIGGER {function} AFTER INSERT ON operation_receipts FOR EACH ROW EXECUTE FUNCTION {function}()")).execute(&store.pool).await.unwrap();
    let mut held = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(751302929)")
        .execute(&mut *held)
        .await
        .unwrap();
    let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *held)
        .await
        .unwrap();
    let mut child = spawn(url, &request, &[("CRABBER_QUEUE_WRITE", "1".into())]);
    let waiter = tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            let pid: Option<i32> = sqlx::query_scalar("SELECT pid FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND query LIKE 'INSERT INTO operation_receipts%'").bind(blocker).fetch_optional(&store.pool).await.unwrap();
            if let Some(pid) = pid { break pid; }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }).await.expect("queue child must reach precommit barrier");
    assert!(
        store
            .lookup_queue(&request.session_id, &request.id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.session_revision(&request.session_id).await.unwrap(),
        revision
    );
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
    assert!(
        store
            .lookup_queue(&request.session_id, &request.id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.session_revision(&request.session_id).await.unwrap(),
        revision
    );
    store.mutate_queue(request.clone()).await.unwrap();
    spawn(url, &request, &[]).wait().await;
}

fn spawn(url: &str, request: &crate::QueueMutation, extras: &[(&str, String)]) -> process::Guard {
    let mut environment = TestDatabase::child_environment(url).unwrap().to_vec();
    environment.extend_from_slice(extras);
    process::spawn(
        "postgres::queue::process_tests::queue_process_helper",
        request,
        None,
        &environment,
    )
}
