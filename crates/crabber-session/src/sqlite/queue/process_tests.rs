use crate::{Store, queue_contract::process};
#[tokio::test]
async fn queue_process_helper() {
    let Some(request) = process::request() else {
        return;
    };
    let path = std::env::var("CRABBER_SQLITE_CHILD_PATH").unwrap();
    let store = if let Ok(marker) = std::env::var("CRABBER_QUEUE_BARRIER") {
        crate::sqlite::test_support::connect_with_test_options(
            std::path::Path::new(&path),
            crate::sqlite::test_support::TestOptions {
                barrier: Some(marker.into()),
                ..Default::default()
            },
        )
        .await
    } else {
        crate::SqliteStore::connect(&path).await.unwrap()
    };
    if std::env::var_os("CRABBER_QUEUE_WRITE").is_some() {
        store.mutate_queue(request).await.unwrap();
        process::committed_marker();
    } else {
        process::verify(&store, request).await;
    }
}
#[tokio::test]
async fn queue_killed_after_claim_prepared_and_admit_commit() {
    let (store, path, directory) = crate::sqlite::tests::temp_store().await;
    for step in 0..3 {
        let request = process::setup(&store, step).await;
        let marker = directory.path().join(format!("committed-{step}"));
        let mut child = process::spawn(
            "sqlite::queue::process_tests::queue_process_helper",
            &request,
            crabber_test_support::ChildDatabase::Sqlite(&path),
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
        process::spawn(
            "sqlite::queue::process_tests::queue_process_helper",
            &request,
            crabber_test_support::ChildDatabase::Sqlite(&path),
            &[],
        )
        .wait()
        .await;
    }
}
#[tokio::test]
async fn queue_killed_before_claim_prepared_and_admit_commit() {
    let _guard = crate::sqlite::test_support::BARRIER_LOCK.lock().await;
    let (store, path, directory) = crate::sqlite::tests::temp_store().await;
    for step in 0..3 {
        let request = process::setup(&store, step).await;
        let revision = store.session_revision(&request.session_id).await.unwrap();
        sqlx::query("CREATE TRIGGER queue_barrier AFTER INSERT ON operation_receipts BEGIN SELECT 1 WHERE 'a' < 'b' COLLATE crabber_test_barrier; END").execute(&store.writer).await.unwrap();
        let marker = directory.path().join(format!("precommit-{step}"));
        let mut child = process::spawn(
            "sqlite::queue::process_tests::queue_process_helper",
            &request,
            crabber_test_support::ChildDatabase::Sqlite(&path),
            &[
                ("CRABBER_QUEUE_WRITE", "1".into()),
                (
                    "CRABBER_QUEUE_BARRIER",
                    marker.to_string_lossy().into_owned(),
                ),
            ],
        );
        process::marker(&marker).await;
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
        sqlx::query("DROP TRIGGER queue_barrier")
            .execute(&store.writer)
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
        process::spawn(
            "sqlite::queue::process_tests::queue_process_helper",
            &request,
            crabber_test_support::ChildDatabase::Sqlite(&path),
            &[],
        )
        .wait()
        .await;
    }
}
