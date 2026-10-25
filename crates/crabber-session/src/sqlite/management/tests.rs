use crate::{
    Store,
    management_contract::{self, process},
};
#[tokio::test]
async fn management_contract() {
    let (store, _, _directory) = crate::sqlite::tests::temp_store().await;
    management_contract::run_contract(&store).await;
}
#[tokio::test]
async fn management_fault_contract() {
    let (store, _, _directory) = crate::sqlite::tests::temp_store().await;
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
    let path = std::env::var("CRABBER_SQLITE_CHILD_PATH").unwrap();
    if let Ok(marker) = std::env::var("CRABBER_MANAGEMENT_BARRIER") {
        let store = crate::sqlite::test_support::connect_with_test_options(
            std::path::Path::new(&path),
            crate::sqlite::test_support::TestOptions {
                barrier: Some(marker.into()),
                ..Default::default()
            },
        )
        .await;
        store.manage_session(request).await.unwrap();
    } else {
        let store = super::SqliteStore::connect(std::path::Path::new(&path))
            .await
            .unwrap();
        process::verify(&store, request).await;
    }
}
#[tokio::test]
async fn committed_management_response_loss_survives_fresh_process() {
    let (store, path, _directory) = crate::sqlite::tests::temp_store().await;
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
        "sqlite::management::tests::process_helper",
        &request,
        crabber_test_support::ChildDatabase::Sqlite(&path),
        &[],
    )
    .wait()
    .await;
}
#[tokio::test]
async fn killed_management_writer_rolls_back_session_metadata_revision_audit_and_receipt() {
    use crate::sqlite::test_support;
    let _guard = test_support::BARRIER_LOCK.lock().await;
    let (store, path, directory) = crate::sqlite::tests::temp_store().await;
    let request = management_contract::create();
    let marker = directory.path().join("management.marker");
    sqlx::query("CREATE TRIGGER management_barrier AFTER INSERT ON operation_receipts BEGIN SELECT 1 WHERE 'a' < 'b' COLLATE crabber_test_barrier; END").execute(&store.writer).await.unwrap();
    let mut child = test_support::child(
        &path,
        "sqlite::management::tests::process_helper",
        &[
            (
                "CRABBER_MANAGEMENT_CHILD",
                serde_json::to_string(&request).unwrap(),
            ),
            (
                "CRABBER_MANAGEMENT_BARRIER",
                marker.to_str().unwrap().into(),
            ),
        ],
    );
    test_support::marker(&marker, &mut child).await;
    process::absent(&store, &request).await;
    child.kill();
    sqlx::query("DROP TRIGGER management_barrier")
        .execute(&store.writer)
        .await
        .unwrap();
    process::absent(&store, &request).await;
    store.manage_session(request.clone()).await.unwrap();
    process::spawn(
        "sqlite::management::tests::process_helper",
        &request,
        crabber_test_support::ChildDatabase::Sqlite(&path),
        &[],
    )
    .wait()
    .await;
}

#[tokio::test]
async fn incoherent_management_indexes_cannot_bypass_archive_admission() {
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    for mismatch in ["archive", "workspace"] {
        let saved = store
            .manage_session(management_contract::create())
            .await
            .unwrap();
        if mismatch == "archive" {
            // Both are valid JSON/SQL values, but the indexed visibility disagrees.
            sqlx::query("UPDATE session_management SET data_json=$2 WHERE session_id=$1")
                .bind(&saved.session_id.0).bind(r#"{"archived":true,"defaults":{"provider_id":"host-provider","model_id":"host-model"}}"#).execute(&store.writer).await.unwrap();
        } else {
            sqlx::query("UPDATE session_management SET workspace_id='foreign-workspace' WHERE session_id=$1").bind(&saved.session_id.0).execute(&store.writer).await.unwrap();
        }
        management_contract::corrupt_admission_refused(&store, &saved).await;
    }
}
