use crate::Store;
use crate::deletion::process;
#[tokio::test]
async fn quiet_deletion_process_helper() {
    let Some(request) = process::request() else {
        return;
    };
    let path = std::path::PathBuf::from(std::env::var("CRABBER_SQLITE_CHILD_PATH").unwrap());
    let store = crate::SqliteStore::connect(&path).await.unwrap();
    process::execute(&store, request).await;
}
#[tokio::test]
async fn quiet_deletion_killed_before_and_after_commit() {
    let (store, path, directory) = crate::sqlite::tests::temp_store().await;
    for point in ["before_commit", "after_commit"] {
        let request = process::setup(&store).await;
        let marker = directory.path().join(point);
        let mut child = process::spawn(
            "sqlite::deletion::coordinator::tests::process_tests::quiet_deletion_process_helper",
            &request,
            crabber_test_support::ChildDatabase::Sqlite(&path),
            Some((point, &marker)),
        );
        process::marker(&marker).await;
        let cold = crate::SqliteStore::connect(&path).await.unwrap();
        if point == "before_commit" {
            assert!(
                cold.get_session(&request.session_id)
                    .await
                    .unwrap()
                    .is_some()
            );
            assert!(
                cold.lookup_session_deletion(&request.session_id, &request.id)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                cold.session_revision(&request.session_id).await.unwrap(),
                request.expected_revision
            );
        } else {
            assert!(
                cold.get_session(&request.session_id)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert!(
                cold.lookup_session_deletion(&request.session_id, &request.id)
                    .await
                    .unwrap()
                    .is_some()
            );
        }
        child.kill();
        if point == "before_commit" {
            assert!(matches!(
                cold.delete_session(request.clone()).await.unwrap(),
                crate::DeletionOutcome::Applied(_)
            ));
        }
        process::spawn(
            "sqlite::deletion::coordinator::tests::process_tests::quiet_deletion_process_helper",
            &request,
            crabber_test_support::ChildDatabase::Sqlite(&path),
            None,
        )
        .wait()
        .await;
    }
}
