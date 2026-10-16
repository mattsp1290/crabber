use crate::Store;
use crate::deletion::process;
#[tokio::test]
async fn quiet_deletion_process_helper() {
    let Some(request) = process::request() else {
        return;
    };
    let url = crate::postgres::tests::test_url().unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    process::execute(&store, request).await;
}
#[tokio::test]
async fn quiet_deletion_killed_before_and_after_commit() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    let directory = tempfile::tempdir().unwrap();
    for point in ["before_commit", "after_commit"] {
        let request = process::setup(&store).await;
        let marker = directory.path().join(point);
        let mut child = process::spawn(
            "postgres::deletion::coordinator::tests::process_tests::quiet_deletion_process_helper",
            &request,
            None,
            Some((point, &marker)),
        );
        process::marker(&marker).await;
        let cold = crate::PostgresStore::connect(&url).await.unwrap();
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
            "postgres::deletion::coordinator::tests::process_tests::quiet_deletion_process_helper",
            &request,
            None,
            None,
        )
        .wait()
        .await;
    }
}
