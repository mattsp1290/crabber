use crate::Store;
use crate::deletion::process;
#[tokio::test]
async fn quiet_deletion_process_helper() {
    let Some(request) = process::request() else {
        return;
    };
    let url = crabber_test_support::postgres::TestDatabase::attached_url()
        .await
        .unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    process::execute(&store, request).await;
}
#[tokio::test]
async fn quiet_deletion_killed_before_and_after_commit() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {

    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    let directory = tempfile::tempdir().unwrap();
    for point in ["before_commit", "after_commit"] {
        let request = process::setup(&store).await;
        let marker = directory.path().join(point);
        let mut child = process::spawn(
            "postgres::deletion::coordinator::tests::process_tests::quiet_deletion_process_helper",
            &request,
            crabber_test_support::ChildDatabase::Postgres(&url),
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
            crabber_test_support::ChildDatabase::Postgres(&url),
            None,
        )
        .wait()
        .await;
    }

    }).await.unwrap();
}
