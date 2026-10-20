use crate::Store;
use std::sync::Arc;
#[tokio::test]
async fn pending_tool_preflight_authenticates_actual_latest_recovery_and_original_actors() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("latest-recovery.db");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = crate::SqliteStore::connect(&path)
        .await
        .unwrap()
        .with_clock(clock.clone());
    for unkeyed in [false, true] {
        for kind in 0..3 {
            let request = Box::pin(
                crate::successor_driver_contract::tools::pending::after_recovery(
                    &store, &clock, kind, unkeyed,
                ),
            )
            .await;
            let before = store
                .session_revision(request.context.session_id())
                .await
                .unwrap();
            let metadata = store
                .inspect_pending_tool_preparation(request.clone())
                .await
                .unwrap();
            request.verify_metadata(&metadata).unwrap();
            assert_eq!(
                store
                    .session_revision(request.context.session_id())
                    .await
                    .unwrap(),
                before
            );
        }
        let request = Box::pin(
            crate::successor_driver_contract::approval::pending::after_recovery(
                &store, &clock, unkeyed,
            ),
        )
        .await;
        let metadata = store
            .inspect_pending_approval_preparation(request.clone())
            .await
            .unwrap();
        request.verify_metadata(&metadata).unwrap();
    }
}
