use super::*;
use crate::Store;
use crate::session_owner::preparation::PendingToolSource;
use crabber_core::Clock;
use std::sync::Arc;
#[tokio::test]
async fn pending_tool_preflight_is_pure_and_one_budget() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("tool-preflight.sqlite");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = crate::SqliteStore::connect(&path)
        .await
        .unwrap()
        .with_clock(clock.clone());
    for &(kind, recovery, unkeyed) in crate::successor_driver_contract::tools::pending::CASES {
        let request = crate::successor_driver_contract::tools::pending::unprepared(
            &store, &clock, kind, recovery, unkeyed,
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
        let mut wrong_source = metadata.clone();
        wrong_source.source_digest = crabber_core::InputFingerprint::new("f".repeat(64)).unwrap();
        assert!(request.verify_metadata(&wrong_source).is_err());
        let mut wrong = metadata.clone();
        wrong.expected_revision = wrong.expected_revision.next().unwrap();
        assert!(request.verify_metadata(&wrong).is_err());
        let mut tx = crate::transaction::begin(&store.readers).await.unwrap();
        let mut proof = Proof::bounded(32 << 20);
        Box::pin(read(&mut tx, &request, clock.now(), &mut proof))
            .await
            .unwrap();
        assert!(proof.used() > 1000);
        Box::pin(read(
            &mut tx,
            &request,
            clock.now(),
            &mut Proof::bounded(proof.used()),
        ))
        .await
        .unwrap();
        assert!(
            Box::pin(read(
                &mut tx,
                &request,
                clock.now(),
                &mut Proof::bounded(proof.used() - 1)
            ))
            .await
            .is_err()
        );
        tx.rollback().await.unwrap();
        assert_eq!(
            store
                .session_revision(request.context.session_id())
                .await
                .unwrap(),
            before
        );
        let mut wrong = request.clone();
        match &mut wrong.source {
            PendingToolSource::Claim(c) => {
                c.expected_revision = c.expected_revision.next().unwrap();
            }
            PendingToolSource::Recovery(r) => {
                r.expected_revision = r.expected_revision.next().unwrap();
            }
            PendingToolSource::FrontendClaim(c) => {
                c.claim.expected_revision = c.claim.expected_revision.next().unwrap();
            }
            PendingToolSource::FrontendRecovery(r) => {
                r.expected_revision = r.expected_revision.next().unwrap();
            }
        }
        assert!(store.inspect_pending_tool_preparation(wrong).await.is_err());
    }
}

#[tokio::test]
async fn legacy_pending_tool_preflight_remains_unsupported() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("legacy-preflight.sqlite");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = crate::SqliteStore::connect(path)
        .await
        .unwrap()
        .with_clock(clock.clone());
    let request = crate::successor_driver_contract::tools::pending::unprepared(
        &store, &clock, 0, false, false,
    )
    .await;
    // Old-format import only inside a rolled-back test transaction.
    let mut tx = store.begin_write().await.unwrap();
    for name in [
        "tool_input_cohorts_delete",
        "tool_input_sources_delete",
        "tool_input_identity",
    ] {
        sqlx::query(&format!("DROP TRIGGER {name}"))
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    for command in [
        "DELETE FROM tool_input_cohorts WHERE session_id=$1",
        "DELETE FROM tool_input_sources WHERE session_id=$1",
        "UPDATE tool_continuations SET input_source_protocol=0 WHERE session_id=$1",
    ] {
        sqlx::query(command)
            .bind(&request.context.session_id().0)
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    let metadata = choice::metadata(&request).unwrap();
    assert!(matches!(
        Box::pin(inspect(
            &mut tx,
            &request,
            metadata,
            clock.now(),
            &mut Proof::bounded(32 << 20)
        ))
        .await,
        Err(Error::Unsupported)
    ));
    tx.rollback().await.unwrap();
}
