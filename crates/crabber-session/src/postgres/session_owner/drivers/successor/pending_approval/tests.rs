use super::*;
use crate::Store;
use crabber_core::Clock;
use std::sync::Arc;
#[tokio::test]
async fn pending_approval_preflight_is_pure_and_one_budget() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let _guard = crate::postgres::pending_initial_test_lock().await;
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert_ne!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref(),
            Ok("1")
        );
        return;
    };
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock.clone());
    for (recovery, unkeyed) in [(false, false), (true, false), (false, true), (true, true)] {
        let request = crate::successor_driver_contract::approval::pending::unprepared(
            &store, &clock, recovery, unkeyed,
        )
        .await;
        let before = store
            .session_revision(request.context.session_id())
            .await
            .unwrap();
        let metadata = store
            .inspect_pending_approval_preparation(request.clone())
            .await
            .unwrap();
        request.verify_metadata(&metadata).unwrap();
        let mut wrong_source = metadata.clone();
        wrong_source.source_digest = crabber_core::InputFingerprint::new("f".repeat(64)).unwrap();
        assert!(request.verify_metadata(&wrong_source).is_err());
        let mut wrong = metadata.clone();
        wrong.expected_revision = wrong.expected_revision.next().unwrap();
        assert!(request.verify_metadata(&wrong).is_err());
        let mut tx = crate::postgres::transaction::begin(&store.pool)
            .await
            .unwrap();
        let metadata = choice::metadata(&request).unwrap();
        let mut proof = Proof::bounded(32 << 20);
        Box::pin(inspect(
            &mut tx,
            &request,
            metadata.clone(),
            clock.now(),
            &mut proof,
        ))
        .await
        .unwrap();
        assert!(proof.used() > 1000);
        assert!(
            Box::pin(inspect(
                &mut tx,
                &request,
                metadata,
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
            PendingApprovalSource::Claim(c) => {
                c.expected_revision = c.expected_revision.next().unwrap();
            }
            PendingApprovalSource::Recovery(r) => {
                r.expected_revision = r.expected_revision.next().unwrap();
            }
        }
        assert!(
            store
                .inspect_pending_approval_preparation(wrong)
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn legacy_pending_approval_preflight_remains_unsupported() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let _guard = crate::postgres::pending_initial_test_lock().await;
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert_ne!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref(),
            Ok("1")
        );
        return;
    };
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(clock.clone());
    let request = crate::successor_driver_contract::approval::pending::unprepared(
        &store, &clock, false, false,
    )
    .await;
    // Old-format import only inside a rolled-back test transaction.
    let mut tx = store
        .session_transaction(request.context.session_id())
        .await
        .unwrap();
    for table in [
        "continuations",
        "approval_input_sources",
        "approval_input_cohorts",
    ] {
        sqlx::query(&format!("ALTER TABLE {table} DISABLE TRIGGER USER"))
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    for command in [
        "DELETE FROM approval_input_cohorts WHERE session_id=$1",
        "DELETE FROM approval_input_sources WHERE session_id=$1",
        "UPDATE continuations SET input_source_protocol=0 WHERE session_id=$1",
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
