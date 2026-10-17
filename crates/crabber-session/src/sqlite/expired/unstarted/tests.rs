//! Genuine Begin sources cannot be removed by restoring only the projection.
use crate::{AttemptCommand, AttemptMutation, AttemptMutationOutcome, Store};
use crabber_core::{Clock, ControlId, ManualClock};
use std::sync::Arc;
#[tokio::test]
async fn expired_unstarted_refuses_actual_begin_projection_restore() {
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("unstarted.sqlite");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = crate::SqliteStore::connect(&path)
        .await
        .unwrap()
        .with_clock(clock.clone());
    let (admitted, reserved, authority) =
        crate::run_control_contract::expired::unstarted::reserved(&store, &clock, true).await;
    let id = ControlId::new();
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    let AttemptMutationOutcome::Applied { receipt: begun, .. } = execution
        .mutate_attempt(
            AttemptMutation {
                id: id.clone(),
                expected_revision: reserved.revision,
                command: AttemptCommand::Begin {
                    target: reserved.attempt.target.clone(),
                },
            },
            Some(authority),
        )
        .await
        .unwrap()
    else {
        panic!("genuine Begin");
    };
    let mut tx = crate::transaction::begin(&store.writer).await.unwrap();
    sqlx::query("UPDATE execution_attempts SET state='Unstarted' WHERE run_id=$1 AND id=$2")
        .bind(&admitted.run.id.0)
        .bind(&reserved.attempt.target.id.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("SAVEPOINT retain_revision")
        .execute(&mut *tx)
        .await
        .unwrap();
    // The native-era Session guard independently refuses a public revision
    // rewind. Keep that guard intact; restore only mutations it permits.
    let rewind = sqlx::query("UPDATE sessions SET public_revision=$2 WHERE id=$1")
        .bind(&admitted.session.id.0)
        .bind(i64::try_from(reserved.revision.get()).unwrap())
        .execute(&mut *tx)
        .await
        .unwrap_err();
    assert!(
        rewind
            .to_string()
            .contains("crabber_session_identity_immutable")
    );
    sqlx::query("ROLLBACK TO SAVEPOINT retain_revision")
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("DELETE FROM operation_receipts WHERE session_id=$1 AND operation_id=$2")
        .bind(&admitted.session.id.0)
        .bind(&id.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query("SAVEPOINT retain_begin")
        .execute(&mut *tx)
        .await
        .unwrap();
    // Actual independent witness FK refuses removal of its original event.
    assert!(
        sqlx::query("DELETE FROM events WHERE seq=$1")
            .bind(i64::try_from(begun.event_cursor.0).unwrap())
            .execute(&mut *tx)
            .await
            .is_err()
    );
    sqlx::query("ROLLBACK TO SAVEPOINT retain_begin")
        .execute(&mut *tx)
        .await
        .unwrap();
    clock.set(admitted.run.lease_until);
    assert!(
        super::super::context(
            &mut tx,
            crate::session_owner::preparation::InspectExpiredRunPreparation {
                session_id: admitted.session.id,
                expected_fence: admitted.fence,
                owner: "refused restored parent".into(),
                lease: std::time::Duration::from_secs(30),
            },
            clock.now()
        )
        .await
        .is_err()
    );
    tx.rollback().await.unwrap();
}
