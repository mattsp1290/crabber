//! Selected original metadata is independent of later state, but never of its publication.
use super::super::{proof, source};
use crate::public_session::v2::proof::Proof;
#[tokio::test]
async fn driver_original_publication_refuses_valid_projection_rewrites() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    for unkeyed in [false, true] {
        let request = if unkeyed {
            crate::driver_contract::request_unkeyed(&store).await
        } else {
            crate::driver_contract::request(&store).await
        };
        let session = request.context.session_id();
        let mut tx = crate::postgres::transaction::begin(&store.pool)
            .await
            .unwrap();
        let original =
            source::original_read(&mut tx, session, &request.fence.run_id, &mut allowance())
                .await
                .unwrap();
        let audit = &original.audits[0];
        for downgrade in [false, true] {
            let mut changed = audit.clone();
            if downgrade {
                changed.version = 2;
            } else {
                changed.record.owner = "different-host".into();
            }
            changed.validate().unwrap();
            sqlx::query("UPDATE session_owner_audits SET data_json=$3 WHERE session_id=$1 AND operation_id=$2")
                .bind(&session.0).bind(&audit.receipt.operation_id.0)
                .bind(serde_json::to_string(&changed).unwrap()).execute(&mut *tx).await.unwrap();
            // A decoder-valid projection cannot replace the actual original writer's source.
            super::super::super::reads::audit(&mut tx, session, &audit.receipt.operation_id)
                .await
                .unwrap()
                .unwrap();
            assert!(
                proof::read_audit(
                    &mut tx,
                    session,
                    &audit.receipt.operation_id,
                    &mut allowance()
                )
                .await
                .is_err()
            );
        }
        marker_rewind_is_refused(&mut tx, audit).await;
        assert!(
            proof::read_audit(
                &mut tx,
                session,
                &audit.receipt.operation_id,
                &mut allowance()
            )
            .await
            .is_err()
        );
        sqlx::query(
            "UPDATE session_owner_audits SET data_json=$3 WHERE session_id=$1 AND operation_id=$2",
        )
        .bind(&session.0)
        .bind(&audit.receipt.operation_id.0)
        .bind(serde_json::to_string(audit).unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
        sqlx::query(
            "UPDATE session_owner_current SET data_json='{}' WHERE session_id=$1 AND owner_id=$2",
        )
        .bind(&session.0)
        .bind(&audit.record.snapshot.target.id.0)
        .execute(&mut *tx)
        .await
        .unwrap();
        // Later current corruption invalidates current authority, not the immutable original metadata.
        assert!(
            proof::current(
                &mut tx,
                session,
                &audit.record.snapshot.target.id,
                &mut allowance()
            )
            .await
            .is_err()
        );
        source::original_read(&mut tx, session, &request.fence.run_id, &mut allowance())
            .await
            .unwrap();
        tx.rollback().await.unwrap();
    }
}
fn allowance() -> Proof {
    Proof::bounded(32 * 1024 * 1024)
}

async fn marker_rewind_is_refused(
    tx: &mut crate::postgres::transaction::Transaction<'_, sqlx::Postgres>,
    audit: &crate::session_owner::audit::Audit,
) {
    sqlx::query("SAVEPOINT marker_corruption")
        .execute(&mut **tx)
        .await
        .unwrap();
    sqlx::query("DROP TRIGGER session_owner_protocol_immutable ON session_owner_current")
        .execute(&mut **tx)
        .await
        .unwrap();
    // The independent cohort's composite FK still protects the birth marker.
    assert!(
        sqlx::query(
            "UPDATE session_owner_current SET source_protocol=0 WHERE session_id=$1 AND owner_id=$2"
        )
        .bind(&audit.receipt.session_id.0)
        .bind(&audit.record.snapshot.target.id.0)
        .execute(&mut **tx)
        .await
        .is_err()
    );
    sqlx::query("ROLLBACK TO SAVEPOINT marker_corruption")
        .execute(&mut **tx)
        .await
        .unwrap();
    sqlx::query("RELEASE SAVEPOINT marker_corruption")
        .execute(&mut **tx)
        .await
        .unwrap();
}
