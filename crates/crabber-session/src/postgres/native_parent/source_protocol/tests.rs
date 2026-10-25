//! Real native claim history, immutable head, and bounded original ACK proof.
use super::*;
use crate::Store;
use crate::public_session::v2::proof::{Failure, Proof, ReadError};
use std::sync::Arc;
fn proof() -> Proof {
    Proof::bounded(32 << 20)
}
#[tokio::test]
async fn protected_native_history_rejects_complete_tail_erasure_and_keeps_original_ack() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        let clock = Arc::new(crabber_core::ManualClock::new(
            time::OffsetDateTime::UNIX_EPOCH,
        ));
        let store = store.with_clock(clock.clone());
        let f = crate::session_owner::preparation_contract::claim_history::fixture(&store, &clock)
            .await;
        let session = &f.before_tail.session_id;
        let run = &f.before_tail.id;
        let mut tx = crate::postgres::transaction::begin(&store.pool)
            .await
            .unwrap();
        let mut charged = proof();
        assert!(history(&mut tx, session, run, &mut charged).await.unwrap());
        assert!(
            history(&mut tx, session, run, &mut Proof::bounded(charged.used()))
                .await
                .unwrap()
        );
        assert!(matches!(
            history(
                &mut tx,
                session,
                run,
                &mut Proof::bounded(charged.used() - 1)
            )
            .await,
            Err(ReadError::Budget(Failure::Limited(_)))
        ));
        let head = head(&mut tx, session, run, &mut proof())
            .await
            .unwrap()
            .unwrap();
        sqlx::query(
            "DELETE FROM native_run_issuances WHERE session_id=$1 AND run_id=$2 AND sequence=$3",
        )
        .bind(&session.0)
        .bind(&run.0)
        .bind(i64::try_from(head.sequence).unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
        sqlx::query("DELETE FROM operation_receipts WHERE session_id=$1 AND operation_id=$2")
            .bind(&session.0)
            .bind(&f.tail.operation_id.0)
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("DELETE FROM events WHERE session_id=$1 AND seq=$2")
            .bind(&session.0)
            .bind(i64::try_from(f.tail.event_cursor.0).unwrap())
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE runs SET status=$3,claim_token=$4,data=$5::jsonb WHERE session_id=$1 AND id=$2",
        )
        .bind(&session.0)
        .bind(&run.0)
        .bind("running")
        .bind(&f.before_tail.claim_token)
        .bind(serde_json::to_string(&f.before_tail).unwrap())
        .execute(&mut *tx)
        .await
        .unwrap();
        assert!(
            original(&mut tx, session, run, 1, &mut proof())
                .await
                .unwrap()
                .is_some()
        );
        assert!(history(&mut tx, session, run, &mut proof()).await.is_err());
        tx.rollback().await.unwrap();
        let mut tx = crate::postgres::transaction::begin(&store.pool)
            .await
            .unwrap();
        for query in [
            "DELETE FROM native_run_sources WHERE run_id=$1",
            "DELETE FROM native_run_source_heads WHERE run_id=$1",
            "UPDATE runs SET native_source_protocol=0 WHERE id=$1",
        ] {
            let mut save = tx.begin().await.unwrap();
            assert!(
                sqlx::query(query)
                    .bind(&run.0)
                    .execute(&mut *save)
                    .await
                    .is_err()
            );
            save.rollback().await.unwrap();
        }
        tx.rollback().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn native_session_identity_and_revision_cannot_be_rewound_or_reborn() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        let session = crabber_core::SessionId::new();
        let keyed =
            crate::admission_execution_contract::keyed(&session, time::OffsetDateTime::now_utc());
        let crate::KeyedAdmitOutcome::Started { admitted, .. } =
            store.admit_keyed_run(keyed).await.unwrap()
        else {
            panic!()
        };
        sqlx::query("UPDATE sessions SET public_revision=public_revision+1 WHERE id=$1")
            .bind(&session.0)
            .execute(&store.pool)
            .await
            .unwrap();
        for query in [
            "UPDATE sessions SET public_revision=public_revision-1 WHERE id=$1",
            "UPDATE sessions SET native_run_protocol=0 WHERE id=$1",
            "UPDATE sessions SET id='moved' WHERE id=$1",
            "DELETE FROM sessions WHERE id=$1",
        ] {
            assert!(
                sqlx::query(query)
                    .bind(&session.0)
                    .execute(&store.pool)
                    .await
                    .is_err(),
                "{query}"
            );
        }
        let mut tx = crate::postgres::transaction::begin(&store.pool)
            .await
            .unwrap();
        let mut charged = proof();
        assert!(
            original(&mut tx, &session, &admitted.run.id, 1, &mut charged)
                .await
                .unwrap()
                .is_some()
        );
        sqlx::query(
            "UPDATE native_run_issuances SET data_json=$3 WHERE session_id=$1 AND run_id=$2",
        )
        .bind(&session.0)
        .bind(&admitted.run.id.0)
        .bind("x".repeat(1 << 20))
        .execute(&mut *tx)
        .await
        .unwrap();
        assert!(matches!(
            original(
                &mut tx,
                &session,
                &admitted.run.id,
                1,
                &mut Proof::bounded(4096)
            )
            .await,
            Err(ReadError::Budget(Failure::Limited(_)))
        ));
        tx.rollback().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn retained_context_native_era_survives_session_creation_and_rename() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url).await.unwrap();

        let session = crabber_core::SessionId::new();
        let reservation = crate::session_owner_contract::reservation(session.clone());
        store
            .reserve_session_owner(reservation.clone(), None)
            .await
            .unwrap();
        let mut tx = crate::postgres::transaction::begin(&store.pool)
            .await
            .unwrap();
        // Model an actual retained pre19 context without changing its authenticated
        // owner/audit sources. Only this fixture may disable the new era guard.
        sqlx::query("ALTER TABLE session_owner_contexts DISABLE TRIGGER USER")
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("UPDATE session_owner_contexts SET native_run_protocol=0 WHERE session_id=$1")
            .bind(&session.0)
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query("ALTER TABLE session_owner_contexts ENABLE TRIGGER USER")
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        let saved = store
            .manage_session(crate::ManageSession {
                id: crabber_core::ControlId::new(),
                session_id: session.clone(),
                expected_revision: crabber_core::SessionRevision::INITIAL,
                change: crate::SessionChange::Create {
                    workspace_id: reservation.binding.workspace_id,
                    directory: reservation.binding.directory,
                    title: "legacy context".into(),
                    defaults: crate::SessionDefaults::default(),
                },
            })
            .await
            .unwrap();
        store
            .manage_session(crate::ManageSession {
                id: crabber_core::ControlId::new(),
                session_id: session.clone(),
                expected_revision: saved.revision,
                change: crate::SessionChange::Rename {
                    title: "retained era".into(),
                },
            })
            .await
            .unwrap();
        let marker: i64 = sqlx::query_scalar(
            "SELECT CAST(native_run_protocol AS BIGINT) FROM sessions WHERE id=$1",
        )
        .bind(&session.0)
        .fetch_one(&store.pool)
        .await
        .unwrap();
        assert_eq!(marker, 0);
        let mut tx = crate::postgres::transaction::begin(&store.pool)
            .await
            .unwrap();
        for query in [
            "UPDATE session_owner_contexts SET native_run_protocol=1 WHERE session_id=$1",
            "UPDATE session_owner_contexts SET session_id='moved' WHERE session_id=$1",
            "UPDATE session_owner_contexts SET session_id=NULL WHERE session_id=$1",
            "DELETE FROM session_owner_contexts WHERE session_id=$1",
        ] {
            let mut save = tx.begin().await.unwrap();
            assert!(
                sqlx::query(query)
                    .bind(&session.0)
                    .execute(&mut *save)
                    .await
                    .is_err()
            );
            save.rollback().await.unwrap();
        }
        tx.rollback().await.unwrap();
    })
    .await
    .unwrap();
}
