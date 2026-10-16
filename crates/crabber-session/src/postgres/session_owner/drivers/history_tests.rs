//! A selected joined Driver does not settle its still-Running admitted Run.
use super::*;
use crate::session_owner::driver::history_fixture;
#[tokio::test]
async fn selected_driver_join_requires_actual_join_and_original_sources() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = std::sync::Arc::new(crate::PostgresStore::connect(&url).await.unwrap());
    for unkeyed in [false, true] {
        for dispatch in [false, true] {
            let f = history_fixture::joined(store.clone(), unkeyed, dispatch).await;
            let mut tx = store.pool.begin().await.unwrap();
            let record =
                super::super::super::reads::audit(&mut tx, f.request.context.session_id(), &f.join)
                    .await
                    .unwrap()
                    .unwrap()
                    .record;
            let mut proof = Proof::bounded(32 * 1024 * 1024);
            assert!(classified(&mut tx, &record, &mut proof).await.unwrap());
            let used = proof.used();
            assert!(
                classified(&mut tx, &record, &mut Proof::bounded(used))
                    .await
                    .unwrap()
            );
            assert!(matches!(
                classified(&mut tx, &record, &mut Proof::bounded(used - 1)).await,
                Err(Failure::Limited(crate::PublicSessionLimitV2::ProofBytes))
            ));
            let mut label_only = record.clone();
            label_only.owned_join = None;
            label_only.owned_scope = None;
            assert!(
                !classified(&mut tx, &label_only, &mut Proof::bounded(used))
                    .await
                    .unwrap()
            );
            let original = source::original_read(
                &mut tx,
                f.request.context.session_id(),
                &f.request.fence.run_id,
                &mut Proof::bounded(32 * 1024 * 1024),
            )
            .await
            .unwrap();
            sqlx::query("UPDATE session_owner_audits SET data_json='{}' WHERE session_id=$1 AND operation_id=$2")
                .bind(&f.request.context.session_id().0)
                .bind(&original.witness.join_id().unwrap().0)
                .execute(&mut *tx).await.unwrap();
            assert!(matches!(
                classified(&mut tx, &record, &mut Proof::bounded(32 * 1024 * 1024)).await,
                Err(Failure::Error(_))
            ));
            tx.rollback().await.unwrap();
        }
    }
}

#[path = "history_tests/forged.rs"]
mod forged;
