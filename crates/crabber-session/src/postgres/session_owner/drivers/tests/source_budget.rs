//! Selected original evidence keeps capacity distinct from corrupt source data.
use super::super::{Error, source};
use crate::public_session::v2::proof::{Failure, Proof, ReadError};
#[tokio::test]
async fn original_driver_source_preserves_shared_budget_failure() {
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
        let mut tx = store.pool.begin().await.unwrap();
        let now = store.clock.now();
        let mut fresh_budget = Proof::bounded(32 * 1024 * 1024);
        Box::pin(super::super::fresh::fresh(
            &mut tx,
            &request,
            None,
            now,
            &mut fresh_budget,
        ))
        .await
        .unwrap();
        let fresh_used = fresh_budget.used();
        Box::pin(super::super::fresh::fresh(
            &mut tx,
            &request,
            None,
            now,
            &mut Proof::bounded(fresh_used),
        ))
        .await
        .unwrap();
        assert!(matches!(
            Box::pin(super::super::fresh::fresh(
                &mut tx,
                &request,
                None,
                now,
                &mut Proof::bounded(fresh_used - 1)
            ))
            .await,
            Err(Error::UnknownStoreOutcome)
        ));
        let mut budget = Proof::bounded(32 * 1024 * 1024);
        let original = source::original_read(
            &mut tx,
            request.context.session_id(),
            &request.fence.run_id,
            &mut budget,
        )
        .await
        .unwrap();
        let used = budget.used();
        assert!(used > 0);
        source::original_read(
            &mut tx,
            request.context.session_id(),
            &request.fence.run_id,
            &mut Proof::bounded(used),
        )
        .await
        .unwrap();
        assert!(matches!(
            source::original_read(
                &mut tx,
                request.context.session_id(),
                &request.fence.run_id,
                &mut Proof::bounded(used - 1)
            )
            .await,
            Err(ReadError::Budget(Failure::Limited(
                crate::PublicSessionLimitV2::ProofBytes
            )))
        ));
        sqlx::query("UPDATE session_owner_audits SET data_json='{}' WHERE session_id=$1 AND operation_id=$2")
            .bind(&request.context.session_id().0)
            .bind(&original.witness.join_id().unwrap().0)
            .execute(&mut *tx).await.unwrap();
        assert!(matches!(
            source::original_read(
                &mut tx,
                request.context.session_id(),
                &request.fence.run_id,
                &mut Proof::bounded(32 * 1024 * 1024)
            )
            .await,
            Err(ReadError::Domain(Error::UnknownStoreOutcome))
        ));
        tx.rollback().await.unwrap();
    }
}
