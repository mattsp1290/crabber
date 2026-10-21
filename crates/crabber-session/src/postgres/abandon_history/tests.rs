use super::*;
use crate::sql_deletion::readiness::Failure;
use crate::{DeletionBlocker, Store};
use crabber_core::{AbandonAuthority, AbandonRequest};
#[tokio::test]
async fn original_unstarted_abandonment_has_bounded_positive_domain_proof() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {

    super::super::PostgresStore::migrate(&url).await.unwrap();
    let store = super::super::PostgresStore::connect(&url).await.unwrap();
    let now = time::OffsetDateTime::now_utc();
    for started in [false, true] {
        let keyed = crate::admission_execution_contract::keyed(&SessionId::new(), now);
        let crate::KeyedAdmitOutcome::Started { receipt, admitted } =
            store.admit_keyed_run(keyed.clone()).await.unwrap()
        else {
            panic!()
        };
        if started {
            store
                .execution(admitted.fence.clone())
                .await
                .unwrap()
                .begin_admission_execution()
                .await
                .unwrap();
        }
        store
            .abandon_run(AbandonRequest {
                expected: admitted.fence,
                expected_owner: admitted.run.owner,
                authority: AbandonAuthority::HostStoppedOwner,
            })
            .await
            .unwrap();
        let mut tx = crate::postgres::transaction::begin(&store.pool)
            .await
            .unwrap();
        let result = verify(
            &mut tx,
            &receipt.session_id,
            &receipt.run_id,
            &mut Proof::bounded(32 * 1024 * 1024),
        )
        .await;
        assert!(
            matches!(result,Ok(value) if value!=started),
            "actual original abandonment domain failed"
        );
        assert!(matches!(
            verify(
                &mut tx,
                &receipt.session_id,
                &receipt.run_id,
                &mut Proof::bounded(1)
            )
            .await,
            Err(Failure::Blocked(DeletionBlocker::ProofLimit))
        ));
        tx.rollback().await.unwrap();
        if !started {
            sqlx::query("UPDATE abandonment_commits SET data=jsonb_set(data,'{no_effect_v1}','null'::jsonb) WHERE run_id=$1").bind(&receipt.run_id.0).execute(&store.pool).await.unwrap();
            let mut tx = crate::postgres::transaction::begin(&store.pool)
                .await
                .unwrap();
            assert!(matches!(
                verify(
                    &mut tx,
                    &receipt.session_id,
                    &receipt.run_id,
                    &mut Proof::bounded(32 * 1024 * 1024)
                )
                .await,
                Err(Failure::Error(crate::DeletionError::UnknownStoreOutcome))
            ));
            tx.rollback().await.unwrap();
            sqlx::query("UPDATE abandonment_commits SET data=jsonb_set(data,'{outcome,run,checkpoint}',to_jsonb(repeat('x',34603008))) WHERE run_id=$1").bind(&receipt.run_id.0).execute(&store.pool).await.unwrap();
            let mut tx = crate::postgres::transaction::begin(&store.pool)
                .await
                .unwrap();
            assert!(matches!(
                verify(
                    &mut tx,
                    &receipt.session_id,
                    &receipt.run_id,
                    &mut Proof::bounded(32 * 1024 * 1024)
                )
                .await,
                Err(Failure::Blocked(DeletionBlocker::ProofLimit))
            ));
            tx.rollback().await.unwrap();
        }
    }
    store.pool.close().await;

    }).await.unwrap();
}
