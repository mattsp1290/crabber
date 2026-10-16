use super::*;
use crate::sql_deletion::readiness::Failure;
use crate::{DeletionBlocker, Store};
use crabber_core::{AbandonAuthority, AbandonRequest};
#[tokio::test]
async fn original_unstarted_abandonment_has_bounded_positive_domain_proof() {
    let (store, _path, _directory) = super::super::tests::temp_store().await;
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
        let mut tx = store.writer.begin().await.unwrap();
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
            sqlx::query("UPDATE abandonment_commits SET data=json_set(data,'$.no_effect_v1',NULL) WHERE run_id=$1").bind(&receipt.run_id.0).execute(&store.writer).await.unwrap();
            let mut tx = store.writer.begin().await.unwrap();
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
            sqlx::query("UPDATE abandonment_commits SET data=json_set(data,'$.outcome.run.checkpoint',replace(hex(zeroblob(17301504)),'0','x')) WHERE run_id=$1").bind(&receipt.run_id.0).execute(&store.writer).await.unwrap();
            let mut tx = store.writer.begin().await.unwrap();
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
    store.writer.close().await;
}
