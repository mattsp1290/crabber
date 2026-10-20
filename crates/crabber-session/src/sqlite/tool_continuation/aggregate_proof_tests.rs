use crate::PublicSessionLimitV2;
use crate::public_session::v2::proof::{Failure, Proof, ReadError};
#[tokio::test]
async fn aggregate_tool_current_actual_shared_carrier_budget() {
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    let (f, batch, _) = crate::frontend_contract::fixture::paused(&store).await;
    let mut tx = crate::transaction::begin(&store.readers).await.unwrap();
    let mut proof = Proof::bounded(32 * 1024 * 1024);
    let record = super::read::load_with_proof(
        &mut tx,
        &f.request.session_id,
        &f.request.run_id,
        Some(&batch.continuation.id),
        &mut proof,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(record.batch.continuation, batch.continuation);
    let once = proof.used();
    assert!(once > 8192);
    let mut exact = Proof::bounded(once);
    super::read::load_with_proof(
        &mut tx,
        &f.request.session_id,
        &f.request.run_id,
        Some(&batch.continuation.id),
        &mut exact,
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(exact.used(), once);
    let mut short = Proof::bounded(once - 1);
    assert!(matches!(
        super::read::load_with_proof(
            &mut tx,
            &f.request.session_id,
            &f.request.run_id,
            Some(&batch.continuation.id),
            &mut short
        )
        .await,
        Err(ReadError::Budget(Failure::Limited(
            PublicSessionLimitV2::ProofBytes
        )))
    ));
    assert!(matches!(
        super::read::load_with_proof(
            &mut tx,
            &f.request.session_id,
            &f.request.run_id,
            Some(&batch.continuation.id),
            &mut exact
        )
        .await,
        Err(ReadError::Budget(Failure::Limited(
            PublicSessionLimitV2::ProofBytes
        )))
    ));
    tx.rollback().await.unwrap();
}
