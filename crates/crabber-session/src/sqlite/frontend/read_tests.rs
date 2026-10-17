use super::read;
use crate::PublicSessionLimitV2;
use crate::public_session::v2::proof::{Proof, ReadError};
#[tokio::test]
async fn aggregate_frontend_actual_carrier_budget_and_narrow_scope() {
    let (store, _path, _directory) = crate::sqlite::tests::temp_store().await;
    let f = crate::frontend_contract::fixture::fixture(&store).await;
    f.execution
        .stage_frontend_call(f.request.clone(), f.authority.clone())
        .await
        .unwrap();
    let mut tx = crate::transaction::begin(&store.readers).await.unwrap();
    let mut first = Proof::bounded(32 * 1024 * 1024);
    let scope = read::scope_with_proof(
        &mut tx,
        &f.request.session_id,
        &f.request.run_id,
        &mut first,
    )
    .await
    .unwrap();
    let start = first.used();
    let original = read::origin_with_proof(&mut tx, &scope, &f.request.call.id, &mut first)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original.receipt.operation_id, f.request.id);
    let once = first.used() - start;
    assert!(once > 1000);
    // Every repeated source read consumes this SAME remaining allowance.
    let mut repeated = Proof::bounded(once * 2 - 1);
    read::origin_with_proof(&mut tx, &scope, &f.request.call.id, &mut repeated)
        .await
        .unwrap();
    assert_eq!(repeated.used(), once);
    assert!(matches!(
        read::origin_with_proof(&mut tx, &scope, &f.request.call.id, &mut repeated).await,
        Err(ReadError::Budget(
            crate::public_session::v2::proof::Failure::Limited(PublicSessionLimitV2::ProofBytes)
        ))
    ));
    tx.rollback().await.unwrap();
    // Irrelevant private Run data is neither materialized nor charged.
    sqlx::query(CHECKPOINT)
        .bind(&f.request.run_id.0)
        .bind("private-checkpoint-marker".repeat(100_000))
        .execute(&store.writer)
        .await
        .unwrap();
    let mut tx = crate::transaction::begin(&store.readers).await.unwrap();
    let mut proof = Proof::bounded(start + once);
    let scope = read::scope_with_proof(
        &mut tx,
        &f.request.session_id,
        &f.request.run_id,
        &mut proof,
    )
    .await
    .unwrap();
    read::origin_with_proof(&mut tx, &scope, &f.request.call.id, &mut proof)
        .await
        .unwrap();
    assert_eq!(proof.used(), first.used());
    tx.rollback().await.unwrap();
    sqlx::query("UPDATE frontend_tool_origins SET data_json=$2 WHERE call_id=$1")
        .bind(&f.request.call.id.0)
        .bind("malformed-private-origin".repeat(100_000))
        .execute(&store.writer)
        .await
        .unwrap();
    let mut tx = crate::transaction::begin(&store.readers).await.unwrap();
    let mut proof = Proof::bounded(8192);
    let scope = read::scope_with_proof(
        &mut tx,
        &f.request.session_id,
        &f.request.run_id,
        &mut proof,
    )
    .await
    .unwrap();
    assert!(matches!(
        read::origin_with_proof(&mut tx, &scope, &f.request.call.id, &mut proof).await,
        Err(ReadError::Budget(
            crate::public_session::v2::proof::Failure::Limited(PublicSessionLimitV2::ProofBytes)
        ))
    ));
    tx.rollback().await.unwrap();
}
const CHECKPOINT: &str = "UPDATE runs SET data=json_set(data,'$.checkpoint',$2) WHERE id=$1";
