//! A compound caller's consumed allowance cannot reset inside the issuer writer.
use crate::Store;
use crate::public_session::v2::proof::Proof;
#[tokio::test]
async fn native_issuer_write_reuses_the_remaining_enclosing_proof() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {

    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    let session = crabber_core::SessionId(format!(
        "{}",
        uuid::Uuid::new_v4().as_u128() % 1_000_000_000_000 + 1_000_000_000_000
    ));
    let request =
        crate::admission_execution_contract::keyed(&session, time::OffsetDateTime::now_utc());
    let crate::KeyedAdmitOutcome::Started { admitted, .. } =
        store.admit_keyed_run(request).await.unwrap()
    else {
        panic!("fresh admission")
    };
    let mut next = admitted.run;
    let run_id = next.id.clone();
    next.claim_token = uuid::Uuid::new_v4().to_string();
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    let before = super::head(&mut tx, &session, &run_id, &mut Proof::bounded(32 << 20))
        .await
        .unwrap()
        .unwrap();
    let mut exhausted = Proof::bounded(32 << 20);
    exhausted.charge((32 << 20) - 1).unwrap();
    assert!(
        crate::postgres::save_run_with_proof(&mut tx, &next, None, &mut exhausted)
            .await
            .is_err()
    );
    assert!(exhausted.used() > 32 << 20);
    // An allowance failure precedes publication, and the actual writer can
    // still advance under a sufficient shared allowance in this transaction.
    crate::postgres::save_run_with_proof(&mut tx, &next, None, &mut Proof::bounded(32 << 20))
        .await
        .unwrap();
    let after = super::head(&mut tx, &session, &run_id, &mut Proof::bounded(32 << 20))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(after.sequence, before.sequence + 1);
    tx.rollback().await.unwrap();
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    let restored = super::head(&mut tx, &session, &run_id, &mut Proof::bounded(32 << 20))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restored.sequence, before.sequence);
    assert_eq!(restored.chain_digest, before.chain_digest);
    sqlx::query("UPDATE runs SET data=jsonb_set(data,'{status}','\"failed\"'::jsonb) WHERE id=$1")
        .bind(&run_id.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    assert!(
        super::current(&mut tx, &session, &run_id, &mut Proof::bounded(32 << 20))
            .await
            .is_err()
    );
    // A numeric JSON identifier must not compare equal to the indexed TEXT identity.
    sqlx::query("UPDATE runs SET data=jsonb_set(jsonb_set(data,'{status}',to_jsonb(status)),'{session_id}',to_jsonb(CAST(session_id AS BIGINT))) WHERE id=$1").bind(&run_id.0).execute(&mut *tx).await.unwrap();
    assert!(
        super::current(&mut tx, &session, &run_id, &mut Proof::bounded(32 << 20))
            .await
            .is_err()
    );
    let erased_type: bool =
        sqlx::query_scalar("SELECT data->>'session_id'=session_id FROM runs WHERE id=$1")
            .bind(&run_id.0)
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    assert!(
        erased_type,
        "the legacy text comparison must accept this malformed numeric identity"
    );
    // Independently authenticated original metadata survives later mutable damage.
    assert!(
        super::original(&mut tx, &session, &run_id, 1, &mut Proof::bounded(32 << 20))
            .await
            .unwrap()
            .is_some()
    );

    }).await.unwrap();
}
