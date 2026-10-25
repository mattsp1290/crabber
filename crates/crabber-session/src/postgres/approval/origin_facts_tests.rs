use crate::public_session::v2::proof::{Failure, Proof, ReadError};
use crate::{PublicHeaderOutcome, PublicHeaderRequest, Store};
#[tokio::test]
async fn aggregate_approval_origin_materializes_only_coherent_run_facts() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
            time::OffsetDateTime::UNIX_EPOCH,
        ));
        let store = store.with_clock(clock.clone());
        let f = crate::approval_contract::fixture::staged(&store, &clock).await;
        f.execution
            .suspend_for_approval(f.batch.clone(), f.owner.clone())
            .await
            .unwrap();
        let request = PublicHeaderRequest {
            session_id: f.run.session_id.clone(),
            run_id: Some(f.run.id.clone()),
            expected_revision: store.session_revision(&f.run.session_id).await.unwrap(),
            max_bytes: 65536,
            max_proof_bytes: 1024 * 1024,
            max_interactions: 64,
        };
        let PublicHeaderOutcome::Header(before) =
            store.public_control_header(request.clone()).await.unwrap()
        else {
            panic!("header")
        };
        sqlx::query(MUTATE)
            .bind(&f.run.id.0)
            .bind("private-error-marker".repeat(150_000))
            .execute(&store.pool)
            .await
            .unwrap();
        let PublicHeaderOutcome::Header(after) =
            store.public_control_header(request).await.unwrap()
        else {
            panic!("header")
        };
        assert_eq!(after.read_usage.proof_bytes, before.read_usage.proof_bytes);
        assert_eq!(after.approval, before.approval);
        assert!(
            !serde_json::to_string(&after)
                .unwrap()
                .contains("private-error-marker")
        );
    })
    .await
    .unwrap();
}
const MUTATE: &str =
    "UPDATE runs SET data=jsonb_set(data,'{error}',to_jsonb(CAST($2 AS TEXT))) WHERE id=$1";

#[tokio::test]
async fn aggregate_approval_current_actual_shared_carrier_budget() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
            time::OffsetDateTime::UNIX_EPOCH,
        ));
        let store = store.with_clock(clock.clone());
        let f = crate::approval_contract::fixture::staged(&store, &clock).await;
        f.execution
            .suspend_for_approval(f.batch.clone(), f.owner.clone())
            .await
            .unwrap();
        let mut tx = crate::postgres::transaction::begin(&store.pool)
            .await
            .unwrap();
        let mut proof = Proof::bounded(32 * 1024 * 1024);
        let (record, _) = super::load_with_proof(&mut tx, &f.run.session_id, &f.run.id, &mut proof)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.batch.continuation, f.batch.continuation);
        let once = proof.used();
        assert!(once > 1000);
        let mut exact = Proof::bounded(once);
        super::load_with_proof(&mut tx, &f.run.session_id, &f.run.id, &mut exact)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(exact.used(), once);
        let mut short = Proof::bounded(once - 1);
        assert!(matches!(
            super::load_with_proof(&mut tx, &f.run.session_id, &f.run.id, &mut short).await,
            Err(ReadError::Budget(Failure::Limited(
                crate::PublicSessionLimitV2::ProofBytes
            )))
        ));
        tx.rollback().await.unwrap();
    })
    .await
    .unwrap();
}
