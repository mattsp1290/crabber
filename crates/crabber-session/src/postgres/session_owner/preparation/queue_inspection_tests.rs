use super::*;
#[tokio::test]
async fn pending_queue_original_inspection_has_one_exact_selected_budget() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        let (_, original, _) =
            crate::session_owner::preparation_contract::resources::queue::prepared(&store).await;
        let crate::session_owner::preparation::PreparationCompletion::Queue(outcome) =
            store.complete_preparation(original.clone()).await.unwrap()
        else {
            panic!("Queue completion")
        };
        let receipt = outcome.receipt().clone();
        let mut proof = Proof::bounded(inspection::MAX_PROOF);
        assert_eq!(
            store
                .inspect_queue_admission_with_proof(&original, &mut proof)
                .await
                .unwrap(),
            Some(receipt.clone())
        );
        let used = proof.used();
        assert_eq!(
            store
                .inspect_queue_admission_with_proof(&original, &mut Proof::bounded(used))
                .await
                .unwrap(),
            Some(receipt)
        );
        assert!(
            store
                .inspect_queue_admission_with_proof(&original, &mut Proof::bounded(used - 1))
                .await
                .is_err()
        );
    })
    .await
    .unwrap();
}
