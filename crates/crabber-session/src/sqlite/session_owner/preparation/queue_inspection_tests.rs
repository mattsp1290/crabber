use super::*;
#[tokio::test]
async fn pending_queue_original_inspection_has_one_exact_selected_budget() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("queue-inspection.sqlite");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = crate::SqliteStore::connect(&path).await.unwrap();
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
}
