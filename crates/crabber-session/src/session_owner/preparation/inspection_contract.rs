//! Same original registered preparation; this consumer never invokes a factory.
use super::super::{JoinedPreparation, PreparationCompletion};
use crate::Store;
use crate::session_owner::preparation::PreparationKey;
use crate::session_owner::preparation_contract::resources::initial_unkeyed_observation;
pub(crate) async fn fresh(store: &impl Store) -> (JoinedPreparation, crate::AdmitOutcome) {
    assert!(store.supports_initial_preparation_admission_inspection());
    let original = initial_unkeyed_observation(store).await;
    let session = &original
        .observed_grant()
        .original_reservation()
        .owner
        .binding
        .session_id;
    let revision = store.session_revision(session).await.unwrap();
    assert!(
        store
            .inspect_initial_preparation_admission(&original)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(store.session_revision(session).await.unwrap(), revision);
    let PreparationCompletion::Admitted(admitted) =
        store.complete_preparation(original.clone()).await.unwrap()
    else {
        panic!("fresh original admission");
    };
    (original, admitted)
}
pub(crate) async fn run(store: &impl Store) {
    let (original, admitted) = fresh(store).await;
    let session = &admitted.session.id;
    let revision = store.session_revision(session).await.unwrap();
    let receipt = store
        .inspect_initial_preparation_admission(&original)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.session_id, *session);
    assert_eq!(receipt.run_id, admitted.run.id);
    let PreparationKey::Unkeyed(id) = &original.observed_grant().original_reservation().key else {
        panic!();
    };
    assert_eq!(&receipt.operation_id, id);
    assert_eq!(
        store
            .inspect_initial_preparation_admission(&original)
            .await
            .unwrap(),
        Some(receipt)
    );
    assert_eq!(store.session_revision(session).await.unwrap(), revision);
}
