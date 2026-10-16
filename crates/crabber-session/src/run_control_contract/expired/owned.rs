//! Actual gated preparation task, used when the original capsule declares ownership.
use super::{JoinedPreparation, PreparationGrant, PreparedOperation, Store};
use crate::session_owner::{
    owned_scope::{
        OwnedScopeActivationOutcome, OwnedScopeParentAuthority, OwnedScopeSource,
        OwnedTaskRegistration,
    },
    preparation::PreparationResourceOutcome,
};
pub(crate) async fn observe(
    store: &impl Store,
    root: PreparationGrant,
    operation: PreparedOperation,
) -> JoinedPreparation {
    observe_counted(store, root, operation, std::sync::Arc::default()).await
}
pub(super) async fn observe_counted(
    store: &impl Store,
    root: PreparationGrant,
    operation: PreparedOperation,
    effects: std::sync::Arc<std::sync::atomic::AtomicUsize>,
) -> JoinedPreparation {
    let PreparationResourceOutcome::Applied { receipt, grant } =
        store.retain_preparation_resources(root).await.unwrap()
    else {
        panic!("fresh linked child");
    };
    let mut task = OwnedTaskRegistration::register(
        OwnedScopeSource::from_receipts(&receipt.reserved, &receipt.began).unwrap(),
        || async move {
            effects.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok::<_, ()>(((), operation))
        },
    )
    .unwrap();
    let OwnedScopeActivationOutcome::Applied { permit, .. } = store
        .activate_owned_scope(
            task.activation().clone(),
            grant.retained_resources_authority().unwrap().clone(),
            Some(OwnedScopeParentAuthority::preparation((*grant).clone())),
        )
        .await
        .unwrap()
    else {
        panic!("fresh applied-only permit");
    };
    task.dispatch(*permit).unwrap();
    JoinedPreparation::observe_registered(*grant, &mut task)
        .await
        .unwrap()
        .unwrap()
        .1
}
