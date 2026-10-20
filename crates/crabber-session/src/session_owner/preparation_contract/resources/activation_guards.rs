//! Child capacity never revives a completed, cancelled, or unrelated root.
use super::*;
use crate::session_owner::owned_scope::{
    OwnedMainDisposition, OwnedScopeParentAuthority, OwnedScopeSource, OwnedTaskRegistration,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
pub(super) async fn run(store: &impl Store) {
    for concluded in [false, true] {
        let (original, bound) = setup(store, true).await;
        let PreparationResourceOutcome::Applied { receipt, grant } =
            store.retain_preparation_resources(original).await.unwrap()
        else {
            panic!("fresh child")
        };
        let child = grant.retained_resources_authority().unwrap().clone();
        let effects = Arc::new(AtomicUsize::new(0));
        let invoked = effects.clone();
        let mut task = OwnedTaskRegistration::register(
            OwnedScopeSource::from_receipts(&receipt.reserved, &receipt.began).unwrap(),
            move || {
                invoked.fetch_add(1, Ordering::SeqCst);
                async { 42 }
            },
        )
        .unwrap();
        if concluded {
            store
                .complete_preparation(joined((*grant).clone(), bound).await)
                .await
                .unwrap();
        } else {
            store
                .request_session_owner_cancel(crate::session_owner::RequestSessionOwnerCancel {
                    id: crabber_core::SessionOwnerOperationId::new(),
                    session_id: child.session_id().clone(),
                    expected_revision: Some(
                        store.session_revision(child.session_id()).await.unwrap(),
                    ),
                    target: grant.owner_authority().target().clone(),
                    claim: crate::session_owner::SessionOwnerResolver {
                        resolver_id: "host".into(),
                        authority_claim: "stop the exact preparation".into(),
                    },
                })
                .await
                .unwrap();
        }
        assert!(
            store
                .activate_owned_scope(
                    task.activation().clone(),
                    child,
                    Some(OwnedScopeParentAuthority::preparation(*grant))
                )
                .await
                .is_err()
        );
        assert_eq!(
            task.join().await.unwrap().disposition(),
            OwnedMainDisposition::NoDispatch
        );
        assert_eq!(effects.load(Ordering::SeqCst), 0);
    }
}
