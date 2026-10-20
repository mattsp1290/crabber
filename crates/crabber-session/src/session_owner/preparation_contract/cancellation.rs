//! Cancellation is a foreign public publication even on this same owner.
use super::*;
use crate::session_owner::{
    ObserveSessionOwnerCancel, RequestSessionOwnerCancel, SessionOwnerResolver,
};
use crabber_core::SessionOwnerOperationId;
pub(crate) async fn run(store: &impl Store) {
    for existing in [false, true] {
        for observe in [false, true] {
            let session = SessionId::new();
            let mut bound = fixture(&session, SessionRevision::INITIAL);
            let revision = if existing {
                create(store, &bound).await
            } else {
                SessionRevision::INITIAL
            };
            bound = fixture(&session, revision);
            let request = reservation(&bound);
            let grant = begin(store, &request).await;
            let cancel = RequestSessionOwnerCancel {
                id: SessionOwnerOperationId::new(),
                session_id: session.clone(),
                target: grant.owner_authority().target().clone(),
                expected_revision: if existing {
                    Some(store.session_revision(&session).await.unwrap())
                } else {
                    None
                },
                claim: SessionOwnerResolver {
                    resolver_id: "controller".into(),
                    authority_claim: "stop preparation".into(),
                },
            };
            store
                .request_session_owner_cancel(cancel.clone())
                .await
                .unwrap();
            if observe {
                // This fixture has an actual signal, unlike Runtime's pending
                // pre-factory check. Acknowledgement still cannot excuse CAS.
                let signal = tokio::sync::Notify::new();
                signal.notify_waiters();
                store
                    .observe_session_owner_cancel(
                        ObserveSessionOwnerCancel {
                            id: SessionOwnerOperationId::new(),
                            session_id: session.clone(),
                            target: cancel.target.clone(),
                            request_id: cancel.id,
                        },
                        grant.owner_authority().clone(),
                    )
                    .await
                    .unwrap();
            }
            assert!(
                store
                    .complete_preparation(joined(grant, bound.clone()).await)
                    .await
                    .is_err()
            );
            assert!(
                store
                    .lookup_admission(&session, &bound.keyed.options.key)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                store
                    .get_session_owner(&session, &request.owner.binding.id)
                    .await
                    .unwrap()
                    .state,
                SessionOwnerState::Active
            );
        }
    }
}
