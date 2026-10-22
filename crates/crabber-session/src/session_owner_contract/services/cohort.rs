//! Reconstruction/new root cannot conceal an unresolved old scoped dependency.
use super::{activated_in, generation};
use crate::{
    Store,
    session_owner::{
        MutateSessionOwner, SessionOwnerCommand, SessionOwnerError, SessionOwnerResolver,
        SessionOwnerTermination,
        preparation::PreparationGrant,
        service::{RegisterSessionServiceDependency, SessionServiceDependencyOutcome},
    },
};
use crabber_core::{SessionId, SessionOwnerOperationId};
use std::sync::Arc;
async fn end_root(store: &dyn Store, grant: &PreparationGrant) {
    store
        .mutate_session_owner(
            MutateSessionOwner {
                id: grant.reconciliation().unwrap().join_id,
                session_id: grant.owner_authority().session_id().clone(),
                target: grant.owner_authority().target().clone(),
                command: SessionOwnerCommand::Join {
                    termination: SessionOwnerTermination::Cancelled,
                },
            },
            Some(grant.owner_authority().clone()),
        )
        .await
        .unwrap();
}
pub(crate) async fn run<S: Store + Clone + 'static>(store: Arc<S>) {
    for uncertain in [false, true] {
        let session = SessionId::new();
        let original = activated_in(store.as_ref(), session.clone()).await;
        let service = super::handoff::enable(store.as_ref(), original.clone()).await;
        if uncertain {
            store
                .mutate_session_owner(
                    MutateSessionOwner {
                        id: SessionOwnerOperationId::new(),
                        session_id: session.clone(),
                        target: service.authority.target().clone(),
                        command: SessionOwnerCommand::MarkUncertain {
                            claim: SessionOwnerResolver {
                                resolver_id: "host".into(),
                                authority_claim: "this exact service outcome is uncertain".into(),
                            },
                        },
                    },
                    Some(service.authority.clone()),
                )
                .await
                .unwrap();
        }
        end_root(store.as_ref(), &original).await;
        let next = activated_in(store.as_ref(), session.clone()).await;
        let access = store
            .check_session_service_dependency_access(
                crate::session_owner::service::SessionServiceDependencyAccess {
                    grant: next.clone(),
                    source: crate::session_owner::service::SessionServiceLink::for_grant(
                        &original,
                        generation(),
                    )
                    .unwrap(),
                    authority: service.authority.clone(),
                },
            )
            .await;
        if uncertain {
            assert!(access.is_err());
        } else {
            access.unwrap();
        }
        let mut descriptor = generation();
        descriptor.id = "new-captured-generation".into();
        let request = RegisterSessionServiceDependency {
            grant: next.clone(),
            generation: descriptor.clone(),
        };
        let expected = request.source().unwrap().identities().unwrap().reserve;
        assert!(matches!(
            store.register_session_service_dependency(request).await,
            Err(SessionOwnerError::Busy)
        ));
        assert!(
            store
                .lookup_session_owner(&session, &expected)
                .await
                .unwrap()
                .is_none()
        );
        // Even an exact old generation cannot be rebound to the later root.
        assert!(matches!(
            store
                .register_session_service_dependency(RegisterSessionServiceDependency {
                    grant: next.clone(),
                    generation: generation()
                })
                .await,
            Err(SessionOwnerError::SemanticConflict)
        ));
        end_root(store.as_ref(), &next).await;
        super::handoff::close(store.as_ref(), service).await;
        let third = activated_in(store.as_ref(), session).await;
        assert!(matches!(
            store
                .register_session_service_dependency(RegisterSessionServiceDependency {
                    grant: third,
                    generation: descriptor
                })
                .await
                .unwrap(),
            SessionServiceDependencyOutcome::Applied { .. }
        ));
    }
}

pub(crate) async fn source_fixture(
    store: &dyn Store,
) -> (
    RegisterSessionServiceDependency,
    crate::session_owner::service::SessionServiceDependencyReceipt,
    PreparationGrant,
) {
    let request = RegisterSessionServiceDependency {
        grant: super::activated_setup_on(store).await,
        generation: generation(),
    };
    let SessionServiceDependencyOutcome::Applied { receipt, .. } = store
        .register_session_service_dependency(request.clone())
        .await
        .unwrap()
    else {
        panic!("fresh source")
    };
    let session = request.grant.owner_authority().session_id().clone();
    end_root(store, &request.grant).await;
    let next = super::activated_in(store, session).await;
    (request, *receipt, next)
}
pub(crate) async fn source_refuses(
    store: &dyn Store,
    request: RegisterSessionServiceDependency,
    receipt: crate::session_owner::service::SessionServiceDependencyReceipt,
    expected: SessionOwnerError,
    grant: PreparationGrant,
) {
    assert!(
        matches!(store.register_session_service_dependency(request.clone()).await.unwrap(),SessionServiceDependencyOutcome::Replayed(saved) if *saved==receipt)
    );
    let session = request.grant.owner_authority().session_id().clone();
    let mut generation = generation();
    generation.id = "replacement-after-corruption".into();
    let request = RegisterSessionServiceDependency { grant, generation };
    let id = request.source().unwrap().identities().unwrap().reserve;
    let result = store.register_session_service_dependency(request).await;
    assert!(
        matches!(&result, Err(error) if *error == expected),
        "expected {expected:?}, got {result:?}"
    );
    assert!(
        store
            .lookup_session_owner(&session, &id)
            .await
            .unwrap()
            .is_none()
    );
}

/// A later root captures the real session revision after a genuine service cancellation.
#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub(crate) async fn cancelled_across_roots(
    store: &dyn Store,
) -> (
    crate::session_owner::service::SessionServiceDependencyAccess,
    super::handoff::Service,
    SessionOwnerOperationId,
) {
    let session = SessionId::new();
    let binding = crate::session_owner_contract::reservation(session.clone()).binding;
    store
        .manage_session(crate::ManageSession {
            id: crabber_core::ControlId::new(),
            session_id: session.clone(),
            expected_revision: crabber_core::SessionRevision::INITIAL,
            change: crate::SessionChange::Create {
                workspace_id: binding.workspace_id,
                directory: binding.directory,
                title: "service cancellation provenance".into(),
                defaults: crate::SessionDefaults::default(),
            },
        })
        .await
        .unwrap();
    let original = super::activated_in(store, session.clone()).await;
    let service = super::handoff::enable(store, original.clone()).await;
    let cancel = SessionOwnerOperationId::new();
    store
        .request_session_owner_cancel(crate::session_owner::RequestSessionOwnerCancel {
            id: cancel.clone(),
            session_id: session.clone(),
            target: service.authority.target().clone(),
            expected_revision: Some(store.session_revision(&session).await.unwrap()),
            claim: SessionOwnerResolver {
                resolver_id: "host".into(),
                authority_claim: "stop original dependency".into(),
            },
        })
        .await
        .unwrap();
    end_root(store, &original).await;
    let next = super::activated_in(store, session).await;
    let access = crate::session_owner::service::SessionServiceDependencyAccess {
        grant: next,
        source: crate::session_owner::service::SessionServiceLink::for_grant(
            &original,
            generation(),
        )
        .unwrap(),
        authority: service.authority.clone(),
    };
    (access, service, cancel)
}
