//! The exact service publications contribute to a real prepared native commit.
use crate::{
    Store,
    session_owner::{
        SessionOwnerAuthority,
        owned_scope::{
            OwnedCleanupTask, OwnedScopeActivationOutcome, OwnedScopeParentAuthority,
            OwnedScopeSource, OwnedTaskRegistration,
        },
        preparation::{
            JoinedPreparation, PreparationGrant, PreparationResourceOutcome, PreparedOperation,
        },
        service::{RegisterSessionServiceDependency, SessionServiceDependencyOutcome},
    },
};
use std::sync::Arc;
pub(crate) struct Service {
    task: OwnedTaskRegistration<()>,
    close: Arc<tokio::sync::Notify>,
    pub(crate) authority: SessionOwnerAuthority,
}
pub(crate) async fn enable(store: &dyn Store, grant: PreparationGrant) -> Service {
    let request = RegisterSessionServiceDependency {
        grant: grant.clone(),
        generation: super::generation(),
    };
    let SessionServiceDependencyOutcome::Applied { receipt, authority } = store
        .register_session_service_dependency(request)
        .await
        .unwrap()
    else {
        panic!("fresh service")
    };
    let close = Arc::new(tokio::sync::Notify::new());
    let stop = close.clone();
    let entered = Arc::new(tokio::sync::Notify::new());
    let started = entered.clone();
    let mut task = OwnedTaskRegistration::register(
        OwnedScopeSource::from_receipts(&receipt.reserved, &receipt.began).unwrap(),
        move || async move {
            started.notify_one();
            stop.notified().await;
        },
    )
    .unwrap();
    let OwnedScopeActivationOutcome::Applied { permit, .. } = store
        .activate_owned_scope(
            task.activation().clone(),
            authority.clone(),
            Some(OwnedScopeParentAuthority::ServicePreparation(Box::new(
                grant,
            ))),
        )
        .await
        .unwrap()
    else {
        panic!("fresh activation")
    };
    task.dispatch(*permit).unwrap();
    entered.notified().await;
    Service {
        task,
        close,
        authority,
    }
}
pub(crate) async fn close(store: &dyn Store, mut service: Service) {
    service.close.notify_one();
    if let Some(cancel) = store
        .pending_session_owner_cancel(&service.authority)
        .await
        .unwrap()
    {
        store
            .observe_session_owner_cancel(
                crate::session_owner::ObserveSessionOwnerCancel {
                    id: crabber_core::SessionOwnerOperationId::new(),
                    session_id: service.authority.session_id().clone(),
                    target: service.authority.target().clone(),
                    request_id: cancel.request_id,
                },
                service.authority.clone(),
            )
            .await
            .unwrap();
    }
    let mut cleanup = OwnedCleanupTask::from_host_task(
        service.task.activation().clone(),
        tokio::spawn(async { Ok(()) }),
    );
    let proof = service
        .task
        .join()
        .await
        .unwrap()
        .observe_resources(&mut cleanup)
        .await
        .unwrap();
    store
        .join_owned_scope(proof, service.authority)
        .await
        .unwrap();
}
async fn cancel_service(store: &dyn Store, authority: &SessionOwnerAuthority) {
    store
        .request_session_owner_cancel(crate::session_owner::RequestSessionOwnerCancel {
            id: crabber_core::SessionOwnerOperationId::new(),
            session_id: authority.session_id().clone(),
            target: authority.target().clone(),
            expected_revision: revision(store, authority.session_id()).await,
            claim: crate::session_owner::SessionOwnerResolver {
                resolver_id: "host".into(),
                authority_claim: "cancel this exact session dependency".into(),
            },
        })
        .await
        .unwrap();
}
pub(crate) async fn run<S: Store + Clone + 'static>(store: Arc<S>) {
    let _ = run_sessions(store, true).await;
}
pub(crate) async fn run_sessions<S: Store + Clone + 'static>(
    store: Arc<S>,
    existing: bool,
) -> Vec<crabber_core::SessionId> {
    let mut completed_sessions = Vec::new();
    for cancel in [false, true] {
        let (original, bound) =
            crate::session_owner::preparation_contract::resources::declaration::setup_session(
                store.as_ref(),
                existing,
            )
            .await;
        let session = bound.binding.request.session_id.clone();
        let key = bound.keyed.options.key.clone();
        let PreparationResourceOutcome::Applied { receipt, grant } =
            store.retain_preparation_resources(original).await.unwrap()
        else {
            panic!("fresh child")
        };
        let grant = *grant;
        let child = grant.retained_resources_authority().unwrap().clone();
        let source = OwnedScopeSource::from_receipts(&receipt.reserved, &receipt.began).unwrap();
        let worker = store.clone();
        let captured = grant.clone();
        let mut task = OwnedTaskRegistration::register(source, move || async move {
            let service = enable(worker.as_ref(), captured).await;
            if cancel {
                cancel_service(worker.as_ref(), &service.authority).await;
            }
            Ok::<_, ()>((service, PreparedOperation::AdmitKeyed(Box::new(bound))))
        })
        .unwrap();
        let OwnedScopeActivationOutcome::Applied { permit, .. } = store
            .activate_owned_scope(
                task.activation().clone(),
                child.clone(),
                Some(OwnedScopeParentAuthority::preparation(grant.clone())),
            )
            .await
            .unwrap()
        else {
            panic!("fresh parent activation")
        };
        task.dispatch(*permit).unwrap();
        let (service, prepared) = JoinedPreparation::observe_registered(grant, &mut task)
            .await
            .unwrap()
            .unwrap();
        let before = revision(store.as_ref(), &session).await;
        let completed = store.complete_preparation(prepared.clone()).await;
        if cancel {
            assert!(completed.is_err());
            assert_eq!(revision(store.as_ref(), &session).await, before);
            assert!(
                store
                    .lookup_admission(&session, &key)
                    .await
                    .unwrap()
                    .is_none()
            );
        } else {
            assert!(!completed.unwrap().is_replayed());
            assert!(
                store
                    .lookup_admission(&session, &key)
                    .await
                    .unwrap()
                    .is_some()
            );
            assert!(
                store
                    .complete_preparation(prepared)
                    .await
                    .unwrap()
                    .is_replayed()
            );
        }
        close(store.as_ref(), service).await;
        let mut cleanup = OwnedCleanupTask::from_host_task(
            task.activation().clone(),
            tokio::spawn(async { Ok(()) }),
        );
        let proof = task
            .join()
            .await
            .unwrap()
            .observe_resources(&mut cleanup)
            .await
            .unwrap();
        store.join_owned_scope(proof, child).await.unwrap();
        if !cancel {
            completed_sessions.push(session);
        }
    }
    completed_sessions
}

async fn revision(
    store: &dyn Store,
    session: &crabber_core::SessionId,
) -> Option<crabber_core::SessionRevision> {
    match store.session_revision(session).await {
        Ok(revision) => Some(revision),
        Err(crate::StoreError::NotFound) => None,
        Err(error) => panic!("session revision source: {error:?}"),
    }
}
