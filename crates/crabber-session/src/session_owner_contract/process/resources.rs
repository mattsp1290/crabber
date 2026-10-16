//! A killed controller never turns linked resource metadata into authority.
use super::*;
use crate::session_owner::preparation::{
    OWNED_PREPARATION_COMPONENT, PreparationGrant, PreparationKey, PreparationPurpose,
    PreparationResourceLink, ReservePreparation,
};
pub(super) async fn retain(store: &impl Store, request: ReservePreparation) {
    let (reserved, authority) = applied(store.reserve_preparation(request.clone()).await.unwrap());
    let authority = authority.unwrap();
    store
        .mutate_session_owner(
            MutateSessionOwner {
                id: request.reconciliation().unwrap().begin_id,
                session_id: request.owner.binding.session_id.clone(),
                target: reserved.owner.target,
                command: SessionOwnerCommand::Begin,
            },
            Some(authority.clone()),
        )
        .await
        .unwrap();
    store
        .retain_preparation_resources(
            PreparationGrant::from_fresh_begin(request, authority).unwrap(),
        )
        .await
        .unwrap();
}
pub(super) async fn verify(store: &impl Store, request: ReservePreparation, committed: bool) {
    let metadata = request.reconciliation().unwrap();
    let session = &metadata.session_id;
    let link = PreparationResourceLink {
        session_id: session.clone(),
        root: metadata.owner,
        root_reserve: metadata.reserve_id,
        root_begin: metadata.begin_id,
        root_digest: request.fingerprint().unwrap(),
    };
    let ids = link.identities().unwrap();
    let reserved = store
        .lookup_session_owner(session, &ids.reserve)
        .await
        .unwrap();
    let began = store
        .lookup_session_owner(session, &ids.begin)
        .await
        .unwrap();
    assert_eq!(reserved.is_some(), committed);
    assert_eq!(began.is_some(), committed);
    assert_eq!(
        store
            .get_session_owner(session, &link.root.id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Active
    );
    if committed {
        let child = began.unwrap().owner;
        assert_eq!(child.state, SessionOwnerState::Active);
        assert!(matches!(
            store
                .mutate_session_owner(
                    MutateSessionOwner {
                        id: SessionOwnerOperationId::new(),
                        session_id: session.clone(),
                        target: child.target,
                        command: SessionOwnerCommand::Begin
                    },
                    None
                )
                .await,
            Err(SessionOwnerError::StaleOwner)
        ));
    }
    assert!(matches!(
        store.reserve_preparation(request).await.unwrap(),
        SessionOwnerOutcome::Replayed(_)
    ));
    assert!(store.get_session(session).await.unwrap().is_none());
}
pub(super) async fn run(helper: &str, path: Option<&Path>) {
    for committed in [false, true] {
        let mut request = ReservePreparation {
            owner: reservation(SessionId::new()),
            source: None,
            key: PreparationKey::Unkeyed(crabber_core::ControlId::new()),
            expected_revision: crabber_core::SessionRevision::INITIAL,
            purpose: PreparationPurpose::OrdinaryAdmission,
            intent: crabber_core::InputFingerprint::new("b".repeat(64)).unwrap(),
        };
        request.owner.binding.component_id = OWNED_PREPARATION_COMPONENT.into();
        let key = request.key.clone();
        let request = request.bind_key(key).unwrap();
        let directory = tempfile::tempdir().unwrap();
        let marker_path = directory.path().join("resource-commit");
        let mut child = spawn(
            helper,
            &Operation::Resources(request.clone()),
            path,
            Some(if committed {
                "resources-after"
            } else {
                "resources-before"
            }),
            &marker_path,
        );
        marker(&marker_path).await;
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        let mut cold = spawn(
            helper,
            &Operation::VerifyResources { request, committed },
            path,
            None,
            &marker_path,
        );
        joined(&mut cold).await;
    }
}
