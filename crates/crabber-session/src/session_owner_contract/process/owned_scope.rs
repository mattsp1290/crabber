//! Physical death never delivers a local dispatch permit to another controller.
use super::*;
use crate::session_owner::owned_scope::{
    OwnedCleanupTask, OwnedScopeActivation, OwnedScopeActivationOutcome, OwnedScopeSource,
    OwnedTaskRegistration,
};
use crabber_test_support::ChildDatabase;
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Metadata {
    pub activation: OwnedScopeActivation,
    pub join: Option<SessionOwnerOperationId>,
}
fn sidecar(marker: &Path) -> std::path::PathBuf {
    marker.with_extension("scope")
}
pub(super) async fn act(store: &dyn Store, reserve: ReserveSessionOwner, joining: bool) {
    let (reserved, authority) = applied(store.reserve_session_owner(reserve, None).await.unwrap());
    let authority = authority.unwrap();
    let command = mutation(&reserved.owner, SessionOwnerCommand::Begin);
    let (began, _) = applied(
        store
            .mutate_session_owner(command, Some(authority.clone()))
            .await
            .unwrap(),
    );
    let marker = std::path::PathBuf::from(std::env::var("CRABBER_OWNER_MARKER").unwrap());
    let effect = marker.with_extension("effect");
    let mut task = OwnedTaskRegistration::register(
        OwnedScopeSource::from_receipts(&reserved, &began).unwrap(),
        move || {
            std::fs::write(effect, b"unexpected dispatch").unwrap();
            async {}
        },
    )
    .unwrap();
    let activation = task.activation().clone();
    std::fs::write(
        sidecar(&marker),
        serde_json::to_vec(&Metadata {
            activation: activation.clone(),
            join: None,
        })
        .unwrap(),
    )
    .unwrap();
    let OwnedScopeActivationOutcome::Applied { permit, .. } = store
        .activate_owned_scope(activation.clone(), authority.clone(), None)
        .await
        .unwrap()
    else {
        panic!("fresh activation")
    };
    // A permit is never used: the test process can die inside either commit,
    // without a future controller inheriting the ability to invoke this FnOnce.
    drop(permit);
    if joining {
        let observed = task.join().await.unwrap();
        let mut cleanup =
            OwnedCleanupTask::from_host_task(activation.clone(), tokio::spawn(async { Ok(()) }));
        let proof = observed.observe_resources(&mut cleanup).await.unwrap();
        let join = proof.request().unwrap().id;
        std::fs::write(
            sidecar(&marker),
            serde_json::to_vec(&Metadata {
                activation,
                join: Some(join),
            })
            .unwrap(),
        )
        .unwrap();
        store.join_owned_scope(proof, authority).await.unwrap();
    }
}
pub(super) async fn verify(store: &dyn Store, metadata: Metadata, committed: bool) {
    let activation = metadata.activation;
    let session = activation.session_id();
    let key = metadata.join.as_ref().unwrap_or(activation.operation_id());
    assert_eq!(
        store
            .lookup_session_owner(session, key)
            .await
            .unwrap()
            .is_some(),
        committed
    );
    let owner = store
        .get_session_owner(session, &activation.target().id)
        .await
        .unwrap();
    assert_eq!(
        owner.state,
        if metadata.join.is_some() && committed {
            SessionOwnerState::Joined
        } else {
            SessionOwnerState::Active
        }
    );
    if metadata.join.is_some() || committed {
        // Wrong private token deliberately proves original metadata comes first.
        // Its Replayed variant has no permit and cannot invoke any local task.
        let wrong = SessionOwnerAuthority::from_store(
            session.clone(),
            activation.target().clone(),
            "wrong-token".into(),
        )
        .unwrap();
        assert!(matches!(
            store
                .activate_owned_scope(activation.clone(), wrong, None)
                .await
                .unwrap(),
            OwnedScopeActivationOutcome::Replayed(_)
        ));
    }
    assert!(store.get_session(session).await.unwrap().is_none());
}
pub(super) async fn run(helper: &str, path: ChildDatabase<'_>) {
    for joining in [false, true] {
        for committed in [false, true] {
            let mut request = reservation(SessionId::new());
            request.binding.kind = SessionOwnerKind::RetainedCapacity;
            let directory = tempfile::tempdir().unwrap();
            let marker_path = directory.path().join("scope-commit");
            let point = match (joining, committed) {
                (false, false) => "scope-before",
                (false, true) => "scope-after",
                (true, false) => "scope-join-before",
                (true, true) => "scope-join-after",
            };
            let mut child = spawn(
                helper,
                &Operation::OwnedScope { request, joining },
                path,
                Some(point),
                &marker_path,
            );
            marker(&marker_path).await;
            child.0.kill().unwrap();
            child.0.wait().unwrap();
            assert!(!marker_path.with_extension("effect").exists());
            let metadata: Metadata =
                serde_json::from_slice(&std::fs::read(sidecar(&marker_path)).unwrap()).unwrap();
            let mut cold = spawn(
                helper,
                &Operation::VerifyOwnedScope {
                    metadata,
                    committed,
                },
                path,
                None,
                &marker_path,
            );
            joined(&mut cold).await;
            assert!(!marker_path.with_extension("effect").exists());
        }
    }
}
