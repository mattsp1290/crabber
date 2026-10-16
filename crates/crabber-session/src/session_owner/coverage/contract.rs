//! Actual native-parent and physical-resource fixtures shared by all backends.
use crate::Store;
use crate::session_owner::{
    MutateSessionOwner, ReserveSessionOwner, SessionOwnerAuthority, SessionOwnerCommand,
    SessionOwnerKind, SessionOwnerOutcome, SessionOwnerParent, SessionOwnerParentAuthority,
    SessionOwnerTermination,
    owned_scope::{
        OwnedCleanupFailure, OwnedCleanupTask, OwnedScopeActivationOutcome,
        OwnedScopeParentAuthority, OwnedScopeSource, OwnedTaskRegistration,
        RUNTIME_DETACHED_RESOURCE_CONTRACT_V1,
    },
};
use crabber_core::{InputFingerprint, SessionId, SessionOwnerId, SessionOwnerOperationId};

#[derive(Clone, Copy)]
pub(crate) enum Mode {
    Declared,
    Legacy,
    Claim,
    NoActivation,
    ActivatedNoDispatch,
}
pub(crate) async fn fixture(store: &impl Store, mode: Mode) -> (SessionId, SessionOwnerId) {
    let (request, parent) = native_parent(store).await;
    fixture_from_parent(store, mode, request, parent).await
}
pub(crate) async fn fixture_from_parent(
    store: &impl Store,
    mode: Mode,
    mut request: ReserveSessionOwner,
    parent: SessionOwnerParentAuthority,
) -> (SessionId, SessionOwnerId) {
    request.lease_ms = 60_000;
    request.binding.resource_contract = match mode {
        Mode::Legacy => None,
        _ => Some(InputFingerprint::new(RUNTIME_DETACHED_RESOURCE_CONTRACT_V1).unwrap()),
    };
    let session = request.binding.session_id.clone();
    let id = request.binding.id.clone();
    let SessionOwnerOutcome::Applied {
        receipt: reserved,
        authority: Some(authority),
    } = store
        .reserve_session_owner(request, Some(parent.clone()))
        .await
        .unwrap()
    else {
        panic!("actual original reserve")
    };
    let SessionOwnerOutcome::Applied { receipt: began, .. } = store
        .mutate_session_owner(
            MutateSessionOwner {
                id: SessionOwnerOperationId::new(),
                session_id: session.clone(),
                target: authority.target().clone(),
                command: SessionOwnerCommand::Begin,
            },
            Some(authority.clone()),
        )
        .await
        .unwrap()
    else {
        panic!("actual original Begin")
    };
    if matches!(mode, Mode::Claim) {
        tokio::spawn(async {}).await.unwrap();
        store
            .mutate_session_owner(
                MutateSessionOwner {
                    id: SessionOwnerOperationId::new(),
                    session_id: session.clone(),
                    target: authority.target().clone(),
                    command: SessionOwnerCommand::Join {
                        termination: SessionOwnerTermination::Returned,
                    },
                },
                Some(authority),
            )
            .await
            .unwrap();
    } else {
        observed_join(
            store,
            authority,
            parent,
            OwnedScopeSource::from_receipts(&reserved, &began).unwrap(),
            mode,
        )
        .await;
    }
    (session, id)
}

async fn observed_join(
    store: &impl Store,
    authority: SessionOwnerAuthority,
    parent: SessionOwnerParentAuthority,
    source: OwnedScopeSource,
    mode: Mode,
) {
    let resources = crabber::extension::ToolResourceOwner::new();
    let scope = resources.clone();
    let mut task = OwnedTaskRegistration::register(source, move || async move {
        scope.resources().spawn(async { Ok(()) }).unwrap();
        42
    })
    .unwrap();
    if !matches!(mode, Mode::NoActivation) {
        let OwnedScopeActivationOutcome::Applied { permit, .. } = store
            .activate_owned_scope(
                task.activation().clone(),
                authority.clone(),
                Some(OwnedScopeParentAuthority::Native(parent)),
            )
            .await
            .unwrap()
        else {
            panic!("fresh exact dispatch")
        };
        if !matches!(mode, Mode::ActivatedNoDispatch) {
            task.dispatch(*permit).unwrap();
        }
    }
    let activation = task.activation().clone();
    let observed = task.join().await.unwrap();
    let mut cleanup = OwnedCleanupTask::from_host_task(
        activation,
        tokio::spawn(async move {
            let report = resources.drain(std::time::Duration::from_secs(1)).await;
            if report.sealed && report.pending == 0 && report.failures.is_empty() {
                Ok(())
            } else {
                Err(OwnedCleanupFailure::Failed)
            }
        }),
    );
    let proof = observed.observe_resources(&mut cleanup).await.unwrap();
    store.join_owned_scope(proof, authority).await.unwrap();
}

pub(crate) async fn native_parent(
    store: &impl Store,
) -> (ReserveSessionOwner, SessionOwnerParentAuthority) {
    let (admitted, execution, attempt, claim) = crate::phase_contract::fixture(store).await;
    let (receipt, authority) = crate::phase_contract::applied(
        execution
            .mutate_phase(claim, Some(crate::PhaseWriteAuthority::Attempt(attempt)))
            .await
            .unwrap(),
    );
    let authority = authority.unwrap();
    execution
        .start_tool_phase(
            crate::PhaseStart {
                begin: crate::phase_contract::mutation(
                    store.session_revision(&admitted.session.id).await.unwrap(),
                    crate::PhaseCommand::Begin {
                        target: receipt.phase.target.clone(),
                    },
                ),
                turn_id: None,
                created_at: admitted.run.created_at,
            },
            Some(authority.clone()),
        )
        .await
        .unwrap();
    let mut request = crate::session_owner_contract::reservation(admitted.session.id);
    request.binding.workspace_id = admitted.session.workspace_id;
    request.binding.directory = admitted.session.directory;
    request.binding.kind = SessionOwnerKind::DetachedInvocation;
    request.binding.parent = Some(SessionOwnerParent {
        run_id: admitted.run.id,
        call_id: Some(receipt.phase.target.call_id.clone()),
        phase: Some(receipt.phase.target),
    });
    (request, SessionOwnerParentAuthority::Phase(authority))
}
