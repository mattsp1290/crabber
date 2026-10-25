//! Actual native Begin provenance for detached ownership handoff.
use super::*;
#[path = "parent/owned_scope.rs"]
mod owned_scope;
use crate::{
    PhaseCommand, PhaseMutation, PhaseStart, PhaseWriteAuthority,
    session_owner::{SessionOwnerParent, SessionOwnerParentAuthority},
};
use crabber_core::ToolCallStatus;
pub(crate) async fn fixture(
    store: &dyn Store,
) -> (
    ReserveSessionOwner,
    SessionOwnerParentAuthority,
    crate::AdmitOutcome,
    Box<dyn crate::ExecutionStore>,
) {
    let (admitted, execution, attempt, claim) = crate::phase_contract::fixture(store).await;
    let (receipt, authority) = crate::phase_contract::applied(
        execution
            .mutate_phase(claim, Some(PhaseWriteAuthority::Attempt(attempt.clone())))
            .await
            .unwrap(),
    );
    let authority = authority.unwrap();
    let mut request = reservation(admitted.session.id.clone());
    request
        .binding
        .workspace_id
        .clone_from(&admitted.session.workspace_id);
    request
        .binding
        .directory
        .clone_from(&admitted.session.directory);
    request.binding.kind = SessionOwnerKind::DetachedInvocation;
    let mut attempt_request = request.clone();
    attempt_request.binding.parent = Some(SessionOwnerParent {
        invocation: None,
        run_id: admitted.run.id.clone(),
        call_id: None,
        phase: None,
    });
    applied(
        store
            .reserve_session_owner(
                attempt_request,
                Some(SessionOwnerParentAuthority::Attempt(attempt)),
            )
            .await
            .unwrap(),
    );
    request.id = SessionOwnerOperationId::new();
    request.binding.id = SessionOwnerId::new();

    request.binding.parent = Some(SessionOwnerParent {
        invocation: None,
        run_id: admitted.run.id.clone(),
        call_id: Some(receipt.phase.target.call_id.clone()),
        phase: Some(receipt.phase.target.clone()),
    });
    let parent = SessionOwnerParentAuthority::Phase(authority.clone());
    assert!(matches!(
        store
            .reserve_session_owner(request.clone(), Some(parent.clone()))
            .await,
        Err(SessionOwnerError::StaleOwner)
    ));
    execution
        .start_tool_phase(
            PhaseStart {
                begin: PhaseMutation {
                    id: ControlId::new(),
                    expected_revision: store.session_revision(&admitted.session.id).await.unwrap(),
                    command: PhaseCommand::Begin {
                        target: receipt.phase.target,
                    },
                },
                turn_id: None,
                created_at: admitted.run.created_at,
            },
            Some(authority),
        )
        .await
        .unwrap();
    (request, parent, admitted, execution)
}
pub(crate) async fn run(store: &dyn Store, clock: &ManualClock) {
    owned_scope::run(store, clock).await;
    let (request, parent, admitted, execution) = fixture(store).await;
    let (receipt, authority) = applied(
        store
            .reserve_session_owner(request.clone(), Some(parent.clone()))
            .await
            .unwrap(),
    );
    let authority = authority.unwrap();
    let before = store.session_revision(&admitted.session.id).await.unwrap();
    execution
        .renew_lease(admitted.run.lease_until + time::Duration::seconds(1))
        .await
        .unwrap();
    assert_eq!(
        store.session_revision(&admitted.session.id).await.unwrap(),
        before
    );
    let mut fresh = request.clone();
    fresh.id = SessionOwnerOperationId::new();
    fresh.binding.id = SessionOwnerId::new();
    applied(
        store
            .reserve_session_owner(fresh, Some(parent.clone()))
            .await
            .unwrap(),
    );
    clock.set(admitted.run.lease_until + time::Duration::seconds(2));
    let mut stale = request.clone();
    stale.id = SessionOwnerOperationId::new();
    stale.binding.id = SessionOwnerId::new();
    assert!(matches!(
        store.reserve_session_owner(stale, Some(parent)).await,
        Err(SessionOwnerError::StaleOwner)
    ));
    assert!(
        matches!(store.reserve_session_owner(request.clone(),None).await.unwrap(),SessionOwnerOutcome::Replayed(saved) if saved==receipt)
    );
    applied(
        store
            .mutate_session_owner(
                mutation(
                    &receipt.owner,
                    SessionOwnerCommand::Join {
                        termination: SessionOwnerTermination::Returned,
                    },
                ),
                Some(authority),
            )
            .await
            .unwrap(),
    );
    assert_eq!(
        store
            .list_unfinished_tool_calls(&admitted.run.id)
            .await
            .unwrap()[0]
            .status,
        ToolCallStatus::Running
    );
}
