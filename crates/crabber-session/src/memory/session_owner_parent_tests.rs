use super::MemoryStore;
use crate::{
    PhaseCommand, PhaseMutation, PhaseStart, PhaseState, PhaseWriteAuthority, Store,
    session_owner::{
        MutateSessionOwner, ReserveSessionOwner, SessionOwnerCommand, SessionOwnerError as Error,
        SessionOwnerKind, SessionOwnerOutcome, SessionOwnerParent, SessionOwnerParentAuthority,
        SessionOwnerTermination,
    },
};
use crabber_core::{ControlId, ManualClock, SessionOwnerOperationId, ToolCallStatus};
use std::sync::Arc;
use time::OffsetDateTime;
#[tokio::test]
async fn detached_session_owner_requires_actual_parent_begin_source() {
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let store = MemoryStore::with_clock(clock.clone());
    let (admitted, execution, owner, claim) = crate::phase_contract::fixture(&store).await;
    let (receipt, grant) = crate::phase_contract::applied(
        execution
            .mutate_phase(claim, Some(PhaseWriteAuthority::Attempt(owner)))
            .await
            .unwrap(),
    );
    let grant = grant.unwrap();
    let mut request = crate::session_owner_contract::reservation(admitted.session.id.clone());
    request
        .binding
        .workspace_id
        .clone_from(&admitted.session.workspace_id);
    request
        .binding
        .directory
        .clone_from(&admitted.session.directory);
    request.binding.kind = SessionOwnerKind::DetachedInvocation;
    request.binding.parent = Some(SessionOwnerParent {
        invocation: None,
        run_id: admitted.run.id.clone(),
        call_id: Some(receipt.phase.target.call_id.clone()),
        phase: Some(receipt.phase.target.clone()),
    });
    let parent = SessionOwnerParentAuthority::Phase(grant.clone());
    assert!(matches!(
        store
            .reserve_session_owner(request.clone(), Some(parent.clone()))
            .await,
        Err(Error::StaleOwner)
    ));
    forge_labels(&store, &receipt.phase.target, true);
    assert!(matches!(
        store
            .reserve_session_owner(request.clone(), Some(parent.clone()))
            .await,
        Err(Error::UnknownStoreOutcome)
    ));
    assert_eq!(
        store
            .lookup_session_owner(&admitted.session.id, &request.id)
            .await
            .unwrap(),
        None
    );
    forge_labels(&store, &receipt.phase.target, false);
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
            Some(grant),
        )
        .await
        .unwrap();
    reject_wrong_parent_route(&store, &request, &parent, &admitted.run).await;
    reject_missing_session(&store, &request, &parent).await;

    let SessionOwnerOutcome::Applied {
        receipt: reserved,
        authority,
    } = store
        .reserve_session_owner(request.clone(), Some(parent.clone()))
        .await
        .unwrap()
    else {
        panic!("fresh detached owner")
    };
    let authority = authority.unwrap();
    store
        .mutate_session_owner(
            MutateSessionOwner {
                id: SessionOwnerOperationId::new(),
                session_id: admitted.session.id.clone(),
                target: reserved.owner.target.clone(),
                command: SessionOwnerCommand::Begin,
            },
            Some(authority.clone()),
        )
        .await
        .unwrap();
    clock.set(admitted.run.lease_until + time::Duration::seconds(1));
    let mut stale = request.clone();
    stale.id = SessionOwnerOperationId::new();
    stale.binding.id = crabber_core::SessionOwnerId::new();
    assert!(matches!(
        store.reserve_session_owner(stale, Some(parent)).await,
        Err(Error::StaleOwner)
    ));
    independent_join(&store, &request, reserved, authority).await;
}
fn forge_labels(store: &MemoryStore, target: &crate::PhaseTarget, started: bool) {
    let mut state = store.state.lock().unwrap();
    state.phases.get_mut(&target.id).unwrap().snapshot.state = if started {
        PhaseState::Started
    } else {
        PhaseState::Claimed
    };
    state.calls.get_mut(&target.call_id).unwrap().status = if started {
        ToolCallStatus::Running
    } else {
        ToolCallStatus::Pending
    };
}
async fn independent_join(
    store: &MemoryStore,
    request: &ReserveSessionOwner,
    reserved: crate::session_owner::SessionOwnerReceipt,
    authority: crate::session_owner::SessionOwnerAuthority,
) {
    let joined = store
        .mutate_session_owner(
            MutateSessionOwner {
                id: SessionOwnerOperationId::new(),
                session_id: request.binding.session_id.clone(),
                target: reserved.owner.target.clone(),
                command: SessionOwnerCommand::Join {
                    termination: SessionOwnerTermination::Returned,
                },
            },
            Some(authority),
        )
        .await
        .unwrap();
    assert!(matches!(
        joined,
        SessionOwnerOutcome::Applied {
            authority: None,
            ..
        }
    ));
    assert!(
        matches!(store.reserve_session_owner(request.clone(), None).await.unwrap(), SessionOwnerOutcome::Replayed(receipt) if receipt == reserved)
    );
}

async fn reject_wrong_parent_route(
    store: &MemoryStore,
    request: &ReserveSessionOwner,
    parent: &SessionOwnerParentAuthority,
    run: &crabber_core::Run,
) {
    let wrong_key = crabber_core::RunId::new();
    store
        .state
        .lock()
        .unwrap()
        .runs
        .insert(wrong_key.clone(), run.clone());
    let mut mismatched = request.clone();
    mismatched.binding.parent.as_mut().unwrap().run_id = wrong_key.clone();
    assert!(matches!(
        store
            .reserve_session_owner(mismatched, Some(parent.clone()))
            .await,
        Err(Error::UnknownStoreOutcome)
    ));
    store.state.lock().unwrap().runs.remove(&wrong_key);
}

#[tokio::test]
async fn detached_owner_refuses_restored_parent_after_real_run_only_takeover() {
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let store = MemoryStore::with_clock(clock.clone());
    let (admitted, _execution, parent, _claim) = crate::phase_contract::fixture(&store).await;
    let mut original = store.get_run(&admitted.run.id).await.unwrap().unwrap();
    clock.set(original.lease_until + time::Duration::hours(1));
    store
        .claim_expired_run(&original.id, "replacement")
        .await
        .unwrap();
    original.lease_until += time::Duration::hours(2);
    {
        let mut state = store.state.lock().unwrap();
        state.runs.insert(original.id.clone(), original.clone());
        for attempt in state
            .attempts
            .values_mut()
            .filter(|a| a.snapshot.run_id == original.id)
        {
            attempt.lease_until = original.lease_until;
        }
    }
    let mut request = crate::session_owner_contract::reservation(original.session_id.clone());
    request.binding.workspace_id = admitted.session.workspace_id;
    request.binding.directory = admitted.session.directory;
    request.binding.kind = SessionOwnerKind::DetachedInvocation;
    request.binding.parent = Some(SessionOwnerParent {
        invocation: None,
        run_id: original.id,
        call_id: None,
        phase: None,
    });
    assert!(matches!(
        store
            .reserve_session_owner(
                request.clone(),
                Some(SessionOwnerParentAuthority::Attempt(parent))
            )
            .await,
        Err(Error::UnknownStoreOutcome)
    ));
    assert_eq!(
        store
            .lookup_session_owner(&original.session_id, &request.id)
            .await
            .unwrap(),
        None
    );
}

async fn reject_missing_session(
    store: &MemoryStore,
    request: &ReserveSessionOwner,
    parent: &SessionOwnerParentAuthority,
) {
    let missing = store
        .state
        .lock()
        .unwrap()
        .sessions
        .remove(&request.binding.session_id)
        .unwrap();
    assert!(matches!(
        store
            .reserve_session_owner(request.clone(), Some(parent.clone()))
            .await,
        Err(Error::UnknownStoreOutcome)
    ));
    assert_eq!(
        store
            .lookup_session_owner(&request.binding.session_id, &request.id)
            .await
            .unwrap(),
        None
    );
    store
        .state
        .lock()
        .unwrap()
        .sessions
        .insert(missing.id.clone(), missing);
}
