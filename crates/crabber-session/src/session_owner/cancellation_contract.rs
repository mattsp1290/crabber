//! Shared native cancellation conformance, independent of supervisor adoption.
use super::*;
use crate::{ManageSession, SessionChange, SessionDefaults, Store};
use crabber_core::{
    Clock, ControlId, ManualClock, SessionId, SessionOwnerOperationId, SessionRevision,
};
#[path = "cancellation_contract/faults.rs"]
pub(crate) mod faults;
#[path = "cancellation_contract/lifecycle.rs"]
mod lifecycle;
#[path = "cancellation_contract/races.rs"]
mod races;
pub(crate) fn applied(outcome: SessionOwnerOutcome) -> SessionOwnerReceipt {
    let SessionOwnerOutcome::Applied {
        receipt,
        authority: None,
    } = outcome
    else {
        panic!("cancellation has metadata only")
    };
    receipt
}
pub(crate) fn request(
    owner: &SessionOwnerSnapshot,
    revision: Option<SessionRevision>,
) -> RequestSessionOwnerCancel {
    RequestSessionOwnerCancel {
        id: SessionOwnerOperationId::new(),
        session_id: owner.binding.session_id.clone(),
        target: owner.target.clone(),
        expected_revision: revision,
        claim: SessionOwnerResolver {
            resolver_id: "cancel-controller".into(),
            authority_claim: "authenticated cancellation assertion".into(),
        },
    }
}
pub(crate) fn observe(request: &RequestSessionOwnerCancel) -> ObserveSessionOwnerCancel {
    ObserveSessionOwnerCancel {
        id: SessionOwnerOperationId::new(),
        session_id: request.session_id.clone(),
        target: request.target.clone(),
        request_id: request.id.clone(),
    }
}
pub(crate) async fn reserve(store: &impl Store) -> (SessionOwnerSnapshot, SessionOwnerAuthority) {
    let request = crate::session_owner_contract::reservation(SessionId::new());
    let SessionOwnerOutcome::Applied {
        receipt,
        authority: Some(authority),
    } = store.reserve_session_owner(request, None).await.unwrap()
    else {
        panic!("reserve")
    };
    (receipt.owner, authority)
}
pub(crate) async fn create(store: &impl Store, owner: &SessionOwnerSnapshot) {
    store
        .manage_session(ManageSession {
            id: ControlId::new(),
            session_id: owner.binding.session_id.clone(),
            expected_revision: SessionRevision::INITIAL,
            change: SessionChange::Create {
                workspace_id: owner.binding.workspace_id.clone(),
                directory: owner.binding.directory.clone(),
                title: "cancel fixture".into(),
                defaults: SessionDefaults::default(),
            },
        })
        .await
        .unwrap();
}
pub(crate) fn mutation(
    owner: &SessionOwnerSnapshot,
    command: SessionOwnerCommand,
) -> MutateSessionOwner {
    MutateSessionOwner {
        id: SessionOwnerOperationId::new(),
        session_id: owner.binding.session_id.clone(),
        target: owner.target.clone(),
        command,
    }
}
pub(crate) async fn run(store: &impl Store, clock: &ManualClock) {
    assert!(store.supports_session_owner_cancellation());
    lifecycle::absent(store, clock).await;
    lifecycle::existing(store, clock).await;
    lifecycle::stale(store, clock).await;
    races::run(store).await;
    super::preparation_contract::cancellation::run(store).await;
}

pub(crate) async fn source_fixture(
    store: &impl Store,
) -> (
    SessionOwnerSnapshot,
    SessionOwnerAuthority,
    RequestSessionOwnerCancel,
    ObserveSessionOwnerCancel,
) {
    let (owner, authority) = reserve(store).await;
    let cancel = request(&owner, None);
    store
        .request_session_owner_cancel(cancel.clone())
        .await
        .unwrap();
    store
        .mutate_session_owner(
            mutation(&owner, SessionOwnerCommand::Begin),
            Some(authority.clone()),
        )
        .await
        .unwrap();
    let observation = observe(&cancel);
    store
        .observe_session_owner_cancel(observation.clone(), authority.clone())
        .await
        .unwrap();
    store
        .mutate_session_owner(
            mutation(&owner, SessionOwnerCommand::Renew { lease_ms: 30_000 }),
            Some(authority.clone()),
        )
        .await
        .unwrap();
    (owner, authority, cancel, observation)
}
