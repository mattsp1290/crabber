//! Credentials-free owner-ledger conformance. This does not certify Runtime
//! adoption, resource cleanup or deletion readiness merely from an empty ledger.
use crate::session_owner::{
    MutateSessionOwner, ReserveSessionOwner, SessionOwnerAuthority, SessionOwnerBinding,
    SessionOwnerCommand, SessionOwnerError, SessionOwnerKind, SessionOwnerOutcome,
    SessionOwnerReceipt, SessionOwnerResolver, SessionOwnerSnapshot, SessionOwnerState,
    SessionOwnerTermination,
};
use crate::{ManageSession, SessionChange, SessionDefaults, Store};
use crabber_core::{
    ControlId, InputFingerprint, ManualClock, SessionId, SessionOwnerId, SessionOwnerOperationId,
    SessionRevision,
};
use std::sync::Arc;
use time::OffsetDateTime;
pub(crate) fn reservation(session: SessionId) -> ReserveSessionOwner {
    ReserveSessionOwner {
        id: SessionOwnerOperationId::new(),
        binding: SessionOwnerBinding {
            session_id: session,
            workspace_id: "workspace/所有者".into(),
            directory: "/admitted/opaque-root".into(),
            id: SessionOwnerId::new(),
            kind: SessionOwnerKind::Preparation,
            component_id: "host-preparation".into(),
            behavior_fingerprint: InputFingerprint::new("b".repeat(64)).unwrap(),
            resource_contract: None,
            parent: None,
        },
        owner: "controller-a".into(),
        lease_ms: 100,
    }
}
fn mutation(snapshot: &SessionOwnerSnapshot, command: SessionOwnerCommand) -> MutateSessionOwner {
    MutateSessionOwner {
        id: SessionOwnerOperationId::new(),
        session_id: snapshot.binding.session_id.clone(),
        target: snapshot.target.clone(),
        command,
    }
}
fn applied(outcome: SessionOwnerOutcome) -> (SessionOwnerReceipt, Option<SessionOwnerAuthority>) {
    let SessionOwnerOutcome::Applied { receipt, authority } = outcome else {
        panic!("fresh operation must apply")
    };
    (receipt, authority)
}
fn resolver() -> SessionOwnerResolver {
    SessionOwnerResolver {
        resolver_id: "host-supervisor".into(),
        authority_claim: "the exact owner's retained joins were observed after its lease expired"
            .into(),
    }
}
/// # Panics
/// Panics if fresh native transitions, immutable retries, Store clock or exact
/// fencing differ from the ownership contract.
pub async fn run_contract<S: Store>(factory: impl FnOnce(Arc<ManualClock>) -> S) {
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let store = factory(clock.clone());
    run_borrowed(&store, &clock).await;
}

async fn run_borrowed(store: &dyn Store, clock: &Arc<ManualClock>) {
    assert!(store.supports_session_owners());
    let mut request = reservation(SessionId::new());
    request.binding.resource_contract = Some(InputFingerprint::new("c".repeat(64)).unwrap());
    let session = &request.binding.session_id;
    let (reserved, authority, created_revision) = reserve_and_create(store, &request).await;
    immutable_resource_contract(store, &request, created_revision).await;
    let begin = mutation(&reserved.owner, SessionOwnerCommand::Begin);
    let (active, grant) = applied(
        store
            .mutate_session_owner(begin.clone(), Some(authority.clone()))
            .await
            .unwrap(),
    );
    assert!(grant.is_none());
    assert_eq!(active.owner.state, SessionOwnerState::Active);
    assert_eq!(
        store.session_revision(session).await.unwrap(),
        created_revision.next().unwrap()
    );
    assert!(
        matches!(store.mutate_session_owner(begin, None).await.unwrap(), SessionOwnerOutcome::Replayed(saved) if saved == active)
    );
    let captured_revision = store.session_revision(session).await.unwrap();
    clock.set(OffsetDateTime::UNIX_EPOCH + time::Duration::milliseconds(10));
    let renew = mutation(&active.owner, SessionOwnerCommand::Renew { lease_ms: 200 });
    let (renewed, _) = applied(
        store
            .mutate_session_owner(renew, Some(authority.clone()))
            .await
            .unwrap(),
    );
    assert_eq!(renewed.owner, active.owner);
    assert_eq!(
        store.session_revision(session).await.unwrap(),
        captured_revision
    );
    clock.set(OffsetDateTime::UNIX_EPOCH + time::Duration::milliseconds(211));
    assert_eq!(
        store
            .get_session_owner(session, &request.binding.id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Active
    );
    let reclaim = mutation(
        &active.owner,
        SessionOwnerCommand::ReclaimUnstarted {
            owner: "replacement".into(),
            lease_ms: 100,
        },
    );
    assert!(matches!(
        store.mutate_session_owner(reclaim, None).await,
        Err(SessionOwnerError::InvalidTransition)
    ));
    let uncertain = mutation(
        &active.owner,
        SessionOwnerCommand::MarkUncertain { claim: resolver() },
    );
    let (uncertain, _) = applied(store.mutate_session_owner(uncertain, None).await.unwrap());
    let join = mutation(
        &uncertain.owner,
        SessionOwnerCommand::Join {
            termination: SessionOwnerTermination::Returned,
        },
    );
    let (joined, grant) = applied(
        store
            .mutate_session_owner(join, Some(authority.clone()))
            .await
            .unwrap(),
    );
    assert!(grant.is_none());
    assert_eq!(joined.owner.state, SessionOwnerState::Joined);
    assert_eq!(
        joined.owner.termination,
        Some(SessionOwnerTermination::Returned)
    );
    assert_eq!(
        store
            .lookup_session_owner(session, &request.id)
            .await
            .unwrap(),
        Some(reserved)
    );
    let late = mutation(&active.owner, SessionOwnerCommand::Begin);
    assert!(matches!(
        store.mutate_session_owner(late, Some(authority)).await,
        Err(SessionOwnerError::InvalidTransition)
    ));
    unstarted(store, clock, session).await;
    races::run(store).await;
}
async fn immutable_resource_contract(
    store: &dyn Store,
    original: &ReserveSessionOwner,
    revision: SessionRevision,
) {
    let mut changed = original.clone();
    changed.binding.resource_contract = None;
    assert!(matches!(
        store.reserve_session_owner(changed, None).await,
        Err(SessionOwnerError::SemanticConflict)
    ));
    assert_eq!(
        store
            .session_revision(&original.binding.session_id)
            .await
            .unwrap(),
        revision
    );
}

async fn reserve_and_create(
    store: &dyn Store,
    request: &ReserveSessionOwner,
) -> (SessionOwnerReceipt, SessionOwnerAuthority, SessionRevision) {
    let session = &request.binding.session_id;
    let (reserved, authority) = applied(
        store
            .reserve_session_owner(request.clone(), None)
            .await
            .unwrap(),
    );
    let authority = authority.unwrap();
    assert!(store.get_session(session).await.unwrap().is_none());
    let mut retry = request.clone();
    retry.owner = "new-controller-bookkeeping".into();
    retry.lease_ms = 500;
    assert!(
        matches!(store.reserve_session_owner(retry, None).await.unwrap(), SessionOwnerOutcome::Replayed(saved) if saved == reserved)
    );
    let mut changed = request.clone();
    changed.binding.behavior_fingerprint = InputFingerprint::new("c".repeat(64)).unwrap();
    assert!(matches!(
        store.reserve_session_owner(changed, None).await,
        Err(SessionOwnerError::SemanticConflict)
    ));
    let create = ManageSession {
        id: ControlId::new(),
        session_id: session.clone(),
        expected_revision: SessionRevision::INITIAL,
        change: SessionChange::Create {
            workspace_id: request.binding.workspace_id.clone(),
            directory: request.binding.directory.clone(),
            title: "owner fixture".into(),
            defaults: SessionDefaults::default(),
        },
    };
    let mut drift = create.clone();
    if let SessionChange::Create { directory, .. } = &mut drift.change {
        directory.push('/');
    }
    assert!(store.manage_session(drift).await.is_err());
    let created = store.manage_session(create).await.unwrap();
    (reserved, authority, created.revision)
}
async fn unstarted(store: &dyn Store, clock: &ManualClock, session: &SessionId) {
    let request = reservation(session.clone());
    let (original, old_authority) = applied(
        store
            .reserve_session_owner(request.clone(), None)
            .await
            .unwrap(),
    );
    let reclaim = mutation(
        &original.owner,
        SessionOwnerCommand::ReclaimUnstarted {
            owner: "controller-b".into(),
            lease_ms: 100,
        },
    );
    assert!(matches!(
        store.mutate_session_owner(reclaim.clone(), None).await,
        Err(SessionOwnerError::LiveOwner)
    ));
    clock.set(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1));
    let (reclaimed, authority) = applied(
        store
            .mutate_session_owner(reclaim.clone(), None)
            .await
            .unwrap(),
    );
    let authority = authority.unwrap();
    assert_eq!(
        reclaimed.owner.target.generation,
        original.owner.target.generation.next().unwrap()
    );
    assert!(
        matches!(store.mutate_session_owner(reclaim, None).await.unwrap(), SessionOwnerOutcome::Replayed(saved) if saved == reclaimed)
    );
    let old_begin = mutation(&original.owner, SessionOwnerCommand::Begin);
    assert!(matches!(
        store.mutate_session_owner(old_begin, old_authority).await,
        Err(SessionOwnerError::GenerationConflict)
    ));
    let begin = mutation(&reclaimed.owner, SessionOwnerCommand::Begin);
    applied(
        store
            .mutate_session_owner(begin, Some(authority))
            .await
            .unwrap(),
    );
    clock.set(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(2));
    let resolve = mutation(
        &reclaimed.owner,
        SessionOwnerCommand::ResolveJoined {
            termination: SessionOwnerTermination::Cancelled,
            claim: resolver(),
        },
    );
    let (receipt, grant) = applied(
        store
            .mutate_session_owner(resolve.clone(), None)
            .await
            .unwrap(),
    );
    assert_eq!(receipt.owner.state, SessionOwnerState::Joined);
    assert!(grant.is_none());
    assert!(
        matches!(store.mutate_session_owner(resolve, None).await.unwrap(), SessionOwnerOutcome::Replayed(saved) if saved == receipt)
    );
}

#[cfg(test)]
#[path = "session_owner_contract/faults.rs"]
pub(crate) mod faults;

#[cfg(test)]
#[path = "session_owner_contract/parent.rs"]
pub(crate) mod parent;

#[cfg(test)]
#[path = "session_owner_contract/process.rs"]
#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub(crate) mod process;

#[path = "session_owner_contract/races.rs"]
mod races;

#[cfg(test)]
#[path = "session_owner_contract/capacity.rs"]
pub(crate) mod capacity;

#[cfg(test)]
#[path = "session_owner_contract/services.rs"]
pub(crate) mod services;
