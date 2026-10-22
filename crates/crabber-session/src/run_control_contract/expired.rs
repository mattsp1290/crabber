//! Actual source reads, retained task joins and atomic native claims.
#[path = "expired/owned.rs"]
pub(crate) mod owned;
#[path = "expired/owned_admission.rs"]
mod owned_admission;
#[path = "expired/unkeyed.rs"]
pub(crate) mod unkeyed;
#[path = "expired/unstarted.rs"]
pub(crate) mod unstarted;
use super::checkpoint::pause_admitted;
use super::{Clock, ManualClock, Store};
use crate::session_owner::{
    MutateSessionOwner, SessionOwnerCommand, SessionOwnerError as Error, SessionOwnerOutcome,
    SessionOwnerState,
    preparation::{
        ExistingWorkTarget, ExpiredRunOutcome, ExpiredRunPreparation, ExpiredRunReceipt,
        InspectExpiredRunPreparation, JoinedPreparation, PreparationCompletion, PreparationGrant,
        PreparationKey, PreparationPurpose, PreparationSource, PreparedExpiredRunResume,
        PreparedOperation, ReservePreparation,
    },
};
use crabber_core::{ControlId, InputEnvelope, RunFence, SessionId, SessionRevision};

pub(super) async fn expired_inspection(store: &impl Store, clock: &ManualClock) {
    Box::pin(running(store, clock)).await;
    Box::pin(unkeyed::run(store, clock)).await;
    Box::pin(unstarted::run(store, clock)).await;
    let (source, binding) = paused(store, clock).await;
    let before = store
        .session_revision(&source.request().session_id)
        .await
        .unwrap();
    assert_eq!(source.expected_revision(), before);
    source.verify_binding(&binding).unwrap();
    let reread = store
        .inspect_expired_run_preparation(source.request().clone())
        .await
        .unwrap();
    assert_eq!(source.fingerprint().unwrap(), reread.fingerprint().unwrap());
    source.verify_source(&reread).unwrap();
    assert_eq!(
        store
            .session_revision(&source.request().session_id)
            .await
            .unwrap(),
        before
    );
    let mut changed = binding.clone();
    changed.request.title.push_str("changed");
    assert_eq!(
        source.verify_binding(&changed),
        Err(Error::SemanticConflict)
    );
    let mut stale = source.request().clone();
    stale.expected_fence.claim_token.push_str("changed");
    assert!(matches!(
        store.inspect_expired_run_preparation(stale).await,
        Err(Error::StaleOwner)
    ));
    let mut changed_lease = source.request().clone();
    changed_lease.lease += std::time::Duration::from_secs(1);
    assert!(
        ExpiredRunPreparation::from_verified_source(
            changed_lease,
            source.expected_revision(),
            source.target().clone()
        )
        .is_err()
    );
    Box::pin(atomic(store, source, binding)).await;
}
fn bound(session: &SessionId, now: time::OffsetDateTime) -> crate::BoundKeyedAdmitRequest {
    crate::typed_admission_contract::prepare(
        crate::execution_binding_contract::fixture(session, now, "expired-source", 'b'),
        InputEnvelope::text("original expired input", SessionRevision::INITIAL).unwrap(),
        now,
    )
}
async fn running(store: &dyn Store, clock: &ManualClock) {
    let session = SessionId::new();
    let bound = bound(&session, clock.now());
    let binding = bound.binding.clone();
    let crate::KeyedAdmitOutcome::Started { admitted, .. } =
        store.admit_bound_keyed_run(bound).await.unwrap()
    else {
        panic!("fresh bound admission")
    };
    let request = InspectExpiredRunPreparation {
        session_id: session,
        expected_fence: admitted.fence.clone(),
        owner: "resume-initial".into(),
        lease: std::time::Duration::from_secs(30),
    };
    assert!(matches!(
        store.inspect_expired_run_preparation(request.clone()).await,
        Err(Error::LiveOwner)
    ));
    store
        .execution(admitted.fence)
        .await
        .unwrap()
        .begin_admission_execution()
        .await
        .unwrap();
    clock.set(admitted.run.lease_until);
    let source = store
        .inspect_expired_run_preparation(request)
        .await
        .unwrap();
    Box::pin(atomic(store, source, binding)).await;
}
async fn paused(
    store: &impl Store,
    clock: &ManualClock,
) -> (ExpiredRunPreparation, crate::ExecutionBindingCapsule) {
    let session = SessionId::new();
    let mut bound = bound(&session, clock.now());
    bound.keyed.request.session_id = None;
    let binding = bound.binding;
    let admitted = store
        .admit_bound_run(crate::BoundAdmitRequest {
            request: bound.keyed.request,
            binding: binding.clone(),
        })
        .await
        .unwrap();
    let (target, _) = pause_admitted(store, clock, false, admitted).await;
    let run = store.get_run(&target.run_id).await.unwrap().unwrap();
    clock.set(clock.now() + time::Duration::seconds(31));
    let request = InspectExpiredRunPreparation {
        session_id: session,
        expected_fence: RunFence {
            run_id: run.id,
            claim_token: run.claim_token,
        },
        owner: "resume-paused".into(),
        lease: std::time::Duration::from_secs(30),
    };
    (
        store
            .inspect_expired_run_preparation(request)
            .await
            .unwrap(),
        binding,
    )
}
/// Actual pending completion, without manufacturing or serializing joined proof.
#[cfg(test)]
pub(crate) struct Pending {
    pub source: ExpiredRunPreparation,
    pub joined: JoinedPreparation,
}
#[cfg(test)]
pub(crate) async fn pending(store: &impl Store, clock: &ManualClock) -> Pending {
    let (source, binding) = paused(store, clock).await;
    let joined = prepare(store, &source, binding).await;
    Pending { source, joined }
}
fn reservation(
    source: &ExpiredRunPreparation,
    binding: &crate::ExecutionBindingCapsule,
) -> ReservePreparation {
    let mut owner = crate::session_owner_contract::reservation(source.request().session_id.clone());
    owner
        .binding
        .workspace_id
        .clone_from(&binding.request.workspace_id);
    owner
        .binding
        .directory
        .clone_from(&binding.request.directory);
    owner
        .binding
        .behavior_fingerprint
        .clone_from(&binding.behavior_fingerprint);
    owner.lease_ms = 30_000;
    let resource_contract =
        crate::session_owner::preparation::resource_contract(&binding.runtime_semantics).unwrap();
    if resource_contract.is_some() {
        owner.binding.component_id =
            crate::session_owner::preparation::OWNED_PREPARATION_COMPONENT.into();
    }
    let key = PreparationKey::ExpiredRun {
        run_id: source.request().expected_fence.run_id.clone(),
        issuance: source.expected_issuance(),
    };
    ReservePreparation {
        owner,
        key: key.clone(),
        resource_contract,
        source: Some(PreparationSource::ExpiredRun(source.clone())),
        expected_revision: source.expected_revision(),
        purpose: PreparationPurpose::ExistingRun {
            run_id: source.request().expected_fence.run_id.clone(),
            work: ExistingWorkTarget::ExpiredRun(Box::new(source.target().clone())),
        },
        intent: source.fingerprint().unwrap(),
    }
    .bind_key(key)
    .unwrap()
}
#[cfg(test)]
pub(crate) async fn reserved(
    store: &impl Store,
    clock: &ManualClock,
) -> (
    ExpiredRunPreparation,
    ReservePreparation,
    crate::session_owner::SessionOwnerAuthority,
) {
    let (source, binding) = paused(store, clock).await;
    let reservation = reservation(&source, &binding);
    let SessionOwnerOutcome::Applied {
        authority: Some(authority),
        ..
    } = store
        .reserve_preparation(reservation.clone())
        .await
        .unwrap()
    else {
        panic!("fresh reserve");
    };
    (source, reservation, authority)
}
#[cfg(test)]
pub(crate) fn begin_request(
    reservation: &ReservePreparation,
    authority: &crate::session_owner::SessionOwnerAuthority,
) -> MutateSessionOwner {
    MutateSessionOwner {
        id: reservation.reconciliation().unwrap().begin_id,
        session_id: reservation.owner.binding.session_id.clone(),
        target: authority.target().clone(),
        command: SessionOwnerCommand::Begin,
    }
}
pub(crate) async fn prepare(
    store: &dyn Store,
    source: &ExpiredRunPreparation,
    binding: crate::ExecutionBindingCapsule,
) -> JoinedPreparation {
    let reservation = reservation(source, &binding);
    let SessionOwnerOutcome::Applied {
        authority: Some(authority),
        ..
    } = store
        .reserve_preparation(reservation.clone())
        .await
        .unwrap()
    else {
        panic!("fresh source reserve")
    };
    assert!(
        store
            .claim_expired_run(&source.request().expected_fence.run_id, "contender")
            .await
            .is_err()
    );
    store
        .mutate_session_owner(
            MutateSessionOwner {
                id: reservation.reconciliation().unwrap().begin_id,
                session_id: source.request().session_id.clone(),
                target: authority.target().clone(),
                command: SessionOwnerCommand::Begin,
            },
            Some(authority.clone()),
        )
        .await
        .unwrap();
    let owned = reservation.resource_contract.is_some();
    let grant = PreparationGrant::from_fresh_begin(reservation, authority).unwrap();
    let prepared = PreparedExpiredRunResume {
        source: source.clone(),
        binding: Box::new(binding),
    };
    if owned {
        return Box::pin(owned::observe(
            store,
            grant,
            PreparedOperation::ExpiredRunResume(Box::new(prepared)),
        ))
        .await;
    }
    JoinedPreparation::observe(
        grant,
        tokio::spawn(async move {
            Ok::<_, ()>(((), PreparedOperation::ExpiredRunResume(Box::new(prepared))))
        }),
    )
    .await
    .unwrap()
    .unwrap()
    .1
}
async fn atomic(
    store: &dyn Store,
    source: ExpiredRunPreparation,
    binding: crate::ExecutionBindingCapsule,
) {
    let joined = prepare(store, &source, binding).await;
    assert!(lookup(store, &source).await.is_none());
    let (left, right) = futures::join!(
        Box::pin(store.complete_preparation(joined.clone())),
        Box::pin(store.complete_preparation(joined.clone())),
    );
    let pair = [left.unwrap(), right.unwrap()];
    assert_eq!(
        pair.iter().filter(|outcome| outcome.is_replayed()).count(),
        1
    );
    let [first, second] = pair;
    let (claimed, replayed) = if first.is_replayed() {
        (second, first)
    } else {
        (first, second)
    };
    let PreparationCompletion::ExpiredRun(ExpiredRunOutcome::Claimed { receipt, fence }) = claimed
    else {
        panic!("one fresh exact claim");
    };
    assert!(
        matches!(replayed, PreparationCompletion::ExpiredRun(ExpiredRunOutcome::Replayed(ref saved)) if saved == &receipt)
    );
    assert_ne!(
        fence.claim_token,
        source.request().expected_fence.claim_token
    );
    assert_eq!(receipt.issuance, source.expected_issuance() + 1);
    assert_eq!(
        receipt.revision,
        store.session_revision(&receipt.session_id).await.unwrap()
    );
    assert_eq!(
        store
            .get_session_owner(
                &receipt.session_id,
                &joined.observed_grant().owner_authority().target().id
            )
            .await
            .unwrap()
            .state,
        SessionOwnerState::Joined
    );
    metadata(store, &source, &receipt, joined).await;
}
async fn metadata(
    store: &dyn Store,
    source: &ExpiredRunPreparation,
    receipt: &ExpiredRunReceipt,
    joined: JoinedPreparation,
) {
    assert_eq!(lookup(store, source).await.as_ref(), Some(receipt));
    store
        .manage_session(crate::ManageSession {
            id: ControlId::new(),
            session_id: receipt.session_id.clone(),
            expected_revision: receipt.revision,
            change: crate::SessionChange::Rename {
                title: "changed after original claim".into(),
            },
        })
        .await
        .unwrap();
    assert!(store.session_revision(&receipt.session_id).await.unwrap() > receipt.revision);
    assert_eq!(lookup(store, source).await.as_ref(), Some(receipt));
    let replay = store.complete_preparation(joined).await.unwrap();
    assert!(replay.is_replayed());
    let PreparationCompletion::ExpiredRun(ExpiredRunOutcome::Replayed(original)) = replay else {
        panic!("metadata only")
    };
    assert_eq!(&original, receipt);
}
async fn lookup(store: &dyn Store, source: &ExpiredRunPreparation) -> Option<ExpiredRunReceipt> {
    store
        .lookup_expired_run_preparation(
            &source.request().session_id,
            &source.request().expected_fence.run_id,
            source.expected_issuance(),
        )
        .await
        .unwrap()
}
/// Before/after actual native commit failures reconcile the same observed task.
#[cfg(test)]
pub(crate) async fn faults(store: &impl Store, clock: &ManualClock, inject: impl Fn(u8)) {
    for boundary in [1, 2] {
        let pending = pending(store, clock).await;
        let before = store
            .session_revision(&pending.source.request().session_id)
            .await
            .unwrap();
        inject(boundary);
        assert!(
            store
                .complete_preparation(pending.joined.clone())
                .await
                .is_err()
        );
        inject(0);
        assert_eq!(
            lookup(store, &pending.source).await.is_some(),
            boundary == 2
        );
        let outcome = store
            .complete_preparation(pending.joined.clone())
            .await
            .unwrap();
        assert_eq!(outcome.is_replayed(), boundary == 2);
        let PreparationCompletion::ExpiredRun(
            ExpiredRunOutcome::Claimed { receipt, .. } | ExpiredRunOutcome::Replayed(receipt),
        ) = outcome
        else {
            panic!("exact domain")
        };
        assert_eq!(receipt.revision, before.next().unwrap());
        assert_eq!(lookup(store, &pending.source).await, Some(receipt));
    }
}

/// Genuine keyed initial source before a cold prepared claim.
#[cfg(test)]
pub(crate) async fn pending_keyed(store: &dyn Store, clock: &ManualClock) -> Pending {
    let session = SessionId::new();
    let bound = bound(&session, clock.now());
    let binding = bound.binding.clone();
    let crate::KeyedAdmitOutcome::Started { admitted, .. } =
        store.admit_bound_keyed_run(bound).await.unwrap()
    else {
        panic!("fresh keyed source")
    };
    store
        .execution(admitted.fence.clone())
        .await
        .unwrap()
        .begin_admission_execution()
        .await
        .unwrap();
    clock.set(admitted.run.lease_until);
    let source = store
        .inspect_expired_run_preparation(InspectExpiredRunPreparation {
            session_id: session,
            expected_fence: admitted.fence,
            owner: "keyed cold claim".into(),
            lease: std::time::Duration::from_secs(30),
        })
        .await
        .unwrap();
    let joined = prepare(store, &source, binding).await;
    Pending { source, joined }
}
