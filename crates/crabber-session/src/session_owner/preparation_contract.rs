//! Shared adapter proofs for exact original CAS and physical preparation join.
use super::preparation::{
    JoinedPreparation, PreparationCompletion, PreparationGrant, PreparationPurpose,
    PreparedOperation, ReservePreparation,
};
use super::{
    MutateSessionOwner, SessionOwnerCommand, SessionOwnerError, SessionOwnerOutcome,
    SessionOwnerState,
};
use crate::{KeyedAdmitOutcome, ManageSession, SessionChange, SessionDefaults, Store, StoreError};
use crabber_core::{ControlId, InputEnvelope, InputFingerprint, SessionId, SessionRevision};
use time::OffsetDateTime;

fn reservation(bound: &crate::BoundKeyedAdmitRequest) -> ReservePreparation {
    let mut owner =
        crate::session_owner_contract::reservation(bound.binding.request.session_id.clone());
    owner
        .binding
        .workspace_id
        .clone_from(&bound.binding.request.workspace_id);
    owner
        .binding
        .directory
        .clone_from(&bound.binding.request.directory);
    owner.lease_ms = 30_000;
    ReservePreparation {
        key: super::preparation::PreparationKey::Unkeyed(ControlId::new()),
        source: None,
        resource_contract: None,
        owner,
        expected_revision: bound.keyed.request.expected_revision.unwrap(),
        purpose: PreparationPurpose::OrdinaryAdmission,
        intent: super::preparation::admission_intent(
            &bound.binding.request,
            bound.binding.input_envelope.as_ref().unwrap(),
            Some(&bound.keyed.options),
        )
        .unwrap(),
    }
    .bind_key(super::preparation::PreparationKey::Admission(
        bound.keyed.options.key.clone(),
    ))
    .unwrap()
}
async fn begin(store: &impl Store, request: &ReservePreparation) -> PreparationGrant {
    let SessionOwnerOutcome::Applied {
        authority: Some(authority),
        ..
    } = store.reserve_preparation(request.clone()).await.unwrap()
    else {
        panic!("fresh reservation")
    };
    store
        .mutate_session_owner(
            MutateSessionOwner {
                id: request.reconciliation().unwrap().begin_id,
                session_id: request.owner.binding.session_id.clone(),
                target: authority.target().clone(),
                command: SessionOwnerCommand::Begin,
            },
            Some(authority.clone()),
        )
        .await
        .unwrap();
    PreparationGrant::from_fresh_begin(request.clone(), authority).unwrap()
}
async fn joined(
    grant: PreparationGrant,
    bound: crate::BoundKeyedAdmitRequest,
) -> JoinedPreparation {
    JoinedPreparation::observe(
        grant,
        tokio::spawn(
            async move { Ok::<_, ()>(((), PreparedOperation::AdmitKeyed(Box::new(bound)))) },
        ),
    )
    .await
    .unwrap()
    .unwrap()
    .1
}
fn fixture(session: &SessionId, revision: SessionRevision) -> crate::BoundKeyedAdmitRequest {
    crate::typed_admission_contract::prepare(
        crate::execution_binding_contract::fixture(
            session,
            OffsetDateTime::UNIX_EPOCH,
            "prepared-backend",
            'b',
        ),
        InputEnvelope::text("original input", revision).unwrap(),
        OffsetDateTime::UNIX_EPOCH,
    )
}
async fn create(store: &impl Store, bound: &crate::BoundKeyedAdmitRequest) -> SessionRevision {
    store
        .manage_session(ManageSession {
            id: ControlId::new(),
            session_id: bound.binding.request.session_id.clone(),
            expected_revision: SessionRevision::INITIAL,
            change: SessionChange::Create {
                workspace_id: bound.binding.request.workspace_id.clone(),
                directory: bound.binding.request.directory.clone(),
                title: "preparation".into(),
                defaults: SessionDefaults::default(),
            },
        })
        .await
        .unwrap()
        .revision
}
pub(crate) async fn run(store: &impl Store) {
    assert!(store.supports_preparation_protocol());
    for existing in [false, true] {
        let session = SessionId::new();
        let mut bound = fixture(&session, SessionRevision::INITIAL);
        let original_revision = if existing {
            create(store, &bound).await
        } else {
            SessionRevision::INITIAL
        };
        bound = fixture(&session, original_revision);
        let request = reservation(&bound);
        let grant = begin(store, &request).await;
        if existing {
            assert_eq!(
                store.session_revision(&session).await.unwrap(),
                original_revision.next().unwrap().next().unwrap()
            );
        } else {
            assert!(store.get_session(&session).await.unwrap().is_none());
        }
        let completion = joined(grant.clone(), bound.clone()).await;
        let PreparationCompletion::Keyed(KeyedAdmitOutcome::Started { receipt, .. }) =
            store.complete_preparation(completion).await.unwrap()
        else {
            panic!("one fresh successor")
        };
        assert_eq!(
            store
                .get_session_owner(&session, &request.owner.binding.id)
                .await
                .unwrap()
                .state,
            SessionOwnerState::Joined
        );
        assert_eq!(
            store
                .execution_binding(&session, &receipt.run_id)
                .await
                .unwrap()
                .unwrap()
                .capsule,
            bound.binding
        );
        let final_revision = store.session_revision(&session).await.unwrap();
        let replay = store
            .complete_preparation(joined(grant, bound).await)
            .await
            .unwrap();
        assert!(
            matches!(replay, PreparationCompletion::Keyed(KeyedAdmitOutcome::Replayed(saved)) if saved == receipt)
        );
        assert_eq!(
            store.session_revision(&session).await.unwrap(),
            final_revision
        );
        assert!(matches!(
            store.reserve_preparation(request.clone()).await.unwrap(),
            SessionOwnerOutcome::Replayed(_)
        ));
        let mut changed = request;
        changed.expected_revision = final_revision;
        assert!(matches!(
            store.reserve_preparation(changed).await,
            Err(SessionOwnerError::SemanticConflict)
        ));
    }
    Box::pin(intent::run(store)).await;
    absent::run(store).await;
    unrelated_revision(store).await;
    presence_change(store).await;
}
async fn unrelated_revision(store: &impl Store) {
    let session = SessionId::new();
    let bound = fixture(&session, SessionRevision::INITIAL);
    let revision = create(store, &bound).await;
    let bound = fixture(&session, revision);
    let request = reservation(&bound);
    let grant = begin(store, &request).await;
    let current = store.session_revision(&session).await.unwrap();
    store
        .manage_session(ManageSession {
            id: ControlId::new(),
            session_id: session.clone(),
            expected_revision: current,
            change: SessionChange::Rename {
                title: "unrelated mutation".into(),
            },
        })
        .await
        .unwrap();
    assert_eq!(
        store
            .complete_preparation(joined(grant, bound).await)
            .await
            .unwrap_err(),
        super::preparation::PreparationCommitError::Store(StoreError::AdmissionRevisionConflict)
    );
    assert_eq!(
        store
            .get_session_owner(&session, &request.owner.binding.id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Active
    );
    assert!(
        store
            .list_unfinished_runs()
            .await
            .unwrap()
            .iter()
            .all(|run| run.session_id != session)
    );
}
async fn presence_change(store: &impl Store) {
    let session = SessionId::new();
    let bound = fixture(&session, SessionRevision::INITIAL);
    let request = reservation(&bound);
    let grant = begin(store, &request).await;
    create(store, &bound).await;
    assert_eq!(
        store
            .complete_preparation(joined(grant, bound).await)
            .await
            .unwrap_err(),
        super::preparation::PreparationCommitError::Store(StoreError::AdmissionRevisionConflict)
    );
}

#[path = "preparation_contract/faults.rs"]
pub(crate) mod faults;

#[path = "preparation_contract/intent.rs"]
pub(crate) mod intent;

#[path = "preparation_contract/continuations.rs"]
pub(crate) mod continuations;

#[path = "preparation_contract/retry.rs"]
mod retry;

#[path = "preparation_contract/absent.rs"]
pub(crate) mod absent;

#[path = "preparation_contract/cancellation.rs"]
pub(crate) mod cancellation;

#[path = "preparation_contract/frontend.rs"]
pub(crate) mod frontend;
#[path = "preparation_contract/resources.rs"]
pub(crate) mod resources;

#[path = "preparation_contract/claim_history.rs"]
pub(crate) mod claim_history;
