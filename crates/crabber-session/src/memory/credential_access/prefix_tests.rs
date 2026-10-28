//! Genuine native preparation originals qualify the pure historical verifier.
use crate::native_enrollment::pending_initial::{Identity, prefix};
use crate::public_session::v2::proof::Proof;
use crate::session_owner::{
    SessionOwnerState,
    audit::Audit,
    preparation::{JoinedPreparation, PreparationCommitError},
};
use crate::{MemoryStore, Store};

fn sources(store: &MemoryStore, original: &JoinedPreparation) -> [Audit; 5] {
    let mut proof = Proof::bounded(32 << 20);
    let ids = prefix::select(original, &mut proof).unwrap();
    let ids = [
        ids.root_reserve,
        ids.root_begin,
        ids.child_reserve,
        ids.child_begin,
        ids.activated,
    ];
    let state = store.state.lock().unwrap();
    let session = &original
        .observed_grant()
        .original_reservation()
        .owner
        .binding
        .session_id;
    std::array::from_fn(|index| {
        let actual = state
            .session_owners
            .source_audit(session, &ids[index])
            .unwrap();
        proof.measure(actual).unwrap();
        actual.clone()
    })
}

fn selected(audits: &[Audit; 5]) -> prefix::InitialPrefixAudits<'_> {
    prefix::InitialPrefixAudits {
        root_reserve: &audits[0],
        root_begin: &audits[1],
        child_reserve: &audits[2],
        child_begin: &audits[3],
        activated: &audits[4],
    }
}

async fn original(store: &MemoryStore) -> JoinedPreparation {
    crate::session_owner::preparation_contract::resources::initial_unkeyed_observation(store).await
}

#[tokio::test]
async fn historical_prefix_survives_actual_preparation_consumption() {
    let store = MemoryStore::new();
    let original = original(&store).await;
    let audits = sources(&store, &original);
    let identity = Identity {
        epoch: crabber_core::ControlId::new(),
        complete: crabber_core::ControlId::new(),
        seal: crabber_core::ControlId::new(),
    };
    let expected = prefix::expected(&original, &identity, &mut Proof::bounded(32 << 20)).unwrap();
    let before = prefix::verify(&original, selected(&audits), &mut Proof::bounded(32 << 20))
        .unwrap()
        .correlation(&identity, &mut Proof::bounded(32 << 20))
        .unwrap();
    assert!(before == expected);
    Store::complete_preparation(&store, original.clone())
        .await
        .unwrap();
    let root = original.observed_grant().owner_authority();
    assert_eq!(
        store
            .get_session_owner(root.session_id(), &root.target().id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Joined
    );
    let after = sources(&store, &original);
    let mut proof = Proof::bounded(32 << 20);
    let historical = prefix::verify(&original, selected(&after), &mut proof).unwrap();
    assert!(historical.correlation(&identity, &mut proof).unwrap() == expected);
}

#[tokio::test]
async fn historical_prefix_checks_actual_private_tokens_and_foreign_activation() {
    let store = MemoryStore::new();
    let original = original(&store).await;
    let audits = sources(&store, &original);
    let foreign = self::original(&store).await;
    let other = sources(&store, &foreign);
    for index in [0, 1, 2, 3, 4] {
        let mut changed = audits.clone();
        changed[index] = other[index].clone();
        assert!(
            prefix::verify(&original, selected(&changed), &mut Proof::bounded(32 << 20)).is_err()
        );
    }
    for index in [1, 3, 4] {
        let mut changed = audits.clone();
        changed[index].record.token = "e".repeat(64);
        assert!(
            prefix::verify(&original, selected(&changed), &mut Proof::bounded(32 << 20)).is_err()
        );
    }
    assert!(prefix::verify(&original, selected(&audits), &mut Proof::bounded(32 << 20)).is_ok());
}

#[tokio::test]
async fn historical_prefix_uses_exact_same_bounded_proof() {
    let store = MemoryStore::new();
    let original = original(&store).await;
    let audits = sources(&store, &original);
    let mut measured = Proof::bounded(32 << 20);
    assert!(prefix::verify(&original, selected(&audits), &mut measured).is_ok());
    let used = measured.used();
    assert!(used > 0);
    assert!(prefix::verify(&original, selected(&audits), &mut Proof::bounded(used)).is_ok());
    assert!(matches!(
        prefix::verify(&original, selected(&audits), &mut Proof::bounded(used - 1)),
        Err(PreparationCommitError::Store(crate::StoreError::AdmissionLimit(limit)))
            if limit.limit_bytes() == Some(u64::try_from(used - 1).unwrap())
    ));

    let mut shared = Proof::bounded(32 << 20);
    let _prepared = prefix::prepare(&original, &mut shared).unwrap();
    let precharged = shared.used();
    crate::native_enrollment::caller::initial::with_initial_original_async(
        &original,
        &mut shared,
        |scope, proof| {
            Box::pin(async move {
                let value = prefix::verify_selected(
                    scope.original(),
                    selected(&audits),
                    Some(scope),
                    proof,
                )?;
                proof
                    .measure(value.key())
                    .map_err(|_| crate::session_owner::SessionOwnerError::Capacity)?;
                let key = value.key().clone();
                let cloned = scope.original().clone();
                assert!(
                    prefix::verify_selected(&cloned, selected(&audits), Some(scope), proof)
                        .is_err()
                );
                assert!(
                    scope
                        .verify(scope.original(), &Proof::bounded(32 << 20))
                        .is_err()
                );
                Ok(key)
            })
        },
    )
    .await
    .unwrap();
    assert!(shared.used() > precharged);
}
