//! Batch only fixture interiors; public calls retain every capacity oracle.
use super::*;
use crate::Store;

pub(super) async fn fill(store: &MemoryStore) -> (ReserveSessionOwner, SessionOwnerAuthority) {
    use crate::session_owner::SessionOwnerState;
    use crabber_core::SessionOwnerOperationId;
    let first = crate::session_owner_contract::reservation(crabber_core::SessionId::new());
    let SessionOwnerOutcome::Applied { receipt, authority } = store
        .reserve_session_owner(first.clone(), None)
        .await
        .unwrap()
    else {
        panic!("public first capacity reservation must apply");
    };
    assert_eq!(receipt.owner.state, SessionOwnerState::ReservedUnstarted);
    let authority = authority.unwrap();
    let mut expected = {
        let mut state = store.state.lock().unwrap();
        state
            .session_owners
            .seed_capacity_interior_for_test(&first, store.clock.as_ref())
    };
    {
        let state = store.state.lock().unwrap();
        verify(&state, &expected);
    }
    let mut last = first.clone();
    last.id = SessionOwnerOperationId::new();
    last.binding.id = crabber_core::SessionOwnerId::new();
    let SessionOwnerOutcome::Applied { receipt, .. } = store
        .reserve_session_owner(last.clone(), None)
        .await
        .unwrap()
    else {
        panic!("public reservation 1024 must apply");
    };
    assert_eq!(receipt.owner.state, SessionOwnerState::ReservedUnstarted);
    expected.push(last);
    let overflow = || {
        let mut request = first.clone();
        request.id = SessionOwnerOperationId::new();
        request.binding.id = crabber_core::SessionOwnerId::new();
        request
    };
    assert!(
        matches!(
            store.reserve_session_owner(overflow(), None).await,
            Err(Error::Capacity)
        ),
        "public reservation 1025 must refuse at the real 1024-owner limit"
    );
    let original = {
        let mut state = store.state.lock().unwrap();
        verify(&state, &expected);
        state
            .session_owners
            .forge_capacity_current_joined_for_test(&first)
    };
    // This counterfeit label changes no immutable source and must free no slot.
    assert!(
        matches!(
            store.reserve_session_owner(overflow(), None).await,
            Err(Error::Capacity)
        ),
        "a mutable Joined label must not free immutable owner capacity"
    );
    {
        let mut state = store.state.lock().unwrap();
        state
            .session_owners
            .restore_capacity_current_for_test(original);
        verify(&state, &expected);
    }
    (first, authority)
}

fn verify(state: &State, expected: &[ReserveSessionOwner]) {
    let session = &expected[0].binding.session_id;
    assert!(!state.sessions.contains_key(session));
    assert!(!state.public_revisions.contains_key(session));
    assert_eq!(
        revision(state, session),
        RevisionState::Absent(u64::try_from(expected.len()).unwrap())
    );
    state
        .session_owners
        .verify_capacity_fixture_for_test(expected);
}
