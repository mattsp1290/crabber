//! Late cleanup/cancellation reconciliation distinguishes erasure from rollback.
use super::*;
use crate::Store;
use crate::session_owner::{
    SessionOwnerError as Error, SessionOwnerOutcome, preparation::PreparationKey,
};

#[tokio::test]
async fn deleted_owner_lookups_and_held_cancel_authority_return_deleted() {
    let store = MemoryStore::new();
    let reservation = crate::session_owner_contract::reservation(SessionId::new());
    let SessionOwnerOutcome::Applied {
        receipt,
        authority: Some(authority),
    } = store
        .reserve_session_owner(reservation.clone(), None)
        .await
        .unwrap()
    else {
        panic!("actual retained private grant")
    };
    let session = &reservation.binding.session_id;
    let mut tombstone = crate::deletion::contract::deleted();
    tombstone.receipt.session_id = session.clone();
    tombstone.receipt.semantic_digest = crate::DeleteSession {
        id: tombstone.receipt.operation_id.clone(),
        session_id: session.clone(),
        expected_revision: tombstone.expected_revision,
    }
    .fingerprint()
    .unwrap();
    tombstone.verify(session).unwrap();
    store
        .state
        .lock()
        .unwrap()
        .tombstones
        .insert(session.clone(), tombstone);
    let before = store
        .state
        .lock()
        .unwrap()
        .session_owners
        .publication_count(session);
    assert_eq!(
        store
            .get_session_owner(session, &reservation.binding.id)
            .await
            .unwrap_err(),
        Error::Deleted
    );
    assert_eq!(
        store
            .lookup_session_owner(session, &reservation.id)
            .await
            .unwrap_err(),
        Error::Deleted
    );
    assert_eq!(
        store
            .lookup_preparation(session, &PreparationKey::Unkeyed(ControlId::new()))
            .await
            .unwrap_err(),
        Error::Deleted
    );
    let request = crate::session_owner::cancellation_contract::request(&receipt.owner, None);
    assert_eq!(
        store
            .request_session_owner_cancel(request.clone())
            .await
            .unwrap_err(),
        Error::Deleted
    );
    assert_eq!(
        store
            .pending_session_owner_cancel(&authority)
            .await
            .unwrap_err(),
        Error::Deleted
    );
    let observation = crate::session_owner::cancellation_contract::observe(&request);
    assert_eq!(
        store
            .observe_session_owner_cancel(observation, authority)
            .await
            .unwrap_err(),
        Error::Deleted
    );
    assert_eq!(
        store
            .state
            .lock()
            .unwrap()
            .session_owners
            .publication_count(session),
        before
    );
}

#[tokio::test]
async fn deleted_payload_guards_precede_retained_rows_without_mutation() {
    let store = MemoryStore::new();
    let session = SessionId::new();
    let revision = crate::deletion::contract::seed_payloads(&store, &session).await;
    let before = store.state.lock().unwrap().extension_values.clone();
    let inbox_before: Vec<_> = store
        .state
        .lock()
        .unwrap()
        .inbox
        .iter()
        .map(|row| (&row.session_id, &row.message, &row.consumed_by_run))
        .map(|row| serde_json::to_value(row).unwrap())
        .collect();
    let tombstone = crate::deletion::contract::deleted_for(&session);
    store
        .state
        .lock()
        .unwrap()
        .tombstones
        .insert(session.clone(), tombstone);
    crate::deletion::contract::payload_guards(&store, &session).await;
    let state = store.state.lock().unwrap();
    assert_eq!(state.extension_values, before);
    let inbox_after: Vec<_> = state
        .inbox
        .iter()
        .map(|row| {
            serde_json::to_value((&row.session_id, &row.message, &row.consumed_by_run)).unwrap()
        })
        .collect();
    assert_eq!(inbox_after, inbox_before);
    assert_eq!(state.public_revisions.get(&session), Some(&revision));
}
