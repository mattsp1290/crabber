//! Capacity follows immutable latest state and actual joins.
use super::*;
pub(crate) async fn fill(store: &impl Store) -> (ReserveSessionOwner, SessionOwnerAuthority) {
    let first = reservation(SessionId::new());
    let (receipt, authority) = applied(
        store
            .reserve_session_owner(first.clone(), None)
            .await
            .unwrap(),
    );
    for _ in 1..1024 {
        let mut next = first.clone();
        next.id = SessionOwnerOperationId::new();
        next.binding.id = SessionOwnerId::new();
        applied(store.reserve_session_owner(next, None).await.unwrap());
    }
    let mut next = first.clone();
    next.id = SessionOwnerOperationId::new();
    next.binding.id = SessionOwnerId::new();
    assert!(matches!(
        store.reserve_session_owner(next, None).await,
        Err(SessionOwnerError::Capacity)
    ));
    assert_eq!(receipt.owner.state, SessionOwnerState::ReservedUnstarted);
    (first, authority.unwrap())
}
pub(crate) async fn joined_slot(
    store: &impl Store,
    first: ReserveSessionOwner,
    authority: SessionOwnerAuthority,
) {
    let snapshot = store
        .get_session_owner(&first.binding.session_id, &first.binding.id)
        .await
        .unwrap();
    applied(
        store
            .mutate_session_owner(
                mutation(
                    &snapshot,
                    SessionOwnerCommand::Join {
                        termination: SessionOwnerTermination::Returned,
                    },
                ),
                Some(authority),
            )
            .await
            .unwrap(),
    );
    let mut next = first;
    next.id = SessionOwnerOperationId::new();
    next.binding.id = SessionOwnerId::new();
    applied(store.reserve_session_owner(next, None).await.unwrap());
}
