//! Capacity follows immutable latest state and actual joins.
use super::*;
pub(crate) async fn joined_slot(
    store: &dyn Store,
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
