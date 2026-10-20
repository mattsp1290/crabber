//! Stable-key and pre-session context serialization across independent callers.
use super::{
    SessionId, SessionOwnerError, SessionOwnerId, SessionOwnerOperationId, SessionOwnerOutcome,
    Store, applied, reservation,
};
pub(super) async fn run(store: &impl Store) {
    escaped_context(store).await;
    let request = reservation(SessionId::new());
    let (left, right) = futures::join!(
        store.reserve_session_owner(request.clone(), None),
        store.reserve_session_owner(request.clone(), None)
    );
    let outcomes = [left.unwrap(), right.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(
                outcome,
                SessionOwnerOutcome::Applied {
                    authority: Some(_),
                    ..
                }
            ))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, SessionOwnerOutcome::Replayed(_)))
            .count(),
        1
    );
    let request = reservation(SessionId::new());
    let mut changed = request.clone();
    changed.binding.component_id.push_str("/changed");
    let (left, right) = futures::join!(
        store.reserve_session_owner(request, None),
        store.reserve_session_owner(changed, None)
    );
    assert_eq!(
        [left, right]
            .into_iter()
            .filter(|outcome| matches!(outcome, Err(SessionOwnerError::SemanticConflict)))
            .count(),
        1
    );
    let request = reservation(SessionId::new());
    let mut drift = request.clone();
    drift.id = SessionOwnerOperationId::new();
    drift.binding.id = SessionOwnerId::new();
    drift.binding.directory.push('/');
    let (left, right) = futures::join!(
        store.reserve_session_owner(request, None),
        store.reserve_session_owner(drift, None)
    );
    let outcomes = [left, right];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Ok(SessionOwnerOutcome::Applied { .. })))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, Err(SessionOwnerError::IdentityMismatch)))
            .count(),
        1
    );
    let mut request = reservation(SessionId(format!(
        "{}/{}",
        "所有者".repeat(30),
        uuid::Uuid::new_v4()
    )));
    request.binding.workspace_id = "workspace\nopaque".into();
    request.binding.directory = "/opaque\nroot".into();
    let (receipt, _) = applied(
        store
            .reserve_session_owner(request.clone(), None)
            .await
            .unwrap(),
    );
    assert_eq!(
        store
            .get_session_owner(&request.binding.session_id, &request.binding.id)
            .await
            .unwrap(),
        receipt.owner
    );
    let mut bad = request;
    bad.id = SessionOwnerOperationId::new();
    bad.binding.directory.push('\0');
    assert!(matches!(
        store.reserve_session_owner(bad, None).await,
        Err(SessionOwnerError::InvalidRequest)
    ));
}

async fn escaped_context(store: &impl Store) {
    let mut request = reservation(SessionId::new());
    request.binding.workspace_id = "\u{0001}".repeat(512);
    request.binding.directory = "\n".repeat(4096);
    let (receipt, _) = applied(
        store
            .reserve_session_owner(request.clone(), None)
            .await
            .unwrap(),
    );
    assert_eq!(
        store
            .get_session_owner(&request.binding.session_id, &request.binding.id)
            .await
            .unwrap(),
        receipt.owner
    );
    let mut another = request.clone();
    another.id = SessionOwnerOperationId::new();
    another.binding.id = SessionOwnerId::new();
    applied(store.reserve_session_owner(another, None).await.unwrap());
    assert!(
        matches!(store.reserve_session_owner(request,None).await.unwrap(),SessionOwnerOutcome::Replayed(saved)if saved==receipt)
    );
}
