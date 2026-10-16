use crate::{
    Store,
    session_owner::{
        MutateSessionOwner, SessionOwnerCommand, SessionOwnerError, SessionOwnerOutcome,
        SessionOwnerState,
    },
};
use crabber_core::{ManualClock, SessionId, SessionOwnerOperationId};
use std::sync::{Arc, atomic::Ordering};
use time::OffsetDateTime;
#[tokio::test]
async fn session_owner_contract() {
    crate::session_owner_contract::run_contract(|clock| super::MemoryStore::with_clock(clock))
        .await;
}
#[tokio::test]
async fn session_owner_ack_loss_never_reissues_dispatch() {
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let store = super::MemoryStore::with_clock(clock.clone());
    let request = crate::session_owner_contract::reservation(SessionId::new());
    let session = &request.binding.session_id;
    store.session_owner_fault.store(1, Ordering::SeqCst);
    assert!(matches!(
        store.reserve_session_owner(request.clone(), None).await,
        Err(SessionOwnerError::UnknownStoreOutcome)
    ));
    assert_eq!(
        store
            .lookup_session_owner(session, &request.id)
            .await
            .unwrap(),
        None
    );
    store.session_owner_fault.store(2, Ordering::SeqCst);
    assert!(matches!(
        store.reserve_session_owner(request.clone(), None).await,
        Err(SessionOwnerError::UnknownStoreOutcome)
    ));
    let original = store
        .lookup_session_owner(session, &request.id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(store.reserve_session_owner(request.clone(), None).await.unwrap(), SessionOwnerOutcome::Replayed(saved) if saved == original)
    );
    let begin = MutateSessionOwner {
        id: SessionOwnerOperationId::new(),
        session_id: session.clone(),
        target: original.owner.target.clone(),
        command: SessionOwnerCommand::Begin,
    };
    assert!(matches!(
        store.mutate_session_owner(begin, None).await,
        Err(SessionOwnerError::StaleOwner)
    ));
    clock.set(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1));
    store.session_owner_fault.store(0, Ordering::SeqCst);
    let reclaim = MutateSessionOwner {
        id: SessionOwnerOperationId::new(),
        session_id: session.clone(),
        target: original.owner.target,
        command: SessionOwnerCommand::ReclaimUnstarted {
            owner: "recovered".into(),
            lease_ms: 100,
        },
    };
    let SessionOwnerOutcome::Applied {
        receipt: reclaimed,
        authority,
    } = store.mutate_session_owner(reclaim, None).await.unwrap()
    else {
        panic!("fresh reclaim")
    };
    let authority = authority.unwrap();
    begin_ack_loss(&store, &request, reclaimed, authority).await;
}
async fn begin_ack_loss(
    store: &super::MemoryStore,
    request: &crate::session_owner::ReserveSessionOwner,
    reclaimed: crate::session_owner::SessionOwnerReceipt,
    authority: crate::session_owner::SessionOwnerAuthority,
) {
    let session = &request.binding.session_id;
    let begin = MutateSessionOwner {
        id: SessionOwnerOperationId::new(),
        session_id: session.clone(),
        target: reclaimed.owner.target,
        command: SessionOwnerCommand::Begin,
    };
    store.session_owner_fault.store(1, Ordering::SeqCst);
    assert!(matches!(
        store
            .mutate_session_owner(begin.clone(), Some(authority.clone()))
            .await,
        Err(SessionOwnerError::UnknownStoreOutcome)
    ));
    assert_eq!(
        store
            .get_session_owner(session, &request.binding.id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::ReservedUnstarted
    );
    store.session_owner_fault.store(2, Ordering::SeqCst);
    assert!(matches!(
        store
            .mutate_session_owner(begin.clone(), Some(authority))
            .await,
        Err(SessionOwnerError::UnknownStoreOutcome)
    ));
    assert_eq!(
        store
            .get_session_owner(session, &request.binding.id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Active
    );
    assert!(matches!(
        store.mutate_session_owner(begin, None).await.unwrap(),
        SessionOwnerOutcome::Replayed(_)
    ));
}
