use super::*;
use crate::session_owner::{
    ReserveSessionOwner, SessionOwnerBinding, SessionOwnerKind, SessionOwnerOperation,
    SessionOwnerReceipt, SessionOwnerSnapshot, SessionOwnerState,
};
use crabber_core::{SessionOwnerGeneration, SessionOwnerId, SessionOwnerOperationId};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
fn source(name: &str) -> OwnedScopeSource {
    let binding = SessionOwnerBinding {
        session_id: SessionId::from(name),
        workspace_id: "workspace".into(),
        directory: "/work".into(),
        id: SessionOwnerId::from(name),
        kind: SessionOwnerKind::RetainedCapacity,
        component_id: "test".into(),
        behavior_fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
        parent: None,
    };
    let snapshot = SessionOwnerSnapshot {
        binding: binding.clone(),
        target: SessionOwnerTarget {
            id: binding.id.clone(),
            generation: SessionOwnerGeneration::INITIAL,
        },
        state: SessionOwnerState::ReservedUnstarted,
        termination: None,
        cancellation: None,
        created_at: time::OffsetDateTime::UNIX_EPOCH,
        updated_at: time::OffsetDateTime::UNIX_EPOCH,
    };
    let reserve = SessionOwnerReceipt {
        session_id: binding.session_id.clone(),
        operation_id: SessionOwnerOperationId::from("reserve"),
        operation: SessionOwnerOperation::Reserve,
        semantic_digest: ReserveSessionOwner {
            id: SessionOwnerOperationId::from("reserve"),
            binding,
            owner: "host".into(),
            lease_ms: 1000,
        }
        .fingerprint()
        .unwrap(),
        owner: snapshot.clone(),
    };
    let mut begin = reserve.clone();
    begin.operation = SessionOwnerOperation::Begin;
    begin.operation_id = SessionOwnerOperationId::from("begin");
    begin.owner.state = SessionOwnerState::Active;
    begin.semantic_digest = crate::session_owner::MutateSessionOwner {
        id: begin.operation_id.clone(),
        session_id: begin.session_id.clone(),
        target: begin.owner.target.clone(),
        command: crate::session_owner::SessionOwnerCommand::Begin,
    }
    .fingerprint()
    .unwrap();
    OwnedScopeSource::from_receipts(&reserve, &begin).unwrap()
}
#[tokio::test]
async fn unknown_ack_closes_actual_gate_without_invoking_factory() {
    let calls = Arc::new(AtomicUsize::new(0));
    let factory_calls = calls.clone();
    let mut registration = OwnedTaskRegistration::register(source("unknown"), move || {
        factory_calls.fetch_add(1, Ordering::SeqCst);
        async { 42 }
    })
    .unwrap();
    let joined = registration.join().await.unwrap();
    assert_eq!(joined.disposition(), OwnedMainDisposition::NoDispatch);
    assert_eq!(joined.value(), None);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn fresh_exact_permit_invokes_once_and_wrong_nonce_retains_gate() {
    let calls = Arc::new(AtomicUsize::new(0));
    let factory_calls = calls.clone();
    let mut registration = OwnedTaskRegistration::register(source("ack"), move || {
        factory_calls.fetch_add(1, Ordering::SeqCst);
        async { 42 }
    })
    .unwrap();
    let wrong = OwnedScopeActivation::new(source("ack")).unwrap();
    assert!(matches!(
        registration.dispatch(OwnedDispatchPermit::applied(wrong)),
        Err(Error::SemanticConflict)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let permit = OwnedDispatchPermit::applied(registration.activation().clone());
    registration.dispatch(permit).unwrap();
    let joined = registration.join().await.unwrap();
    assert_eq!(joined.disposition(), OwnedMainDisposition::Returned);
    assert_eq!(joined.value(), Some(&42));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn main_panic_has_actual_disposition_and_no_cleanup_inference() {
    let mut registration = OwnedTaskRegistration::register(source("panic"), || async {
        panic!("main");
    })
    .unwrap();
    registration
        .dispatch(OwnedDispatchPermit::applied(
            registration.activation().clone(),
        ))
        .unwrap();
    let joined = registration.join().await.unwrap();
    assert_eq!(joined.disposition(), OwnedMainDisposition::Panicked);
}

#[tokio::test]
async fn interrupted_main_observation_retains_the_actual_task_and_value() {
    let release = Arc::new(tokio::sync::Notify::new());
    let released = release.clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let dispatched = calls.clone();
    let mut registration = OwnedTaskRegistration::register(source("retained"), move || {
        dispatched.fetch_add(1, Ordering::SeqCst);
        async move {
            released.notified().await;
            42
        }
    })
    .unwrap();
    registration
        .dispatch(OwnedDispatchPermit::applied(
            registration.activation().clone(),
        ))
        .unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), registration.join())
            .await
            .is_err()
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    release.notify_one();
    assert_eq!(registration.join().await.unwrap().value(), Some(&42));
    assert_eq!(registration.join().await.unwrap().value(), Some(&42));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
