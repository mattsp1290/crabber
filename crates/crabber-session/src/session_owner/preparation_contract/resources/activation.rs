//! Actual waiting gate, metadata retries, and exact child publication lineage.
use super::*;
use crate::session_owner::SessionOwnerAuthority;
use crate::session_owner::owned_scope::{
    OwnedMainDisposition, OwnedScopeActivationOutcome, OwnedScopeSource, OwnedTaskRegistration,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
pub(super) async fn run(store: &impl Store) {
    let (original, bound) = super::declaration::setup(store).await;
    let PreparationResourceOutcome::Applied {
        receipt: resources,
        grant,
    } = store.retain_preparation_resources(original).await.unwrap()
    else {
        panic!("fresh child")
    };
    let child = grant.retained_resources_authority().unwrap().clone();
    let source = OwnedScopeSource::from_receipts(&resources.reserved, &resources.began).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let dispatched = calls.clone();
    let mut task = OwnedTaskRegistration::register(source.clone(), move || {
        dispatched.fetch_add(1, Ordering::SeqCst);
        async move {
            Ok::<_, ()>((
                std::sync::Mutex::new(42),
                PreparedOperation::AdmitKeyed(Box::new(bound)),
            ))
        }
    })
    .unwrap();
    let request = task.activation().clone();
    let before = store.session_revision(request.session_id()).await.unwrap();
    let OwnedScopeActivationOutcome::Applied { receipt, permit } = store
        .activate_owned_scope(
            request.clone(),
            child.clone(),
            Some(
                crate::session_owner::owned_scope::OwnedScopeParentAuthority::preparation(
                    (*grant).clone(),
                ),
            ),
        )
        .await
        .unwrap()
    else {
        panic!("fresh permit")
    };
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        store.session_revision(request.session_id()).await.unwrap(),
        before.next().unwrap()
    );
    assert!(
        matches!(store.activate_owned_scope(request.clone(), child.clone(), None).await.unwrap(), OwnedScopeActivationOutcome::Replayed(saved) if saved==receipt)
    );
    let mut replacement = OwnedTaskRegistration::register(source, || async { 99 }).unwrap();
    assert!(matches!(
        store
            .activate_owned_scope(replacement.activation().clone(), child.clone(), None)
            .await,
        Err(SessionOwnerError::SemanticConflict)
    ));
    assert_eq!(
        replacement.join().await.unwrap().disposition(),
        OwnedMainDisposition::NoDispatch
    );
    task.dispatch(*permit).unwrap();
    let wrong_grant = changed_grant(&grant, &child);
    assert!(matches!(
        JoinedPreparation::observe_registered(wrong_grant, &mut task).await,
        Err(SessionOwnerError::SemanticConflict)
    ));
    let (value, completion) = JoinedPreparation::observe_registered(*grant, &mut task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(*value.lock().unwrap(), 42);
    assert!(task.take_value().await.unwrap().is_none());
    let observed = task.join().await.unwrap();
    assert_eq!(observed.disposition(), OwnedMainDisposition::Returned);
    assert!(observed.value().is_none());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    // The native guard authenticates the exact child's third public publication
    // and preserves the caller's original request/envelope revision.
    assert!(
        !store
            .complete_preparation(completion)
            .await
            .unwrap()
            .is_replayed()
    );
    assert_eq!(
        store
            .get_session_owner(request.session_id(), &child.target().id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Active
    );
    assert!(
        matches!(store.activate_owned_scope(request.clone(), child.clone(), None).await.unwrap(), OwnedScopeActivationOutcome::Replayed(saved) if saved==receipt)
    );
    joined_resources(store, observed, &child).await;
}

fn changed_grant(grant: &PreparationGrant, child: &SessionOwnerAuthority) -> PreparationGrant {
    let mut wrong_original = grant.original_reservation().clone();
    wrong_original.owner.binding.directory = "/other".into();
    PreparationGrant::from_fresh_begin(wrong_original, grant.owner_authority().clone())
        .unwrap()
        .with_retained_resources(child.clone())
        .unwrap()
}

async fn joined_resources<T>(
    store: &impl Store,
    observed: &crate::session_owner::owned_scope::JoinedOwnedTask<T>,
    child: &SessionOwnerAuthority,
) {
    use crate::session_owner::owned_scope::{OwnedCleanupFailure, OwnedCleanupTask};
    use crabber_extension::ToolResourceOwner;
    let owner = ToolResourceOwner::new();
    let release = Arc::new(tokio::sync::Notify::new());
    let waiter = release.clone();
    owner
        .resources()
        .spawn(async move {
            waiter.notified().await;
            Ok(())
        })
        .unwrap();
    let cleanup_owner = owner.clone();
    let mut cleanup = OwnedCleanupTask::from_host_task(
        observed.activation().clone(),
        tokio::spawn(async move {
            loop {
                let report = cleanup_owner
                    .drain(std::time::Duration::from_millis(5))
                    .await;
                if !report.failures.is_empty() {
                    return Err(OwnedCleanupFailure::Failed);
                }
                if report.pending == 0 {
                    return Ok(());
                }
            }
        }),
    );
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(10),
            observed.observe_resources(&mut cleanup)
        )
        .await
        .is_err()
    );
    assert_eq!(
        store
            .get_session_owner(child.session_id(), &child.target().id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Active
    );
    release.notify_one();
    let proof = observed.observe_resources(&mut cleanup).await.unwrap();
    let SessionOwnerOutcome::Applied {
        receipt,
        authority: None,
    } = store
        .join_owned_scope(proof.clone(), child.clone())
        .await
        .unwrap()
    else {
        panic!("fresh joined source")
    };
    assert_eq!(receipt.owner.state, SessionOwnerState::Joined);
    assert!(
        matches!(store.join_owned_scope(proof, child.clone()).await.unwrap(), SessionOwnerOutcome::Replayed(saved) if saved==receipt)
    );
    assert_eq!(
        store
            .get_session_owner(child.session_id(), &child.target().id)
            .await
            .unwrap()
            .state,
        SessionOwnerState::Joined
    );
    // A different cleanup task is never accepted as the original scope.
    let mut failure = OwnedCleanupTask::from_host_task(
        observed.activation().clone(),
        tokio::spawn(async { Err(OwnedCleanupFailure::Failed) }),
    );
    assert!(observed.observe_resources(&mut failure).await.is_err());
    assert!(observed.observe_resources(&mut failure).await.is_err());
}
