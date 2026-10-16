//! Genuine native first-driver task ownership on every Store adapter.
use crate::{
    Store,
    session_owner::{driver::*, owned_scope::*},
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
pub(crate) async fn request(store: &impl Store) -> RegisterExecutionDriver {
    let admitted =
        crate::session_owner::preparation_contract::resources::initial_driver(store).await;
    RegisterExecutionDriver {
        context: crabber_core::SessionContext::new(
            admitted.session.id,
            admitted.session.workspace_id,
            admitted.session.directory,
        ),
        fence: admitted.fence,
    }
}
pub(crate) async fn request_unkeyed(store: &impl Store) -> RegisterExecutionDriver {
    let admitted =
        crate::session_owner::preparation_contract::resources::initial_unkeyed_driver(store).await;
    RegisterExecutionDriver {
        context: crabber_core::SessionContext::new(
            admitted.session.id,
            admitted.session.workspace_id,
            admitted.session.directory,
        ),
        fence: admitted.fence,
    }
}
async fn request_for(store: &impl Store, unkeyed: bool) -> RegisterExecutionDriver {
    if unkeyed {
        request_unkeyed(store).await
    } else {
        request(store).await
    }
}
pub(crate) async fn run_unkeyed<S: Store + 'static>(store: Arc<S>) {
    run_for(store, true).await;
}
pub(crate) async fn run<S: Store + 'static>(store: Arc<S>) {
    run_for(store, false).await;
}
async fn run_for<S: Store + 'static>(store: Arc<S>, unkeyed: bool) {
    assert!(store.supports_execution_drivers());
    let request = request_for(store.as_ref(), unkeyed).await;
    let ExecutionDriverOutcome::Applied { receipt, grant } = store
        .register_execution_driver(request.clone())
        .await
        .unwrap()
    else {
        panic!("fresh driver");
    };
    assert!(
        matches!(store.register_execution_driver(request.clone()).await.unwrap(),ExecutionDriverOutcome::Replayed(saved) if saved==receipt)
    );
    rejects_changed_metadata(store.as_ref(), &request, &receipt, &grant).await;
    let count = Arc::new(AtomicUsize::new(0));
    let effect = count.clone();
    let worker = store.clone();
    let fence = request.fence.clone();
    let session = request.context.session_id().clone();
    let mut task =
        OwnedTaskRegistration::register(grant.scope_source().unwrap(), move || async move {
            if unkeyed {
                begin_unkeyed(worker.as_ref(), &session, fence).await;
            } else {
                worker
                    .execution(fence)
                    .await
                    .unwrap()
                    .begin_admission_execution()
                    .await
                    .unwrap();
            }
            effect.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 0);
    let OwnedScopeActivationOutcome::Applied { permit, .. } = store
        .activate_owned_scope(
            task.activation().clone(),
            grant.authority().clone(),
            Some(OwnedScopeParentAuthority::ExecutionDriver(grant.clone())),
        )
        .await
        .unwrap()
    else {
        panic!("fresh permit");
    };
    task.dispatch(*permit).unwrap();
    let joined = task.join().await.unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 1);
    let mut cleanup = OwnedCleanupTask::from_host_task(
        joined.activation().clone(),
        tokio::spawn(async { Ok(()) }),
    );
    let proof = joined.observe_resources(&mut cleanup).await.unwrap();
    store
        .join_owned_scope(proof, grant.authority().clone())
        .await
        .unwrap();
    assert!(
        matches!(store.register_execution_driver(request).await.unwrap(),ExecutionDriverOutcome::Replayed(saved) if saved==receipt)
    );
}
async fn begin_unkeyed(
    store: &impl Store,
    session: &crabber_core::SessionId,
    fence: crabber_core::RunFence,
) {
    let execution = store.execution(fence).await.unwrap();
    let mut reserve =
        crate::attempt_contract::reserve(store.session_revision(session).await.unwrap());
    reserve.command = crate::AttemptCommand::ReserveBound {
        target: reserve.command.target().clone(),
    };
    let crate::AttemptMutationOutcome::Applied { receipt, authority } =
        execution.mutate_attempt(reserve, None).await.unwrap()
    else {
        panic!("fresh unkeyed attempt");
    };
    execution
        .mutate_attempt(
            crate::AttemptMutation {
                id: crabber_core::ControlId::new(),
                expected_revision: receipt.revision,
                command: crate::AttemptCommand::Begin {
                    target: receipt.attempt.target,
                },
            },
            authority,
        )
        .await
        .unwrap();
}
#[tokio::test]
async fn memory_unkeyed_driver_contract() {
    run_unkeyed(Arc::new(crate::MemoryStore::new())).await;
}
#[tokio::test]
async fn memory_initial_driver_contract() {
    run(Arc::new(crate::MemoryStore::new())).await;
}

#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub(crate) async fn faults<S: Store + 'static>(store: Arc<S>, fault: impl Fn(u8)) {
    faults_for(store, fault, false).await;
}
#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub(crate) async fn faults_unkeyed<S: Store + 'static>(store: Arc<S>, fault: impl Fn(u8)) {
    faults_for(store, fault, true).await;
}
#[cfg(any(feature = "postgres", feature = "sqlite"))]
async fn faults_for<S: Store + 'static>(store: Arc<S>, fault: impl Fn(u8), unkeyed: bool) {
    for boundary in [1, 2] {
        fault(0);
        let request = request_for(store.as_ref(), unkeyed).await;
        fault(boundary);
        assert!(matches!(
            store.register_execution_driver(request.clone()).await,
            Err(crate::session_owner::SessionOwnerError::UnknownStoreOutcome)
        ));
        fault(0);
        let retry = store.register_execution_driver(request).await.unwrap();
        assert_eq!(
            matches!(retry, ExecutionDriverOutcome::Replayed(_)),
            boundary == 2
        );
    }
    for boundary in [1, 2] {
        activation_fault(store.clone(), &fault, boundary, unkeyed).await;
    }
}
#[cfg(any(feature = "postgres", feature = "sqlite"))]
async fn activation_fault<S: Store + 'static>(
    store: Arc<S>,
    fault: &impl Fn(u8),
    boundary: u8,
    unkeyed: bool,
) {
    fault(0);
    let request = request_for(store.as_ref(), unkeyed).await;
    let ExecutionDriverOutcome::Applied { grant, .. } =
        store.register_execution_driver(request).await.unwrap()
    else {
        panic!("fresh");
    };
    let count = Arc::new(AtomicUsize::new(0));
    let effect = count.clone();
    let mut task =
        OwnedTaskRegistration::register(grant.scope_source().unwrap(), move || async move {
            effect.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    fault(boundary);
    assert!(matches!(
        store
            .activate_owned_scope(
                task.activation().clone(),
                grant.authority().clone(),
                Some(OwnedScopeParentAuthority::ExecutionDriver(grant.clone()))
            )
            .await,
        Err(crate::session_owner::SessionOwnerError::UnknownStoreOutcome)
    ));
    fault(0);
    match store
        .activate_owned_scope(
            task.activation().clone(),
            grant.authority().clone(),
            Some(OwnedScopeParentAuthority::ExecutionDriver(grant)),
        )
        .await
        .unwrap()
    {
        OwnedScopeActivationOutcome::Applied { permit, .. } => {
            assert_eq!(boundary, 1);
            task.dispatch(*permit).unwrap();
        }
        OwnedScopeActivationOutcome::Replayed(_) => assert_eq!(boundary, 2),
    }
    let joined = task.join().await.unwrap();
    assert_eq!(count.load(Ordering::SeqCst), usize::from(boundary == 1));
    assert_eq!(
        joined.disposition() == OwnedMainDisposition::NoDispatch,
        boundary == 2
    );
}

async fn rejects_changed_metadata(
    store: &impl Store,
    request: &RegisterExecutionDriver,
    receipt: &ExecutionDriverReceipt,
    grant: &ExecutionDriverGrant,
) {
    let mut changed = request.clone();
    changed.fence.claim_token = "replacement-fence".into();
    assert!(matches!(
        store.register_execution_driver(changed).await,
        Err(crate::session_owner::SessionOwnerError::SemanticConflict)
    ));
    let fake = ExecutionDriverGrant::from_fresh_begin(
        receipt.clone(),
        grant.authority().clone(),
        request.clone(),
        crabber_core::InputFingerprint::new("f".repeat(64)).unwrap(),
    )
    .unwrap();
    let mut task = OwnedTaskRegistration::register(fake.scope_source().unwrap(), || async {
        panic!("substituted identity dispatched");
    })
    .unwrap();
    assert!(matches!(
        store
            .activate_owned_scope(
                task.activation().clone(),
                grant.authority().clone(),
                Some(OwnedScopeParentAuthority::ExecutionDriver(Box::new(fake)))
            )
            .await,
        Err(crate::session_owner::SessionOwnerError::SemanticConflict)
    ));
    assert_eq!(
        task.join().await.unwrap().disposition(),
        OwnedMainDisposition::NoDispatch
    );
}

#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub(crate) async fn concurrent<S: Store + 'static>(store: Arc<S>) -> RegisterExecutionDriver {
    concurrent_for(store, false).await
}
#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub(crate) async fn concurrent_unkeyed<S: Store + 'static>(
    store: Arc<S>,
) -> RegisterExecutionDriver {
    concurrent_for(store, true).await
}
#[cfg(any(feature = "postgres", feature = "sqlite"))]
async fn concurrent_for<S: Store + 'static>(
    store: Arc<S>,
    unkeyed: bool,
) -> RegisterExecutionDriver {
    let request = request_for(store.as_ref(), unkeyed).await;
    let (first, second) = tokio::join!(
        store.register_execution_driver(request.clone()),
        store.register_execution_driver(request.clone())
    );
    let outcomes = [first.unwrap(), second.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, ExecutionDriverOutcome::Applied { .. }))
            .count(),
        1
    );
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, ExecutionDriverOutcome::Replayed(_)))
            .count(),
        1
    );
    request
}
