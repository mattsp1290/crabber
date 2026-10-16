//! An explicit original unkeyed source cannot be inferred from a missing journal.
use super::*;
use crate::Store;
use crate::session_owner::owned_scope::{
    OwnedCleanupTask, OwnedScopeActivationOutcome, OwnedScopeParentAuthority, OwnedTaskRegistration,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
async fn setup() -> (MemoryStore, RegisterExecutionDriver) {
    let store = MemoryStore::with_clock(Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    )));
    let admitted =
        crate::session_owner::preparation_contract::resources::initial_unkeyed_driver(&store).await;
    (
        store,
        RegisterExecutionDriver {
            context: crabber_core::SessionContext::new(
                admitted.session.id,
                admitted.session.workspace_id,
                admitted.session.directory,
            ),
            fence: admitted.fence,
        },
    )
}
async fn register(
    store: &MemoryStore,
    request: &RegisterExecutionDriver,
) -> Box<crate::session_owner::driver::ExecutionDriverGrant> {
    let ExecutionDriverOutcome::Applied { grant, .. } = store
        .register_execution_driver(request.clone())
        .await
        .unwrap()
    else {
        panic!("fresh unkeyed driver");
    };
    grant
}
async fn activation_refuses(
    store: &MemoryStore,
    grant: Box<crate::session_owner::driver::ExecutionDriverGrant>,
) {
    let effects = Arc::new(AtomicUsize::new(0));
    let task_effects = effects.clone();
    let task = OwnedTaskRegistration::register(grant.scope_source().unwrap(), move || async move {
        task_effects.fetch_add(1, Ordering::SeqCst);
    })
    .unwrap();
    assert!(
        store
            .activate_owned_scope(
                task.activation().clone(),
                grant.authority().clone(),
                Some(OwnedScopeParentAuthority::ExecutionDriver(grant))
            )
            .await
            .is_err()
    );
    assert_eq!(effects.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn unkeyed_original_register_dispatch_join_and_replay_have_no_keyed_receipt() {
    let (store, request) = setup().await;
    let grant = register(&store, &request).await;
    {
        let state = store.state.lock().unwrap();
        assert!(
            !state
                .admission_executions
                .contains_key(&request.fence.run_id)
        );
        assert!(
            !state
                .admission_execution_protocols
                .contains(&request.fence.run_id)
        );
        assert!(state.receipts.is_empty());
    }
    let effects = Arc::new(AtomicUsize::new(0));
    let task_effects = effects.clone();
    let mut task =
        OwnedTaskRegistration::register(grant.scope_source().unwrap(), move || async move {
            task_effects.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
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
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    task.dispatch(*permit).unwrap();
    let joined = task.join().await.unwrap();
    let mut cleanup = OwnedCleanupTask::from_host_task(
        joined.activation().clone(),
        tokio::spawn(async { Ok(()) }),
    );
    let proof = joined.observe_resources(&mut cleanup).await.unwrap();
    Store::join_owned_scope(&store, proof, grant.authority().clone())
        .await
        .unwrap();
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    // Original metadata does not depend on today's execution-binding carrier.
    store
        .state
        .lock()
        .unwrap()
        .execution_bindings
        .remove(&request.fence.run_id);
    assert!(matches!(
        store.register_execution_driver(request).await.unwrap(),
        ExecutionDriverOutcome::Replayed(_)
    ));
}
#[tokio::test]
async fn unkeyed_registration_ack_faults_preserve_original_metadata_only() {
    for point in [1, 2] {
        let (store, request) = setup().await;
        store.session_owner_fault.store(point, Ordering::SeqCst);
        assert!(matches!(
            store.register_execution_driver(request.clone()).await,
            Err(Error::UnknownStoreOutcome)
        ));
        store.session_owner_fault.store(0, Ordering::SeqCst);
        let retry = store.register_execution_driver(request).await.unwrap();
        assert_eq!(
            matches!(retry, ExecutionDriverOutcome::Replayed(_)),
            point == 2
        );
    }
}
fn change_binding(store: &MemoryStore, request: &RegisterExecutionDriver) {
    let mut state = store.state.lock().unwrap();
    let retained = state
        .execution_bindings
        .get_mut(&request.fence.run_id)
        .unwrap();
    let mut binding: crate::RetainedExecutionBinding = serde_json::from_str(retained).unwrap();
    binding.capsule.plan_fingerprint = crabber_core::InputFingerprint::new("e".repeat(64)).unwrap();
    binding.capsule.input_fingerprint = binding.capsule.computed_input_fingerprint().unwrap();
    *retained = serde_json::to_string(&binding).unwrap();
    state
        .runs
        .get_mut(&request.fence.run_id)
        .unwrap()
        .plan_fingerprint = binding.capsule.plan_fingerprint.as_str().into();
}
#[tokio::test]
async fn unkeyed_coherently_changed_current_binding_cannot_replace_original_join() {
    for activating in [false, true] {
        let (store, request) = setup().await;
        let grant = if activating {
            Some(register(&store, &request).await)
        } else {
            None
        };
        let revision = store
            .session_revision(request.context.session_id())
            .await
            .unwrap();
        change_binding(&store, &request);
        if let Some(grant) = grant {
            activation_refuses(&store, grant).await;
        } else {
            assert!(
                store
                    .register_execution_driver(request.clone())
                    .await
                    .is_err()
            );
        }
        assert_eq!(
            store
                .session_revision(request.context.session_id())
                .await
                .unwrap(),
            revision
        );
    }
}
async fn dispatch_and_erase_attempt(store: &MemoryStore, request: &RegisterExecutionDriver) {
    let revision = store
        .session_revision(request.context.session_id())
        .await
        .unwrap();
    let execution = store.execution(request.fence.clone()).await.unwrap();
    let mut reserve = crate::attempt_contract::reserve(revision);
    reserve.command = crate::AttemptCommand::ReserveBound {
        target: reserve.command.target().clone(),
    };
    let crate::AttemptMutationOutcome::Applied { receipt, authority } = execution
        .mutate_attempt(reserve.clone(), None)
        .await
        .unwrap()
    else {
        panic!("fresh actual attempt");
    };
    let begin = crate::AttemptMutation {
        id: crabber_core::ControlId::new(),
        expected_revision: receipt.revision,
        command: crate::AttemptCommand::Begin {
            target: receipt.attempt.target.clone(),
        },
    };
    execution
        .mutate_attempt(begin.clone(), authority)
        .await
        .unwrap();
    let mut state = store.state.lock().unwrap();
    state.attempts.remove(&receipt.attempt.target.id);
    state
        .operation_receipts
        .remove(&(request.context.session_id().clone(), reserve.id));
    state
        .operation_receipts
        .remove(&(request.context.session_id().clone(), begin.id));
    state.events.remove_session(request.context.session_id());
}
#[tokio::test]
async fn unkeyed_erased_actual_started_attempt_cannot_restore_first_driver_eligibility() {
    for activating in [false, true] {
        let (store, request) = setup().await;
        let grant = if activating {
            Some(register(&store, &request).await)
        } else {
            None
        };
        dispatch_and_erase_attempt(&store, &request).await;
        let revision = store
            .session_revision(request.context.session_id())
            .await
            .unwrap();
        if let Some(grant) = grant {
            activation_refuses(&store, grant).await;
        } else {
            assert!(
                store
                    .register_execution_driver(request.clone())
                    .await
                    .is_err()
            );
        }
        assert_eq!(
            store
                .session_revision(request.context.session_id())
                .await
                .unwrap(),
            revision
        );
    }
}

#[path = "unkeyed/negative.rs"]
mod negative;

#[path = "unkeyed/ack.rs"]
mod ack;
