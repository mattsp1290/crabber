//! Actual fresh gated dispatch, with metadata retries granting no second effect.
use super::super::*;
pub(crate) async fn exercise(store: &impl Store, request: RegisterSuccessorExecutionDriver) {
    let SuccessorExecutionDriverOutcome::Applied { receipt, grant } = store
        .register_successor_execution_driver(request.clone())
        .await
        .unwrap()
    else {
        panic!("fresh compound successor");
    };
    assert!(
        matches!(store.register_successor_execution_driver(request.clone()).await.unwrap(), SuccessorExecutionDriverOutcome::Replayed(saved) if saved == receipt)
    );
    let mut changed = request.clone();
    changed.fence.claim_token.push('x');
    assert!(
        store
            .register_successor_execution_driver(changed)
            .await
            .is_err()
    );
    let effects = Arc::new(AtomicUsize::new(0));
    let count = effects.clone();
    let mut task =
        OwnedTaskRegistration::register(grant.scope_source().unwrap(), move || async move {
            count.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    let activation = task.activation().clone();
    let OwnedScopeActivationOutcome::Applied {
        permit,
        receipt: activated,
    } = store
        .activate_owned_scope(
            activation.clone(),
            grant.authority().clone(),
            Some(OwnedScopeParentAuthority::SuccessorExecutionDriver(
                grant.clone(),
            )),
        )
        .await
        .unwrap()
    else {
        panic!("fresh exact compound permit");
    };
    assert!(
        matches!(store.activate_owned_scope(activation, grant.authority().clone(), Some(OwnedScopeParentAuthority::SuccessorExecutionDriver(grant.clone()))).await.unwrap(), OwnedScopeActivationOutcome::Replayed(saved) if saved == activated)
    );
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    task.dispatch(*permit).unwrap();
    let joined = task.join().await.unwrap();
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    let mut resources = OwnedCleanupTask::from_host_task(
        joined.activation().clone(),
        tokio::spawn(async { Ok(()) }),
    );
    store
        .join_owned_scope(
            joined.observe_resources(&mut resources).await.unwrap(),
            grant.authority().clone(),
        )
        .await
        .unwrap();
    assert!(
        matches!(store.register_successor_execution_driver(request).await.unwrap(), SuccessorExecutionDriverOutcome::Replayed(saved) if saved == receipt)
    );
    assert_eq!(effects.load(Ordering::SeqCst), 1);
}

pub(crate) async fn faults(
    store: &impl Store,
    request: RegisterSuccessorExecutionDriver,
    boundary: u8,
    fault: &impl Fn(u8),
) {
    fault(boundary);
    assert!(matches!(
        store
            .register_successor_execution_driver(request.clone())
            .await,
        Err(crate::session_owner::SessionOwnerError::UnknownStoreOutcome)
    ));
    fault(0);
    match store
        .register_successor_execution_driver(request.clone())
        .await
        .unwrap()
    {
        SuccessorExecutionDriverOutcome::Applied { grant, .. } if boundary == 1 => {
            super::super::faults::exercise(store, grant, boundary, fault).await;
        }
        SuccessorExecutionDriverOutcome::Replayed(receipt) if boundary == 2 => {
            assert!(
                matches!(store.register_successor_execution_driver(request).await.unwrap(), SuccessorExecutionDriverOutcome::Replayed(saved) if saved == receipt)
            );
            // Unknown committed Register ACK cannot reconstruct its lost grant.
            assert_eq!(
                store
                    .get_session_owner(
                        &receipt.worker.session_id,
                        &receipt.worker.begin.owner.target.id
                    )
                    .await
                    .unwrap()
                    .state,
                crate::session_owner::SessionOwnerState::Active
            );
        }
        _ => panic!("exact fault reconciliation"),
    }
}
pub(crate) async fn activation_fault(
    store: &impl Store,
    request: RegisterSuccessorExecutionDriver,
    fault: &impl Fn(u8),
) {
    let SuccessorExecutionDriverOutcome::Applied { grant, .. } = store
        .register_successor_execution_driver(request)
        .await
        .unwrap()
    else {
        panic!("fresh fault grant");
    };
    super::super::faults::exercise(store, grant, 2, fault).await;
}

/// Cancellation, lease expiry and unrelated public state never grant the waiting gate.
pub(crate) async fn refusals(
    store: &impl Store,
    request: RegisterSuccessorExecutionDriver,
    clock: &ManualClock,
    mode: u8,
) {
    if mode == 7 {
        advance(store, &request).await;
        assert!(
            store
                .register_successor_execution_driver(request)
                .await
                .is_err()
        );
        return;
    }
    let SuccessorExecutionDriverOutcome::Applied { receipt, grant } = store
        .register_successor_execution_driver(request.clone())
        .await
        .unwrap()
    else {
        panic!("fresh refusal grant");
    };
    let effects = Arc::new(AtomicUsize::new(0));
    let count = effects.clone();
    let mut task =
        OwnedTaskRegistration::register(grant.scope_source().unwrap(), move || async move {
            count.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    match mode {
        4 => {
            let revision = store
                .session_revision(request.context.session_id())
                .await
                .unwrap();
            let cancel = crate::session_owner::cancellation_contract::request(
                &receipt.worker.begin.owner,
                Some(revision),
            );
            store.request_session_owner_cancel(cancel).await.unwrap();
        }
        5 => clock.set(
            store
                .get_run(&request.fence.run_id)
                .await
                .unwrap()
                .unwrap()
                .lease_until,
        ),
        6 => advance(store, &request).await,
        _ => unreachable!(),
    }
    assert!(
        store
            .activate_owned_scope(
                task.activation().clone(),
                grant.authority().clone(),
                Some(OwnedScopeParentAuthority::SuccessorExecutionDriver(
                    grant.clone()
                ))
            )
            .await
            .is_err()
    );
    assert!(
        matches!(store.register_successor_execution_driver(request).await.unwrap(), SuccessorExecutionDriverOutcome::Replayed(saved) if saved == receipt)
    );
    let joined = task.join().await.unwrap();
    let mut resources = OwnedCleanupTask::from_host_task(
        joined.activation().clone(),
        tokio::spawn(async { Ok(()) }),
    );
    let _actual_join = joined.observe_resources(&mut resources).await.unwrap();
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    // A refusal or expiry is not native Joined. The explicit blocker remains.
    assert_eq!(
        store
            .get_session_owner(
                &receipt.worker.session_id,
                &receipt.worker.begin.owner.target.id
            )
            .await
            .unwrap()
            .state,
        crate::session_owner::SessionOwnerState::Active
    );
}
async fn advance(store: &impl Store, request: &RegisterSuccessorExecutionDriver) {
    store
        .execution(request.fence.clone())
        .await
        .unwrap()
        .mutate_public_state(crate::PublicStateMutation {
            id: crabber_core::ControlId::new(),
            expected_revision: store
                .session_revision(request.context.session_id())
                .await
                .unwrap(),
            contents: crabber_core::PublicStateContents::new(
                serde_json::json!({"unrelated":true}),
                Vec::new(),
            )
            .unwrap(),
        })
        .await
        .unwrap();
}
