use super::*;

#[tokio::test]
async fn explicit_cleanup_after_unknown_native_commit_reconciles_original_source() {
    for keyed in [false, true] {
        let h = Harness::new();
        h.gate.preparation_fault.store(2, Ordering::SeqCst);
        let error = if keyed {
            h.runtime
                .start_keyed(h.input.clone(), options())
                .await
                .err()
                .unwrap()
        } else {
            let mut input = h.input.clone();
            input.session_id = None;
            h.runtime.start(input).await.err().unwrap()
        };
        let descriptor = error.preparation_reconciliation().unwrap().clone();
        let original = h
            .memory
            .lookup_session_owner(&descriptor.session_id, &descriptor.join_id)
            .await
            .unwrap()
            .unwrap();
        h.runtime
            .cleanup_retained_preparation(&descriptor)
            .await
            .unwrap();
        h.finish_cleanup(&descriptor).await;
        assert_eq!(
            h.memory
                .lookup_session_owner(&descriptor.session_id, &descriptor.join_id)
                .await
                .unwrap()
                .unwrap(),
            original
        );
        assert_eq!(h.provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(h.fake.requests().len(), 0);
    }
}

#[tokio::test]
async fn unknown_activation_ack_never_opens_factory_and_explicit_cleanup_observes_no_dispatch() {
    for point in [1, 2] {
        let h = Harness::new();
        h.gate.activation_fault.store(point, Ordering::SeqCst);
        let error = h
            .runtime
            .start_keyed(h.input.clone(), options())
            .await
            .err()
            .unwrap();
        let descriptor = error.preparation_reconciliation().unwrap().clone();
        assert_eq!(h.provider.calls.load(Ordering::SeqCst), 0);
        assert_eq!(h.fake.requests().len(), 0);
        h.runtime
            .cleanup_retained_preparation(&descriptor)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !h
                .runtime
                .retained_preparation_status(&descriptor.session_id)
                .is_empty()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(h.child_state(&descriptor).await, SessionOwnerState::Joined);
        assert_eq!(
            h.memory
                .get_session_owner(&descriptor.session_id, &descriptor.owner.id)
                .await
                .unwrap()
                .state,
            SessionOwnerState::Joined
        );
        assert!(
            h.memory
                .lookup_admission(&descriptor.session_id, &options().key)
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn cleanup_join_ack_retry_retains_actual_observation_and_never_dispatches() {
    for point in [1, 2] {
        let h = Harness::new();
        h.gate.owned_join_fault.store(point, Ordering::SeqCst);
        let crate::Admission::Started { handle, .. } = h
            .runtime
            .start_keyed(h.input.clone(), options())
            .await
            .unwrap()
        else {
            panic!("fresh native")
        };
        let descriptor = h.descriptor().await;
        handle.done().await.unwrap();
        h.provider.cleanup_started.notified().await;
        h.provider.cleanup_release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let status = h
                    .runtime
                    .retained_preparation_status(&descriptor.session_id);
                if status.first().is_some_and(|value| {
                    value.status == crate::RetainedPreparationStatus::CleanupUncertain
                }) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            h.child_state(&descriptor).await,
            if point == 1 {
                SessionOwnerState::Active
            } else {
                SessionOwnerState::Joined
            }
        );
        h.runtime
            .cleanup_retained_preparation(&descriptor)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !h
                .runtime
                .retained_preparation_status(&descriptor.session_id)
                .is_empty()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(h.child_state(&descriptor).await, SessionOwnerState::Joined);
        assert_eq!(h.provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(h.provider.cleanup_completed.load(Ordering::SeqCst), 1);
        assert_eq!(h.fake.requests().len(), 1);
    }
}

#[tokio::test]
async fn malformed_cleanup_ack_retains_original_observation_for_exact_retry() {
    for malformed in 1..=5 {
        let h = Harness::new();
        h.gate
            .malformed_owned_join
            .store(malformed, Ordering::SeqCst);
        let crate::Admission::Started { handle, .. } = h
            .runtime
            .start_keyed(h.input.clone(), options())
            .await
            .unwrap()
        else {
            panic!("fresh native")
        };
        let descriptor = h.descriptor().await;
        handle.done().await.unwrap();
        h.provider.cleanup_started.notified().await;
        h.provider.cleanup_release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if h.runtime
                    .retained_preparation_status(&descriptor.session_id)
                    .first()
                    .is_some_and(|value| {
                        value.status == crate::RetainedPreparationStatus::CleanupUncertain
                    })
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        h.runtime
            .cleanup_retained_preparation(&descriptor)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !h
                .runtime
                .retained_preparation_status(&descriptor.session_id)
                .is_empty()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(h.provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(h.provider.cleanup_completed.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn explicit_failure_cleanup_joins_actual_scope_before_new_work() {
    let h = Harness::new();
    h.gate.preparation_fault.store(1, Ordering::SeqCst);
    let error = h
        .runtime
        .start_keyed(h.input.clone(), options())
        .await
        .err()
        .unwrap();
    let descriptor = error.preparation_reconciliation().unwrap().clone();
    h.runtime
        .cleanup_retained_preparation(&descriptor)
        .await
        .unwrap();
    h.finish_cleanup(&descriptor).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while h
            .memory
            .get_session_owner(&descriptor.session_id, &descriptor.owner.id)
            .await
            .unwrap()
            .state
            != SessionOwnerState::Joined
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        h.memory
            .lookup_admission(&descriptor.session_id, &options().key)
            .await
            .unwrap()
            .is_none()
    );
    let mut next = options();
    next.key = AdmissionKey::new("after-observed-cleanup").unwrap();
    let crate::Admission::Started { handle, .. } =
        h.runtime.start_keyed(h.input.clone(), next).await.unwrap()
    else {
        panic!("new work")
    };
    handle.done().await.unwrap();
    h.provider.cleanup_release.notify_one();
    assert_eq!(h.provider.calls.load(Ordering::SeqCst), 2);
    assert_eq!(h.fake.requests().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn running_factory_cancel_is_observed_after_signal_and_waits_for_cleanup() {
    use crabber_session::session_owner::{RequestSessionOwnerCancel, SessionOwnerResolver};
    let h = Harness::new();
    h.provider.wait_for_close.store(true, Ordering::SeqCst);
    h.gate.owner_observe_ack_lost.store(true, Ordering::SeqCst);
    let runtime = h.runtime.clone();
    let input = h.input.clone();
    let waiter = tokio::spawn(async move { runtime.start_keyed(input, options()).await });
    h.provider.entered.notified().await;
    let descriptor = h.descriptor().await;
    let requested = RequestSessionOwnerCancel {
        id: crabber_core::SessionOwnerOperationId::new(),
        session_id: descriptor.session_id.clone(),
        target: descriptor.owner.clone(),
        expected_revision: None,
        claim: SessionOwnerResolver {
            resolver_id: "host".into(),
            authority_claim: "cancel own preparation".into(),
        },
    };
    h.memory
        .request_session_owner_cancel(requested.clone())
        .await
        .unwrap();
    tokio::time::advance(Duration::from_secs(5)).await;
    let error = waiter.await.unwrap().err().unwrap();
    assert_eq!(error.preparation_reconciliation(), Some(&descriptor));
    let owner = h
        .memory
        .get_session_owner(&descriptor.session_id, &descriptor.owner.id)
        .await
        .unwrap();
    assert_eq!(owner.state, SessionOwnerState::Active);
    assert_eq!(owner.cancellation.unwrap().request_id, requested.id);
    let keys = h.gate.owner_observe_keys.lock().unwrap().clone();
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0], keys[1]);
    assert_eq!(h.fake.requests().len(), 0);
    h.runtime
        .cleanup_retained_preparation(&descriptor)
        .await
        .unwrap();
    h.finish_cleanup(&descriptor).await;
    assert!(
        h.memory
            .lookup_admission(&descriptor.session_id, &options().key)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn unkeyed_unknown_commit_retries_metadata_and_retains_resources_until_join() {
    let h = Harness::new();
    h.gate.preparation_fault.store(2, Ordering::SeqCst);
    let mut fresh = h.input.clone();
    fresh.session_id = None;
    let error = h.runtime.start(fresh).await.err().unwrap();
    let descriptor = error.preparation_reconciliation().unwrap().clone();
    assert_eq!(h.provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(h.child_state(&descriptor).await, SessionOwnerState::Active);
    let progress = h
        .runtime
        .retry_preparation_completion(&descriptor)
        .await
        .unwrap();
    assert!(progress.replayed);
    let receipt = progress.unkeyed_admission.unwrap();
    assert_eq!(receipt.session_id, descriptor.session_id);
    let original = h.memory.get_run(&receipt.run_id).await.unwrap().unwrap();
    assert_eq!(original.status, RunStatus::Running);
    h.finish_cleanup(&descriptor).await;
    assert_eq!(h.provider.calls.load(Ordering::SeqCst), 1);
    assert_eq!(h.fake.requests().len(), 0);
}
