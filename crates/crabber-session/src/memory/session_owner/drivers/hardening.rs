//! Original source removal and rolled-back current labels never open a gate.
use super::*;
use crate::session_owner::{RequestSessionOwnerCancel, SessionOwnerResolver, SessionOwnerTarget};

fn child(store: &MemoryStore, request: &RegisterExecutionDriver) -> SessionOwnerTarget {
    let state = store.state.lock().unwrap();
    let witness = state
        .preparation_admissions
        .get(&request.fence.run_id)
        .unwrap();
    let begin = witness.audit_ids().unwrap()[4].clone();
    state
        .session_owners
        .source_audit(request.context.session_id(), &begin)
        .unwrap()
        .receipt
        .owner
        .target
        .clone()
}

async fn registered(
    store: &MemoryStore,
    request: &RegisterExecutionDriver,
) -> Box<crate::session_owner::driver::ExecutionDriverGrant> {
    let ExecutionDriverOutcome::Applied { grant, .. } = store
        .register_execution_driver(request.clone())
        .await
        .unwrap()
    else {
        panic!("fresh driver");
    };
    grant
}

async fn refused_activation(
    store: &MemoryStore,
    grant: Box<crate::session_owner::driver::ExecutionDriverGrant>,
) {
    let count = Arc::new(AtomicUsize::new(0));
    let effect = count.clone();
    let mut task =
        OwnedTaskRegistration::register(grant.scope_source().unwrap(), move || async move {
            effect.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    assert!(matches!(
        store
            .activate_owned_scope(
                task.activation().clone(),
                grant.authority().clone(),
                Some(OwnedScopeParentAuthority::ExecutionDriver(grant)),
            )
            .await,
        Err(Error::UnknownStoreOutcome)
    ));
    assert_eq!(
        task.join().await.unwrap().disposition(),
        crate::session_owner::owned_scope::OwnedMainDisposition::NoDispatch
    );
    assert_eq!(count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn erased_child_cancel_with_restored_current_still_blocks_driver_activation() {
    let (store, request) = setup().await;
    let grant = registered(&store, &request).await;
    let child = child(&store, &request);
    let cancel = crabber_core::SessionOwnerOperationId::new();
    store
        .request_session_owner_cancel(RequestSessionOwnerCancel {
            id: cancel.clone(),
            session_id: request.context.session_id().clone(),
            target: child,
            expected_revision: Some(
                store
                    .session_revision(request.context.session_id())
                    .await
                    .unwrap(),
            ),
            claim: SessionOwnerResolver {
                resolver_id: "test-host".into(),
                authority_claim: "stop child".into(),
            },
        })
        .await
        .unwrap();
    store
        .state
        .lock()
        .unwrap()
        .session_owners
        .rollback_driver_child_cancel_for_test(request.context.session_id(), &cancel);
    refused_activation(&store, grant).await;
}

#[tokio::test]
async fn oversized_actual_child_is_charged_before_driver_mutation_clone() {
    for activate in [false, true] {
        let (store, request) = setup().await;
        let grant = if activate {
            Some(registered(&store, &request).await)
        } else {
            None
        };
        let child = child(&store, &request);
        store
            .state
            .lock()
            .unwrap()
            .session_owners
            .oversize_driver_child_for_test(request.context.session_id(), &child.id);
        let mut result = None;
        let allocation = if let Some(grant) = grant {
            let mut task =
                OwnedTaskRegistration::register(grant.scope_source().unwrap(), || async {})
                    .unwrap();
            let activation = task.activation().clone();
            let parent = OwnedScopeParentAuthority::ExecutionDriver(grant.clone());
            let measured = allocation_counter::measure(|| {
                result = Some(
                    store
                        .publish_owned_scope_activation(
                            &activation,
                            grant.authority(),
                            Some(&parent),
                        )
                        .map(|_| ()),
                );
            });
            assert_eq!(
                task.join().await.unwrap().disposition(),
                crate::session_owner::owned_scope::OwnedMainDisposition::NoDispatch
            );
            measured
        } else {
            allocation_counter::measure(|| {
                result = Some(store.register_driver(&request).map(|_| ()));
            })
        };
        assert!(matches!(result, Some(Err(Error::UnknownStoreOutcome))));
        // Ordinary source validation allocates small bounded carriers. A copy
        // of the selected >32MiB owner would exceed this bound by over 32x.
        assert!(
            allocation.bytes_total < 1024 * 1024,
            "oversized selected child was copied before refusal: {}",
            allocation.bytes_total
        );
    }
}

#[tokio::test]
async fn original_owned_declaration_tamper_is_not_a_legacy_fallback() {
    for activate in [false, true] {
        let (store, request) = setup().await;
        let grant = if activate {
            Some(registered(&store, &request).await)
        } else {
            None
        };
        {
            let mut state = store.state.lock().unwrap();
            let reserve = state
                .preparation_admissions
                .get(&request.fence.run_id)
                .unwrap()
                .audit_ids()
                .unwrap()[0]
                .clone();
            state
                .session_owners
                .erase_driver_original_declaration_for_test(request.context.session_id(), &reserve);
        }
        if let Some(grant) = grant {
            refused_activation(&store, grant).await;
        } else {
            assert!(matches!(
                store.register_execution_driver(request).await,
                Err(Error::UnknownStoreOutcome)
            ));
        }
    }
}

#[tokio::test]
async fn driver_replay_requires_original_journal_but_not_current_unstarted_label() {
    let (store, request) = setup().await;
    let ExecutionDriverOutcome::Applied {
        receipt: original, ..
    } = store
        .register_execution_driver(request.clone())
        .await
        .unwrap()
    else {
        panic!("fresh driver");
    };
    let key = (
        request.fence.run_id.clone(),
        crate::admission_journal::Kind::Init,
    );
    let saved = store
        .state
        .lock()
        .unwrap()
        .admission_execution_sources
        .remove(&key)
        .unwrap();
    assert!(matches!(
        store.register_execution_driver(request.clone()).await,
        Err(Error::UnknownStoreOutcome)
    ));
    store
        .state
        .lock()
        .unwrap()
        .admission_execution_sources
        .insert(key, saved);
    let receipt_key = {
        let state = store.state.lock().unwrap();
        (
            request.context.session_id().clone(),
            state
                .admission_executions
                .get(&request.fence.run_id)
                .unwrap()
                .key
                .clone(),
        )
    };
    let saved = store
        .state
        .lock()
        .unwrap()
        .receipts
        .remove(&receipt_key)
        .unwrap();
    assert!(matches!(
        store.register_execution_driver(request.clone()).await,
        Err(Error::UnknownStoreOutcome)
    ));
    store
        .state
        .lock()
        .unwrap()
        .receipts
        .insert(receipt_key, saved);
    assert!(
        store
            .state
            .lock()
            .unwrap()
            .admission_execution_protocols
            .remove(&request.fence.run_id)
    );
    assert!(matches!(
        store.register_execution_driver(request.clone()).await,
        Err(Error::UnknownStoreOutcome)
    ));
    store
        .state
        .lock()
        .unwrap()
        .admission_execution_protocols
        .insert(request.fence.run_id.clone());
    store
        .execution(request.fence.clone())
        .await
        .unwrap()
        .begin_admission_execution()
        .await
        .unwrap();
    assert!(
        matches!(store.register_execution_driver(request).await.unwrap(),
        ExecutionDriverOutcome::Replayed(receipt) if receipt == original)
    );
}

#[tokio::test]
async fn genuinely_legacy_none_declaration_does_not_gain_driver_capability() {
    let store = MemoryStore::new();
    let receipt =
        crate::session_owner::preparation_contract::resources::successful(&store, false).await;
    let session = store
        .get_session(&receipt.session_id)
        .await
        .unwrap()
        .unwrap();
    let run = store.get_run(&receipt.run_id).await.unwrap().unwrap();
    let request = RegisterExecutionDriver {
        context: crabber_core::SessionContext::new(
            session.id,
            session.workspace_id,
            session.directory,
        ),
        fence: crabber_core::RunFence {
            run_id: run.id,
            claim_token: run.claim_token,
        },
    };
    assert!(matches!(
        store.register_execution_driver(request).await,
        Err(Error::Unsupported)
    ));
}

#[tokio::test]
async fn replay_rejects_oversized_original_init_before_digest_allocation() {
    let (store, request) = setup().await;
    let _grant = registered(&store, &request).await;
    let key = (
        request.fence.run_id.clone(),
        crate::admission_journal::Kind::Init,
    );
    {
        let mut state = store.state.lock().unwrap();
        let source = state.admission_execution_sources.get(&key).unwrap();
        let mut value = serde_json::to_value(source).unwrap();
        value["source"]["capsule_digest"] = serde_json::Value::String("x".repeat(1 << 20));
        // Inject a coherent typed carrier so the production reader, rather
        // than fixture encoding, must enforce the actual 8 KiB source bound.
        let oversized = serde_json::from_value(value).unwrap();
        state.admission_execution_sources.insert(key, oversized);
    }
    let mut result = None;
    let allocation = allocation_counter::measure(|| {
        result = Some(store.register_driver(&request));
    });
    assert!(matches!(result, Some(Err(Error::UnknownStoreOutcome))));
    assert!(
        allocation.bytes_total < 1 << 20,
        "oversized Init serialized before its source bound: {}",
        allocation.bytes_total
    );
}

#[tokio::test]
async fn adapter_grant_metadata_cannot_replace_native_driver_source_or_fence() {
    for changed_fence in [false, true] {
        let (store, request) = setup().await;
        let original = registered(&store, &request).await;
        let mut receipt = original.receipt().clone();
        let mut supplied = request;
        let source_identity = if changed_fence {
            supplied.fence.claim_token = "replacement-fence".into();
            receipt.request_digest = supplied.fingerprint().unwrap();
            original.source_identity().clone()
        } else {
            crabber_core::InputFingerprint::new("0".repeat(64)).unwrap()
        };
        let grant = crate::session_owner::driver::ExecutionDriverGrant::from_fresh_begin(
            receipt,
            original.authority().clone(),
            supplied,
            source_identity,
        )
        .unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let effect = count.clone();
        let mut task =
            OwnedTaskRegistration::register(grant.scope_source().unwrap(), move || async move {
                effect.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();
        assert!(
            store
                .activate_owned_scope(
                    task.activation().clone(),
                    grant.authority().clone(),
                    Some(OwnedScopeParentAuthority::ExecutionDriver(Box::new(grant)))
                )
                .await
                .is_err()
        );
        assert_eq!(
            task.join().await.unwrap().disposition(),
            crate::session_owner::owned_scope::OwnedMainDisposition::NoDispatch
        );
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }
}
