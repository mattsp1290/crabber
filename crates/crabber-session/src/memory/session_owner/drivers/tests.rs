use super::*;
use crate::Store;
use crate::session_owner::owned_scope::{
    OwnedCleanupTask, OwnedScopeActivationOutcome, OwnedScopeParentAuthority, OwnedTaskRegistration,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
struct Clock(time::OffsetDateTime);
impl crabber_core::Clock for Clock {
    fn now(&self) -> time::OffsetDateTime {
        self.0
    }
}
async fn setup() -> (MemoryStore, RegisterExecutionDriver) {
    let store = MemoryStore::with_clock(Arc::new(Clock(time::OffsetDateTime::now_utc())));
    let admitted =
        crate::session_owner::preparation_contract::resources::initial_driver(&store).await;
    let request = RegisterExecutionDriver {
        context: crabber_core::SessionContext::new(
            admitted.session.id.clone(),
            admitted.session.workspace_id,
            admitted.session.directory,
        ),
        fence: admitted.fence,
    };
    (store, request)
}
#[tokio::test]
async fn original_driver_register_ack_and_actual_main_join_are_distinct() {
    let (store, request) = setup().await;
    let ExecutionDriverOutcome::Applied { receipt, grant } = store
        .register_execution_driver(request.clone())
        .await
        .unwrap()
    else {
        panic!("fresh driver")
    };
    let count = Arc::new(AtomicUsize::new(0));
    let effect = count.clone();
    let mut task =
        OwnedTaskRegistration::register(grant.scope_source().unwrap(), move || async move {
            effect.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert!(
        matches!(store.register_execution_driver(request.clone()).await.unwrap(), ExecutionDriverOutcome::Replayed(saved) if saved == receipt)
    );
    let OwnedScopeActivationOutcome::Applied { permit, .. } = store
        .activate_owned_scope(
            task.activation().clone(),
            grant.authority().clone(),
            Some(OwnedScopeParentAuthority::ExecutionDriver(grant.clone())),
        )
        .await
        .unwrap()
    else {
        panic!("fresh permit")
    };
    task.dispatch(*permit).unwrap();
    let joined = task.join().await.unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 1);
    let mut cleanup = OwnedCleanupTask::from_host_task(
        joined.activation().clone(),
        tokio::spawn(async { Ok(()) }),
    );
    let proof = joined.observe_resources(&mut cleanup).await.unwrap();
    Store::join_owned_scope(&store, proof, grant.authority().clone())
        .await
        .unwrap();
    assert!(
        matches!(store.register_execution_driver(request).await.unwrap(), ExecutionDriverOutcome::Replayed(saved) if saved == receipt)
    );
}
#[tokio::test]
async fn driver_registration_before_and_after_commit_faults_never_issue_replayed_authority() {
    let (store, request) = setup().await;
    store.session_owner_fault.store(1, Ordering::SeqCst);
    assert!(matches!(
        store.register_execution_driver(request.clone()).await,
        Err(Error::UnknownStoreOutcome)
    ));
    assert!(
        store
            .lookup_session_owner(
                request.context.session_id(),
                &request.identities().unwrap().reserve
            )
            .await
            .unwrap()
            .is_none()
    );
    store.session_owner_fault.store(2, Ordering::SeqCst);
    assert!(matches!(
        store.register_execution_driver(request.clone()).await,
        Err(Error::UnknownStoreOutcome)
    ));
    assert!(matches!(
        store
            .register_execution_driver(request.clone())
            .await
            .unwrap(),
        ExecutionDriverOutcome::Replayed(_)
    ));
    let mut changed = request.clone();
    changed.fence.claim_token = "different-fence".into();
    assert!(matches!(
        store.register_execution_driver(changed).await,
        Err(Error::SemanticConflict)
    ));
    store
        .state
        .lock()
        .unwrap()
        .preparation_admissions
        .remove(&request.fence.run_id);
    assert!(store.register_execution_driver(request).await.is_err());
}
#[tokio::test]
async fn exact_initial_source_is_rechecked_at_activation_before_factory() {
    for corrupt in [false, true] {
        let (store, request) = setup().await;
        let ExecutionDriverOutcome::Applied { grant, .. } = store
            .register_execution_driver(request.clone())
            .await
            .unwrap()
        else {
            panic!("fresh")
        };
        let count = Arc::new(AtomicUsize::new(0));
        let effect = count.clone();
        let mut task =
            OwnedTaskRegistration::register(grant.scope_source().unwrap(), move || async move {
                effect.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();
        if corrupt {
            store
                .state
                .lock()
                .unwrap()
                .preparation_admissions
                .remove(&request.fence.run_id);
        } else {
            store
                .state
                .lock()
                .unwrap()
                .runs
                .get_mut(&request.fence.run_id)
                .unwrap()
                .claim_token = "changed".into();
        }
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
        assert_eq!(
            task.join().await.unwrap().disposition(),
            crate::session_owner::owned_scope::OwnedMainDisposition::NoDispatch
        );
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }
}

fn damage_journal(store: &MemoryStore, request: &RegisterExecutionDriver, case: usize) {
    let mut state = store.state.lock().unwrap();
    let run = &request.fence.run_id;
    match case {
        0 => {
            state
                .admission_execution_sources
                .remove(&(run.clone(), crate::admission_journal::Kind::Init));
        }
        1 => {
            state.admission_execution_protocols.remove(run);
        }
        2 => {
            let key = state.admission_executions[run].key.clone();
            state
                .receipts
                .remove(&(request.context.session_id().clone(), key));
        }
        3 => {
            state.admission_executions.get_mut(run).unwrap().state =
                crate::AdmissionExecutionState::Started;
        }
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn erased_or_started_journal_cannot_register_a_driver() {
    for case in 0..4 {
        let (store, request) = setup().await;
        damage_journal(&store, &request, case);
        assert!(
            store
                .register_execution_driver(request.clone())
                .await
                .is_err(),
            "case {case}"
        );
        assert!(
            store
                .lookup_session_owner(
                    request.context.session_id(),
                    &request.identities().unwrap().reserve
                )
                .await
                .unwrap()
                .is_none()
        );
    }
}

#[tokio::test]
async fn journal_erasure_between_registration_and_activation_never_dispatches() {
    for case in 0..4 {
        let (store, request) = setup().await;
        let ExecutionDriverOutcome::Applied { grant, .. } = store
            .register_execution_driver(request.clone())
            .await
            .unwrap()
        else {
            panic!("fresh")
        };
        let count = Arc::new(AtomicUsize::new(0));
        let effect = count.clone();
        let mut task =
            OwnedTaskRegistration::register(grant.scope_source().unwrap(), move || async move {
                effect.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();
        damage_journal(&store, &request, case);
        assert!(
            store
                .activate_owned_scope(
                    task.activation().clone(),
                    grant.authority().clone(),
                    Some(OwnedScopeParentAuthority::ExecutionDriver(grant))
                )
                .await
                .is_err(),
            "case {case}"
        );
        assert_eq!(
            task.join().await.unwrap().disposition(),
            crate::session_owner::owned_scope::OwnedMainDisposition::NoDispatch
        );
        assert_eq!(count.load(Ordering::SeqCst), 0);
    }
}

#[path = "hardening.rs"]
mod hardening;
