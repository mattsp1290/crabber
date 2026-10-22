//! Public changes, cancellation and a real later claim never grant stale dispatch.
use super::*;
use crate::session_owner::{
    RequestSessionOwnerCancel, SessionOwnerError as Error, SessionOwnerResolver, SessionOwnerState,
};
pub(super) async fn run(store: &impl Store, clock: &ManualClock) {
    for admission in [true, false] {
        let request = fixture::request(store, clock, admission).await;
        rename(store, &request).await;
        assert!(matches!(
            store.register_successor_execution_driver(request).await,
            Err(Error::RevisionConflict)
        ));
        for changed in 0..3 {
            let request = fixture::request(store, clock, admission).await;
            let SuccessorExecutionDriverOutcome::Applied { receipt, grant } = store
                .register_successor_execution_driver(request.clone())
                .await
                .unwrap()
            else {
                panic!("fresh worker");
            };
            let effects = Arc::new(AtomicUsize::new(0));
            let count = effects.clone();
            let mut task = OwnedTaskRegistration::register(
                grant.scope_source().unwrap(),
                move || async move {
                    count.fetch_add(1, Ordering::SeqCst);
                },
            )
            .unwrap();
            alter(store, clock, &request, &grant, admission, changed).await;
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
            let joined = task.join().await.unwrap();
            assert_eq!(joined.disposition(), OwnedMainDisposition::NoDispatch);
            let mut cleanup = OwnedCleanupTask::from_host_task(
                joined.activation().clone(),
                tokio::spawn(async { Ok(()) }),
            );
            store
                .join_owned_scope(
                    joined.observe_resources(&mut cleanup).await.unwrap(),
                    grant.authority().clone(),
                )
                .await
                .unwrap();
            assert_eq!(effects.load(Ordering::SeqCst), 0);
            assert_eq!(
                store
                    .get_session_owner(request.context.session_id(), &grant.authority().target().id)
                    .await
                    .unwrap()
                    .state,
                SessionOwnerState::Joined
            );
            assert!(
                matches!(store.register_successor_execution_driver(request).await.unwrap(),SuccessorExecutionDriverOutcome::Replayed(saved)if saved==receipt)
            );
        }
    }
}
async fn alter(
    store: &dyn Store,
    clock: &ManualClock,
    request: &RegisterSuccessorExecutionDriver,
    grant: &SuccessorExecutionDriverGrant,
    admission: bool,
    changed: u8,
) {
    match changed {
        0 => rename(store, request).await,
        1 => {
            store
                .request_session_owner_cancel(RequestSessionOwnerCancel {
                    id: crabber_core::SessionOwnerOperationId::new(),
                    session_id: request.context.session_id().clone(),
                    target: grant.authority().target().clone(),
                    expected_revision: Some(
                        store
                            .session_revision(request.context.session_id())
                            .await
                            .unwrap(),
                    ),
                    claim: SessionOwnerResolver {
                        resolver_id: "host".into(),
                        authority_claim: "test supervisor".into(),
                    },
                })
                .await
                .unwrap();
        }
        _ => {
            if admission {
                store
                    .execution(request.fence.clone())
                    .await
                    .unwrap()
                    .begin_admission_execution()
                    .await
                    .unwrap();
            }
            let run = store.get_run(&request.fence.run_id).await.unwrap().unwrap();
            clock.set(run.lease_until);
            store
                .claim_expired_run(&run.id, "later-worker")
                .await
                .unwrap();
        }
    }
}
async fn rename(store: &dyn Store, request: &RegisterSuccessorExecutionDriver) {
    store
        .manage_session(crate::ManageSession {
            id: crabber_core::ControlId::new(),
            session_id: request.context.session_id().clone(),
            expected_revision: store
                .session_revision(request.context.session_id())
                .await
                .unwrap(),
            change: crate::SessionChange::Rename {
                title: "unrelated".into(),
            },
        })
        .await
        .unwrap();
}
#[test]
fn oversized_context_refuses_before_clone() {
    let request = RegisterSuccessorExecutionDriver {
        context: crabber_core::SessionContext::new(
            crabber_core::SessionId::new(),
            "workspace",
            "x".repeat(1 << 20),
        ),
        fence: crabber_core::RunFence {
            run_id: crabber_core::RunId::new(),
            claim_token: "private-token".into(),
        },
        claim: NativeClaimLocator {
            issuance: 2,
            root_join_id: crabber_core::SessionOwnerOperationId::new(),
        },
    };
    let allocated = allocation_counter::measure(|| assert!(request.identities().is_err()));
    assert!(allocated.bytes_total < 4096);
}
