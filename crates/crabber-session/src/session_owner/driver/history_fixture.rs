//! Actual admitted Run and independently joined native outer task.
use super::{ExecutionDriverOutcome, RegisterExecutionDriver};
use crate::Store;
use crate::session_owner::owned_scope::*;
use crabber_core::{SessionContext, SessionOwnerOperationId};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
pub(crate) struct Fixture {
    pub request: RegisterExecutionDriver,
    pub join: SessionOwnerOperationId,
}
pub(crate) async fn joined<S: Store + 'static>(
    store: Arc<S>,
    unkeyed: bool,
    dispatch: bool,
) -> Fixture {
    let admitted = if unkeyed {
        crate::session_owner::preparation_contract::resources::initial_unkeyed_driver(
            store.as_ref(),
        )
        .await
    } else {
        crate::session_owner::preparation_contract::resources::initial_driver(store.as_ref()).await
    };
    let request = RegisterExecutionDriver {
        context: SessionContext::new(
            admitted.session.id,
            admitted.session.workspace_id,
            admitted.session.directory,
        ),
        fence: admitted.fence,
    };
    let ExecutionDriverOutcome::Applied { grant, .. } = store
        .register_execution_driver(request.clone())
        .await
        .unwrap()
    else {
        panic!("fresh driver");
    };
    let count = Arc::new(AtomicUsize::new(0));
    let observed = count.clone();
    let mut main =
        OwnedTaskRegistration::register(grant.scope_source().unwrap(), move || async move {
            observed.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();
    if dispatch {
        let OwnedScopeActivationOutcome::Applied { permit, .. } = store
            .activate_owned_scope(
                main.activation().clone(),
                grant.authority().clone(),
                Some(OwnedScopeParentAuthority::ExecutionDriver(grant.clone())),
            )
            .await
            .unwrap()
        else {
            panic!("fresh activation");
        };
        main.dispatch(*permit).unwrap();
    }
    let joined = main.join().await.unwrap();
    assert_eq!(count.load(Ordering::SeqCst), usize::from(dispatch));
    let mut cleanup = OwnedCleanupTask::from_host_task(
        joined.activation().clone(),
        tokio::spawn(async { Ok(()) }),
    );
    let proof = joined.observe_resources(&mut cleanup).await.unwrap();
    let join = proof.request().unwrap().id;
    store
        .join_owned_scope(proof, grant.authority().clone())
        .await
        .unwrap();
    // A physical main join does not manufacture a terminal native Run.
    assert_eq!(
        store
            .get_run(&request.fence.run_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        crabber_core::RunStatus::Running
    );
    Fixture { request, join }
}

#[cfg(any(feature = "postgres", feature = "sqlite"))]
#[path = "history_fixture/active.rs"]
pub(crate) mod active;

/// New same-fence native work is not inside the already joined outer task.
pub(crate) async fn begin_after_join<S: Store + 'static>(store: Arc<S>) -> Fixture {
    use crate::{AttemptCommand, AttemptMutation, AttemptMutationOutcome, AttemptState};
    let fixture = joined(store.clone(), false, true).await;
    let session = fixture.request.context.session_id();
    let fence = &fixture.request.fence;
    let original = store
        .lookup_session_owner(session, &fixture.join)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        original.owner.state,
        crate::session_owner::SessionOwnerState::Joined
    );
    let execution = store.execution(fence.clone()).await.unwrap();
    // This is the genuine one-shot transition, after the original task joined.
    execution.begin_admission_execution().await.unwrap();
    let AttemptMutationOutcome::Applied {
        receipt: reserved,
        authority: Some(authority),
    } = execution
        .mutate_attempt(
            crate::attempt_contract::reserve(store.session_revision(session).await.unwrap()),
            None,
        )
        .await
        .unwrap()
    else {
        panic!("fresh same-fence reservation")
    };
    let AttemptMutationOutcome::Applied {
        receipt: begun,
        authority: None,
    } = execution
        .mutate_attempt(
            AttemptMutation {
                id: crabber_core::ControlId::new(),
                expected_revision: reserved.revision,
                command: AttemptCommand::Begin {
                    target: reserved.attempt.target,
                },
            },
            Some(authority),
        )
        .await
        .unwrap()
    else {
        panic!("fresh same-fence Begin")
    };
    assert_eq!(begun.attempt.state, AttemptState::Started);
    assert_eq!(
        store
            .execution_attempt(&fence.run_id, &begun.attempt.target.id)
            .await
            .unwrap()
            .attempt,
        begun.attempt
    );
    let run = store.get_run(&fence.run_id).await.unwrap().unwrap();
    assert_eq!(run.status, crabber_core::RunStatus::Running);
    assert_eq!(run.claim_token, fence.claim_token);
    assert_eq!(
        store
            .lookup_session_owner(session, &fixture.join)
            .await
            .unwrap(),
        Some(original)
    );
    fixture
}
