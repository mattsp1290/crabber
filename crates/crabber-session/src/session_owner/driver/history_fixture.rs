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
