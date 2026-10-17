//! Actual initial/successor registration and activation require Raw origin.
use crate::Store;
use crate::session_owner::{
    SessionOwnerAuthority, SessionOwnerError as Error,
    driver::{
        ExecutionDriverOutcome, RegisterExecutionDriver, RegisterSuccessorExecutionDriver,
        SuccessorExecutionDriverOutcome,
    },
    owned_scope::{
        OwnedCleanupTask, OwnedScopeActivationOutcome, OwnedScopeParentAuthority, OwnedScopeSource,
        OwnedTaskRegistration,
    },
};
use crabber_core::{ManualClock, RunId, SessionContext, SessionId};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[async_trait::async_trait]
pub(crate) trait Damage: Store {
    type Saved: Send;
    async fn damage(&self, session: &SessionId, run: &RunId) -> Self::Saved;
    async fn restore(&self, session: &SessionId, run: &RunId, saved: Self::Saved);
}
enum Request {
    Initial(Box<RegisterExecutionDriver>),
    Successor(Box<RegisterSuccessorExecutionDriver>),
}
impl Request {
    async fn new(store: &impl Store, clock: &ManualClock, family: usize) -> Self {
        if family == 0 {
            let admitted =
                crate::session_owner::preparation_contract::resources::initial_driver(store).await;
            Self::Initial(Box::new(RegisterExecutionDriver {
                context: SessionContext::new(
                    admitted.session.id,
                    admitted.session.workspace_id,
                    admitted.session.directory,
                ),
                fence: admitted.fence,
            }))
        } else {
            Self::Successor(Box::new(
                crate::successor_driver_contract::fixture::request(store, clock, family == 1).await,
            ))
        }
    }
    fn scope(&self) -> (&SessionId, &RunId) {
        match self {
            Self::Initial(r) => (r.context.session_id(), &r.fence.run_id),
            Self::Successor(r) => (r.context.session_id(), &r.fence.run_id),
        }
    }
    async fn register(&self, store: &impl Store) -> Result<OwnedScopeParentAuthority, Error> {
        match self {
            Self::Initial(r) => match store.register_execution_driver(*r.clone()).await? {
                ExecutionDriverOutcome::Applied { grant, .. } => {
                    Ok(OwnedScopeParentAuthority::ExecutionDriver(grant))
                }
                ExecutionDriverOutcome::Replayed(_) => Err(Error::InvalidTransition),
            },
            Self::Successor(r) => match store
                .register_successor_execution_driver(*r.clone())
                .await?
            {
                SuccessorExecutionDriverOutcome::Applied { grant, .. } => {
                    Ok(OwnedScopeParentAuthority::SuccessorExecutionDriver(grant))
                }
                SuccessorExecutionDriverOutcome::Replayed(_) => Err(Error::InvalidTransition),
            },
        }
    }
}
fn grant_parts(parent: &OwnedScopeParentAuthority) -> (SessionOwnerAuthority, OwnedScopeSource) {
    match parent {
        OwnedScopeParentAuthority::ExecutionDriver(g) => {
            (g.authority().clone(), g.scope_source().unwrap())
        }
        OwnedScopeParentAuthority::SuccessorExecutionDriver(g) => {
            (g.authority().clone(), g.scope_source().unwrap())
        }
        _ => panic!("actual driver only"),
    }
}
async fn activation(store: &impl Damage, request: &Request) -> bool {
    let parent = request.register(store).await.unwrap();
    let (authority, source) = grant_parts(&parent);
    let effects = Arc::new(AtomicUsize::new(0));
    let count = effects.clone();
    let mut task = OwnedTaskRegistration::register(source, move || async move {
        count.fetch_add(1, Ordering::SeqCst);
    })
    .unwrap();
    let (session, run) = request.scope();
    let saved = store.damage(session, run).await;
    let result = store
        .activate_owned_scope(
            task.activation().clone(),
            authority.clone(),
            Some(parent.clone()),
        )
        .await;
    store.restore(session, run, saved).await;
    let refused = result.is_err();
    let untouched = effects.load(Ordering::SeqCst) == 0;
    if refused {
        let OwnedScopeActivationOutcome::Applied { permit, .. } = store
            .activate_owned_scope(task.activation().clone(), authority.clone(), Some(parent))
            .await
            .unwrap()
        else {
            panic!("restored original fresh activation")
        };
        task.dispatch(*permit).unwrap();
        let joined = task.join().await.unwrap();
        let mut cleanup = OwnedCleanupTask::from_host_task(
            joined.activation().clone(),
            tokio::spawn(async { Ok(()) }),
        );
        store
            .join_owned_scope(
                joined.observe_resources(&mut cleanup).await.unwrap(),
                authority,
            )
            .await
            .unwrap();
        assert_eq!(effects.load(Ordering::SeqCst), 1);
    } else {
        drop(result);
        assert_eq!(
            task.join().await.unwrap().disposition(),
            crate::session_owner::owned_scope::OwnedMainDisposition::NoDispatch
        );
    }
    assert!(untouched, "no factory before actual dispatch");
    refused
}
pub(crate) async fn run(store: &impl Damage, clock: &ManualClock) {
    let mut observed = Vec::new();
    for family in 0..3 {
        let request = Request::new(store, clock, family).await;
        let (session, run) = request.scope();
        let saved = store.damage(session, run).await;
        let result = request.register(store).await;
        store.restore(session, run, saved).await;
        let refused = result.is_err();
        if refused {
            request.register(store).await.unwrap();
        }
        observed.push((family, "registration", refused));
        let request = Request::new(store, clock, family).await;
        observed.push((family, "activation", activation(store, &request).await));
    }
    assert!(
        observed.iter().all(|(_, _, refused)| *refused),
        "actual Raw Driver origin gates: {observed:?}"
    );
}
