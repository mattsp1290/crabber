//! Experimental adapter conformance for attempt transitions and dispatch authority.
#[path = "attempt_contract/bound.rs"]
mod bound;
#[path = "attempt_contract/cleanup.rs"]
mod cleanup;
#[path = "attempt_contract/conclusion.rs"]
pub(crate) mod conclusion;
#[path = "attempt_contract/faults.rs"]
mod faults;
#[cfg(test)]
pub(crate) use faults::{Case as RawAttemptCase, fixture as raw_attempt_fixture};
pub use faults::{FaultPoint, run_fault_contract};
#[path = "attempt_contract/owned_call.rs"]
mod owned_call;
#[path = "attempt_contract/recovery.rs"]
mod recovery;
#[path = "attempt_contract/resume.rs"]
mod resume;
use crate::{
    AttemptAuthority, AttemptCommand, AttemptError as Error, AttemptFinish, AttemptMutation,
    AttemptMutationOutcome as Outcome, AttemptReceipt, AttemptState, AttemptTarget, Store,
};
use crabber_core::{AttemptGeneration, AttemptId, ControlId, ManualClock, SessionRevision};
use std::sync::Arc;
use time::OffsetDateTime;

/// Reserve/begin/finish retain one owner; replay returns metadata only. This
/// proves store transitions, not native resource cleanup or task joining.
/// # Panics
/// Panics if the adapter violates the contract or fixture setup fails.
pub async fn run_contract<S: Store>(factory: impl FnOnce(Arc<ManualClock>) -> S) {
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    let store = factory(clock.clone());
    lifecycle(&store).await;
    bound::run(&store, &clock).await;
    conclusion::run(&store, &clock).await;
    resume::run(&store, &clock).await;
    competing_begin(&store).await;
    cleanup::run(&store, &clock).await;
    recovery::run(&store, &clock).await;
    owned_call::run(&store, &clock).await;
}
pub(crate) fn reserve(revision: SessionRevision) -> AttemptMutation {
    AttemptMutation {
        id: ControlId::new(),
        expected_revision: revision,
        command: AttemptCommand::Reserve {
            target: AttemptTarget {
                id: AttemptId::new(),
                generation: AttemptGeneration::INITIAL,
            },
        },
    }
}
fn applied(result: Outcome) -> (AttemptReceipt, Option<AttemptAuthority>) {
    let Outcome::Applied { receipt, authority } = result else {
        panic!("fresh mutation must apply");
    };
    (receipt, authority)
}
async fn lifecycle(store: &dyn Store) {
    let admitted = crate::public_state_contract::admit(store, OffsetDateTime::UNIX_EPOCH).await;
    let execution = store.execution(admitted.fence).await.unwrap();
    let request = reserve(store.session_revision(&admitted.session.id).await.unwrap());
    let (reserved, authority) = applied(
        execution
            .mutate_attempt(request.clone(), None)
            .await
            .unwrap(),
    );
    let authority = authority.expect("fresh reserve returns ephemeral authority");
    assert_eq!(
        reserved.semantic_digest,
        request
            .fingerprint(&admitted.session.id, &admitted.run.id)
            .unwrap()
    );
    let snapshot = store
        .execution_attempt(&admitted.run.id, &reserved.attempt.target.id)
        .await
        .unwrap();
    assert_eq!(snapshot.revision, reserved.revision);
    assert_eq!(snapshot.attempt, reserved.attempt);
    assert_eq!(reserved.attempt.state, AttemptState::Unstarted);
    assert!(
        matches!(execution.mutate_attempt(request.clone(), None).await.unwrap(), Outcome::Replayed(r) if r == reserved)
    );
    let begin = AttemptMutation {
        id: ControlId::new(),
        expected_revision: reserved.revision,
        command: AttemptCommand::Begin {
            target: reserved.attempt.target.clone(),
        },
    };
    assert_eq!(
        execution
            .mutate_attempt(begin.clone(), None)
            .await
            .unwrap_err(),
        Error::StaleOwner
    );
    let (started, extra) = applied(
        execution
            .mutate_attempt(begin.clone(), Some(authority.clone()))
            .await
            .unwrap(),
    );
    assert!(extra.is_none());
    assert_eq!(started.attempt.state, AttemptState::Started);
    assert!(
        matches!(execution.mutate_attempt(begin, None).await.unwrap(), Outcome::Replayed(r) if r == started)
    );
    let mut conflict = request.clone();
    conflict.command = AttemptCommand::Reserve {
        target: AttemptTarget {
            id: AttemptId::new(),
            generation: AttemptGeneration::INITIAL,
        },
    };
    assert_eq!(
        execution.mutate_attempt(conflict, None).await.unwrap_err(),
        Error::SemanticConflict
    );
    let finish = AttemptMutation {
        id: ControlId::new(),
        expected_revision: started.revision,
        command: AttemptCommand::Finish {
            target: started.attempt.target.clone(),
            outcome: AttemptFinish::Suspended,
        },
    };
    let (finished, _) = applied(
        execution
            .mutate_attempt(finish, Some(authority))
            .await
            .unwrap(),
    );
    let mut blocked = reserve(finished.revision);
    if let AttemptCommand::Reserve { target } = &mut blocked.command {
        target.generation = target.generation.next().unwrap();
    }
    assert_eq!(
        execution.mutate_attempt(blocked, None).await.unwrap_err(),
        Error::Busy
    );
    reconcile(store, execution.as_ref(), &request, &reserved, &finished).await;
}
async fn reconcile(
    store: &dyn Store,
    execution: &dyn crate::ExecutionStore,
    request: &AttemptMutation,
    reserved: &AttemptReceipt,
    finished: &AttemptReceipt,
) {
    assert!(
        matches!(execution.mutate_attempt(request.clone(), None).await.unwrap(), Outcome::Replayed(r) if r == *reserved)
    );
    assert_eq!(
        store
            .session_revision(&finished.attempt.session_id)
            .await
            .unwrap(),
        finished.revision
    );
    assert_eq!(
        store
            .lookup_attempt_mutation(&finished.attempt.session_id, &request.id)
            .await
            .unwrap(),
        Some(reserved.clone())
    );
    let state_request = crate::PublicStateMutation {
        id: request.id.clone(),
        expected_revision: finished.revision,
        contents: crabber_core::PublicStateContents::default(),
    };
    assert_eq!(
        execution
            .mutate_public_state(state_request)
            .await
            .unwrap_err(),
        crate::PublicStateError::SemanticConflict
    );
    assert_eq!(
        store
            .lookup_public_state_mutation(&finished.attempt.session_id, &request.id)
            .await
            .unwrap_err(),
        crate::PublicStateError::SemanticConflict
    );
    assert_eq!(
        store
            .list_events(&finished.attempt.session_id, None, 100)
            .await
            .unwrap()
            .len(),
        3
    );
}
async fn competing_begin(store: &dyn Store) {
    let admitted = crate::public_state_contract::admit(store, OffsetDateTime::UNIX_EPOCH).await;
    let execution = store.execution(admitted.fence).await.unwrap();
    let request = reserve(store.session_revision(&admitted.session.id).await.unwrap());
    let (reserved, authority) = applied(execution.mutate_attempt(request, None).await.unwrap());
    let begin = AttemptMutation {
        id: ControlId::new(),
        expected_revision: reserved.revision,
        command: AttemptCommand::Begin {
            target: reserved.attempt.target,
        },
    };
    let (left, right) = futures::join!(
        execution.mutate_attempt(begin.clone(), authority.clone()),
        execution.mutate_attempt(begin.clone(), authority)
    );
    let results = [left.unwrap(), right.unwrap()];
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Outcome::Applied { .. }))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Outcome::Replayed(_)))
            .count(),
        1
    );
    assert_eq!(
        store
            .list_events(&admitted.session.id, None, 100)
            .await
            .unwrap()
            .len(),
        2
    );
}

#[cfg(all(test, any(feature = "sqlite", feature = "postgres")))]
#[path = "attempt_contract/process.rs"]
pub(crate) mod process;

#[cfg(all(test, any(feature = "sqlite", feature = "postgres")))]
#[path = "attempt_contract/phase_fixture.rs"]
pub(crate) mod phase_fixture;
