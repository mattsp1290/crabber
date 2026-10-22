use super::{applied, reserve};
use crate::{
    AttemptCommand, AttemptError as Error, AttemptMutation, AttemptMutationOutcome as Outcome,
    Store,
};
use crabber_core::{Clock, ControlId, ManualClock};

pub(super) async fn run(store: &dyn Store, clock: &ManualClock) {
    let admitted = crate::public_state_contract::admit(store, clock.now()).await;
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    let reserve = reserve(store.session_revision(&admitted.session.id).await.unwrap());
    let (reserved, authority) = applied(
        execution
            .mutate_attempt(reserve.clone(), None)
            .await
            .unwrap(),
    );
    let renewed_until = admitted.run.lease_until + time::Duration::seconds(10);
    execution.renew_lease(renewed_until).await.unwrap();
    assert_eq!(
        store.session_revision(&admitted.session.id).await.unwrap(),
        reserved.revision
    );
    clock.set(admitted.run.lease_until);
    let reclaim = AttemptMutation {
        id: ControlId::new(),
        expected_revision: reserved.revision,
        command: AttemptCommand::ReclaimUnstarted {
            target: reserved.attempt.target.clone(),
        },
    };
    assert_eq!(
        execution
            .mutate_attempt(reclaim.clone(), None)
            .await
            .unwrap_err(),
        Error::LiveOwner
    );
    clock.set(renewed_until);
    let replacement = store
        .claim_expired_run(&admitted.run.id, "replacement")
        .await
        .unwrap();
    let new_execution = store.execution(replacement).await.unwrap();
    assert!(
        matches!(execution.mutate_attempt(reserve, None).await.unwrap(), Outcome::Replayed(r) if r == reserved)
    );
    let mut begin = AttemptMutation {
        id: ControlId::new(),
        expected_revision: reserved.revision,
        command: AttemptCommand::Begin {
            target: reserved.attempt.target.clone(),
        },
    };
    assert_eq!(
        execution
            .mutate_attempt(begin.clone(), authority.clone())
            .await
            .unwrap_err(),
        Error::StaleOwner
    );
    let (reclaimed, current) = applied(
        new_execution
            .mutate_attempt(reclaim.clone(), None)
            .await
            .unwrap(),
    );
    assert!(
        matches!(new_execution.mutate_attempt(reclaim.clone(), None).await.unwrap(), Outcome::Replayed(r) if r == reclaimed)
    );
    assert_eq!(reclaimed.attempt.target, reserved.attempt.target);
    begin.expected_revision = reclaimed.revision;
    assert_eq!(
        new_execution
            .mutate_attempt(begin.clone(), authority)
            .await
            .unwrap_err(),
        Error::StaleOwner
    );
    let (started, _) = applied(
        new_execution
            .mutate_attempt(begin.clone(), current)
            .await
            .unwrap(),
    );
    deny_started_reclaim(
        store,
        clock,
        &admitted.run.id,
        &admitted.session.id,
        begin,
        &started,
    )
    .await;
}
async fn deny_started_reclaim(
    store: &dyn Store,
    clock: &ManualClock,
    run: &crabber_core::RunId,
    session: &crabber_core::SessionId,
    begin: AttemptMutation,
    started: &crate::AttemptReceipt,
) {
    let retained = store.get_run(run).await.unwrap().unwrap();
    clock.set(retained.lease_until);
    let newest = store.claim_expired_run(run, "newest").await.unwrap();
    let newest = store.execution(newest).await.unwrap();
    let reclaim = AttemptMutation {
        id: ControlId::new(),
        expected_revision: started.revision,
        command: AttemptCommand::ReclaimUnstarted {
            target: started.attempt.target.clone(),
        },
    };
    assert_eq!(
        newest.mutate_attempt(reclaim, None).await.unwrap_err(),
        Error::InvalidTransition
    );
    assert!(
        matches!(newest.mutate_attempt(begin, None).await.unwrap(), Outcome::Replayed(r) if r == *started)
    );
    assert_eq!(
        store.list_events(session, None, 100).await.unwrap().len(),
        3
    );
}
