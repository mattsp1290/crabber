use super::{applied, reserve};
use crate::{
    AttemptCleanup, AttemptCleanupResult, AttemptCommand, AttemptError as Error, AttemptFinish,
    AttemptMutation, AttemptState, PublicStateMutation, Store,
};
use crabber_core::{Clock, ControlId, ManualClock, PublicStateContents};

pub(super) async fn run(store: &dyn Store, clock: &ManualClock) {
    let admitted = crate::public_state_contract::admit(store, clock.now()).await;
    let execution = store.execution(admitted.fence).await.unwrap();
    let (reserved, authority) = applied(
        execution
            .mutate_attempt(
                reserve(store.session_revision(&admitted.session.id).await.unwrap()),
                None,
            )
            .await
            .unwrap(),
    );
    let target = reserved.attempt.target;
    let begin = AttemptMutation {
        id: ControlId::new(),
        expected_revision: reserved.revision,
        command: AttemptCommand::Begin {
            target: target.clone(),
        },
    };
    let (started, _) = applied(
        execution
            .mutate_attempt(begin, authority.clone())
            .await
            .unwrap(),
    );
    let finish = AttemptMutation {
        id: ControlId::new(),
        expected_revision: started.revision,
        command: AttemptCommand::Finish {
            target: target.clone(),
            outcome: AttemptFinish::Suspended,
        },
    };
    let (finished, _) = applied(
        execution
            .mutate_attempt(finish, authority.clone())
            .await
            .unwrap(),
    );
    let mut cleanup = AttemptMutation {
        id: ControlId::new(),
        expected_revision: finished.revision,
        command: AttemptCommand::RecordCleanup {
            target: target.clone(),
            result: AttemptCleanupResult::Incomplete,
        },
    };
    let (incomplete, _) = applied(
        execution
            .mutate_attempt(cleanup.clone(), authority.clone())
            .await
            .unwrap(),
    );
    assert_eq!(incomplete.attempt.cleanup, AttemptCleanup::Incomplete);
    cleanup.id = ControlId::new();
    cleanup.expected_revision = incomplete.revision;
    cleanup.command = AttemptCommand::RecordCleanup {
        target,
        result: AttemptCleanupResult::Complete,
    };
    let (complete, _) = applied(
        execution
            .mutate_attempt(cleanup, authority.clone())
            .await
            .unwrap(),
    );
    let mut next = reserve(complete.revision);
    if let AttemptCommand::Reserve { target } = &mut next.command {
        target.generation = target.generation.next().unwrap();
    }
    let (next_receipt, next_authority) =
        applied(execution.mutate_attempt(next, None).await.unwrap());
    assert!(next_authority.is_some());
    let begin = AttemptMutation {
        id: ControlId::new(),
        expected_revision: next_receipt.revision,
        command: AttemptCommand::Begin {
            target: next_receipt.attempt.target.clone(),
        },
    };
    assert_eq!(
        execution
            .mutate_attempt(begin, authority)
            .await
            .unwrap_err(),
        Error::StaleOwner
    );
    assert_eq!(next_receipt.attempt.state, AttemptState::Unstarted);
    namespace(
        store,
        execution.as_ref(),
        &admitted.session.id,
        next_receipt.revision,
    )
    .await;
}
async fn namespace(
    store: &dyn Store,
    execution: &dyn crate::ExecutionStore,
    session: &crabber_core::SessionId,
    revision: crabber_core::SessionRevision,
) {
    let state = PublicStateMutation {
        id: ControlId::new(),
        expected_revision: revision,
        contents: PublicStateContents::default(),
    };
    let state_id = state.id.clone();
    let receipt = execution.mutate_public_state(state).await.unwrap();
    let mut conflict = reserve(receipt.revision);
    conflict.id = state_id.clone();
    assert_eq!(
        execution.mutate_attempt(conflict, None).await.unwrap_err(),
        Error::SemanticConflict
    );
    assert_eq!(
        store
            .lookup_attempt_mutation(session, &state_id)
            .await
            .unwrap_err(),
        Error::SemanticConflict
    );
    assert_eq!(
        store.session_revision(session).await.unwrap(),
        receipt.revision
    );
}
