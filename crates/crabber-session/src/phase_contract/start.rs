//! A real dispatch start and its running source cannot commit independently.
use super::{FaultPoint, applied, fixture, mutation};
use crate::{
    PhaseAuthority, PhaseCommand, PhaseError as Error, PhaseMutationOutcome as Outcome, PhaseStart,
    PhaseState, PhaseWriteAuthority, Store,
};
use crabber_core::{EventKind, ToolCallStatus, TurnId};

async fn prepared(
    store: &impl Store,
) -> (
    crate::AdmitOutcome,
    Box<dyn crate::ExecutionStore>,
    PhaseAuthority,
    PhaseStart,
) {
    let (admitted, execution, owner, claim) = fixture(store).await;
    let (claimed, worker) = applied(
        execution
            .mutate_phase(claim, Some(PhaseWriteAuthority::Attempt(owner)))
            .await
            .unwrap(),
    );
    let start = PhaseStart {
        begin: mutation(
            claimed.revision,
            PhaseCommand::Begin {
                target: claimed.phase.target,
            },
        ),
        turn_id: Some(TurnId::new()),
        created_at: admitted.run.created_at,
    };
    (admitted, execution, worker.unwrap(), start)
}
pub(super) async fn run(store: &impl Store) {
    let (admitted, execution, owner, start) = prepared(store).await;
    let call = store
        .list_unfinished_tool_calls(&admitted.run.id)
        .await
        .unwrap()
        .remove(0);
    let messages = store
        .list_messages(&admitted.session.id, None)
        .await
        .unwrap();
    assert_eq!(
        execution
            .start_tool_phase(start.clone(), None)
            .await
            .unwrap_err(),
        Error::StaleOwner
    );
    let (receipt, grant) = applied(
        execution
            .start_tool_phase(start.clone(), Some(owner.clone()))
            .await
            .unwrap(),
    );
    assert!(grant.is_none());
    assert_eq!(receipt.phase.state, PhaseState::Started);
    assert_eq!(
        receipt.semantic_digest,
        start
            .fingerprint(&admitted.session.id, &admitted.run.id)
            .unwrap()
    );
    assert_ne!(
        receipt.semantic_digest,
        start
            .begin
            .fingerprint(&admitted.session.id, &admitted.run.id)
            .unwrap()
    );
    assert_eq!(
        store
            .list_messages(&admitted.session.id, None)
            .await
            .unwrap(),
        messages
    );
    let calls = store
        .list_unfinished_tool_calls(&admitted.run.id)
        .await
        .unwrap();
    assert_eq!(calls[0].status, ToolCallStatus::Running);
    assert!(calls[0].result.is_none());
    let events = store
        .list_events(&admitted.session.id, None, 1000)
        .await
        .unwrap();
    let running: Vec<_> = events
        .iter()
        .filter(|event| event.kind == EventKind::ToolCallRunning)
        .collect();
    assert_eq!(running.len(), 1);
    let mut expected = start.materialize(&admitted.run, &call).unwrap();
    expected.cursor = running[0].cursor;
    assert_eq!(*running[0], expected);
    assert!(running[0].cursor.unwrap() < receipt.event_cursor);
    let mut retry = start.clone();
    retry.turn_id = Some(TurnId::new());
    retry.created_at += time::Duration::seconds(1);
    assert!(
        matches!(execution.start_tool_phase(retry, None).await.unwrap(), Outcome::Replayed(saved) if saved == receipt)
    );
    assert_eq!(
        store
            .list_events(&admitted.session.id, None, 1000)
            .await
            .unwrap(),
        events
    );
    assert_eq!(
        execution
            .mutate_phase(start.begin, Some(PhaseWriteAuthority::Phase(owner)))
            .await
            .unwrap_err(),
        Error::SemanticConflict
    );
    reverse_domain(store).await;
}
async fn reverse_domain(store: &impl Store) {
    let (admitted, execution, owner, start) = prepared(store).await;
    execution
        .mutate_phase(
            start.begin.clone(),
            Some(PhaseWriteAuthority::Phase(owner.clone())),
        )
        .await
        .unwrap();
    assert_eq!(
        execution
            .start_tool_phase(start, Some(owner))
            .await
            .unwrap_err(),
        Error::SemanticConflict
    );
    assert!(
        !store
            .list_events(&admitted.session.id, None, 1000)
            .await
            .unwrap()
            .iter()
            .any(|event| event.kind == EventKind::ToolCallRunning)
    );
}
pub(super) async fn faults(store: &impl Store, inject: &impl Fn(FaultPoint)) {
    for point in [FaultPoint::BeforeCommit, FaultPoint::AfterCommit] {
        let (admitted, execution, owner, start) = prepared(store).await;
        let before = store
            .tool_phase(&admitted.run.id, &start.begin.command.target().id)
            .await
            .unwrap();
        let events = store
            .list_events(&admitted.session.id, None, 1000)
            .await
            .unwrap();
        inject(point);
        let result = execution
            .start_tool_phase(start.clone(), Some(owner.clone()))
            .await;
        inject(FaultPoint::Disabled);
        assert_eq!(result.unwrap_err(), Error::UnknownStoreOutcome);
        let saved = store
            .lookup_phase_mutation(&admitted.session.id, &start.begin.id)
            .await
            .unwrap();
        match point {
            FaultPoint::BeforeCommit => {
                assert!(saved.is_none());
                assert_eq!(
                    store
                        .tool_phase(&admitted.run.id, &start.begin.command.target().id)
                        .await
                        .unwrap(),
                    before
                );
                assert_eq!(
                    store
                        .list_unfinished_tool_calls(&admitted.run.id)
                        .await
                        .unwrap()[0]
                        .status,
                    ToolCallStatus::Pending
                );
                assert_eq!(
                    store
                        .list_events(&admitted.session.id, None, 1000)
                        .await
                        .unwrap(),
                    events
                );
                assert_eq!(
                    store.session_revision(&admitted.session.id).await.unwrap(),
                    start.begin.expected_revision
                );
                assert!(matches!(
                    execution
                        .start_tool_phase(start, Some(owner))
                        .await
                        .unwrap(),
                    Outcome::Applied { .. }
                ));
            }
            FaultPoint::AfterCommit => {
                let saved = saved.unwrap();
                assert_eq!(saved.phase.state, PhaseState::Started);
                assert_eq!(
                    store
                        .list_unfinished_tool_calls(&admitted.run.id)
                        .await
                        .unwrap()[0]
                        .status,
                    ToolCallStatus::Running
                );
                assert_eq!(
                    store
                        .list_events(&admitted.session.id, None, 1000)
                        .await
                        .unwrap()
                        .len(),
                    events.len() + 2
                );
                assert!(
                    matches!(execution.start_tool_phase(start, None).await.unwrap(), Outcome::Replayed(receipt) if receipt == saved)
                );
                assert_eq!(
                    store.session_revision(&admitted.session.id).await.unwrap(),
                    saved.revision
                );
            }
            FaultPoint::Disabled => unreachable!(),
        }
    }
}
