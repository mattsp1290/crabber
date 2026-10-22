use super::{
    AttemptCommand, AttemptConclusion, AttemptFinish, AttemptId, AttemptMutation,
    AttemptMutationOutcome, AttemptTarget, AttemptUncertainty, AttemptUncertaintyCause, ControlId,
    EventKind, EventRecord, RunBoundary, Store, ToolCallId, ToolCallRecord, ToolCallStatus, Usage,
    claim, fresh, historical_turn, result_order, retirement,
};
#[allow(clippy::too_many_lines)] // One real suspended/resumed checkpoint retirement journey.
pub(super) async fn run(store: &dyn Store, already_closed: bool) {
    let (admitted, execution, owner, planned) = crate::phase_contract::fixture(store).await;
    let mut order = vec![
        ToolCallId::from(format!("z-checkpoint/{}", admitted.run.id)),
        planned.command.target().call_id.clone(),
        ToolCallId::from(format!("a-checkpoint/{}", admitted.run.id)),
    ];
    for id in [&order[0], &order[2]] {
        execution
            .create_tool_call(
                ToolCallRecord {
                    id: id.clone(),
                    run_id: admitted.run.id.clone(),
                    name: "pending".into(),
                    arguments: serde_json::json!({}),
                    status: ToolCallStatus::Pending,
                    retry_safe: false,
                    result: None,
                },
                EventRecord {
                    cursor: None,
                    session_id: admitted.session.id.clone(),
                    run_id: admitted.run.id.clone(),
                    turn_id: None,
                    kind: EventKind::ToolCallPending,
                    payload: serde_json::json!({}),
                    correlation: None,
                    live_only: false,
                    created_at: admitted.run.created_at,
                },
            )
            .await
            .unwrap();
    }
    historical_turn(&*execution, &admitted.run).await;
    let original_source = crabber_core::TurnId::new();
    if already_closed {
        execution
            .append_event(EventRecord {
                cursor: None,
                session_id: admitted.session.id.clone(),
                run_id: admitted.run.id.clone(),
                turn_id: Some(original_source.clone()),
                kind: EventKind::TurnCompleted,
                payload: serde_json::Value::Null,
                correlation: None,
                live_only: false,
                created_at: admitted.run.created_at,
            })
            .await
            .unwrap();
    }
    let target = planned.command.target().attempt.clone();
    execution
        .conclude_attempt(
            AttemptConclusion {
                finish: AttemptMutation {
                    id: ControlId::new(),
                    expected_revision: store.session_revision(&admitted.session.id).await.unwrap(),
                    command: AttemptCommand::Finish {
                        target: target.clone(),
                        outcome: AttemptFinish::Suspended,
                    },
                },
                boundary: RunBoundary::Suspended {
                    checkpoint: serde_json::json!({"version":4,"pending_calls":order,"source_turn_id":original_source}),
                    usage: Usage::default(),
                },
                created_at: admitted.run.created_at,
                turn_id: None,
                elapsed: std::time::Duration::ZERO,
            },
            Some(owner),
        )
        .await
        .unwrap();
    let fence = store
        .claim_expired_run(&admitted.run.id, "resumed-fixture")
        .await
        .unwrap();
    let execution = store.execution(fence).await.unwrap();
    let next = AttemptTarget {
        id: AttemptId::new(),
        generation: target.generation.next().unwrap(),
    };
    let AttemptMutationOutcome::Applied { receipt, authority } = execution
        .mutate_attempt(
            AttemptMutation {
                id: ControlId::new(),
                expected_revision: store.session_revision(&admitted.session.id).await.unwrap(),
                command: AttemptCommand::Reserve {
                    target: next.clone(),
                },
            },
            None,
        )
        .await
        .unwrap()
    else {
        panic!("resume reserve");
    };
    execution
        .mutate_attempt(
            AttemptMutation {
                id: ControlId::new(),
                expected_revision: receipt.revision,
                command: AttemptCommand::Begin {
                    target: next.clone(),
                },
            },
            authority,
        )
        .await
        .unwrap();
    let mark = AttemptUncertainty {
        session_id: admitted.session.id.clone(),
        run_id: admitted.run.id.clone(),
        id: ControlId::new(),
        expected_revision: store.session_revision(&admitted.session.id).await.unwrap(),
        target: next,
        cause: AttemptUncertaintyCause::HostStopped { claim: claim() },
    };
    let marked = fresh(store.mark_attempt_uncertain(mark.clone()).await.unwrap());
    store
        .retire_attempt(retirement(&mark, marked.revision))
        .await
        .unwrap();
    let events = store
        .list_events(&admitted.session.id, None, 100)
        .await
        .unwrap();
    let closed = events
        .iter()
        .filter(|event| {
            event.kind == EventKind::TurnCompleted && event.turn_id == Some(original_source.clone())
        })
        .collect::<Vec<_>>();
    assert_eq!(closed.len(), 1);
    assert_eq!(closed[0].turn_id, Some(original_source.clone()));
    assert!(
        events
            .iter()
            .filter(|event| event.kind == EventKind::ToolCallSettled)
            .all(|event| event.turn_id == Some(original_source.clone()))
    );
    assert_eq!(
        result_order(store, &admitted.session.id).await,
        std::mem::take(&mut order)
    );
}
