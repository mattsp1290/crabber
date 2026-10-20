use super::{
    ApprovalClaimOutcome, ApprovalContinuationClaim, ApprovalContinuationState, ApprovalReply,
    AttemptCommand, AttemptError, AttemptMutation, AttemptUncertainty, AttemptUncertaintyCause,
    CallApproval, ControlId, EventKind, ManualClock, Store, Usage, claim, fresh, historical_turn,
    result_order, retirement, source_event,
};
#[allow(clippy::too_many_lines)] // One real approval claim and ordered parent retirement journey.
pub(super) async fn run(store: &impl Store, clock: &ManualClock, later_turn: bool) {
    let mut f = crate::approval_contract::fixture::staged(store, clock).await;
    f.batch.calls.reverse();
    let source = crabber_core::TurnId::new();
    f.batch.source_turn_id = Some(source.clone());
    historical_turn(&*f.execution, &f.run).await;
    f.batch.expected_revision = store.session_revision(&f.run.session_id).await.unwrap();
    f.batch.usage = Usage {
        input_tokens: 37,
        output_tokens: 11,
    };
    f.execution
        .suspend_for_approval(f.batch.clone(), f.owner.clone())
        .await
        .unwrap();
    let waiting = AttemptUncertainty {
        session_id: f.run.session_id.clone(),
        run_id: f.run.id.clone(),
        id: ControlId::new(),
        expected_revision: store.session_revision(&f.run.session_id).await.unwrap(),
        target: f.batch.attempt.clone(),
        cause: AttemptUncertaintyCause::HostStopped { claim: claim() },
    };
    assert_eq!(
        store.mark_attempt_uncertain(waiting).await.unwrap_err(),
        AttemptError::Busy
    );
    for index in 0..f.batch.calls.len() {
        if matches!(f.batch.calls[index].decision, CallApproval::Ask { .. }) {
            store
                .answer_approval(
                    crate::approval_contract::fixture::answer(
                        store,
                        &f,
                        index,
                        ApprovalReply::Grant,
                    )
                    .await,
                )
                .await
                .unwrap();
        }
    }
    let ApprovalClaimOutcome::Applied { receipt, grant } = store
        .claim_approval_continuation(ApprovalContinuationClaim {
            id: ControlId::new(),
            expected_revision: store.session_revision(&f.run.session_id).await.unwrap(),
            session_id: f.run.session_id.clone(),
            run_id: f.run.id.clone(),
            continuation: f.batch.continuation.clone(),
            expected_plan: f.batch.expected_plan.clone(),
            owner: "joined-parent".into(),
            lease_ms: 30_000,
        })
        .await
        .unwrap()
    else {
        panic!("continuation");
    };
    let execution = store.execution(grant.fence).await.unwrap();
    execution
        .mutate_attempt(
            AttemptMutation {
                id: ControlId::new(),
                expected_revision: receipt.revision,
                command: AttemptCommand::Begin {
                    target: f.batch.next_attempt.clone(),
                },
            },
            Some(grant.authority),
        )
        .await
        .unwrap();
    let later_source = crabber_core::TurnId::new();
    if later_turn {
        source_event(
            &*execution,
            &f.run,
            source.clone(),
            EventKind::TurnCompleted,
        )
        .await;
        source_event(
            &*execution,
            &f.run,
            later_source.clone(),
            EventKind::TurnStarted,
        )
        .await;
    }
    let mark = AttemptUncertainty {
        session_id: f.run.session_id.clone(),
        run_id: f.run.id.clone(),
        id: ControlId::new(),
        expected_revision: store.session_revision(&f.run.session_id).await.unwrap(),
        target: f.batch.next_attempt.clone(),
        cause: AttemptUncertaintyCause::HostStopped { claim: claim() },
    };
    let marked = fresh(store.mark_attempt_uncertain(mark.clone()).await.unwrap());
    assert_eq!(
        store
            .approval_snapshot(&f.run.session_id, &f.run.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        ApprovalContinuationState::Claimed
    );
    store
        .retire_attempt(retirement(&mark, marked.revision))
        .await
        .unwrap();
    assert_eq!(
        store
            .approval_snapshot(&f.run.session_id, &f.run.id)
            .await
            .unwrap()
            .unwrap()
            .state,
        ApprovalContinuationState::Released
    );
    assert_eq!(
        store.get_run(&f.run.id).await.unwrap().unwrap().usage,
        f.batch.usage
    );
    let expected = f
        .batch
        .calls
        .iter()
        .map(|call| call.target.call_id.clone())
        .collect::<Vec<_>>();
    assert_eq!(result_order(store, &f.run.session_id).await, expected);
    let events = store
        .list_events(&f.run.session_id, None, 100)
        .await
        .unwrap();
    let current = if later_turn {
        later_source
    } else {
        source.clone()
    };
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == EventKind::TurnCompleted && e.turn_id == Some(current.clone()))
            .count(),
        1
    );
    assert!(
        events
            .iter()
            .filter(|e| e.kind == EventKind::ToolCallSettled)
            .all(|e| e.turn_id == Some(source.clone()))
    );
    assert_eq!(
        events
            .iter()
            .find(|e| e.kind == EventKind::RunSettled)
            .unwrap()
            .turn_id,
        Some(current)
    );
}
