use super::{
    Clock, ControlId, ManualClock, RunControlStatus, RunControlTarget, RunStatus, Store, accept,
    cancel_action, request, terminal,
};
use crate::{AttemptCommand, AttemptConclusion, AttemptFinish, AttemptMutation, RunBoundary};
use crabber_core::{
    EventKind, EventRecord, ToolCallId, ToolCallRecord, ToolCallStatus, ToolResultStatus, TurnId,
};
pub(super) async fn paused(
    store: &impl Store,
    clock: &ManualClock,
    before_pause: bool,
) -> (RunControlTarget, Option<crate::RunControlRequest>) {
    let admitted = crate::public_state_contract::admit(store, clock.now()).await;
    let (execution, owner, target) = terminal::start_attempt(store, &admitted).await;
    let turn = TurnId::new();
    let call = ToolCallRecord {
        id: ToolCallId::new(),
        run_id: admitted.run.id.clone(),
        name: "never-started".into(),
        arguments: serde_json::json!({"private":"original"}),
        status: ToolCallStatus::Pending,
        retry_safe: false,
        result: None,
    };
    execution
        .create_tool_call(
            call.clone(),
            EventRecord {
                cursor: None,
                session_id: admitted.session.id.clone(),
                run_id: admitted.run.id.clone(),
                turn_id: Some(turn.clone()),
                kind: EventKind::ToolCallPending,
                payload: serde_json::json!({"call_id":call.id}),
                correlation: None,
                live_only: false,
                created_at: clock.now(),
            },
        )
        .await
        .unwrap();
    let checkpoint =
        serde_json::json!({"version":4,"pending_calls":[call.id],"source_turn_id":turn});
    let accepted = if before_pause {
        let target = RunControlTarget::of(&admitted.run);
        let cancel = request(store, &target, cancel_action()).await;
        accept(store, cancel.clone()).await;
        Some(cancel)
    } else {
        None
    };
    let conclusion = AttemptConclusion {
        finish: AttemptMutation {
            id: ControlId::new(),
            expected_revision: store.session_revision(&admitted.session.id).await.unwrap(),
            command: AttemptCommand::Finish {
                target,
                outcome: AttemptFinish::Suspended,
            },
        },
        boundary: RunBoundary::Suspended {
            checkpoint,
            usage: admitted.run.usage.clone(),
        },
        created_at: clock.now(),
        turn_id: Some(turn),
        elapsed: std::time::Duration::ZERO,
    };
    execution
        .conclude_attempt(conclusion, Some(owner))
        .await
        .unwrap();
    let run = store.get_run(&admitted.run.id).await.unwrap().unwrap();
    assert_eq!(run.status, RunStatus::Paused);
    let target = RunControlTarget::of(&run);
    (target, accepted)
}
pub(super) async fn run(store: &impl Store, clock: &ManualClock) {
    let (target, _) = paused(store, clock, false).await;
    let cancel = request(store, &target, cancel_action()).await;
    accept(store, cancel).await;
    assert_eq!(
        store.get_run(&target.run_id).await.unwrap().unwrap().status,
        RunStatus::Interrupted
    );
    assert_eq!(
        store.run_controls(&target).await.unwrap().controls[0].status,
        RunControlStatus::TerminalCommitted {
            status: RunStatus::Interrupted
        }
    );
    assert_eq!(
        store
            .list_unfinished_tool_calls(&target.run_id)
            .await
            .unwrap(),
        []
    );
    let calls = crate::approval_contract::termination::calls(store, &target.session_id).await;
    assert_eq!(calls.len(), 1);
    assert_eq!(
        calls[0].result.as_ref().unwrap().status,
        ToolResultStatus::Interrupted
    );
    let native = store
        .latest_execution_attempt(&target.run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(native.attempt.state, crate::AttemptState::Suspended);
    assert_eq!(native.attempt.cleanup, crate::AttemptCleanup::Complete);
}
