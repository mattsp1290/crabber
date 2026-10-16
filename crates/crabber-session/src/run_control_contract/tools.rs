use super::{RunControlStatus, RunControlTarget, Store, accept, cancel_action, request};
use crate::tool_continuation_contract::fixture;
use crabber_core::{InteractionState, RunStatus, ToolCallStatus};
pub(super) async fn run(store: &impl Store) {
    for answered in 0..=2 {
        let f = fixture::fixture(store).await;
        f.execution
            .suspend_tool_continuation(f.suspension.clone(), f.authority.clone())
            .await
            .unwrap();
        for index in 0..answered {
            store
                .answer_tool_interaction(fixture::answer(store, &f, index).await)
                .await
                .unwrap();
        }
        let target = RunControlTarget::of(&f.admitted.run);
        let cancel = request(store, &target, cancel_action()).await;
        let saved = accept(store, cancel.clone()).await;
        let snapshot = store
            .tool_continuation_snapshot(&target.session_id, &target.run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.state, crate::ToolContinuationState::Released);
        for (index, question) in snapshot.interactions.iter().enumerate() {
            assert_eq!(
                question.state,
                if index < answered {
                    InteractionState::Answered
                } else {
                    InteractionState::Cancelled
                }
            );
            assert!(question.consumed);
        }
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
            store.control_run(cancel).await.unwrap(),
            crate::RunControlOutcome::Replayed(saved)
        );
        assert_eq!(
            store
                .list_unfinished_tool_calls(&target.run_id)
                .await
                .unwrap(),
            []
        );
        let calls = crate::approval_contract::termination::calls(store, &target.session_id).await;
        assert_eq!(calls.len(), f.suspension.calls.len());
        assert!(
            calls
                .iter()
                .all(|call| call.status == ToolCallStatus::Interrupted)
        );
        let phase = store
            .tool_phase(&target.run_id, &f.suspension.phases[0].previous.id)
            .await
            .unwrap()
            .phase;
        assert_eq!(phase.state, crate::PhaseState::Suspended);
        assert_eq!(phase.quiescence, crate::PhaseQuiescence::Complete);
        assert_eq!(
            store
                .latest_execution_attempt(&target.run_id)
                .await
                .unwrap()
                .unwrap()
                .attempt
                .state,
            crate::AttemptState::Suspended
        );
    }
    claim_race(store).await;
}

async fn claim_race(store: &impl Store) {
    let f = fixture::fixture(store).await;
    f.execution
        .suspend_tool_continuation(f.suspension.clone(), f.authority.clone())
        .await
        .unwrap();
    for index in 0..2 {
        store
            .answer_tool_interaction(fixture::answer(store, &f, index).await)
            .await
            .unwrap();
    }
    let target = RunControlTarget::of(&f.admitted.run);
    let claim = fixture::claim(store, &f).await;
    let cancel = request(store, &target, cancel_action()).await;
    let (claimed, cancelled) = futures::join!(
        store.claim_tool_continuation(claim),
        store.control_run(cancel.clone())
    );
    assert_eq!(
        usize::from(claimed.is_ok()) + usize::from(cancelled.is_ok()),
        1
    );
    let snapshot = store
        .tool_continuation_snapshot(&target.session_id, &target.run_id)
        .await
        .unwrap()
        .unwrap();
    match cancelled {
        Ok(_) => {
            assert!(matches!(
                claimed,
                Err(crate::ToolContinuationError::RevisionConflict)
            ));
            assert_eq!(snapshot.state, crate::ToolContinuationState::Released);
            assert_eq!(
                store.get_run(&target.run_id).await.unwrap().unwrap().status,
                RunStatus::Interrupted
            );
        }
        Err(error) => {
            assert_eq!(error, crate::RunControlError::RevisionConflict);
            assert_eq!(snapshot.state, crate::ToolContinuationState::Claimed);
            assert!(
                store
                    .lookup_run_control(&target.session_id, &cancel.id)
                    .await
                    .unwrap()
                    .is_none()
            );
            assert_eq!(
                store.get_run(&target.run_id).await.unwrap().unwrap().status,
                RunStatus::Running
            );
        }
    }
}
