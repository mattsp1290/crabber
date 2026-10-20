use super::{
    ApprovalReply, ManualClock, RunControlOutcome, RunControlStatus, RunControlTarget, RunStatus,
    Store, accept, cancel_action, request, steer,
};
use crate::approval_contract::fixture::{answer, claim, staged};
use crabber_core::{InteractionState, ToolCallStatus, ToolResultStatus};
pub(super) async fn run(store: &impl Store, clock: &ManualClock) {
    for answered in 0..=2 {
        let f = staged(store, clock).await;
        f.execution
            .suspend_for_approval(f.batch.clone(), f.owner.clone())
            .await
            .unwrap();
        for index in 0..answered {
            store
                .answer_approval(answer(store, &f, index, ApprovalReply::Grant).await)
                .await
                .unwrap();
        }
        let run = store.get_run(&f.run.id).await.unwrap().unwrap();
        let target = RunControlTarget::of(&run);
        let waiting_steer = steer(store, &target, "UNAPPLIED_WAITING_STEER").await;
        accept(store, waiting_steer).await;
        let cancel = request(store, &target, cancel_action()).await;
        let saved = accept(store, cancel.clone()).await;
        let run = store.get_run(&target.run_id).await.unwrap().unwrap();
        assert_eq!(run.status, RunStatus::Interrupted);
        assert_ne!(run.claim_token, f.run.claim_token);
        let page = store.run_controls(&target).await.unwrap();
        assert_eq!(
            page.controls[0].status,
            RunControlStatus::UnappliedTerminal {
                status: RunStatus::Interrupted
            }
        );
        assert_eq!(
            page.controls[1].status,
            RunControlStatus::TerminalCommitted {
                status: RunStatus::Interrupted
            }
        );
        let snapshot = store
            .approval_snapshot(&target.session_id, &target.run_id)
            .await
            .unwrap()
            .unwrap();
        assert!(snapshot.interactions.iter().all(|i| matches!(
            i.state,
            InteractionState::Cancelled | InteractionState::Expired | InteractionState::Answered
        )));
        let calls = crate::approval_contract::termination::calls(store, &target.session_id).await;
        assert_eq!(calls.len(), f.batch.calls.len());
        for call in calls {
            assert_eq!(call.status, ToolCallStatus::Interrupted);
            assert_eq!(call.result.unwrap().status, ToolResultStatus::Interrupted);
        }
        assert!(
            store
                .claim_approval_continuation(claim(store, &f).await)
                .await
                .is_err()
        );
        assert_eq!(
            store.control_run(cancel).await.unwrap(),
            RunControlOutcome::Replayed(saved)
        );
        assert!(f.execution.observe_run_controls(None).await.is_err());
        assert_eq!(
            store
                .list_unfinished_tool_calls(&target.run_id)
                .await
                .unwrap(),
            [] as [crabber_core::ToolCallRecord; 0]
        );
    }
}
