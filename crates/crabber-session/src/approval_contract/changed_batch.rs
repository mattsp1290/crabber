//! A new revision cannot silently expand a previously approved effect set.
use super::{Error, Store, fixture};
use crabber_core::{Clock, EventKind, EventRecord, ManualClock, ToolCallRecord, ToolCallStatus};

pub(super) async fn run(store: &impl Store, clock: &ManualClock) {
    let f = fixture::staged(store, clock).await;
    f.execution
        .suspend_for_approval(f.batch.clone(), f.owner.clone())
        .await
        .unwrap();
    for index in 0..2 {
        let answer = fixture::answer(store, &f, index, crate::ApprovalReply::Grant).await;
        store.answer_approval(answer).await.unwrap();
    }
    let fence = store
        .claim_expired_run(&f.run.id, "changed-batch")
        .await
        .unwrap();
    let execution = store.execution(fence.clone()).await.unwrap();
    execution
        .create_tool_call(
            ToolCallRecord {
                id: crabber_core::ToolCallId::new(),
                run_id: f.run.id.clone(),
                name: "unapproved-extra".into(),
                arguments: serde_json::json!({}),
                status: ToolCallStatus::Pending,
                retry_safe: true,
                result: None,
            },
            EventRecord {
                cursor: None,
                session_id: f.run.session_id.clone(),
                run_id: f.run.id.clone(),
                turn_id: None,
                kind: EventKind::ToolCallPending,
                payload: serde_json::json!({}),
                correlation: None,
                live_only: false,
                created_at: clock.now(),
            },
        )
        .await
        .unwrap();
    let request = fixture::claim(store, &f).await;
    let before = store
        .approval_snapshot(&f.run.session_id, &f.run.id)
        .await
        .unwrap();
    assert_eq!(
        store
            .claim_approval_continuation(request.clone())
            .await
            .unwrap_err(),
        Error::InvalidTransition
    );
    assert_eq!(
        store
            .lookup_approval(&f.run.session_id, &request.id)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .approval_snapshot(&f.run.session_id, &f.run.id)
            .await
            .unwrap(),
        before
    );
    let run = store.get_run(&f.run.id).await.unwrap().unwrap();
    assert_eq!(run.claim_token, fence.claim_token);
    assert_eq!(run.status, crabber_core::RunStatus::Paused);
}
