//! Historical plan lookup must ignore unrelated current private carriers before allocation.
use super::*;
use crate::Store;
#[tokio::test]
async fn original_plans_ignore_changed_private_runs_and_oversized_current_records() {
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = MemoryStore::with_clock(clock.clone());
    let a = crate::approval_contract::fixture::staged(&store, &clock).await;
    a.execution
        .suspend_for_approval(a.batch.clone(), a.owner.clone())
        .await
        .unwrap();
    let t = crate::tool_continuation_contract::fixture::fixture(&store).await;
    t.execution
        .suspend_tool_continuation(t.suspension.clone(), t.authority.clone())
        .await
        .unwrap();
    {
        let mut state = store.state.lock().unwrap();
        for run in [&a.run.id, &t.admitted.run.id] {
            let saved = state.runs.get_mut(run).unwrap();
            saved.plan_fingerprint = "unrelated mutable replacement".into();
            saved.checkpoint = Some(serde_json::json!({"private": "x".repeat(4*1024*1024)}));
        }
        state
            .approvals
            .get_mut(&a.batch.continuation.id)
            .unwrap()
            .workspace_id = "x".repeat(4 * 1024 * 1024);
        state
            .tool_continuations
            .get_mut(&t.suspension.continuation.id)
            .unwrap()
            .workspace_id = "x".repeat(4 * 1024 * 1024);
    }
    let allocation = allocation_counter::measure(|| {
        assert_eq!(
            store
                .read_original_approval_plan(&a.run.session_id, &a.run.id, &a.batch.continuation)
                .unwrap(),
            a.batch.expected_plan
        );
        assert_eq!(
            store
                .read_original_tool_plan(
                    &t.admitted.session.id,
                    &t.admitted.run.id,
                    &t.suspension.continuation
                )
                .unwrap(),
            t.suspension.expected_plan
        );
    });
    assert!(allocation.bytes_max < 131_072, "{allocation:?}");
    // Changing the selected original batch cannot borrow its old protected audit.
    {
        let mut state = store.state.lock().unwrap();
        state
            .approvals
            .get_mut(&a.batch.continuation.id)
            .unwrap()
            .batch
            .expected_plan = crabber_core::InputFingerprint::new("f".repeat(64)).unwrap();
        state
            .tool_continuations
            .get_mut(&t.suspension.continuation.id)
            .unwrap()
            .batch
            .expected_plan = crabber_core::InputFingerprint::new("f".repeat(64)).unwrap();
    }
    assert_eq!(
        store
            .original_approval_plan(&a.run.session_id, &a.run.id, &a.batch.continuation)
            .await
            .unwrap_err(),
        Error::UnknownStoreOutcome
    );
    assert_eq!(
        store
            .original_tool_continuation_plan(
                &t.admitted.session.id,
                &t.admitted.run.id,
                &t.suspension.continuation
            )
            .await
            .unwrap_err(),
        crate::ToolContinuationError::UnknownStoreOutcome
    );
}
