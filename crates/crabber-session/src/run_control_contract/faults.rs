use super::{
    OffsetDateTime, RunControlError, RunControlOutcome, RunControlTarget, Store, TurnId, accept,
    cancel_action, request, steer,
};
/// Fail native acceptance and worker boundary commits before and after commit.
/// # Panics
/// Panics if a failed transaction leaks partial progress/history or retries append.
pub async fn run_fault_contract(store: &impl Store, set: impl Fn(u8)) {
    for point in [1, 2] {
        let admitted = crate::public_state_contract::admit(store, OffsetDateTime::UNIX_EPOCH).await;
        let target = RunControlTarget::of(&admitted.run);
        let request = steer(store, &target, "PRIVATE_ACK_INPUT").await;
        let before = store.session_revision(&target.session_id).await.unwrap();
        set(point);
        assert_eq!(
            store.control_run(request.clone()).await.unwrap_err(),
            RunControlError::UnknownStoreOutcome
        );
        set(0);
        let saved = store
            .lookup_run_control(&target.session_id, &request.id)
            .await
            .unwrap();
        if point == 1 {
            assert!(saved.is_none());
            assert_eq!(
                store.session_revision(&target.session_id).await.unwrap(),
                before
            );
            accept(store, request).await;
        } else {
            assert!(saved.is_some());
            assert_eq!(
                store.control_run(request).await.unwrap(),
                RunControlOutcome::Replayed(saved.unwrap())
            );
        }
    }
    waiting(store, &set).await;
    tools(store, &set).await;
    for cancel in [false, true] {
        for point in [1, 2] {
            let admitted =
                crate::public_state_contract::admit(store, OffsetDateTime::UNIX_EPOCH).await;
            let target = RunControlTarget::of(&admitted.run);
            let execution = store.execution(admitted.fence).await.unwrap();
            let request = if cancel {
                request(store, &target, cancel_action()).await
            } else {
                steer(store, &target, "PRIVATE_BOUNDARY_INPUT").await
            };
            accept(store, request).await;
            let before = store.run_controls(&target).await.unwrap();
            let history = store.list_all_messages(&target.session_id).await.unwrap();
            let turn = TurnId::new();
            set(point);
            assert_eq!(
                execution
                    .observe_run_controls(Some(&turn))
                    .await
                    .unwrap_err(),
                RunControlError::UnknownStoreOutcome
            );
            set(0);
            if point == 1 {
                assert_eq!(store.run_controls(&target).await.unwrap(), before);
                assert_eq!(
                    store.list_all_messages(&target.session_id).await.unwrap(),
                    history
                );
            }
            let repeated = execution.observe_run_controls(Some(&turn)).await.unwrap();
            assert_eq!(repeated.incorporated, usize::from(!cancel && point == 1));
            let settled = store.run_controls(&target).await.unwrap();
            let history = store.list_all_messages(&target.session_id).await.unwrap();
            execution.observe_run_controls(Some(&turn)).await.unwrap();
            assert_eq!(store.run_controls(&target).await.unwrap(), settled);
            assert_eq!(
                store.list_all_messages(&target.session_id).await.unwrap(),
                history
            );
        }
    }
}

async fn tools(store: &impl Store, set: &impl Fn(u8)) {
    for point in [1, 2] {
        let f = crate::tool_continuation_contract::fixture::fixture(store).await;
        f.execution
            .suspend_tool_continuation(f.suspension, f.authority)
            .await
            .unwrap();
        let target = RunControlTarget::of(&f.admitted.run);
        let cancel = request(store, &target, cancel_action()).await;
        let before = store.session_revision(&target.session_id).await.unwrap();
        set(point);
        assert_eq!(
            store.control_run(cancel.clone()).await.unwrap_err(),
            RunControlError::UnknownStoreOutcome
        );
        set(0);
        let snapshot = store
            .tool_continuation_snapshot(&target.session_id, &target.run_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            snapshot.state,
            if point == 1 {
                crate::ToolContinuationState::Waiting
            } else {
                crate::ToolContinuationState::Released
            }
        );
        if point == 1 {
            assert_eq!(
                store.session_revision(&target.session_id).await.unwrap(),
                before
            );
        }
        let saved = store.control_run(cancel.clone()).await.unwrap();
        let after = store.session_revision(&target.session_id).await.unwrap();
        let history = store.list_all_messages(&target.session_id).await.unwrap();
        assert!(matches!(
            store.control_run(cancel).await.unwrap(),
            RunControlOutcome::Replayed(_)
        ));
        assert_eq!(
            store.session_revision(&target.session_id).await.unwrap(),
            after
        );
        assert_eq!(
            store.list_all_messages(&target.session_id).await.unwrap(),
            history
        );
        assert_eq!(matches!(saved, RunControlOutcome::Applied(_)), point == 1);
    }
}

async fn waiting(store: &impl Store, set: &impl Fn(u8)) {
    for point in [1, 2] {
        let clock = crabber_core::ManualClock::new(time::OffsetDateTime::UNIX_EPOCH);
        let (target, original) = super::checkpoint::paused(store, &clock, true).await;
        let original = original.unwrap();
        let request = crate::WaitingCancellation {
            target: target.clone(),
            original_cancel_id: original.id.clone(),
            expected_revision: store.session_revision(&target.session_id).await.unwrap(),
        };
        assert_eq!(
            store.run_controls(&target).await.unwrap().controls[0].status,
            crate::RunControlStatus::Accepted
        );
        let before = store.run_controls(&target).await.unwrap();
        let history = store.list_all_messages(&target.session_id).await.unwrap();
        set(point);
        assert_eq!(
            store
                .settle_waiting_cancellation(request.clone())
                .await
                .unwrap_err(),
            RunControlError::UnknownStoreOutcome
        );
        set(0);
        if point == 1 {
            assert_eq!(store.run_controls(&target).await.unwrap(), before);
            assert_eq!(
                store.list_all_messages(&target.session_id).await.unwrap(),
                history
            );
        }
        let result = store
            .settle_waiting_cancellation(request.clone())
            .await
            .unwrap();
        assert_eq!(
            result.status,
            crate::RunControlStatus::TerminalCommitted {
                status: crabber_core::RunStatus::Interrupted
            }
        );
        let current = store.session_revision(&target.session_id).await.unwrap();
        assert_eq!(
            store.settle_waiting_cancellation(request).await.unwrap(),
            result
        );
        assert_eq!(
            store.session_revision(&target.session_id).await.unwrap(),
            current
        );
        assert!(matches!(
            store.control_run(original).await.unwrap(),
            RunControlOutcome::Replayed(_)
        ));
    }
}
