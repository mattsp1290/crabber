//! Corrupt prepared calls and phase ancestry cannot release a waiting barrier.
use crate::{ApprovalError, PhaseQuiescence, PhaseSnapshot, PhaseState, Store};
use crabber_core::{Clock, ManualClock, ToolCallStatus, ToolOutput, ToolResult, ToolResultStatus};

#[tokio::test]
async fn termination_refuses_changed_call_and_incomplete_or_foreign_phase() {
    let clock = std::sync::Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = super::MemoryStore::with_clock(clock.clone());
    let f = crate::approval_contract::fixture::staged(&store, &clock).await;
    f.execution
        .suspend_for_approval(f.batch.clone(), f.owner.clone())
        .await
        .unwrap();
    let request = crate::approval_contract::termination::request(&store, &f, false).await;
    let original = store.state.lock().unwrap().clone();
    for case in 0..6 {
        {
            let mut state = store.state.lock().unwrap();
            *state = original.clone();
            if case == 0 {
                state
                    .calls
                    .get_mut(&f.batch.calls[0].target.call_id)
                    .unwrap()
                    .arguments = serde_json::json!({"changed":true});
            } else {
                historical_phase(&mut state, &f, case);
            }
        }
        let run = store.get_run(&f.run.id).await.unwrap();
        let events = store
            .list_events(&f.run.session_id, None, 1000)
            .await
            .unwrap();
        let error = store.terminate_approval(request.clone()).await.unwrap_err();
        assert!(matches!(
            error,
            ApprovalError::SemanticConflict
                | ApprovalError::UnknownStoreOutcome
                | ApprovalError::CleanupBlocked
        ));
        assert_eq!(store.get_run(&f.run.id).await.unwrap(), run);
        assert_eq!(
            store
                .list_events(&f.run.session_id, None, 1000)
                .await
                .unwrap(),
            events
        );
        assert_eq!(
            store.session_revision(&f.run.session_id).await.unwrap(),
            request.expected_revision
        );
        assert!(
            store
                .lookup_approval(&f.run.session_id, &request.id)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store.state.lock().unwrap().approval_barriers[&f.run.session_id],
            request.continuation.id
        );
    }
    *store.state.lock().unwrap() = original;
    store.terminate_approval(request).await.unwrap();
    assert!(store.get_run(&f.run.id).await.unwrap().unwrap().lease_until <= clock.now());
}

fn historical_phase(
    state: &mut super::super::State,
    f: &crate::approval_contract::fixture::Fixture,
    case: usize,
) {
    let mut call = state.calls[&f.batch.calls[0].target.call_id].clone();
    call.id = crabber_core::ToolCallId::new();
    call.status = ToolCallStatus::Completed;
    let output = ToolOutput::new(vec![], false).unwrap();
    call.result = Some(ToolResult {
        status: ToolResultStatus::Completed,
        content: output.to_content_blocks(),
    });
    let mut target = f.batch.calls[0].target.clone();
    target.id = crabber_core::PhaseId::new();
    target.call_id = call.id.clone();
    target.attempt = f.batch.attempt.clone();
    if case == 2 {
        target.attempt.id = crabber_core::AttemptId::new();
    }
    if case == 3 {
        target.attempt.generation = target.attempt.generation.next().unwrap();
    }
    let mut phase = crate::phase::PhaseRecord {
        snapshot: PhaseSnapshot {
            session_id: f.run.session_id.clone(),
            run_id: f.run.id.clone(),
            target,
            call_binding: crate::phase::call_binding(&call).unwrap(),
            state: PhaseState::Completed,
            quiescence: if case == 1 {
                PhaseQuiescence::Incomplete
            } else {
                PhaseQuiescence::Complete
            },
        },
        token: "historical-token".into(),
        output: Some(output),
    };
    if case == 4 {
        call.result
            .as_mut()
            .unwrap()
            .content
            .push(crabber_core::ContentBlock::Text {
                text: "forged".into(),
            });
    }
    if case == 5 {
        phase.snapshot.state = PhaseState::Uncertain;
        phase.output = None;
    }
    state
        .call_order
        .push(f.run.session_id.clone(), call.id.clone());
    state.calls.insert(call.id.clone(), call);
    state.phases.insert(phase.snapshot.target.id.clone(), phase);
}
