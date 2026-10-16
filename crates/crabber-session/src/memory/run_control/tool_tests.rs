use super::*;
use crate::tool_continuation_contract::fixture;

#[tokio::test]
async fn waiting_termination_requires_its_immutable_current_proof() {
    for corruption in 0..9 {
        let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
        let store = MemoryStore::with_clock(clock);
        let f = fixture::fixture(&store).await;
        f.execution
            .suspend_tool_continuation(f.suspension.clone(), f.authority)
            .await
            .unwrap();
        let original = store
            .state
            .lock()
            .unwrap()
            .tool_continuations
            .get(&f.suspension.continuation.id)
            .unwrap()
            .clone();
        let target = RunControlTarget::of(&f.admitted.run);
        let request = RunControlRequest {
            id: ControlId::new(),
            target: target.clone(),
            expected_revision: store.session_revision(&target.session_id).await.unwrap(),
            action: RunControlAction::Cancel {
                resolver: crate::ApprovalResolverClaim {
                    resolver_id: "controller".into(),
                    authority: crabber_core::AuthorityRequirement::new("host", "1").unwrap(),
                    authority_claim: "host cancellation".into(),
                },
            },
        };
        let crate::RunControlOutcome::Applied(saved) =
            store.control_run(request.clone()).await.unwrap()
        else {
            panic!("fresh cancellation")
        };
        {
            let mut state = store.state.lock().unwrap();
            corrupt(&mut state, original, &target, &request.id, corruption);
        }
        assert_eq!(
            store
                .tool_continuation_snapshot(&target.session_id, &target.run_id)
                .await
                .unwrap_err(),
            crate::ToolContinuationError::UnknownStoreOutcome,
            "corruption {corruption}"
        );
        if corruption < 7 {
            assert_eq!(
                store.control_run(request).await.unwrap(),
                crate::RunControlOutcome::Replayed(saved)
            );
        } else {
            assert!(store.control_run(request).await.is_err());
        }
    }
}

fn corrupt(
    state: &mut State,
    original: crate::tool_continuation::ToolContinuationRecord,
    target: &RunControlTarget,
    cancel_id: &ControlId,
    corruption: u8,
) {
    let id = &original.batch.continuation.id.clone();
    match corruption {
        0 => state.tool_continuations.get_mut(id).unwrap().termination = None,
        1 => {
            state.tool_continuations.get_mut(id).unwrap().state =
                crate::ToolContinuationState::Waiting;
        }
        2 => {
            state.tool_continuations.get_mut(id).unwrap().questions[0]
                .view
                .state = crabber_core::InteractionState::Pending;
        }
        3 => {
            let proof = state
                .tool_continuations
                .get(id)
                .unwrap()
                .termination
                .as_ref()
                .unwrap();
            let key = (target.session_id.clone(), proof.id.clone());
            state.operation_receipts.remove(&key);
        }
        4 => {
            state.tool_continuations.insert(id.clone(), original);
            state
                .tool_barriers
                .insert(target.session_id.clone(), id.clone());
        }
        5 => {
            state.tool_continuations.remove(id);
            state.tool_barriers.remove(&target.session_id);
        }
        6 => {
            let proof = state
                .tool_continuations
                .get_mut(id)
                .unwrap()
                .termination
                .as_mut()
                .unwrap();
            let RunControlAction::Cancel { resolver } = &mut proof.original.action else {
                panic!("cancel")
            };
            resolver.authority_claim = "changed claimed decision".into();
            proof.accepted.semantic_digest = proof.original.fingerprint().unwrap();
        }
        7 => {
            state
                .operation_receipts
                .remove(&(target.session_id.clone(), cancel_id.clone()));
        }
        _ => {
            let foreign = state
                .operation_receipts
                .values()
                .find(|receipt| matches!(receipt, OperationReceipt::Attempt(_)))
                .unwrap()
                .clone();
            state
                .operation_receipts
                .insert((target.session_id.clone(), cancel_id.clone()), foreign);
        }
    }
}
