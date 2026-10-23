use super::*;
use crate::{
    PhaseCommand, PhaseQuiescence, PhaseState, PhaseWriteAuthority, Store, UncertainPhaseChoice,
    phase_contract::{applied, fixture, mutation, output, resolver},
};

#[tokio::test]
async fn resolution_retains_typed_output_without_acknowledging_cleanup() {
    let store = MemoryStore::with_clock(std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    )));
    for success in [false, true] {
        let (admitted, execution, owner, claim) = fixture(&store).await;
        let (claimed, _) = applied(
            execution
                .mutate_phase(claim, Some(PhaseWriteAuthority::Attempt(owner)))
                .await
                .unwrap(),
        );
        let (uncertain, _) = applied(
            execution
                .mutate_phase(
                    mutation(
                        claimed.revision,
                        PhaseCommand::MarkStoppedUncertain {
                            target: claimed.phase.target,
                            claim: resolver(),
                        },
                    ),
                    None,
                )
                .await
                .unwrap(),
        );
        let value = output();
        let choice = if success {
            UncertainPhaseChoice::Success {
                output: value.clone(),
            }
        } else {
            UncertainPhaseChoice::Failure {
                output: Some(value.clone()),
            }
        };
        let crate::PhaseResolutionOutcome::Applied(resolved) = store
            .resolve_uncertain_phase(crate::PhaseResolution {
                session_id: admitted.session.id.clone(),
                run_id: admitted.run.id.clone(),
                resolution: mutation(
                    uncertain.revision,
                    PhaseCommand::ResolveUncertain {
                        target: uncertain.phase.target,
                        claim: resolver(),
                        choice,
                    },
                ),
                message_id: crabber_core::MessageId::new(),
                part_id: crabber_core::PartId::new(),
                turn_id: None,
                created_at: time::OffsetDateTime::UNIX_EPOCH,
                duration_ms: 0,
            })
            .await
            .unwrap()
        else {
            panic!("fresh resolution");
        };
        let state = store.state.lock().unwrap();
        let record = state.phases.get(&resolved.phase.target.id).unwrap();
        assert_eq!(record.output.as_ref().unwrap().content(), value.content());
        assert_eq!(record.output.as_ref().unwrap().is_error(), !success);
        assert_eq!(record.snapshot.quiescence, PhaseQuiescence::Pending);
        let call = state.calls.get(&resolved.phase.target.call_id).unwrap();
        assert_eq!(
            call.result.as_ref().unwrap().content,
            value.to_content_blocks()
        );
        assert_eq!(
            call.status,
            if success {
                crabber_core::ToolCallStatus::Completed
            } else {
                crabber_core::ToolCallStatus::Failed
            }
        );

        let receipt = serde_json::to_string(&resolved).unwrap();
        assert!(!receipt.contains("private result"));
        let event = state
            .events
            .iter()
            .find(|e| e.cursor == Some(resolved.event_cursor))
            .unwrap();
        let event_json = serde_json::to_string(event).unwrap();
        assert!(!event_json.contains("private result"));
        assert!(event_json.contains("operator-fixture"));
        assert_eq!(admitted.run.id, resolved.phase.run_id);
    }
}

#[tokio::test]
async fn unresolved_outcome_blocks_attempt_cleanup_even_with_quiescence_complete() {
    let store = MemoryStore::with_clock(std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    )));
    let (admitted, execution, owner, claim) = fixture(&store).await;
    let (claimed, _) = applied(
        execution
            .mutate_phase(claim, Some(PhaseWriteAuthority::Attempt(owner.clone())))
            .await
            .unwrap(),
    );
    let (uncertain, _) = applied(
        execution
            .mutate_phase(
                mutation(
                    claimed.revision,
                    PhaseCommand::MarkStoppedUncertain {
                        target: claimed.phase.target,
                        claim: resolver(),
                    },
                ),
                None,
            )
            .await
            .unwrap(),
    );
    let request = mutation(
        uncertain.revision,
        PhaseCommand::RecordQuiescence {
            target: uncertain.phase.target.clone(),
            result: crate::PhaseQuiescenceResult::Complete,
            host_claim: Some(resolver()),
        },
    );
    let (quiet, _) = applied(execution.mutate_phase(request, None).await.unwrap());
    assert_eq!(quiet.phase.state, PhaseState::Uncertain);
    let finish = crate::AttemptMutation {
        id: ControlId::new(),
        expected_revision: quiet.revision,
        command: crate::AttemptCommand::Finish {
            target: quiet.phase.target.attempt.clone(),
            outcome: crate::AttemptFinish::OutcomeUnknown,
        },
    };
    let crate::AttemptMutationOutcome::Applied { receipt, .. } = execution
        .mutate_attempt(finish, Some(owner.clone()))
        .await
        .unwrap()
    else {
        panic!("fresh finish");
    };
    let cleanup = crate::AttemptMutation {
        id: ControlId::new(),
        expected_revision: receipt.revision,
        command: crate::AttemptCommand::RecordCleanup {
            target: receipt.attempt.target,
            result: crate::AttemptCleanupResult::Complete,
        },
    };
    assert_eq!(
        execution
            .mutate_attempt(cleanup, Some(owner))
            .await
            .unwrap_err(),
        crate::AttemptError::CleanupBlocked
    );
    assert_eq!(
        store.session_revision(&admitted.session.id).await.unwrap(),
        receipt.revision
    );
}

#[tokio::test]
async fn operation_namespace_and_legacy_claim_cannot_bypass_phase() {
    let store = MemoryStore::with_clock(std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    )));
    let (admitted, execution, owner, claim) = fixture(&store).await;
    let (claimed, _) = applied(
        execution
            .mutate_phase(claim.clone(), Some(PhaseWriteAuthority::Attempt(owner)))
            .await
            .unwrap(),
    );
    let public_state = crate::PublicStateMutation {
        id: claim.id.clone(),
        expected_revision: claimed.revision,
        contents: crabber_core::PublicStateContents::new(serde_json::Value::Null, vec![]).unwrap(),
    };
    assert_eq!(
        execution
            .mutate_public_state(public_state)
            .await
            .unwrap_err(),
        crate::PublicStateError::SemanticConflict
    );
    assert_eq!(
        store
            .lookup_attempt_mutation(&admitted.session.id, &claim.id)
            .await
            .unwrap_err(),
        crate::AttemptError::SemanticConflict
    );
    let mut event = store
        .list_events(&admitted.session.id, None, 100)
        .await
        .unwrap()
        .pop()
        .unwrap();
    event.kind = crabber_core::EventKind::ToolCallRunning;
    assert!(matches!(
        execution
            .claim_tool_call(&claimed.phase.target.call_id, event)
            .await,
        Err(crate::StoreError::Conflict)
    ));
    let mut changed = claim;
    changed.command = PhaseCommand::Begin {
        target: claimed.phase.target.clone(),
    };
    assert!(matches!(
        execution.mutate_phase(changed, None).await,
        Err(Error::SemanticConflict)
    ));
    assert!(matches!(
        execution
            .mutate_phase(
                mutation(
                    claimed.revision,
                    PhaseCommand::Claim {
                        target: {
                            let mut target = claimed.phase.target.clone();
                            target.id = PhaseId::new();
                            target
                        }
                    }
                ),
                None
            )
            .await,
        Err(Error::StaleOwner)
    ));
    assert_eq!(
        store.session_revision(&admitted.session.id).await.unwrap(),
        claimed.revision
    );
}

#[tokio::test]
async fn memory_phase_commit_faults() {
    let store = MemoryStore::with_clock(std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    )));
    crate::phase_contract::run_fault_contract(
        &store,
        crate::binding_fault::setter(
            &store.phase_fault,
            |point: crate::phase_contract::FaultPoint| point as u8,
        ),
    )
    .await;
}

#[tokio::test]
async fn resolution_rejects_changed_authoritative_call_binding_without_writes() {
    let store = MemoryStore::with_clock(std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    )));
    let (admitted, execution, owner, claim) = fixture(&store).await;
    let (claimed, _) = applied(
        execution
            .mutate_phase(claim, Some(PhaseWriteAuthority::Attempt(owner)))
            .await
            .unwrap(),
    );
    let (uncertain, _) = applied(
        execution
            .mutate_phase(
                mutation(
                    claimed.revision,
                    PhaseCommand::MarkStoppedUncertain {
                        target: claimed.phase.target,
                        claim: resolver(),
                    },
                ),
                None,
            )
            .await
            .unwrap(),
    );
    let request = crate::PhaseResolution {
        session_id: admitted.session.id.clone(),
        run_id: admitted.run.id.clone(),
        resolution: mutation(
            uncertain.revision,
            PhaseCommand::ResolveUncertain {
                target: uncertain.phase.target.clone(),
                claim: resolver(),
                choice: UncertainPhaseChoice::Success { output: output() },
            },
        ),
        message_id: crabber_core::MessageId::new(),
        part_id: crabber_core::PartId::new(),
        turn_id: None,
        created_at: time::OffsetDateTime::UNIX_EPOCH,
        duration_ms: 0,
    };
    // Corrupt the authoritative record behind its existing immutable phase
    // binding. The control must not accept freshly recaptured arguments.
    let before = {
        let mut state = store.state.lock().unwrap();
        state
            .calls
            .get_mut(&uncertain.phase.target.call_id)
            .unwrap()
            .arguments = serde_json::json!({"changed": "after phase ownership was recorded"});
        state.clone()
    };
    assert_eq!(
        store
            .resolve_uncertain_phase(request.clone())
            .await
            .unwrap_err(),
        Error::SemanticConflict
    );
    assert!(
        store
            .lookup_phase_mutation(&admitted.session.id, &request.resolution.id)
            .await
            .unwrap()
            .is_none()
    );
    let state = store.state.lock().unwrap();
    assert_eq!(state.runs, before.runs);
    assert_eq!(state.calls, before.calls);
    assert_eq!(state.messages, before.messages);
    assert_eq!(state.events, before.events);
    assert_eq!(state.public_revisions, before.public_revisions);
    assert_eq!(state.snapshot_revisions, before.snapshot_revisions);
    assert_eq!(
        state.operation_receipts.len(),
        before.operation_receipts.len()
    );
    assert_eq!(state.approval_barriers, before.approval_barriers);
    let phase = state.phases.get(&uncertain.phase.target.id).unwrap();
    let old_phase = before.phases.get(&uncertain.phase.target.id).unwrap();
    assert_eq!(phase.snapshot, old_phase.snapshot);
    assert_eq!(phase.token, old_phase.token);
    assert_eq!(phase.output, old_phase.output);
    let attempt = state
        .attempts
        .get(&uncertain.phase.target.attempt.id)
        .unwrap();
    let old_attempt = before
        .attempts
        .get(&uncertain.phase.target.attempt.id)
        .unwrap();
    assert_eq!(attempt.snapshot, old_attempt.snapshot);
    assert_eq!(attempt.owner, old_attempt.owner);
    assert_eq!(attempt.token, old_attempt.token);
    assert_eq!(attempt.run_token, old_attempt.run_token);
    assert_eq!(attempt.lease_until, old_attempt.lease_until);
}

#[tokio::test]
async fn phase_page_rejects_corrupt_sentinel_instead_of_omitting_it() {
    let store = MemoryStore::with_clock(std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    )));
    let (mut request, sentinel) = crate::phase_contract::controls::pagination(&store).await;
    request.limit = 1;
    let original = store.list_tool_phases(request.clone()).await.unwrap();
    assert_eq!(original.phases.len(), 1);
    assert!(original.next_after.is_some());
    store
        .state
        .lock()
        .unwrap()
        .phases
        .get_mut(&sentinel)
        .unwrap()
        .snapshot
        .session_id = crabber_core::SessionId::new();
    assert_eq!(
        store.list_tool_phases(request).await.unwrap_err(),
        crate::PhaseError::UnknownStoreOutcome
    );
}

#[tokio::test]
async fn terminal_carrier_accepts_large_valid_call() {
    crate::phase_contract::terminal_carrier::run(&MemoryStore::new()).await;
}
