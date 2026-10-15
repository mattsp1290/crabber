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
        let (resolved, _) = applied(
            execution
                .mutate_phase(
                    mutation(
                        uncertain.revision,
                        PhaseCommand::ResolveUncertain {
                            target: uncertain.phase.target,
                            claim: resolver(),
                            choice,
                        },
                    ),
                    None,
                )
                .await
                .unwrap(),
        );
        let state = store.state.lock().unwrap();
        let record = state.phases.get(&resolved.phase.target.id).unwrap();
        assert_eq!(record.output.as_ref().unwrap().content(), value.content());
        assert_eq!(record.output.as_ref().unwrap().is_error(), !success);
        assert_eq!(record.snapshot.quiescence, PhaseQuiescence::Pending);
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
        |point: crate::phase_contract::FaultPoint| {
            store
                .phase_fault
                .store(point as u8, std::sync::atomic::Ordering::SeqCst);
        },
    )
    .await;
}
