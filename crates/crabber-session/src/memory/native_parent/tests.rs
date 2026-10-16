use super::*;
use crate::{
    AttemptCommand, AttemptMutation, AttemptMutationOutcome, PhaseCommand, PhaseMutation,
    PhaseStart, PhaseWriteAuthority, Store,
};
use crabber_core::{ControlId, ManualClock};
use std::sync::{Arc, atomic::Ordering};
use time::OffsetDateTime;
fn store() -> (super::super::MemoryStore, Arc<ManualClock>) {
    let clock = Arc::new(ManualClock::new(OffsetDateTime::UNIX_EPOCH));
    (super::super::MemoryStore::with_clock(clock.clone()), clock)
}
fn current(
    store: &super::super::MemoryStore,
    run: &crabber_core::RunId,
    phase: Option<&crate::PhaseTarget>,
) -> Result<Witness, Error> {
    let state = store.state.lock().unwrap();
    let run = &state.runs[run];
    let attempt = state
        .attempts
        .values()
        .filter(|a| a.snapshot.run_id == run.id)
        .max_by_key(|a| a.snapshot.target.generation)
        .unwrap();
    match phase {
        Some(target) => verify_phase(&state, run, attempt, &state.phases[&target.id]),
        None => verify_attempt(&state, run, attempt),
    }
}
#[tokio::test]
async fn native_begin_witnesses_cover_both_phase_paths_and_private_heartbeats() {
    for compound in [false, true] {
        let (store, _clock) = store();
        let (admitted, execution, owner, claim) = crate::phase_contract::fixture(&store).await;
        let parent = current(&store, &admitted.run.id, None).unwrap();
        let (claimed, phase) = crate::phase_contract::applied(
            execution
                .mutate_phase(claim, Some(PhaseWriteAuthority::Attempt(owner)))
                .await
                .unwrap(),
        );
        let begin = PhaseMutation {
            id: ControlId::new(),
            expected_revision: claimed.revision,
            command: PhaseCommand::Begin {
                target: claimed.phase.target.clone(),
            },
        };
        let result = if compound {
            execution
                .start_tool_phase(
                    PhaseStart {
                        begin,
                        turn_id: None,
                        created_at: admitted.run.created_at,
                    },
                    phase,
                )
                .await
        } else {
            execution
                .mutate_phase(begin, phase.map(PhaseWriteAuthority::Phase))
                .await
        };
        let (started, _) = crate::phase_contract::applied(result.unwrap());
        let witness = current(&store, &admitted.run.id, Some(&started.phase.target)).unwrap();
        let revision = store.session_revision(&admitted.session.id).await.unwrap();
        execution
            .renew_lease(admitted.run.lease_until + time::Duration::seconds(1))
            .await
            .unwrap();
        assert_eq!(
            store.session_revision(&admitted.session.id).await.unwrap(),
            revision
        );
        assert!(current(&store, &admitted.run.id, None).unwrap() == parent);
        assert!(current(&store, &admitted.run.id, Some(&started.phase.target)).unwrap() == witness);
        let text = serde_json::to_string(&witness).unwrap();
        let state = store.state.lock().unwrap();
        assert!(!text.contains(&state.runs[&admitted.run.id].claim_token));
        assert!(!text.contains(&state.phases[&started.phase.target.id].token));
    }
}
#[tokio::test]
async fn native_parent_latest_revocation_refuses_restored_private_records() {
    let (store, _clock) = store();
    let (admitted, _execution, _owner, planned) = crate::phase_contract::fixture(&store).await;
    let (run, attempt) = {
        let state = store.state.lock().unwrap();
        (
            state.runs[&admitted.run.id].clone(),
            state.attempts[&planned.command.target().attempt.id].clone(),
        )
    };
    store
        .mark_attempt_uncertain(crate::attempt_control_contract::uncertainty(
            &admitted,
            attempt.snapshot.target.clone(),
            store.session_revision(&admitted.session.id).await.unwrap(),
        ))
        .await
        .unwrap();
    {
        let mut state = store.state.lock().unwrap();
        state.runs.insert(run.id.clone(), run);
        state
            .attempts
            .insert(attempt.snapshot.target.id.clone(), attempt);
    }
    assert!(current(&store, &admitted.run.id, None).is_err());
}
#[tokio::test]
async fn native_parent_latest_phase_completion_refuses_restored_private_records() {
    let (store, _clock) = store();
    let (admitted, execution, owner, claim) = crate::phase_contract::fixture(&store).await;
    let (claimed, phase) = crate::phase_contract::applied(
        execution
            .mutate_phase(claim, Some(PhaseWriteAuthority::Attempt(owner)))
            .await
            .unwrap(),
    );
    let phase = phase.unwrap();
    let (started, _) = crate::phase_contract::applied(
        execution
            .mutate_phase(
                crate::phase_contract::mutation(
                    claimed.revision,
                    PhaseCommand::Begin {
                        target: claimed.phase.target,
                    },
                ),
                Some(PhaseWriteAuthority::Phase(phase.clone())),
            )
            .await
            .unwrap(),
    );
    let saved = store.state.lock().unwrap().phases[&started.phase.target.id].clone();
    execution
        .mutate_phase(
            crate::phase_contract::mutation(
                started.revision,
                PhaseCommand::Complete {
                    target: started.phase.target.clone(),
                    output: crate::phase_contract::output(),
                },
            ),
            Some(PhaseWriteAuthority::Phase(phase)),
        )
        .await
        .unwrap();
    store
        .state
        .lock()
        .unwrap()
        .phases
        .insert(started.phase.target.id.clone(), saved);
    assert!(current(&store, &admitted.run.id, Some(&started.phase.target)).is_err());
}
#[tokio::test]
async fn reclaimed_same_target_begin_authenticates_new_tokens_only() {
    let (store, clock) = store();
    let admitted = crate::public_state_contract::admit(&store, OffsetDateTime::UNIX_EPOCH).await;
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    let AttemptMutationOutcome::Applied {
        receipt: reserved,
        authority: Some(old),
    } = execution
        .mutate_attempt(
            crate::attempt_contract::reserve(
                store.session_revision(&admitted.session.id).await.unwrap(),
            ),
            None,
        )
        .await
        .unwrap()
    else {
        panic!("reserve")
    };
    let original = store.state.lock().unwrap().attempts[&reserved.attempt.target.id].clone();
    clock.set(admitted.run.lease_until);
    let fence = store
        .claim_expired_run(&admitted.run.id, "replacement")
        .await
        .unwrap();
    let execution = store.execution(fence).await.unwrap();
    let AttemptMutationOutcome::Applied {
        receipt: reclaimed,
        authority: Some(owner),
    } = execution
        .mutate_attempt(
            AttemptMutation {
                id: ControlId::new(),
                expected_revision: reserved.revision,
                command: AttemptCommand::ReclaimUnstarted {
                    target: reserved.attempt.target.clone(),
                },
            },
            None,
        )
        .await
        .unwrap()
    else {
        panic!("reclaim")
    };
    assert_eq!(reclaimed.attempt.target, reserved.attempt.target);
    execution
        .mutate_attempt(
            AttemptMutation {
                id: ControlId::new(),
                expected_revision: reclaimed.revision,
                command: AttemptCommand::Begin {
                    target: reclaimed.attempt.target,
                },
            },
            Some(owner),
        )
        .await
        .unwrap();
    assert!(current(&store, &admitted.run.id, None).is_ok());
    {
        let mut state = store.state.lock().unwrap();
        let mut restored = original;
        restored.snapshot.state = crate::AttemptState::Started;
        restored.lease_until = state.runs[&admitted.run.id].lease_until;
        state
            .runs
            .get_mut(&admitted.run.id)
            .unwrap()
            .claim_token
            .clone_from(&restored.run_token);
        assert!(old.matches_store(&restored.snapshot, &restored.token, &restored.run_token));
        state
            .attempts
            .insert(restored.snapshot.target.id.clone(), restored);
    }
    assert!(current(&store, &admitted.run.id, None).is_err());
}
#[tokio::test]
async fn missing_legacy_witness_is_never_reconstructed_on_metadata_retry() {
    let (store, _clock) = store();
    let (admitted, _execution, _owner, _planned) = crate::phase_contract::fixture(&store).await;
    store.state.lock().unwrap().native_parent_witnesses.clear();
    assert!(current(&store, &admitted.run.id, None).is_err());
    let source = store
        .state
        .lock()
        .unwrap()
        .operation_receipts
        .values()
        .find_map(|r| match r {
            OperationReceipt::Attempt(r) if r.attempt.state == crate::AttemptState::Started => {
                Some(r.clone())
            }
            _ => None,
        })
        .unwrap();
    assert!(
        store
            .attempt_receipt(&admitted.session.id, &source.operation_id)
            .unwrap()
            .is_some()
    );
    let execution = store.execution(admitted.fence).await.unwrap();
    assert!(matches!(
        execution
            .mutate_attempt(
                AttemptMutation {
                    id: source.operation_id,
                    expected_revision: crabber_core::SessionRevision::new(
                        source.revision.get() - 1
                    )
                    .unwrap(),
                    command: AttemptCommand::Begin {
                        target: source.attempt.target
                    },
                },
                None
            )
            .await
            .unwrap(),
        AttemptMutationOutcome::Replayed(_)
    ));
    assert!(
        store
            .state
            .lock()
            .unwrap()
            .native_parent_witnesses
            .is_empty()
    );
}
#[tokio::test]
async fn begin_witness_rolls_back_with_begin_and_replays_after_lost_ack() {
    let (store, _clock) = store();
    let admitted = crate::public_state_contract::admit(&store, OffsetDateTime::UNIX_EPOCH).await;
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    let AttemptMutationOutcome::Applied {
        receipt: reserved,
        authority,
    } = execution
        .mutate_attempt(
            crate::attempt_contract::reserve(
                store.session_revision(&admitted.session.id).await.unwrap(),
            ),
            None,
        )
        .await
        .unwrap()
    else {
        panic!("reserve")
    };
    let begin = AttemptMutation {
        id: ControlId::new(),
        expected_revision: reserved.revision,
        command: AttemptCommand::Begin {
            target: reserved.attempt.target,
        },
    };
    store.attempt_fault.store(1, Ordering::SeqCst);
    assert!(
        execution
            .mutate_attempt(begin.clone(), authority.clone())
            .await
            .is_err()
    );
    assert!(
        store
            .state
            .lock()
            .unwrap()
            .native_parent_witnesses
            .is_empty()
    );
    store.attempt_fault.store(2, Ordering::SeqCst);
    assert!(
        execution
            .mutate_attempt(begin.clone(), authority)
            .await
            .is_err()
    );
    assert_eq!(store.state.lock().unwrap().native_parent_witnesses.len(), 1);
    store.attempt_fault.store(0, Ordering::SeqCst);
    assert!(matches!(
        execution.mutate_attempt(begin, None).await.unwrap(),
        AttemptMutationOutcome::Replayed(_)
    ));
    assert_eq!(store.state.lock().unwrap().native_parent_witnesses.len(), 1);
    assert!(current(&store, &admitted.run.id, None).is_ok());
}

#[tokio::test]
async fn phase_begin_witness_is_atomic_with_generic_and_compound_begin() {
    for compound in [false, true] {
        let (store, _clock) = store();
        let (admitted, execution, owner, claim) = crate::phase_contract::fixture(&store).await;
        let (receipt, phase) = crate::phase_contract::applied(
            execution
                .mutate_phase(claim, Some(PhaseWriteAuthority::Attempt(owner)))
                .await
                .unwrap(),
        );
        let request = crate::phase_contract::mutation(
            receipt.revision,
            PhaseCommand::Begin {
                target: receipt.phase.target,
            },
        );
        for point in [1, 2, 0] {
            store.phase_fault.store(point, Ordering::SeqCst);
            let result = if compound {
                execution
                    .start_tool_phase(
                        PhaseStart {
                            begin: request.clone(),
                            turn_id: None,
                            created_at: admitted.run.created_at,
                        },
                        phase.clone(),
                    )
                    .await
            } else {
                execution
                    .mutate_phase(
                        request.clone(),
                        phase.clone().map(PhaseWriteAuthority::Phase),
                    )
                    .await
            };
            if point == 0 {
                assert!(matches!(
                    result.unwrap(),
                    crate::PhaseMutationOutcome::Replayed(_)
                ));
            } else {
                assert!(result.is_err());
            }
            assert_eq!(
                store.state.lock().unwrap().native_parent_witnesses.len(),
                if point == 1 { 1 } else { 2 }
            );
        }
        assert!(current(&store, &admitted.run.id, Some(request.command.target())).is_ok());
    }
}

#[test]
fn corrupt_parent_carriers_are_bounded_before_copying() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (store, _clock) = store();
    let (admitted, _, _, _) = runtime.block_on(crate::phase_contract::fixture(&store));
    let saved = store.state.lock().unwrap().clone();
    for case in 0..3 {
        let mut state = saved.clone();
        let run = &saved.runs[&admitted.run.id];
        let attempt = state
            .attempts
            .values()
            .find(|a| a.snapshot.run_id == run.id)
            .unwrap();
        let id = attempt.snapshot.target.id.clone();
        if case == 0 {
            let mut event = state
                .events
                .iter()
                .rev()
                .find(|e| e.kind == EventKind::ExecutionAttemptChanged)
                .unwrap()
                .clone();
            event.correlation = Some("x".repeat(1024 * 1024));
            event.cursor = Some(state.events.next_cursor().unwrap());
            state.events.append(event).unwrap();
        } else if case == 1 {
            state
                .native_parent_witnesses
                .values_mut()
                .next()
                .unwrap()
                .source
                .operation_id
                .0 = "x".repeat(1024 * 1024);
        } else {
            state.attempts.get_mut(&id).unwrap().token = "x".repeat(1024 * 1024);
        }
        let measured = allocation_counter::measure(|| {
            assert!(verify_attempt(&state, run, &state.attempts[&id]).is_err());
        });
        assert!(measured.bytes_max < 131_072, "case {case}: {measured:?}");
    }
}

#[tokio::test]
async fn real_run_only_claim_cannot_be_hidden_by_restoring_live_private_tokens() {
    let (store, clock) = store();
    let (admitted, execution, owner, planned) = crate::phase_contract::fixture(&store).await;
    let before = store.get_run(&admitted.run.id).await.unwrap().unwrap();
    let issuance = current(&store, &admitted.run.id, None).unwrap();
    clock.set(before.lease_until);
    store
        .claim_expired_run(&before.id, "replacement")
        .await
        .unwrap();
    let live = store.get_run(&before.id).await.unwrap().unwrap();
    let sequence = store.state.lock().unwrap().native_run_issuances.len();
    {
        let mut state = store.state.lock().unwrap();
        let mut restored = before;
        restored.lease_until = live.lease_until;
        restored.updated_at = live.updated_at;
        state.runs.insert(restored.id.clone(), restored);
        let attempt = state
            .attempts
            .get_mut(&planned.command.target().attempt.id)
            .unwrap();
        attempt.lease_until = live.lease_until;
        assert!(owner.matches_store(&attempt.snapshot, &attempt.token, &attempt.run_token));
        assert_eq!(state.native_parent_witnesses.len(), 1);
        assert!(state.native_parent_witnesses.values().next().unwrap() == &issuance);
    }
    assert!(current(&store, &admitted.run.id, None).is_err());
    // Saving a private heartbeat with that stale restored fence cannot make its
    // token the new immutable source. No public revision is involved.
    assert!(
        execution
            .renew_lease(live.lease_until + time::Duration::seconds(1))
            .await
            .is_err()
    );
    assert_eq!(
        store.state.lock().unwrap().native_run_issuances.len(),
        sequence
    );
    assert!(current(&store, &admitted.run.id, None).is_err());
}

#[tokio::test]
async fn legacy_run_without_source_membership_refuses_fresh_begin() {
    let (store, _clock) = store();
    let admitted = crate::public_state_contract::admit(&store, OffsetDateTime::UNIX_EPOCH).await;
    let execution = store.execution(admitted.fence).await.unwrap();
    let AttemptMutationOutcome::Applied { receipt, authority } = execution
        .mutate_attempt(
            crate::attempt_contract::reserve(
                store.session_revision(&admitted.session.id).await.unwrap(),
            ),
            None,
        )
        .await
        .unwrap()
    else {
        panic!("reserve")
    };
    {
        let mut state = store.state.lock().unwrap();
        state.native_run_issuances.clear();
        state.native_run_sources.clear();
        state.native_run_source_heads.clear();
        state.native_run_protocols.clear();
        state.native_session_protocols.clear();
        state
            .session_owners
            .legacy_native_session_for_test(&admitted.session.id);
    }
    let begin = AttemptMutation {
        id: ControlId::new(),
        expected_revision: receipt.revision,
        command: AttemptCommand::Begin {
            target: receipt.attempt.target,
        },
    };
    assert!(matches!(
        execution.mutate_attempt(begin, authority).await,
        Err(crate::AttemptError::UnknownStoreOutcome)
    ));
    let state = store.state.lock().unwrap();
    assert!(state.native_run_issuances.is_empty());
    assert!(state.native_parent_witnesses.is_empty());
    drop(state);
    assert!(current(&store, &admitted.run.id, None).is_err());
}
