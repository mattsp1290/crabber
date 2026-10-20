//! A cached genuine worker cannot use coherently restored private projections.
use crate::Store;
use std::sync::Arc;
#[tokio::test]
async fn erased_source_tail_and_coherent_private_restore_cannot_issue_fresh_begin() {
    let clock = Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = crate::MemoryStore::with_clock(clock.clone());
    let (f, (attempt, phases, continuation, execution)) =
        crate::session_owner::preparation_contract::claim_history::fixture_checkpoint(
            &store,
            &clock,
            |run, target, continuation| {
                let store = &store;
                async move {
                    let (attempt, phases, record) = {
                        let state = store.state.lock().unwrap();
                        let attempt = state.attempts.get(&target.id).unwrap().clone();
                        let phases = state
                            .phases
                            .iter()
                            .filter(|(_, p)| p.snapshot.run_id == run.id)
                            .map(|(id, p)| (id.clone(), p.clone()))
                            .collect::<Vec<_>>();
                        let record = state.tool_continuations.get(&continuation).unwrap().clone();
                        (attempt, phases, record)
                    };
                    let execution = store
                        .execution(crabber_core::RunFence {
                            run_id: run.id.clone(),
                            claim_token: run.claim_token.clone(),
                        })
                        .await
                        .unwrap();
                    (attempt, phases, (continuation, record), execution)
                }
            },
        )
        .await;
    let run = f.before_tail;
    let grant = f.first_grant;
    let crate::ToolContinuationOutcome::ReclaimedUnstarted {
        attempt: source_attempt,
        ..
    } = &f.first.outcome
    else {
        panic!("actual first recovery")
    };
    let target = source_attempt.target.clone();
    {
        let mut state = store.state.lock().unwrap();
        let key = (run.session_id.clone(), run.id.clone());
        let sequence = state.native_run_source_heads[&key].sequence;
        state
            .native_run_issuances
            .remove(&(key.0.clone(), key.1, sequence));
        state
            .operation_receipts
            .remove(&(run.session_id.clone(), f.tail.operation_id));
        state.events.remove_for_test(f.tail.event_cursor).unwrap();
        state.runs.insert(run.id.clone(), run.clone());
        state.attempts.insert(target.id.clone(), attempt);
        for (id, phase) in phases {
            state.phases.insert(id, phase);
        }
        state
            .tool_continuations
            .insert(continuation.0, continuation.1);
    }
    clock.set(run.updated_at);
    let revision = store.session_revision(&run.session_id).await.unwrap();
    assert_eq!(grant.fence.claim_token, run.claim_token);
    let outcome = execution
        .mutate_attempt(
            crate::AttemptMutation {
                id: crabber_core::ControlId::new(),
                expected_revision: revision,
                command: crate::AttemptCommand::Begin { target },
            },
            Some(grant.authority),
        )
        .await;
    assert!(
        matches!(outcome, Err(crate::AttemptError::UnknownStoreOutcome)),
        "restored actual old authority must not issue a fresh Begin: {outcome:?}"
    );
    assert_eq!(
        store.session_revision(&run.session_id).await.unwrap(),
        revision
    );
}
