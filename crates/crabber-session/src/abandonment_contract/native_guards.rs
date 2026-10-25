//! Legacy abandonment must not invent outcomes for native execution.
use super::{FixtureStore, event, request};
use crate::{
    PhaseCommand, PhaseMutationOutcome, PhaseQuiescenceResult, PhaseResolverClaim, PhaseStart,
    PhaseTarget, PhaseWriteAuthority, Store,
};
use crabber_core::{
    AbandonAuthority, AbandonError, AbandonRequest, Clock, EventKind, ManualClock, RunId,
    RunStatus, Usage,
};
use serde_json::{Value, json};
use time::OffsetDateTime;

pub(super) async fn run(store: &impl FixtureStore, clock: &ManualClock) {
    // Each fixture has a real Started attempt; mode zero has no phase.
    // The remaining modes cover Started/Pending and Uncertain with Pending
    // or Incomplete cleanup, without assuming process death proves cleanup.
    for mode in 0..4 {
        clock.set(OffsetDateTime::UNIX_EPOCH);
        let (admitted, execution, owner, claim) = crate::phase_contract::fixture(store).await;
        let target = claim.command.target().clone();
        let phase = if mode > 0 {
            let PhaseMutationOutcome::Applied {
                receipt,
                authority: Some(authority),
            } = execution
                .mutate_phase(claim, Some(PhaseWriteAuthority::Attempt(owner)))
                .await
                .unwrap()
            else {
                panic!("fresh phase claim")
            };
            let begin = crate::phase_contract::mutation(
                receipt.revision,
                PhaseCommand::Begin {
                    target: target.clone(),
                },
            );
            execution
                .start_tool_phase(
                    PhaseStart {
                        begin,
                        turn_id: None,
                        created_at: OffsetDateTime::UNIX_EPOCH,
                    },
                    Some(authority),
                )
                .await
                .unwrap();
            if mode >= 2 {
                change(
                    store,
                    execution.as_ref(),
                    &admitted.run.id,
                    PhaseCommand::MarkStoppedUncertain {
                        target: target.clone(),
                        claim: resolver(),
                    },
                )
                .await;
            }
            if mode == 3 {
                change(
                    store,
                    execution.as_ref(),
                    &admitted.run.id,
                    PhaseCommand::RecordQuiescence {
                        target: target.clone(),
                        result: PhaseQuiescenceResult::Incomplete,
                        host_claim: Some(resolver()),
                    },
                )
                .await;
            }
            Some(target)
        } else {
            None
        };
        refuse(store, clock, &admitted.run.id, phase.as_ref()).await;
    }
    // A genuine persisted approval suspension has no native tool phases and
    // completed prior-attempt cleanup; it still requires typed cancellation.
    clock.set(OffsetDateTime::UNIX_EPOCH);
    let f = crate::approval_contract::fixture::staged(store, clock).await;
    f.execution
        .suspend_for_approval(f.batch, f.owner)
        .await
        .unwrap();
    refuse(store, clock, &f.run.id, None).await;
}

fn resolver() -> PhaseResolverClaim {
    PhaseResolverClaim {
        resolver_id: "test-host".into(),
        authority_claim: "worker stopped, resources unresolved".into(),
    }
}

async fn change(
    store: &impl Store,
    execution: &dyn crate::ExecutionStore,
    run: &RunId,
    command: PhaseCommand,
) {
    let run = store.get_run(run).await.unwrap().unwrap();
    execution
        .mutate_phase(
            crate::phase_contract::mutation(
                store.session_revision(&run.session_id).await.unwrap(),
                command,
            ),
            None,
        )
        .await
        .unwrap();
}

async fn state(store: &impl FixtureStore, run_id: &RunId, phase: Option<&PhaseTarget>) -> Value {
    let run = store.get_run(run_id).await.unwrap().unwrap();
    let phase = match phase {
        Some(target) => Some(store.tool_phase(run_id, &target.id).await.unwrap()),
        None => None,
    };
    json!({
        "run": run,
        "calls": store.calls(run_id, None).await,
        "messages": store.list_all_messages(&run.session_id).await.unwrap(),
        "events": store.list_events(&run.session_id, None, 1000).await.unwrap(),
        "revision": store.session_revision(&run.session_id).await.unwrap(),
        "attempt": store.latest_execution_attempt(run_id).await.unwrap(),
        "phase": phase,
        "approval": store.approval_snapshot(&run.session_id, run_id).await.unwrap(),
    })
}

async fn refuse(
    store: &impl FixtureStore,
    clock: &ManualClock,
    run_id: &RunId,
    phase: Option<&PhaseTarget>,
) {
    let mut run = store.get_run(run_id).await.unwrap().unwrap();
    // Suspension expires its lease. Reclaim only the legacy run fence so these
    // calls reach the native-evidence guard rather than failing lease checks.
    // This never grants native attempt/phase authority or resumes execution.
    if run.lease_until <= clock.now() {
        store
            .claim_expired_run(run_id, "legacy-boundary-guard-fixture")
            .await
            .unwrap();
        run = store.get_run(run_id).await.unwrap().unwrap();
    }
    let before = state(store, run_id, phase).await;
    let execution = store
        .execution(crabber_core::RunFence {
            run_id: run.id.clone(),
            claim_token: run.claim_token.clone(),
        })
        .await
        .unwrap();
    assert_eq!(
        execution
            .pause_run(
                json!({"overwrite": "forbidden"}),
                event(&run, EventKind::RunPaused)
            )
            .await
            .unwrap_err(),
        crate::StoreError::Conflict
    );
    assert_eq!(state(store, run_id, phase).await, before);
    assert_eq!(
        execution
            .settle_run(
                RunStatus::Completed,
                None,
                Usage::default(),
                event(&run, EventKind::RunSettled)
            )
            .await
            .unwrap_err(),
        crate::StoreError::Conflict
    );
    assert_eq!(state(store, run_id, phase).await, before);
    for authority in [
        AbandonAuthority::HostStoppedOwner,
        AbandonAuthority::ExpiredLease,
    ] {
        if authority == AbandonAuthority::ExpiredLease {
            clock.set(run.lease_until);
        }
        let request = AbandonRequest {
            expected: crabber_core::RunFence {
                run_id: run.id.clone(),
                claim_token: run.claim_token.clone(),
            },
            expected_owner: run.owner.clone(),
            authority,
        };
        for _ in 0..2 {
            assert_eq!(
                store.abandon_run(request.clone()).await.unwrap_err(),
                AbandonError::NativeReconciliationRequired
            );
            assert_eq!(state(store, run_id, phase).await, before);
        }
        let session = store.get_session(&run.session_id).await.unwrap().unwrap();
        let mut next = request_for_session(&session);
        next.expected_revision = Some(store.session_revision(&run.session_id).await.unwrap());
        assert!(matches!(
            store.admit_run(next).await,
            Err(crate::StoreError::Busy)
        ));
        assert_eq!(state(store, run_id, phase).await, before);
    }
}

fn request_for_session(session: &crabber_core::Session) -> crate::AdmitRequest {
    let mut next = request();
    next.session_id = Some(session.id.clone());
    next.user_message.session_id = session.id.clone();
    next.workspace_id.clone_from(&session.workspace_id);
    next.directory.clone_from(&session.directory);
    next
}
