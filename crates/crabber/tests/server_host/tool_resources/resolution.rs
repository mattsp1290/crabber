//! Host resolution records an external outcome without asserting cleanup.
use super::*;
use crabber::core::{ControlId, EventKind, MessageId, PartId, Role, ToolOutput};
use crabber::session::{
    PhaseCommand, PhaseError, PhaseMutation, PhaseResolution, PhaseResolutionOutcome,
    PhaseResolverClaim, UncertainPhaseChoice,
};
use std::sync::atomic::Ordering;
use time::OffsetDateTime;

#[tokio::test]
async fn closed_host_resolves_once_and_preserves_incomplete_cleanup() {
    let fixture = Arc::new(Fixture::default());
    let agent = agent(fixture.clone(), "noncooperative", false);
    let handle = agent
        .prompt(None, "reconcile external effect")
        .await
        .unwrap();
    let run = handle.run_id().clone();
    let session = handle.session_id().clone();
    let target = assert_uncertain(handle, &fixture, PhaseQuiescence::Incomplete).await;
    let original_run = fixture.store.get_run(&run).await.unwrap().unwrap();
    let attempt = fixture
        .store
        .latest_execution_attempt(&run)
        .await
        .unwrap()
        .unwrap()
        .attempt;
    agent.close_extensions().await.unwrap();
    record_outcome(&agent, &fixture, &session, &run, &target).await;
    assert_eq!(fixture.invocations.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.store.get_run(&run).await.unwrap().unwrap(),
        original_run
    );
    assert_eq!(
        fixture
            .store
            .latest_execution_attempt(&run)
            .await
            .unwrap()
            .unwrap()
            .attempt,
        attempt
    );
    assert_eq!(
        fixture
            .store
            .list_unfinished_tool_calls(&run)
            .await
            .unwrap(),
        []
    );
    let messages = fixture.store.list_all_messages(&session).await.unwrap();
    assert_eq!(messages.iter().filter(|m| m.role == Role::Tool).count(), 1);
    let events = fixture
        .store
        .list_events(&session, None, 100)
        .await
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == EventKind::ToolCallSettled)
            .count(),
        1
    );
    assert!(!events.iter().any(|e| e.kind == EventKind::RunSettled));
    fixture.release.notify_one();
    let report = agent.drain_execution_tasks(BOUND).await;
    assert_eq!(report.resources[0].resources.pending, 0);
    assert_eq!(
        agent
            .tool_phase(&run, &target.id)
            .await
            .unwrap()
            .phase
            .quiescence,
        PhaseQuiescence::Incomplete
    );
    reconcile_joined_cleanup(&agent, &fixture, &session, &run, &target).await;
}
async fn reconcile_joined_cleanup(
    agent: &Agent,
    fixture: &Fixture,
    session: &crabber::SessionId,
    run: &crabber::core::RunId,
    target: &crabber::session::PhaseTarget,
) {
    use crabber::session::{
        PhaseCleanup, PhaseControlOutcome, PhasePageRequest, PhaseQuiescenceResult,
        PhaseUncertainty, PhaseUncertaintyCause,
    };
    let page = agent
        .list_tool_phases(PhasePageRequest {
            session_id: session.clone(),
            run_id: run.clone(),
            limit: 64,
            after: None,
            expected_revision: None,
        })
        .await
        .unwrap();
    assert_eq!(page.phases.len(), 1);
    assert_eq!(&page.phases[0].target, target);
    let stopped = PhaseUncertainty {
        session_id: session.clone(),
        run_id: run.clone(),
        id: ControlId::new(),
        expected_revision: page.revision,
        target: target.clone(),
        cause: PhaseUncertaintyCause::HostStopped {
            claim: PhaseResolverClaim {
                resolver_id: "verified-reaper".into(),
                authority_claim: "exact phase scope joined".into(),
            },
        },
    };
    assert_eq!(
        agent
            .mark_phase_uncertain(stopped.clone())
            .await
            .unwrap_err(),
        PhaseError::InvalidTransition
    );
    let request = PhaseCleanup {
        session_id: session.clone(),
        run_id: run.clone(),
        id: ControlId::new(),
        expected_revision: page.revision,
        target: target.clone(),
        result: PhaseQuiescenceResult::Complete,
        claim: match stopped.cause {
            PhaseUncertaintyCause::HostStopped { claim } => claim,
            PhaseUncertaintyCause::ExpiredOrRevokedOwner => unreachable!(),
        },
    };
    let PhaseControlOutcome::Applied(receipt) =
        agent.record_phase_cleanup(request.clone()).await.unwrap()
    else {
        panic!("fresh cleanup")
    };
    assert_eq!(receipt.phase.quiescence, PhaseQuiescence::Complete);
    assert_eq!(
        agent.record_phase_cleanup(request).await.unwrap(),
        PhaseControlOutcome::Replayed(receipt)
    );
    assert_eq!(fixture.invocations.load(Ordering::SeqCst), 1);
}

async fn record_outcome(
    agent: &Agent,
    fixture: &Fixture,
    session: &crabber::SessionId,
    run: &crabber::core::RunId,
    target: &crabber::session::PhaseTarget,
) {
    let refused = request(fixture, session, run, target, UncertainPhaseChoice::Refuse).await;
    let PhaseResolutionOutcome::Applied(refusal) = agent
        .resolve_uncertain_phase(refused.clone())
        .await
        .unwrap()
    else {
        panic!("fresh refusal")
    };
    assert_eq!(refusal.phase.state, PhaseState::Uncertain);
    let output =
        ToolOutput::from_json(&json!({"host_protected":"external operation succeeded"})).unwrap();
    let resolved = request(
        fixture,
        session,
        run,
        target,
        UncertainPhaseChoice::Success { output },
    )
    .await;
    let PhaseResolutionOutcome::Applied(receipt) = agent
        .resolve_uncertain_phase(resolved.clone())
        .await
        .unwrap()
    else {
        panic!("fresh success")
    };
    assert_eq!(receipt.phase.state, PhaseState::Completed);
    assert_eq!(receipt.phase.quiescence, PhaseQuiescence::Incomplete);
    assert_eq!(
        agent
            .lookup_phase_mutation(session, &resolved.resolution.id)
            .await
            .unwrap(),
        Some(receipt.clone())
    );
    assert_eq!(
        agent.resolve_uncertain_phase(refused).await.unwrap(),
        PhaseResolutionOutcome::Replayed(refusal)
    );
    let mut retry = resolved.clone();
    retry.message_id = MessageId::new();
    retry.part_id = PartId::new();
    retry.created_at = OffsetDateTime::UNIX_EPOCH;
    assert_eq!(
        agent.resolve_uncertain_phase(retry).await.unwrap(),
        PhaseResolutionOutcome::Replayed(receipt)
    );
    let mut changed = resolved;
    if let PhaseCommand::ResolveUncertain { choice, .. } = &mut changed.resolution.command {
        *choice = UncertainPhaseChoice::Failure { output: None };
    }
    assert_eq!(
        agent.resolve_uncertain_phase(changed).await.unwrap_err(),
        PhaseError::SemanticConflict
    );
}

async fn request(
    fixture: &Fixture,
    session: &crabber::SessionId,
    run: &crabber::core::RunId,
    target: &crabber::session::PhaseTarget,
    choice: UncertainPhaseChoice,
) -> PhaseResolution {
    PhaseResolution {
        session_id: session.clone(),
        run_id: run.clone(),
        resolution: PhaseMutation {
            id: ControlId::new(),
            expected_revision: fixture.store.session_revision(session).await.unwrap(),
            command: PhaseCommand::ResolveUncertain {
                target: target.clone(),
                choice,
                claim: PhaseResolverClaim {
                    resolver_id: "verified-host".into(),
                    authority_claim: "external owner inspected the operation".into(),
                },
            },
        },
        message_id: MessageId::new(),
        part_id: PartId::new(),
        turn_id: None,
        created_at: OffsetDateTime::now_utc(),
        duration_ms: 0,
    }
}
