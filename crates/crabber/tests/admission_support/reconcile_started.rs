//! A reaped OS worker supplies cleanup evidence, never a replay permission.
use super::*;
use crabber::core::{ControlId, MessageId, PartId, ToolCallStatus, ToolResultStatus};
use crabber::session::{
    AttemptError, AttemptState, PhaseCleanup, PhaseCommand, PhaseControlOutcome, PhaseMutation,
    PhasePageRequest, PhaseQuiescence, PhaseQuiescenceResult, PhaseResolution,
    PhaseResolutionOutcome, PhaseResolverClaim, PhaseState, PhaseUncertainty,
    PhaseUncertaintyCause,
};

pub(super) async fn recover(
    agent: &Agent,
    store: &dyn Store,
    session: &SessionId,
    receipt: &AdmissionReceipt,
    old: Run,
    dir: &Path,
) {
    let attempt = store
        .latest_execution_attempt(&receipt.run_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(attempt.attempt.state, AttemptState::Started);
    let before_events = store.list_events(session, None, 1000).await.unwrap();
    let before_revision = store.session_revision(session).await.unwrap();
    assert!(matches!(
        agent.resume(&receipt.run_id).await,
        Err(RuntimeError::Attempt(AttemptError::Busy))
    ));
    assert_eq!(store.get_run(&receipt.run_id).await.unwrap().unwrap(), old);
    assert_eq!(
        store.session_revision(session).await.unwrap(),
        before_revision
    );
    assert_eq!(
        store.list_events(session, None, 1000).await.unwrap(),
        before_events
    );
    let page = agent
        .list_tool_phases(PhasePageRequest {
            session_id: session.clone(),
            run_id: receipt.run_id.clone(),
            limit: 64,
            after: None,
            expected_revision: None,
        })
        .await
        .unwrap();
    assert!(page.next_after.is_none());
    assert_eq!(page.phases.len(), 1);
    let phase = &page.phases[0];
    assert_eq!(phase.state, PhaseState::Started);
    assert_eq!(phase.quiescence, PhaseQuiescence::Pending);
    let calls = store
        .list_unfinished_tool_calls(&receipt.run_id)
        .await
        .unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].status, ToolCallStatus::Running);
    assert!(calls[0].result.is_none());
    let turn = before_events
        .iter()
        .find(|event| {
            event.kind == crabber::core::EventKind::ToolCallRunning
                && event.payload["call_id"] == phase.target.call_id.0
        })
        .unwrap()
        .turn_id
        .clone();
    assert!(turn.is_some());
    reconcile(
        agent,
        session,
        receipt,
        phase.target.clone(),
        page.revision,
        turn,
        dir,
    )
    .await;
    verify_reconciled(agent, store, session, receipt, &old, &attempt.attempt, dir).await;
}
async fn verify_reconciled(
    agent: &Agent,
    store: &dyn Store,
    session: &SessionId,
    receipt: &AdmissionReceipt,
    old: &Run,
    attempt: &crabber::session::AttemptSnapshot,
    dir: &Path,
) {
    assert_eq!(&store.get_run(&receipt.run_id).await.unwrap().unwrap(), old);
    assert_eq!(
        store
            .latest_execution_attempt(&receipt.run_id)
            .await
            .unwrap()
            .unwrap()
            .attempt,
        *attempt
    );
    assert!(matches!(
        agent.resume(&receipt.run_id).await,
        Err(RuntimeError::Attempt(AttemptError::Busy))
    ));
    let call = store
        .snapshot(crabber::session::SnapshotRequest {
            session_id: session.clone(),
            continuation: None,
            limits: crabber::session::SnapshotLimits {
                messages: 100,
                tool_calls: 100,
                parts: 100,
                text_bytes: 100_000,
                encoded_bytes: 1_000_000,
            },
        })
        .await
        .unwrap();
    let crabber::session::SnapshotOutcome::Page(snapshot) = call else {
        panic!("fixture snapshot")
    };
    assert!(snapshot.continuation.is_none());
    assert_eq!(snapshot.tool_calls.len(), 1);
    let result = snapshot.tool_calls[0].result.as_ref().unwrap();
    assert_eq!(snapshot.tool_calls[0].status, ToolCallStatus::Failed);
    assert_eq!(result.status, ToolResultStatus::Failed);
    assert_eq!(
        result.content,
        crabber::core::ToolOutput::new(vec![], true)
            .unwrap()
            .to_content_blocks()
    );
    let events = store.list_events(session, None, 1000).await.unwrap();
    assert!(
        !events
            .iter()
            .any(|event| event.kind == crabber::core::EventKind::RunSettled)
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == crabber::core::EventKind::ToolCallSettled)
            .count(),
        1
    );
    assert_eq!(count(&dir.join("ledger"), "provider"), 1);
    assert_eq!(count(&dir.join("ledger"), "tool"), 1);
    assert_eq!(
        agent
            .lookup_admission(session, &options().key)
            .await
            .unwrap(),
        Some(receipt.clone())
    );
}
async fn reconcile(
    agent: &Agent,
    session: &SessionId,
    receipt: &AdmissionReceipt,
    target: crabber::session::PhaseTarget,
    revision: crabber::core::SessionRevision,
    turn: Option<crabber::core::TurnId>,
    dir: &Path,
) {
    let proof: serde_json::Value =
        serde_json::from_slice(&fs::read(dir.join("owner-stop-proof")).unwrap()).unwrap();
    assert_eq!(proof["run_id"], receipt.run_id.0);
    assert_eq!(proof["joined"], true);
    let pid = proof["pid"].as_u64().unwrap();
    assert!(pid > 0);
    let status = proof["status"].as_str().unwrap();
    assert_ne!(status, "");
    let claim = PhaseResolverClaim {
        resolver_id: "restart-fixture-parent".into(),
        authority_claim: format!(
            "fixture parent killed and reaped OS worker {pid}: {status}; tool has only in-process resources"
        ),
    };
    let uncertain = PhaseUncertainty {
        session_id: session.clone(),
        run_id: receipt.run_id.clone(),
        id: ControlId::new(),
        expected_revision: revision,
        target: target.clone(),
        cause: PhaseUncertaintyCause::HostStopped {
            claim: claim.clone(),
        },
    };
    let PhaseControlOutcome::Applied(marked) =
        agent.mark_phase_uncertain(uncertain.clone()).await.unwrap()
    else {
        panic!("fresh stopped-owner assertion")
    };
    assert_eq!(marked.phase.state, PhaseState::Uncertain);
    assert_eq!(marked.phase.quiescence, PhaseQuiescence::Pending);
    let cleanup = PhaseCleanup {
        session_id: session.clone(),
        run_id: receipt.run_id.clone(),
        id: ControlId::new(),
        expected_revision: marked.revision,
        target: target.clone(),
        result: PhaseQuiescenceResult::Complete,
        claim: claim.clone(),
    };
    let PhaseControlOutcome::Applied(joined) =
        agent.record_phase_cleanup(cleanup.clone()).await.unwrap()
    else {
        panic!("fresh resource join proof")
    };
    assert_eq!(joined.phase.state, PhaseState::Uncertain);
    assert_eq!(joined.phase.quiescence, PhaseQuiescence::Complete);
    let resolution = PhaseResolution {
        session_id: session.clone(),
        run_id: receipt.run_id.clone(),
        resolution: PhaseMutation {
            id: ControlId::new(),
            expected_revision: joined.revision,
            command: PhaseCommand::ResolveUncertain {
                target,
                claim,
                choice: crabber::session::UncertainPhaseChoice::Failure { output: None },
            },
        },
        message_id: MessageId::new(),
        part_id: PartId::new(),
        turn_id: turn,
        created_at: time::OffsetDateTime::now_utc(),
        duration_ms: 0,
    };
    let PhaseResolutionOutcome::Applied(resolved) = agent
        .resolve_uncertain_phase(resolution.clone())
        .await
        .unwrap()
    else {
        panic!("fresh external failure choice")
    };
    assert_eq!(resolved.phase.state, PhaseState::Failed);
    assert_eq!(resolved.phase.quiescence, PhaseQuiescence::Complete);
    assert!(
        matches!(agent.mark_phase_uncertain(uncertain).await.unwrap(), PhaseControlOutcome::Replayed(saved) if saved == marked)
    );
    assert!(
        matches!(agent.record_phase_cleanup(cleanup).await.unwrap(), PhaseControlOutcome::Replayed(saved) if saved == joined)
    );
    assert!(
        matches!(agent.resolve_uncertain_phase(resolution).await.unwrap(), PhaseResolutionOutcome::Replayed(saved) if saved == resolved)
    );
}
