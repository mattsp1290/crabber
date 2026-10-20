//! Fresh-process read-only reconciliation for native phase operations.
use super::{Fixture, Mode, resolution, settlement, start};
use crate::{PhaseError, PhaseMutationOutcome, PhaseState, Store};
use crabber_core::{EventKind, ToolCallRecord, ToolCallStatus};

pub(crate) async fn verify_loss(store: &impl Store, fixture: Fixture) {
    if matches!(fixture.mode, Mode::Uncertainty | Mode::Cleanup) {
        super::controls::verify(store, &fixture).await;
        return;
    }
    if matches!(fixture.mode, Mode::Start) {
        verify_start_loss(store, fixture).await;
        return;
    }
    if matches!(fixture.mode, Mode::Success | Mode::Failure | Mode::Refuse) {
        verify_resolution_loss(store, fixture).await;
        return;
    }
    if matches!(fixture.mode, Mode::Settle) {
        verify_settlement_loss(store, fixture).await;
        return;
    }
    let receipt = store
        .lookup_phase_mutation(&fixture.session, &fixture.request.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        receipt.semantic_digest,
        fixture
            .request
            .fingerprint(&fixture.session, &fixture.fence.run_id)
            .unwrap()
    );
    let current = store
        .tool_phase(&fixture.fence.run_id, &receipt.phase.target.id)
        .await
        .unwrap();
    assert_eq!(current.phase, receipt.phase);
    assert_eq!(current.revision, receipt.revision);
    let calls = store
        .list_unfinished_tool_calls(&fixture.fence.run_id)
        .await
        .unwrap();
    let call = calls
        .iter()
        .find(|c| c.id == receipt.phase.target.call_id)
        .unwrap();
    let expected_call = if matches!(fixture.mode, Mode::Begin) {
        ToolCallStatus::Running
    } else {
        ToolCallStatus::Pending
    };
    assert_eq!(call.status, expected_call);
    assert_eq!(
        store
            .list_events(&fixture.session, None, 100)
            .await
            .unwrap()
            .len(),
        fixture.mode.preoperations() + 2
    );
    let execution = store.execution(fixture.fence.clone()).await.unwrap();
    assert!(
        matches!(execution.mutate_phase(fixture.request, None).await.unwrap(), PhaseMutationOutcome::Replayed(r) if r == receipt)
    );
    assert_eq!(
        store
            .tool_phase(&fixture.fence.run_id, &receipt.phase.target.id)
            .await
            .unwrap(),
        current
    );
    let expected = match fixture.mode {
        Mode::Claim => PhaseState::Claimed,
        Mode::Begin => PhaseState::Started,
        Mode::Success => PhaseState::Completed,
        Mode::Failure => PhaseState::Failed,
        Mode::Refuse => PhaseState::Uncertain,
        Mode::Settle | Mode::Start | Mode::Uncertainty | Mode::Cleanup => unreachable!(),
    };
    assert_eq!(receipt.phase.state, expected);
}

async fn verify_settlement_loss(store: &impl Store, fixture: Fixture) {
    let receipt = store
        .lookup_phase_mutation(&fixture.session, &fixture.request.id)
        .await
        .unwrap()
        .unwrap();
    let request = settlement(&fixture);
    assert_eq!(
        receipt.semantic_digest,
        request
            .fingerprint(&fixture.session, &fixture.fence.run_id)
            .unwrap()
    );
    assert_eq!(receipt.phase.state, PhaseState::Completed);
    assert_eq!(receipt.phase.quiescence, crate::PhaseQuiescence::Complete);
    assert_eq!(
        store
            .tool_phase(&fixture.fence.run_id, &receipt.phase.target.id)
            .await
            .unwrap()
            .phase,
        receipt.phase
    );
    assert_eq!(
        store.session_revision(&fixture.session).await.unwrap(),
        receipt.revision
    );
    assert_eq!(
        store
            .list_unfinished_tool_calls(&fixture.fence.run_id)
            .await
            .unwrap(),
        [] as [ToolCallRecord; 0]
    );
    let messages = store.list_messages(&fixture.session, None).await.unwrap();
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[1].id, request.message_id);
    let events = store
        .list_events(&fixture.session, None, 100)
        .await
        .unwrap();
    assert_eq!(events.len(), fixture.mode.preoperations() + 3);
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == EventKind::ToolCallSettled)
            .count(),
        1
    );
    let execution = store.execution(fixture.fence).await.unwrap();
    assert!(
        matches!(execution.settle_tool_phase(request, None).await.unwrap(), PhaseMutationOutcome::Replayed(saved) if saved == receipt)
    );
    assert_eq!(
        store.list_messages(&fixture.session, None).await.unwrap(),
        messages
    );
    assert_eq!(
        store
            .list_events(&fixture.session, None, 100)
            .await
            .unwrap(),
        events
    );
}

async fn verify_start_loss(store: &impl Store, fixture: Fixture) {
    let receipt = store
        .lookup_phase_mutation(&fixture.session, &fixture.request.id)
        .await
        .unwrap()
        .unwrap();
    let request = start(&fixture);
    assert_eq!(
        receipt.semantic_digest,
        request
            .fingerprint(&fixture.session, &fixture.fence.run_id)
            .unwrap()
    );
    assert_eq!(receipt.phase.state, PhaseState::Started);
    let call = store
        .list_unfinished_tool_calls(&fixture.fence.run_id)
        .await
        .unwrap()
        .remove(0);
    assert_eq!(call.status, ToolCallStatus::Running);
    assert!(call.result.is_none());
    let events = store
        .list_events(&fixture.session, None, 1000)
        .await
        .unwrap();
    assert_eq!(events.len(), fixture.mode.preoperations() + 3);
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == EventKind::ToolCallRunning)
            .count(),
        1
    );
    let execution = store.execution(fixture.fence.clone()).await.unwrap();
    assert!(
        matches!(execution.start_tool_phase(request, None).await.unwrap(), PhaseMutationOutcome::Replayed(saved) if saved == receipt)
    );
    assert_eq!(
        execution
            .mutate_phase(fixture.request, None)
            .await
            .unwrap_err(),
        PhaseError::SemanticConflict
    );
    assert_eq!(
        store
            .list_events(&fixture.session, None, 1000)
            .await
            .unwrap(),
        events
    );
    assert_eq!(
        store.session_revision(&fixture.session).await.unwrap(),
        receipt.revision
    );
}

async fn verify_resolution_loss(store: &impl Store, fixture: Fixture) {
    let request = resolution(&fixture);
    let receipt = store
        .lookup_phase_mutation(&fixture.session, &fixture.request.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt.semantic_digest, request.fingerprint().unwrap());
    assert_eq!(receipt.phase.quiescence, crate::PhaseQuiescence::Pending);
    assert_eq!(
        store.get_run(&fixture.fence.run_id).await.unwrap(),
        Some(fixture.initial_run)
    );
    let attempt = store
        .latest_execution_attempt(&fixture.fence.run_id)
        .await
        .unwrap()
        .unwrap()
        .attempt;
    assert_eq!(attempt.state, crate::AttemptState::Started);
    assert_eq!(attempt.cleanup, crate::AttemptCleanup::Pending);
    let calls = crate::phase_contract::settlement::read_calls(store, &fixture.session)
        .await
        .unwrap();
    let call = calls
        .iter()
        .find(|call| call.id == receipt.phase.target.call_id)
        .unwrap();
    request.verify_replay(&receipt, call).unwrap();
    let messages = store.list_messages(&fixture.session, None).await.unwrap();
    let events = store
        .list_events(&fixture.session, None, 100)
        .await
        .unwrap();
    let refused = matches!(fixture.mode, Mode::Refuse);
    assert_eq!(messages.len(), if refused { 1 } else { 2 });
    assert_eq!(
        events.len(),
        fixture.mode.preoperations() + if refused { 2 } else { 3 }
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == EventKind::ToolCallSettled)
            .count(),
        usize::from(!refused)
    );
    if !refused {
        assert_eq!(messages[1].id, request.message_id);
    }
    assert_eq!(
        store.resolve_uncertain_phase(request).await.unwrap(),
        crate::PhaseResolutionOutcome::Replayed(receipt.clone())
    );
    assert_eq!(
        store.list_messages(&fixture.session, None).await.unwrap(),
        messages
    );
    assert_eq!(
        store
            .list_events(&fixture.session, None, 100)
            .await
            .unwrap(),
        events
    );
    assert_eq!(
        store.session_revision(&fixture.session).await.unwrap(),
        receipt.revision
    );
}
